# WAL (write-ahead-log) and Snapshot process

All writes get buffered into a wal buffer and then every flush interval all the contents are flushed to disk at which point the writes become durable and
the clients are sent the confirmation that their writes are successful. At each flush interval there is also a check to see if queryable buffer need to
evict data to disk, this process is called snapshotting. The rest of this doc discusses all the moving parts in wal flush and snapshotting.


## Overview

```



                                 ┌────────────┐           ┌────────────┐
                                 │flush buffer│──────────►│ wal buffer │
                                 └────────────┘           └────────────┘
                                    ▲
                                    │
                (takes everything from wal buffer and writes to wal file)
                                    │
                                    │   (actual background_wal_flush uses wal trait - which wal obj store impls)
                                    │        ┌──────────────(background create)───────────────────────────┐
                                    │        │                                                            ▼
                              ┌─────┴──────┐ │                                                        ┌────────┐
                        ┌────►│wal objstore├─┴──(remove wal,need each wal file num / drop semaphore)─►│wal file│
                        │     └─────┬──────┘                                                          └────────┘
                        │           │
                        │           │
                        │           │
                        │           │
                        │           │
                        │           │
                        │   (notifies wal
                        │    and snapshot optionally & snapshot semaphore taken)    ┌─────────────┐
                        │           │                              ┌───────────────►│update buffer│ (whatever wal flushed into chunks)
                        │           ▼                              │                └─────────────┘
                        │     ┌────────────┐                       │                ┌─────────────┐
                        │     │query buffer├───────────────────────┼───────────────►│parquet file │
                        │     └─────┬──────┘                       │                └─────────────┘
                        │           │                              │
                        │   (run snapshot)                         │                ┌─────────────┐
                        └───────────┘                              ├───────────────►│snapshot file│ (holds last wal seq number)
                                                                   │                └─────────────┘
                                                                   │                ┌────────────┐
                                                                   └───────────────►│clear buffer│ (whatever snapshotted is removed)
                                                                                    └────────────┘
```


#### Steps

1. When _writes_ comes in, they go into a write batch in wal buffer. These batches are held per database and the batches keep track of min
   and max times within each batch. These batches further hold per table chunks. This chunk is created by taking incoming rows and pinning
   them to a period. It is done by `t - (t % gen_1_duration)`. If `gen_1_duration` is 10 mins, then all of the rows will be divided into
   10 min chunks. As an example if there are rows for 10.29 and 10.35 then they both go into 2 separate chunks (10.20 and 10.30). And this
   10.20 and 10.30 are used later as the key in queryable buffer.
2. Every flush interval, the wal buffer is flushed and all batches are written to to wal file (converts to wal content and gets min/max
   times from all batches) and every time wal file contents are written, we add wal period to snapshot tracker that tracks min/max times across
   batches that went into single wal file
3. Then snapshot tracker is checked to see if there are enough wal periods to trigger snapshot. There are different conditions checked, the
   common scenarios are,
     - wal periods > (1.5 * snapshot size). e.g default settings will lead to snapshot size = 600, if we have 900 wal periods trigger snapshot
     - force snapshot, irrespective of sizes go ahead and run snapshot

   If going ahead with force snapshotting, pick all the wal periods in the tracker and find the max time from most recent wal period. This will be
   used as the `end_time_marker` to evict data from query buffer. Because forcing a snapshot can be triggered when wal buffer is empty (even though
   queryable buffer is full), we need to add `Noop` (a no-op WalOp) to the wal file to hold the snapshot details in wal file.

   ```

                                          Snapshot (all, emptying wal periods in tracker)
               ▲                           ▲
               │◄───wal in snapshot───────►│
               ┌──────┬──────┬──────┬──────┤
               │  0   │  1   │  2   │  3   │
               │      │      │      │      │
           ────┴──────┴──────┴──────┴──────┴─────► (time)

   ```

   If it is a normal snapshot, then leave one wal period (`3` in eg below) and pick the last one (`2` in eg below) max time used as `end_time_marker`
   and every wal period with max time is less than this `end_time_marker` will be removed from snapshot tracker.

   ```
                                          Snapshot (wal max < last wal period max,
                                           3 is max here that is left behind)
               ▲                    ▲      ▲
               │◄─ wal in snapshot─►│      │
               ┌──────┬──────┬──────┬──────┤
               │  0   │  1   │  2   │  3   │
               │      │      │      │      │
           ────┴──────┴──────┴──────┴──────┴─────────────► (time)

   ```
   At this point we may or may not have to snapshot, but the wal buffer will still need to be emptied into the wal file. The clients that have been
   writing data are held up at most of flush interval time (defaults to 1s). The clients get notified together that the writes have been successful
   when writes are persisted to wal file.

4. Then query buffer is notified to update the buffer, and query buffer does the following things in sequence,
      - it updates the buffer with incoming rows
      - writes to parquet file,
      - writes snapshot summary file
      - clears the buffer (using the `end_time_marker` that has been passed along)

   It is useful to visualize how query buffer holds data internally to understand how the buffer is cleared.
     - query buffer holds data as mapping between chunk time and MutableTableChunks (these are col id -> arrow array builder mappings with
       min/max times for that chunk), looks roughly like below
         ```

            │
            ├───10.20───────────►┌────────────────────────────┐
            │                    │  chunk 10.20 - 10.29       │
            │                    │                            │
            ├───10.30───────────►├────────────────────────────┤
            │                    │  chunk 10.30 - 10.39       │
            │                    │                            │
            ├───10.40───────────►└────────────────────────────┘
            │
            ▼
            time

         ```
       The crucial thing to note here is, `10.20` and `10.30` are the keys and they're taken from the write batches that have been derived
       from newly added rows. These are added to query buffer which manages an arrow backed buffer that holds all the newly added rows.

     - Above mutable chunks are added each time a wal flush happens, when snapshotting it uses the `end_time_marker` to evict data. Say,
       10.30 is the `end_time_marker` to evict data from queryable buffer, then query buffer evicts all data before 10.30 and holds it as
       snapshot chunk which has converted the arrow arrays to a record batch that's ready to be passed to sort/dedupe before writing to
       parquet file. Then a summary snapshot file is written which tracks what wal file number was last snapshotted along with pointers to
       parquet files created. This data is updated in persisted files so that any new query which spans that time can find data from the
       parquet files as buffer will not have them anymore.

5. Once snapshotting process is complete, now deletes all the wal files up to a configured number of wal files to retain. When replaying
   the last snapshotted file is looked up from snapshot summary file that has been created so none of the wal files that've already been
   snapshotted is loaded into the buffer.
6. If server crashed and restarted wal replay happens. But before that all snapshots are loaded, but even though snapshot file is written
   out it doesn't guarantee that wal files relevant to snapshot has been removed.

## Example

Below section walks through some of the nuances touched in the overall process described above

- These are the wal files (1-4) with data for different time ranges [20 - 50] etc. Notice data can be overlapping between time periods
```
1[20 - 50]
2[31 - 70]
3[51 - 90]
4[45 - 110] - snapshot! <= 2 (wal file num), <= 70 (end_time_marker). snapshot details has a file number and end_time_marker.
```

- This is a mapping between queryable buffer's chunk time and what wal files the data comes from. This is not how queryable buffer
  maps the data internally, it doesn't know about wal files - however this mapping helps visualise the dependency with wal file better.
  The data for time slice 20 originates purely from wal file 1, for 40 it holds rows from wal files 1, 2 and 4 etc.
```
20 - 1
30 - 1
40 - 1, 2, 4
50 - 2, 4
60 - 2, 3, 4
70 - 2, 3, 4
80 - 3, 4
90 - 3, 4
```

- At this point, lets say we need to snapshot as per the details above (`snapshot! <= 2 (wal file num), <= 70 (end_time_marker)`) file is written out
  & parquet files removing all items in queryable buffer upto time 70. This is the left over in queryable buffer -
  mainly the data that was in wal files 3, 4 are now in parquet files already.
```
80 - 3, 4
90 - 3, 4
```

- If there's been a restart at this point data is loaded from wal files (due to restart), times 60 and 70 will be loaded into memory. so it may look like this,
```
60 - 3, 4
70 - 3, 4
80 - 3, 4
90 - 3, 4
```

- And if further snapshot is kicked off by replaying earlier snapshot from wal file 4, because the `end_time_marker`(=70) is still  there in wal file
  we'd end up removing it from the query buffer leaving the query buffer in state as expected.
```
80 - 3, 4
90 - 3, 4
```

Say instead we force snapshotted instead of normal snapshotting,

- Everything will be removed from query buffer. These are the wal files with data for different time ranges [20 - 50] etc. Notice data can be overlapping between time periods
```
1[20 - 50]
2[31 - 70]
3[51 - 90]
4[45 - 110] - snapshot! <= 4 (wal file num), <= 110 (end_time_marker)
```

- And query buffer looks like,
```
20 - 1
30 - 1
40 - 1, 2, 4
50 - 2, 4
60 - 2, 3, 4
70 - 2, 3, 4
80 - 3, 4
90 - 3, 4
100 - 4
110 - 4
```

- At this point, snapshot file is written out & parquet files removing all items in queryable buffer upto time 110. So, all files 1, 2, 3, 4 are ready for deletion
- There should be nothing in queryable buffer as end time marker is 110 and if there's been a restart at this point there are no wal files to load and everything is in parquet files (snapshotted)

