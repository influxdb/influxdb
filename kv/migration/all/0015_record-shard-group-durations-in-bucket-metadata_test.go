package all

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/influxdata/influxdb/v2/kv"
	"github.com/stretchr/testify/require"
)

func TestMigration_ShardGroupDuration(t *testing.T) {
	testRepairMissingShardGroupDurations(t, 15)
}

func testRepairMissingShardGroupDurations(t *testing.T, migrationNum int) {
	t.Helper()
	for _, useTokenHashing := range []bool{false, true} {
		name := fmt.Sprintf("RepairMissingShardGroupDurations/migrationNum=%d/useTokenHashing=%t", migrationNum, useTokenHashing)
		t.Run(name, func(t *testing.T) {
			testRepairMissingShardGroupDurationsWithTokenHashing(t, migrationNum, useTokenHashing)
		})
	}
}

func testRepairMissingShardGroupDurationsWithTokenHashing(t *testing.T, migrationNum int, useTokenHashing bool) {
	ctx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()

	// Run up to the migration before the migration-under-test.
	ts := newService(t, ctx, migrationNum-2, useTokenHashing)

	// Seed some buckets.
	buckets := []*influxdb.Bucket{
		{
			ID:              platform.ID(1),
			Name:            "infinite",
			OrgID:           ts.Org.ID,
			RetentionPeriod: 0,
		},
		{
			ID:              platform.ID(2),
			Name:            "1w",
			OrgID:           ts.Org.ID,
			RetentionPeriod: humanize.Week,
		},
		{
			ID:              platform.ID(3),
			Name:            "1d",
			OrgID:           ts.Org.ID,
			RetentionPeriod: humanize.Day,
		},
		{
			ID:              platform.ID(4),
			Name:            "1h",
			OrgID:           ts.Org.ID,
			RetentionPeriod: time.Hour,
		},
	}

	bucketBucket := []byte("bucketsv1")
	ids := make([][]byte, len(buckets))
	err := ts.Store.Update(context.Background(), func(tx kv.Tx) error {
		bkt, err := tx.Bucket(bucketBucket)
		require.NoError(t, err)
		for i, b := range buckets {
			js, err := json.Marshal(b)
			require.NoError(t, err)

			ids[i], err = b.ID.Encode()
			require.NoError(t, err)
			require.NoError(t, bkt.Put(ids[i], js))
		}
		return nil
	})
	require.NoError(t, err)

	// Run the migration-under-test.
	require.NoError(t, Migrations[migrationNum-1].Up(context.Background(), ts.Store))

	// Read the buckets back out of the store.
	migratedBuckets := make([]influxdb.Bucket, len(buckets))
	err = ts.Store.View(context.Background(), func(tx kv.Tx) error {
		bkt, err := tx.Bucket(bucketBucket)
		require.NoError(t, err)

		rawBuckets, err := bkt.GetBatch(ids...)
		require.NoError(t, err)

		for i, rawBucket := range rawBuckets {
			require.NoError(t, json.Unmarshal(rawBucket, &migratedBuckets[i]))
		}

		return nil
	})
	require.NoError(t, err)

	// Check that normalized shard-group durations were backfilled.
	require.Equal(t, humanize.Week, migratedBuckets[0].ShardGroupDuration)
	require.Equal(t, humanize.Day, migratedBuckets[1].ShardGroupDuration)
	require.Equal(t, time.Hour, migratedBuckets[2].ShardGroupDuration)
	require.Equal(t, time.Hour, migratedBuckets[3].ShardGroupDuration)
}
