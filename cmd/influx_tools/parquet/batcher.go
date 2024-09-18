package parquet

import (
	"context"
	"fmt"
	"os"
	"sort"

	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/tsdb"
	"github.com/influxdata/influxql"
)

type row struct {
	timestamp int64
	tags      map[string]string
	fields    map[string]interface{}
}

type batcher struct {
	measurement []byte
	shard       *tsdb.Shard

	converter   map[string]func(interface{}) (interface{}, error)
	nameMapping map[string]string

	series []seriesEntry
	start  int64
}

func newBatcher(
	ctx context.Context,
	shard *tsdb.Shard,
	measurement string,
	series []seriesEntry,
	converter map[string]func(interface{}) (interface{}, error),
	nameMapping map[string]string,
) (*batcher, error) {
	seriesCursor, err := shard.CreateSeriesCursor(
		ctx,
		tsdb.SeriesCursorRequest{},
		influxql.MustParseExpr("_name = '"+measurement+"'"),
	)
	if err != nil {
		return nil, fmt.Errorf("getting series cursor failed: %w", err)
	}
	defer seriesCursor.Close()

	return &batcher{
		measurement: []byte(measurement),
		shard:       shard,
		series:      series,
		converter:   converter,
		nameMapping: nameMapping,
		start:       models.MinNanoTime,
	}, nil
}

func (b *batcher) reset() {
	b.start = models.MinNanoTime
}

func (b *batcher) next(ctx context.Context) ([]row, error) {
	// Iterate over the series and fields and accumulate the data row-wise
	iter, err := b.shard.CreateCursorIterator(ctx)
	if err != nil {
		return nil, fmt.Errorf("getting cursor iterator for %q failed: %w", string(b.measurement), err)
	}

	data := make(map[string]map[int64]row)
	end := models.MaxNanoTime
	for _, s := range b.series {
		data[s.key] = make(map[int64]row)
		tags := make(map[string]string, len(s.tags))
		for _, t := range s.tags {
			tags[string(t.Key)] = string(t.Value)
		}
		for field := range s.fields {
			cursor, err := iter.Next(ctx,
				&tsdb.CursorRequest{
					Name:      b.measurement,
					Tags:      s.tags,
					Field:     field,
					Ascending: true,
					StartTime: b.start,
					EndTime:   models.MaxNanoTime,
				},
			)
			if err != nil {
				return nil, fmt.Errorf("getting cursor for %s-%s failed: %w", s.key, field, err)
			}
			if cursor == nil {
				continue
			}

			// Prepare mappings
			fname := field
			if n, found := b.nameMapping[field]; found {
				fname = n
			}
			converter := identity
			if c, found := b.converter[field]; found {
				converter = c
			}
			fieldEnd := models.MaxNanoTime
			switch c := cursor.(type) {
			case tsdb.IntegerArrayCursor:
				values := c.Next()
				for i, t := range values.Timestamps {
					v, err := converter(values.Values[i])
					if err != nil {
						fmt.Fprintf(os.Stderr, "converting %v of field %q failed: %v", values.Values[i], field, err)
						continue
					}

					if _, found := data[s.key][t]; !found {
						data[s.key][t] = row{
							timestamp: t,
							tags:      tags,
							fields:    make(map[string]interface{}),
						}
					}

					data[s.key][t].fields[fname] = v
					fieldEnd = t
				}
			case tsdb.FloatArrayCursor:
				values := c.Next()
				for i, t := range values.Timestamps {
					v, err := converter(values.Values[i])
					if err != nil {
						fmt.Fprintf(os.Stderr, "converting %v of field %q failed: %v", values.Values[i], field, err)
						continue
					}

					if _, found := data[s.key][t]; !found {
						data[s.key][t] = row{
							timestamp: t,
							tags:      tags,
							fields:    make(map[string]interface{}),
						}
					}

					data[s.key][t].fields[fname] = v
					fieldEnd = t
				}
			case tsdb.UnsignedArrayCursor:
				values := c.Next()
				for i, t := range values.Timestamps {
					v, err := converter(values.Values[i])
					if err != nil {
						fmt.Fprintf(os.Stderr, "converting %v of field %q failed: %v", values.Values[i], field, err)
						continue
					}

					if _, found := data[s.key][t]; !found {
						data[s.key][t] = row{
							timestamp: t,
							tags:      tags,
							fields:    make(map[string]interface{}),
						}
					}

					data[s.key][t].fields[fname] = v
					fieldEnd = t
				}
			case tsdb.BooleanArrayCursor:
				values := c.Next()
				for i, t := range values.Timestamps {
					v, err := converter(values.Values[i])
					if err != nil {
						fmt.Fprintf(os.Stderr, "converting %v of field %q failed: %v", values.Values[i], field, err)
						continue
					}

					if _, found := data[s.key][t]; !found {
						data[s.key][t] = row{
							timestamp: t,
							tags:      tags,
							fields:    make(map[string]interface{}),
						}
					}

					data[s.key][t].fields[fname] = v
					fieldEnd = t
				}
			case tsdb.StringArrayCursor:
				values := c.Next()
				for i, t := range values.Timestamps {
					v, err := converter(values.Values[i])
					if err != nil {
						fmt.Fprintf(os.Stderr, "converting %v of field %q failed: %v", values.Values[i], field, err)
						continue
					}

					if _, found := data[s.key][t]; !found {
						data[s.key][t] = row{
							timestamp: t,
							tags:      tags,
							fields:    make(map[string]interface{}),
						}
					}

					data[s.key][t].fields[fname] = v
					fieldEnd = t
				}
			default:
				cursor.Close()
				panic(fmt.Errorf("unexpected type %T", cursor))
			}
			cursor.Close()
			end = min(end, fieldEnd)
		}
	}
	if len(data) == 0 {
		return nil, nil
	}

	// Extract the rows ordered by timestamp
	var rows []row
	for _, tmap := range data {
		for _, r := range tmap {
			rows = append(rows, r)
		}
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].timestamp < rows[j].timestamp })

	// Only include rows that are before the end-timestamp to avoid duplicate
	// or incomplete entries due to not iterating through all data
	n := sort.Search(len(rows), func(i int) bool { return rows[i].timestamp > end })

	// Remember the earliest datum to use this for the next batch excluding the entry itself
	b.start = end + 1

	return rows[:n], nil
}
