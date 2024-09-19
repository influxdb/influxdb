package parquet

import (
	"context"
	"fmt"
	"sort"

	"go.uber.org/zap"

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

	typeResolutions map[string]influxql.DataType
	converter       map[string]func(interface{}) (interface{}, error)
	nameResolutions map[string]string

	series []seriesEntry
	start  int64

	logger *zap.SugaredLogger
}

func (b *batcher) init() error {
	// Setup the type converters for the conflicting fields
	b.converter = make(map[string]func(interface{}) (interface{}, error), len(b.typeResolutions))
	for field, ftype := range b.typeResolutions {
		switch ftype {
		case influxql.Float:
			b.converter[field] = toFloat
		case influxql.Unsigned:
			b.converter[field] = toUint
		case influxql.Integer:
			b.converter[field] = toInt
		case influxql.Boolean:
			b.converter[field] = toBool
		case influxql.String:
			b.converter[field] = toString
		default:
			return fmt.Errorf("unknown converter %v for field %q", ftype, field)
		}
	}

	b.start = models.MinNanoTime

	return nil
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
			if n, found := b.nameResolutions[field]; found {
				fname = n
			}
			converter := identity
			if c, found := b.converter[field]; found {
				converter = c
			}
			fieldEnd := models.MaxNanoTime

			c, err := newValueCursor(cursor)
			if err != nil {
				return nil, fmt.Errorf("creating value cursor failed: %w", err)
			}

			for {
				// Check if we do still have data
				timestamp, ok := c.peek()
				if !ok {
					break
				}

				timestamp, value := c.next()
				v, err := converter(value)
				if err != nil {
					b.logger.Errorf("converting %v of field %q failed: %v", value, field, err)
					continue
				}

				if _, found := data[s.key][timestamp]; !found {
					data[s.key][timestamp] = row{
						timestamp: timestamp,
						tags:      tags,
						fields:    make(map[string]interface{}),
					}
				}

				data[s.key][timestamp].fields[fname] = v
				fieldEnd = timestamp
			}

			c.close()
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
