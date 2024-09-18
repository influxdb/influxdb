package parquet

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/apache/arrow/go/v16/arrow"

	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/tsdb"
	"github.com/influxdata/influxql"
)

type seriesEntry struct {
	key    string
	tags   models.Tags
	fields map[string]influxql.DataType
}

type schemaCreator struct {
	measurement string
	shards      []*tsdb.Shard

	series map[uint64][]seriesEntry

	tags      []string
	fields    map[string]influxql.DataType
	fieldKeys []string
	conflicts map[string][]influxql.DataType

	typeResolutions map[string]influxql.DataType
	nameResolutions map[string]string
}

func (s *schemaCreator) extractSchema(ctx context.Context) error {
	// Iterate over the shards and extract all series
	for _, shard := range s.shards {
		// Extract all fields of the measurement
		fields := shard.MeasurementFields([]byte(s.measurement)).FieldSet()
		if len(fields) == 0 {
			continue
		}

		// Collect all available series in the shard and measurement and store
		// them for later use and series accumulation
		seriesCursor, err := shard.CreateSeriesCursor(
			ctx,
			tsdb.SeriesCursorRequest{},
			influxql.MustParseExpr("_name = '"+s.measurement+"'"),
		)
		if err != nil {
			return fmt.Errorf("getting series cursor failed: %w", err)
		}
		defer seriesCursor.Close()

		for {
			cur, err := seriesCursor.Next()
			if err != nil {
				return fmt.Errorf("advancing series cursor failed: %w", err)
			}
			if cur == nil {
				break
			}
			mname := string(cur.Name)
			if mname != s.measurement {
				continue
			}

			s.series[shard.ID()] = append(s.series[shard.ID()], seriesEntry{
				key:    s.measurement + "." + string(cur.Tags.HashKey(true)),
				tags:   cur.Tags.Clone(),
				fields: fields,
			})
		}
	}

	// Collect all tags and fields for creating the overall schema
	tags := make(map[string]bool)
	conflicts := make(map[string]map[influxql.DataType]bool)
	s.fields = make(map[string]influxql.DataType)
	for _, series := range s.series {
		for _, serie := range series {
			// Dedup tag keys
			for _, tag := range serie.tags {
				tags[string(tag.Key)] = true
			}
			// Detect field type conflicts and collect the fields
			for name, current := range serie.fields {
				if existing, found := s.fields[name]; found && current != existing {
					if _, exists := conflicts[name]; !exists {
						conflicts[name] = make(map[influxql.DataType]bool)
					}
					conflicts[name][current] = true
					conflicts[name][existing] = true
				}
				s.fields[name] = current
			}
		}
	}

	// Apply the type resolutions
	for name, ftype := range s.typeResolutions {
		if _, found := s.fields[name]; !found {
			continue
		}
		s.fields[name] = ftype
	}

	// Boil down all tags, fields and conflicts for later reuse
	s.tags = make([]string, 0, len(tags))
	for name := range tags {
		s.tags = append(s.tags, name)
	}
	sort.Strings(s.tags)

	s.fieldKeys = make([]string, 0, len(s.fields))
	for name, ftype := range s.fields {
		// Detect unconvertible fields
		switch ftype {
		case influxql.Float, influxql.Integer, influxql.String, influxql.Boolean, influxql.Unsigned:
			// Accepted type
			s.fieldKeys = append(s.fieldKeys, name)
		default:
			// Unconvertible type
			panic(fmt.Errorf("unconvertible field %q with type %v", name, ftype))
		}
	}
	sort.Strings(s.fieldKeys)

	s.conflicts = make(map[string][]influxql.DataType, len(conflicts))
	for name, conflict := range conflicts {
		s.conflicts[name] = make([]influxql.DataType, 0, len(conflict))
		for ftype := range conflict {
			s.conflicts[name] = append(s.conflicts[name], ftype)
		}
	}

	return nil
}

func (s *schemaCreator) validate() (bool, error) {
	var hasConflicts bool
	var typeConflicts []string
	for field := range s.conflicts {
		hasConflicts = true
		if _, resolved := s.typeResolutions[field]; !resolved {
			typeConflicts = append(typeConflicts, field)
		}
	}
	if len(typeConflicts) > 0 {
		return true, fmt.Errorf("unresolved type conflicts for %q", strings.Join(typeConflicts, ","))
	}

	var nameConflicts []string
	for _, field := range s.fieldKeys {
		for _, tag := range s.tags {
			if tag == field {
				hasConflicts = true
				if _, resolved := s.nameResolutions[field]; !resolved {
					nameConflicts = append(nameConflicts, tag)
				}
			}
		}
	}
	if len(nameConflicts) > 0 {
		return true, fmt.Errorf("unresolved name conflicts for %q", strings.Join(nameConflicts, ","))
	}

	return hasConflicts, nil
}

func (s *schemaCreator) schema(info map[string]string) (*arrow.Schema, error) {
	columns := make([]arrow.Field, 0, 1+len(s.tags)+len(s.fields))

	// Add the timestamp column first
	columns = append(columns, arrow.Field{
		Name:     "time",
		Type:     &arrow.TimestampType{Unit: arrow.Nanosecond},
		Metadata: arrow.MetadataFrom(map[string]string{"kind": "timestamp"}),
	})

	// Add tags in alphabetical order
	for _, tag := range s.tags {
		columns = append(columns, arrow.Field{
			Name:     tag,
			Type:     &arrow.StringType{},
			Nullable: true,
			Metadata: arrow.MetadataFrom(map[string]string{"kind": "tag"}),
		})
	}

	// Add fields in alphabetical order with the corresponding arrow type
	for _, name := range s.fieldKeys {
		ftype := s.fields[name]
		if t, found := s.typeResolutions[name]; found {
			ftype = t
		}
		fname := name
		if n, found := s.nameResolutions[name]; found {
			fname = n
		}

		var dtype arrow.DataType
		switch ftype {
		case influxql.Float:
			dtype = &arrow.Float64Type{}
		case influxql.Integer:
			dtype = &arrow.Int64Type{}
		case influxql.String:
			dtype = &arrow.StringType{}
		case influxql.Boolean:
			dtype = &arrow.BooleanType{}
		case influxql.Unsigned:
			dtype = &arrow.Uint64Type{}
		default:
			panic(fmt.Errorf("unconvertible field %q", name))
		}
		columns = append(columns, arrow.Field{
			Name:     fname,
			Type:     dtype,
			Nullable: true,
			Metadata: arrow.MetadataFrom(map[string]string{"kind": "field"}),
		})
	}

	// Add the metadata given as argument and add info about column kinds
	if info == nil {
		info = make(map[string]string)
	}
	info["tags"] = strings.Join(s.tags, ",")
	info["fields"] = strings.Join(s.fieldKeys, ",")
	metadata := arrow.MetadataFrom(info)

	return arrow.NewSchema(columns, &metadata), nil
}
