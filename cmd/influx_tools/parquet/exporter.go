package parquet

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/apache/arrow/go/v16/arrow"
	"github.com/apache/arrow/go/v16/arrow/array"
	"github.com/apache/arrow/go/v16/parquet"
	"github.com/apache/arrow/go/v16/parquet/pqarrow"
	"go.uber.org/zap"

	"github.com/influxdata/flux/memory"
	"github.com/influxdata/influxdb/cmd/influx_tools/server"
	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/services/meta"
	"github.com/influxdata/influxdb/tsdb"
	"github.com/influxdata/influxql"
)

type config struct {
	Database        string
	RP              string
	Measurements    string
	TypeResolutions string
	NameResolutions string
	Output          string
}

type exporter struct {
	client server.MetaClient
	store  *tsdb.Store

	// Input selection
	db, rp       string
	measurements []string

	// Output file settings
	path      string
	filenames map[string]int

	// Source data and corresponding information
	groups    []meta.ShardGroupInfo
	startDate time.Time
	endDate   time.Time
	shards    []*tsdb.Shard

	// Schema information and resolutions
	schemata        map[string]*schemaCreator
	typeResolutions map[string]map[string]influxql.DataType
	nameResolutions map[string]map[string]string

	// Parquet metadata information
	exportStart time.Time

	logger *zap.SugaredLogger
}

func newExporter(server server.Interface, cfg *config, logger *zap.Logger) (*exporter, error) {
	client := server.MetaClient()

	db := client.Database(cfg.Database)
	if db == nil {
		return nil, fmt.Errorf("database %q does not exist", cfg.Database)
	}

	if cfg.RP == "" {
		cfg.RP = db.DefaultRetentionPolicy
	}

	rp, err := client.RetentionPolicy(cfg.Database, cfg.RP)
	if rp == nil || err != nil {
		return nil, fmt.Errorf("retention policy %q does not exist", cfg.RP)
	}

	store := tsdb.NewStore(server.TSDBConfig().Dir)
	if server.Logger() != nil {
		store.WithLogger(server.Logger())
	}
	store.EngineOptions.MonitorDisabled = true
	store.EngineOptions.CompactionDisabled = true
	store.EngineOptions.Config = server.TSDBConfig()
	store.EngineOptions.EngineVersion = server.TSDBConfig().Engine
	store.EngineOptions.IndexVersion = server.TSDBConfig().Index
	store.EngineOptions.DatabaseFilter = func(database string) bool {
		return database == cfg.Database
	}
	store.EngineOptions.RetentionPolicyFilter = func(_, rpolicy string) bool {
		return rpolicy == cfg.RP
	}

	// Create the exporter
	e := &exporter{
		client:          client,
		store:           store,
		db:              cfg.Database,
		rp:              cfg.RP,
		path:            cfg.Output,
		typeResolutions: make(map[string]map[string]influxql.DataType),
		nameResolutions: make(map[string]map[string]string),
		filenames:       make(map[string]int),
		logger:          logger.Sugar().Named("exporter"),
	}

	// Split the given measurements
	if cfg.Measurements != "" && cfg.Measurements != "*" {
		e.measurements = strings.Split(cfg.Measurements, ",")
	}

	// Prepare type resolutions
	if cfg.TypeResolutions != "" {
		for _, r := range strings.Split(cfg.TypeResolutions, ",") {
			field, ftype, found := strings.Cut(r, "=")
			if !found {
				return nil, fmt.Errorf("invalid format in type conflict resolution %q", r)
			}
			measurement, field, found := strings.Cut(field, ".")
			if !found {
				return nil, fmt.Errorf("invalid measurement in type conflict resolution %q", r)
			}
			if _, exists := e.typeResolutions[measurement]; !exists {
				e.typeResolutions[measurement] = make(map[string]influxql.DataType)
			}

			switch strings.ToLower(ftype) {
			case "float":
				e.typeResolutions[measurement][field] = influxql.Float
			case "int":
				e.typeResolutions[measurement][field] = influxql.Integer
			case "uint":
				e.typeResolutions[measurement][field] = influxql.Unsigned
			case "bool":
				e.typeResolutions[measurement][field] = influxql.Boolean
			case "string":
				e.typeResolutions[measurement][field] = influxql.String
			default:
				return nil, fmt.Errorf("invalid type in conflict resolution %q", r)
			}
		}
	}

	// Prepare name resolutions
	if cfg.NameResolutions != "" {
		for _, r := range strings.Split(cfg.NameResolutions, ",") {
			field, name, found := strings.Cut(r, "=")
			if !found {
				return nil, fmt.Errorf("invalid format in name conflict resolution %q", r)
			}
			measurement, field, found := strings.Cut(field, ".")
			if !found {
				return nil, fmt.Errorf("invalid measurement in name conflict resolution %q", r)
			}
			if _, exists := e.nameResolutions[measurement]; !exists {
				e.nameResolutions[measurement] = make(map[string]string)
			}
			e.nameResolutions[measurement][field] = name
		}
	}
	return e, nil
}

func (e *exporter) open(ctx context.Context) error {
	if err := e.store.Open(); err != nil {
		return err
	}

	// Determine all shard groups in the database
	min := time.Unix(0, models.MinNanoTime)
	max := time.Unix(0, models.MaxNanoTime)
	groups, err := e.client.NodeShardGroupsByTimeRange(e.db, e.rp, min, max)
	if err != nil {
		return err
	}

	if len(groups) == 0 {
		return nil
	}

	sort.Sort(meta.ShardGroupInfos(groups))
	e.startDate = groups[0].StartTime
	e.endDate = groups[len(groups)-1].EndTime
	e.groups = groups

	// Collect all shards
	for _, grp := range groups {
		ids := make([]uint64, 0, len(grp.Shards))
		for _, s := range grp.Shards {
			ids = append(ids, s.ID)
		}
		e.shards = append(e.shards, e.store.Shards(ids)...)
	}

	// Determine all measurements in all shards
	if len(e.measurements) == 0 {
		measurements := make(map[string]bool)
		for _, shard := range e.shards {
			if err := shard.ForEachMeasurementName(func(name []byte) error {
				measurements[string(name)] = true
				return nil
			}); err != nil {
				return fmt.Errorf("getting measurement names failed: %w", err)
			}
		}
		for m := range measurements {
			e.measurements = append(e.measurements, m)
		}
	}
	sort.Strings(e.measurements)

	// Collect the schemata for all measurments
	e.schemata = make(map[string]*schemaCreator, len(e.measurements))
	for _, m := range e.measurements {
		creator := &schemaCreator{
			measurement:     m,
			shards:          e.shards,
			series:          make(map[uint64][]seriesEntry, len(e.shards)),
			typeResolutions: e.typeResolutions[m],
			nameResolutions: e.nameResolutions[m],
		}
		if err := creator.extractSchema(ctx); err != nil {
			return fmt.Errorf("extracting schema for measurement %q failed: %w", m, err)
		}
		e.schemata[m] = creator
	}

	return nil
}

func (e *exporter) close() error {
	return e.store.Close()
}

func (e *exporter) printPlan(w io.Writer) {
	tw := tabwriter.NewWriter(w, 10, 8, 1, '\t', 0)

	fmt.Fprintf(w, "Exporting source data from %s to %s in %d shard group(s):\n", e.startDate, e.endDate, len(e.groups))
	fmt.Fprintln(tw, "  Group\tStart\tEnd\t#Shards")
	fmt.Fprintln(tw, "  -----\t-----\t---\t-------")
	for _, g := range e.groups {
		fmt.Fprintf(tw, "  %d\t%s\t%s\t%d\n", g.ID, g.StartTime, g.EndTime, len(g.Shards))
	}
	fmt.Fprintln(tw)

	fmt.Fprintf(w, "Creating the following schemata for %d measurement(s):\n", len(e.measurements))
	for _, measurement := range e.measurements {
		creator := e.schemata[measurement]
		hasConflicts, err := creator.validate()
		if err != nil {
			fmt.Fprintf(
				w,
				"!!Measurement %q with conflict(s) in %d tag(s), %d field(s):\n",
				measurement,
				len(creator.tags),
				len(creator.fieldKeys),
			)
		} else if hasConflicts {
			fmt.Fprintf(
				w,
				"* Measurement %q with resolved conflicts in %d tag(s), %d field(s):\n",
				measurement,
				len(creator.tags),
				len(creator.fieldKeys),
			)
		} else {
			fmt.Fprintf(
				w,
				"  Measurement %q with %d tag(s) and  %d field(s):\n",
				measurement,
				len(creator.tags),
				len(creator.fieldKeys),
			)

		}
		fmt.Fprintln(tw, "    Column\tKind\tDatatype")
		fmt.Fprintln(tw, "    ------\t----\t--------")
		fmt.Fprintln(tw, "    time\ttimestamp\ttimestamp (nanosecond)")
		for _, name := range creator.tags {
			fmt.Fprintf(tw, "    %s\ttag\tstring\n", name)
		}
		for _, name := range creator.fieldKeys {
			ftype := creator.fields[name].String()
			if types, found := creator.conflicts[name]; found {
				parts := make([]string, 0, len(types))
				for _, t := range types {
					parts = append(parts, t.String())
				}
				ftype = strings.Join(parts, "|")
				if rftype, found := creator.typeResolutions[name]; found {
					ftype += " -> " + rftype.String()
				}
			}
			fname := name
			if n, found := creator.nameResolutions[name]; found {
				fname += " -> " + n
			}
			fmt.Fprintf(tw, "    %s\tfield\t%s\n", fname, ftype)
		}
		if err != nil {
			fmt.Fprintln(tw, " ", err)
		}
		fmt.Fprintln(tw)
	}

	tw.Flush()
}

func (e *exporter) export(ctx context.Context) error {
	// Check if the schema has unresolved conflicts
	for m, s := range e.schemata {
		if _, err := s.validate(); err != nil {
			return fmt.Errorf("%w in schema of measurement %q", err, m)
		}
	}

	// Create the export directory if it doesn't exist
	if err := os.MkdirAll(e.path, 0700); err != nil {
		return fmt.Errorf("creating directory %q failed: %w", e.path, err)
	}

	// Export shard-wise
	e.exportStart = time.Now()
	e.logger.Info("Starting export...")
	for _, shard := range e.shards {
		start := time.Now()
		e.logger.Infof("Starting export of shard %d...", shard.ID())

		for _, m := range e.measurements {
			if err := e.exportMeasurement(ctx, shard, m); err != nil {
				path := "unknown"
				if f, serr := shard.SeriesFile(); serr != nil {
					e.logger.Errorf("determining series file failed: %v", serr)
				} else {
					path = f.Path()
				}
				return fmt.Errorf("exporting measurement %q in shard %d at %q failed: %w", m, shard.ID(), path, err)
			}
		}
		e.logger.Infof("Finished export of shard %d in %v...", shard.ID(), time.Since(start))
	}
	e.logger.Infof("Finished export in %v...", time.Since(e.exportStart))

	return nil
}

func (e *exporter) exportMeasurement(ctx context.Context, shard *tsdb.Shard, measurement string) error {
	startMeasurement := time.Now()

	// Get the cumulative scheme with all tags and fields for the measurement
	creator, found := e.schemata[measurement]
	if !found {
		return errors.New("no schema creator found")
	}

	if len(creator.fieldKeys) == 0 {
		e.logger.Warnf("  Skipping measurement %q without fields", measurement)
		return nil
	}

	// Create a batch processor
	batcher := &batcher{
		measurement:     []byte(measurement),
		shard:           shard,
		series:          creator.series[shard.ID()],
		typeResolutions: creator.typeResolutions,
		nameResolutions: creator.nameResolutions,
		start:           models.MinNanoTime,
		logger:          e.logger.Named("batcher"),
	}
	if err := batcher.init(); err != nil {
		return fmt.Errorf("creating batcher failed: %w", err)
	}

	// Check if we do have data for the measurement and skip the shard if
	// that's not the case
	rows, err := batcher.next(ctx)
	if err != nil {
		return fmt.Errorf("checking data failed: %w", err)
	}
	if len(rows) == 0 {
		e.logger.Warnf("  Skipping measurement %q without data", measurement)
		return nil
	}
	batcher.reset()

	e.logger.Infof("  Exporting measurement %q...", measurement)

	// Create a parquet schema for writing the data
	metadata := map[string]string{
		"export":      e.exportStart.Format(time.RFC3339),
		"database":    e.db,
		"retention":   e.rp,
		"measurement": measurement,
		"shard":       strconv.FormatUint(shard.ID(), 10),
		"start_time":  e.startDate.Format(time.RFC3339Nano),
		"end_time":    e.endDate.Format(time.RFC3339Nano),
	}
	schema, err := creator.schema(metadata)
	if err != nil {
		return fmt.Errorf("creating arrow schema failed: %w", err)
	}

	// Create parquet file with a increasing sequence number per measurement
	filename := filepath.Join(
		e.path,
		fmt.Sprintf("%s-%05d.parquet", measurement, e.filenames[measurement]),
	)
	e.filenames[measurement]++
	file, err := os.Create(filename)
	if err != nil {
		return fmt.Errorf("creating file %q failed: %w", filename, err)
	}
	defer file.Close()

	writer, err := pqarrow.NewFileWriter(
		schema,
		file,
		parquet.NewWriterProperties(parquet.WithCreatedBy("influx_tools")),
		pqarrow.NewArrowWriterProperties(pqarrow.WithCoerceTimestamps(arrow.Nanosecond)),
	)
	if err != nil {
		return fmt.Errorf("creating parquet writer for file %q failed: %w", filename, err)
	}
	defer writer.Close()

	// Prepare the record builder
	builder := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer builder.Release()

	// Process the data in batches
	var count int
	for {
		last := time.Now()

		// Read the next batch
		rows, err := batcher.next(ctx)
		if err != nil {
			return fmt.Errorf("reading batch failed: %w", err)
		}
		if len(rows) == 0 {
			break
		}
		count += len(rows)

		// Convert the data to an arrow representation
		record := e.convertData(rows, builder, creator.tags, creator.fieldKeys)

		// Write data
		if err := writer.WriteBuffered(record); err != nil {
			return fmt.Errorf("writing parquet file %q failed: %w", filename, err)
		}
		e.logger.Infof("    exported %d rows in %v", len(rows), time.Since(last))
	}
	e.logger.Infof(
		"  exported %d rows of measurement %q to %q in %v...",
		count,
		measurement,
		filename,
		time.Since(startMeasurement),
	)

	return nil
}

func (e *exporter) convertData(rows []row, builder *array.RecordBuilder, tags, fields []string) arrow.Record {
	for _, r := range rows {
		builder.Field(0).(*array.TimestampBuilder).Append(arrow.Timestamp(r.timestamp))
		base := 1
		for i, k := range tags {
			if v, found := r.tags[k]; found {
				builder.Field(base + i).(*array.StringBuilder).Append(v)
			} else {
				builder.Field(base + i).AppendNull()
			}
		}
		base = len(tags) + 1
		for i, k := range fields {
			v, found := r.fields[k]
			if !found {
				builder.Field(base + i).AppendNull()
				continue
			}
			switch b := builder.Field(base + i).(type) {
			case *array.Float64Builder:
				b.Append(v.(float64))
			case *array.Int64Builder:
				b.Append(v.(int64))
			case *array.Uint64Builder:
				b.Append(v.(uint64))
			case *array.StringBuilder:
				b.Append(v.(string))
			case *array.BooleanBuilder:
				b.Append(v.(bool))
			}
		}
	}

	return builder.NewRecord()
}
