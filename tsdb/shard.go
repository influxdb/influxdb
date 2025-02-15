package tsdb

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode"
	"unsafe"

	"github.com/influxdata/influxdb/logger"
	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/pkg/bytesutil"
	"github.com/influxdata/influxdb/pkg/data/gensyncmap"
	errors2 "github.com/influxdata/influxdb/pkg/errors"
	"github.com/influxdata/influxdb/pkg/estimator"
	"github.com/influxdata/influxdb/pkg/file"
	"github.com/influxdata/influxdb/pkg/limiter"
	"github.com/influxdata/influxdb/pkg/slices"
	"github.com/influxdata/influxdb/query"
	internal "github.com/influxdata/influxdb/tsdb/internal"
	"github.com/influxdata/influxql"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

const (
	statWriteReq           = "writeReq"
	statWriteReqOK         = "writeReqOk"
	statWriteReqErr        = "writeReqErr"
	statSeriesCreate       = "seriesCreate"
	statFieldsCreate       = "fieldsCreate"
	statWritePointsErr     = "writePointsErr"
	statWritePointsDropped = "writePointsDropped"
	statWritePointsOK      = "writePointsOk"
	statWriteValuesOK      = "writeValuesOk"
	statWriteBytes         = "writeBytes"
	statDiskBytes          = "diskBytes"

	FieldsChangeFile = "fields.idxl"
	bytesInInt64     = 8
)

var (
	// ErrFieldTypeConflict is returned when a new field already exists with a different type.
	ErrFieldTypeConflict = errors.New("field type conflict")

	// ErrFieldNotFound is returned when a field cannot be found.
	ErrFieldNotFound = errors.New("field not found")

	// ErrFieldUnmappedID is returned when the system is presented, during decode, with a field ID
	// there is no mapping for.
	ErrFieldUnmappedID = errors.New("field ID not mapped")

	// ErrEngineClosed is returned when a caller attempts indirectly to
	// access the shard's underlying engine.
	ErrEngineClosed = errors.New("engine is closed")

	// ErrShardDisabled is returned when a the shard is not available for
	// queries or writes.
	ErrShardDisabled = errors.New("shard is disabled")

	// ErrUnknownFieldsFormat is returned when the fields index file is not identifiable by
	// the file's magic number.
	ErrUnknownFieldsFormat = errors.New("unknown field index format")

	// ErrUnknownFieldType is returned when the type of a field cannot be determined.
	ErrUnknownFieldType = errors.New("unknown field type")

	// ErrShardNotIdle is returned when an operation requring the shard to be idle/cold is
	// attempted on a hot shard.
	ErrShardNotIdle = errors.New("shard not idle")

	// fieldsIndexMagicNumber is the file magic number for the fields index file.
	fieldsIndexMagicNumber = []byte{0, 6, 1, 3}
)

var (
	// Static objects to prevent small allocs.
	timeBytes = []byte("time")
)

// A ShardError implements the error interface, and contains extra
// context about the shard that generated the error.
type ShardError struct {
	id  uint64
	Err error
}

// NewShardError returns a new ShardError.
func NewShardError(id uint64, err error) error {
	if err == nil {
		return nil
	}
	return ShardError{id: id, Err: err}
}

// Error returns the string representation of the error, to satisfy the error interface.
func (e ShardError) Error() string {
	return fmt.Sprintf("[shard %d] %s", e.id, e.Err)
}

// Unwrap returns the underlying error.
func (e ShardError) Unwrap() error {
	return e.Err
}

// PartialWriteError indicates a write request could only write a portion of the
// requested values.
type PartialWriteError struct {
	Reason  string
	Dropped int

	// A sorted slice of series keys that were dropped.
	DroppedKeys     [][]byte
	Database        string
	RetentionPolicy string
}

func (e PartialWriteError) Error() string {
	message := fmt.Sprintf("partial write: %s dropped=%d", e.Reason, e.Dropped)
	if len(e.Database) > 0 {
		message = fmt.Sprintf("%s for database: %s", message, e.Database)
	}
	if len(e.RetentionPolicy) > 0 {
		message = fmt.Sprintf("%s for retention policy: %s", message, e.RetentionPolicy)
	}
	return message
}

// Shard represents a self-contained time series database. An inverted index of
// the measurement and tag data is kept along with the raw time series data.
// Data can be split across many shards. The query engine in TSDB is responsible
// for combining the output of many shards into a single query result.
type Shard struct {
	path    string
	walPath string
	id      uint64

	database        string
	retentionPolicy string

	sfile   *SeriesFile
	options EngineOptions

	mu      sync.RWMutex
	_engine Engine
	index   Index
	enabled bool

	// expvar-based stats.
	stats       *ShardStatistics
	defaultTags models.StatisticTags

	baseLogger *zap.Logger
	logger     *zap.Logger

	EnableOnOpen bool

	// CompactionDisabled specifies the shard should not schedule compactions.
	// This option is intended for offline tooling.
	CompactionDisabled bool
}

// NewShard returns a new initialized Shard. walPath doesn't apply to the b1 type index
func NewShard(id uint64, path string, walPath string, sfile *SeriesFile, opt EngineOptions) *Shard {
	db, rp := decodeStorePath(path)
	logger := zap.NewNop()

	s := &Shard{
		id:      id,
		path:    path,
		walPath: walPath,
		sfile:   sfile,
		options: opt,

		stats: &ShardStatistics{},
		defaultTags: models.StatisticTags{
			"path":            path,
			"walPath":         walPath,
			"id":              fmt.Sprintf("%d", id),
			"database":        db,
			"retentionPolicy": rp,
			"engine":          opt.EngineVersion,
		},

		database:        db,
		retentionPolicy: rp,

		logger:       logger,
		baseLogger:   logger,
		EnableOnOpen: true,
	}
	return s
}

// WithLogger sets the logger on the shard. It must be called before Open.
func (s *Shard) WithLogger(log *zap.Logger) {
	s.baseLogger = log
	engine, err := s.Engine()
	if err == nil {
		engine.WithLogger(s.baseLogger)
		s.index.WithLogger(s.baseLogger)
	}
	s.logger = s.baseLogger.With(zap.String("service", "shard"))
}

// SetEnabled enables the shard for queries and write.  When disabled, all
// writes and queries return an error and compactions are stopped for the shard.
func (s *Shard) SetEnabled(enabled bool) {
	s.mu.Lock()
	s.setEnabledNoLock(enabled)
	s.mu.Unlock()
}

// ! setEnabledNoLock performs actual work of SetEnabled. Must hold s.mu before calling.
func (s *Shard) setEnabledNoLock(enabled bool) {
	// Prevent writes and queries
	s.enabled = enabled
	if s._engine != nil && !s.CompactionDisabled {
		// Disable background compactions and snapshotting
		s._engine.SetEnabled(enabled)
	}
}

// SetNewReadersBlocked sets if new readers can access the shard. If blocked
// is true, the number of reader blocks is incremented and new readers will
// receive an error instead of shard access. If blocked is false, the number
// of reader blocks is decremented. If the reader blocks drops to 0, then
// new readers will be granted access to the shard.
func (s *Shard) SetNewReadersBlocked(blocked bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s._engine.SetNewReadersBlocked(blocked)
}

// InUse returns true if this shard is in-use.
func (s *Shard) InUse() (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s._engine.InUse()
}

// ScheduleFullCompaction forces a full compaction to be schedule on the shard.
func (s *Shard) ScheduleFullCompaction() error {
	engine, err := s.Engine()
	if err != nil {
		return err
	}
	return engine.ScheduleFullCompaction()
}

// ID returns the shards ID.
func (s *Shard) ID() uint64 {
	return s.id
}

// Database returns the database of the shard.
func (s *Shard) Database() string {
	return s.database
}

// RetentionPolicy returns the retention policy of the shard.
func (s *Shard) RetentionPolicy() string {
	return s.retentionPolicy
}

// ShardStatistics maintains statistics for a shard.
type ShardStatistics struct {
	WriteReq           int64
	WriteReqOK         int64
	WriteReqErr        int64
	FieldsCreated      int64
	WritePointsErr     int64
	WritePointsDropped int64
	WritePointsOK      int64
	WriteValuesOK      int64
	BytesWritten       int64
	DiskBytes          int64
}

// Statistics returns statistics for periodic monitoring.
func (s *Shard) Statistics(tags map[string]string) []models.Statistic {
	engine, err := s.Engine()
	if err != nil {
		return nil
	}

	// Refresh our disk size stat
	if _, err := s.DiskSize(); err != nil {
		return nil
	}
	seriesN := engine.SeriesN()

	tags = s.defaultTags.Merge(tags)

	// Set the index type on the tags.  N.B this needs to be checked since it's
	// only set when the shard is opened.
	if indexType := s.IndexType(); indexType != "" {
		tags["indexType"] = indexType
	}

	statistics := []models.Statistic{{
		Name: "shard",
		Tags: tags,
		Values: map[string]interface{}{
			statWriteReq:           atomic.LoadInt64(&s.stats.WriteReq),
			statWriteReqOK:         atomic.LoadInt64(&s.stats.WriteReqOK),
			statWriteReqErr:        atomic.LoadInt64(&s.stats.WriteReqErr),
			statSeriesCreate:       seriesN,
			statFieldsCreate:       atomic.LoadInt64(&s.stats.FieldsCreated),
			statWritePointsErr:     atomic.LoadInt64(&s.stats.WritePointsErr),
			statWritePointsDropped: atomic.LoadInt64(&s.stats.WritePointsDropped),
			statWritePointsOK:      atomic.LoadInt64(&s.stats.WritePointsOK),
			statWriteValuesOK:      atomic.LoadInt64(&s.stats.WriteValuesOK),
			statWriteBytes:         atomic.LoadInt64(&s.stats.BytesWritten),
			statDiskBytes:          atomic.LoadInt64(&s.stats.DiskBytes),
		},
	}}

	// Add the index and engine statistics.
	statistics = append(statistics, engine.Statistics(tags)...)
	return statistics
}

// Path returns the path set on the shard when it was created.
func (s *Shard) Path() string { return s.path }

// Open initializes and opens the shard's store.
func (s *Shard) Open() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.openNoLock()
}

// openNoLock does work of Open. Must hold s.mu before calling.
func (s *Shard) openNoLock() error {
	if err := func() error {
		// Return if the shard is already open
		if s._engine != nil {
			return nil
		}

		seriesIDSet := NewSeriesIDSet()

		// Initialize underlying index.
		ipath := filepath.Join(s.path, "index")
		idx, err := NewIndex(s.id, s.database, ipath, seriesIDSet, s.sfile, s.options)
		if err != nil {
			return err
		}

		idx.WithLogger(s.baseLogger)

		// Open index.
		if err := idx.Open(); err != nil {
			return err
		}
		s.index = idx

		// Initialize underlying engine.
		e, err := NewEngine(s.id, idx, s.path, s.walPath, s.sfile, s.options)
		if err != nil {
			return err
		}

		// Set log output on the engine.
		e.WithLogger(s.baseLogger)

		// Disable compactions while loading the index
		e.SetEnabled(false)

		// Open engine.
		if err := e.Open(); err != nil {
			return err
		}

		// Load metadata index for the inmem index only.
		// If the shard already has data, this is what loads the series ids into the series file, even
		// if we are on TSI indexing.
		if err := e.LoadMetadataIndex(s.id, s.index); err != nil {
			return err
		}
		s._engine = e

		return nil
	}(); err != nil {
		s.closeNoLock()
		return NewShardError(s.id, err)
	}

	if s.EnableOnOpen {
		// enable writes, queries and compactions
		s.setEnabledNoLock(true)
	}

	return nil
}

// Close shuts down the shard's store.
func (s *Shard) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closeNoLock()
}

// closeNoLock closes the shard an removes reference to the shard from associated
// indexes, unless clean is false.
func (s *Shard) closeNoLock() error {
	if s._engine == nil {
		return nil
	}

	err := s._engine.Close()
	if err == nil {
		s._engine = nil
	}

	if e := s.index.Close(); e == nil {
		s.index = nil
	}
	return err
}

// IndexType returns the index version being used for this shard.
//
// IndexType returns the empty string if it is called before the shard is opened,
// since it is only that point that the underlying index type is known.
func (s *Shard) IndexType() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s._engine == nil || s.index == nil { // Shard not open yet.
		return ""
	}
	return s.index.Type()
}

// ready determines if the Shard is ready for queries or writes.
// It returns nil if ready, otherwise ErrShardClosed or ErrShardDisabled
func (s *Shard) ready() error {
	var err error
	if s._engine == nil {
		err = ErrEngineClosed
	} else if !s.enabled {
		err = ErrShardDisabled
	}
	return err
}

// LastModified returns the time when this shard was last modified.
func (s *Shard) LastModified() time.Time {
	engine, err := s.Engine()
	if err != nil {
		return time.Time{}
	}
	return engine.LastModified()
}

// Index returns a reference to the underlying index. It returns an error if
// the index is nil.
func (s *Shard) Index() (Index, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if err := s.ready(); err != nil {
		return nil, err
	}
	return s.index, nil
}

// SeriesFile returns a reference the underlying series file. If return an error
// if the series file is nil.
func (s *Shard) SeriesFile() (*SeriesFile, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if err := s.ready(); err != nil {
		return nil, err
	}
	return s.sfile, nil
}

// IsIdle return true if the shard is not receiving writes and is fully compacted.
func (s *Shard) IsIdle() (state bool, reason string) {
	engine, err := s.Engine()
	if err != nil {
		return true, ""
	}
	return engine.IsIdle()
}

func (s *Shard) Free() error {
	engine, err := s.Engine()
	if err != nil {
		return err
	}

	// Disable compactions to stop background goroutines
	s.SetCompactionsEnabled(false)

	return engine.Free()
}

// SetCompactionsEnabled enables or disable shard background compactions.
func (s *Shard) SetCompactionsEnabled(enabled bool) {
	engine, err := s.Engine()
	if err != nil {
		return
	}
	engine.SetCompactionsEnabled(enabled)
}

// DiskSize returns the size on disk of this shard.
func (s *Shard) DiskSize() (int64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	// We don't use engine() becuase we still want to report the shard's disk
	// size even if the shard has been disabled.
	if s._engine == nil {
		return 0, ErrEngineClosed
	}
	size := s._engine.DiskSize()
	atomic.StoreInt64(&s.stats.DiskBytes, size)
	return size, nil
}

// FieldCreate holds information for a field to create on a measurement.
type FieldCreate struct {
	Measurement []byte
	Field       *Field
}

type StatsTracker struct {
	AddedPoints            func(points, values int64)
	AddedMeasurementPoints func(measurement []byte, points, values int64)
	AddedSeries            func(newSeries int64)
	AddedMeasurementSeries func(measurement []byte, newSeries int64)
}

func NoopStatsTracker() StatsTracker {
	return StatsTracker{}
}

// WritePoints() will write the raw data points and any new metadata
// to the index in the shard.
func (s *Shard) WritePoints(points []models.Point, tracker StatsTracker) error {
	s.mu.RLock()
	defer s.mu.RUnlock()

	engine, err := s.engineNoLock()
	if err != nil {
		return err
	}

	var writeError error
	atomic.AddInt64(&s.stats.WriteReq, 1)

	points, fieldsToCreate, err := s.validateSeriesAndFields(points, tracker)
	if err != nil {
		if _, ok := err.(PartialWriteError); !ok {
			return err
		}
		// There was a partial write (points dropped), hold onto the error to return
		// to the caller, but continue on writing the remaining points.
		writeError = err
	}

	// add any new fields and keep track of what needs to be saved
	if numFieldsCreated, err := s.createFieldsAndMeasurements(fieldsToCreate); err != nil {
		return err
	} else {
		atomic.AddInt64(&s.stats.FieldsCreated, int64(numFieldsCreated))
	}
	engineTracker := tracker
	engineTracker.AddedPoints = func(points, values int64) {
		if tracker.AddedPoints != nil {
			// notify outer tracker (e.g. http service)
			tracker.AddedPoints(points, values)
		}
		atomic.AddInt64(&s.stats.WritePointsOK, points)
		atomic.AddInt64(&s.stats.WriteValuesOK, values)
	}
	// Write to the engine.
	if err := engine.WritePoints(points, engineTracker); err != nil {
		atomic.AddInt64(&s.stats.WritePointsErr, int64(len(points)))
		atomic.AddInt64(&s.stats.WriteReqErr, 1)
		return fmt.Errorf("engine: %w", err)
	}

	// increment the number OK write requests
	atomic.AddInt64(&s.stats.WriteReqOK, 1)

	return writeError
}

// validateSeriesAndFields checks which series and fields are new and whose metadata should be saved and indexed.
func (s *Shard) validateSeriesAndFields(points []models.Point, tracker StatsTracker) ([]models.Point, []*FieldCreate, error) {
	var (
		fieldsToCreate []*FieldCreate
		err            error
		dropped        int
		reason         string // only first error reason is set unless returned from CreateSeriesListIfNotExists
	)

	// Create all series against the index in bulk.
	keys := make([][]byte, len(points))
	names := make([][]byte, len(points))
	tagsSlice := make([]models.Tags, len(points))

	// Check if keys should be unicode validated.
	validateKeys := s.options.Config.ValidateKeys

	var j int
	for i, p := range points {
		tags := p.Tags()

		// Drop any series w/ a "time" tag, these are illegal
		if v := tags.Get(timeBytes); v != nil {
			dropped++
			if reason == "" {
				reason = fmt.Sprintf(
					"invalid tag key: input tag \"%s\" on measurement \"%s\" is invalid",
					"time", string(p.Name()))
			}
			continue
		}

		// Drop any series with invalid unicode characters in the key.
		if validateKeys && !models.ValidKeyTokens(string(p.Name()), tags) {
			dropped++
			if reason == "" {
				reason = fmt.Sprintf("key contains invalid unicode: %q", makePrintable(string(p.Key())))
			}
			continue
		}

		keys[j] = p.Key()
		names[j] = p.Name()
		tagsSlice[j] = tags
		points[j] = points[i]
		j++
	}
	points, keys, names, tagsSlice = points[:j], keys[:j], names[:j], tagsSlice[:j]

	engine, err := s.engineNoLock()
	if err != nil {
		return nil, nil, err
	}

	// Add new series. Check for partial writes.
	var droppedKeys [][]byte
	if err := engine.CreateSeriesListIfNotExists(keys, names, tagsSlice, tracker); err != nil {
		switch err := err.(type) {
		// (DSB) This was previously *PartialWriteError. Now catch pointer and value types.
		case PartialWriteError:
			reason = err.Reason
			dropped += err.Dropped
			droppedKeys = err.DroppedKeys
			atomic.AddInt64(&s.stats.WritePointsDropped, int64(err.Dropped))
		case *PartialWriteError:
			reason = err.Reason
			dropped += err.Dropped
			droppedKeys = err.DroppedKeys
			atomic.AddInt64(&s.stats.WritePointsDropped, int64(err.Dropped))
		default:
			return nil, nil, err
		}
	}

	j = 0
	for i, p := range points {
		// Skip any points with only invalid fields.
		iter := p.FieldIterator()
		validField := false
		for iter.Next() {
			if bytes.Equal(iter.FieldKey(), timeBytes) {
				continue
			}
			validField = true
			break
		}
		if !validField {
			if reason == "" {
				reason = fmt.Sprintf(
					"invalid field name: input field \"%s\" on measurement \"%s\" is invalid",
					"time", string(p.Name()))
			}
			dropped++
			continue
		}

		// Skip any points whose keys have been dropped. Dropped has already been incremented for them.
		if len(droppedKeys) > 0 && bytesutil.Contains(droppedKeys, keys[i]) {
			continue
		}

		var newFields []*FieldCreate
		var validateErr error
		name := p.Name()
		mf := engine.MeasurementFields(name)
		// Check with the field validator.
		if newFields, validateErr = ValidateFields(mf, p, s.options.Config.SkipFieldSizeValidation); validateErr != nil {
			var err PartialWriteError
			switch {
			case errors.As(validateErr, &err):
				// This will turn into an error later, outside this lambda
				if reason == "" {
					reason = err.Reason
				}
				dropped += err.Dropped
				atomic.AddInt64(&s.stats.WritePointsDropped, int64(err.Dropped))
				continue
			default:
				// Return validateErr, because err will be nil here
				return nil, nil, validateErr
			}
		}

		points[j] = points[i]
		j++
		fieldsToCreate = append(fieldsToCreate, newFields...)
	}
	if dropped > 0 {
		err = PartialWriteError{Reason: reason, Dropped: dropped, Database: s.database, RetentionPolicy: s.retentionPolicy}
	}

	return points[:j], fieldsToCreate, err
}

const unPrintReplRune = '?'
const unPrintMaxReplRune = 3

// makePrintable - replace invalid and non-printable unicode characters with a few '?' runes
func makePrintable(s string) string {
	b := strings.Builder{}
	b.Grow(len(s))
	c := 0
	for _, r := range strings.ToValidUTF8(s, string(unicode.ReplacementChar)) {
		if !unicode.IsPrint(r) || r == unicode.ReplacementChar {
			if c < unPrintMaxReplRune {
				b.WriteRune(unPrintReplRune)
			}
			c++
		} else {
			b.WriteRune(r)
			c = 0
		}
	}
	return b.String()
}

func (s *Shard) createFieldsAndMeasurements(fieldsToCreate []*FieldCreate) (int, error) {
	if len(fieldsToCreate) == 0 {
		return 0, nil
	}

	engine, err := s.engineNoLock()
	if err != nil {
		return 0, err
	}
	numCreated := 0
	// add fields
	changes := make([]*FieldChange, 0, len(fieldsToCreate))
	for _, f := range fieldsToCreate {
		mf := engine.MeasurementFields(f.Measurement)
		if created, err := mf.CreateFieldIfNotExists(f.Field.Name, f.Field.Type); err != nil {
			return 0, err
		} else if created {
			numCreated++
			s.index.SetFieldName(f.Measurement, f.Field.Name)
			changes = append(changes, &FieldChange{
				FieldCreate: *f,
				ChangeType:  AddMeasurementField,
			})
		}
	}

	return numCreated, engine.MeasurementFieldSet().Save(changes)
}

// DeleteSeriesRange deletes all values from for seriesKeys between min and max (inclusive)
func (s *Shard) DeleteSeriesRange(itr SeriesIterator, min, max int64) error {
	engine, err := s.Engine()
	if err != nil {
		return err
	}
	return engine.DeleteSeriesRange(itr, min, max)
}

// DeleteSeriesRangeWithPredicate deletes all values from for seriesKeys between min and max (inclusive)
// for which predicate() returns true. If predicate() is nil, then all values in range are deleted.
func (s *Shard) DeleteSeriesRangeWithPredicate(itr SeriesIterator, predicate func(name []byte, tags models.Tags) (int64, int64, bool)) error {
	engine, err := s.Engine()
	if err != nil {
		return err
	}
	return engine.DeleteSeriesRangeWithPredicate(itr, predicate)
}

// DeleteMeasurement deletes a measurement and all underlying series.
func (s *Shard) DeleteMeasurement(name []byte) error {
	engine, err := s.Engine()
	if err != nil {
		return err
	}
	return engine.DeleteMeasurement(name)
}

// SeriesN returns the unique number of series in the shard.
func (s *Shard) SeriesN() int64 {
	engine, err := s.Engine()
	if err != nil {
		return 0
	}
	return engine.SeriesN()
}

// SeriesSketches returns the measurement sketches for the shard.
func (s *Shard) SeriesSketches() (estimator.Sketch, estimator.Sketch, error) {
	engine, err := s.Engine()
	if err != nil {
		return nil, nil, err
	}
	return engine.SeriesSketches()
}

// MeasurementsSketches returns the measurement sketches for the shard.
func (s *Shard) MeasurementsSketches() (estimator.Sketch, estimator.Sketch, error) {
	engine, err := s.Engine()
	if err != nil {
		return nil, nil, err
	}
	return engine.MeasurementsSketches()
}

// MeasurementNamesByRegex returns names of measurements matching the regular expression.
func (s *Shard) MeasurementNamesByRegex(re *regexp.Regexp) ([][]byte, error) {
	engine, err := s.Engine()
	if err != nil {
		return nil, err
	}
	return engine.MeasurementNamesByRegex(re)
}

// MeasurementNamesByPredicate returns fields for a measurement filtered by an expression.
func (s *Shard) MeasurementNamesByPredicate(expr influxql.Expr) ([][]byte, error) {
	index, err := s.Index()
	if err != nil {
		return nil, err
	}
	indexSet := IndexSet{Indexes: []Index{index}, SeriesFile: s.sfile}
	return indexSet.MeasurementNamesByPredicate(query.OpenAuthorizer, expr)
}

// MeasurementFields returns fields for a measurement.
func (s *Shard) MeasurementFields(name []byte) *MeasurementFields {
	engine, err := s.Engine()
	if err != nil {
		return nil
	}
	return engine.MeasurementFields(name)
}

// MeasurementExists returns true if the shard contains name.
// TODO(edd): This method is currently only being called from tests; do we
// really need it?
func (s *Shard) MeasurementExists(name []byte) (bool, error) {
	engine, err := s.Engine()
	if err != nil {
		return false, err
	}
	return engine.MeasurementExists(name)
}

// WriteTo writes the shard's data to w.
func (s *Shard) WriteTo(w io.Writer) (int64, error) {
	engine, err := s.Engine()
	if err != nil {
		return 0, err
	}
	n, err := engine.WriteTo(w)
	atomic.AddInt64(&s.stats.BytesWritten, int64(n))
	return n, err
}

// CreateIterator returns an iterator for the data in the shard.
func (s *Shard) CreateIterator(ctx context.Context, m *influxql.Measurement, opt query.IteratorOptions) (query.Iterator, error) {
	engine, err := s.Engine()
	if err != nil {
		return nil, err
	}
	switch m.SystemIterator {
	case "_fieldKeys":
		return NewFieldKeysIterator(s, opt)
	case "_series":
		// TODO(benbjohnson): Move up to the Shards.CreateIterator().
		index, err := s.Index()
		if err != nil {
			return nil, err
		}
		indexSet := IndexSet{Indexes: []Index{index}, SeriesFile: s.sfile}

		itr, err := NewSeriesPointIterator(indexSet, opt)
		if err != nil {
			return nil, err
		}

		return query.NewInterruptIterator(itr, opt.InterruptCh), nil
	case "_tagKeys":
		return NewTagKeysIterator(s, opt)
	}
	return engine.CreateIterator(ctx, m.Name, opt)
}

func (s *Shard) CreateSeriesCursor(ctx context.Context, req SeriesCursorRequest, cond influxql.Expr) (SeriesCursor, error) {
	index, err := s.Index()
	if err != nil {
		return nil, err
	}
	return newSeriesCursor(req, IndexSet{Indexes: []Index{index}, SeriesFile: s.sfile}, cond)
}

func (s *Shard) CreateCursorIterator(ctx context.Context) (CursorIterator, error) {
	engine, err := s.Engine()
	if err != nil {
		return nil, err
	}
	return engine.CreateCursorIterator(ctx)
}

// FieldDimensions returns unique sets of fields and dimensions across a list of sources.
func (s *Shard) FieldDimensions(measurements []string) (fields map[string]influxql.DataType, dimensions map[string]struct{}, err error) {
	engine, err := s.Engine()
	if err != nil {
		return nil, nil, err
	}

	fields = make(map[string]influxql.DataType)
	dimensions = make(map[string]struct{})

	index, err := s.Index()
	if err != nil {
		return nil, nil, err
	}
	for _, name := range measurements {
		// Handle system sources.
		if strings.HasPrefix(name, "_") {
			var keys []string
			switch name {
			case "_fieldKeys":
				keys = []string{"fieldKey", "fieldType"}
			case "_series":
				keys = []string{"key"}
			case "_tagKeys":
				keys = []string{"tagKey"}
			}

			if len(keys) > 0 {
				for _, k := range keys {
					if fields[k].LessThan(influxql.String) {
						fields[k] = influxql.String
					}
				}
				continue
			}
			// Unknown system source so default to looking for a measurement.
		}

		// Retrieve measurement.
		if exists, err := engine.MeasurementExists([]byte(name)); err != nil {
			return nil, nil, err
		} else if !exists {
			continue
		}

		// Append fields and dimensions.
		mf := engine.MeasurementFields([]byte(name))
		if mf != nil {
			for k, typ := range mf.FieldSet() {
				if fields[k].LessThan(typ) {
					fields[k] = typ
				}
			}
		}

		indexSet := IndexSet{Indexes: []Index{index}, SeriesFile: s.sfile}
		if err := indexSet.ForEachMeasurementTagKey([]byte(name), func(key []byte) error {
			dimensions[string(key)] = struct{}{}
			return nil
		}); err != nil {
			return nil, nil, err
		}
	}

	return fields, dimensions, nil
}

// mapType returns the data type for the field within the measurement.
func (s *Shard) mapType(measurement, field string) (influxql.DataType, error) {
	engine, err := s.engineNoLock()
	if err != nil {
		return 0, err
	}

	switch field {
	case "_name", "_tagKey", "_tagValue", "_seriesKey":
		return influxql.String, nil
	}

	// Process system measurements.
	switch measurement {
	case "_fieldKeys":
		if field == "fieldKey" || field == "fieldType" {
			return influxql.String, nil
		}
		return influxql.Unknown, nil
	case "_series":
		if field == "key" {
			return influxql.String, nil
		}
		return influxql.Unknown, nil
	case "_tagKeys":
		if field == "tagKey" {
			return influxql.String, nil
		}
		return influxql.Unknown, nil
	}
	// Unknown system source so default to looking for a measurement.

	if exists, _ := engine.MeasurementExists([]byte(measurement)); !exists {
		return influxql.Unknown, nil
	}

	mf := engine.MeasurementFields([]byte(measurement))
	if mf != nil {
		f := mf.Field(field)
		if f != nil {
			return f.Type, nil
		}
	}

	if exists, _ := engine.HasTagKey([]byte(measurement), []byte(field)); exists {
		return influxql.Tag, nil
	}

	return influxql.Unknown, nil
}

// expandSources expands regex sources and removes duplicates.
// NOTE: sources must be normalized (db and rp set) before calling this function.
func (s *Shard) expandSources(sources influxql.Sources) (influxql.Sources, error) {
	engine, err := s.engineNoLock()
	if err != nil {
		return nil, err
	}

	// Use a map as a set to prevent duplicates.
	set := map[string]influxql.Source{}

	// Iterate all sources, expanding regexes when they're found.
	for _, source := range sources {
		switch src := source.(type) {
		case *influxql.Measurement:
			// Add non-regex measurements directly to the set.
			if src.Regex == nil {
				set[src.String()] = src
				continue
			}

			// Loop over matching measurements.
			names, err := engine.MeasurementNamesByRegex(src.Regex.Val)
			if err != nil {
				return nil, err
			}

			for _, name := range names {
				other := &influxql.Measurement{
					Database:        src.Database,
					RetentionPolicy: src.RetentionPolicy,
					Name:            string(name),
				}
				set[other.String()] = other
			}

		default:
			return nil, fmt.Errorf("expandSources: unsupported source type: %T", source)
		}
	}

	// Convert set to sorted slice.
	names := make([]string, 0, len(set))
	for name := range set {
		names = append(names, name)
	}
	sort.Strings(names)

	// Convert set to a list of Sources.
	expanded := make(influxql.Sources, 0, len(set))
	for _, name := range names {
		expanded = append(expanded, set[name])
	}

	return expanded, nil
}

// Backup backs up the shard by creating a tar archive of all TSM files that
// have been modified since the provided time. See Engine.Backup for more details.
func (s *Shard) Backup(w io.Writer, basePath string, since time.Time) error {
	engine, err := s.Engine()
	if err != nil {
		return err
	}
	return engine.Backup(w, basePath, since)
}

func (s *Shard) Export(w io.Writer, basePath string, start time.Time, end time.Time) error {
	engine, err := s.Engine()
	if err != nil {
		return err
	}
	return engine.Export(w, basePath, start, end)
}

// Restore restores data to the underlying engine for the shard.
// The shard is reopened after restore.
func (s *Shard) Restore(r io.Reader, basePath string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Special case - we can still restore to a disabled shard, so we should
	// only check if the engine is closed and not care if the shard is
	// disabled.
	if s._engine == nil {
		return ErrEngineClosed
	}

	// Restore to engine.
	if err := s._engine.Restore(r, basePath); err != nil {
		return err
	}

	// Close shard.
	if err := s.closeNoLock(); err != nil {
		return err
	}

	// Reopen engine.
	return s.openNoLock()
}

// Import imports data to the underlying engine for the shard. r should
// be a reader from a backup created by Backup.
func (s *Shard) Import(r io.Reader, basePath string) error {
	// Special case - we can still import to a disabled shard, so we should
	// only check if the engine is closed and not care if the shard is
	// disabled.
	s.mu.Lock()
	defer s.mu.Unlock()
	if s._engine == nil {
		return ErrEngineClosed
	}

	// Import to engine.
	return s._engine.Import(r, basePath)
}

// CreateSnapshot will return a path to a temp directory
// containing hard links to the underlying shard files.
func (s *Shard) CreateSnapshot(skipCacheOk bool) (string, error) {
	engine, err := s.Engine()
	if err != nil {
		return "", err
	}
	return engine.CreateSnapshot(skipCacheOk)
}

// ForEachMeasurementName iterates over each measurement in the shard.
func (s *Shard) ForEachMeasurementName(fn func(name []byte) error) error {
	engine, err := s.Engine()
	if err != nil {
		return err
	}
	return engine.ForEachMeasurementName(fn)
}

func (s *Shard) TagKeyCardinality(name, key []byte) int {
	engine, err := s.Engine()
	if err != nil {
		return 0
	}
	return engine.TagKeyCardinality(name, key)
}

// Digest returns a digest of the shard.
func (s *Shard) Digest() (io.ReadCloser, int64, error, string) {
	engine, err := s.Engine()
	if err != nil {
		return nil, 0, err, ""
	}

	// Make sure the shard is idle/cold. (No use creating a digest of a
	// hot shard that is rapidly changing.)
	if isIdle, reason := engine.IsIdle(); !isIdle {
		return nil, 0, ErrShardNotIdle, reason
	}

	readCloser, size, err := engine.Digest()
	return readCloser, size, err, ""
}

// engine safely (under an RLock) returns a reference to the shard's Engine, or
// an error if the Engine is closed, or the shard is currently disabled.
//
// The shard's Engine should always be accessed via a call to engine(), rather
// than directly referencing Shard.engine.
//
// If a caller needs an Engine reference but is already under a lock, then they
// should use engineNoLock().
func (s *Shard) Engine() (Engine, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.engineNoLock()
}

// engineNoLock is similar to calling engine(), but the caller must guarantee
// that they already hold an appropriate lock.
func (s *Shard) engineNoLock() (Engine, error) {
	if err := s.ready(); err != nil {
		return nil, err
	}
	return s._engine, nil
}

type ShardGroup interface {
	MeasurementsByRegex(re *regexp.Regexp) []string
	FieldKeysByMeasurement(name []byte) []string
	FieldDimensions(measurements []string) (fields map[string]influxql.DataType, dimensions map[string]struct{}, err error)
	MapType(measurement, field string) influxql.DataType
	CreateIterator(ctx context.Context, measurement *influxql.Measurement, opt query.IteratorOptions) (query.Iterator, error)
	IteratorCost(measurement string, opt query.IteratorOptions) (query.IteratorCost, error)
	ExpandSources(sources influxql.Sources) (influxql.Sources, error)
}

// Shards represents a sortable list of shards.
type Shards []*Shard

// Len implements sort.Interface.
func (a Shards) Len() int { return len(a) }

// Less implements sort.Interface.
func (a Shards) Less(i, j int) bool { return a[i].id < a[j].id }

// Swap implements sort.Interface.
func (a Shards) Swap(i, j int) { a[i], a[j] = a[j], a[i] }

// MeasurementsByRegex returns the unique set of measurements matching the
// provided regex, for all the shards.
func (a Shards) MeasurementsByRegex(re *regexp.Regexp) []string {
	var m map[string]struct{}
	for _, sh := range a {
		names, err := sh.MeasurementNamesByRegex(re)
		if err != nil {
			continue // Skip this shard's results—previous behaviour.
		}

		if m == nil {
			m = make(map[string]struct{}, len(names))
		}

		for _, name := range names {
			m[string(name)] = struct{}{}
		}
	}

	if len(m) == 0 {
		return nil
	}

	names := make([]string, 0, len(m))
	for key := range m {
		names = append(names, key)
	}
	sort.Strings(names)
	return names
}

// FieldKeysByMeasurement returns a de-duplicated, sorted, set of field keys for
// the provided measurement name.
func (a Shards) FieldKeysByMeasurement(name []byte) []string {
	if len(a) == 1 {
		mf := a[0].MeasurementFields(name)
		if mf == nil {
			return nil
		}
		return mf.FieldKeys()
	}

	all := make([][]string, 0, len(a))
	for _, shard := range a {
		mf := shard.MeasurementFields(name)
		if mf == nil {
			continue
		}
		all = append(all, mf.FieldKeys())
	}
	return slices.MergeSortedStrings(all...)
}

// MeasurementNamesByPredicate returns the measurements that match the given predicate.
func (a Shards) MeasurementNamesByPredicate(expr influxql.Expr) ([][]byte, error) {
	if len(a) == 1 {
		return a[0].MeasurementNamesByPredicate(expr)
	}

	all := make([][][]byte, len(a))
	for i, shard := range a {
		names, err := shard.MeasurementNamesByPredicate(expr)
		if err != nil {
			return nil, err
		}
		all[i] = names
	}
	return slices.MergeSortedBytes(all...), nil
}

// FieldKeysByPredicate returns the field keys for series that match
// the given predicate.
func (a Shards) FieldKeysByPredicate(expr influxql.Expr) (map[string][]string, error) {
	names, err := a.MeasurementNamesByPredicate(expr)
	if err != nil {
		return nil, err
	}

	all := make(map[string][]string, len(names))
	for _, name := range names {
		all[string(name)] = a.FieldKeysByMeasurement(name)
	}
	return all, nil
}

func (a Shards) FieldDimensions(measurements []string) (fields map[string]influxql.DataType, dimensions map[string]struct{}, err error) {
	fields = make(map[string]influxql.DataType)
	dimensions = make(map[string]struct{})

	for _, sh := range a {
		f, d, err := sh.FieldDimensions(measurements)
		if err != nil {
			return nil, nil, err
		}
		for k, typ := range f {
			if fields[k].LessThan(typ) {
				fields[k] = typ
			}
		}
		for k := range d {
			dimensions[k] = struct{}{}
		}
	}
	return
}

func (a Shards) MapType(measurement, field string) influxql.DataType {
	var typ influxql.DataType
	for _, sh := range a {
		sh.mu.RLock()
		if t, err := sh.mapType(measurement, field); err == nil && typ.LessThan(t) {
			typ = t
		}
		sh.mu.RUnlock()
	}
	return typ
}

func (a Shards) CallType(name string, args []influxql.DataType) (influxql.DataType, error) {
	typmap := query.CallTypeMapper{}
	return typmap.CallType(name, args)
}

func (a Shards) CreateIterator(ctx context.Context, measurement *influxql.Measurement, opt query.IteratorOptions) (query.Iterator, error) {
	switch measurement.SystemIterator {
	case "_series":
		return a.createSeriesIterator(ctx, opt)
	}

	itrs := make([]query.Iterator, 0, len(a))
	for _, sh := range a {
		itr, err := sh.CreateIterator(ctx, measurement, opt)
		if err != nil {
			query.Iterators(itrs).Close()
			return nil, err
		} else if itr == nil {
			continue
		}
		itrs = append(itrs, itr)

		select {
		case <-opt.InterruptCh:
			query.Iterators(itrs).Close()
			return nil, query.ErrQueryInterrupted
		default:
		}

		// Enforce series limit at creation time.
		if opt.MaxSeriesN > 0 {
			stats := itr.Stats()
			if stats.SeriesN > opt.MaxSeriesN {
				query.Iterators(itrs).Close()
				return nil, fmt.Errorf("max-select-series limit exceeded: (%d/%d)", stats.SeriesN, opt.MaxSeriesN)
			}
		}
	}
	return query.Iterators(itrs).Merge(opt)
}

func (a Shards) createSeriesIterator(ctx context.Context, opt query.IteratorOptions) (_ query.Iterator, err error) {
	var (
		idxs  = make([]Index, 0, len(a))
		sfile *SeriesFile
	)
	for _, sh := range a {
		var idx Index
		if idx, err = sh.Index(); err == nil {
			idxs = append(idxs, idx)
		}
		if sfile == nil {
			sfile, _ = sh.SeriesFile()
		}
	}

	if sfile == nil {
		return nil, nil
	}

	return NewSeriesPointIterator(IndexSet{Indexes: idxs, SeriesFile: sfile}, opt)
}

func (a Shards) IteratorCost(measurement string, opt query.IteratorOptions) (query.IteratorCost, error) {
	var costs query.IteratorCost
	var costerr error
	var mu sync.RWMutex

	setErr := func(err error) {
		mu.Lock()
		defer mu.Unlock()
		if costerr == nil {
			costerr = err
		}
	}

	limit := limiter.NewFixed(runtime.GOMAXPROCS(0))
	var wg sync.WaitGroup
	for _, sh := range a {
		limit.Take()
		wg.Add(1)

		mu.RLock()
		if costerr != nil {
			mu.RUnlock()
			break
		}
		mu.RUnlock()

		go func(sh *Shard) {
			defer limit.Release()
			defer wg.Done()

			engine, err := sh.Engine()
			if err != nil {
				setErr(err)
				return
			}

			cost, err := engine.IteratorCost(measurement, opt)
			if err != nil {
				setErr(err)
				return
			}

			mu.Lock()
			costs = costs.Combine(cost)
			mu.Unlock()
		}(sh)
	}
	wg.Wait()
	return costs, costerr
}

func (a Shards) CreateSeriesCursor(ctx context.Context, req SeriesCursorRequest, cond influxql.Expr) (_ SeriesCursor, err error) {
	var (
		idxs  []Index
		sfile *SeriesFile
	)
	for _, sh := range a {
		var idx Index
		if idx, err = sh.Index(); err == nil {
			idxs = append(idxs, idx)
		}
		if sfile == nil {
			sfile, _ = sh.SeriesFile()
		}
	}

	if sfile == nil {
		return nil, errors.New("CreateSeriesCursor: no series file")
	}

	return newSeriesCursor(req, IndexSet{Indexes: idxs, SeriesFile: sfile}, cond)
}

func (a Shards) ExpandSources(sources influxql.Sources) (influxql.Sources, error) {
	// Use a map as a set to prevent duplicates.
	set := map[string]influxql.Source{}

	// Iterate through every shard and expand the sources.
	for _, sh := range a {
		sh.mu.RLock()
		expanded, err := sh.expandSources(sources)
		sh.mu.RUnlock()
		if err != nil {
			return nil, err
		}

		for _, src := range expanded {
			switch src := src.(type) {
			case *influxql.Measurement:
				set[src.String()] = src
			default:
				return nil, fmt.Errorf("Store.ExpandSources: unsupported source type: %T", src)
			}
		}
	}

	// Convert set to sorted slice.
	names := make([]string, 0, len(set))
	for name := range set {
		names = append(names, name)
	}
	sort.Strings(names)

	// Convert set to a list of Sources.
	sorted := make([]influxql.Source, 0, len(set))
	for _, name := range names {
		sorted = append(sorted, set[name])
	}
	return sorted, nil
}

// MeasurementFields holds the fields of a measurement and their codec.
type MeasurementFields struct {
	fields gensyncmap.Map[string, *Field]
}

// NewMeasurementFields returns an initialised *MeasurementFields value.
func NewMeasurementFields() *MeasurementFields {
	return &MeasurementFields{fields: gensyncmap.Map[string, *Field]{}}
}

func (m *MeasurementFields) FieldKeys() []string {
	var a []string
	m.fields.Range(func(k string, _ *Field) bool {
		a = append(a, k)
		return true
	})

	sort.Strings(a)
	return a
}

// bytes estimates the memory footprint of this MeasurementFields, in bytes.
func (m *MeasurementFields) bytes() int {
	var b int
	b += int(unsafe.Sizeof(m.fields))
	m.fields.Range(func(k string, v *Field) bool {
		b += int(unsafe.Sizeof(k)) + len(k)
		b += int(unsafe.Sizeof(v)+unsafe.Sizeof(*v)) + len(v.Name)
		return true
	})
	return b
}

// CreateFieldIfNotExists creates a new field with the given name and type.
// Returns an error if the field already exists with a different type.
func (m *MeasurementFields) CreateFieldIfNotExists(name string, typ influxql.DataType) (bool, error) {
	newField := &Field{
		Name: name,
		Type: typ,
	}
	if f, loaded := m.fields.LoadOrStore(newField.Name, newField); loaded {
		if f.Type != typ {
			return false, ErrFieldTypeConflict
		}
		return false, nil
	}
	return true, nil
}

func (m *MeasurementFields) FieldN() int {
	return m.fields.Len()
}

// Field returns the field for name, or nil if there is no field for name.
func (m *MeasurementFields) Field(name string) *Field {
	f, _ := m.fields.Load(name)
	return f
}

func (m *MeasurementFields) HasField(name string) bool {
	if m == nil {
		return false
	}
	_, ok := m.fields.Load(name)
	return ok
}

// FieldSet returns the set of fields and their types for the measurement.
func (m *MeasurementFields) FieldSet() map[string]influxql.DataType {
	fieldTypes := make(map[string]influxql.DataType)
	m.fields.Range(func(k string, v *Field) bool {
		fieldTypes[k] = v.Type
		return true
	})
	return fieldTypes
}

func (m *MeasurementFields) ForEachField(fn func(name string, typ influxql.DataType) bool) {
	m.fields.Range(func(k string, v *Field) bool {
		return fn(k, v.Type)
	})
}

type FieldChanges []*FieldChange

func MeasurementsToFieldChangeDeletions(measurements []string) FieldChanges {
	fcs := make([]*FieldChange, 0, len(measurements))
	for _, m := range measurements {
		fcs = append(fcs, &FieldChange{
			FieldCreate: FieldCreate{
				Measurement: []byte(m),
				Field:       nil,
			},
			ChangeType: DeleteMeasurement,
		})
	}
	return fcs
}

// MeasurementFieldSet represents a collection of fields by measurement.
// This safe for concurrent use.
type MeasurementFieldSet struct {
	mu     sync.RWMutex
	fields map[string]*MeasurementFields
	// path is the location to persist field sets
	path      string
	changeMgr *measurementFieldSetChangeMgr
}

// NewMeasurementFieldSet returns a new instance of MeasurementFieldSet.
func NewMeasurementFieldSet(path string, logger *zap.Logger) (*MeasurementFieldSet, error) {
	const MaxCombinedWrites = 100
	fs := &MeasurementFieldSet{
		fields: make(map[string]*MeasurementFields),
		path:   path,
	}
	if nil == logger {
		logger = zap.NewNop()
	}
	fs.SetMeasurementFieldSetWriter(MaxCombinedWrites, logger)
	// If there is a load error, return the error and an empty set so
	// it can be rebuild manually.
	return fs, fs.load()
}

func (fs *MeasurementFieldSet) Close() error {
	if fs != nil && fs.changeMgr != nil {
		fs.changeMgr.Close()
		// If there is a change log file, save the in-memory version
		if _, err := os.Stat(fs.changeMgr.changeFilePath); err == nil {
			return fs.WriteToFile()
		} else if os.IsNotExist(err) {
			return nil
		} else {
			return fmt.Errorf("cannot get file information for %s: %w", fs.changeMgr.changeFilePath, err)
		}
	}
	return nil
}

func (fs *MeasurementFieldSet) ChangesPath() string {
	return fs.changeMgr.changeFilePath
}

// Bytes estimates the memory footprint of this MeasurementFieldSet, in bytes.
func (fs *MeasurementFieldSet) Bytes() int {
	var b int
	fs.mu.RLock()
	b += 24 // mu RWMutex is 24 bytes
	for k, v := range fs.fields {
		b += int(unsafe.Sizeof(k)) + len(k)
		b += int(unsafe.Sizeof(v)) + v.bytes()
	}
	b += int(unsafe.Sizeof(fs.fields))
	b += int(unsafe.Sizeof(fs.path)) + len(fs.path)
	fs.mu.RUnlock()
	return b
}

// Fields returns fields for a measurement by name.
func (fs *MeasurementFieldSet) Fields(name []byte) *MeasurementFields {
	fs.mu.RLock()
	mf := fs.fields[string(name)]
	fs.mu.RUnlock()
	return mf
}

// MeasurementNames returns the names of all the measurements in the field set in
// lexicographic order.
func (fs *MeasurementFieldSet) MeasurementNames() []string {
	fs.mu.RLock()
	defer fs.mu.RUnlock()

	names := make([]string, 0, len(fs.fields))
	for name := range fs.fields {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// FieldsByString returns fields for a measurment by name.
func (fs *MeasurementFieldSet) FieldsByString(name string) *MeasurementFields {
	fs.mu.RLock()
	mf := fs.fields[name]
	fs.mu.RUnlock()
	return mf
}

// CreateFieldsIfNotExists returns fields for a measurement by name.
func (fs *MeasurementFieldSet) CreateFieldsIfNotExists(name []byte) *MeasurementFields {
	fs.mu.RLock()
	mf := fs.fields[string(name)]
	fs.mu.RUnlock()

	if mf != nil {
		return mf
	}

	fs.mu.Lock()
	mf = fs.fields[string(name)]
	if mf == nil {
		mf = NewMeasurementFields()
		fs.fields[string(name)] = mf
	}
	fs.mu.Unlock()
	return mf
}

// Delete removes a field set for a measurement.
func (fs *MeasurementFieldSet) Delete(name string) {
	fs.mu.Lock()
	fs.deleteNoLock(name)
	fs.mu.Unlock()
}

// DeleteWithLock executes fn and removes a field set from a measurement under lock.
func (fs *MeasurementFieldSet) DeleteWithLock(name string, fn func() error) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	if err := fn(); err != nil {
		return err
	}

	fs.deleteNoLock(name)
	return nil
}

// deleteNoLock removes a field set for a measurement
func (fs *MeasurementFieldSet) deleteNoLock(name string) {
	delete(fs.fields, name)
}

func (fs *MeasurementFieldSet) IsEmpty() bool {
	fs.mu.RLock()
	defer fs.mu.RUnlock()
	return len(fs.fields) == 0
}

type errorChannel chan<- error

type writeRequest struct {
	errorReturn chan<- error
	changes     FieldChanges
}

type measurementFieldSetChangeMgr struct {
	mu             sync.Mutex
	wg             sync.WaitGroup
	writeRequests  chan writeRequest
	changeFilePath string
	logger         *zap.Logger
	changeFileSize int64
}

// SetMeasurementFieldSetWriter - initialize the queue for write requests
// and start the background write process
func (fs *MeasurementFieldSet) SetMeasurementFieldSetWriter(queueLength int, logger *zap.Logger) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	fs.changeMgr = &measurementFieldSetChangeMgr{
		writeRequests:  make(chan writeRequest, queueLength),
		changeFilePath: filepath.Join(filepath.Dir(fs.path), FieldsChangeFile),
		logger:         logger,
		changeFileSize: int64(0),
	}
	fs.changeMgr.wg.Add(1)
	go fs.changeMgr.SaveWriter()
}

func (fscm *measurementFieldSetChangeMgr) Close() {
	if fscm != nil {
		close(fscm.writeRequests)
		fscm.wg.Wait()
	}
}

func (fs *MeasurementFieldSet) Save(changes FieldChanges) error {
	return fs.changeMgr.RequestSave(changes)
}

func (fscm *measurementFieldSetChangeMgr) RequestSave(changes FieldChanges) error {
	done := make(chan error)
	fscm.writeRequests <- writeRequest{errorReturn: done, changes: changes}
	return <-done
}

func (fscm *measurementFieldSetChangeMgr) SaveWriter() {
	defer fscm.wg.Done()
	// Block until someone modifies the MeasurementFieldSet, and
	// it needs to be written to disk. Exit when the channel is closed
	for wr, ok := <-fscm.writeRequests; ok; wr, ok = <-fscm.writeRequests {
		fscm.appendToChangesFile(wr)
	}
}

// WriteToFile: Write the new index to a temp file and rename when it's sync'd
// This locks the MeasurementFieldSet during the marshaling, the write, and the rename.
func (fs *MeasurementFieldSet) WriteToFile() error {
	path := fs.path + ".tmp"

	// Open the temp file
	fd, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_EXCL|os.O_SYNC, 0666)
	if err != nil {
		return fmt.Errorf("failed opening %s: %w", fs.path, err)
	}
	// Ensure temp file is cleaned up
	defer func() {
		if e := os.RemoveAll(path); err == nil && e != nil {
			err = fmt.Errorf("failed removing temporary file %s: %w", path, e)
		}
		if e := os.RemoveAll(fs.changeMgr.changeFilePath); err == nil && e != nil {
			err = fmt.Errorf("failed removing saved field changes - %s: %w", fs.changeMgr.changeFilePath, e)
		}
	}()
	fs.mu.RLock()
	defer fs.mu.RUnlock()
	isEmpty, err := func() (isEmpty bool, err error) {
		// ensure temp file closed before rename (for Windows)
		defer func() {
			if e := fd.Close(); err == nil && e != nil {
				err = fmt.Errorf("closing %s: %w", path, e)
			}
		}()
		if _, err = fd.Write(fieldsIndexMagicNumber); err != nil {
			return true, fmt.Errorf("failed writing magic number for %s: %w", path, err)
		}

		// Lock, copy, and marshal the in-memory index
		b, err := fs.marshalMeasurementFieldSetNoLock()
		if err != nil {
			return true, fmt.Errorf("failed marshaling fields for %s: %w", fs.path, err)
		}
		if b == nil {
			// No fields, file removed, all done
			return true, nil
		}
		if _, err := fd.Write(b); err != nil {
			return true, fmt.Errorf("failed saving fields to %s: %w", path, err)
		}
		return false, nil
	}()
	if err != nil {
		return err
	} else if isEmpty {
		// remove empty file
		if err = os.RemoveAll(fs.path); err != nil {
			return fmt.Errorf("cannot remove %s: %w", fs.path, err)
		} else {
			return nil
		}
	}

	return fs.renameFileNoLock(path)
}

// appendToChangesFile: Write a change file for fields.idx
// Only called in one Go proc, so does not need locking.
func (fscm *measurementFieldSetChangeMgr) appendToChangesFile(first writeRequest) {
	var err error = nil
	// Put the errorChannel on which we blocked into a slice to allow more invocations
	// to share the return code from the file write
	errorChannels := []errorChannel{first.errorReturn}
	changes := []FieldChanges{first.changes}
	// On return, send the error to every go proc that send changes
	defer func() {
		for _, c := range errorChannels {
			c <- err
			close(c)
		}
	}()
	log, end := logger.NewOperation(fscm.logger, "saving field index changes", "MeasurementFieldSet")
	defer end()
	// Do some blocking IO operations before marshalling the changes,
	// to allow other changes to be queued up and be captured in one
	// write operation, in case we are under heavy field creation load
	fscm.mu.Lock()
	defer fscm.mu.Unlock()
	fd, err := os.OpenFile(fscm.changeFilePath, os.O_CREATE|os.O_APPEND|os.O_SYNC|os.O_WRONLY, 0666)
	if err != nil {
		err = fmt.Errorf("opening %s: %w", fscm.changeFilePath, err)
		log.Error("failed", zap.Error(err))
		return
	}

	// ensure file closed
	defer errors2.Capture(&err, func() error {
		if e := fd.Close(); e != nil {
			e = fmt.Errorf("closing %s: %w", fd.Name(), e)
			log.Error("failed", zap.Error(e))
			return e
		} else {
			return nil
		}
	})()

	var fi os.FileInfo
	if fi, err = fd.Stat(); err != nil {
		err = fmt.Errorf("unable to get size of %s: %w", fd.Name(), err)
		log.Error("failed", zap.Error(err))
		return
	} else if fi.Size() > fscm.changeFileSize {
		// If we had a partial write last time, truncate the file to remove it.
		if err = fd.Truncate(fscm.changeFileSize); err != nil {
			err = fmt.Errorf("cannot truncate %s to last known good size of %d after incomplete write: %w", fd.Name(), fscm.changeFileSize, err)
			log.Error("failed", zap.Error(err))
			return
		}
	}

	// Read all the pending field and measurement write or delete
	// requests
	for {
		select {
		case wr, ok := <-fscm.writeRequests:
			if ok {
				changes = append(changes, wr.changes)
				errorChannels = append(errorChannels, wr.errorReturn)
				continue
			}
		default:
		}
		break
	}
	// marshal the slice of slices of field changes in size-prefixed protobuf
	var b []byte
	b, err = marshalFieldChanges(changes...)
	if err != nil {
		err = fmt.Errorf("error marshaling changes for %s: %w", fd.Name(), err)
		log.Error("failed", zap.Error(err))
		return
	}

	if _, err = fd.Write(b); err != nil {
		err = fmt.Errorf("failed writing to %s: %w", fd.Name(), err)
		log.Error("failed", zap.Error(err))
		return
	} else if fi, err = fd.Stat(); err != nil {
		err = fmt.Errorf("unable to get final size of %s after appendation: %w", fd.Name(), err)
		log.Error("failed", zap.Error(err))
		return
	} else {
		fscm.changeFileSize = fi.Size()
	}
}

func readSizePlusBuffer(r io.Reader, b []byte) ([]byte, error) {
	var numBuf [bytesInInt64]byte

	if _, err := r.Read(numBuf[:]); err != nil {
		return nil, err
	}
	size := int(binary.LittleEndian.Uint64(numBuf[:]))
	if cap(b) < size {
		b = make([]byte, size)
	}
	_, err := io.ReadAtLeast(r, b, size)
	if err != nil {
		return nil, err
	}
	return b, nil
}

func (fs *MeasurementFieldSet) renameFileNoLock(path string) error {
	if err := file.RenameFile(path, fs.path); err != nil {
		return fmt.Errorf("cannot rename %s to %s: %w", path, fs.path, err)
	}

	dir := filepath.Dir(fs.path)
	if err := file.SyncDir(dir); err != nil {
		return fmt.Errorf("cannot sync directory %s: %w", dir, err)
	}

	return nil
}

// marshalMeasurementFieldSetNoLock: remove the fields.idx file if no fields
// otherwise, copy the in-memory version into a protobuf to write to
// disk
func (fs *MeasurementFieldSet) marshalMeasurementFieldSetNoLock() (marshalled []byte, err error) {
	if len(fs.fields) == 0 {
		// If no fields left, remove the fields index file
		return nil, nil
	}

	pb := internal.MeasurementFieldSet{
		Measurements: make([]*internal.MeasurementFields, 0, len(fs.fields)),
	}

	for name, mf := range fs.fields {
		imf := &internal.MeasurementFields{
			Name:   []byte(name),
			Fields: make([]*internal.Field, 0, mf.FieldN()),
		}

		mf.ForEachField(func(field string, typ influxql.DataType) bool {
			imf.Fields = append(imf.Fields, &internal.Field{Name: []byte(field), Type: int32(typ)})
			return true
		})

		pb.Measurements = append(pb.Measurements, imf)
	}
	b, err := proto.Marshal(&pb)
	if err != nil {
		return nil, err
	} else {
		return b, nil
	}
}

func marshalFieldChanges(changeSet ...FieldChanges) ([]byte, error) {
	fcs := internal.FieldChangeSet{
		Changes: nil,
	}
	for _, fc := range changeSet {
		for _, f := range fc {
			mfc := &internal.MeasurementFieldChange{
				Measurement: f.Measurement,
				Change:      internal.ChangeType(f.ChangeType),
			}
			if f.Field != nil {
				mfc.Field = &internal.Field{
					Name: []byte(f.Field.Name),
					Type: int32(f.Field.Type),
				}
				fcs.Changes = append(fcs.Changes, mfc)
			}
		}
	}
	mo := proto.MarshalOptions{}
	var numBuf [bytesInInt64]byte

	b, err := mo.MarshalAppend(numBuf[:], &fcs)
	binary.LittleEndian.PutUint64(b[0:bytesInInt64], uint64(len(b)-bytesInInt64))

	if err != nil {
		fields := make([]string, 0, len(fcs.Changes))
		for _, fc := range changeSet {
			for _, f := range fc {
				fields = append(fields, fmt.Sprintf("%q.%q", f.Measurement, f.Field.Name))
			}
		}
		return nil, fmt.Errorf("failed marshaling new fields - %s: %w", strings.Join(fields, ", "), err)
	}
	return b, nil
}

func (fs *MeasurementFieldSet) load() (rErr error) {
	err := func() error {
		fs.mu.Lock()
		defer fs.mu.Unlock()

		pb, err := fs.loadParseFieldIndexPB()
		if err != nil {
			return err
		}
		fs.fields = make(map[string]*MeasurementFields, len(pb.GetMeasurements()))
		for _, measurement := range pb.GetMeasurements() {
			set := NewMeasurementFields()
			for _, field := range measurement.GetFields() {
				name := string(field.GetName())
				set.fields.Store(name, &Field{Name: name, Type: influxql.DataType(field.GetType())})
			}
			fs.fields[string(measurement.GetName())] = set
		}
		return nil
	}()

	if err != nil {
		return fmt.Errorf("failed loading field indices: %w", err)
	}
	return fs.ApplyChanges()
}

func (fs *MeasurementFieldSet) loadParseFieldIndexPB() (pb *internal.MeasurementFieldSet, rErr error) {
	pb = &internal.MeasurementFieldSet{}

	fd, err := os.Open(fs.path)
	if os.IsNotExist(err) {
		return pb, nil
	} else if err != nil {
		err = fmt.Errorf("failed opening %s: %w", fs.path, err)
		return nil, err
	}

	defer errors2.Capture(&rErr, func() error {
		if e := fd.Close(); e != nil {
			return fmt.Errorf("failed closing %s: %w", fd.Name(), e)
		} else {
			return nil
		}
	})()

	var magic [4]byte
	if _, err := fd.Read(magic[:]); err != nil {
		err = fmt.Errorf("failed reading %s: %w", fs.path, err)
		return nil, err
	}

	if !bytes.Equal(magic[:], fieldsIndexMagicNumber) {
		return nil, fmt.Errorf("%q: %w", fs.path, ErrUnknownFieldsFormat)
	}

	b, err := io.ReadAll(fd)
	if err != nil {
		err = fmt.Errorf("failed reading %s: %w", fs.path, err)
		return nil, err
	}
	if err = proto.Unmarshal(b, pb); err != nil {
		err = fmt.Errorf("failed unmarshaling %s: %w", fs.path, err)
		return nil, err
	}
	return pb, err
}

func (fscm *measurementFieldSetChangeMgr) loadAllFieldChanges(log *zap.Logger) (changes []FieldChanges, rErr error) {
	var fcs FieldChanges

	fscm.mu.Lock()
	defer fscm.mu.Unlock()
	fd, err := os.Open(fscm.changeFilePath)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		err = fmt.Errorf("failed opening %s: %w", fscm.changeFilePath, err)
		log.Error("field index file of changes", zap.Error(err))
		return nil, err
	}
	defer errors2.Capture(&rErr, func() error {
		if e := fd.Close(); e != nil {
			return fmt.Errorf("failed closing %s: %w", fd.Name(), e)
		} else {
			return nil
		}
	})()
	changesetCount := 0
	totalChanges := 0
	for fcs, err = fscm.loadFieldChangeSet(fd); err == nil; fcs, err = fscm.loadFieldChangeSet(fd) {
		totalChanges += len(fcs)
		changesetCount++
		log.Debug("loading field change set",
			zap.Int("set", changesetCount),
			zap.Int("changes", len(fcs)),
			zap.Int("total_changes", totalChanges))
		changes = append(changes, fcs)
	}
	if errors.Is(err, io.EOF) {
		return changes, nil
	} else if errors.Is(err, io.ErrUnexpectedEOF) {
		log.Warn("last entry was an incomplete write", zap.Error(err))
		return changes, nil
	} else {
		log.Error("field index file of changes", zap.Error(err))
		return nil, err
	}
}

func (fscm *measurementFieldSetChangeMgr) loadFieldChangeSet(r io.Reader) (FieldChanges, error) {
	var pb internal.FieldChangeSet

	b, err := readSizePlusBuffer(r, nil)
	if err != nil {
		return nil, fmt.Errorf("failed reading %s: %w", fscm.changeFilePath, err)
	}
	if err := proto.Unmarshal(b, &pb); err != nil {
		return nil, fmt.Errorf("failed unmarshalling %s: %w", fscm.changeFilePath, err)
	}

	fcs := make([]*FieldChange, 0, len(pb.Changes))

	for _, fc := range pb.Changes {
		fcs = append(fcs, &FieldChange{
			FieldCreate: FieldCreate{
				Measurement: fc.Measurement,
				Field: &Field{
					Name: string(fc.Field.Name),
					Type: influxql.DataType(fc.Field.Type),
				},
			},
			ChangeType: ChangeType(fc.Change),
		})
	}
	return fcs, nil
}

func (fs *MeasurementFieldSet) ApplyChanges() error {
	log, end := logger.NewOperation(fs.changeMgr.logger, "loading changes", "field indices", zap.String("path", fs.changeMgr.changeFilePath))
	defer end()
	changes, err := fs.changeMgr.loadAllFieldChanges(log)
	if err != nil {
		return err
	}
	if len(changes) <= 0 {
		return os.RemoveAll(fs.changeMgr.changeFilePath)
	}

	for _, fcs := range changes {
		for _, fc := range fcs {
			if fc.ChangeType == DeleteMeasurement {
				fs.Delete(string(fc.Measurement))
			} else {
				mf := fs.CreateFieldsIfNotExists(fc.Measurement)
				if _, err := mf.CreateFieldIfNotExists(fc.Field.Name, fc.Field.Type); err != nil {
					err = fmt.Errorf("failed creating %q.%q: %w", fc.Measurement, fc.Field.Name, err)
					log.Error("field creation", zap.Error(err))
					return err
				}
			}
		}
	}
	return fs.WriteToFile()
}

// Field represents a series field. All of the fields must be hashable.
type Field struct {
	Name string
	Type influxql.DataType
}

type FieldChange struct {
	FieldCreate
	ChangeType ChangeType
}

type ChangeType int

const (
	AddMeasurementField = ChangeType(internal.ChangeType_AddMeasurementField)
	DeleteMeasurement   = ChangeType(internal.ChangeType_DeleteMeasurement)
)

// NewFieldKeysIterator returns an iterator that can be iterated over to
// retrieve field keys.
func NewFieldKeysIterator(sh *Shard, opt query.IteratorOptions) (query.Iterator, error) {
	itr := &fieldKeysIterator{shard: sh}

	index, err := sh.Index()
	if err != nil {
		return nil, err
	}

	// Retrieve measurements from shard. Filter if condition specified.
	//
	// FGA is currently not supported when retrieving field keys.
	indexSet := IndexSet{Indexes: []Index{index}, SeriesFile: sh.sfile}
	names, err := indexSet.MeasurementNamesByExpr(query.OpenAuthorizer, opt.Condition)
	if err != nil {
		return nil, err
	}
	itr.names = names

	return itr, nil
}

// fieldKeysIterator iterates over measurements and gets field keys from each measurement.
type fieldKeysIterator struct {
	shard *Shard
	names [][]byte // remaining measurement names
	buf   struct {
		name   []byte  // current measurement name
		fields []Field // current measurement's fields
	}
}

// Stats returns stats about the points processed.
func (itr *fieldKeysIterator) Stats() query.IteratorStats { return query.IteratorStats{} }

// Close closes the iterator.
func (itr *fieldKeysIterator) Close() error { return nil }

// Next emits the next tag key name.
func (itr *fieldKeysIterator) Next() (*query.FloatPoint, error) {
	for {
		// If there are no more keys then move to the next measurements.
		if len(itr.buf.fields) == 0 {
			if len(itr.names) == 0 {
				return nil, nil
			}

			itr.buf.name = itr.names[0]
			mf := itr.shard.MeasurementFields(itr.buf.name)
			if mf != nil {
				fset := mf.FieldSet()
				if len(fset) == 0 {
					itr.names = itr.names[1:]
					continue
				}

				keys := make([]string, 0, len(fset))
				for k := range fset {
					keys = append(keys, k)
				}
				sort.Strings(keys)

				itr.buf.fields = make([]Field, len(keys))
				for i, name := range keys {
					itr.buf.fields[i] = Field{Name: name, Type: fset[name]}
				}
			}
			itr.names = itr.names[1:]
			continue
		}

		// Return next key.
		field := itr.buf.fields[0]
		p := &query.FloatPoint{
			Name: string(itr.buf.name),
			Aux:  []interface{}{field.Name, field.Type.String()},
		}
		itr.buf.fields = itr.buf.fields[1:]

		return p, nil
	}
}

// NewTagKeysIterator returns a new instance of TagKeysIterator.
func NewTagKeysIterator(sh *Shard, opt query.IteratorOptions) (query.Iterator, error) {
	fn := func(name []byte) ([][]byte, error) {
		index, err := sh.Index()
		if err != nil {
			return nil, err
		}

		indexSet := IndexSet{Indexes: []Index{index}, SeriesFile: sh.sfile}
		var keys [][]byte
		if err := indexSet.ForEachMeasurementTagKey(name, func(key []byte) error {
			keys = append(keys, key)
			return nil
		}); err != nil {
			return nil, err
		}
		return keys, nil
	}
	return newMeasurementKeysIterator(sh, fn, opt)
}

// measurementKeyFunc is the function called by measurementKeysIterator.
type measurementKeyFunc func(name []byte) ([][]byte, error)

func newMeasurementKeysIterator(sh *Shard, fn measurementKeyFunc, opt query.IteratorOptions) (*measurementKeysIterator, error) {
	index, err := sh.Index()
	if err != nil {
		return nil, err
	}

	indexSet := IndexSet{Indexes: []Index{index}, SeriesFile: sh.sfile}
	itr := &measurementKeysIterator{fn: fn}
	names, err := indexSet.MeasurementNamesByExpr(opt.Authorizer, opt.Condition)
	if err != nil {
		return nil, err
	}
	itr.names = names

	return itr, nil
}

// measurementKeysIterator iterates over measurements and gets keys from each measurement.
type measurementKeysIterator struct {
	names [][]byte // remaining measurement names
	buf   struct {
		name []byte   // current measurement name
		keys [][]byte // current measurement's keys
	}
	fn measurementKeyFunc
}

// Stats returns stats about the points processed.
func (itr *measurementKeysIterator) Stats() query.IteratorStats { return query.IteratorStats{} }

// Close closes the iterator.
func (itr *measurementKeysIterator) Close() error { return nil }

// Next emits the next tag key name.
func (itr *measurementKeysIterator) Next() (*query.FloatPoint, error) {
	for {
		// If there are no more keys then move to the next measurements.
		if len(itr.buf.keys) == 0 {
			if len(itr.names) == 0 {
				return nil, nil
			}

			itr.buf.name, itr.names = itr.names[0], itr.names[1:]

			keys, err := itr.fn(itr.buf.name)
			if err != nil {
				return nil, err
			}
			itr.buf.keys = keys
			continue
		}

		// Return next key.
		p := &query.FloatPoint{
			Name: string(itr.buf.name),
			Aux:  []interface{}{string(itr.buf.keys[0])},
		}
		itr.buf.keys = itr.buf.keys[1:]

		return p, nil
	}
}

// LimitError represents an error caused by a configurable limit.
type LimitError struct {
	Reason string
}

func (e *LimitError) Error() string { return e.Reason }
