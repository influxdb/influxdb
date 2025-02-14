package tsi1

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math/rand"
	"os"
	"sort"
	"sync"
	"time"
	"unsafe"

	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/pkg/bloom"
	"github.com/influxdata/influxdb/pkg/estimator"
	"github.com/influxdata/influxdb/pkg/estimator/hll"
	"github.com/influxdata/influxdb/pkg/mmap"
	"github.com/influxdata/influxdb/tsdb"
)

// Log errors.
var (
	ErrLogEntryChecksumMismatch = errors.New("log entry checksum mismatch")
)

// Log entry flag constants.
const (
	LogEntrySeriesTombstoneFlag      = 0x01
	LogEntryMeasurementTombstoneFlag = 0x02
	LogEntryTagKeyTombstoneFlag      = 0x04
	LogEntryTagValueTombstoneFlag    = 0x08
)

// defaultLogFileBufferSize describes the size of the buffer that the LogFile's buffered
// writer uses. If the LogFile does not have an explicit buffer size set then
// this is the size of the buffer; it is equal to the default buffer size used
// by a bufio.Writer.
const defaultLogFileBufferSize = 4096

// indexFileBufferSize is the buffer size used when compacting the LogFile down
// into a .tsi file.
const indexFileBufferSize = 1 << 17 // 128K

// LogFile represents an on-disk write-ahead log file.
type LogFile struct {
	mu         sync.RWMutex
	wg         sync.WaitGroup // ref count
	id         int            // file sequence identifier
	data       []byte         // mmap
	file       *os.File       // writer
	w          *bufio.Writer  // buffered writer
	bufferSize int            // The size of the buffer used by the buffered writer
	nosync     bool           // Disables buffer flushing and file syncing. Useful for offline tooling.
	buf        []byte         // marshaling buffer
	keyBuf     []byte

	sfile   *tsdb.SeriesFile // series lookup
	size    int64            // tracks current file size
	modTime time.Time        // tracks last time write occurred

	// In-memory series existence/tombstone sets.
	seriesIDSet, tombstoneSeriesIDSet *tsdb.SeriesIDSet

	// In-memory index.
	mms logMeasurements

	// Filepath to the log file.
	path string
}

// NewLogFile returns a new instance of LogFile.
func NewLogFile(sfile *tsdb.SeriesFile, path string) *LogFile {
	return &LogFile{
		sfile: sfile,
		path:  path,
		mms:   make(logMeasurements),

		seriesIDSet:          tsdb.NewSeriesIDSet(),
		tombstoneSeriesIDSet: tsdb.NewSeriesIDSet(),
	}
}

// bytes estimates the memory footprint of this LogFile, in bytes.
func (f *LogFile) bytes() int {
	f.mu.RLock()
	defer f.mu.RUnlock()
	var b int
	b += 24 // mu RWMutex is 24 bytes
	b += 16 // wg WaitGroup is 16 bytes
	b += int(unsafe.Sizeof(f.id))
	// Do not include f.data because it is mmap'd
	// TODO(jacobmarble): Uncomment when we are using go >= 1.10.0
	//b += int(unsafe.Sizeof(f.w)) + f.w.Size()
	b += int(unsafe.Sizeof(f.buf)) + len(f.buf)
	b += int(unsafe.Sizeof(f.keyBuf)) + len(f.keyBuf)
	// Do not count SeriesFile because it belongs to the code that constructed this Index.
	b += int(unsafe.Sizeof(f.size))
	b += int(unsafe.Sizeof(f.modTime))
	b += int(unsafe.Sizeof(f.seriesIDSet)) + f.seriesIDSet.Bytes()
	b += int(unsafe.Sizeof(f.tombstoneSeriesIDSet)) + f.tombstoneSeriesIDSet.Bytes()
	b += int(unsafe.Sizeof(f.mms)) + f.mms.bytes()
	b += int(unsafe.Sizeof(f.path)) + len(f.path)
	return b
}

// Open reads the log from a file and validates all the checksums.
func (f *LogFile) Open() error {
	if err := f.open(); err != nil {
		f.Close()
		return err
	}
	return nil
}

func (f *LogFile) open() error {
	f.id, _ = ParseFilename(f.path)

	// Open file for appending.
	file, err := os.OpenFile(f.Path(), os.O_WRONLY|os.O_CREATE, 0666)
	if err != nil {
		return err
	}
	f.file = file

	if f.bufferSize == 0 {
		f.bufferSize = defaultLogFileBufferSize
	}
	f.w = bufio.NewWriterSize(f.file, f.bufferSize)

	// Finish opening if file is empty.
	fi, err := file.Stat()
	if err != nil {
		return err
	} else if fi.Size() == 0 {
		return nil
	}
	f.size = fi.Size()
	f.modTime = fi.ModTime()

	// Open a read-only memory map of the existing data.
	data, err := mmap.Map(f.Path(), 0)
	if err != nil {
		return err
	}
	f.data = data

	// Read log entries from mmap.
	var n int64
	for buf := f.data; len(buf) > 0; {
		// Read next entry. Truncate partial writes.
		var e LogEntry
		if err := e.UnmarshalBinary(buf); errors.Is(err, io.ErrShortBuffer) || errors.Is(err, ErrLogEntryChecksumMismatch) {
			break
		} else if err != nil {
			return fmt.Errorf("%q: %w", f.path, err)
		}

		// Execute entry against in-memory index.
		f.execEntry(&e)

		// Move buffer forward.
		n += int64(e.Size)
		buf = buf[e.Size:]
	}

	// Move to the end of the file.
	f.size = n
	_, err = file.Seek(n, io.SeekStart)
	return err
}

// Close shuts down the file handle and mmap.
func (f *LogFile) Close() error {
	// Wait until the file has no more references.
	f.wg.Wait()

	if f.w != nil {
		f.w.Flush()
		f.w = nil
	}

	if f.file != nil {
		f.file.Close()
		f.file = nil
	}

	if f.data != nil {
		mmap.Unmap(f.data)
	}

	f.mms = make(logMeasurements)
	return nil
}

// FlushAndSync flushes buffered data to disk and then fsyncs the underlying file.
// If the LogFile has disabled flushing and syncing then FlushAndSync is a no-op.
func (f *LogFile) FlushAndSync() error {
	if f.nosync {
		return nil
	}

	if f.w != nil {
		if err := f.w.Flush(); err != nil {
			return err
		}
	}

	if f.file == nil {
		return nil
	}
	return f.file.Sync()
}

// FlushAndSync flushes buffered data to disk
// If the LogFile has disabled flushing and syncing then FlushAndSync is a no-op.
func (f *LogFile) Flush() error {
	if f.nosync {
		return nil
	}

	if f.w != nil {
		if err := f.w.Flush(); err != nil {
			return err
		}
	}

	return nil
}

// ID returns the file sequence identifier.
func (f *LogFile) ID() int { return f.id }

// Path returns the file path.
func (f *LogFile) Path() string { return f.path }

// SetPath sets the log file's path.
func (f *LogFile) SetPath(path string) { f.path = path }

// Level returns the log level of the file.
func (f *LogFile) Level() int { return 0 }

// Filter returns the bloom filter for the file.
func (f *LogFile) Filter() *bloom.Filter { return nil }

// Retain adds a reference count to the file.
func (f *LogFile) Retain() { f.wg.Add(1) }

// Release removes a reference count from the file.
func (f *LogFile) Release() { f.wg.Done() }

// Stat returns size and last modification time of the file.
func (f *LogFile) Stat() (int64, time.Time) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	size, modTime := f.size, f.modTime
	return size, modTime
}

// SeriesIDSet returns the series existence set.
func (f *LogFile) SeriesIDSet() (*tsdb.SeriesIDSet, error) {
	return f.seriesIDSet, nil
}

// TombstoneSeriesIDSet returns the series tombstone set.
func (f *LogFile) TombstoneSeriesIDSet() (*tsdb.SeriesIDSet, error) {
	return f.tombstoneSeriesIDSet, nil
}

// Size returns the size of the file, in bytes.
func (f *LogFile) Size() int64 {
	f.mu.RLock()
	defer f.mu.RUnlock()
	v := f.size
	return v
}

// ModTime returns the last modified time of the file
func (f *LogFile) ModTime() time.Time {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.modTime
}

// Measurement returns a measurement element.
func (f *LogFile) Measurement(name []byte) MeasurementElem {
	f.mu.RLock()
	defer f.mu.RUnlock()

	mm, ok := f.mms[string(name)]
	if !ok {
		return nil
	}

	return mm
}

func (f *LogFile) MeasurementHasSeries(ss *tsdb.SeriesIDSet, name []byte) bool {
	f.mu.RLock()
	defer f.mu.RUnlock()

	mm, ok := f.mms[string(name)]
	if !ok {
		return false
	}

	// TODO(edd): if mm is using a seriesSet then this could be changed to do a fast intersection.
	for _, id := range mm.seriesIDs() {
		if ss.Contains(id) {
			return true
		}
	}
	return false
}

// MeasurementNames returns an ordered list of measurement names.
func (f *LogFile) MeasurementNames() []string {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.measurementNames()
}

func (f *LogFile) measurementNames() []string {
	a := make([]string, 0, len(f.mms))
	for name := range f.mms {
		a = append(a, name)
	}
	sort.Strings(a)
	return a
}

// DeleteMeasurement adds a tombstone for a measurement to the log file.
func (f *LogFile) DeleteMeasurement(name []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	e := LogEntry{Flag: LogEntryMeasurementTombstoneFlag, Name: name}
	if err := f.appendEntry(&e); err != nil {
		return err
	}
	f.execEntry(&e)

	// Flush buffer and sync to disk.
	return f.FlushAndSync()
}

// TagKeySeriesIDIterator returns a series iterator for a tag key.
func (f *LogFile) TagKeySeriesIDIterator(name, key []byte) (tsdb.SeriesIDIterator, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	mm, ok := f.mms[string(name)]
	if !ok {
		return nil, nil
	}

	tk, ok := mm.tagSet[string(key)]
	if !ok {
		return nil, nil
	}

	// Combine iterators across all tag keys.
	itrs := make([]tsdb.SeriesIDIterator, 0, len(tk.tagValues))
	for _, tv := range tk.tagValues {
		if tv.cardinality() == 0 {
			continue
		}
		if itr := tsdb.NewSeriesIDSetIterator(tv.seriesIDSet()); itr != nil {
			itrs = append(itrs, itr)
		}
	}

	return tsdb.MergeSeriesIDIterators(itrs...), nil
}

// TagKeyIterator returns a value iterator for a measurement.
func (f *LogFile) TagKeyIterator(name []byte) TagKeyIterator {
	f.mu.RLock()
	defer f.mu.RUnlock()

	mm, ok := f.mms[string(name)]
	if !ok {
		return nil
	}

	a := make([]logTagKey, 0, len(mm.tagSet))
	for _, k := range mm.tagSet {
		a = append(a, k)
	}
	return newLogTagKeyIterator(a)
}

// TagKey returns a tag key element.
func (f *LogFile) TagKey(name, key []byte) TagKeyElem {
	f.mu.RLock()
	defer f.mu.RUnlock()

	mm, ok := f.mms[string(name)]
	if !ok {
		return nil
	}

	tk, ok := mm.tagSet[string(key)]
	if !ok {
		return nil
	}

	return &tk
}

// TagValue returns a tag value element.
func (f *LogFile) TagValue(name, key, value []byte) TagValueElem {
	f.mu.RLock()
	defer f.mu.RUnlock()

	mm, ok := f.mms[string(name)]
	if !ok {
		return nil
	}

	tk, ok := mm.tagSet[string(key)]
	if !ok {
		return nil
	}

	tv, ok := tk.tagValues[string(value)]
	if !ok {
		return nil
	}

	return &tv
}

// TagValueIterator returns a value iterator for a tag key.
func (f *LogFile) TagValueIterator(name, key []byte) TagValueIterator {
	f.mu.RLock()
	defer f.mu.RUnlock()

	mm, ok := f.mms[string(name)]
	if !ok {
		return nil
	}

	tk, ok := mm.tagSet[string(key)]
	if !ok {
		return nil
	}
	return tk.TagValueIterator()
}

// DeleteTagKey adds a tombstone for a tag key to the log file.
func (f *LogFile) DeleteTagKey(name, key []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	e := LogEntry{Flag: LogEntryTagKeyTombstoneFlag, Name: name, Key: key}
	if err := f.appendEntry(&e); err != nil {
		return err
	}
	f.execEntry(&e)

	// Flush buffer and sync to disk.
	return f.FlushAndSync()
}

// TagValueSeriesIDSet returns a series iterator for a tag value.
func (f *LogFile) TagValueSeriesIDSet(name, key, value []byte) (*tsdb.SeriesIDSet, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	mm, ok := f.mms[string(name)]
	if !ok {
		return nil, nil
	}

	tk, ok := mm.tagSet[string(key)]
	if !ok {
		return nil, nil
	}

	tv, ok := tk.tagValues[string(value)]
	if !ok {
		return nil, nil
	} else if tv.cardinality() == 0 {
		return nil, nil
	}

	return tv.seriesIDSet(), nil
}

// MeasurementN returns the total number of measurements.
func (f *LogFile) MeasurementN() (n uint64) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return uint64(len(f.mms))
}

// TagKeyN returns the total number of keys.
func (f *LogFile) TagKeyN() (n uint64) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	for _, mm := range f.mms {
		n += uint64(len(mm.tagSet))
	}
	return n
}

// TagValueN returns the total number of values.
func (f *LogFile) TagValueN() (n uint64) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	for _, mm := range f.mms {
		for _, k := range mm.tagSet {
			n += uint64(len(k.tagValues))
		}
	}
	return n
}

// DeleteTagValue adds a tombstone for a tag value to the log file.
func (f *LogFile) DeleteTagValue(name, key, value []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	e := LogEntry{Flag: LogEntryTagValueTombstoneFlag, Name: name, Key: key, Value: value}
	if err := f.appendEntry(&e); err != nil {
		return err
	}
	f.execEntry(&e)

	// Flush buffer and sync to disk.
	return f.FlushAndSync()
}

// AddSeriesList adds a list of series to the log file in bulk.
func (f *LogFile) AddSeriesList(seriesSet *tsdb.SeriesIDSet, names [][]byte, tagsSlice []models.Tags, tracker tsdb.StatsTracker) ([]uint64, error) {
	seriesIDs, err := f.sfile.CreateSeriesListIfNotExists(names, tagsSlice, tracker)
	if err != nil {
		return nil, err
	}

	var writeRequired bool
	entries := make([]LogEntry, 0, len(names))
	seriesSet.RLock()
	for i := range names {
		if seriesSet.ContainsNoLock(seriesIDs[i]) {
			// We don't need to allocate anything for this series.
			seriesIDs[i] = 0
			continue
		}
		writeRequired = true
		entries = append(entries, LogEntry{SeriesID: seriesIDs[i], name: names[i], tags: tagsSlice[i], cached: true, batchidx: i})
	}
	seriesSet.RUnlock()

	// Exit if all series already exist.
	if !writeRequired {
		return seriesIDs, nil
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	seriesSet.Lock()
	defer seriesSet.Unlock()

	for i := range entries { // NB - this doesn't evaluate all series ids returned from series file.
		entry := &entries[i]
		if seriesSet.ContainsNoLock(entry.SeriesID) {
			// We don't need to allocate anything for this series.
			seriesIDs[entry.batchidx] = 0
			continue
		}
		if err := f.appendEntry(entry); err != nil {
			return nil, err
		}
		f.execEntry(entry)
		seriesSet.AddNoLock(entry.SeriesID)
	}

	if rand.Intn(100) < 10 {
		// Flush buffer and sync to disk.
		if err := f.FlushAndSync(); err != nil {
			return nil, err
		}
	} else {
		// Flush buffer.
		if err := f.Flush(); err != nil {
			return nil, err
		}
	}

	return seriesIDs, nil
}

// DeleteSeriesID adds a tombstone for a series id.
func (f *LogFile) DeleteSeriesID(id uint64) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	e := LogEntry{Flag: LogEntrySeriesTombstoneFlag, SeriesID: id}
	if err := f.appendEntry(&e); err != nil {
		return err
	}
	f.execEntry(&e)

	// Flush buffer and sync to disk.
	return f.FlushAndSync()
}

// DeleteSeriesIDList adds a tombstone for seriesIDList
func (f *LogFile) DeleteSeriesIDList(ids []uint64) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	for _, id := range ids {
		e := LogEntry{Flag: LogEntrySeriesTombstoneFlag, SeriesID: id}
		if err := f.appendEntry(&e); err != nil {
			return err
		}
		f.execEntry(&e)
	}

	// Flush buffer and sync to disk.
	return f.FlushAndSync()
}

// SeriesN returns the total number of series in the file.
func (f *LogFile) SeriesN() (n uint64) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	for _, mm := range f.mms {
		n += uint64(mm.cardinality())
	}
	return n
}

// appendEntry adds a log entry to the end of the file.
func (f *LogFile) appendEntry(e *LogEntry) error {
	// Marshal entry to the local buffer.
	f.buf = appendLogEntry(f.buf[:0], e)

	// Save the size of the record.
	e.Size = len(f.buf)

	// Write record to file.
	n, err := f.w.Write(f.buf)
	if err != nil {
		// Move position backwards over partial entry.
		// Log should be reopened if seeking cannot be completed.
		if n > 0 {
			f.w.Reset(f.file)
			if _, err := f.file.Seek(int64(-n), io.SeekCurrent); err != nil {
				f.Close()
			}
		}
		return err
	}

	// Update in-memory file size & modification time.
	f.size += int64(n)
	f.modTime = time.Now()

	return nil
}

// execEntry executes a log entry against the in-memory index.
// This is done after appending and on replay of the log.
func (f *LogFile) execEntry(e *LogEntry) {
	switch e.Flag {
	case LogEntryMeasurementTombstoneFlag:
		f.execDeleteMeasurementEntry(e)
	case LogEntryTagKeyTombstoneFlag:
		f.execDeleteTagKeyEntry(e)
	case LogEntryTagValueTombstoneFlag:
		f.execDeleteTagValueEntry(e)
	default:
		f.execSeriesEntry(e)
	}
}

func (f *LogFile) execDeleteMeasurementEntry(e *LogEntry) {
	mm := f.createMeasurementIfNotExists(e.Name)
	mm.deleted = true
	mm.tagSet = make(map[string]logTagKey)
	mm.series = make(map[uint64]struct{})
	mm.seriesSet = nil
}

func (f *LogFile) execDeleteTagKeyEntry(e *LogEntry) {
	mm := f.createMeasurementIfNotExists(e.Name)
	ts := mm.createTagSetIfNotExists(e.Key)

	ts.deleted = true

	mm.tagSet[string(e.Key)] = ts
}

func (f *LogFile) execDeleteTagValueEntry(e *LogEntry) {
	mm := f.createMeasurementIfNotExists(e.Name)
	ts := mm.createTagSetIfNotExists(e.Key)
	tv := ts.createTagValueIfNotExists(e.Value)

	tv.deleted = true

	ts.tagValues[string(e.Value)] = tv
	mm.tagSet[string(e.Key)] = ts
}

func (f *LogFile) execSeriesEntry(e *LogEntry) {
	var seriesKey []byte
	if e.cached {
		sz := tsdb.SeriesKeySize(e.name, e.tags)
		if len(f.keyBuf) < sz {
			f.keyBuf = make([]byte, 0, sz)
		}
		seriesKey = tsdb.AppendSeriesKey(f.keyBuf[:0], e.name, e.tags)
	} else {
		seriesKey = f.sfile.SeriesKey(e.SeriesID)
	}

	// Series keys can be removed if the series has been deleted from
	// the entire database and the server is restarted. This would cause
	// the log to replay its insert but the key cannot be found.
	//
	// https://github.com/influxdata/influxdb/issues/9444
	if seriesKey == nil {
		return
	}

	// Check if deleted.
	deleted := e.Flag == LogEntrySeriesTombstoneFlag

	// Read key size.
	_, remainder := tsdb.ReadSeriesKeyLen(seriesKey)
	if len(remainder) == 0 {
		return
	}

	// Read measurement name.
	name, remainder := tsdb.ReadSeriesKeyMeasurement(remainder)
	mm := f.createMeasurementIfNotExists(name)
	mm.deleted = false
	if !deleted {
		mm.addSeriesID(e.SeriesID)
	} else {
		mm.removeSeriesID(e.SeriesID)
	}

	// Read tag count.
	tagN, remainder := tsdb.ReadSeriesKeyTagN(remainder)

	// Save tags.
	var k, v []byte
	for i := 0; i < tagN; i++ {
		k, v, remainder = tsdb.ReadSeriesKeyTag(remainder)
		ts := mm.createTagSetIfNotExists(k)
		tv := ts.createTagValueIfNotExists(v)

		// Add/remove a reference to the series on the tag value.
		if !deleted {
			tv.addSeriesID(e.SeriesID)
		} else {
			tv.removeSeriesID(e.SeriesID)
		}

		ts.tagValues[string(v)] = tv
		mm.tagSet[string(k)] = ts
	}

	// Add/remove from appropriate series id sets.
	if !deleted {
		f.seriesIDSet.Add(e.SeriesID)
		f.tombstoneSeriesIDSet.Remove(e.SeriesID)
	} else {
		f.seriesIDSet.Remove(e.SeriesID)
		f.tombstoneSeriesIDSet.Add(e.SeriesID)
	}
}

// SeriesIDIterator returns an iterator over all series in the log file.
func (f *LogFile) SeriesIDIterator() tsdb.SeriesIDIterator {
	f.mu.RLock()
	defer f.mu.RUnlock()

	ss := tsdb.NewSeriesIDSet()
	allSeriesSets := make([]*tsdb.SeriesIDSet, 0, len(f.mms))

	for _, mm := range f.mms {
		if mm.seriesSet != nil {
			allSeriesSets = append(allSeriesSets, mm.seriesSet)
			continue
		}

		// measurement is not using seriesSet to store series IDs.
		mm.forEach(func(seriesID uint64) {
			ss.AddNoLock(seriesID)
		})
	}

	// Fast merge all seriesSets.
	if len(allSeriesSets) > 0 {
		ss.Merge(allSeriesSets...)
	}

	return tsdb.NewSeriesIDSetIterator(ss)
}

// createMeasurementIfNotExists returns a measurement by name.
func (f *LogFile) createMeasurementIfNotExists(name []byte) *logMeasurement {
	mm := f.mms[string(name)]
	if mm == nil {
		mm = &logMeasurement{
			name:   name,
			tagSet: make(map[string]logTagKey),
			series: make(map[uint64]struct{}),
		}
		f.mms[string(name)] = mm
	}
	return mm
}

// MeasurementIterator returns an iterator over all the measurements in the file.
func (f *LogFile) MeasurementIterator() MeasurementIterator {
	f.mu.RLock()
	defer f.mu.RUnlock()

	var itr logMeasurementIterator
	for _, mm := range f.mms {
		itr.mms = append(itr.mms, *mm)
	}
	sort.Sort(logMeasurementSlice(itr.mms))
	return &itr
}

// MeasurementSeriesIDIterator returns an iterator over all series for a measurement.
func (f *LogFile) MeasurementSeriesIDIterator(name []byte) tsdb.SeriesIDIterator {
	f.mu.RLock()
	defer f.mu.RUnlock()

	mm := f.mms[string(name)]
	if mm == nil || mm.cardinality() == 0 {
		return nil
	}
	return tsdb.NewSeriesIDSetIterator(mm.seriesIDSet())
}

// CompactTo compacts the log file and writes it to w.
func (f *LogFile) CompactTo(w io.Writer, m, k uint64, cancel <-chan struct{}) (n int64, err error) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	// Check for cancellation.
	select {
	case <-cancel:
		return n, ErrCompactionInterrupted
	default:
	}

	// Wrap in bufferred writer with a buffer equivalent to the LogFile size.
	bw := bufio.NewWriterSize(w, indexFileBufferSize) // 128K

	// Setup compaction offset tracking data.
	var t IndexFileTrailer
	info := newLogFileCompactInfo()
	info.cancel = cancel

	// Write magic number.
	if err := writeTo(bw, []byte(FileSignature), &n); err != nil {
		return n, err
	}

	// Retreve measurement names in order.
	names := f.measurementNames()

	// Flush buffer & mmap series block.
	if err := bw.Flush(); err != nil {
		return n, err
	}

	// Write tagset blocks in measurement order.
	if err := f.writeTagsetsTo(bw, names, info, &n); err != nil {
		return n, err
	}

	// Write measurement block.
	t.MeasurementBlock.Offset = n
	if err := f.writeMeasurementBlockTo(bw, names, info, &n); err != nil {
		return n, err
	}
	t.MeasurementBlock.Size = n - t.MeasurementBlock.Offset

	// Write series set.
	t.SeriesIDSet.Offset = n
	nn, err := f.seriesIDSet.WriteTo(bw)
	if n += nn; err != nil {
		return n, err
	}
	t.SeriesIDSet.Size = n - t.SeriesIDSet.Offset

	// Write tombstone series set.
	t.TombstoneSeriesIDSet.Offset = n
	nn, err = f.tombstoneSeriesIDSet.WriteTo(bw)
	if n += nn; err != nil {
		return n, err
	}
	t.TombstoneSeriesIDSet.Size = n - t.TombstoneSeriesIDSet.Offset

	// Build series sketches.
	sSketch, sTSketch, err := f.seriesSketches()
	if err != nil {
		return n, err
	}

	// Write series sketches.
	t.SeriesSketch.Offset = n
	data, err := sSketch.MarshalBinary()
	if err != nil {
		return n, err
	} else if _, err := bw.Write(data); err != nil {
		return n, err
	}
	t.SeriesSketch.Size = int64(len(data))
	n += t.SeriesSketch.Size

	t.TombstoneSeriesSketch.Offset = n
	if data, err = sTSketch.MarshalBinary(); err != nil {
		return n, err
	} else if _, err := bw.Write(data); err != nil {
		return n, err
	}
	t.TombstoneSeriesSketch.Size = int64(len(data))
	n += t.TombstoneSeriesSketch.Size

	// Write trailer.
	nn, err = t.WriteTo(bw)
	n += nn
	if err != nil {
		return n, err
	}

	// Flush buffer.
	if err := bw.Flush(); err != nil {
		return n, err
	}

	return n, nil
}

func (f *LogFile) writeTagsetsTo(w io.Writer, names []string, info *logFileCompactInfo, n *int64) error {
	for _, name := range names {
		if err := f.writeTagsetTo(w, name, info, n); err != nil {
			return err
		}
	}
	return nil
}

// writeTagsetTo writes a single tagset to w and saves the tagset offset.
func (f *LogFile) writeTagsetTo(w io.Writer, name string, info *logFileCompactInfo, n *int64) error {
	mm := f.mms[name]

	// Check for cancellation.
	select {
	case <-info.cancel:
		return ErrCompactionInterrupted
	default:
	}

	enc := NewTagBlockEncoder(w)
	var valueN int
	for _, k := range mm.keys() {
		tag := mm.tagSet[k]

		// Encode tag. Skip values if tag is deleted.
		if err := enc.EncodeKey(tag.name, tag.deleted); err != nil {
			return err
		} else if tag.deleted {
			continue
		}

		// Sort tag values.
		values := make([]string, 0, len(tag.tagValues))
		for v := range tag.tagValues {
			values = append(values, v)
		}
		sort.Strings(values)

		// Add each value.
		for _, v := range values {
			value := tag.tagValues[v]
			if err := enc.EncodeValue(value.name, value.deleted, value.seriesIDSet()); err != nil {
				return err
			}

			// Check for cancellation periodically.
			if valueN++; valueN%1000 == 0 {
				select {
				case <-info.cancel:
					return ErrCompactionInterrupted
				default:
				}
			}
		}
	}

	// Save tagset offset to measurement.
	offset := *n

	// Flush tag block.
	err := enc.Close()
	*n += enc.N()
	if err != nil {
		return err
	}

	// Save tagset offset to measurement.
	size := *n - offset

	info.mms[name] = &logFileMeasurementCompactInfo{offset: offset, size: size}

	return nil
}

func (f *LogFile) writeMeasurementBlockTo(w io.Writer, names []string, info *logFileCompactInfo, n *int64) error {
	mw := NewMeasurementBlockWriter()

	// Check for cancellation.
	select {
	case <-info.cancel:
		return ErrCompactionInterrupted
	default:
	}

	// Add measurement data.
	for _, name := range names {
		mm := f.mms[name]
		mmInfo := info.mms[name]
		assert(mmInfo != nil, "measurement info not found")
		mw.Add(mm.name, mm.deleted, mmInfo.offset, mmInfo.size, mm.seriesIDs())
	}

	// Flush data to writer.
	nn, err := mw.WriteTo(w)
	*n += nn
	return err
}

// logFileCompactInfo is a context object to track compaction position info.
type logFileCompactInfo struct {
	cancel <-chan struct{}
	mms    map[string]*logFileMeasurementCompactInfo
}

// newLogFileCompactInfo returns a new instance of logFileCompactInfo.
func newLogFileCompactInfo() *logFileCompactInfo {
	return &logFileCompactInfo{
		mms: make(map[string]*logFileMeasurementCompactInfo),
	}
}

type logFileMeasurementCompactInfo struct {
	offset int64
	size   int64
}

// MeasurementsSketches returns sketches for existing and tombstoned measurement names.
func (f *LogFile) MeasurementsSketches() (sketch, tSketch estimator.Sketch, err error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.measurementsSketches()
}

func (f *LogFile) measurementsSketches() (sketch, tSketch estimator.Sketch, err error) {
	sketch, tSketch = hll.NewDefaultPlus(), hll.NewDefaultPlus()
	for _, mm := range f.mms {
		if mm.deleted {
			tSketch.Add(mm.name)
		} else {
			sketch.Add(mm.name)
		}
	}
	return sketch, tSketch, nil
}

// SeriesSketches returns sketches for existing and tombstoned series.
func (f *LogFile) SeriesSketches() (sketch, tSketch estimator.Sketch, err error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.seriesSketches()
}

func (f *LogFile) seriesSketches() (sketch, tSketch estimator.Sketch, err error) {
	sketch = hll.NewDefaultPlus()
	f.seriesIDSet.ForEach(func(id uint64) {
		name, keys := f.sfile.Series(id)
		sketch.Add(models.MakeKey(name, keys))
	})

	tSketch = hll.NewDefaultPlus()
	f.tombstoneSeriesIDSet.ForEach(func(id uint64) {
		name, keys := f.sfile.Series(id)
		tSketch.Add(models.MakeKey(name, keys))
	})
	return sketch, tSketch, nil
}

func (f *LogFile) Writes(entries []LogEntry) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	for i := range entries {
		entry := &entries[i]
		if err := f.appendEntry(entry); err != nil {
			return err
		}
		f.execEntry(entry)
	}
	// Flush buffer and sync to disk.
	return f.FlushAndSync()
}

// LogEntry represents a single log entry in the write-ahead log.
type LogEntry struct {
	Flag     byte   // flag
	SeriesID uint64 // series id
	Name     []byte // measurement name
	Key      []byte // tag key
	Value    []byte // tag value
	Checksum uint32 // checksum of flag/name/tags.
	Size     int    // total size of record, in bytes.

	cached   bool        // Hint to LogFile that series data is already parsed
	name     []byte      // series naem, this is a cached copy of the parsed measurement name
	tags     models.Tags // series tags, this is a cached copied of the parsed tags
	batchidx int         // position of entry in batch.
}

// UnmarshalBinary unmarshals data into e.
func (e *LogEntry) UnmarshalBinary(data []byte) error {
	var sz uint64
	var n int
	var seriesID uint64
	var err error

	orig := data
	start := len(data)

	// Parse flag data.
	if len(data) < 1 {
		return io.ErrShortBuffer
	}
	e.Flag, data = data[0], data[1:]

	// Parse series id.
	if seriesID, n, err = uvarint(data); err != nil {
		return err
	}
	e.SeriesID, data = seriesID, data[n:]

	// Parse name length.
	if sz, n, err = uvarint(data); err != nil {
		return err
	}

	// Read name data.
	if len(data) < n+int(sz) {
		return io.ErrShortBuffer
	}
	e.Name, data = data[n:n+int(sz)], data[n+int(sz):]

	// Parse key length.
	if sz, n, err = uvarint(data); err != nil {
		return err
	}

	// Read key data.
	if len(data) < n+int(sz) {
		return io.ErrShortBuffer
	}
	e.Key, data = data[n:n+int(sz)], data[n+int(sz):]

	// Parse value length.
	if sz, n, err = uvarint(data); err != nil {
		return err
	}

	// Read value data.
	if len(data) < n+int(sz) {
		return io.ErrShortBuffer
	}
	e.Value, data = data[n:n+int(sz)], data[n+int(sz):]

	// Compute checksum.
	chk := crc32.ChecksumIEEE(orig[:start-len(data)])

	// Parse checksum.
	if len(data) < 4 {
		return io.ErrShortBuffer
	}
	e.Checksum, data = binary.BigEndian.Uint32(data[:4]), data[4:]

	// Verify checksum.
	if chk != e.Checksum {
		return ErrLogEntryChecksumMismatch
	}

	// Save length of elem.
	e.Size = start - len(data)

	return nil
}

// appendLogEntry appends to dst and returns the new buffer.
// This updates the checksum on the entry.
func appendLogEntry(dst []byte, e *LogEntry) []byte {
	var buf [binary.MaxVarintLen64]byte
	start := len(dst)

	// Append flag.
	dst = append(dst, e.Flag)

	// Append series id.
	n := binary.PutUvarint(buf[:], uint64(e.SeriesID))
	dst = append(dst, buf[:n]...)

	// Append name.
	n = binary.PutUvarint(buf[:], uint64(len(e.Name)))
	dst = append(dst, buf[:n]...)
	dst = append(dst, e.Name...)

	// Append key.
	n = binary.PutUvarint(buf[:], uint64(len(e.Key)))
	dst = append(dst, buf[:n]...)
	dst = append(dst, e.Key...)

	// Append value.
	n = binary.PutUvarint(buf[:], uint64(len(e.Value)))
	dst = append(dst, buf[:n]...)
	dst = append(dst, e.Value...)

	// Calculate checksum.
	e.Checksum = crc32.ChecksumIEEE(dst[start:])

	// Append checksum.
	binary.BigEndian.PutUint32(buf[:4], e.Checksum)
	dst = append(dst, buf[:4]...)

	return dst
}

// logMeasurements represents a map of measurement names to measurements.
type logMeasurements map[string]*logMeasurement

// bytes estimates the memory footprint of this logMeasurements, in bytes.
func (mms *logMeasurements) bytes() int {
	var b int
	for k, v := range *mms {
		b += len(k)
		b += v.bytes()
	}
	b += int(unsafe.Sizeof(*mms))
	return b
}

type logMeasurement struct {
	name      []byte
	tagSet    map[string]logTagKey
	deleted   bool
	series    map[uint64]struct{}
	seriesSet *tsdb.SeriesIDSet
}

// bytes estimates the memory footprint of this logMeasurement, in bytes.
func (m *logMeasurement) bytes() int {
	var b int
	b += len(m.name)
	for k, v := range m.tagSet {
		b += len(k)
		b += v.bytes()
	}
	b += (int(m.cardinality()) * 8)
	b += int(unsafe.Sizeof(*m))
	return b
}

func (m *logMeasurement) addSeriesID(x uint64) {
	if m.seriesSet != nil {
		m.seriesSet.AddNoLock(x)
		return
	}

	m.series[x] = struct{}{}

	// If the map is getting too big it can be converted into a roaring seriesSet.
	if len(m.series) > 25 {
		m.seriesSet = tsdb.NewSeriesIDSet()
		for id := range m.series {
			m.seriesSet.AddNoLock(id)
		}
		m.series = nil
	}
}

func (m *logMeasurement) removeSeriesID(x uint64) {
	if m.seriesSet != nil {
		m.seriesSet.RemoveNoLock(x)
		return
	}
	delete(m.series, x)
}

func (m *logMeasurement) cardinality() int64 {
	if m.seriesSet != nil {
		return int64(m.seriesSet.Cardinality())
	}
	return int64(len(m.series))
}

// forEach applies fn to every series ID in the logMeasurement.
func (m *logMeasurement) forEach(fn func(uint64)) {
	if m.seriesSet != nil {
		m.seriesSet.ForEachNoLock(fn)
		return
	}

	for seriesID := range m.series {
		fn(seriesID)
	}
}

// seriesIDs returns a sorted set of seriesIDs.
func (m *logMeasurement) seriesIDs() []uint64 {
	a := make([]uint64, 0, m.cardinality())
	if m.seriesSet != nil {
		m.seriesSet.ForEachNoLock(func(id uint64) { a = append(a, id) })
		return a // IDs are already sorted.
	}

	for seriesID := range m.series {
		a = append(a, seriesID)
	}
	sort.Sort(uint64Slice(a))
	return a
}

// seriesIDSet returns a copy of the logMeasurement's seriesSet, or creates a new
// one
func (m *logMeasurement) seriesIDSet() *tsdb.SeriesIDSet {
	if m.seriesSet != nil {
		return m.seriesSet.CloneNoLock()
	}

	ss := tsdb.NewSeriesIDSet()
	for seriesID := range m.series {
		ss.AddNoLock(seriesID)
	}
	return ss
}

func (m *logMeasurement) Name() []byte  { return m.name }
func (m *logMeasurement) Deleted() bool { return m.deleted }

func (m *logMeasurement) createTagSetIfNotExists(key []byte) logTagKey {
	ts, ok := m.tagSet[string(key)]
	if !ok {
		ts = logTagKey{name: key, tagValues: make(map[string]logTagValue)}
	}
	return ts
}

// keys returns a sorted list of tag keys.
func (m *logMeasurement) keys() []string {
	a := make([]string, 0, len(m.tagSet))
	for k := range m.tagSet {
		a = append(a, k)
	}
	sort.Strings(a)
	return a
}

// logMeasurementSlice is a sortable list of log measurements.
type logMeasurementSlice []logMeasurement

func (a logMeasurementSlice) Len() int           { return len(a) }
func (a logMeasurementSlice) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a logMeasurementSlice) Less(i, j int) bool { return bytes.Compare(a[i].name, a[j].name) == -1 }

// logMeasurementIterator represents an iterator over a slice of measurements.
type logMeasurementIterator struct {
	mms []logMeasurement
}

// Next returns the next element in the iterator.
func (itr *logMeasurementIterator) Next() (e MeasurementElem) {
	if len(itr.mms) == 0 {
		return nil
	}
	e, itr.mms = &itr.mms[0], itr.mms[1:]
	return e
}

type logTagKey struct {
	name      []byte
	deleted   bool
	tagValues map[string]logTagValue
}

// bytes estimates the memory footprint of this logTagKey, in bytes.
func (tk *logTagKey) bytes() int {
	var b int
	b += len(tk.name)
	for k, v := range tk.tagValues {
		b += len(k)
		b += v.bytes()
	}
	b += int(unsafe.Sizeof(*tk))
	return b
}

func (tk *logTagKey) Key() []byte   { return tk.name }
func (tk *logTagKey) Deleted() bool { return tk.deleted }

func (tk *logTagKey) TagValueIterator() TagValueIterator {
	a := make([]logTagValue, 0, len(tk.tagValues))
	for _, v := range tk.tagValues {
		a = append(a, v)
	}
	return newLogTagValueIterator(a)
}

func (tk *logTagKey) createTagValueIfNotExists(value []byte) logTagValue {
	tv, ok := tk.tagValues[string(value)]
	if !ok {
		tv = logTagValue{name: value, series: make(map[uint64]struct{})}
	}
	return tv
}

// logTagKey is a sortable list of log tag keys.
type logTagKeySlice []logTagKey

func (a logTagKeySlice) Len() int           { return len(a) }
func (a logTagKeySlice) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a logTagKeySlice) Less(i, j int) bool { return bytes.Compare(a[i].name, a[j].name) == -1 }

type logTagValue struct {
	name      []byte
	deleted   bool
	series    map[uint64]struct{}
	seriesSet *tsdb.SeriesIDSet
}

// bytes estimates the memory footprint of this logTagValue, in bytes.
func (tv *logTagValue) bytes() int {
	var b int
	b += len(tv.name)
	b += int(unsafe.Sizeof(*tv))
	b += (int(tv.cardinality()) * 8)
	return b
}

func (tv *logTagValue) addSeriesID(x uint64) {
	if tv.seriesSet != nil {
		tv.seriesSet.AddNoLock(x)
		return
	}

	tv.series[x] = struct{}{}

	// If the map is getting too big it can be converted into a roaring seriesSet.
	if len(tv.series) > 25 {
		tv.seriesSet = tsdb.NewSeriesIDSet()
		for id := range tv.series {
			tv.seriesSet.AddNoLock(id)
		}
		tv.series = nil
	}
}

func (tv *logTagValue) removeSeriesID(x uint64) {
	if tv.seriesSet != nil {
		tv.seriesSet.RemoveNoLock(x)
		return
	}
	delete(tv.series, x)
}

func (tv *logTagValue) cardinality() int64 {
	if tv.seriesSet != nil {
		return int64(tv.seriesSet.Cardinality())
	}
	return int64(len(tv.series))
}

// seriesIDSet returns a copy of the logMeasurement's seriesSet, or creates a new
// one
func (tv *logTagValue) seriesIDSet() *tsdb.SeriesIDSet {
	if tv.seriesSet != nil {
		return tv.seriesSet.CloneNoLock()
	}

	ss := tsdb.NewSeriesIDSet()
	for seriesID := range tv.series {
		ss.AddNoLock(seriesID)
	}
	return ss
}

func (tv *logTagValue) Value() []byte { return tv.name }
func (tv *logTagValue) Deleted() bool { return tv.deleted }

// logTagValue is a sortable list of log tag values.
type logTagValueSlice []logTagValue

func (a logTagValueSlice) Len() int           { return len(a) }
func (a logTagValueSlice) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a logTagValueSlice) Less(i, j int) bool { return bytes.Compare(a[i].name, a[j].name) == -1 }

// logTagKeyIterator represents an iterator over a slice of tag keys.
type logTagKeyIterator struct {
	a []logTagKey
}

// newLogTagKeyIterator returns a new instance of logTagKeyIterator.
func newLogTagKeyIterator(a []logTagKey) *logTagKeyIterator {
	sort.Sort(logTagKeySlice(a))
	return &logTagKeyIterator{a: a}
}

// Next returns the next element in the iterator.
func (itr *logTagKeyIterator) Next() (e TagKeyElem) {
	if len(itr.a) == 0 {
		return nil
	}
	e, itr.a = &itr.a[0], itr.a[1:]
	return e
}

// logTagValueIterator represents an iterator over a slice of tag values.
type logTagValueIterator struct {
	a []logTagValue
}

// newLogTagValueIterator returns a new instance of logTagValueIterator.
func newLogTagValueIterator(a []logTagValue) *logTagValueIterator {
	sort.Sort(logTagValueSlice(a))
	return &logTagValueIterator{a: a}
}

// Next returns the next element in the iterator.
func (itr *logTagValueIterator) Next() (e TagValueElem) {
	if len(itr.a) == 0 {
		return nil
	}
	e, itr.a = &itr.a[0], itr.a[1:]
	return e
}

// FormatLogFileName generates a log filename for the given index.
func FormatLogFileName(id int) string {
	return fmt.Sprintf("L0-%08d%s", id, LogFileExt)
}
