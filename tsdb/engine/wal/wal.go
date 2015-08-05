package wal

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/golang/snappy"
	"github.com/influxdb/influxdb/tsdb"
)

const (
	DefaultSegmentSize         = 2 * 1024 * 1024 // 2MB, default size of a segment file in a partitions
	DefaultReadySeriesSize     = 32 * 1024       // 32KB, series is ready to flush once it reaches this size
	DefaultCompactionThreshold = 0.5             // flush and compact a partition once this ratio of keys are over the flush size
	DefaultMaxSeriesSize       = 2 * 1024 * 1024 // if any key in a WAL partition reaches this size, we'll force compaction of the partition

	// if there have been no writes to a partition in this amount of time, we'll force a full flush
	DefaultFlushInterval = 30 * time.Minute

	// When a partition gets to this size in memory, we should slow down writes until it gets a chance to compact.
	// This will force clients to get backpressure if they're writing too fast. We need this because the WAL can
	// take writes much faster than the index. So eventually we'll need to create backpressure, otherwise we'll fill
	// up the memory and die. This number multiplied by the parition count is roughly the max possible memory size for the in-memory WAL cache.
	DefaultPartitionSizeThreshold = 200 * 1024 * 1024

	DefaultPartitionCount = 10

	// the file extension we expect for wal segments
	FileExtension = "wal"

	// flushes are triggered automatically by different criteria. check on this interval
	flushCheckInterval = 500 * time.Millisecond
)

var (
	// ErrCompactionRunning to return if we attempt to run a compaction on a partition that is currently running one
	ErrCompactionRunning = errors.New("compaction running")

	// ErrCompactionBlock gets returned if we're reading a compressed block and its a file compaction description
	ErrCompactionBlock = errors.New("compaction description")

	// within a segment file, it could be a compaction file, which has parts in it that
	// indicate a file name that was compacted. We use this sequence to identify those sections
	CompactSequence = []byte{0xFF, 0xFF}
)

type Log struct {
	path           string
	walSegmentSize int
	flushSize      int

	flush           chan int    // signals a background flush on the given partition
	flushCheckTimer *time.Timer // check this often to see if a background flush should happen

	// These coordinate closing and waiting for running goroutines.
	wg      sync.WaitGroup
	closing chan struct{}

	// Used for out-of-band error messages.
	logger *log.Logger

	mu         sync.RWMutex
	partitions map[uint8]*partition

	// The writer used by the logger.
	LogOutput     io.Writer
	FlushInterval time.Duration
	SegmentSize   int

	// Settings that control when a partition should get flushed to index and compacted
	// flush if any series in the partition has exceeded this size threshold
	MaxSeriesSize int
	// a series is ready to flush once it has this much data in it
	ReadySeriesSize int
	// a partition is ready to flush if this percentage of series has hit the readySeriesSize or greater
	CompactionThreshold float64
	// a partition should be flushed if it has reached this size in memory
	PartitionSizeThreshold uint64
	// the number of separate partitions to create for the WAL. Compactions happen per partition. So this number
	// will affect what percentage of the WAL gets compacted at a time. For instance, a setting of 10 means
	// we generally will be compacting about 10% of the WAL at a time.
	PartitionCount uint64

	IndexWriter indexWriter
}

// indexWriter is an interface for the indexed database the WAL flushes data to
type indexWriter interface {
	// time ascending points where each byte array is:
	//   int64 time
	//   data
	WriteIndex(pointsByKey map[string][][]byte) error
}

func NewLog(path string) *Log {
	return &Log{
		path:  path,
		flush: make(chan int, 1),

		// these options should be overriden by any options in the config
		LogOutput:              os.Stderr,
		FlushInterval:          DefaultFlushInterval,
		SegmentSize:            DefaultSegmentSize,
		MaxSeriesSize:          DefaultMaxSeriesSize,
		CompactionThreshold:    DefaultCompactionThreshold,
		PartitionSizeThreshold: DefaultPartitionSizeThreshold,
		ReadySeriesSize:        DefaultReadySeriesSize,
		PartitionCount:         DefaultPartitionCount,
	}
}

// Open opens and initializes the Log. Will recover from previous unclosed shutdowns
func (w *Log) Open() error {
	// open the partitions
	w.partitions = make(map[uint8]*partition)
	for i := uint64(1); i <= w.PartitionCount; i++ {
		p, err := newPartition(uint8(i), w.path, w.SegmentSize)
		if err != nil {
			return err
		}
		w.partitions[uint8(i)] = p
	}
	if err := w.openPartitionFiles(); err != nil {
		return err
	}

	w.logger = log.New(w.LogOutput, "[wal] ", log.LstdFlags)

	w.flushCheckTimer = time.NewTimer(flushCheckInterval)

	// Start background goroutines.
	w.wg = *&sync.WaitGroup{}
	w.wg.Add(1)
	w.closing = make(chan struct{})
	go w.autoflusher(w.closing)

	return nil
}

// Cursor will return a cursor object to Seek and iterate with Next for the WAL cache for the given
func (w *Log) Cursor(key string) tsdb.Cursor {
	w.mu.RLock()
	defer w.mu.RUnlock()

	p := w.walPartition([]byte(key))

	return p.cursor(key)
}

// partition is a set of files for a partition of the WAL. We use multiple partitions so when compactions occur
// only a portion of the WAL must be flushed and compacted
type partition struct {
	id                 uint8
	path               string
	mu                 sync.Mutex
	currentSegmentFile *os.File
	currentSegmentSize int
	currentSegmentID   uint32
	lastFileID         uint32
	totalSize          uint64
	lastWrite          time.Time
	maxSegmentSize     int
	cache              map[string][][]byte
	// this cache is a temporary placeholder to keep data while its being flushed
	// and compacted. It's for cursors to combine the cache and this if a flush is occuring
	flushCache        map[string][][]byte
	cacheDirtySort    map[string]bool // will be true if the key needs to be sorted
	cacheSizes        map[string]int
	compactionRunning bool
}

func newPartition(id uint8, path string, segmentSize int) (*partition, error) {
	return &partition{
		id:             id,
		path:           path,
		maxSegmentSize: segmentSize,
		lastWrite:      time.Now(),
		cache:          make(map[string][][]byte),
		cacheDirtySort: make(map[string]bool),
		cacheSizes:     make(map[string]int),
	}, nil
}

// Close resets the caches and closes the currently open segment file
func (p *partition) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.cache = nil
	p.cacheDirtySort = nil
	p.cacheSizes = nil
	if err := p.currentSegmentFile.Close(); err != nil {
		return err
	}

	return nil
}

// write will write a compressed block of the points to the current segment file. If the segment
// file is larger than the max size, it will roll over to a new file before performing the write.
// This method will also add the points to the in memory cache
func (p *partition) write(points []tsdb.Point) error {
	block := make([]byte, 0)
	for _, pp := range points {
		block = append(block, marshalWALEntry(pp.Key(), pp.UnixNano(), pp.Data())...)
	}
	b := snappy.Encode(nil, block)

	p.mu.Lock()
	defer p.mu.Unlock()

	// rotate to a new file if we've gone over our limit
	if p.currentSegmentFile == nil || p.currentSegmentSize > p.maxSegmentSize {
		err := p.newSegmentFile()
		if err != nil {
			return err
		}
	}

	if n, err := p.currentSegmentFile.Write(u64tob(uint64(len(b)))); err != nil {
		return err
	} else if n != 8 {
		return fmt.Errorf("expected to write %d bytes but wrote %d", 8, n)
	}

	if n, err := p.currentSegmentFile.Write(b); err != nil {
		return err
	} else if n != len(b) {
		return fmt.Errorf("expected to write %d bytes but wrote %d", len(b), n)
	}

	if err := p.currentSegmentFile.Sync(); err != nil {
		return err
	}

	p.currentSegmentSize += (8 + len(b))
	p.lastWrite = time.Now()

	for _, pp := range points {
		p.addToCache(pp.Key(), pp.Data(), pp.UnixNano())
	}
	return nil
}

// newSegmentFile will close the current segment file and open a new one, updating bookkeeping info on the partition
func (p *partition) newSegmentFile() error {
	p.currentSegmentID += 1
	if p.currentSegmentFile != nil {
		if err := p.currentSegmentFile.Close(); err != nil {
			return err
		}
	}

	fileName := p.fileNameForSegment(p.currentSegmentID)

	ff, err := os.OpenFile(fileName, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return err
	}
	p.currentSegmentFile = ff
	return nil
}

// fileNameForSegment will return the full path and filename for a given segment ID
func (p *partition) fileNameForSegment(id uint32) string {
	return filepath.Join(p.path, fmt.Sprintf("%02d.%06d.%s", p.id, id, FileExtension))
}

// compactionFileName will return the file name that should be used for a conmpaction onthis partition
func (p *partition) compactionFileName() string {
	return filepath.Join(p.path, fmt.Sprintf("%02d.%06d.CPT", p.id, 1))
}

// fileIDFromName will return the segment ID from the file name
func (p *partition) fileIDFromName(name string) (uint32, error) {
	parts := strings.Split(filepath.Base(name), ".")
	if len(parts) != 3 {
		return 0, fmt.Errorf("file name doesn't follow wal format: %s", name)
	}
	id, err := strconv.ParseUint(parts[1], 10, 32)
	if err != nil {
		return 0, err
	}
	return uint32(id), nil
}

// shouldFlush returns true if a partition should be flushed. The criteria are
// maxSeriesSize - flush if any series in the partition has exceeded this size threshold
// readySeriesSize - a series is ready to flush once it has this much data in it
// compactionThreshold - a partition is ready to flush if this percentage of series has hit the readySeriesSize or greater
// partitionSizeThreshold - a partition should be flushed if it has reached this size in memory
func (p *partition) shouldFlush(maxSeriesSize, readySeriesSize int, compactionThreshold float64, partitionSizeThreshold uint64) bool {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.totalSize > partitionSizeThreshold {
		return true
	}

	countReady := float64(0)
	for _, s := range p.cacheSizes {
		if s > maxSeriesSize {
			return true
		} else if s > readySeriesSize {
			countReady += 1
		}
	}

	if countReady/float64(len(p.cacheSizes)) > compactionThreshold {
		return true
	}

	return false
}

// flushAndCompact will flush any series that are over their threshold and then read in all old segment files and
// write the data that was not flushed to a new file
func (p *partition) flushAndCompact(idx indexWriter, maxSeriesSize, readySeriesSize int) error {
	seriesToFlush := make(map[string][][]byte)
	sizeOfFlush := 0
	var compactFilesLessThan uint32

	if err := func() error {
		p.mu.Lock()
		defer p.mu.Unlock()

		if p.compactionRunning {
			return ErrCompactionRunning
		}
		p.compactionRunning = true

		for k, s := range p.cacheSizes {
			// if the series is over the threshold, save it in the map to flush later
			if s >= maxSeriesSize || s >= readySeriesSize {
				sizeOfFlush += s
				seriesToFlush[k] = p.cache[k]
				delete(p.cacheSizes, k)
				delete(p.cache, k)

				// always hand the index data that is sorted
				if p.cacheDirtySort[k] {
					sort.Sort(byteSlices(seriesToFlush[k]))
					delete(p.cacheDirtySort, k)
				}
			}
		}
		p.flushCache = seriesToFlush

		// roll over a new segment file so we can compact all the old ones
		if err := p.newSegmentFile(); err != nil {
			return err
		}
		compactFilesLessThan = p.currentSegmentID

		return nil
	}(); err == ErrCompactionRunning {
		return nil
	} else if err != nil {
		return err
	}

	// write the data to the index first
	if err := idx.WriteIndex(seriesToFlush); err != nil {
		// if we can't write the index, we should just bring down the server hard
		panic(fmt.Sprintf("error writing the wal to the index: %s", err.Error()))
	}

	// clear the flush cache
	p.mu.Lock()
	p.flushCache = nil
	p.mu.Unlock()

	// now compact all the old data
	fileNames, err := p.segmentFileNames()
	if err != nil {
		return err
	}

	// all compacted data from the segments will go into this file
	compactionFile, err := os.OpenFile(p.compactionFileName(), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return err
	}

	for _, n := range fileNames {
		id, err := p.idFromFileName(n)
		if err != nil {
			return err
		}

		// only compact files that are older than the segment that became active when we started the flush
		if id >= compactFilesLessThan {
			break
		}

		f, err := os.OpenFile(n, os.O_RDONLY, 0600)
		if err != nil {
			return err
		}

		sf := newSegmentFile(f)
		var entries []*entry
		for {
			a, err := sf.readCompressedBlock()

			if err == ErrCompactionBlock {
				continue
			} else if err != nil {
				return err
			}

			if a == nil {
				break
			}

			// only compact the entries from series that haven't been flushed
			for _, e := range a {
				if _, ok := seriesToFlush[string(e.key)]; !ok {
					entries = append(entries, e)
				}
			}
		}

		if err := p.writeCompactionEntry(compactionFile, entries); err != nil {
			return err
		}

		// now close and delete the file
		if err := f.Close(); err != nil {
			return err
		}
		os.Remove(n)
	}

	if err := compactionFile.Close(); err != nil {
		return err
	}

	// close the compaction file and rename it so that it will appear as the very first segment
	compactionFile.Close()
	os.Rename(compactionFile.Name(), p.fileNameForSegment(1))

	// and mark the compaction as done
	p.mu.Lock()
	p.compactionRunning = false
	p.mu.Unlock()

	return nil
}

// writeCompactionEntry will write a marker for the beginning of the file we're compacting, a compressed block
// for all entries, then a marker for the end of the file
func (p *partition) writeCompactionEntry(f *os.File, entries []*entry) error {
	if err := p.writeCompactionFileName(f); err != nil {
		return err
	}

	block := make([]byte, 0)
	for _, e := range entries {
		block = append(block, marshalWALEntry(e.key, e.timestamp, e.data)...)
	}
	b := snappy.Encode(nil, block)

	if n, err := f.Write(u64tob(uint64(len(b)))); err != nil {
		return err
	} else if n != 8 {
		return fmt.Errorf("compaction expected to write %d bytes but wrote %d", 8, n)
	}

	if n, err := f.Write(b); err != nil {
		return err
	} else if n != len(b) {
		return fmt.Errorf("compaction expected to write %d bytes but wrote %d", len(b), n)
	}

	if err := p.writeCompactionFileName(f); err != nil {
		return err
	}

	return f.Sync()
}

// writeCompactionFileName will write a compaction log length entry and the name of the file that is compacted
func (p *partition) writeCompactionFileName(f *os.File) error {
	name := []byte(f.Name())
	length := u64tob(uint64(len(name)))

	// the beginning of the length has two bytes to indicate that this is a compaction log entry
	length[0] = 0xFF
	length[1] = 0xFF

	if n, err := f.Write(length); err != nil {
		return err
	} else if n != 8 {
		return fmt.Errorf("compaction expected to write %d bytes but wrote %d", 8, n)
	}

	if n, err := f.Write(name); err != nil {
		return err
	} else if n != len(name) {
		return fmt.Errorf("compaction expected to write %d bytes but wrote %d", len(name), n)
	}

	return nil
}

// readFile will read a segment file and marshal its entries into the cache
func (p *partition) readFile(path string) (entries []*entry, err error) {
	id, err := p.fileIDFromName(path)
	if err != nil {
		return nil, err
	}

	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}

	sf := newSegmentFile(f)
	for {
		a, err := sf.readCompressedBlock()

		if err == ErrCompactionBlock {
			continue
		} else if err != nil {
			return nil, err
		} else if a == nil {
			break
		}

		entries = append(entries, a...)
	}

	// if this is the highest segment file, it'll be the one we use, otherwise close it out now that we're done reading
	if id > p.currentSegmentID {
		p.currentSegmentID = id
		p.currentSegmentFile = f
		p.currentSegmentSize = sf.size
	} else {
		if err := f.Close(); err != nil {
			return nil, err
		}
	}
	return
}

// addToCache will marshal the entry and add it to the in memory cache. It will also mark if this key will need sorting later
func (p *partition) addToCache(key, data []byte, timestamp int64) {
	// Generate in-memory cache entry of <timestamp,data>.
	v := MarshalEntry(timestamp, data)
	p.totalSize += uint64(len(v))
	// Determine if we'll need to sort the values for this key later
	a := p.cache[string(key)]
	needSort := !(len(a) == 0 || bytes.Compare(a[len(a)-1], v) == -1)
	p.cacheDirtySort[string(key)] = needSort

	// Append to cache list.
	p.cache[string(key)] = append(a, v)
	p.cacheSizes[string(key)] += len(v)
}

// cursor will combine the in memory cache and flush cache (if a flush is currently happening) to give a single ordered cursor for the key
func (p *partition) cursor(key string) *cursor {
	p.mu.Lock()
	defer p.mu.Unlock()

	cache := p.cache[key]
	if cache == nil {
		return &cursor{}
	}

	// if we're in the middle of a flush, combine the previous cache
	// with this one for the cursor
	if fc, ok := p.flushCache[key]; ok {
		c := make([][]byte, len(fc), len(fc)+len(cache))
		copy(c, fc)
		c = append(c, cache...)
		sort.Sort(byteSlices(c))
		return &cursor{cache: c, position: -1}
	}

	if p.cacheDirtySort[key] {
		sort.Sort(byteSlices(cache))
		delete(p.cacheDirtySort, key)
	}

	return &cursor{cache: cache, position: -1}
}

// idFromFileName parses the segment file ID from its name
func (p *partition) idFromFileName(name string) (uint32, error) {
	parts := strings.Split(name, ".")
	if len(parts) != 3 {
		return 0, fmt.Errorf("file %s has wrong name format to be a segment file", name)
	}

	id, err := strconv.ParseUint(parts[1], 10, 32)

	return uint32(id), err
}

// segmentFileNames returns all the segment files names for the partition
func (p *partition) segmentFileNames() ([]string, error) {
	path := filepath.Join(p.path, fmt.Sprintf("%02d.*.%s", p.id, FileExtension))
	fileNames, err := filepath.Glob(path)
	return fileNames, err
}

// segmentFile is a struct for reading in segment files from the WAL. Used on startup only while loading
type segmentFile struct {
	f      *os.File
	block  []byte
	length []byte
	size   int
}

func newSegmentFile(f *os.File) *segmentFile {
	return &segmentFile{
		length: make([]byte, 8),
		f:      f,
	}
}

// readCompressedBlock will read the next compressed block from the file and marshal the entries.
// if we've hit the end of the file or corruption the entry array will be nil
func (s *segmentFile) readCompressedBlock() (entries []*entry, err error) {
	n, err := s.f.Read(s.length)
	if err == io.EOF {
		return nil, nil
	} else if err != nil {
		return nil, err
	}
	s.size += n

	if n != len(s.length) {
		log.Println("unable to read the size of a data block from file: ", s.f.Name())
		// seek back before this length so we can start overwriting the file from here
		s.f.Seek(-int64(n), 1)
		return nil, nil
	}

	// Compacted WAL files will have a magic byte sequence that indicate the next part is a file name
	// instead of a compressed block. We can ignore these bytes and the ensuing file name to get to the next block.
	isCompactionFileNameBlock := false
	if bytes.Compare(s.length[0:2], CompactSequence) == 0 {
		s.length[0] = 0x00
		s.length[1] = 0x00
		isCompactionFileNameBlock = true
	}

	dataLength := btou64(s.length)

	// make sure we haven't hit the end of data. trailing end of file can be zero bytes
	if dataLength == 0 {
		s.f.Seek(-int64(len(s.length)), 1)
		return nil, nil
	}

	if len(s.block) < int(dataLength) {
		s.block = make([]byte, dataLength)
	}

	n, err = s.f.Read(s.block[:dataLength])
	if err != nil {
		return nil, err
	}
	s.size += n

	// read the compressed block and decompress it. if partial or corrupt,
	// overwrite with zeroes so we can start over on this wal file
	if n != int(dataLength) {
		log.Println("partial compressed block in file: ", s.f.Name())

		// seek back to before this block and its size so we can overwrite the corrupt data
		s.f.Seek(-int64(len(s.length)+n), 1)
		if err := s.zeroRestOfFile(); err != nil {
			return nil, err
		}

		return nil, nil
	}

	// skip the rest if this is just the filename from a compaction
	if isCompactionFileNameBlock {
		return nil, ErrCompactionBlock
	}

	buf, err := snappy.Decode(nil, s.block[:dataLength])

	// if there was an error decoding, this is a corrupt block so we zero out the rest of the file
	if err != nil {
		log.Println("corrupt compressed block in file: ", err.Error(), s.f.Name())

		// go back to the start of this block and zero out the rest of the file
		s.f.Seek(-int64(len(s.length)+n), 1)
		if err := s.zeroRestOfFile(); err != nil {
			return nil, err
		}

		return nil, nil
	}

	// read in the individual data points from the decompressed wal block
	bytesRead := 0
	for {
		if bytesRead >= len(buf) {
			break
		}
		n, key, timestamp, data := unmarshalWALEntry(buf[bytesRead:])
		bytesRead += n
		entries = append(entries, &entry{key: key, data: data, timestamp: timestamp})
	}

	return
}

// zeroRestOfFile will write zeroes for the rest of the segment file. This is used if for some reason a partial write occured.
// it basically resets the rest of the segment file so we can continue to use it
func (s *segmentFile) zeroRestOfFile() error {
	buf := make([]byte, 1024*512)
	bytesToOverwrite := int64(0)
	for {
		n, err := s.f.Read(buf)
		bytesToOverwrite += int64(n)
		if err != nil || n == 0 {
			break
		}
	}
	if _, err := s.f.Seek(-bytesToOverwrite, 1); err != nil {
		return err
	}
	for i := int64(0); i < bytesToOverwrite; i++ {
		if _, err := s.f.Write([]byte{0x00}); err != nil {
			return err
		}
	}
	if err := s.f.Sync(); err != nil {
		return err
	}
	_, err := s.f.Seek(-bytesToOverwrite, 1)
	return err
}

// entry is used as a temporary object when reading data from segment files
type entry struct {
	key       []byte
	data      []byte
	timestamp int64
}

// cursor is a forward cursor for a given entry in the cache
type cursor struct {
	cache    [][]byte
	position int
}

// Seek will point the cursor to the given time (or key)
func (c *cursor) Seek(seek []byte) (key, value []byte) {
	for i, p := range c.cache {
		if bytes.Compare(seek, p[0:8]) >= 0 {
			c.position = i
			return p[0:8], p[8:]
		}
	}
	return nil, nil
}

// Next moves the cursor to the next key/value. will return nil if at the end
func (c *cursor) Next() (key, value []byte) {
	pos := c.position + 1
	if pos < len(c.cache) {
		c.position = pos
		v := c.cache[c.position]
		return v[0:8], v[8:]
	}
	return nil, nil
}

func (w *Log) WritePoints(points []tsdb.Point) error {
	partitionsToWrite := w.pointsToPartitions(points)

	// get it to disk
	if err := func() error {
		w.mu.RLock()
		defer w.mu.RUnlock()

		for p, points := range partitionsToWrite {
			if err := p.write(points); err != nil {
				return err
			}
		}
		return nil
	}(); err != nil {
		return err
	}

	return nil
}

func (l *Log) Flush() error {
	return fmt.Errorf("explicit call to flush isn't implemented yet")
}

// pointsToPartitions returns a map that organizes the points into the partitions they should be mapped to
func (w *Log) pointsToPartitions(points []tsdb.Point) map[*partition][]tsdb.Point {
	m := make(map[*partition][]tsdb.Point)
	for _, p := range points {
		pp := w.walPartition(p.Key())
		m[pp] = append(m[pp], p)
	}
	return m
}

// openPartitionFiles will open all partitions and read their segment files
func (w *Log) openPartitionFiles() error {
	results := make(chan error, len(w.partitions))
	for _, p := range w.partitions {

		go func(p *partition) {
			fileNames, err := p.segmentFileNames()
			if err != nil {
				results <- err
				return
			}
			for _, n := range fileNames {
				entries, err := p.readFile(n)
				if err != nil {
					results <- err
					return
				}
				for _, e := range entries {
					p.addToCache(e.key, e.data, e.timestamp)
				}
			}
			results <- nil
		}(p)
	}

	for i := 0; i < len(w.partitions); i++ {
		err := <-results
		if err != nil {
			return err
		}
	}

	return nil
}

// Close will finish any flush that is currently in process and close file handles
func (w *Log) Close() error {
	// stop the autoflushing process so it doesn't try to kick another one off
	if w.closing != nil {
		close(w.closing)
		w.closing = nil
	}

	w.wg.Wait()

	w.mu.Lock()
	defer w.mu.Unlock()

	// clear the cache
	w.partitions = nil

	return w.close()
}

// close all the open Log partitions and file handles
func (w *Log) close() error {
	for _, p := range w.partitions {
		if err := p.Close(); err != nil {
			return err
		}
	}

	return nil
}

// triggerAutoFlush will flush and compact any partitions that have hit the thresholds for compaction
func (l *Log) triggerAutoFlush() {
	l.mu.RLock()
	defer l.mu.RUnlock()
	for _, p := range l.partitions {
		if p.shouldFlush(l.MaxSeriesSize, l.ReadySeriesSize, l.CompactionThreshold, l.PartitionSizeThreshold) {
			if err := p.flushAndCompact(l.IndexWriter, l.MaxSeriesSize, l.ReadySeriesSize); err != nil {
				l.logger.Printf("error flushing partition %d: %s\n", p.id, err)
			}
		}
	}
}

// autoflusher waits for notification of a flush and kicks it off in the background.
// This method runs in a separate goroutine.
func (w *Log) autoflusher(closing chan struct{}) {
	defer w.wg.Done()

	for {
		// Wait for close or flush signal.
		select {
		case <-closing:
			return
		case <-w.flushCheckTimer.C:
			w.triggerAutoFlush()
		case <-w.flush:
			if err := w.Flush(); err != nil {
				w.logger.Printf("flush error: %s", err)
			}
		}
	}
}

// walPartition returns the partition number that key belongs to.
func (l *Log) walPartition(key []byte) *partition {
	h := fnv.New64a()
	h.Write(key)
	id := uint8(h.Sum64()%l.PartitionCount + 1)
	p := l.partitions[id]
	if p == nil {
		if p, err := newPartition(id, l.path, l.SegmentSize); err != nil {
			panic(err)

		} else {
			l.partitions[id] = p
		}
	}
	return p
}

// marshalWALEntry encodes point data into a single byte slice.
//
// The format of the byte slice is:
//
//     uint64 timestamp
//     uint32 key length
//     uint32 data length
//     []byte key
//     []byte data
//
func marshalWALEntry(key []byte, timestamp int64, data []byte) []byte {
	v := make([]byte, 8+4+4, 8+4+4+len(key)+len(data))
	binary.BigEndian.PutUint64(v[0:8], uint64(timestamp))
	binary.BigEndian.PutUint32(v[8:12], uint32(len(key)))
	binary.BigEndian.PutUint32(v[12:16], uint32(len(data)))

	v = append(v, key...)
	v = append(v, data...)

	return v
}

// unmarshalWALEntry decodes a WAL entry into it's separate parts.
// Returned byte slices point to the original slice.
func unmarshalWALEntry(v []byte) (bytesRead int, key []byte, timestamp int64, data []byte) {
	timestamp = int64(binary.BigEndian.Uint64(v[0:8]))
	keyLen := binary.BigEndian.Uint32(v[8:12])
	dataLen := binary.BigEndian.Uint32(v[12:16])

	key = v[16 : 16+keyLen]
	data = v[16+keyLen : 16+keyLen+dataLen]
	bytesRead = 16 + int(keyLen) + int(dataLen)
	return
}

// marshalCacheEntry encodes the timestamp and data to a single byte slice.
//
// The format of the byte slice is:
//
//     uint64 timestamp
//     []byte data
//
func MarshalEntry(timestamp int64, data []byte) []byte {
	buf := make([]byte, 8+len(data))
	binary.BigEndian.PutUint64(buf[0:8], uint64(timestamp))
	copy(buf[8:], data)
	return buf
}

// unmarshalCacheEntry returns the timestamp and data from an encoded byte slice.
func UnmarshalEntry(buf []byte) (timestamp int64, data []byte) {
	timestamp = int64(binary.BigEndian.Uint64(buf[0:8]))
	data = buf[8:]
	return
}

// u64tob converts a uint64 into an 8-byte slice.
func u64tob(v uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, v)
	return b
}

func btou64(b []byte) uint64 {
	return binary.BigEndian.Uint64(b)
}

// byteSlices represents a sortable slice of byte slices.
type byteSlices [][]byte

func (a byteSlices) Len() int           { return len(a) }
func (a byteSlices) Less(i, j int) bool { return bytes.Compare(a[i], a[j]) == -1 }
func (a byteSlices) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
