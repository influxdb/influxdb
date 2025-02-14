package tsm1

// Compactions are the process of creating read-optimized TSM files.
// The files are created by converting write-optimized WAL entries
// to read-optimized TSM format.  They can also be created from existing
// TSM files when there are tombstone records that neeed to be removed, points
// that were overwritten by later writes and need to updated, or multiple
// smaller TSM files need to be merged to reduce file counts and improve
// compression ratios.
//
// The compaction process is stream-oriented using multiple readers and
// iterators.  The resulting stream is written sorted and chunked to allow for
// one-pass writing of a new TSM file.

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/influxdata/influxdb/pkg/limiter"
	"github.com/influxdata/influxdb/tsdb"
	"go.uber.org/zap"
)

const logEvery = 2 * DefaultSegmentSize

const (
	// CompactionTempExtension is the extension used for temporary files created during compaction.
	CompactionTempExtension = "tmp"

	// TSMFileExtension is the extension used for TSM files.
	TSMFileExtension = "tsm"

	// DefaultMaxSavedErrors is the number of errors that are stored by a TSMBatchKeyReader before
	// subsequent errors are discarded
	DefaultMaxSavedErrors = 100
)

var (
	errMaxFileExceeded     = fmt.Errorf("max file exceeded")
	errSnapshotsDisabled   = fmt.Errorf("snapshots disabled")
	errCompactionsDisabled = fmt.Errorf("compactions disabled")
)

type errCompactionInProgress struct {
	err error
}

// Error returns the string representation of the error, to satisfy the error interface.
func (e errCompactionInProgress) Error() string {
	if e.err != nil {
		return fmt.Sprintf("compaction in progress: %s", e.err)
	}
	return "compaction in progress"
}

func (e errCompactionInProgress) Unwrap() error {
	return e.err
}

type errCompactionAborted struct {
	err error
}

func (e errCompactionAborted) Error() string {
	if e.err != nil {
		return fmt.Sprintf("compaction aborted: %s", e.err)
	}
	return "compaction aborted"
}

type errBlockRead struct {
	file string
	err  error
}

func (e errBlockRead) Unwrap() error {
	return e.err
}

func (e errBlockRead) Is(target error) bool {
	_, ok := target.(errBlockRead)
	return ok
}

func (e errBlockRead) Error() string {
	if e.err != nil {
		return fmt.Sprintf("block read error on %s: %s", e.file, e.err)
	}
	return fmt.Sprintf("block read error on %s", e.file)
}

// CompactionGroup represents a list of files eligible to be compacted together.
type CompactionGroup []string

// CompactionPlanner determines what TSM files and WAL segments to include in a
// given compaction run.
type CompactionPlanner interface {
	Plan(lastWrite time.Time) ([]CompactionGroup, int64)
	PlanLevel(level int) ([]CompactionGroup, int64)
	// PlanOptimize will return the groups for compaction, the compaction group length,
	// and the amount of generations within the compaction group.
	// generationCount needs to be set to decide how many points per block during compaction.
	// This value is mostly ignored in normal compaction code paths, but,
	// for the edge case where there is a single generation with many
	// files under 2 GB this value is an important indicator.
	PlanOptimize() (compactGroup []CompactionGroup, compactionGroupLen int64, generationCount int64)
	Release(group []CompactionGroup)
	FullyCompacted() (bool, string)

	// ForceFull causes the planner to return a full compaction plan the next
	// time Plan() is called if there are files that could be compacted.
	ForceFull()

	SetFileStore(fs *FileStore)
}

// DefaultPlanner implements CompactionPlanner using a strategy to roll up
// multiple generations of TSM files into larger files in stages.  It attempts
// to minimize the number of TSM files on disk while rolling up a bounder number
// of files.
type DefaultPlanner struct {
	FileStore fileStore

	// compactFullWriteColdDuration specifies the length of time after
	// which if no writes have been committed to the WAL, the engine will
	// do a full compaction of the TSM files in this shard. This duration
	// should always be greater than the CacheFlushWriteColdDuraion
	compactFullWriteColdDuration time.Duration

	// lastPlanCheck is the last time Plan was called
	lastPlanCheck time.Time

	mu sync.RWMutex
	// lastFindGenerations is the last time findGenerations was run
	lastFindGenerations time.Time

	// lastGenerations is the last set of generations found by findGenerations
	lastGenerations tsmGenerations

	// forceFull causes the next full plan requests to plan any files
	// that may need to be compacted.  Normally, these files are skipped and scheduled
	// infrequently as the plans are more expensive to run.
	forceFull bool

	// filesInUse is the set of files that have been returned as part of a plan and might
	// be being compacted.  Two plans should not return the same file at any given time.
	filesInUse map[string]struct{}
}

type fileStore interface {
	Stats() []FileStat
	LastModified() time.Time
	BlockCount(path string, idx int) int
	ParseFileName(path string) (int, int, error)
}

func NewDefaultPlanner(fs fileStore, writeColdDuration time.Duration) *DefaultPlanner {
	return &DefaultPlanner{
		FileStore:                    fs,
		compactFullWriteColdDuration: writeColdDuration,
		filesInUse:                   make(map[string]struct{}),
	}
}

// tsmGeneration represents the TSM files within a generation.
// 000001-01.tsm, 000001-02.tsm would be in the same generation
// 000001 each with different sequence numbers.
type tsmGeneration struct {
	id            int
	files         []FileStat
	parseFileName ParseFileNameFunc
}

func newTsmGeneration(id int, parseFileNameFunc ParseFileNameFunc) *tsmGeneration {
	return &tsmGeneration{
		id:            id,
		parseFileName: parseFileNameFunc,
	}
}

// size returns the total size of the files in the generation.
func (t *tsmGeneration) size() uint64 {
	var n uint64
	for _, f := range t.files {
		n += uint64(f.Size)
	}
	return n
}

// compactionLevel returns the level of the files in this generation.
func (t *tsmGeneration) level() int {
	// Level 0 is always created from the result of a cache compaction.  It generates
	// 1 file with a sequence num of 1.  Level 2 is generated by compacting multiple
	// level 1 files.  Level 3 is generate by compacting multiple level 2 files.  Level
	// 4 is for anything else.
	_, seq, _ := t.parseFileName(t.files[0].Path)
	if seq < 4 {
		return seq
	}

	return 4
}

// count returns the number of files in the generation.
func (t *tsmGeneration) count() int {
	return len(t.files)
}

// hasTombstones returns true if there are keys removed for any of the files.
func (t *tsmGeneration) hasTombstones() bool {
	for _, f := range t.files {
		if f.HasTombstone {
			return true
		}
	}
	return false
}

func (c *DefaultPlanner) SetFileStore(fs *FileStore) {
	c.FileStore = fs
}

func (c *DefaultPlanner) ParseFileName(path string) (int, int, error) {
	return c.FileStore.ParseFileName(path)
}

// FullyCompacted returns true if the shard is fully compacted.
func (c *DefaultPlanner) FullyCompacted() (bool, string) {
	gens := c.findGenerations(false)
	if len(gens) > 1 {
		return false, "not fully compacted and not idle because of more than one generation"
	} else if gens.hasTombstones() {
		return false, "not fully compacted and not idle because of tombstones"
	} else {
		// For planning we want to ensure that if there is a single generation
		// shard, but it has many files that are under 2 GB and many files that are
		// not at the aggressive compaction points per block count (100,000) we further
		// compact the shard. It is okay to stop compaction if there are many
		// files that are under 2 GB but at the aggressive points per block count.
		if len(gens) == 1 && len(gens[0].files) > 1 {
			aggressivePointsPerBlockCount := 0
			filesUnderMaxTsmSizeCount := 0
			for _, tsmFile := range gens[0].files {
				if c.FileStore.BlockCount(tsmFile.Path, 1) >= tsdb.AggressiveMaxPointsPerBlock {
					aggressivePointsPerBlockCount++
				}
				if tsmFile.Size < tsdb.MaxTSMFileSize {
					filesUnderMaxTsmSizeCount++
				}
			}

			if filesUnderMaxTsmSizeCount > 1 && aggressivePointsPerBlockCount < len(gens[0].files) {
				return false, tsdb.SingleGenerationReasonText
			}
		}
		return true, ""
	}
}

// ForceFull causes the planner to return a full compaction plan the next time
// a plan is requested.  When ForceFull is called, level and optimize plans will
// not return plans until a full plan is requested and released.
func (c *DefaultPlanner) ForceFull() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.forceFull = true
}

// PlanLevel returns a set of TSM files to rewrite for a specific level.
func (c *DefaultPlanner) PlanLevel(level int) ([]CompactionGroup, int64) {
	// If a full plan has been requested, don't plan any levels which will prevent
	// the full plan from acquiring them.
	c.mu.RLock()
	if c.forceFull {
		c.mu.RUnlock()
		return nil, 0
	}
	c.mu.RUnlock()

	// Determine the generations from all files on disk.  We need to treat
	// a generation conceptually as a single file even though it may be
	// split across several files in sequence.
	generations := c.findGenerations(true)

	// If there is only one generation and no tombstones, then there's nothing to
	// do.
	if len(generations) <= 1 && !generations.hasTombstones() {
		return nil, 0
	}

	// Group each generation by level such that two adjacent generations in the same
	// level become part of the same group.
	var currentGen tsmGenerations
	var groups []tsmGenerations
	for i := 0; i < len(generations); i++ {
		cur := generations[i]

		// See if this generation is orphaned which would prevent it from being further
		// compacted until a final full compaction runs.
		if i < len(generations)-1 {
			if cur.level() < generations[i+1].level() {
				currentGen = append(currentGen, cur)
				continue
			}
		}

		if len(currentGen) == 0 || currentGen.level() == cur.level() {
			currentGen = append(currentGen, cur)
			continue
		}
		groups = append(groups, currentGen)

		currentGen = tsmGenerations{}
		currentGen = append(currentGen, cur)
	}

	if len(currentGen) > 0 {
		groups = append(groups, currentGen)
	}

	// Remove any groups in the wrong level
	var levelGroups []tsmGenerations
	for _, cur := range groups {
		if cur.level() == level {
			levelGroups = append(levelGroups, cur)
		}
	}

	minGenerations := 4
	if level == 1 {
		minGenerations = 8
	}

	var cGroups []CompactionGroup
	for _, group := range levelGroups {
		for _, chunk := range group.chunk(minGenerations) {
			var cGroup CompactionGroup
			var hasTombstones bool
			for _, gen := range chunk {
				if gen.hasTombstones() {
					hasTombstones = true
				}
				for _, file := range gen.files {
					cGroup = append(cGroup, file.Path)
				}
			}

			if len(chunk) < minGenerations && !hasTombstones {
				continue
			}

			cGroups = append(cGroups, cGroup)
		}
	}

	if !c.acquire(cGroups) {
		return nil, int64(len(cGroups))
	}

	return cGroups, int64(len(cGroups))
}

// PlanOptimize returns all TSM files if they are in different generations in order
// to optimize the index across TSM files.  Each returned compaction group can be
// compacted concurrently.
func (c *DefaultPlanner) PlanOptimize() (compactGroup []CompactionGroup, compactionGroupLen int64, generationCount int64) {
	// If a full plan has been requested, don't plan any levels which will prevent
	// the full plan from acquiring them.
	c.mu.RLock()
	if c.forceFull {
		c.mu.RUnlock()
		return nil, 0, 0
	}
	c.mu.RUnlock()

	// Determine the generations from all files on disk.  We need to treat
	// a generation conceptually as a single file even though it may be
	// split across several files in sequence.
	generations := c.findGenerations(true)
	fullyCompacted, _ := c.FullyCompacted()

	if fullyCompacted {
		return nil, 0, 0
	}

	// Group each generation by level such that two adjacent generations in the same
	// level become part of the same group.
	var currentGen tsmGenerations
	var groups []tsmGenerations
	for i := 0; i < len(generations); i++ {
		cur := generations[i]

		// See if this generation is orphan'd which would prevent it from being further
		// compacted until a final full compactin runs.
		if i < len(generations)-1 {
			if cur.level() < generations[i+1].level() {
				currentGen = append(currentGen, cur)
				continue
			}
		}

		if len(currentGen) == 0 || currentGen.level() >= cur.level() {
			currentGen = append(currentGen, cur)
			continue
		}
		groups = append(groups, currentGen)

		currentGen = tsmGenerations{}
		currentGen = append(currentGen, cur)
	}

	if len(currentGen) > 0 {
		groups = append(groups, currentGen)
	}

	// Only optimize level 4 files since using lower-levels will collide
	// with the level planners. If this is a single generation optimization
	// do not skip any levels.
	var levelGroups []tsmGenerations
	if len(generations) == 1 {
		levelGroups = append(levelGroups, groups...)
	} else {
		for _, cur := range groups {
			if cur.level() == 4 {
				levelGroups = append(levelGroups, cur)
			}
		}
	}

	var cGroups []CompactionGroup
	for _, group := range levelGroups {
		var cGroup CompactionGroup
		for _, gen := range group {
			for _, file := range gen.files {
				cGroup = append(cGroup, file.Path)
			}
		}

		cGroups = append(cGroups, cGroup)
	}

	if !c.acquire(cGroups) {
		return nil, int64(len(cGroups)), int64(len(generations))
	}

	return cGroups, int64(len(cGroups)), int64(len(generations))
}

// Plan returns a set of TSM files to rewrite for level 4 or higher.  The planning returns
// multiple groups if possible to allow compactions to run concurrently.
func (c *DefaultPlanner) Plan(lastWrite time.Time) ([]CompactionGroup, int64) {
	generations := c.findGenerations(true)

	c.mu.RLock()
	forceFull := c.forceFull
	c.mu.RUnlock()

	// first check if we should be doing a full compaction because nothing has been written in a long time
	if forceFull || c.compactFullWriteColdDuration > 0 && time.Since(lastWrite) > c.compactFullWriteColdDuration && len(generations) > 1 {

		// Reset the full schedule if we planned because of it.
		if forceFull {
			c.mu.Lock()
			c.forceFull = false
			c.mu.Unlock()
		}

		var tsmFiles []string
		var genCount int
		for i, group := range generations {
			var skip bool

			// Skip the file if it's over the max size and contains a full block and it does not have any tombstones
			if len(generations) > 2 && group.size() > uint64(tsdb.MaxTSMFileSize) && c.FileStore.BlockCount(group.files[0].Path, 1) >= tsdb.DefaultMaxPointsPerBlock && !group.hasTombstones() {
				skip = true
			}

			// We need to look at the level of the next file because it may need to be combined with this generation
			// but won't get picked up on it's own if this generation is skipped.  This allows the most recently
			// created files to get picked up by the full compaction planner and avoids having a few less optimally
			// compressed files.
			if i < len(generations)-1 {
				if generations[i+1].level() <= 3 {
					skip = false
				}
			}

			if skip {
				continue
			}

			for _, f := range group.files {
				tsmFiles = append(tsmFiles, f.Path)
			}
			genCount += 1
		}
		sort.Strings(tsmFiles)

		// Make sure we have more than 1 file and more than 1 generation
		if len(tsmFiles) <= 1 || genCount <= 1 {
			return nil, 0
		}

		group := []CompactionGroup{tsmFiles}
		if !c.acquire(group) {
			return nil, int64(len(group))
		}
		return group, int64(len(group))
	}

	// don't plan if nothing has changed in the filestore
	if c.lastPlanCheck.After(c.FileStore.LastModified()) && !generations.hasTombstones() {
		return nil, 0
	}

	c.lastPlanCheck = time.Now()

	// If there is only one generation, return early to avoid re-compacting the same file
	// over and over again.
	if len(generations) <= 1 && !generations.hasTombstones() {
		return nil, 0
	}

	// Need to find the ending point for level 4 files.  They will be the oldest files. We scan
	// each generation in descending break once we see a file less than 4.
	end := 0
	start := 0
	for i, g := range generations {
		if g.level() <= 3 {
			break
		}
		end = i + 1
	}

	// As compactions run, the oldest files get bigger.  We don't want to re-compact them during
	// this planning if they are maxed out so skip over any we see.
	var hasTombstones bool
	for i, g := range generations[:end] {
		if g.hasTombstones() {
			hasTombstones = true
		}

		if hasTombstones {
			continue
		}

		// Skip the file if it's over the max size and contains a full block or the generation is split
		// over multiple files.  In the latter case, that would mean the data in the file spilled over
		// the 2GB limit.
		if g.size() > uint64(tsdb.MaxTSMFileSize) && c.FileStore.BlockCount(g.files[0].Path, 1) >= tsdb.DefaultMaxPointsPerBlock {
			start = i + 1
		}

		// This is an edge case that can happen after multiple compactions run.  The files at the beginning
		// can become larger faster than ones after them.  We want to skip those really big ones and just
		// compact the smaller ones until they are closer in size.
		if i > 0 {
			if g.size()*2 < generations[i-1].size() {
				start = i
				break
			}
		}
	}

	// step is how may files to compact in a group.  We want to clamp it at 4 but also stil
	// return groups smaller than 4.
	step := 4
	if step > end {
		step = end
	}

	// slice off the generations that we'll examine
	generations = generations[start:end]

	// Loop through the generations in groups of size step and see if we can compact all (or
	// some of them as group)
	groups := []tsmGenerations{}
	for i := 0; i < len(generations); i += step {
		var skipGroup bool
		startIndex := i

		for j := i; j < i+step && j < len(generations); j++ {
			gen := generations[j]
			lvl := gen.level()

			// Skip compacting this group if there happens to be any lower level files in the
			// middle.  These will get picked up by the level compactors.
			if lvl <= 3 {
				skipGroup = true
				break
			}

			// Skip the file if it's over the max size and it contains a full block
			if gen.size() >= uint64(tsdb.MaxTSMFileSize) && c.FileStore.BlockCount(gen.files[0].Path, 1) >= tsdb.DefaultMaxPointsPerBlock && !gen.hasTombstones() {
				startIndex++
				continue
			}
		}

		if skipGroup {
			continue
		}

		endIndex := i + step
		if endIndex > len(generations) {
			endIndex = len(generations)
		}
		if endIndex-startIndex > 0 {
			groups = append(groups, generations[startIndex:endIndex])
		}
	}

	if len(groups) == 0 {
		return nil, 0
	}

	// With the groups, we need to evaluate whether the group as a whole can be compacted
	compactable := []tsmGenerations{}
	for _, group := range groups {
		// if we don't have enough generations to compact, skip it
		if len(group) < 4 && !group.hasTombstones() {
			continue
		}
		compactable = append(compactable, group)
	}

	// All the files to be compacted must be compacted in order.  We need to convert each
	// group to the actual set of files in that group to be compacted.
	var tsmFiles []CompactionGroup
	for _, c := range compactable {
		var cGroup CompactionGroup
		for _, group := range c {
			for _, f := range group.files {
				cGroup = append(cGroup, f.Path)
			}
		}
		sort.Strings(cGroup)
		tsmFiles = append(tsmFiles, cGroup)
	}

	if !c.acquire(tsmFiles) {
		return nil, int64(len(tsmFiles))
	}
	return tsmFiles, int64(len(tsmFiles))
}

// findGenerations groups all the TSM files by generation based
// on their filename, then returns the generations in descending order (newest first).
// If skipInUse is true, tsm files that are part of an existing compaction plan
// are not returned.
func (c *DefaultPlanner) findGenerations(skipInUse bool) tsmGenerations {
	c.mu.Lock()
	defer c.mu.Unlock()

	last := c.lastFindGenerations
	lastGen := c.lastGenerations

	if !last.IsZero() && c.FileStore.LastModified().Equal(last) {
		return lastGen
	}

	genTime := c.FileStore.LastModified()
	tsmStats := c.FileStore.Stats()
	generations := make(map[int]*tsmGeneration, len(tsmStats))
	for _, f := range tsmStats {
		gen, _, _ := c.ParseFileName(f.Path)

		// Skip any files that are assigned to a current compaction plan
		if _, ok := c.filesInUse[f.Path]; skipInUse && ok {
			continue
		}

		group := generations[gen]
		if group == nil {
			group = newTsmGeneration(gen, c.ParseFileName)
			generations[gen] = group
		}
		group.files = append(group.files, f)
	}

	orderedGenerations := make(tsmGenerations, 0, len(generations))
	for _, g := range generations {
		orderedGenerations = append(orderedGenerations, g)
	}
	if !orderedGenerations.IsSorted() {
		sort.Sort(orderedGenerations)
	}

	c.lastFindGenerations = genTime
	c.lastGenerations = orderedGenerations

	return orderedGenerations
}

func (c *DefaultPlanner) acquire(groups []CompactionGroup) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	// See if the new files are already in use
	for _, g := range groups {
		for _, f := range g {
			if _, ok := c.filesInUse[f]; ok {
				return false
			}
		}
	}

	// Mark all the new files in use
	for _, g := range groups {
		for _, f := range g {
			c.filesInUse[f] = struct{}{}
		}
	}
	return true
}

// Release removes the files reference in each compaction group allowing new plans
// to be able to use them.
func (c *DefaultPlanner) Release(groups []CompactionGroup) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, g := range groups {
		for _, f := range g {
			delete(c.filesInUse, f)
		}
	}
}

// Compactor merges multiple TSM files into new files or
// writes a Cache into 1 or more TSM files.
type Compactor struct {
	Dir  string
	Size int

	FileStore interface {
		NextGeneration() int
		TSMReader(path string) (*TSMReader, error)
	}

	// RateLimit is the limit for disk writes for all concurrent compactions.
	RateLimit limiter.Rate

	formatFileName FormatFileNameFunc
	parseFileName  ParseFileNameFunc

	mu                 sync.RWMutex
	snapshotsEnabled   bool
	compactionsEnabled bool

	// lastSnapshotDuration is the amount of time the last snapshot took to complete.
	lastSnapshotDuration time.Duration

	snapshotLatencies *latencies

	// The channel to signal that any in progress snapshots should be aborted.
	snapshotsInterrupt chan struct{}
	// The channel to signal that any in progress level compactions should be aborted.
	compactionsInterrupt chan struct{}

	files map[string]struct{}
}

// NewCompactor returns a new instance of Compactor.
func NewCompactor() *Compactor {
	return &Compactor{
		formatFileName: DefaultFormatFileName,
		parseFileName:  DefaultParseFileName,
	}
}

func (c *Compactor) WithFormatFileNameFunc(formatFileNameFunc FormatFileNameFunc) {
	c.formatFileName = formatFileNameFunc
}

func (c *Compactor) WithParseFileNameFunc(parseFileNameFunc ParseFileNameFunc) {
	c.parseFileName = parseFileNameFunc
}

// Open initializes the Compactor.
func (c *Compactor) Open() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.snapshotsEnabled || c.compactionsEnabled {
		return
	}

	c.snapshotsEnabled = true
	c.compactionsEnabled = true
	c.snapshotsInterrupt = make(chan struct{})
	c.compactionsInterrupt = make(chan struct{})
	c.snapshotLatencies = &latencies{values: make([]time.Duration, 4)}

	c.files = make(map[string]struct{})
}

// Close disables the Compactor.
func (c *Compactor) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !(c.snapshotsEnabled || c.compactionsEnabled) {
		return
	}
	c.snapshotsEnabled = false
	c.compactionsEnabled = false
	if c.compactionsInterrupt != nil {
		close(c.compactionsInterrupt)
	}
	if c.snapshotsInterrupt != nil {
		close(c.snapshotsInterrupt)
	}
}

// DisableSnapshots disables the compactor from performing snapshots.
func (c *Compactor) DisableSnapshots() {
	c.mu.Lock()
	c.snapshotsEnabled = false
	if c.snapshotsInterrupt != nil {
		close(c.snapshotsInterrupt)
		c.snapshotsInterrupt = nil
	}
	c.mu.Unlock()
}

// EnableSnapshots allows the compactor to perform snapshots.
func (c *Compactor) EnableSnapshots() {
	c.mu.Lock()
	c.snapshotsEnabled = true
	if c.snapshotsInterrupt == nil {
		c.snapshotsInterrupt = make(chan struct{})
	}
	c.mu.Unlock()
}

// DisableSnapshots disables the compactor from performing compactions.
func (c *Compactor) DisableCompactions() {
	c.mu.Lock()
	c.compactionsEnabled = false
	if c.compactionsInterrupt != nil {
		close(c.compactionsInterrupt)
		c.compactionsInterrupt = nil
	}
	c.mu.Unlock()
}

// EnableCompactions allows the compactor to perform compactions.
func (c *Compactor) EnableCompactions() {
	c.mu.Lock()
	c.compactionsEnabled = true
	if c.compactionsInterrupt == nil {
		c.compactionsInterrupt = make(chan struct{})
	}
	c.mu.Unlock()
}

// WriteSnapshot writes a Cache snapshot to one or more new TSM files.
func (c *Compactor) WriteSnapshot(cache *Cache, logger *zap.Logger) ([]string, error) {
	c.mu.RLock()
	enabled := c.snapshotsEnabled
	intC := c.snapshotsInterrupt
	c.mu.RUnlock()

	if !enabled {
		return nil, errSnapshotsDisabled
	}

	start := time.Now()
	card := cache.Count()

	// Enable throttling if we have lower cardinality or snapshots are going fast.
	throttle := card < 3e6 && c.snapshotLatencies.avg() < 15*time.Second

	// Write snapshot concurrently if cardinality is relatively high.
	concurrency := card / 2e6
	if concurrency < 1 {
		concurrency = 1
	}

	// Special case very high cardinality, use max concurrency and don't throttle writes.
	if card >= 3e6 {
		concurrency = 4
		throttle = false
	}

	splits := cache.Split(concurrency)

	type res struct {
		files []string
		err   error
	}

	resC := make(chan res, concurrency)
	for i := 0; i < concurrency; i++ {
		go func(sp *Cache) {
			iter := NewCacheKeyIterator(sp, tsdb.DefaultMaxPointsPerBlock, intC)
			files, err := c.writeNewFiles(c.FileStore.NextGeneration(), 0, nil, iter, throttle, logger)
			resC <- res{files: files, err: err}

		}(splits[i])
	}

	var errs []error
	files := make([]string, 0, concurrency)
	for i := 0; i < concurrency; i++ {
		result := <-resC
		if result.err != nil {
			errs = append(errs, result.err)
		}
		files = append(files, result.files...)
	}

	dur := time.Since(start).Truncate(time.Second)

	c.mu.Lock()

	// See if we were disabled while writing a snapshot
	enabled = c.snapshotsEnabled
	c.lastSnapshotDuration = dur
	c.snapshotLatencies.add(time.Since(start))
	c.mu.Unlock()

	if !enabled {
		return nil, errSnapshotsDisabled
	}

	return files, errors.Join(errs...)
}

// compact writes multiple smaller TSM files into 1 or more larger files.
func (c *Compactor) compact(fast bool, tsmFiles []string, logger *zap.Logger) ([]string, error) {
	// Sets the points per block size. The larger this value is set
	// the more points there will be in a single index. Under normal
	// conditions this should always be 1000 but there is an edge case
	// where this is increased.
	size := c.Size
	if size <= 0 {
		size = tsdb.DefaultMaxPointsPerBlock
	}

	c.mu.RLock()
	intC := c.compactionsInterrupt
	c.mu.RUnlock()

	// The new compacted files need to added to the max generation in the
	// set.  We need to find that max generation as well as the max sequence
	// number to ensure we write to the next unique location.
	var maxGeneration, maxSequence int
	for _, f := range tsmFiles {
		gen, seq, err := c.parseFileName(f)
		if err != nil {
			return nil, err
		}

		if gen > maxGeneration {
			maxGeneration = gen
			maxSequence = seq
		}

		if gen == maxGeneration && seq > maxSequence {
			maxSequence = seq
		}
	}

	// For each TSM file, create a TSM reader
	var trs []*TSMReader
	for _, file := range tsmFiles {
		select {
		case <-intC:
			return nil, errCompactionAborted{}
		default:
		}

		tr, err := c.FileStore.TSMReader(file)
		if err != nil {
			return nil, errCompactionAborted{fmt.Errorf("error creating reader for %q: %w", file, err)}
		}
		if tr == nil {
			// This would be a bug if this occurred as tsmFiles passed in should only be
			// assigned to one compaction at any one time.  A nil tr would mean the file
			// doesn't exist.
			return nil, errCompactionAborted{fmt.Errorf("bad plan: %s", file)}
		}
		defer tr.Unref() // inform that we're done with this reader when this method returns.
		trs = append(trs, tr)
	}

	if len(trs) == 0 {
		logger.Debug("No input files")
		return nil, nil
	}

	tsm, err := NewTSMBatchKeyIterator(size, fast, DefaultMaxSavedErrors, intC, tsmFiles, trs...)
	if err != nil {
		return nil, err
	}

	return c.writeNewFiles(maxGeneration, maxSequence, tsmFiles, tsm, true, logger)
}

// CompactFull writes multiple smaller TSM files into 1 or more larger files.
func (c *Compactor) CompactFull(tsmFiles []string, logger *zap.Logger) ([]string, error) {
	c.mu.RLock()
	enabled := c.compactionsEnabled
	c.mu.RUnlock()

	if !enabled {
		return nil, errCompactionsDisabled
	}

	if !c.add(tsmFiles) {
		return nil, errCompactionInProgress{}
	}
	defer c.remove(tsmFiles)

	files, err := c.compact(false, tsmFiles, logger)

	// See if we were disabled while writing a snapshot
	c.mu.RLock()
	enabled = c.compactionsEnabled
	c.mu.RUnlock()

	if !enabled {
		if err := c.removeTmpFiles(files); err != nil {
			return nil, err
		}
		return nil, errCompactionsDisabled
	}

	return files, err
}

// CompactFast writes multiple smaller TSM files into 1 or more larger files.
func (c *Compactor) CompactFast(tsmFiles []string, logger *zap.Logger) ([]string, error) {
	c.mu.RLock()
	enabled := c.compactionsEnabled
	c.mu.RUnlock()

	if !enabled {
		return nil, errCompactionsDisabled
	}

	if !c.add(tsmFiles) {
		return nil, errCompactionInProgress{}
	}
	defer c.remove(tsmFiles)

	files, err := c.compact(true, tsmFiles, logger)

	// See if we were disabled while writing a snapshot
	c.mu.RLock()
	enabled = c.compactionsEnabled
	c.mu.RUnlock()

	if !enabled {
		if err := c.removeTmpFiles(files); err != nil {
			return nil, err
		}
		return nil, errCompactionsDisabled
	}

	return files, err

}

// removeTmpFiles is responsible for cleaning up a compaction that
// was started, but then abandoned before the temporary files were dealt with.
func (c *Compactor) removeTmpFiles(files []string) error {
	if len(files) <= 0 {
		return errors.New("files list is empty; no files to remove")
	}
	for _, f := range files {
		if err := os.Remove(f); err != nil {
			return fmt.Errorf("error removing temp compaction file: %v", err)
		}
	}
	return nil
}

// writeNewFiles writes from the iterator into new TSM files, rotating
// to a new file once it has reached the max TSM file size.
func (c *Compactor) writeNewFiles(generation, sequence int, src []string, iter KeyIterator, throttle bool, logger *zap.Logger) ([]string, error) {
	// These are the new TSM files written
	var files []string
	var eInProgress errCompactionInProgress

	for {
		sequence++

		// New TSM files are written to a temp file and renamed when fully completed.
		fileName := filepath.Join(c.Dir, c.formatFileName(generation, sequence)+"."+TSMFileExtension+"."+TmpTSMFileExtension)
		logger.Debug("Compacting files", zap.Int("file_count", len(src)), zap.String("output_file", fileName))

		// Write as much as possible to this file
		rollToNext, err := c.write(fileName, iter, throttle, logger)

		if rollToNext {
			// We've hit the max file limit and there is more to write.  Create a new file
			// and continue.
			files = append(files, fileName)
			logger.Debug("file size or block count exceeded, opening another output file", zap.String("output_file", fileName))
			continue
		} else if errors.Is(err, ErrNoValues) {
			logger.Debug("Dropping empty file", zap.String("output_file", fileName))
			// If the file only contained tombstoned entries, then it would be a 0 length
			// file that we can drop.
			if err := os.RemoveAll(fileName); err != nil {
				return nil, err
			}
			break
		} else if errors.As(err, &eInProgress) {
			if !errors.Is(eInProgress.err, fs.ErrExist) {
				logger.Error("error creating compaction file", zap.String("output_file", fileName), zap.Error(err))
			} else {
				// Don't clean up the file as another compaction is using it.  This should not happen as the
				// planner keeps track of which files are assigned to compaction plans now.
				logger.Warn("file exists, compaction in progress already", zap.String("output_file", fileName))
			}
			return nil, err
		} else if err != nil {
			var errs []error
			errs = append(errs, err)
			// We hit an error and didn't finish the compaction.  Abort.
			// Remove any tmp files we already completed
			// discard later errors to return the first one from the write() call
			for _, f := range files {
				err = os.RemoveAll(f)
				if err != nil {
					errs = append(errs, err)
				}
			}
			// Remove the temp file
			// discard later errors to return the first one from the write() call
			err = os.RemoveAll(fileName)
			if err != nil {
				errs = append(errs, err)
			}

			return nil, errors.Join(errs...)
		}

		files = append(files, fileName)
		break
	}

	return files, nil
}

func (c *Compactor) write(path string, iter KeyIterator, throttle bool, logger *zap.Logger) (rollToNext bool, err error) {
	fd, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_EXCL, 0666)
	if err != nil {
		return false, errCompactionInProgress{err: err}
	}

	// syncingWriter ensures that whatever we wrap the above file descriptor in
	// it will always be able to be synced by the tsm writer, since it does
	// type assertions to attempt to sync.
	type syncingWriter interface {
		io.Writer
		Sync() error
	}

	// Create the write for the new TSM file.
	var (
		w           TSMWriter
		limitWriter syncingWriter = fd
	)

	if c.RateLimit != nil && throttle {
		limitWriter = limiter.NewWriterWithRate(fd, c.RateLimit)
	}

	// Use a disk based TSM buffer if it looks like we might create a big index
	// in memory.
	if iter.EstimatedIndexSize() > 64*1024*1024 {
		w, err = NewTSMWriterWithDiskBuffer(limitWriter)
	} else {
		w, err = NewTSMWriter(limitWriter)
	}
	if err != nil {
		// Close the file and return if we can't create the TSMWriter
		return false, errors.Join(err, fd.Close())
	}
	defer func() {
		var eInProgress errCompactionInProgress

		errs := make([]error, 0, 3)
		errs = append(errs, err)
		closeErr := w.Close()
		errs = append(errs, closeErr)

		// Check for conditions where we should not remove the file
		inProgress := errors.As(err, &eInProgress) && errors.Is(eInProgress.err, fs.ErrExist)
		if (closeErr == nil) && (inProgress || rollToNext) {
			// do not join errors, there is only the one.
			return
		} else if err != nil || closeErr != nil {
			// Remove the file, we have had a problem
			errs = append(errs, w.Remove())
		}
		err = errors.Join(errs...)
	}()

	lastLogSize := w.Size()
	for iter.Next() {
		c.mu.RLock()
		enabled := c.snapshotsEnabled || c.compactionsEnabled
		c.mu.RUnlock()

		if !enabled {
			return false, errCompactionAborted{}
		}
		// Each call to read returns the next sorted key (or the prior one if there are
		// more values to write).  The size of values will be less than or equal to our
		// chunk size (1000)
		key, minTime, maxTime, block, err := iter.Read()
		if err != nil {
			return false, err
		}

		if minTime > maxTime {
			return false, fmt.Errorf("invalid index entry for block. min=%d, max=%d", minTime, maxTime)
		}

		// Write the key and value
		if err := w.WriteBlock(key, minTime, maxTime, block); errors.Is(err, ErrMaxBlocksExceeded) {
			if err := w.WriteIndex(); err != nil {
				return false, err
			}
			return true, err
		} else if err != nil {
			return false, err
		}

		// If we're over maxTSMFileSize, close out the file
		// and return the error.
		if w.Size() > tsdb.MaxTSMFileSize {
			if err := w.WriteIndex(); err != nil {
				return false, err
			}

			return true, errMaxFileExceeded
		} else if (w.Size() - lastLogSize) > logEvery {
			logger.Debug("Compaction progress", zap.String("output_file", path), zap.Uint32("size", w.Size()))
			lastLogSize = w.Size()
		}
	}

	// Were there any errors encountered during iteration?
	if err := iter.Err(); err != nil {
		return false, err
	}

	// We're all done.  Close out the file.
	if err := w.WriteIndex(); err != nil {
		return false, err
	}
	logger.Debug("Compaction finished", zap.String("output_file", path), zap.Uint32("size", w.Size()))
	return false, nil
}

func (c *Compactor) add(files []string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	// See if the new files are already in use
	for _, f := range files {
		if _, ok := c.files[f]; ok {
			return false
		}
	}

	// Mark all the new files in use
	for _, f := range files {
		c.files[f] = struct{}{}
	}
	return true
}

func (c *Compactor) remove(files []string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, f := range files {
		delete(c.files, f)
	}
}

// KeyIterator allows iteration over set of keys and values in sorted order.
type KeyIterator interface {
	// Next returns true if there are any values remaining in the iterator.
	Next() bool

	// Read returns the key, time range, and raw data for the next block,
	// or any error that occurred.
	Read() (key []byte, minTime int64, maxTime int64, data []byte, err error)

	// Close closes the iterator.
	Close() error

	// Err returns any errors encountered during iteration.
	Err() error

	// EstimatedIndexSize returns the estimated size of the index that would
	// be required to store all the series and entries in the KeyIterator.
	EstimatedIndexSize() int
}
type TSMErrors []error

func (t TSMErrors) Error() string {
	e := []string{}
	for _, v := range t {
		e = append(e, v.Error())
	}
	return strings.Join(e, ", ")
}

type block struct {
	key              []byte
	minTime, maxTime int64
	typ              byte
	b                []byte
	tombstones       []TimeRange

	// readMin, readMax are the timestamps range of values have been
	// read and encoded from this block.
	readMin, readMax int64
}

func (b *block) overlapsTimeRange(min, max int64) bool {
	return b.minTime <= max && b.maxTime >= min
}

func (b *block) read() bool {
	return b.readMin <= b.minTime && b.readMax >= b.maxTime
}

func (b *block) markRead(min, max int64) {
	if min < b.readMin {
		b.readMin = min
	}

	if max > b.readMax {
		b.readMax = max
	}
}

func (b *block) partiallyRead() bool {
	// If readMin and readMax are still the initial values, nothing has been read.
	if b.readMin == int64(math.MaxInt64) && b.readMax == int64(math.MinInt64) {
		return false
	}
	return b.readMin != b.minTime || b.readMax != b.maxTime
}

type blocks []*block

func (a blocks) Len() int { return len(a) }

func (a blocks) Less(i, j int) bool {
	cmp := bytes.Compare(a[i].key, a[j].key)
	if cmp == 0 {
		return a[i].minTime < a[j].minTime && a[i].maxTime < a[j].minTime
	}
	return cmp < 0
}

func (a blocks) Swap(i, j int) { a[i], a[j] = a[j], a[i] }

// tsmBatchKeyIterator implements the KeyIterator for set of TSMReaders.  Iteration produces
// keys in sorted order and the values between the keys sorted and deduped.  If any of
// the readers have associated tombstone entries, they are returned as part of iteration.
type tsmBatchKeyIterator struct {
	// readers is the set of readers it produce a sorted key run with
	readers []*TSMReader

	// values is the temporary buffers for each key that is returned by a reader
	values map[string][]Value

	// pos is the current key position within the corresponding readers slice.  A value of
	// pos[0] = 1, means the reader[0] is currently at key 1 in its ordered index.
	pos []int

	// errs is any error we received while iterating values.
	errs TSMErrors

	// errSet is the error strings we have seen before
	errSet map[string]struct{}

	// indicates whether the iterator should choose a faster merging strategy over a more
	// optimally compressed one.  If fast is true, multiple blocks will just be added as is
	// and not combined.  In some cases, a slower path will need to be utilized even when
	// fast is true to prevent overlapping blocks of time for the same key.
	// If false, the blocks will be decoded and duplicated (if needed) and
	// then chunked into the maximally sized blocks.
	fast bool

	// size is the maximum number of values to encode in a single block
	size int

	// key is the current key lowest key across all readers that has not be fully exhausted
	// of values.
	key []byte
	typ byte

	// tsmFiles are the string names of the files for use in tracking errors, ordered the same
	// as iterators and buf
	tsmFiles []string
	// currentTsm is the current TSM file being iterated over
	currentTsm string

	iterators []*BlockIterator
	blocks    blocks

	buf []blocks

	// mergeValues are decoded blocks that have been combined
	mergedFloatValues    *tsdb.FloatArray
	mergedIntegerValues  *tsdb.IntegerArray
	mergedUnsignedValues *tsdb.UnsignedArray
	mergedBooleanValues  *tsdb.BooleanArray
	mergedStringValues   *tsdb.StringArray

	// merged are encoded blocks that have been combined or used as is
	// without decode
	merged    blocks
	interrupt chan struct{}

	// maxErrors is the maximum number of errors to store before discarding.
	maxErrors int
	// overflowErrors is the number of errors we have ignored.
	overflowErrors int
}

// AppendError - store unique errors in the order of first appearance,
// up to a limit of maxErrors.  If the error is unique and stored, return true.
func (t *tsmBatchKeyIterator) AppendError(err error) bool {
	s := err.Error()
	if _, ok := t.errSet[s]; ok {
		return true
	} else if t.maxErrors > len(t.errs) {
		t.errs = append(t.errs, err)
		t.errSet[s] = struct{}{}
		return true
	} else {
		// Was the error dropped?
		t.overflowErrors++
		return false
	}
}

// NewTSMBatchKeyIterator returns a new TSM key iterator from readers.
// size indicates the maximum number of values to encode in a single block.
func NewTSMBatchKeyIterator(size int, fast bool, maxErrors int, interrupt chan struct{}, tsmFiles []string, readers ...*TSMReader) (KeyIterator, error) {
	var iter []*BlockIterator
	for _, r := range readers {
		iter = append(iter, r.BlockIterator())
	}

	return &tsmBatchKeyIterator{
		readers:              readers,
		values:               map[string][]Value{},
		pos:                  make([]int, len(readers)),
		errSet:               map[string]struct{}{},
		size:                 size,
		iterators:            iter,
		fast:                 fast,
		tsmFiles:             tsmFiles,
		buf:                  make([]blocks, len(iter)),
		mergedFloatValues:    &tsdb.FloatArray{},
		mergedIntegerValues:  &tsdb.IntegerArray{},
		mergedUnsignedValues: &tsdb.UnsignedArray{},
		mergedBooleanValues:  &tsdb.BooleanArray{},
		mergedStringValues:   &tsdb.StringArray{},
		interrupt:            interrupt,
		maxErrors:            maxErrors,
	}, nil
}

func (k *tsmBatchKeyIterator) hasMergedValues() bool {
	return k.mergedFloatValues.Len() > 0 ||
		k.mergedIntegerValues.Len() > 0 ||
		k.mergedUnsignedValues.Len() > 0 ||
		k.mergedStringValues.Len() > 0 ||
		k.mergedBooleanValues.Len() > 0
}

func (k *tsmBatchKeyIterator) EstimatedIndexSize() int {
	var size uint32
	for _, r := range k.readers {
		size += r.IndexSize()
	}
	return int(size) / len(k.readers)
}

// Next returns true if there are any values remaining in the iterator.
func (k *tsmBatchKeyIterator) Next() bool {
RETRY:
	// Any merged blocks pending?
	if len(k.merged) > 0 {
		k.merged = k.merged[1:]
		if len(k.merged) > 0 {
			return true
		}
	}

	// Any merged values pending?
	if k.hasMergedValues() {
		k.merge()
		if len(k.merged) > 0 || k.hasMergedValues() {
			return true
		}
	}

	// If we still have blocks from the last read, merge them
	if len(k.blocks) > 0 {
		k.merge()
		if len(k.merged) > 0 || k.hasMergedValues() {
			return true
		}
	}

	// Read the next block from each TSM iterator
	for i, v := range k.buf {
		if len(v) != 0 {
			continue
		}

		iter := k.iterators[i]
		k.currentTsm = k.tsmFiles[i]
		if iter.Next() {
			key, minTime, maxTime, typ, _, b, err := iter.Read()
			if err != nil {
				k.AppendError(errBlockRead{k.currentTsm, err})
			}

			// This block may have ranges of time removed from it that would
			// reduce the block min and max time.
			tombstones := iter.r.TombstoneRange(key)

			var blk *block
			if cap(k.buf[i]) > len(k.buf[i]) {
				k.buf[i] = k.buf[i][:len(k.buf[i])+1]
				blk = k.buf[i][len(k.buf[i])-1]
				if blk == nil {
					blk = &block{}
					k.buf[i][len(k.buf[i])-1] = blk
				}
			} else {
				blk = &block{}
				k.buf[i] = append(k.buf[i], blk)
			}
			blk.minTime = minTime
			blk.maxTime = maxTime
			blk.key = key
			blk.typ = typ
			blk.b = b
			blk.tombstones = tombstones
			blk.readMin = math.MaxInt64
			blk.readMax = math.MinInt64

			blockKey := key
			for bytes.Equal(iter.PeekNext(), blockKey) {
				iter.Next()
				key, minTime, maxTime, typ, _, b, err := iter.Read()
				if err != nil {
					k.AppendError(errBlockRead{k.currentTsm, err})
				}

				tombstones := iter.r.TombstoneRange(key)

				var blk *block
				if cap(k.buf[i]) > len(k.buf[i]) {
					k.buf[i] = k.buf[i][:len(k.buf[i])+1]
					blk = k.buf[i][len(k.buf[i])-1]
					if blk == nil {
						blk = &block{}
						k.buf[i][len(k.buf[i])-1] = blk
					}
				} else {
					blk = &block{}
					k.buf[i] = append(k.buf[i], blk)
				}

				blk.minTime = minTime
				blk.maxTime = maxTime
				blk.key = key
				blk.typ = typ
				blk.b = b
				blk.tombstones = tombstones
				blk.readMin = math.MaxInt64
				blk.readMax = math.MinInt64
			}
		}

		if iter.Err() != nil {
			k.AppendError(errBlockRead{k.currentTsm, iter.Err()})
		}
	}

	// Each reader could have a different key that it's currently at, need to find
	// the next smallest one to keep the sort ordering.
	var minKey []byte
	var minType byte
	for _, b := range k.buf {
		// block could be nil if the iterator has been exhausted for that file
		if len(b) == 0 {
			continue
		}
		if len(minKey) == 0 || bytes.Compare(b[0].key, minKey) < 0 {
			minKey = b[0].key
			minType = b[0].typ
		}
	}
	k.key = minKey
	k.typ = minType

	// Now we need to find all blocks that match the min key so we can combine and dedupe
	// the blocks if necessary
	for i, b := range k.buf {
		if len(b) == 0 {
			continue
		}
		if bytes.Equal(b[0].key, k.key) {
			k.blocks = append(k.blocks, b...)
			k.buf[i] = k.buf[i][:0]
		}
	}

	if len(k.blocks) == 0 {
		return false
	}

	k.merge()

	// After merging all the values for this key, we might not have any.  (e.g. they were all deleted
	// through many tombstones).  In this case, move on to the next key instead of ending iteration.
	if len(k.merged) == 0 {
		goto RETRY
	}

	return len(k.merged) > 0
}

// merge combines the next set of blocks into merged blocks.
func (k *tsmBatchKeyIterator) merge() {
	switch k.typ {
	case BlockFloat64:
		k.mergeFloat()
	case BlockInteger:
		k.mergeInteger()
	case BlockUnsigned:
		k.mergeUnsigned()
	case BlockBoolean:
		k.mergeBoolean()
	case BlockString:
		k.mergeString()
	default:
		k.AppendError(errBlockRead{k.currentTsm, fmt.Errorf("unknown block type: %v", k.typ)})
	}
}

func (k *tsmBatchKeyIterator) handleEncodeError(err error, typ string) {
	k.AppendError(errBlockRead{k.currentTsm, fmt.Errorf("encode error: unable to compress block type %s for key '%s': %w", typ, k.key, err)})
}

func (k *tsmBatchKeyIterator) handleDecodeError(err error, typ string) {
	k.AppendError(errBlockRead{k.currentTsm, fmt.Errorf("decode error: unable to decompress block type %s for key '%s': %w", typ, k.key, err)})
}

func (k *tsmBatchKeyIterator) Read() ([]byte, int64, int64, []byte, error) {
	// See if compactions were disabled while we were running.
	select {
	case <-k.interrupt:
		return nil, 0, 0, nil, errCompactionAborted{}
	default:
	}

	if len(k.merged) == 0 {
		return nil, 0, 0, nil, k.Err()
	}

	block := k.merged[0]
	return block.key, block.minTime, block.maxTime, block.b, k.Err()
}

func (k *tsmBatchKeyIterator) Close() error {
	k.values = nil
	k.pos = nil
	k.iterators = nil
	var errSlice []error
	for _, r := range k.readers {
		errSlice = append(errSlice, r.Close())
	}
	clear(k.errSet)
	k.errs = nil
	return errors.Join(errSlice...)
}

// Err error returns any errors encountered during iteration.
func (k *tsmBatchKeyIterator) Err() error {
	if len(k.errs) == 0 {
		return nil
	}
	// Copy the errors before appending the dropped error count
	errs := make([]error, 0, len(k.errs)+1)
	errs = append(errs, k.errs...)
	errs = append(errs, fmt.Errorf("additional errors dropped: %d", k.overflowErrors))
	return errors.Join(errs...)
}

type cacheKeyIterator struct {
	cache *Cache
	size  int
	order [][]byte

	i         int
	blocks    [][]cacheBlock
	ready     []chan struct{}
	interrupt chan struct{}
	err       error
}

type cacheBlock struct {
	k                []byte
	minTime, maxTime int64
	b                []byte
	err              error
}

// NewCacheKeyIterator returns a new KeyIterator from a Cache.
func NewCacheKeyIterator(cache *Cache, size int, interrupt chan struct{}) KeyIterator {
	keys := cache.Keys()

	chans := make([]chan struct{}, len(keys))
	for i := 0; i < len(keys); i++ {
		chans[i] = make(chan struct{}, 1)
	}

	cki := &cacheKeyIterator{
		i:         -1,
		size:      size,
		cache:     cache,
		order:     keys,
		ready:     chans,
		blocks:    make([][]cacheBlock, len(keys)),
		interrupt: interrupt,
	}
	go cki.encode()
	return cki
}

func (c *cacheKeyIterator) EstimatedIndexSize() int {
	var n int
	for _, v := range c.order {
		n += len(v)
	}
	return n
}

func (c *cacheKeyIterator) encode() {
	concurrency := runtime.GOMAXPROCS(0)
	n := len(c.ready)

	// Divide the keyset across each CPU
	chunkSize := 1
	idx := uint64(0)

	for i := 0; i < concurrency; i++ {
		// Run one goroutine per CPU and encode a section of the key space concurrently
		go func() {
			tenc := getTimeEncoder(tsdb.DefaultMaxPointsPerBlock)
			fenc := getFloatEncoder(tsdb.DefaultMaxPointsPerBlock)
			benc := getBooleanEncoder(tsdb.DefaultMaxPointsPerBlock)
			uenc := getUnsignedEncoder(tsdb.DefaultMaxPointsPerBlock)
			senc := getStringEncoder(tsdb.DefaultMaxPointsPerBlock)
			ienc := getIntegerEncoder(tsdb.DefaultMaxPointsPerBlock)

			defer putTimeEncoder(tenc)
			defer putFloatEncoder(fenc)
			defer putBooleanEncoder(benc)
			defer putUnsignedEncoder(uenc)
			defer putStringEncoder(senc)
			defer putIntegerEncoder(ienc)

			for {
				i := int(atomic.AddUint64(&idx, uint64(chunkSize))) - chunkSize

				if i >= n {
					break
				}

				key := c.order[i]
				values := c.cache.values(key)

				for len(values) > 0 {

					end := len(values)
					if end > c.size {
						end = c.size
					}

					minTime, maxTime := values[0].UnixNano(), values[end-1].UnixNano()
					var b []byte
					var err error

					switch values[0].(type) {
					case FloatValue:
						b, err = encodeFloatBlockUsing(nil, values[:end], tenc, fenc)
					case IntegerValue:
						b, err = encodeIntegerBlockUsing(nil, values[:end], tenc, ienc)
					case UnsignedValue:
						b, err = encodeUnsignedBlockUsing(nil, values[:end], tenc, uenc)
					case BooleanValue:
						b, err = encodeBooleanBlockUsing(nil, values[:end], tenc, benc)
					case StringValue:
						b, err = encodeStringBlockUsing(nil, values[:end], tenc, senc)
					default:
						b, err = Values(values[:end]).Encode(nil)
					}

					values = values[end:]

					c.blocks[i] = append(c.blocks[i], cacheBlock{
						k:       key,
						minTime: minTime,
						maxTime: maxTime,
						b:       b,
						err:     err,
					})

					if err != nil {
						c.err = err
					}
				}
				// Notify this key is fully encoded
				c.ready[i] <- struct{}{}
			}
		}()
	}
}

func (c *cacheKeyIterator) Next() bool {
	if c.i >= 0 && c.i < len(c.ready) && len(c.blocks[c.i]) > 0 {
		c.blocks[c.i] = c.blocks[c.i][1:]
		if len(c.blocks[c.i]) > 0 {
			return true
		}
	}
	c.i++

	if c.i >= len(c.ready) {
		return false
	}

	<-c.ready[c.i]
	return true
}

func (c *cacheKeyIterator) Read() ([]byte, int64, int64, []byte, error) {
	// See if snapshot compactions were disabled while we were running.
	select {
	case <-c.interrupt:
		c.err = errCompactionAborted{}
		return nil, 0, 0, nil, c.err
	default:
	}

	blk := c.blocks[c.i][0]
	return blk.k, blk.minTime, blk.maxTime, blk.b, blk.err
}

func (c *cacheKeyIterator) Close() error {
	return nil
}

func (c *cacheKeyIterator) Err() error {
	return c.err
}

type tsmGenerations []*tsmGeneration

func (a tsmGenerations) Len() int           { return len(a) }
func (a tsmGenerations) Less(i, j int) bool { return a[i].id < a[j].id }
func (a tsmGenerations) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a tsmGenerations) hasTombstones() bool {
	for _, g := range a {
		if g.hasTombstones() {
			return true
		}
	}
	return false
}

func (a tsmGenerations) level() int {
	var level int
	for _, g := range a {
		lev := g.level()
		if lev > level {
			level = lev
		}
	}
	return level
}

func (a tsmGenerations) chunk(size int) []tsmGenerations {
	var chunks []tsmGenerations
	for len(a) > 0 {
		if len(a) >= size {
			chunks = append(chunks, a[:size])
			a = a[size:]
		} else {
			chunks = append(chunks, a)
			a = a[len(a):]
		}
	}
	return chunks
}

func (a tsmGenerations) IsSorted() bool {
	if len(a) == 1 {
		return true
	}

	for i := 1; i < len(a); i++ {
		if a.Less(i, i-1) {
			return false
		}
	}
	return true
}

type latencies struct {
	i      int
	values []time.Duration
}

func (l *latencies) add(t time.Duration) {
	l.values[l.i%len(l.values)] = t
	l.i++
}

func (l *latencies) avg() time.Duration {
	var n int64
	var sum time.Duration
	for _, v := range l.values {
		if v == 0 {
			continue
		}
		sum += v
		n++
	}

	if n > 0 {
		return time.Duration(int64(sum) / n)
	}
	return time.Duration(0)
}
