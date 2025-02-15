package tsm1_test

import (
	"archive/tar"
	"bytes"
	"context"
	"fmt"
	"io"
	"math"
	"math/rand"
	"os"
	"path"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/influxdata/influxdb/logger"
	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/pkg/deep"
	"github.com/influxdata/influxdb/query"
	"github.com/influxdata/influxdb/tsdb"
	"github.com/influxdata/influxdb/tsdb/engine/tsm1"
	"github.com/influxdata/influxdb/tsdb/index/inmem"
	"github.com/influxdata/influxql"
	tassert "github.com/stretchr/testify/assert"
)

var notrack = tsdb.NoopStatsTracker()

// Ensure that deletes only sent to the WAL will clear out the data from the cache on restart
func TestEngine_DeleteWALLoadMetadata(t *testing.T) {
	for _, index := range tsdb.RegisteredIndexes() {
		t.Run(index, func(t *testing.T) {
			e := MustOpenEngine(index)
			defer e.Close()

			if err := e.WritePointsString(
				`cpu,host=A value=1.1 1000000000`,
				`cpu,host=B value=1.2 2000000000`,
			); err != nil {
				t.Fatalf("failed to write points: %s", err.Error())
			}

			// Remove series.
			itr := &seriesIterator{keys: [][]byte{[]byte("cpu,host=A")}}
			if err := e.DeleteSeriesRange(itr, math.MinInt64, math.MaxInt64); err != nil {
				t.Fatalf("failed to delete series: %s", err.Error())
			}

			// Ensure we can close and load index from the WAL
			if err := e.Reopen(); err != nil {
				t.Fatal(err)
			}

			if exp, got := 0, len(e.Cache.Values(tsm1.SeriesFieldKeyBytes("cpu,host=A", "value"))); exp != got {
				t.Fatalf("unexpected number of values: got: %d. exp: %d", got, exp)
			}

			if exp, got := 1, len(e.Cache.Values(tsm1.SeriesFieldKeyBytes("cpu,host=B", "value"))); exp != got {
				t.Fatalf("unexpected number of values: got: %d. exp: %d", got, exp)
			}
		})
	}
}

// See https://github.com/influxdata/influxdb/issues/14229
func TestEngine_DeleteSeriesAfterCacheSnapshot(t *testing.T) {
	for _, index := range tsdb.RegisteredIndexes() {
		t.Run(index, func(t *testing.T) {
			e := MustOpenEngine(index)
			defer e.Close()

			if err := e.WritePointsString(
				`cpu,host=A value=1.1 1000000000`,
				`cpu,host=B value=1.2 2000000000`,
			); err != nil {
				t.Fatalf("failed to write points: %s", err.Error())
			}

			e.MeasurementFields([]byte("cpu")).CreateFieldIfNotExists("value", influxql.Float)
			e.CreateSeriesIfNotExists([]byte("cpu,host=A"), []byte("cpu"), models.NewTags(map[string]string{"host": "A"}), notrack)
			e.CreateSeriesIfNotExists([]byte("cpu,host=B"), []byte("cpu"), models.NewTags(map[string]string{"host": "B"}), notrack)

			// Verify series exist.
			n, err := seriesExist(e, "cpu", []string{"host"})
			if err != nil {
				t.Fatal(err)
			} else if got, exp := n, 2; got != exp {
				t.Fatalf("got %d points, expected %d", got, exp)
			}

			// Simulate restart of server
			if err := e.Reopen(); err != nil {
				t.Fatal(err)
			}

			// Snapshot the cache
			if err := e.WriteSnapshot(); err != nil {
				t.Fatalf("failed to snapshot: %s", err.Error())
			}

			// Verify series exist.
			n, err = seriesExist(e, "cpu", []string{"host"})
			if err != nil {
				t.Fatal(err)
			} else if got, exp := n, 2; got != exp {
				t.Fatalf("got %d points, expected %d", got, exp)
			}

			// Delete the series
			itr := &seriesIterator{keys: [][]byte{
				[]byte("cpu,host=A"),
				[]byte("cpu,host=B"),
			},
			}
			if err := e.DeleteSeriesRange(itr, math.MinInt64, math.MaxInt64); err != nil {
				t.Fatalf("failed to delete series: %s", err.Error())
			}

			// Verify the series are no longer present.
			n, err = seriesExist(e, "cpu", []string{"host"})
			if err != nil {
				t.Fatal(err)
			} else if got, exp := n, 0; got != exp {
				t.Fatalf("got %d points, expected %d", got, exp)
			}

			// Simulate restart of server
			if err := e.Reopen(); err != nil {
				t.Fatal(err)
			}

			// Verify the series are no longer present.
			n, err = seriesExist(e, "cpu", []string{"host"})
			if err != nil {
				t.Fatal(err)
			} else if got, exp := n, 0; got != exp {
				t.Fatalf("got %d points, expected %d", got, exp)
			}
		})
	}
}

func seriesExist(e *Engine, m string, dims []string) (int, error) {
	itr, err := e.CreateIterator(context.Background(), "cpu", query.IteratorOptions{
		Expr:       influxql.MustParseExpr(`value`),
		Dimensions: []string{"host"},
		StartTime:  influxql.MinTime,
		EndTime:    influxql.MaxTime,
		Ascending:  false,
	})
	if err != nil {
		return 0, err
	} else if itr == nil {
		return 0, nil
	}
	defer itr.Close()
	fitr := itr.(query.FloatIterator)

	var n int
	for {
		p, err := fitr.Next()
		if err != nil {
			return 0, err
		} else if p == nil {
			return n, nil
		}
		n++
	}
}

// Ensure that the engine can write & read shard digest files.
func TestEngine_Digest(t *testing.T) {
	e := MustOpenEngine(inmem.IndexName)
	defer e.Close()

	if err := e.Open(); err != nil {
		t.Fatalf("failed to open tsm1 engine: %s", err.Error())
	}

	// Create a few points.
	points := []models.Point{
		MustParsePointString("cpu,host=A value=1.1 1000000000"),
		MustParsePointString("cpu,host=B value=1.2 2000000000"),
	}

	if err := e.WritePoints(points, tsdb.NoopStatsTracker()); err != nil {
		t.Fatalf("failed to write points: %s", err.Error())
	}

	// Force a compaction.
	e.ScheduleFullCompaction()

	digest := func() ([]span, error) {
		// Get a reader for the shard's digest.
		r, sz, err := e.Digest()
		if err != nil {
			return nil, err
		}

		if sz <= 0 {
			t.Fatalf("expected digest size > 0")
		}

		// Make sure the digest can be read.
		dr, err := tsm1.NewDigestReader(r)
		if err != nil {
			r.Close()
			return nil, err
		}
		defer dr.Close()

		_, err = dr.ReadManifest()
		if err != nil {
			t.Fatal(err)
		}

		got := []span{}

		for {
			k, s, err := dr.ReadTimeSpan()
			if err == io.EOF {
				break
			} else if err != nil {
				return nil, err
			}

			got = append(got, span{
				key:   k,
				tspan: s,
			})
		}

		return got, nil
	}

	exp := []span{
		span{
			key: "cpu,host=A#!~#value",
			tspan: &tsm1.DigestTimeSpan{
				Ranges: []tsm1.DigestTimeRange{
					tsm1.DigestTimeRange{
						Min: 1000000000,
						Max: 1000000000,
						N:   1,
						CRC: 1048747083,
					},
				},
			},
		},
		span{
			key: "cpu,host=B#!~#value",
			tspan: &tsm1.DigestTimeSpan{
				Ranges: []tsm1.DigestTimeRange{
					tsm1.DigestTimeRange{
						Min: 2000000000,
						Max: 2000000000,
						N:   1,
						CRC: 734984746,
					},
				},
			},
		},
	}

	for n := 0; n < 2; n++ {
		got, err := digest()
		if err != nil {
			t.Fatalf("n = %d: %s", n, err)
		}

		// Make sure the data in the digest was valid.
		if !reflect.DeepEqual(exp, got) {
			t.Fatalf("n = %d\nexp = %v\ngot = %v\n", n, exp, got)
		}
	}

	// Test that writing more points causes the digest to be updated.
	points = []models.Point{
		MustParsePointString("cpu,host=C value=1.1 3000000000"),
	}

	if err := e.WritePoints(points, tsdb.NoopStatsTracker()); err != nil {
		t.Fatalf("failed to write points: %s", err.Error())
	}

	// Force a compaction.
	e.ScheduleFullCompaction()

	// Get new digest.
	got, err := digest()
	if err != nil {
		t.Fatal(err)
	}

	exp = append(exp, span{
		key: "cpu,host=C#!~#value",
		tspan: &tsm1.DigestTimeSpan{
			Ranges: []tsm1.DigestTimeRange{
				tsm1.DigestTimeRange{
					Min: 3000000000,
					Max: 3000000000,
					N:   1,
					CRC: 2553233514,
				},
			},
		},
	})

	if !reflect.DeepEqual(exp, got) {
		t.Fatalf("\nexp = %v\ngot = %v\n", exp, got)
	}
}

type span struct {
	key   string
	tspan *tsm1.DigestTimeSpan
}

// Ensure engine handles concurrent calls to Digest().
func TestEngine_Digest_Concurrent(t *testing.T) {
	e := MustOpenEngine(inmem.IndexName)
	defer e.Close()

	if err := e.Open(); err != nil {
		t.Fatalf("failed to open tsm1 engine: %s", err.Error())
	}

	// Create a few points.
	points := []models.Point{
		MustParsePointString("cpu,host=A value=1.1 1000000000"),
		MustParsePointString("cpu,host=B value=1.2 2000000000"),
	}

	if err := e.WritePoints(points, tsdb.NoopStatsTracker()); err != nil {
		t.Fatalf("failed to write points: %s", err.Error())
	}

	// Force a compaction.
	e.ScheduleFullCompaction()

	// Start multiple waiting goroutines, ready to call Digest().
	start := make(chan struct{})
	errs := make(chan error)
	wg := &sync.WaitGroup{}
	for n := 0; n < 100; n++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			if _, _, err := e.Digest(); err != nil {
				errs <- err
			}
		}()
	}

	// Goroutine to close errs channel after all routines have finished.
	go func() { wg.Wait(); close(errs) }()

	// Signal all goroutines to call Digest().
	close(start)

	// Check for digest errors.
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
}

// Ensure that the engine will backup any TSM files created since the passed in time
func TestEngine_Backup(t *testing.T) {
	sfile := MustOpenSeriesFile()
	defer sfile.Close()

	// Generate temporary file.
	f, _ := os.CreateTemp("", "tsm")
	f.Close()
	os.Remove(f.Name())
	walPath := filepath.Join(f.Name(), "wal")
	os.MkdirAll(walPath, 0777)
	defer os.RemoveAll(f.Name())

	// Create a few points.
	p1 := MustParsePointString("cpu,host=A value=1.1 1000000000")
	p2 := MustParsePointString("cpu,host=B value=1.2 2000000000")
	p3 := MustParsePointString("cpu,host=C value=1.3 3000000000")

	// Write those points to the engine.
	db := path.Base(f.Name())
	opt := tsdb.NewEngineOptions()
	opt.InmemIndex = inmem.NewIndex(db, sfile.SeriesFile)
	idx := tsdb.MustOpenIndex(1, db, filepath.Join(f.Name(), "index"), tsdb.NewSeriesIDSet(), sfile.SeriesFile, opt)
	defer idx.Close()

	e := tsm1.NewEngine(1, idx, f.Name(), walPath, sfile.SeriesFile, opt).(*tsm1.Engine)

	// mock the planner so compactions don't run during the test
	e.CompactionPlan = &mockPlanner{}

	if err := e.Open(); err != nil {
		t.Fatalf("failed to open tsm1 engine: %s", err.Error())
	}

	if err := e.WritePoints([]models.Point{p1}, tsdb.NoopStatsTracker()); err != nil {
		t.Fatalf("failed to write points: %s", err.Error())
	}
	if err := e.WriteSnapshot(); err != nil {
		t.Fatalf("failed to snapshot: %s", err.Error())
	}

	if err := e.WritePoints([]models.Point{p2}, tsdb.NoopStatsTracker()); err != nil {
		t.Fatalf("failed to write points: %s", err.Error())
	}

	b := bytes.NewBuffer(nil)
	if err := e.Backup(b, "", time.Unix(0, 0)); err != nil {
		t.Fatalf("failed to backup: %s", err.Error())
	}

	tr := tar.NewReader(b)
	if len(e.FileStore.Files()) != 2 {
		t.Fatalf("file count wrong: exp: %d, got: %d", 2, len(e.FileStore.Files()))
	}

	fileNames := map[string]bool{}
	for _, f := range e.FileStore.Files() {
		fileNames[filepath.Base(f.Path())] = true
	}

	th, err := tr.Next()
	for err == nil {
		if !fileNames[th.Name] {
			t.Errorf("Extra file in backup: %q", th.Name)
		}
		delete(fileNames, th.Name)
		th, err = tr.Next()
	}

	if err != nil && err != io.EOF {
		t.Fatalf("Problem reading tar header: %s", err)
	}

	for f := range fileNames {
		t.Errorf("File missing from backup: %s", f)
	}

	if t.Failed() {
		t.FailNow()
	}

	lastBackup := time.Now()

	// we have to sleep for a second because last modified times only have second level precision.
	// so this test won't work properly unless the file is at least a second past the last one
	time.Sleep(time.Second)

	if err := e.WritePoints([]models.Point{p3}, tsdb.NoopStatsTracker()); err != nil {
		t.Fatalf("failed to write points: %s", err.Error())
	}

	b = bytes.NewBuffer(nil)
	if err := e.Backup(b, "", lastBackup); err != nil {
		t.Fatalf("failed to backup: %s", err.Error())
	}

	tr = tar.NewReader(b)
	th, err = tr.Next()
	if err != nil {
		t.Fatalf("error getting next tar header: %s", err.Error())
	}

	mostRecentFile := e.FileStore.Files()[e.FileStore.Count()-1].Path()
	if !strings.Contains(mostRecentFile, th.Name) || th.Name == "" {
		t.Fatalf("file name doesn't match:\n\tgot: %s\n\texp: %s", th.Name, mostRecentFile)
	}
	storeDir := filepath.Dir(e.FileStore.Files()[0].Path())
	dfd, err := os.Open(storeDir)
	if err != nil {
		t.Fatalf("cannot open filestore directory %s: %q", storeDir, err)
	} else {
		defer dfd.Close()
	}
	files, err := dfd.Readdirnames(0)
	if err != nil {
		t.Fatalf("cannot read directory %s: %q", storeDir, err)
	}
	for _, f := range files {
		if strings.HasSuffix(f, tsm1.TmpTSMFileExtension) {
			t.Fatalf("temporary directory for backup not cleaned up: %s", f)
		}
	}
}

func TestEngine_Export(t *testing.T) {
	// Generate temporary file.
	f, _ := os.CreateTemp("", "tsm")
	f.Close()
	os.Remove(f.Name())
	walPath := filepath.Join(f.Name(), "wal")
	os.MkdirAll(walPath, 0777)
	defer os.RemoveAll(f.Name())

	// Create a few points.
	p1 := MustParsePointString("cpu,host=A value=1.1 1000000000")
	p2 := MustParsePointString("cpu,host=B value=1.2 2000000000")
	p3 := MustParsePointString("cpu,host=C value=1.3 3000000000")

	sfile := MustOpenSeriesFile()
	defer sfile.Close()

	// Write those points to the engine.
	db := path.Base(f.Name())
	opt := tsdb.NewEngineOptions()
	opt.InmemIndex = inmem.NewIndex(db, sfile.SeriesFile)
	idx := tsdb.MustOpenIndex(1, db, filepath.Join(f.Name(), "index"), tsdb.NewSeriesIDSet(), sfile.SeriesFile, opt)
	defer idx.Close()

	e := tsm1.NewEngine(1, idx, f.Name(), walPath, sfile.SeriesFile, opt).(*tsm1.Engine)

	// mock the planner so compactions don't run during the test
	e.CompactionPlan = &mockPlanner{}

	if err := e.Open(); err != nil {
		t.Fatalf("failed to open tsm1 engine: %s", err.Error())
	}

	if err := e.WritePoints([]models.Point{p1}, tsdb.NoopStatsTracker()); err != nil {
		t.Fatalf("failed to write points: %s", err.Error())
	}
	if err := e.WriteSnapshot(); err != nil {
		t.Fatalf("failed to snapshot: %s", err.Error())
	}

	if err := e.WritePoints([]models.Point{p2}, tsdb.NoopStatsTracker()); err != nil {
		t.Fatalf("failed to write points: %s", err.Error())
	}
	if err := e.WriteSnapshot(); err != nil {
		t.Fatalf("failed to snapshot: %s", err.Error())
	}

	if err := e.WritePoints([]models.Point{p3}, tsdb.NoopStatsTracker()); err != nil {
		t.Fatalf("failed to write points: %s", err.Error())
	}

	// export the whole DB
	var exBuf bytes.Buffer
	if err := e.Export(&exBuf, "", time.Unix(0, 0), time.Unix(0, 4000000000)); err != nil {
		t.Fatalf("failed to export: %s", err.Error())
	}

	var bkBuf bytes.Buffer
	if err := e.Backup(&bkBuf, "", time.Unix(0, 0)); err != nil {
		t.Fatalf("failed to backup: %s", err.Error())
	}

	if len(e.FileStore.Files()) != 3 {
		t.Fatalf("file count wrong: exp: %d, got: %d", 3, len(e.FileStore.Files()))
	}

	fileNames := map[string]bool{}
	for _, f := range e.FileStore.Files() {
		fileNames[filepath.Base(f.Path())] = true
	}

	fileData, err := getExportData(&exBuf)
	if err != nil {
		t.Errorf("Error extracting data from export: %s", err.Error())
	}

	// TEST 1: did we get any extra files not found in the store?
	for k := range fileData {
		if _, ok := fileNames[k]; !ok {
			t.Errorf("exported a file not in the store: %s", k)
		}
	}

	// TEST 2: did we miss any files that the store had?
	for k := range fileNames {
		if _, ok := fileData[k]; !ok {
			t.Errorf("failed to export a file from the store: %s", k)
		}
	}

	// TEST 3: Does 'backup' get the same files + bits?
	tr := tar.NewReader(&bkBuf)

	th, err := tr.Next()
	for err == nil {
		expData, ok := fileData[th.Name]
		if !ok {
			t.Errorf("Extra file in backup: %q", th.Name)
			continue
		}

		buf := new(bytes.Buffer)
		if _, err := io.Copy(buf, tr); err != nil {
			t.Fatal(err)
		}

		if !equalBuffers(expData, buf) {
			t.Errorf("2Difference in data between backup and Export for file %s", th.Name)
		}

		th, err = tr.Next()
	}

	if t.Failed() {
		t.FailNow()
	}

	// TEST 4:  Are subsets (1), (2), (3), (1,2), (2,3) accurately found in the larger export?
	// export the whole DB
	var ex1 bytes.Buffer
	if err := e.Export(&ex1, "", time.Unix(0, 0), time.Unix(0, 1000000000)); err != nil {
		t.Fatalf("failed to export: %s", err.Error())
	}
	ex1Data, err := getExportData(&ex1)
	if err != nil {
		t.Errorf("Error extracting data from export: %s", err.Error())
	}

	for k, v := range ex1Data {
		fullExp, ok := fileData[k]
		if !ok {
			t.Errorf("Extracting subset resulted in file not found in full export: %s", err.Error())
			continue
		}
		if !equalBuffers(fullExp, v) {
			t.Errorf("2Difference in data between backup and Export for file %s", th.Name)
		}

	}

	var ex2 bytes.Buffer
	if err := e.Export(&ex2, "", time.Unix(0, 1000000001), time.Unix(0, 2000000000)); err != nil {
		t.Fatalf("failed to export: %s", err.Error())
	}

	ex2Data, err := getExportData(&ex2)
	if err != nil {
		t.Errorf("Error extracting data from export: %s", err.Error())
	}

	for k, v := range ex2Data {
		fullExp, ok := fileData[k]
		if !ok {
			t.Errorf("Extracting subset resulted in file not found in full export: %s", err.Error())
			continue
		}
		if !equalBuffers(fullExp, v) {
			t.Errorf("2Difference in data between backup and Export for file %s", th.Name)
		}

	}

	var ex3 bytes.Buffer
	if err := e.Export(&ex3, "", time.Unix(0, 2000000001), time.Unix(0, 3000000000)); err != nil {
		t.Fatalf("failed to export: %s", err.Error())
	}

	ex3Data, err := getExportData(&ex3)
	if err != nil {
		t.Errorf("Error extracting data from export: %s", err.Error())
	}

	for k, v := range ex3Data {
		fullExp, ok := fileData[k]
		if !ok {
			t.Errorf("Extracting subset resulted in file not found in full export: %s", err.Error())
			continue
		}
		if !equalBuffers(fullExp, v) {
			t.Errorf("2Difference in data between backup and Export for file %s", th.Name)
		}

	}

	var ex12 bytes.Buffer
	if err := e.Export(&ex12, "", time.Unix(0, 0), time.Unix(0, 2000000000)); err != nil {
		t.Fatalf("failed to export: %s", err.Error())
	}

	ex12Data, err := getExportData(&ex12)
	if err != nil {
		t.Errorf("Error extracting data from export: %s", err.Error())
	}

	for k, v := range ex12Data {
		fullExp, ok := fileData[k]
		if !ok {
			t.Errorf("Extracting subset resulted in file not found in full export: %s", err.Error())
			continue
		}
		if !equalBuffers(fullExp, v) {
			t.Errorf("2Difference in data between backup and Export for file %s", th.Name)
		}

	}

	var ex23 bytes.Buffer
	if err := e.Export(&ex23, "", time.Unix(0, 1000000001), time.Unix(0, 3000000000)); err != nil {
		t.Fatalf("failed to export: %s", err.Error())
	}

	ex23Data, err := getExportData(&ex23)
	if err != nil {
		t.Errorf("Error extracting data from export: %s", err.Error())
	}

	for k, v := range ex23Data {
		fullExp, ok := fileData[k]
		if !ok {
			t.Errorf("Extracting subset resulted in file not found in full export: %s", err.Error())
			continue
		}
		if !equalBuffers(fullExp, v) {
			t.Errorf("2Difference in data between backup and Export for file %s", th.Name)
		}

	}
}

func equalBuffers(bufA, bufB *bytes.Buffer) bool {
	for i, v := range bufA.Bytes() {
		if v != bufB.Bytes()[i] {
			return false
		}
	}
	return true
}

func getExportData(exBuf *bytes.Buffer) (map[string]*bytes.Buffer, error) {

	tr := tar.NewReader(exBuf)

	fileData := make(map[string]*bytes.Buffer)

	// TEST 1: Get the bits for each file.  If we got a file the store doesn't know about, report error
	for {
		th, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}

		buf := new(bytes.Buffer)
		if _, err := io.Copy(buf, tr); err != nil {
			return nil, err
		}
		fileData[th.Name] = buf

	}

	return fileData, nil
}

// Ensure engine can create an ascending iterator for cached values.
func TestEngine_CreateIterator_Cache_Ascending(t *testing.T) {
	t.Parallel()

	for _, index := range tsdb.RegisteredIndexes() {
		t.Run(index, func(t *testing.T) {
			e := MustOpenEngine(index)
			defer e.Close()

			e.MeasurementFields([]byte("cpu")).CreateFieldIfNotExists("value", influxql.Float)
			e.CreateSeriesIfNotExists([]byte("cpu,host=A"), []byte("cpu"), models.NewTags(map[string]string{"host": "A"}), notrack)

			if err := e.WritePointsString(
				`cpu,host=A value=1.1 1000000000`,
				`cpu,host=A value=1.2 2000000000`,
				`cpu,host=A value=1.3 3000000000`,
			); err != nil {
				t.Fatalf("failed to write points: %s", err.Error())
			}

			itr, err := e.CreateIterator(context.Background(), "cpu", query.IteratorOptions{
				Expr:       influxql.MustParseExpr(`value`),
				Dimensions: []string{"host"},
				StartTime:  influxql.MinTime,
				EndTime:    influxql.MaxTime,
				Ascending:  true,
			})
			if err != nil {
				t.Fatal(err)
			}
			fitr := itr.(query.FloatIterator)

			if p, err := fitr.Next(); err != nil {
				t.Fatalf("unexpected error(0): %v", err)
			} else if !reflect.DeepEqual(p, &query.FloatPoint{Name: "cpu", Tags: ParseTags("host=A"), Time: 1000000000, Value: 1.1}) {
				t.Fatalf("unexpected point(0): %v", p)
			}
			if p, err := fitr.Next(); err != nil {
				t.Fatalf("unexpected error(1): %v", err)
			} else if !reflect.DeepEqual(p, &query.FloatPoint{Name: "cpu", Tags: ParseTags("host=A"), Time: 2000000000, Value: 1.2}) {
				t.Fatalf("unexpected point(1): %v", p)
			}
			if p, err := fitr.Next(); err != nil {
				t.Fatalf("unexpected error(2): %v", err)
			} else if !reflect.DeepEqual(p, &query.FloatPoint{Name: "cpu", Tags: ParseTags("host=A"), Time: 3000000000, Value: 1.3}) {
				t.Fatalf("unexpected point(2): %v", p)
			}
			if p, err := fitr.Next(); err != nil {
				t.Fatalf("expected eof, got error: %v", err)
			} else if p != nil {
				t.Fatalf("expected eof: %v", p)
			}
		})
	}
}

// Ensure engine can create an descending iterator for cached values.
func TestEngine_CreateIterator_Cache_Descending(t *testing.T) {
	t.Parallel()

	for _, index := range tsdb.RegisteredIndexes() {
		t.Run(index, func(t *testing.T) {

			e := MustOpenEngine(index)
			defer e.Close()

			e.MeasurementFields([]byte("cpu")).CreateFieldIfNotExists("value", influxql.Float)
			e.CreateSeriesIfNotExists([]byte("cpu,host=A"), []byte("cpu"), models.NewTags(map[string]string{"host": "A"}), notrack)

			if err := e.WritePointsString(
				`cpu,host=A value=1.1 1000000000`,
				`cpu,host=A value=1.2 2000000000`,
				`cpu,host=A value=1.3 3000000000`,
			); err != nil {
				t.Fatalf("failed to write points: %s", err.Error())
			}

			itr, err := e.CreateIterator(context.Background(), "cpu", query.IteratorOptions{
				Expr:       influxql.MustParseExpr(`value`),
				Dimensions: []string{"host"},
				StartTime:  influxql.MinTime,
				EndTime:    influxql.MaxTime,
				Ascending:  false,
			})
			if err != nil {
				t.Fatal(err)
			}
			fitr := itr.(query.FloatIterator)

			if p, err := fitr.Next(); err != nil {
				t.Fatalf("unexpected error(0): %v", err)
			} else if !reflect.DeepEqual(p, &query.FloatPoint{Name: "cpu", Tags: ParseTags("host=A"), Time: 3000000000, Value: 1.3}) {
				t.Fatalf("unexpected point(0): %v", p)
			}
			if p, err := fitr.Next(); err != nil {
				t.Fatalf("unepxected error(1): %v", err)
			} else if !reflect.DeepEqual(p, &query.FloatPoint{Name: "cpu", Tags: ParseTags("host=A"), Time: 2000000000, Value: 1.2}) {
				t.Fatalf("unexpected point(1): %v", p)
			}
			if p, err := fitr.Next(); err != nil {
				t.Fatalf("unexpected error(2): %v", err)
			} else if !reflect.DeepEqual(p, &query.FloatPoint{Name: "cpu", Tags: ParseTags("host=A"), Time: 1000000000, Value: 1.1}) {
				t.Fatalf("unexpected point(2): %v", p)
			}
			if p, err := fitr.Next(); err != nil {
				t.Fatalf("expected eof, got error: %v", err)
			} else if p != nil {
				t.Fatalf("expected eof: %v", p)
			}
		})
	}
}

// Ensure engine can create an ascending iterator for tsm values.
func TestEngine_CreateIterator_TSM_Ascending(t *testing.T) {
	t.Parallel()

	for _, index := range tsdb.RegisteredIndexes() {
		t.Run(index, func(t *testing.T) {
			e := MustOpenEngine(index)
			defer e.Close()

			e.MeasurementFields([]byte("cpu")).CreateFieldIfNotExists("value", influxql.Float)
			e.CreateSeriesIfNotExists([]byte("cpu,host=A"), []byte("cpu"), models.NewTags(map[string]string{"host": "A"}), notrack)

			if err := e.WritePointsString(
				`cpu,host=A value=1.1 1000000000`,
				`cpu,host=A value=1.2 2000000000`,
				`cpu,host=A value=1.3 3000000000`,
			); err != nil {
				t.Fatalf("failed to write points: %s", err.Error())
			}
			e.MustWriteSnapshot()

			itr, err := e.CreateIterator(context.Background(), "cpu", query.IteratorOptions{
				Expr:       influxql.MustParseExpr(`value`),
				Dimensions: []string{"host"},
				StartTime:  1000000000,
				EndTime:    3000000000,
				Ascending:  true,
			})
			if err != nil {
				t.Fatal(err)
			}
			defer itr.Close()
			fitr := itr.(query.FloatIterator)

			if p, err := fitr.Next(); err != nil {
				t.Fatalf("unexpected error(0): %v", err)
			} else if !reflect.DeepEqual(p, &query.FloatPoint{Name: "cpu", Tags: ParseTags("host=A"), Time: 1000000000, Value: 1.1}) {
				t.Fatalf("unexpected point(0): %v", p)
			}
			if p, err := fitr.Next(); err != nil {
				t.Fatalf("unexpected error(1): %v", err)
			} else if !reflect.DeepEqual(p, &query.FloatPoint{Name: "cpu", Tags: ParseTags("host=A"), Time: 2000000000, Value: 1.2}) {
				t.Fatalf("unexpected point(1): %v", p)
			}
			if p, err := fitr.Next(); err != nil {
				t.Fatalf("unexpected error(2): %v", err)
			} else if !reflect.DeepEqual(p, &query.FloatPoint{Name: "cpu", Tags: ParseTags("host=A"), Time: 3000000000, Value: 1.3}) {
				t.Fatalf("unexpected point(2): %v", p)
			}
			if p, err := fitr.Next(); err != nil {
				t.Fatalf("expected eof, got error: %v", err)
			} else if p != nil {
				t.Fatalf("expected eof: %v", p)
			}
		})
	}
}

// Ensure engine can create an descending iterator for cached values.
func TestEngine_CreateIterator_TSM_Descending(t *testing.T) {
	t.Parallel()

	for _, index := range tsdb.RegisteredIndexes() {
		t.Run(index, func(t *testing.T) {
			e := MustOpenEngine(index)
			defer e.Close()

			e.MeasurementFields([]byte("cpu")).CreateFieldIfNotExists("value", influxql.Float)
			e.CreateSeriesIfNotExists([]byte("cpu,host=A"), []byte("cpu"), models.NewTags(map[string]string{"host": "A"}), notrack)

			if err := e.WritePointsString(
				`cpu,host=A value=1.1 1000000000`,
				`cpu,host=A value=1.2 2000000000`,
				`cpu,host=A value=1.3 3000000000`,
			); err != nil {
				t.Fatalf("failed to write points: %s", err.Error())
			}
			e.MustWriteSnapshot()

			itr, err := e.CreateIterator(context.Background(), "cpu", query.IteratorOptions{
				Expr:       influxql.MustParseExpr(`value`),
				Dimensions: []string{"host"},
				StartTime:  influxql.MinTime,
				EndTime:    influxql.MaxTime,
				Ascending:  false,
			})
			if err != nil {
				t.Fatal(err)
			}
			defer itr.Close()
			fitr := itr.(query.FloatIterator)

			if p, err := fitr.Next(); err != nil {
				t.Fatalf("unexpected error(0): %v", err)
			} else if !reflect.DeepEqual(p, &query.FloatPoint{Name: "cpu", Tags: ParseTags("host=A"), Time: 3000000000, Value: 1.3}) {
				t.Fatalf("unexpected point(0): %v", p)
			}
			if p, err := fitr.Next(); err != nil {
				t.Fatalf("unexpected error(1): %v", err)
			} else if !reflect.DeepEqual(p, &query.FloatPoint{Name: "cpu", Tags: ParseTags("host=A"), Time: 2000000000, Value: 1.2}) {
				t.Fatalf("unexpected point(1): %v", p)
			}
			if p, err := fitr.Next(); err != nil {
				t.Fatalf("unexpected error(2): %v", err)
			} else if !reflect.DeepEqual(p, &query.FloatPoint{Name: "cpu", Tags: ParseTags("host=A"), Time: 1000000000, Value: 1.1}) {
				t.Fatalf("unexpected point(2): %v", p)
			}
			if p, err := fitr.Next(); err != nil {
				t.Fatalf("expected eof, got error: %v", err)
			} else if p != nil {
				t.Fatalf("expected eof: %v", p)
			}
		})
	}
}

// Ensure engine can create an iterator with auxiliary fields.
func TestEngine_CreateIterator_Aux(t *testing.T) {
	t.Parallel()

	for _, index := range tsdb.RegisteredIndexes() {
		t.Run(index, func(t *testing.T) {
			e := MustOpenEngine(index)
			defer e.Close()

			e.MeasurementFields([]byte("cpu")).CreateFieldIfNotExists("value", influxql.Float)
			e.MeasurementFields([]byte("cpu")).CreateFieldIfNotExists("F", influxql.Float)
			e.CreateSeriesIfNotExists([]byte("cpu,host=A"), []byte("cpu"), models.NewTags(map[string]string{"host": "A"}), notrack)

			if err := e.WritePointsString(
				`cpu,host=A value=1.1 1000000000`,
				`cpu,host=A F=100 1000000000`,
				`cpu,host=A value=1.2 2000000000`,
				`cpu,host=A value=1.3 3000000000`,
				`cpu,host=A F=200 3000000000`,
			); err != nil {
				t.Fatalf("failed to write points: %s", err.Error())
			}

			itr, err := e.CreateIterator(context.Background(), "cpu", query.IteratorOptions{
				Expr:       influxql.MustParseExpr(`value`),
				Aux:        []influxql.VarRef{{Val: "F"}},
				Dimensions: []string{"host"},
				StartTime:  influxql.MinTime,
				EndTime:    influxql.MaxTime,
				Ascending:  true,
			})
			if err != nil {
				t.Fatal(err)
			}
			fitr := itr.(query.FloatIterator)

			if p, err := fitr.Next(); err != nil {
				t.Fatalf("unexpected error(0): %v", err)
			} else if !deep.Equal(p, &query.FloatPoint{Name: "cpu", Tags: ParseTags("host=A"), Time: 1000000000, Value: 1.1, Aux: []interface{}{float64(100)}}) {
				t.Fatalf("unexpected point(0): %v", p)
			}
			if p, err := fitr.Next(); err != nil {
				t.Fatalf("unexpected error(1): %v", err)
			} else if !deep.Equal(p, &query.FloatPoint{Name: "cpu", Tags: ParseTags("host=A"), Time: 2000000000, Value: 1.2, Aux: []interface{}{(*float64)(nil)}}) {
				t.Fatalf("unexpected point(1): %v", p)
			}
			if p, err := fitr.Next(); err != nil {
				t.Fatalf("unexpected error(2): %v", err)
			} else if !deep.Equal(p, &query.FloatPoint{Name: "cpu", Tags: ParseTags("host=A"), Time: 3000000000, Value: 1.3, Aux: []interface{}{float64(200)}}) {
				t.Fatalf("unexpected point(2): %v", p)
			}
			if p, err := fitr.Next(); err != nil {
				t.Fatalf("expected eof, got error: %v", err)
			} else if p != nil {
				t.Fatalf("expected eof: %v", p)
			}
		})
	}
}

// Ensure engine can create an iterator with a condition.
func TestEngine_CreateIterator_Condition(t *testing.T) {
	t.Parallel()

	for _, index := range tsdb.RegisteredIndexes() {
		t.Run(index, func(t *testing.T) {
			e := MustOpenEngine(index)
			defer e.Close()

			e.MeasurementFields([]byte("cpu")).CreateFieldIfNotExists("value", influxql.Float)
			e.MeasurementFields([]byte("cpu")).CreateFieldIfNotExists("X", influxql.Float)
			e.MeasurementFields([]byte("cpu")).CreateFieldIfNotExists("Y", influxql.Float)
			e.CreateSeriesIfNotExists([]byte("cpu,host=A"), []byte("cpu"), models.NewTags(map[string]string{"host": "A"}), notrack)
			e.SetFieldName([]byte("cpu"), "X")
			e.SetFieldName([]byte("cpu"), "Y")

			if err := e.WritePointsString(
				`cpu,host=A value=1.1 1000000000`,
				`cpu,host=A X=10 1000000000`,
				`cpu,host=A Y=100 1000000000`,

				`cpu,host=A value=1.2 2000000000`,

				`cpu,host=A value=1.3 3000000000`,
				`cpu,host=A X=20 3000000000`,
				`cpu,host=A Y=200 3000000000`,
			); err != nil {
				t.Fatalf("failed to write points: %s", err.Error())
			}

			itr, err := e.CreateIterator(context.Background(), "cpu", query.IteratorOptions{
				Expr:       influxql.MustParseExpr(`value`),
				Dimensions: []string{"host"},
				Condition:  influxql.MustParseExpr(`X = 10 OR Y > 150`),
				StartTime:  influxql.MinTime,
				EndTime:    influxql.MaxTime,
				Ascending:  true,
			})
			if err != nil {
				t.Fatal(err)
			}
			fitr := itr.(query.FloatIterator)

			if p, err := fitr.Next(); err != nil {
				t.Fatalf("unexpected error(0): %v", err)
			} else if !reflect.DeepEqual(p, &query.FloatPoint{Name: "cpu", Tags: ParseTags("host=A"), Time: 1000000000, Value: 1.1}) {
				t.Fatalf("unexpected point(0): %v", p)
			}
			if p, err := fitr.Next(); err != nil {
				t.Fatalf("unexpected point(1): %v", err)
			} else if !reflect.DeepEqual(p, &query.FloatPoint{Name: "cpu", Tags: ParseTags("host=A"), Time: 3000000000, Value: 1.3}) {
				t.Fatalf("unexpected point(1): %v", p)
			}
			if p, err := fitr.Next(); err != nil {
				t.Fatalf("expected eof, got error: %v", err)
			} else if p != nil {
				t.Fatalf("expected eof: %v", p)
			}
		})
	}
}

// Test that series id set gets updated and returned appropriately.
func TestIndex_SeriesIDSet(t *testing.T) {
	test := func(index string) error {
		engine := MustOpenEngine(index)
		defer engine.Close()

		// Add some series.
		engine.MustAddSeries("cpu", map[string]string{"host": "a", "region": "west"})
		engine.MustAddSeries("cpu", map[string]string{"host": "b", "region": "west"})
		engine.MustAddSeries("cpu", map[string]string{"host": "b"})
		engine.MustAddSeries("gpu", nil)
		engine.MustAddSeries("gpu", map[string]string{"host": "b"})
		engine.MustAddSeries("mem", map[string]string{"host": "z"})

		// Collect series IDs.
		seriesIDMap := map[string]uint64{}
		var e tsdb.SeriesIDElem
		var err error

		itr := engine.sfile.SeriesIDIterator()
		for e, err = itr.Next(); ; e, err = itr.Next() {
			if err != nil {
				return err
			} else if e.SeriesID == 0 {
				break
			}

			name, tags := tsdb.ParseSeriesKey(engine.sfile.SeriesKey(e.SeriesID))
			key := fmt.Sprintf("%s%s", name, tags.HashKey(true))
			seriesIDMap[key] = e.SeriesID
		}

		for _, id := range seriesIDMap {
			if !engine.SeriesIDSet().Contains(id) {
				return fmt.Errorf("bitmap does not contain ID: %d", id)
			}
		}

		// Drop all the series for the gpu measurement and they should no longer
		// be in the series ID set.
		if err := engine.DeleteMeasurement([]byte("gpu")); err != nil {
			return err
		}

		if engine.SeriesIDSet().Contains(seriesIDMap["gpu"]) {
			return fmt.Errorf("bitmap does not contain ID: %d for key %s, but should", seriesIDMap["gpu"], "gpu")
		} else if engine.SeriesIDSet().Contains(seriesIDMap["gpu,host=b"]) {
			return fmt.Errorf("bitmap does not contain ID: %d for key %s, but should", seriesIDMap["gpu,host=b"], "gpu,host=b")
		}
		delete(seriesIDMap, "gpu")
		delete(seriesIDMap, "gpu,host=b")

		// Drop the specific mem series
		ditr := &seriesIterator{keys: [][]byte{[]byte("mem,host=z")}}
		if err := engine.DeleteSeriesRange(ditr, math.MinInt64, math.MaxInt64); err != nil {
			return err
		}

		if engine.SeriesIDSet().Contains(seriesIDMap["mem,host=z"]) {
			return fmt.Errorf("bitmap does not contain ID: %d for key %s, but should", seriesIDMap["mem,host=z"], "mem,host=z")
		}
		delete(seriesIDMap, "mem,host=z")

		// The rest of the keys should still be in the set.
		for key, id := range seriesIDMap {
			if !engine.SeriesIDSet().Contains(id) {
				return fmt.Errorf("bitmap does not contain ID: %d for key %s, but should", id, key)
			}
		}

		// Reopen the engine, and the series should be re-added to the bitmap.
		if err := engine.Reopen(); err != nil {
			panic(err)
		}

		// Check bitset is expected.
		expected := tsdb.NewSeriesIDSet()
		for _, id := range seriesIDMap {
			expected.Add(id)
		}

		if !engine.SeriesIDSet().Equals(expected) {
			return fmt.Errorf("got bitset %s, expected %s", engine.SeriesIDSet().String(), expected.String())
		}
		return nil
	}

	for _, index := range tsdb.RegisteredIndexes() {
		t.Run(index, func(t *testing.T) {
			if err := test(index); err != nil {
				t.Error(err)
			}
		})
	}
}

// Ensures that deleting series from TSM files with multiple fields removes all the
// / series
func TestEngine_DeleteSeries(t *testing.T) {
	for _, index := range tsdb.RegisteredIndexes() {
		t.Run(index, func(t *testing.T) {
			// Create a few points.
			p1 := MustParsePointString("cpu,host=A value=1.1 1000000000")
			p2 := MustParsePointString("cpu,host=B value=1.2 2000000000")
			p3 := MustParsePointString("cpu,host=A sum=1.3 3000000000")

			e, err := NewEngine(index)
			if err != nil {
				t.Fatal(err)
			}

			// mock the planner so compactions don't run during the test
			e.CompactionPlan = &mockPlanner{}
			if err := e.Open(); err != nil {
				t.Fatal(err)
			}
			defer e.Close()

			if err := e.writePoints(p1, p2, p3); err != nil {
				t.Fatalf("failed to write points: %s", err.Error())
			}
			if err := e.WriteSnapshot(); err != nil {
				t.Fatalf("failed to snapshot: %s", err.Error())
			}

			keys := e.FileStore.Keys()
			if exp, got := 3, len(keys); exp != got {
				t.Fatalf("series count mismatch: exp %v, got %v", exp, got)
			}

			itr := &seriesIterator{keys: [][]byte{[]byte("cpu,host=A")}}
			if err := e.DeleteSeriesRange(itr, math.MinInt64, math.MaxInt64); err != nil {
				t.Fatalf("failed to delete series: %v", err)
			}

			keys = e.FileStore.Keys()
			if exp, got := 1, len(keys); exp != got {
				t.Fatalf("series count mismatch: exp %v, got %v", exp, got)
			}

			exp := "cpu,host=B#!~#value"
			if _, ok := keys[exp]; !ok {
				t.Fatalf("wrong series deleted: exp %v, got %v", exp, keys)
			}
		})
	}
}

func TestEngine_DeleteSeriesRange(t *testing.T) {
	for _, index := range tsdb.RegisteredIndexes() {
		t.Run(index, func(t *testing.T) {
			// Create a few points.
			p1 := MustParsePointString("cpu,host=0 value=1.1 6000000000") // Should not be deleted
			p2 := MustParsePointString("cpu,host=A value=1.2 2000000000")
			p3 := MustParsePointString("cpu,host=A value=1.3 3000000000")
			p4 := MustParsePointString("cpu,host=B value=1.3 4000000000") // Should not be deleted
			p5 := MustParsePointString("cpu,host=B value=1.3 5000000000") // Should not be deleted
			p6 := MustParsePointString("cpu,host=C value=1.3 1000000000")
			p7 := MustParsePointString("mem,host=C value=1.3 1000000000")  // Should not be deleted
			p8 := MustParsePointString("disk,host=C value=1.3 1000000000") // Should not be deleted

			e, err := NewEngine(index)
			if err != nil {
				t.Fatal(err)
			}

			// mock the planner so compactions don't run during the test
			e.CompactionPlan = &mockPlanner{}
			if err := e.Open(); err != nil {
				t.Fatal(err)
			}
			defer e.Close()

			for _, p := range []models.Point{p1, p2, p3, p4, p5, p6, p7, p8} {
				if err := e.CreateSeriesIfNotExists(p.Key(), p.Name(), p.Tags(), notrack); err != nil {
					t.Fatalf("create series index error: %v", err)
				}
			}

			if err := e.WritePoints([]models.Point{p1, p2, p3, p4, p5, p6, p7, p8}, tsdb.NoopStatsTracker()); err != nil {
				t.Fatalf("failed to write points: %s", err.Error())
			}
			if err := e.WriteSnapshot(); err != nil {
				t.Fatalf("failed to snapshot: %s", err.Error())
			}

			keys := e.FileStore.Keys()
			if exp, got := 6, len(keys); exp != got {
				t.Fatalf("series count mismatch: exp %v, got %v", exp, got)
			}

			itr := &seriesIterator{keys: [][]byte{[]byte("cpu,host=0"), []byte("cpu,host=A"), []byte("cpu,host=B"), []byte("cpu,host=C")}}
			if err := e.DeleteSeriesRange(itr, 0, 3000000000); err != nil {
				t.Fatalf("failed to delete series: %v", err)
			}

			keys = e.FileStore.Keys()
			if exp, got := 4, len(keys); exp != got {
				t.Fatalf("series count mismatch: exp %v, got %v", exp, got)
			}

			exp := "cpu,host=B#!~#value"
			if _, ok := keys[exp]; !ok {
				t.Fatalf("wrong series deleted: exp %v, got %v", exp, keys)
			}

			// Check that the series still exists in the index
			indexSet := tsdb.IndexSet{Indexes: []tsdb.Index{e.index}, SeriesFile: e.sfile}
			iter, err := indexSet.MeasurementSeriesIDIterator([]byte("cpu"))
			if err != nil {
				t.Fatalf("iterator error: %v", err)
			}
			defer iter.Close()

			elem, err := iter.Next()
			if err != nil {
				t.Fatal(err)
			}
			if elem.SeriesID == 0 {
				t.Fatalf("series index mismatch: EOF, exp 2 series")
			}

			// Lookup series.
			name, tags := e.sfile.Series(elem.SeriesID)
			if got, exp := name, []byte("cpu"); !bytes.Equal(got, exp) {
				t.Fatalf("series mismatch: got %s, exp %s", got, exp)
			}

			if !tags.Equal(models.NewTags(map[string]string{"host": "0"})) && !tags.Equal(models.NewTags(map[string]string{"host": "B"})) {
				t.Fatalf(`series mismatch: got %s, exp either "host=0" or "host=B"`, tags)
			}
			iter.Close()

			// Deleting remaining series should remove them from the series.
			itr = &seriesIterator{keys: [][]byte{[]byte("cpu,host=0"), []byte("cpu,host=B")}}
			if err := e.DeleteSeriesRange(itr, 0, 9000000000); err != nil {
				t.Fatalf("failed to delete series: %v", err)
			}

			indexSet = tsdb.IndexSet{Indexes: []tsdb.Index{e.index}, SeriesFile: e.sfile}
			if iter, err = indexSet.MeasurementSeriesIDIterator([]byte("cpu")); err != nil {
				t.Fatalf("iterator error: %v", err)
			}
			if iter == nil {
				return
			}

			defer iter.Close()
			if elem, err = iter.Next(); err != nil {
				t.Fatal(err)
			}
			if elem.SeriesID != 0 {
				t.Fatalf("got an undeleted series id, but series should be dropped from index")
			}
		})
	}
}

func TestEngine_DeleteSeriesRangeWithPredicate(t *testing.T) {
	for _, index := range tsdb.RegisteredIndexes() {
		t.Run(index, func(t *testing.T) {
			// Create a few points.
			p1 := MustParsePointString("cpu,host=A value=1.1 6000000000") // Should not be deleted
			p2 := MustParsePointString("cpu,host=A value=1.2 2000000000") // Should not be deleted
			p3 := MustParsePointString("cpu,host=B value=1.3 3000000000")
			p4 := MustParsePointString("cpu,host=B value=1.3 4000000000")
			p5 := MustParsePointString("cpu,host=C value=1.3 5000000000") // Should not be deleted
			p6 := MustParsePointString("mem,host=B value=1.3 1000000000")
			p7 := MustParsePointString("mem,host=C value=1.3 1000000000")
			p8 := MustParsePointString("disk,host=C value=1.3 1000000000") // Should not be deleted

			e, err := NewEngine(index)
			if err != nil {
				t.Fatal(err)
			}

			// mock the planner so compactions don't run during the test
			e.CompactionPlan = &mockPlanner{}
			if err := e.Open(); err != nil {
				t.Fatal(err)
			}
			defer e.Close()

			for _, p := range []models.Point{p1, p2, p3, p4, p5, p6, p7, p8} {
				if err := e.CreateSeriesIfNotExists(p.Key(), p.Name(), p.Tags(), notrack); err != nil {
					t.Fatalf("create series index error: %v", err)
				}
			}

			if err := e.WritePoints([]models.Point{p1, p2, p3, p4, p5, p6, p7, p8}, tsdb.NoopStatsTracker()); err != nil {
				t.Fatalf("failed to write points: %s", err.Error())
			}
			if err := e.WriteSnapshot(); err != nil {
				t.Fatalf("failed to snapshot: %s", err.Error())
			}

			keys := e.FileStore.Keys()
			if exp, got := 6, len(keys); exp != got {
				t.Fatalf("series count mismatch: exp %v, got %v", exp, got)
			}

			itr := &seriesIterator{keys: [][]byte{[]byte("cpu,host=A"), []byte("cpu,host=B"), []byte("cpu,host=C"), []byte("mem,host=B"), []byte("mem,host=C")}}
			predicate := func(name []byte, tags models.Tags) (int64, int64, bool) {
				if bytes.Equal(name, []byte("mem")) {
					return math.MinInt64, math.MaxInt64, true
				}
				if bytes.Equal(name, []byte("cpu")) {
					for _, tag := range tags {
						if bytes.Equal(tag.Key, []byte("host")) && bytes.Equal(tag.Value, []byte("B")) {
							return math.MinInt64, math.MaxInt64, true
						}
					}
				}
				return math.MinInt64, math.MaxInt64, false
			}
			if err := e.DeleteSeriesRangeWithPredicate(itr, predicate); err != nil {
				t.Fatalf("failed to delete series: %v", err)
			}

			keys = e.FileStore.Keys()
			if exp, got := 3, len(keys); exp != got {
				t.Fatalf("series count mismatch: exp %v, got %v", exp, got)
			}

			exps := []string{"cpu,host=A#!~#value", "cpu,host=C#!~#value", "disk,host=C#!~#value"}
			for _, exp := range exps {
				if _, ok := keys[exp]; !ok {
					t.Fatalf("wrong series deleted: exp %v, got %v", exps, keys)
				}
			}

			// Check that the series still exists in the index
			indexSet := tsdb.IndexSet{Indexes: []tsdb.Index{e.index}, SeriesFile: e.sfile}
			iter, err := indexSet.MeasurementSeriesIDIterator([]byte("cpu"))
			if err != nil {
				t.Fatalf("iterator error: %v", err)
			}
			defer iter.Close()

			elem, err := iter.Next()
			if err != nil {
				t.Fatal(err)
			}
			if elem.SeriesID == 0 {
				t.Fatalf("series index mismatch: EOF, exp 2 series")
			}

			// Lookup series.
			name, tags := e.sfile.Series(elem.SeriesID)
			if got, exp := name, []byte("cpu"); !bytes.Equal(got, exp) {
				t.Fatalf("series mismatch: got %s, exp %s", got, exp)
			}

			if !tags.Equal(models.NewTags(map[string]string{"host": "A"})) && !tags.Equal(models.NewTags(map[string]string{"host": "C"})) {
				t.Fatalf(`series mismatch: got %s, exp either "host=A" or "host=C"`, tags)
			}
			iter.Close()

			// Deleting remaining series should remove them from the series.
			itr = &seriesIterator{keys: [][]byte{[]byte("cpu,host=A"), []byte("cpu,host=C")}}
			if err := e.DeleteSeriesRange(itr, 0, 9000000000); err != nil {
				t.Fatalf("failed to delete series: %v", err)
			}

			indexSet = tsdb.IndexSet{Indexes: []tsdb.Index{e.index}, SeriesFile: e.sfile}
			if iter, err = indexSet.MeasurementSeriesIDIterator([]byte("cpu")); err != nil {
				t.Fatalf("iterator error: %v", err)
			}
			if iter == nil {
				return
			}

			defer iter.Close()
			if elem, err = iter.Next(); err != nil {
				t.Fatal(err)
			}
			if elem.SeriesID != 0 {
				t.Fatalf("got an undeleted series id, but series should be dropped from index")
			}
		})
	}
}

// Tests that a nil predicate deletes all values returned from the series iterator.
func TestEngine_DeleteSeriesRangeWithPredicate_Nil(t *testing.T) {
	for _, index := range tsdb.RegisteredIndexes() {
		t.Run(index, func(t *testing.T) {
			// Create a few points.
			p1 := MustParsePointString("cpu,host=A value=1.1 6000000000") // Should not be deleted
			p2 := MustParsePointString("cpu,host=A value=1.2 2000000000") // Should not be deleted
			p3 := MustParsePointString("cpu,host=B value=1.3 3000000000")
			p4 := MustParsePointString("cpu,host=B value=1.3 4000000000")
			p5 := MustParsePointString("cpu,host=C value=1.3 5000000000") // Should not be deleted
			p6 := MustParsePointString("mem,host=B value=1.3 1000000000")
			p7 := MustParsePointString("mem,host=C value=1.3 1000000000")
			p8 := MustParsePointString("disk,host=C value=1.3 1000000000") // Should not be deleted

			e, err := NewEngine(index)
			if err != nil {
				t.Fatal(err)
			}

			// mock the planner so compactions don't run during the test
			e.CompactionPlan = &mockPlanner{}
			if err := e.Open(); err != nil {
				t.Fatal(err)
			}
			defer e.Close()

			for _, p := range []models.Point{p1, p2, p3, p4, p5, p6, p7, p8} {
				if err := e.CreateSeriesIfNotExists(p.Key(), p.Name(), p.Tags(), notrack); err != nil {
					t.Fatalf("create series index error: %v", err)
				}
			}

			if err := e.WritePoints([]models.Point{p1, p2, p3, p4, p5, p6, p7, p8}, tsdb.NoopStatsTracker()); err != nil {
				t.Fatalf("failed to write points: %s", err.Error())
			}
			if err := e.WriteSnapshot(); err != nil {
				t.Fatalf("failed to snapshot: %s", err.Error())
			}

			keys := e.FileStore.Keys()
			if exp, got := 6, len(keys); exp != got {
				t.Fatalf("series count mismatch: exp %v, got %v", exp, got)
			}

			itr := &seriesIterator{keys: [][]byte{[]byte("cpu,host=A"), []byte("cpu,host=B"), []byte("cpu,host=C"), []byte("mem,host=B"), []byte("mem,host=C")}}
			if err := e.DeleteSeriesRangeWithPredicate(itr, nil); err != nil {
				t.Fatalf("failed to delete series: %v", err)
			}

			keys = e.FileStore.Keys()
			if exp, got := 1, len(keys); exp != got {
				t.Fatalf("series count mismatch: exp %v, got %v", exp, got)
			}

			// Check that the series still exists in the index
			indexSet := tsdb.IndexSet{Indexes: []tsdb.Index{e.index}, SeriesFile: e.sfile}
			iter, err := indexSet.MeasurementSeriesIDIterator([]byte("cpu"))
			if err != nil {
				t.Fatalf("iterator error: %v", err)
			} else if iter == nil {
				return
			}
			defer iter.Close()

			if elem, err := iter.Next(); err != nil {
				t.Fatal(err)
			} else if elem.SeriesID != 0 {
				t.Fatalf("got an undeleted series id, but series should be dropped from index")
			}

			// Check that disk series still exists
			iter, err = indexSet.MeasurementSeriesIDIterator([]byte("disk"))
			if err != nil {
				t.Fatalf("iterator error: %v", err)
			} else if iter == nil {
				return
			}
			defer iter.Close()

			if elem, err := iter.Next(); err != nil {
				t.Fatal(err)
			} else if elem.SeriesID == 0 {
				t.Fatalf("got an undeleted series id, but series should be dropped from index")
			}
		})
	}
}
func TestEngine_DeleteSeriesRangeWithPredicate_FlushBatch(t *testing.T) {
	for _, index := range tsdb.RegisteredIndexes() {
		t.Run(index, func(t *testing.T) {
			// Create a few points.
			p1 := MustParsePointString("cpu,host=A value=1.1 6000000000") // Should not be deleted
			p2 := MustParsePointString("cpu,host=A value=1.2 2000000000") // Should not be deleted
			p3 := MustParsePointString("cpu,host=B value=1.3 3000000000")
			p4 := MustParsePointString("cpu,host=B value=1.3 4000000000")
			p5 := MustParsePointString("cpu,host=C value=1.3 5000000000") // Should not be deleted
			p6 := MustParsePointString("mem,host=B value=1.3 1000000000")
			p7 := MustParsePointString("mem,host=C value=1.3 1000000000")
			p8 := MustParsePointString("disk,host=C value=1.3 1000000000") // Should not be deleted

			e, err := NewEngine(index)
			if err != nil {
				t.Fatal(err)
			}

			// mock the planner so compactions don't run during the test
			e.CompactionPlan = &mockPlanner{}
			if err := e.Open(); err != nil {
				t.Fatal(err)
			}
			defer e.Close()

			for _, p := range []models.Point{p1, p2, p3, p4, p5, p6, p7, p8} {
				if err := e.CreateSeriesIfNotExists(p.Key(), p.Name(), p.Tags(), notrack); err != nil {
					t.Fatalf("create series index error: %v", err)
				}
			}

			if err := e.WritePoints([]models.Point{p1, p2, p3, p4, p5, p6, p7, p8}, tsdb.NoopStatsTracker()); err != nil {
				t.Fatalf("failed to write points: %s", err.Error())
			}
			if err := e.WriteSnapshot(); err != nil {
				t.Fatalf("failed to snapshot: %s", err.Error())
			}

			keys := e.FileStore.Keys()
			if exp, got := 6, len(keys); exp != got {
				t.Fatalf("series count mismatch: exp %v, got %v", exp, got)
			}

			itr := &seriesIterator{keys: [][]byte{[]byte("cpu,host=A"), []byte("cpu,host=B"), []byte("cpu,host=C"), []byte("mem,host=B"), []byte("mem,host=C")}}
			predicate := func(name []byte, tags models.Tags) (int64, int64, bool) {
				if bytes.Equal(name, []byte("mem")) {
					return 1000000000, 1000000000, true
				}

				if bytes.Equal(name, []byte("cpu")) {
					for _, tag := range tags {
						if bytes.Equal(tag.Key, []byte("host")) && bytes.Equal(tag.Value, []byte("B")) {
							return 3000000000, 4000000000, true
						}
					}
				}
				return math.MinInt64, math.MaxInt64, false
			}
			if err := e.DeleteSeriesRangeWithPredicate(itr, predicate); err != nil {
				t.Fatalf("failed to delete series: %v", err)
			}

			keys = e.FileStore.Keys()
			if exp, got := 3, len(keys); exp != got {
				t.Fatalf("series count mismatch: exp %v, got %v", exp, got)
			}

			exps := []string{"cpu,host=A#!~#value", "cpu,host=C#!~#value", "disk,host=C#!~#value"}
			for _, exp := range exps {
				if _, ok := keys[exp]; !ok {
					t.Fatalf("wrong series deleted: exp %v, got %v", exps, keys)
				}
			}

			// Check that the series still exists in the index
			indexSet := tsdb.IndexSet{Indexes: []tsdb.Index{e.index}, SeriesFile: e.sfile}
			iter, err := indexSet.MeasurementSeriesIDIterator([]byte("cpu"))
			if err != nil {
				t.Fatalf("iterator error: %v", err)
			}
			defer iter.Close()

			elem, err := iter.Next()
			if err != nil {
				t.Fatal(err)
			}
			if elem.SeriesID == 0 {
				t.Fatalf("series index mismatch: EOF, exp 2 series")
			}

			// Lookup series.
			name, tags := e.sfile.Series(elem.SeriesID)
			if got, exp := name, []byte("cpu"); !bytes.Equal(got, exp) {
				t.Fatalf("series mismatch: got %s, exp %s", got, exp)
			}

			if !tags.Equal(models.NewTags(map[string]string{"host": "A"})) && !tags.Equal(models.NewTags(map[string]string{"host": "C"})) {
				t.Fatalf(`series mismatch: got %s, exp either "host=A" or "host=C"`, tags)
			}
			iter.Close()

			// Deleting remaining series should remove them from the series.
			itr = &seriesIterator{keys: [][]byte{[]byte("cpu,host=A"), []byte("cpu,host=C")}}
			if err := e.DeleteSeriesRange(itr, 0, 9000000000); err != nil {
				t.Fatalf("failed to delete series: %v", err)
			}

			indexSet = tsdb.IndexSet{Indexes: []tsdb.Index{e.index}, SeriesFile: e.sfile}
			if iter, err = indexSet.MeasurementSeriesIDIterator([]byte("cpu")); err != nil {
				t.Fatalf("iterator error: %v", err)
			}
			if iter == nil {
				return
			}

			defer iter.Close()
			if elem, err = iter.Next(); err != nil {
				t.Fatal(err)
			}
			if elem.SeriesID != 0 {
				t.Fatalf("got an undeleted series id, but series should be dropped from index")
			}
		})
	}
}

func TestEngine_DeleteSeriesRange_OutsideTime(t *testing.T) {
	for _, index := range tsdb.RegisteredIndexes() {
		t.Run(index, func(t *testing.T) {
			// Create a few points.
			p1 := MustParsePointString("cpu,host=A value=1.1 1000000000") // Should not be deleted

			e, err := NewEngine(index)
			if err != nil {
				t.Fatal(err)
			}

			// mock the planner so compactions don't run during the test
			e.CompactionPlan = &mockPlanner{}
			if err := e.Open(); err != nil {
				t.Fatal(err)
			}
			defer e.Close()

			for _, p := range []models.Point{p1} {
				if err := e.CreateSeriesIfNotExists(p.Key(), p.Name(), p.Tags(), notrack); err != nil {
					t.Fatalf("create series index error: %v", err)
				}
			}

			if err := e.WritePoints([]models.Point{p1}, tsdb.NoopStatsTracker()); err != nil {
				t.Fatalf("failed to write points: %s", err.Error())
			}
			if err := e.WriteSnapshot(); err != nil {
				t.Fatalf("failed to snapshot: %s", err.Error())
			}

			keys := e.FileStore.Keys()
			if exp, got := 1, len(keys); exp != got {
				t.Fatalf("series count mismatch: exp %v, got %v", exp, got)
			}

			itr := &seriesIterator{keys: [][]byte{[]byte("cpu,host=A")}}
			if err := e.DeleteSeriesRange(itr, 0, 0); err != nil {
				t.Fatalf("failed to delete series: %v", err)
			}

			keys = e.FileStore.Keys()
			if exp, got := 1, len(keys); exp != got {
				t.Fatalf("series count mismatch: exp %v, got %v", exp, got)
			}

			exp := "cpu,host=A#!~#value"
			if _, ok := keys[exp]; !ok {
				t.Fatalf("wrong series deleted: exp %v, got %v", exp, keys)
			}

			// Check that the series still exists in the index
			iter, err := e.index.MeasurementSeriesIDIterator([]byte("cpu"))
			if err != nil {
				t.Fatalf("iterator error: %v", err)
			}
			defer iter.Close()

			elem, err := iter.Next()
			if err != nil {
				t.Fatal(err)
			}
			if elem.SeriesID == 0 {
				t.Fatalf("series index mismatch: EOF, exp 1 series")
			}

			// Lookup series.
			name, tags := e.sfile.Series(elem.SeriesID)
			if got, exp := name, []byte("cpu"); !bytes.Equal(got, exp) {
				t.Fatalf("series mismatch: got %s, exp %s", got, exp)
			}

			if got, exp := tags, models.NewTags(map[string]string{"host": "A"}); !got.Equal(exp) {
				t.Fatalf("series mismatch: got %s, exp %s", got, exp)
			}
		})
	}
}

func TestEngine_LastModified(t *testing.T) {
	for _, index := range tsdb.RegisteredIndexes() {
		t.Run(index, func(t *testing.T) {
			// Create a few points.
			p1 := MustParsePointString("cpu,host=A value=1.1 1000000000")
			p2 := MustParsePointString("cpu,host=B value=1.2 2000000000")
			p3 := MustParsePointString("cpu,host=A sum=1.3 3000000000")

			e, err := NewEngine(index)
			if err != nil {
				t.Fatal(err)
			}

			// mock the planner so compactions don't run during the test
			e.CompactionPlan = &mockPlanner{}
			e.SetEnabled(false)
			if err := e.Open(); err != nil {
				t.Fatal(err)
			}
			defer e.Close()

			if err := e.writePoints(p1, p2, p3); err != nil {
				t.Fatalf("failed to write points: %s", err.Error())
			}

			lm := e.LastModified()
			if lm.IsZero() {
				t.Fatalf("expected non-zero time, got %v", lm.UTC())
			}
			e.SetEnabled(true)

			// Artificial sleep added due to filesystems caching the mod time
			// of files.  This prevents the WAL last modified time from being
			// returned and newer than the filestore's mod time.
			time.Sleep(2 * time.Second) // Covers most filesystems.

			if err := e.WriteSnapshot(); err != nil {
				t.Fatalf("failed to snapshot: %s", err.Error())
			}

			lm2 := e.LastModified()

			if got, exp := lm.Equal(lm2), false; exp != got {
				t.Fatalf("expected time change, got %v, exp %v: %s == %s", got, exp, lm.String(), lm2.String())
			}

			itr := &seriesIterator{keys: [][]byte{[]byte("cpu,host=A")}}
			if err := e.DeleteSeriesRange(itr, math.MinInt64, math.MaxInt64); err != nil {
				t.Fatalf("failed to delete series: %v", err)
			}

			lm3 := e.LastModified()
			if got, exp := lm2.Equal(lm3), false; exp != got {
				t.Fatalf("expected time change, got %v, exp %v", got, exp)
			}
		})
	}
}

func TestEngine_SnapshotsDisabled(t *testing.T) {
	sfile := MustOpenSeriesFile()
	defer sfile.Close()

	// Generate temporary file.
	dir, _ := os.MkdirTemp("", "tsm")
	walPath := filepath.Join(dir, "wal")
	os.MkdirAll(walPath, 0777)
	defer os.RemoveAll(dir)

	// Create a tsm1 engine.
	db := path.Base(dir)
	opt := tsdb.NewEngineOptions()
	opt.InmemIndex = inmem.NewIndex(db, sfile.SeriesFile)
	idx := tsdb.MustOpenIndex(1, db, filepath.Join(dir, "index"), tsdb.NewSeriesIDSet(), sfile.SeriesFile, opt)
	defer idx.Close()

	e := tsm1.NewEngine(1, idx, dir, walPath, sfile.SeriesFile, opt).(*tsm1.Engine)

	// mock the planner so compactions don't run during the test
	e.CompactionPlan = &mockPlanner{}

	e.SetEnabled(false)
	if err := e.Open(); err != nil {
		t.Fatalf("failed to open tsm1 engine: %s", err.Error())
	}

	// Make sure Snapshots are disabled.
	e.SetCompactionsEnabled(false)
	e.Compactor.DisableSnapshots()

	// Writing a snapshot should not fail when the snapshot is empty
	// even if snapshots are disabled.
	if err := e.WriteSnapshot(); err != nil {
		t.Fatalf("failed to snapshot: %s", err.Error())
	}
}

func TestEngine_ShouldCompactCache(t *testing.T) {
	nowTime := time.Now()

	e, err := NewEngine(inmem.IndexName)
	if err != nil {
		t.Fatal(err)
	}

	// mock the planner so compactions don't run during the test
	e.CompactionPlan = &mockPlanner{}
	e.SetEnabled(false)
	if err := e.Open(); err != nil {
		t.Fatalf("failed to open tsm1 engine: %s", err.Error())
	}
	defer e.Close()

	e.CacheFlushMemorySizeThreshold = 1024
	e.CacheFlushWriteColdDuration = time.Minute

	if e.ShouldCompactCache(nowTime) {
		t.Fatal("nothing written to cache, so should not compact")
	}

	if err := e.WritePointsString("m,k=v f=3i"); err != nil {
		t.Fatal(err)
	}

	if e.ShouldCompactCache(nowTime) {
		t.Fatal("cache size < flush threshold and nothing written to FileStore, so should not compact")
	}

	if !e.ShouldCompactCache(nowTime.Add(time.Hour)) {
		t.Fatal("last compaction was longer than flush write cold threshold, so should compact")
	}

	e.CacheFlushMemorySizeThreshold = 1
	if !e.ShouldCompactCache(nowTime) {
		t.Fatal("cache size > flush threshold, so should compact")
	}
}

// Ensure engine can create an ascending cursor for cache and tsm values.
func TestEngine_CreateCursor_Ascending(t *testing.T) {
	t.Parallel()

	for _, index := range tsdb.RegisteredIndexes() {
		t.Run(index, func(t *testing.T) {

			e := MustOpenEngine(index)
			defer e.Close()

			e.MeasurementFields([]byte("cpu")).CreateFieldIfNotExists("value", influxql.Float)
			e.CreateSeriesIfNotExists([]byte("cpu,host=A"), []byte("cpu"), models.NewTags(map[string]string{"host": "A"}), notrack)

			if err := e.WritePointsString(
				`cpu,host=A value=1.1 1`,
				`cpu,host=A value=1.2 2`,
				`cpu,host=A value=1.3 3`,
			); err != nil {
				t.Fatalf("failed to write points: %s", err.Error())
			}
			e.MustWriteSnapshot()

			if err := e.WritePointsString(
				`cpu,host=A value=10.1 10`,
				`cpu,host=A value=11.2 11`,
				`cpu,host=A value=12.3 12`,
			); err != nil {
				t.Fatalf("failed to write points: %s", err.Error())
			}

			q, err := e.CreateCursorIterator(context.Background())
			if err != nil {
				t.Fatal(err)
			}

			cur, err := q.Next(context.Background(), &tsdb.CursorRequest{
				Name:      []byte("cpu"),
				Tags:      models.ParseTags([]byte("cpu,host=A")),
				Field:     "value",
				Ascending: true,
				StartTime: 2,
				EndTime:   11,
			})
			if err != nil {
				t.Fatal(err)
			}
			defer cur.Close()

			fcur := cur.(tsdb.FloatArrayCursor)
			a := fcur.Next()
			if !cmp.Equal([]int64{2, 3, 10, 11}, a.Timestamps) {
				t.Fatal("unexpect timestamps")
			}
			if !cmp.Equal([]float64{1.2, 1.3, 10.1, 11.2}, a.Values) {
				t.Fatal("unexpect timestamps")
			}
		})
	}
}

// Ensure engine can create an ascending cursor for tsm values.
func TestEngine_CreateCursor_Descending(t *testing.T) {
	t.Parallel()

	for _, index := range tsdb.RegisteredIndexes() {
		t.Run(index, func(t *testing.T) {

			e := MustOpenEngine(index)
			defer e.Close()

			e.MeasurementFields([]byte("cpu")).CreateFieldIfNotExists("value", influxql.Float)
			e.CreateSeriesIfNotExists([]byte("cpu,host=A"), []byte("cpu"), models.NewTags(map[string]string{"host": "A"}), notrack)

			if err := e.WritePointsString(
				`cpu,host=A value=1.1 1`,
				`cpu,host=A value=1.2 2`,
				`cpu,host=A value=1.3 3`,
			); err != nil {
				t.Fatalf("failed to write points: %s", err.Error())
			}
			e.MustWriteSnapshot()

			if err := e.WritePointsString(
				`cpu,host=A value=10.1 10`,
				`cpu,host=A value=11.2 11`,
				`cpu,host=A value=12.3 12`,
			); err != nil {
				t.Fatalf("failed to write points: %s", err.Error())
			}

			q, err := e.CreateCursorIterator(context.Background())
			if err != nil {
				t.Fatal(err)
			}

			cur, err := q.Next(context.Background(), &tsdb.CursorRequest{
				Name:      []byte("cpu"),
				Tags:      models.ParseTags([]byte("cpu,host=A")),
				Field:     "value",
				Ascending: false,
				StartTime: 1,
				EndTime:   10,
			})
			if err != nil {
				t.Fatal(err)
			}
			defer cur.Close()

			fcur := cur.(tsdb.FloatArrayCursor)
			a := fcur.Next()
			if !cmp.Equal([]int64{10, 3, 2, 1}, a.Timestamps) {
				t.Fatalf("unexpect timestamps %v", a.Timestamps)
			}
			if !cmp.Equal([]float64{10.1, 1.3, 1.2, 1.1}, a.Values) {
				t.Fatal("unexpect values")
			}
		})
	}
}

// Ensure engine can create an descending iterator for cached values.
func TestEngine_CreateIterator_SeriesKey(t *testing.T) {
	t.Parallel()

	for _, index := range tsdb.RegisteredIndexes() {
		t.Run(index, func(t *testing.T) {
			assert := tassert.New(t)
			e := MustOpenEngine(index)
			defer e.Close()

			e.MeasurementFields([]byte("cpu")).CreateFieldIfNotExists("value", influxql.Float)
			e.CreateSeriesIfNotExists([]byte("cpu,host=A,region=east"), []byte("cpu"), models.NewTags(map[string]string{"host": "A", "region": "east"}), notrack)
			e.CreateSeriesIfNotExists([]byte("cpu,host=B,region=east"), []byte("cpu"), models.NewTags(map[string]string{"host": "B", "region": "east"}), notrack)
			e.CreateSeriesIfNotExists([]byte("cpu,host=C,region=east"), []byte("cpu"), models.NewTags(map[string]string{"host": "C", "region": "east"}), notrack)
			e.CreateSeriesIfNotExists([]byte("cpu,host=A,region=west"), []byte("cpu"), models.NewTags(map[string]string{"host": "A", "region": "west"}), notrack)

			if err := e.WritePointsString(
				`cpu,host=A,region=east value=1.1 1000000001`,
				`cpu,host=B,region=east value=1.2 1000000002`,
				`cpu,host=A,region=east value=1.3 1000000003`,
				`cpu,host=C,region=east value=1.4 1000000004`,
				`cpu,host=A,region=west value=1.5 1000000005`,
			); err != nil {
				t.Fatalf("failed to write points: %s", err.Error())
			}

			opts := query.IteratorOptions{
				Expr:       influxql.MustParseExpr(`_seriesKey`),
				Dimensions: []string{},
				StartTime:  influxql.MinTime,
				EndTime:    influxql.MaxTime,
				Condition:  influxql.MustParseExpr(`host = 'A'`),
			}

			itr, err := e.CreateIterator(context.Background(), "cpu", opts)
			if err != nil {
				t.Fatal(err)
			}

			stringItr, ok := itr.(query.StringIterator)
			assert.True(ok, "series iterator must be of type string")
			expectedSeries := map[string]struct{}{
				"cpu,host=A,region=west": struct{}{},
				"cpu,host=A,region=east": struct{}{},
			}
			var str *query.StringPoint
			for str, err = stringItr.Next(); err == nil && str != (*query.StringPoint)(nil); str, err = stringItr.Next() {
				_, ok := expectedSeries[str.Value]
				assert.True(ok, "Saw bad key "+str.Value)
				delete(expectedSeries, str.Value)
			}
			assert.NoError(err)
			assert.NoError(itr.Close())

			countOpts := opts
			countOpts.Expr = influxql.MustParseExpr(`count(_seriesKey)`)
			itr, err = e.CreateIterator(context.Background(), "cpu", countOpts)
			if err != nil {
				t.Fatal(err)
			}

			integerIter, ok := itr.(query.IntegerIterator)
			assert.True(ok, "series count iterator must be of type integer")
			i, err := integerIter.Next()
			assert.NoError(err)
			assert.Equal(int64(2), i.Value, "must count 2 series with host=A")
			i, err = integerIter.Next()
			assert.NoError(err)
			assert.Equal((*query.IntegerPoint)(nil), i, "count iterator has only one output")
			assert.NoError(itr.Close())
		})
	}
}

func makeBlockTypeSlice(n int) []byte {
	r := make([]byte, n)
	b := tsm1.BlockFloat64
	m := tsm1.BlockUnsigned + 1
	for i := 0; i < len(r); i++ {
		r[i] = b % m
	}
	return r
}

var blockType = influxql.Unknown

func BenchmarkBlockTypeToInfluxQLDataType(b *testing.B) {
	t := makeBlockTypeSlice(1000)
	for i := 0; i < b.N; i++ {
		for j := 0; j < len(t); j++ {
			blockType = tsm1.BlockTypeToInfluxQLDataType(t[j])
		}
	}
}

// This test ensures that "sync: WaitGroup is reused before previous Wait has returned" is
// is not raised.
func TestEngine_DisableEnableCompactions_Concurrent(t *testing.T) {
	t.Parallel()

	for _, index := range tsdb.RegisteredIndexes() {
		t.Run(index, func(t *testing.T) {

			e := MustOpenEngine(index)
			defer e.Close()

			var wg sync.WaitGroup
			wg.Add(2)

			go func() {
				defer wg.Done()
				for i := 0; i < 1000; i++ {
					e.SetCompactionsEnabled(true)
					e.SetCompactionsEnabled(false)
				}
			}()

			go func() {
				defer wg.Done()
				for i := 0; i < 1000; i++ {
					e.SetCompactionsEnabled(false)
					e.SetCompactionsEnabled(true)
				}
			}()

			done := make(chan struct{})
			go func() {
				wg.Wait()
				close(done)
			}()

			// Wait for waitgroup or fail if it takes too long.
			select {
			case <-time.NewTimer(30 * time.Second).C:
				t.Fatalf("timed out after 30 seconds waiting for waitgroup")
			case <-done:
			}
		})
	}

}

func TestEngine_WritePointsWithStats(t *testing.T) {
	// Create a few points.
	points := []models.Point{
		MustParsePointString("cpu,host=A value=1.1 1000000000"),
		MustParsePointString("cpu,host=B value=1.2,value2=8 2000000000"),
	}

	expectedPoints, expectedValues := int64(2), int64(3)

	for _, index := range tsdb.RegisteredIndexes() {
		t.Run(index, func(t *testing.T) {
			e := MustOpenEngine(index)

			var numPoints, numValues int64
			var tracker tsdb.StatsTracker
			tracker.AddedPoints = func(points, values int64) {
				numPoints += points
				numValues += values
			}

			var mPoints, mValues int64
			var wrongMeasurement *string
			tracker.AddedMeasurementPoints = func(measurement []byte, points, values int64) {
				if string(measurement) != "cpu" {
					wrongMeasurement = new(string)
					*wrongMeasurement = string(measurement)
				}
				mPoints += points
				mValues += values
			}

			if err := e.WritePoints(points, tracker); err != nil {
				t.Fatalf("failed to write points: %v", err)
			}

			if wrongMeasurement != nil {
				t.Fatalf("Expected only to have cpu measurements, got %s", string(*wrongMeasurement))
			}

			if got, expected := numPoints, expectedPoints; got != expected {
				t.Fatalf("Expected stats to return %d points; got %d", expected, got)
			}

			if got, expected := numValues, expectedValues; got != expected {
				t.Fatalf("Expected stats to return %d points; got %d", expected, got)
			}

			if got, expected := mPoints, expectedPoints; got != expected {
				t.Fatalf("Expected stats to return %d points; got %d", expected, got)
			}

			if got, expected := mValues, expectedValues; got != expected {
				t.Fatalf("Expected stats to return %d points; got %d", expected, got)
			}
		})
	}
}

func TestEngine_WritePoints_TypeConflict(t *testing.T) {
	os.Setenv("INFLUXDB_SERIES_TYPE_CHECK_ENABLED", "1")
	defer os.Unsetenv("INFLUXDB_SERIES_TYPE_CHECK_ENABLED")

	for _, index := range tsdb.RegisteredIndexes() {
		t.Run(index, func(t *testing.T) {

			e := MustOpenEngine(index)
			defer e.Close()

			if err := e.WritePointsString(
				`cpu,host=A value=1.1 1`,
				`cpu,host=A value=1i 2`,
			); err == nil {
				t.Fatalf("expected field type conflict")
			} else if err != tsdb.ErrFieldTypeConflict {
				t.Fatalf("error mismatch: got %v, exp %v", err, tsdb.ErrFieldTypeConflict)
			}

			// Series type should be a float
			got, err := e.Type([]byte(tsm1.SeriesFieldKey("cpu,host=A", "value")))
			if err != nil {
				t.Fatalf("unexpected error getting field type: %v", err)
			}

			if exp := models.Float; got != exp {
				t.Fatalf("field type mismatch: got %v, exp %v", got, exp)
			}

			values := e.Cache.Values([]byte(tsm1.SeriesFieldKey("cpu,host=A", "value")))
			if got, exp := len(values), 1; got != exp {
				t.Fatalf("values len mismatch: got %v, exp %v", got, exp)
			}
		})
	}
}

func TestEngine_WritePoints_Reload(t *testing.T) {
	t.Skip("Disabled until INFLUXDB_SERIES_TYPE_CHECK_ENABLED is enabled by default")

	for _, index := range tsdb.RegisteredIndexes() {
		t.Run(index, func(t *testing.T) {

			e := MustOpenEngine(index)
			defer e.Close()

			if err := e.WritePointsString(
				`cpu,host=A value=1.1 1`,
			); err != nil {
				t.Fatalf("expected field type conflict")
			}

			// Series type should be a float
			got, err := e.Type([]byte(tsm1.SeriesFieldKey("cpu,host=A", "value")))
			if err != nil {
				t.Fatalf("unexpected error getting field type: %v", err)
			}

			if exp := models.Float; got != exp {
				t.Fatalf("field type mismatch: got %v, exp %v", got, exp)
			}

			if err := e.WriteSnapshot(); err != nil {
				t.Fatalf("unexpected error writing snapshot: %v", err)
			}

			if err := e.Reopen(); err != nil {
				t.Fatalf("unexpected error reopning engine: %v", err)
			}

			if err := e.WritePointsString(
				`cpu,host=A value=1i 1`,
			); err != tsdb.ErrFieldTypeConflict {
				t.Fatalf("expected field type conflict: got %v", err)
			}
		})
	}
}

func TestEngine_Invalid_UTF8(t *testing.T) {
	for _, index := range tsdb.RegisteredIndexes() {
		t.Run(index, func(t *testing.T) {
			name := []byte{255, 112, 114, 111, 99} // A known invalid UTF-8 string
			field := []byte{255, 110, 101, 116}    // A known invalid UTF-8 string
			p := MustParsePointString(fmt.Sprintf("%s,host=A %s=1.1 6000000000", name, field))

			e, err := NewEngine(index)
			if err != nil {
				t.Fatal(err)
			}

			// mock the planner so compactions don't run during the test
			e.CompactionPlan = &mockPlanner{}
			if err := e.Open(); err != nil {
				t.Fatal(err)
			}
			defer e.Close()

			if err := e.CreateSeriesIfNotExists(p.Key(), p.Name(), p.Tags(), notrack); err != nil {
				t.Fatalf("create series index error: %v", err)
			}

			if err := e.WritePoints([]models.Point{p}, tsdb.NoopStatsTracker()); err != nil {
				t.Fatalf("failed to write points: %s", err.Error())
			}

			// Re-open the engine
			if err := e.Reopen(); err != nil {
				t.Fatal(err)
			}
		})
	}
}
func BenchmarkEngine_WritePoints(b *testing.B) {
	batchSizes := []int{10, 100, 1000, 5000, 10000}
	for _, sz := range batchSizes {
		for _, index := range tsdb.RegisteredIndexes() {
			e := MustOpenEngine(index)
			e.MeasurementFields([]byte("cpu")).CreateFieldIfNotExists("value", influxql.Float)
			pp := make([]models.Point, 0, sz)
			for i := 0; i < sz; i++ {
				p := MustParsePointString(fmt.Sprintf("cpu,host=%d value=1.2", i))
				pp = append(pp, p)
			}

			b.Run(fmt.Sprintf("%s_%d", index, sz), func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					err := e.WritePoints(pp, tsdb.NoopStatsTracker())
					if err != nil {
						b.Fatal(err)
					}
				}
			})
			e.Close()
		}
	}
}

func BenchmarkEngine_WritePoints_Parallel(b *testing.B) {
	batchSizes := []int{1000, 5000, 10000, 25000, 50000, 75000, 100000, 200000}
	for _, sz := range batchSizes {
		for _, index := range tsdb.RegisteredIndexes() {
			e := MustOpenEngine(index)
			e.MeasurementFields([]byte("cpu")).CreateFieldIfNotExists("value", influxql.Float)

			cpus := runtime.GOMAXPROCS(0)
			pp := make([]models.Point, 0, sz*cpus)
			for i := 0; i < sz*cpus; i++ {
				p := MustParsePointString(fmt.Sprintf("cpu,host=%d value=1.2,other=%di", i, i))
				pp = append(pp, p)
			}

			b.Run(fmt.Sprintf("%s_%d", index, sz), func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					var wg sync.WaitGroup
					errC := make(chan error)
					for i := 0; i < cpus; i++ {
						wg.Add(1)
						go func(i int) {
							defer wg.Done()
							from, to := i*sz, (i+1)*sz
							err := e.WritePoints(pp[from:to], tsdb.NoopStatsTracker())
							if err != nil {
								errC <- err
								return
							}
						}(i)
					}

					go func() {
						wg.Wait()
						close(errC)
					}()

					for err := range errC {
						if err != nil {
							b.Error(err)
						}
					}
				}
			})
			e.Close()
		}
	}
}

var benchmarks = []struct {
	name string
	opt  query.IteratorOptions
}{
	{
		name: "Count",
		opt: query.IteratorOptions{
			Expr:      influxql.MustParseExpr("count(value)"),
			Ascending: true,
			StartTime: influxql.MinTime,
			EndTime:   influxql.MaxTime,
		},
	},
	{
		name: "First",
		opt: query.IteratorOptions{
			Expr:      influxql.MustParseExpr("first(value)"),
			Ascending: true,
			StartTime: influxql.MinTime,
			EndTime:   influxql.MaxTime,
		},
	},
	{
		name: "Last",
		opt: query.IteratorOptions{
			Expr:      influxql.MustParseExpr("last(value)"),
			Ascending: true,
			StartTime: influxql.MinTime,
			EndTime:   influxql.MaxTime,
		},
	},
	{
		name: "Limit",
		opt: query.IteratorOptions{
			Expr:      influxql.MustParseExpr("value"),
			Ascending: true,
			StartTime: influxql.MinTime,
			EndTime:   influxql.MaxTime,
			Limit:     10,
		},
	},
}

var benchmarkVariants = []struct {
	name   string
	modify func(opt query.IteratorOptions) query.IteratorOptions
}{
	{
		name: "All",
		modify: func(opt query.IteratorOptions) query.IteratorOptions {
			return opt
		},
	},
	{
		name: "GroupByTime_1m-1h",
		modify: func(opt query.IteratorOptions) query.IteratorOptions {
			opt.StartTime = 0
			opt.EndTime = int64(time.Hour) - 1
			opt.Interval = query.Interval{
				Duration: time.Minute,
			}
			return opt
		},
	},
	{
		name: "GroupByTime_1h-1d",
		modify: func(opt query.IteratorOptions) query.IteratorOptions {
			opt.StartTime = 0
			opt.EndTime = int64(24*time.Hour) - 1
			opt.Interval = query.Interval{
				Duration: time.Hour,
			}
			return opt
		},
	},
	{
		name: "GroupByTime_1m-1d",
		modify: func(opt query.IteratorOptions) query.IteratorOptions {
			opt.StartTime = 0
			opt.EndTime = int64(24*time.Hour) - 1
			opt.Interval = query.Interval{
				Duration: time.Minute,
			}
			return opt
		},
	},
	{
		name: "GroupByHost",
		modify: func(opt query.IteratorOptions) query.IteratorOptions {
			opt.Dimensions = []string{"host"}
			return opt
		},
	},
	{
		name: "GroupByHostAndTime_1m-1h",
		modify: func(opt query.IteratorOptions) query.IteratorOptions {
			opt.Dimensions = []string{"host"}
			opt.StartTime = 0
			opt.EndTime = int64(time.Hour) - 1
			opt.Interval = query.Interval{
				Duration: time.Minute,
			}
			return opt
		},
	},
	{
		name: "GroupByHostAndTime_1h-1d",
		modify: func(opt query.IteratorOptions) query.IteratorOptions {
			opt.Dimensions = []string{"host"}
			opt.StartTime = 0
			opt.EndTime = int64(24*time.Hour) - 1
			opt.Interval = query.Interval{
				Duration: time.Hour,
			}
			return opt
		},
	},
	{
		name: "GroupByHostAndTime_1m-1d",
		modify: func(opt query.IteratorOptions) query.IteratorOptions {
			opt.Dimensions = []string{"host"}
			opt.StartTime = 0
			opt.EndTime = int64(24*time.Hour) - 1
			opt.Interval = query.Interval{
				Duration: time.Hour,
			}
			return opt
		},
	},
}

func BenchmarkEngine_CreateIterator(b *testing.B) {
	engines := make([]*benchmarkEngine, len(sizes))
	for i, size := range sizes {
		engines[i] = MustInitDefaultBenchmarkEngine(size.name, size.sz)
	}

	for _, tt := range benchmarks {
		for _, variant := range benchmarkVariants {
			name := tt.name + "_" + variant.name
			opt := variant.modify(tt.opt)
			b.Run(name, func(b *testing.B) {
				for _, e := range engines {
					b.Run(e.Name, func(b *testing.B) {
						b.ReportAllocs()
						for i := 0; i < b.N; i++ {
							itr, err := e.CreateIterator(context.Background(), "cpu", opt)
							if err != nil {
								b.Fatal(err)
							}
							query.DrainIterator(itr)
						}
					})
				}
			})
		}
	}
}

type benchmarkEngine struct {
	*Engine
	Name   string
	PointN int
}

var (
	hostNames = []string{"A", "B", "C", "D", "E", "F", "G", "H", "I", "J"}
	sizes     = []struct {
		name string
		sz   int
	}{
		{name: "1K", sz: 1000},
		{name: "100K", sz: 100000},
		{name: "1M", sz: 1000000},
	}
)

// MustInitDefaultBenchmarkEngine creates a new engine using the default index
// and fills it with points.  Reuses previous engine if the same parameters
// were used.
func MustInitDefaultBenchmarkEngine(name string, pointN int) *benchmarkEngine {
	const batchSize = 1000
	if pointN%batchSize != 0 {
		panic(fmt.Sprintf("point count (%d) must be a multiple of batch size (%d)", pointN, batchSize))
	}

	e := MustOpenEngine(tsdb.DefaultIndex)

	// Initialize metadata.
	e.MeasurementFields([]byte("cpu")).CreateFieldIfNotExists("value", influxql.Float)
	e.CreateSeriesIfNotExists([]byte("cpu,host=A"), []byte("cpu"), models.NewTags(map[string]string{"host": "A"}), notrack)

	// Generate time ascending points with jitterred time & value.
	rand := rand.New(rand.NewSource(0))
	for i := 0; i < pointN; i += batchSize {
		var buf bytes.Buffer
		for j := 0; j < batchSize; j++ {
			fmt.Fprintf(&buf, "cpu,host=%s value=%d %d",
				hostNames[j%len(hostNames)],
				100+rand.Intn(50)-25,
				(time.Duration(i+j)*time.Second)+(time.Duration(rand.Intn(500)-250)*time.Millisecond),
			)
			if j != pointN-1 {
				fmt.Fprint(&buf, "\n")
			}
		}

		if err := e.WritePointsString(buf.String()); err != nil {
			panic(err)
		}
	}

	if err := e.WriteSnapshot(); err != nil {
		panic(err)
	}

	// Force garbage collection.
	runtime.GC()

	// Save engine reference for reuse.
	return &benchmarkEngine{
		Engine: e,
		Name:   name,
		PointN: pointN,
	}
}

// Engine is a test wrapper for tsm1.Engine.
type Engine struct {
	*tsm1.Engine
	root      string
	indexPath string
	indexType string
	index     tsdb.Index
	sfile     *tsdb.SeriesFile
}

// NewEngine returns a new instance of Engine at a temporary location.
func NewEngine(index string) (*Engine, error) {
	root, err := os.MkdirTemp("", "tsm1-")
	if err != nil {
		panic(err)
	}

	db := "db0"
	dbPath := filepath.Join(root, "data", db)

	if err := os.MkdirAll(dbPath, os.ModePerm); err != nil {
		return nil, err
	}

	// Setup series file.
	sfile := tsdb.NewSeriesFile(filepath.Join(dbPath, tsdb.SeriesFileDirectory))
	sfile.Logger = logger.New(os.Stdout)
	if err = sfile.Open(); err != nil {
		return nil, err
	}

	opt := tsdb.NewEngineOptions()
	opt.IndexVersion = index
	if index == tsdb.InmemIndexName {
		opt.InmemIndex = inmem.NewIndex(db, sfile)
	}
	// Initialise series id sets. Need to do this as it's normally done at the
	// store level.
	seriesIDs := tsdb.NewSeriesIDSet()
	opt.SeriesIDSets = seriesIDSets([]*tsdb.SeriesIDSet{seriesIDs})

	idxPath := filepath.Join(dbPath, "index")
	idx := tsdb.MustOpenIndex(1, db, idxPath, seriesIDs, sfile, opt)

	tsm1Engine := tsm1.NewEngine(1, idx, filepath.Join(root, "data"), filepath.Join(root, "wal"), sfile, opt).(*tsm1.Engine)

	return &Engine{
		Engine:    tsm1Engine,
		root:      root,
		indexPath: idxPath,
		indexType: index,
		index:     idx,
		sfile:     sfile,
	}, nil
}

// MustOpenEngine returns a new, open instance of Engine.
func MustOpenEngine(index string) *Engine {
	e, err := NewEngine(index)
	if err != nil {
		panic(err)
	}

	if err := e.Open(); err != nil {
		panic(err)
	}
	return e
}

// Close closes the engine and removes all underlying data.
func (e *Engine) Close() error {
	return e.close(true)
}

func (e *Engine) close(cleanup bool) error {
	if e.index != nil {
		e.index.Close()
	}

	if e.sfile != nil {
		e.sfile.Close()
	}

	defer func() {
		if cleanup {
			os.RemoveAll(e.root)
		}
	}()
	return e.Engine.Close()
}

// Reopen closes and reopens the engine.
func (e *Engine) Reopen() error {
	// Close engine without removing underlying engine data.
	if err := e.close(false); err != nil {
		return err
	}

	// Re-open series file. Must create a new series file using the same data.
	e.sfile = tsdb.NewSeriesFile(e.sfile.Path())
	if err := e.sfile.Open(); err != nil {
		return err
	}

	db := path.Base(e.root)
	opt := tsdb.NewEngineOptions()
	opt.InmemIndex = inmem.NewIndex(db, e.sfile)

	// Re-initialise the series id set
	seriesIDSet := tsdb.NewSeriesIDSet()
	opt.SeriesIDSets = seriesIDSets([]*tsdb.SeriesIDSet{seriesIDSet})

	// Re-open index.
	e.index = tsdb.MustOpenIndex(1, db, e.indexPath, seriesIDSet, e.sfile, opt)

	// Re-initialize engine.
	e.Engine = tsm1.NewEngine(1, e.index, filepath.Join(e.root, "data"), filepath.Join(e.root, "wal"), e.sfile, opt).(*tsm1.Engine)

	// Reopen engine
	if err := e.Engine.Open(); err != nil {
		return err
	}

	// Reload series data into index (no-op on TSI).
	return e.LoadMetadataIndex(1, e.index)
}

// SeriesIDSet provides access to the underlying series id bitset in the engine's
// index. It will panic if the underlying index does not have a SeriesIDSet
// method.
func (e *Engine) SeriesIDSet() *tsdb.SeriesIDSet {
	return e.index.SeriesIDSet()
}

// AddSeries adds the provided series data to the index and writes a point to
// the engine with default values for a field and a time of now.
func (e *Engine) AddSeries(name string, tags map[string]string) error {
	point, err := models.NewPoint(name, models.NewTags(tags), models.Fields{"v": 1.0}, time.Now())
	if err != nil {
		return err
	}
	return e.writePoints(point)
}

// WritePointsString calls WritePointsString on the underlying engine, but also
// adds the associated series to the index.
func (e *Engine) WritePointsString(ptstr ...string) error {
	points, err := models.ParsePointsString(strings.Join(ptstr, "\n"))
	if err != nil {
		return err
	}
	return e.writePoints(points...)
}

// writePoints adds the series for the provided points to the index, and writes
// the point data to the engine.
func (e *Engine) writePoints(points ...models.Point) error {
	for _, point := range points {
		// Write into the index.
		if err := e.Engine.CreateSeriesIfNotExists(point.Key(), point.Name(), point.Tags(), notrack); err != nil {
			return err
		}
	}
	// Write the points into the cache/wal.
	return e.WritePoints(points, tsdb.NoopStatsTracker())
}

// MustAddSeries calls AddSeries, panicking if there is an error.
func (e *Engine) MustAddSeries(name string, tags map[string]string) {
	if err := e.AddSeries(name, tags); err != nil {
		panic(err)
	}
}

// MustWriteSnapshot forces a snapshot of the engine. Panic on error.
func (e *Engine) MustWriteSnapshot() {
	if err := e.WriteSnapshot(); err != nil {
		panic(err)
	}
}

// SeriesFile is a test wrapper for tsdb.SeriesFile.
type SeriesFile struct {
	*tsdb.SeriesFile
}

// NewSeriesFile returns a new instance of SeriesFile with a temporary file path.
func NewSeriesFile() *SeriesFile {
	dir, err := os.MkdirTemp("", "tsdb-series-file-")
	if err != nil {
		panic(err)
	}
	return &SeriesFile{SeriesFile: tsdb.NewSeriesFile(dir)}
}

// MustOpenSeriesFile returns a new, open instance of SeriesFile. Panic on error.
func MustOpenSeriesFile() *SeriesFile {
	f := NewSeriesFile()
	if err := f.Open(); err != nil {
		panic(err)
	}
	return f
}

// Close closes the log file and removes it from disk.
func (f *SeriesFile) Close() {
	defer os.RemoveAll(f.Path())
	if err := f.SeriesFile.Close(); err != nil {
		panic(err)
	}
}

// MustParsePointsString parses points from a string. Panic on error.
func MustParsePointsString(buf string) []models.Point {
	a, err := models.ParsePointsString(buf)
	if err != nil {
		panic(err)
	}
	return a
}

// MustParsePointString parses the first point from a string. Panic on error.
func MustParsePointString(buf string) models.Point { return MustParsePointsString(buf)[0] }

type mockPlanner struct{}

func (m *mockPlanner) Plan(lastWrite time.Time) ([]tsm1.CompactionGroup, int64) { return nil, 0 }
func (m *mockPlanner) PlanLevel(level int) ([]tsm1.CompactionGroup, int64)      { return nil, 0 }
func (m *mockPlanner) PlanOptimize() ([]tsm1.CompactionGroup, int64, int64)     { return nil, 0, 0 }
func (m *mockPlanner) Release(groups []tsm1.CompactionGroup)                    {}
func (m *mockPlanner) FullyCompacted() (bool, string)                           { return false, "not compacted" }
func (m *mockPlanner) ForceFull()                                               {}
func (m *mockPlanner) SetFileStore(fs *tsm1.FileStore)                          {}

// ParseTags returns an instance of Tags for a comma-delimited list of key/values.
func ParseTags(s string) query.Tags {
	m := make(map[string]string)
	for _, kv := range strings.Split(s, ",") {
		a := strings.Split(kv, "=")
		m[a[0]] = a[1]
	}
	return query.NewTags(m)
}

type seriesIterator struct {
	keys [][]byte
}

type series struct {
	name    []byte
	tags    models.Tags
	deleted bool
}

func (s series) Name() []byte        { return s.name }
func (s series) Tags() models.Tags   { return s.tags }
func (s series) Deleted() bool       { return s.deleted }
func (s series) Expr() influxql.Expr { return nil }

func (itr *seriesIterator) Close() error { return nil }

func (itr *seriesIterator) Next() (tsdb.SeriesElem, error) {
	if len(itr.keys) == 0 {
		return nil, nil
	}
	name, tags := models.ParseKeyBytes(itr.keys[0])
	s := series{name: name, tags: tags}
	itr.keys = itr.keys[1:]
	return s, nil
}

type seriesIDSets []*tsdb.SeriesIDSet

func (a seriesIDSets) ForEach(f func(ids *tsdb.SeriesIDSet)) error {
	for _, v := range a {
		f(v)
	}
	return nil
}
