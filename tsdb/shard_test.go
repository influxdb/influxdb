package tsdb_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"path"
	"path/filepath"
	"reflect"
	"regexp"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/davecgh/go-spew/spew"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/influxdata/influxdb/internal"
	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/pkg/deep"
	"github.com/influxdata/influxdb/pkg/testing/assert"
	"github.com/influxdata/influxdb/query"
	"github.com/influxdata/influxdb/tsdb"
	_ "github.com/influxdata/influxdb/tsdb/engine"
	"github.com/influxdata/influxdb/tsdb/engine/tsm1"
	_ "github.com/influxdata/influxdb/tsdb/index"
	"github.com/influxdata/influxdb/tsdb/index/inmem"
	"github.com/influxdata/influxql"
	assert2 "github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestShardWriteAndIndex(t *testing.T) {
	tmpDir, _ := os.MkdirTemp("", "shard_test")
	defer os.RemoveAll(tmpDir)
	tmpShard := filepath.Join(tmpDir, "shard")
	tmpWal := filepath.Join(tmpDir, "wal")

	sfile := MustOpenSeriesFile()
	defer sfile.Close()

	opts := tsdb.NewEngineOptions()
	opts.Config.WALDir = filepath.Join(tmpDir, "wal")
	opts.InmemIndex = inmem.NewIndex(filepath.Base(tmpDir), sfile.SeriesFile)

	sh := tsdb.NewShard(1, tmpShard, tmpWal, sfile.SeriesFile, opts)

	// Calling WritePoints when the engine is not open will return
	// ErrEngineClosed.
	got := sh.WritePoints(nil, tsdb.NoopStatsTracker())
	if exp := tsdb.ErrEngineClosed; got != exp {
		t.Fatalf("got %v, expected %v", got, exp)
	}

	if err := sh.Open(); err != nil {
		t.Fatalf("error opening shard: %s", err.Error())
	}

	pt := models.MustNewPoint(
		"cpu",
		models.Tags{{Key: []byte("host"), Value: []byte("server")}},
		map[string]interface{}{"value": 1.0},
		time.Unix(1, 2),
	)

	err := sh.WritePoints([]models.Point{pt}, tsdb.NoopStatsTracker())
	if err != nil {
		t.Fatalf(err.Error())
	}

	pt.SetTime(time.Unix(2, 3))
	err = sh.WritePoints([]models.Point{pt}, tsdb.NoopStatsTracker())
	if err != nil {
		t.Fatalf(err.Error())
	}

	validateIndex := func() {
		cnt := sh.SeriesN()
		if got, exp := cnt, int64(1); got != exp {
			t.Fatalf("got %v series, exp %v series in index", got, exp)
		}
	}

	validateIndex()

	// ensure the index gets loaded after closing and opening the shard
	sh.Close()

	sh = tsdb.NewShard(1, tmpShard, tmpWal, sfile.SeriesFile, opts)
	if err := sh.Open(); err != nil {
		t.Fatalf("error opening shard: %s", err.Error())
	}

	validateIndex()

	// and ensure that we can still write data
	pt.SetTime(time.Unix(2, 6))
	err = sh.WritePoints([]models.Point{pt}, tsdb.NoopStatsTracker())
	if err != nil {
		t.Fatalf(err.Error())
	}
}

func TestShard_Open_CorruptFieldsIndex(t *testing.T) {
	tmpDir, _ := os.MkdirTemp("", "shard_test")
	defer os.RemoveAll(tmpDir)
	tmpShard := filepath.Join(tmpDir, "shard")
	tmpWal := filepath.Join(tmpDir, "wal")

	sfile := MustOpenSeriesFile()
	defer sfile.Close()

	opts := tsdb.NewEngineOptions()
	opts.Config.WALDir = filepath.Join(tmpDir, "wal")
	opts.InmemIndex = inmem.NewIndex(filepath.Base(tmpDir), sfile.SeriesFile)

	sh := tsdb.NewShard(1, tmpShard, tmpWal, sfile.SeriesFile, opts)

	// Calling WritePoints when the engine is not open will return
	// ErrEngineClosed.
	got := sh.WritePoints(nil, tsdb.NoopStatsTracker())
	if exp := tsdb.ErrEngineClosed; got != exp {
		t.Fatalf("got %v, expected %v", got, exp)
	}

	if err := sh.Open(); err != nil {
		t.Fatalf("error opening shard: %s", err.Error())
	}

	pt := models.MustNewPoint(
		"cpu",
		models.Tags{{Key: []byte("host"), Value: []byte("server")}},
		map[string]interface{}{"value": 1.0},
		time.Unix(1, 2),
	)

	err := sh.WritePoints([]models.Point{pt}, tsdb.NoopStatsTracker())
	if err != nil {
		t.Fatalf(err.Error())
	}

	if err := sh.Close(); err != nil {
		t.Fatalf("close shard error: %v", err)
	}

	path := filepath.Join(tmpShard, "fields.idx")
	if err := os.Truncate(path, 6); err != nil {
		t.Fatalf("truncate shard error: %v", err)
	}

	if err := sh.Open(); err != nil {
		t.Fatalf("error opening shard: %s", err.Error())
	}
}

func TestMaxSeriesLimit(t *testing.T) {
	tmpDir, _ := os.MkdirTemp("", "shard_test")
	defer os.RemoveAll(tmpDir)
	tmpShard := filepath.Join(tmpDir, "db", "rp", "1")
	tmpWal := filepath.Join(tmpDir, "wal")

	sfile := MustOpenSeriesFile()
	defer sfile.Close()

	opts := tsdb.NewEngineOptions()
	opts.Config.WALDir = filepath.Join(tmpDir, "wal")
	opts.Config.MaxSeriesPerDatabase = 1000
	index := inmem.NewIndex(filepath.Base(tmpDir), sfile.SeriesFile)
	opts.InmemIndex = index

	sh := tsdb.NewShard(1, tmpShard, tmpWal, sfile.SeriesFile, opts)

	if err := sh.Open(); err != nil {
		t.Fatalf("error opening shard: %s", err.Error())
	}
	defer func() {
		if err := sh.Close(); err != nil {
			t.Fatalf("error on Shard.Close: %v", err)
		}
	}()

	// Writing 1K series should succeed.
	points := []models.Point{}

	for i := 0; i < 1000; i++ {
		pt := models.MustNewPoint(
			"cpu",
			models.Tags{{Key: []byte("host"), Value: []byte(fmt.Sprintf("server%d", i))}},
			map[string]interface{}{"value": 1.0},
			time.Unix(1, 2),
		)
		points = append(points, pt)
	}

	err := sh.WritePoints(points, tsdb.NoopStatsTracker())
	if err != nil {
		t.Fatalf(err.Error())
	}

	// Writing one more series should exceed the series limit.
	pt := models.MustNewPoint(
		"cpu",
		models.Tags{{Key: []byte("host"), Value: []byte("server9999")}},
		map[string]interface{}{"value": 1.0},
		time.Unix(1, 2),
	)

	err = sh.WritePoints([]models.Point{pt}, tsdb.NoopStatsTracker())
	if err == nil {
		t.Fatal("expected error")
	} else if exp, got :=
		fmt.Sprintf(`partial write: max-series-per-database exceeded database=%s limit=1000 series=1000 keys=1 dropped=1 for database: db for retention policy: rp`, index.Database()), err.Error(); exp != got {
		t.Fatalf("unexpected error message:\n\texp = %s\n\tgot = %s", exp, got)
	} else {
		st := sh.Statistics(map[string]string{})
		found := false
		for _, stat := range st {
			if stat.Name == "shard" {
				checkInt64Stat(t, stat, "writePointsOk", int64(opts.Config.MaxSeriesPerDatabase))
				checkInt64Stat(t, stat, "writePointsDropped", 1)
				found = true
			}
		}
		if !found {
			t.Fatalf("statistics for shard not found")
		}
	}
}

func checkInt64Stat(t *testing.T, stat models.Statistic, name string, exp int64) {
	if v, ok := stat.Values[name]; !ok {
		t.Fatalf("value %s not found in statistic %s", name, stat.Name)
	} else if got, ok := v.(int64); !ok {
		t.Fatalf("value %s in statistic %s not an int64", name, stat.Name)
	} else if exp != got {
		t.Fatalf("value %s in statistic %s expected: %d, got: %d", name, stat.Name, exp, got)
	}
}

func TestShard_MaxTagValuesLimit(t *testing.T) {
	tmpDir, _ := os.MkdirTemp("", "shard_test")
	defer os.RemoveAll(tmpDir)
	tmpShard := filepath.Join(tmpDir, "db", "rp", "1")
	tmpWal := filepath.Join(tmpDir, "wal")

	sfile := MustOpenSeriesFile()
	defer sfile.Close()

	opts := tsdb.NewEngineOptions()
	opts.Config.WALDir = filepath.Join(tmpDir, "wal")
	opts.Config.MaxValuesPerTag = 1000
	opts.InmemIndex = inmem.NewIndex(filepath.Base(tmpDir), sfile.SeriesFile)

	sh := tsdb.NewShard(1, tmpShard, tmpWal, sfile.SeriesFile, opts)

	if err := sh.Open(); err != nil {
		t.Fatalf("error opening shard: %s", err.Error())
	}

	// Writing 1K series should succeed.
	points := []models.Point{}

	for i := 0; i < 1000; i++ {
		pt := models.MustNewPoint(
			"cpu",
			models.Tags{{Key: []byte("host"), Value: []byte(fmt.Sprintf("server%d", i))}},
			map[string]interface{}{"value": 1.0},
			time.Unix(1, 2),
		)
		points = append(points, pt)
	}

	err := sh.WritePoints(points, tsdb.NoopStatsTracker())
	if err != nil {
		t.Fatalf(err.Error())
	}

	// Writing one more series should exceed the series limit.
	pt := models.MustNewPoint(
		"cpu",
		models.Tags{{Key: []byte("host"), Value: []byte("server9999")}},
		map[string]interface{}{"value": 1.0},
		time.Unix(1, 2),
	)

	err = sh.WritePoints([]models.Point{pt}, tsdb.NoopStatsTracker())
	if err == nil {
		t.Fatal("expected error")
	} else if exp, got := `partial write: max-values-per-tag limit exceeded (1000/1000): measurement="cpu" tag="host" value="server9999" dropped=1 for database: db for retention policy: rp`, err.Error(); exp != got {
		t.Fatalf("unexpected error message:\n\texp = %s\n\tgot = %s", exp, got)
	}

	sh.Close()
}

func TestWriteTimeTag(t *testing.T) {
	tmpDir, _ := os.MkdirTemp("", "shard_test")
	defer os.RemoveAll(tmpDir)
	tmpShard := filepath.Join(tmpDir, "shard")
	tmpWal := filepath.Join(tmpDir, "wal")

	sfile := MustOpenSeriesFile()
	defer sfile.Close()

	opts := tsdb.NewEngineOptions()
	opts.Config.WALDir = filepath.Join(tmpDir, "wal")
	opts.InmemIndex = inmem.NewIndex(filepath.Base(tmpDir), sfile.SeriesFile)

	sh := tsdb.NewShard(1, tmpShard, tmpWal, sfile.SeriesFile, opts)
	if err := sh.Open(); err != nil {
		t.Fatalf("error opening shard: %s", err.Error())
	}
	defer sh.Close()

	pt := models.MustNewPoint(
		"cpu",
		models.NewTags(map[string]string{}),
		map[string]interface{}{"time": 1.0},
		time.Unix(1, 2),
	)

	if err := sh.WritePoints([]models.Point{pt}, tsdb.NoopStatsTracker()); err == nil {
		t.Fatal("expected error: got nil")
	}

	pt = models.MustNewPoint(
		"cpu",
		models.NewTags(map[string]string{}),
		map[string]interface{}{"value": 1.0, "time": 1.0},
		time.Unix(1, 2),
	)

	if err := sh.WritePoints([]models.Point{pt}, tsdb.NoopStatsTracker()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	mf := sh.MeasurementFields([]byte("cpu"))
	if mf == nil {
		t.Fatal("expected cpu measurement fields")
	}

	if got, exp := mf.FieldN(), 1; got != exp {
		t.Fatalf("invalid number of field names: got=%v exp=%v", got, exp)
	}
}

func TestWriteTimeField(t *testing.T) {
	tmpDir, _ := os.MkdirTemp("", "shard_test")
	defer os.RemoveAll(tmpDir)
	tmpShard := filepath.Join(tmpDir, "shard")
	tmpWal := filepath.Join(tmpDir, "wal")

	sfile := MustOpenSeriesFile()
	defer sfile.Close()

	opts := tsdb.NewEngineOptions()
	opts.Config.WALDir = filepath.Join(tmpDir, "wal")
	opts.InmemIndex = inmem.NewIndex(filepath.Base(tmpDir), sfile.SeriesFile)

	sh := tsdb.NewShard(1, tmpShard, tmpWal, sfile.SeriesFile, opts)
	if err := sh.Open(); err != nil {
		t.Fatalf("error opening shard: %s", err.Error())
	}
	defer sh.Close()

	pt := models.MustNewPoint(
		"cpu",
		models.NewTags(map[string]string{"time": "now"}),
		map[string]interface{}{"value": 1.0},
		time.Unix(1, 2),
	)

	if err := sh.WritePoints([]models.Point{pt}, tsdb.NoopStatsTracker()); err == nil {
		t.Fatal("expected error: got nil")
	}

	key := models.MakeKey([]byte("cpu"), nil)
	if ok, err := sh.MeasurementExists(key); ok && err == nil {
		t.Fatal("unexpected series")
	}
}

func TestShardWriteAddNewField(t *testing.T) {
	tmpDir, _ := os.MkdirTemp("", "shard_test")
	defer os.RemoveAll(tmpDir)
	tmpShard := filepath.Join(tmpDir, "shard")
	tmpWal := filepath.Join(tmpDir, "wal")

	sfile := MustOpenSeriesFile()
	defer sfile.Close()

	opts := tsdb.NewEngineOptions()
	opts.Config.WALDir = filepath.Join(tmpDir, "wal")
	opts.InmemIndex = inmem.NewIndex(filepath.Base(tmpDir), sfile.SeriesFile)

	sh := tsdb.NewShard(1, tmpShard, tmpWal, sfile.SeriesFile, opts)
	if err := sh.Open(); err != nil {
		t.Fatalf("error opening shard: %s", err.Error())
	}
	defer sh.Close()

	pt := models.MustNewPoint(
		"cpu",
		models.NewTags(map[string]string{"host": "server"}),
		map[string]interface{}{"value": 1.0},
		time.Unix(1, 2),
	)

	err := sh.WritePoints([]models.Point{pt}, tsdb.NoopStatsTracker())
	if err != nil {
		t.Fatalf(err.Error())
	}

	pt = models.MustNewPoint(
		"cpu",
		models.NewTags(map[string]string{"host": "server"}),
		map[string]interface{}{"value": 1.0, "value2": 2.0},
		time.Unix(1, 2),
	)

	err = sh.WritePoints([]models.Point{pt}, tsdb.NoopStatsTracker())
	if err != nil {
		t.Fatalf(err.Error())
	}

	if got, exp := sh.SeriesN(), int64(1); got != exp {
		t.Fatalf("got %d series, exp %d series in index", got, exp)
	}
}

// Tests concurrently writing to the same shard with different field types which
// can trigger a panic when the shard is snapshotted to TSM files.
func TestShard_WritePoints_FieldConflictConcurrent(t *testing.T) {
	if testing.Short() || runtime.GOOS == "windows" {
		t.Skip("Skipping on short and windows")
	}
	tmpDir, _ := os.MkdirTemp("", "shard_test")
	defer os.RemoveAll(tmpDir)
	tmpShard := filepath.Join(tmpDir, "shard")
	tmpWal := filepath.Join(tmpDir, "wal")

	sfile := MustOpenSeriesFile()
	defer sfile.Close()

	opts := tsdb.NewEngineOptions()
	opts.Config.WALDir = filepath.Join(tmpDir, "wal")
	opts.InmemIndex = inmem.NewIndex(filepath.Base(tmpDir), sfile.SeriesFile)
	opts.SeriesIDSets = seriesIDSets([]*tsdb.SeriesIDSet{})

	sh := tsdb.NewShard(1, tmpShard, tmpWal, sfile.SeriesFile, opts)
	if err := sh.Open(); err != nil {
		t.Fatalf("error opening shard: %s", err.Error())
	}
	defer sh.Close()

	points := make([]models.Point, 0, 1000)
	for i := 0; i < cap(points); i++ {
		if i < 500 {
			points = append(points, models.MustNewPoint(
				"cpu",
				models.NewTags(map[string]string{"host": "server"}),
				map[string]interface{}{"value": 1.0},
				time.Unix(int64(i), 0),
			))
		} else {
			points = append(points, models.MustNewPoint(
				"cpu",
				models.NewTags(map[string]string{"host": "server"}),
				map[string]interface{}{"value": int64(1)},
				time.Unix(int64(i), 0),
			))
		}
	}

	var wg sync.WaitGroup
	wg.Add(2)
	errC := make(chan error)
	go func() {
		defer wg.Done()
		for i := 0; i < 50; i++ {
			if err := sh.DeleteMeasurement([]byte("cpu")); err != nil {
				errC <- err
				return
			}

			_ = sh.WritePoints(points[:500], tsdb.NoopStatsTracker())
			if f, err := sh.CreateSnapshot(false); err == nil {
				os.RemoveAll(f)
			}

		}
	}()

	go func() {
		defer wg.Done()
		for i := 0; i < 50; i++ {
			if err := sh.DeleteMeasurement([]byte("cpu")); err != nil {
				errC <- err
				return
			}

			_ = sh.WritePoints(points[500:], tsdb.NoopStatsTracker())
			if f, err := sh.CreateSnapshot(false); err == nil {
				os.RemoveAll(f)
			}
		}
	}()

	go func() {
		wg.Wait()
		close(errC)
	}()

	for err := range errC {
		if err != nil {
			t.Error(err)
		}
	}
}

func TestShard_WritePoints_FieldConflictConcurrentQuery(t *testing.T) {
	t.Skip("https://github.com/influxdata/influxdb/issues/14267")
	if testing.Short() {
		t.Skip()
	}
	tmpDir, _ := os.MkdirTemp("", "shard_test")
	defer os.RemoveAll(tmpDir)
	tmpShard := filepath.Join(tmpDir, "shard")
	tmpWal := filepath.Join(tmpDir, "wal")

	sfile := MustOpenSeriesFile()
	defer sfile.Close()

	opts := tsdb.NewEngineOptions()
	opts.Config.WALDir = filepath.Join(tmpDir, "wal")
	opts.InmemIndex = inmem.NewIndex(filepath.Base(tmpDir), sfile.SeriesFile)
	opts.SeriesIDSets = seriesIDSets([]*tsdb.SeriesIDSet{})

	sh := tsdb.NewShard(1, tmpShard, tmpWal, sfile.SeriesFile, opts)
	if err := sh.Open(); err != nil {
		t.Fatalf("error opening shard: %s", err.Error())
	}
	defer sh.Close()

	// Spin up two goroutines that write points with different field types in reverse
	// order concurrently.  After writing them, query them back.
	errC := make(chan error, 2)
	go func() {
		// Write 250 floats and then ints to the same field
		points := make([]models.Point, 0, 500)
		for i := 0; i < cap(points); i++ {
			if i < 250 {
				points = append(points, models.MustNewPoint(
					"cpu",
					models.NewTags(map[string]string{"host": "server"}),
					map[string]interface{}{"value": 1.0},
					time.Unix(int64(i), 0),
				))
			} else {
				points = append(points, models.MustNewPoint(
					"cpu",
					models.NewTags(map[string]string{"host": "server"}),
					map[string]interface{}{"value": int64(1)},
					time.Unix(int64(i), 0),
				))
			}
		}

		for i := 0; i < 500; i++ {
			if err := sh.DeleteMeasurement([]byte("cpu")); err != nil {
				errC <- err
			}

			sh.WritePoints(points, tsdb.NoopStatsTracker())
			m := &influxql.Measurement{Name: "cpu"}
			iter, err := sh.CreateIterator(context.Background(), m, query.IteratorOptions{
				Expr:       influxql.MustParseExpr(`value`),
				Aux:        []influxql.VarRef{{Val: "value"}},
				Dimensions: []string{},
				Ascending:  true,
				StartTime:  influxql.MinTime,
				EndTime:    influxql.MaxTime,
			})
			if err != nil {
				errC <- err
			}

			switch itr := iter.(type) {
			case query.IntegerIterator:
				p, err := itr.Next()
				for p != nil && err == nil {
					p, err = itr.Next()
				}
				iter.Close()

			case query.FloatIterator:
				p, err := itr.Next()
				for p != nil && err == nil {
					p, err = itr.Next()
				}
				iter.Close()

			}

		}
		errC <- nil
	}()

	go func() {
		// Write 250 ints and then floats to the same field
		points := make([]models.Point, 0, 500)
		for i := 0; i < cap(points); i++ {
			if i < 250 {
				points = append(points, models.MustNewPoint(
					"cpu",
					models.NewTags(map[string]string{"host": "server"}),
					map[string]interface{}{"value": int64(1)},
					time.Unix(int64(i), 0),
				))
			} else {
				points = append(points, models.MustNewPoint(
					"cpu",
					models.NewTags(map[string]string{"host": "server"}),
					map[string]interface{}{"value": 1.0},
					time.Unix(int64(i), 0),
				))
			}
		}
		for i := 0; i < 500; i++ {
			if err := sh.DeleteMeasurement([]byte("cpu")); err != nil {
				errC <- err
			}

			sh.WritePoints(points, tsdb.NoopStatsTracker())
			m := &influxql.Measurement{Name: "cpu"}
			iter, err := sh.CreateIterator(context.Background(), m, query.IteratorOptions{
				Expr:       influxql.MustParseExpr(`value`),
				Aux:        []influxql.VarRef{{Val: "value"}},
				Dimensions: []string{},
				Ascending:  true,
				StartTime:  influxql.MinTime,
				EndTime:    influxql.MaxTime,
			})
			if err != nil {
				errC <- err
			}

			switch itr := iter.(type) {
			case query.IntegerIterator:
				p, err := itr.Next()
				for p != nil && err == nil {
					p, err = itr.Next()
				}
				iter.Close()
			case query.FloatIterator:
				p, err := itr.Next()
				for p != nil && err == nil {
					p, err = itr.Next()
				}
				iter.Close()
			}
		}
		errC <- nil
	}()

	// Check results
	for i := 0; i < cap(errC); i++ {
		if err := <-errC; err != nil {
			t.Fatal(err)
		}
	}
}

// Ensures that when a shard is closed, it removes any series meta-data
// from the index.
func TestShard_Close_RemoveIndex(t *testing.T) {
	tmpDir, _ := os.MkdirTemp("", "shard_test")
	defer os.RemoveAll(tmpDir)
	tmpShard := filepath.Join(tmpDir, "shard")
	tmpWal := filepath.Join(tmpDir, "wal")

	sfile := MustOpenSeriesFile()
	defer sfile.Close()

	opts := tsdb.NewEngineOptions()
	opts.Config.WALDir = filepath.Join(tmpDir, "wal")
	opts.InmemIndex = inmem.NewIndex(filepath.Base(tmpDir), sfile.SeriesFile)

	sh := tsdb.NewShard(1, tmpShard, tmpWal, sfile.SeriesFile, opts)
	if err := sh.Open(); err != nil {
		t.Fatalf("error opening shard: %s", err.Error())
	}

	pt := models.MustNewPoint(
		"cpu",
		models.NewTags(map[string]string{"host": "server"}),
		map[string]interface{}{"value": 1.0},
		time.Unix(1, 2),
	)

	err := sh.WritePoints([]models.Point{pt}, tsdb.NoopStatsTracker())
	if err != nil {
		t.Fatalf(err.Error())
	}

	if got, exp := sh.SeriesN(), int64(1); got != exp {
		t.Fatalf("got %d series, exp %d series in index", got, exp)
	}

	// ensure the index gets loaded after closing and opening the shard
	sh.Close()
	sh.Open()

	if got, exp := sh.SeriesN(), int64(1); got != exp {
		t.Fatalf("got %d series, exp %d series in index", got, exp)
	}
}

// Ensure a shard can create iterators for its underlying data.
func TestShard_CreateIterator_Ascending(t *testing.T) {
	for _, index := range tsdb.RegisteredIndexes() {
		t.Run(index, func(t *testing.T) {
			sh := NewShard(index)
			defer sh.Close()

			// Calling CreateIterator when the engine is not open will return
			// ErrEngineClosed.
			m := &influxql.Measurement{Name: "cpu"}
			_, got := sh.CreateIterator(context.Background(), m, query.IteratorOptions{})
			if exp := tsdb.ErrEngineClosed; got != exp {
				t.Fatalf("got %v, expected %v", got, exp)
			}

			if err := sh.Open(); err != nil {
				t.Fatal(err)
			}

			sh.MustWritePointsString(`
cpu,host=serverA,region=uswest value=100 0
cpu,host=serverA,region=uswest value=50,val2=5  10
cpu,host=serverB,region=uswest value=25  0
`)

			// Create iterator.
			var err error
			m = &influxql.Measurement{Name: "cpu"}
			itr, err := sh.CreateIterator(context.Background(), m, query.IteratorOptions{
				Expr:       influxql.MustParseExpr(`value`),
				Aux:        []influxql.VarRef{{Val: "val2"}},
				Dimensions: []string{"host"},
				Ascending:  true,
				StartTime:  influxql.MinTime,
				EndTime:    influxql.MaxTime,
			})
			if err != nil {
				t.Fatal(err)
			}
			defer itr.Close()
			fitr := itr.(query.FloatIterator)

			// Read values from iterator.
			if p, err := fitr.Next(); err != nil {
				t.Fatalf("unexpected error(0): %s", err)
			} else if !deep.Equal(p, &query.FloatPoint{
				Name:  "cpu",
				Tags:  query.NewTags(map[string]string{"host": "serverA"}),
				Time:  time.Unix(0, 0).UnixNano(),
				Value: 100,
				Aux:   []interface{}{(*float64)(nil)},
			}) {
				t.Fatalf("unexpected point(0): %s", spew.Sdump(p))
			}

			if p, err := fitr.Next(); err != nil {
				t.Fatalf("unexpected error(1): %s", err)
			} else if !deep.Equal(p, &query.FloatPoint{
				Name:  "cpu",
				Tags:  query.NewTags(map[string]string{"host": "serverA"}),
				Time:  time.Unix(10, 0).UnixNano(),
				Value: 50,
				Aux:   []interface{}{float64(5)},
			}) {
				t.Fatalf("unexpected point(1): %s", spew.Sdump(p))
			}

			if p, err := fitr.Next(); err != nil {
				t.Fatalf("unexpected error(2): %s", err)
			} else if !deep.Equal(p, &query.FloatPoint{
				Name:  "cpu",
				Tags:  query.NewTags(map[string]string{"host": "serverB"}),
				Time:  time.Unix(0, 0).UnixNano(),
				Value: 25,
				Aux:   []interface{}{(*float64)(nil)},
			}) {
				t.Fatalf("unexpected point(2): %s", spew.Sdump(p))
			}
		})
	}
}

// Ensure a shard can create iterators for its underlying data.
func TestShard_CreateIterator_Descending(t *testing.T) {
	var sh *Shard
	var itr query.Iterator

	test := func(index string) {
		sh = NewShard(index)

		// Calling CreateIterator when the engine is not open will return
		// ErrEngineClosed.
		m := &influxql.Measurement{Name: "cpu"}
		_, got := sh.CreateIterator(context.Background(), m, query.IteratorOptions{})
		if exp := tsdb.ErrEngineClosed; got != exp {
			t.Fatalf("got %v, expected %v", got, exp)
		}

		if err := sh.Open(); err != nil {
			t.Fatal(err)
		}

		sh.MustWritePointsString(`
cpu,host=serverA,region=uswest value=100 0
cpu,host=serverA,region=uswest value=50,val2=5  10
cpu,host=serverB,region=uswest value=25  0
`)

		// Create iterator.
		var err error
		m = &influxql.Measurement{Name: "cpu"}
		itr, err = sh.CreateIterator(context.Background(), m, query.IteratorOptions{
			Expr:       influxql.MustParseExpr(`value`),
			Aux:        []influxql.VarRef{{Val: "val2"}},
			Dimensions: []string{"host"},
			Ascending:  false,
			StartTime:  influxql.MinTime,
			EndTime:    influxql.MaxTime,
		})
		if err != nil {
			t.Fatal(err)
		}
		fitr := itr.(query.FloatIterator)

		// Read values from iterator.
		if p, err := fitr.Next(); err != nil {
			t.Fatalf("unexpected error(0): %s", err)
		} else if !deep.Equal(p, &query.FloatPoint{
			Name:  "cpu",
			Tags:  query.NewTags(map[string]string{"host": "serverB"}),
			Time:  time.Unix(0, 0).UnixNano(),
			Value: 25,
			Aux:   []interface{}{(*float64)(nil)},
		}) {
			t.Fatalf("unexpected point(0): %s", spew.Sdump(p))
		}

		if p, err := fitr.Next(); err != nil {
			t.Fatalf("unexpected error(1): %s", err)
		} else if !deep.Equal(p, &query.FloatPoint{
			Name:  "cpu",
			Tags:  query.NewTags(map[string]string{"host": "serverA"}),
			Time:  time.Unix(10, 0).UnixNano(),
			Value: 50,
			Aux:   []interface{}{float64(5)},
		}) {
			t.Fatalf("unexpected point(1): %s", spew.Sdump(p))
		}

		if p, err := fitr.Next(); err != nil {
			t.Fatalf("unexpected error(2): %s", err)
		} else if !deep.Equal(p, &query.FloatPoint{
			Name:  "cpu",
			Tags:  query.NewTags(map[string]string{"host": "serverA"}),
			Time:  time.Unix(0, 0).UnixNano(),
			Value: 100,
			Aux:   []interface{}{(*float64)(nil)},
		}) {
			t.Fatalf("unexpected point(2): %s", spew.Sdump(p))
		}
	}

	for _, index := range tsdb.RegisteredIndexes() {
		t.Run(index, func(t *testing.T) { test(index) })
		sh.Close()
		itr.Close()
	}
}

func TestShard_CreateIterator_Series_Auth(t *testing.T) {
	type variant struct {
		name string
		m    *influxql.Measurement
		aux  []influxql.VarRef
	}

	examples := []variant{
		{
			name: "use_index",
			m:    &influxql.Measurement{Name: "cpu"},
			aux:  []influxql.VarRef{{Val: "_seriesKey", Type: influxql.String}},
		},
		{
			name: "use_cursors",
			m:    &influxql.Measurement{Name: "cpu", SystemIterator: "_series"},
			aux:  []influxql.VarRef{{Val: "key", Type: influxql.String}},
		},
	}

	test := func(index string, v variant) error {
		sh := MustNewOpenShard(index)
		defer sh.Close()
		sh.MustWritePointsString(`
cpu,host=serverA,region=uswest value=100 0
cpu,host=serverA,region=uswest value=50,val2=5  10
cpu,host=serverB,region=uswest value=25  0
cpu,secret=foo value=100 0
`)

		seriesAuthorizer := &internal.AuthorizerMock{
			AuthorizeSeriesReadFn: func(database string, measurement []byte, tags models.Tags) bool {
				if database == "" || !bytes.Equal(measurement, []byte("cpu")) || tags.GetString("secret") != "" {
					t.Logf("Rejecting series db=%s, m=%s, tags=%v", database, measurement, tags)
					return false
				}
				return true
			},
		}

		// Create iterator for case where we use cursors (e.g., where time
		// included in a SHOW SERIES query).
		itr, err := sh.CreateIterator(context.Background(), v.m, query.IteratorOptions{
			Aux:        v.aux,
			Ascending:  true,
			StartTime:  influxql.MinTime,
			EndTime:    influxql.MaxTime,
			Authorizer: seriesAuthorizer,
		})
		if err != nil {
			return err
		}

		if itr == nil {
			return fmt.Errorf("iterator is nil")
		}
		defer itr.Close()

		fitr := itr.(query.FloatIterator)
		defer fitr.Close()
		var expCount = 2
		var gotCount int
		for {
			f, err := fitr.Next()
			if err != nil {
				return err
			}

			if f == nil {
				break
			}

			if got := f.Aux[0].(string); strings.Contains(got, "secret") {
				return fmt.Errorf("got a series %q that should be filtered", got)
			}
			gotCount++
		}

		if gotCount != expCount {
			return fmt.Errorf("got %d series, expected %d", gotCount, expCount)
		}

		// Delete series cpu,host=serverA,region=uswest
		//
		// We can't call directly on the index as we need to ensure the series
		// file is updated appropriately.
		sitr := &seriesIterator{keys: [][]byte{[]byte("cpu,host=serverA,region=uswest")}}
		if err := sh.DeleteSeriesRange(sitr, math.MinInt64, math.MaxInt64); err != nil {
			t.Fatalf("failed to drop series: %s", err.Error())
		}

		if itr, err = sh.CreateIterator(context.Background(), v.m, query.IteratorOptions{
			Aux:        v.aux,
			Ascending:  true,
			StartTime:  influxql.MinTime,
			EndTime:    influxql.MaxTime,
			Authorizer: seriesAuthorizer,
		}); err != nil {
			return err
		}

		if itr == nil {
			return fmt.Errorf("iterator is nil")
		}
		defer itr.Close()

		fitr = itr.(query.FloatIterator)
		defer fitr.Close()
		expCount = 1
		gotCount = 0
		for {
			f, err := fitr.Next()
			if err != nil {
				return err
			}

			if f == nil {
				break
			}

			if got := f.Aux[0].(string); strings.Contains(got, "secret") {
				return fmt.Errorf("got a series %q that should be filtered", got)
			} else if got := f.Aux[0].(string); strings.Contains(got, "serverA") {
				return fmt.Errorf("got a series %q that should be filtered", got)
			}
			gotCount++
		}

		if gotCount != expCount {
			return fmt.Errorf("got %d series, expected %d", gotCount, expCount)
		}

		return nil
	}

	for _, index := range tsdb.RegisteredIndexes() {
		for _, example := range examples {
			t.Run(index+"_"+example.name, func(t *testing.T) {
				if err := test(index, example); err != nil {
					t.Fatal(err)
				}
			})
		}
	}
}

func TestShard_Disabled_WriteQuery(t *testing.T) {
	var sh *Shard

	test := func(index string) {
		sh = NewShard(index)
		if err := sh.Open(); err != nil {
			t.Fatal(err)
		}

		sh.SetEnabled(false)

		pt := models.MustNewPoint(
			"cpu",
			models.NewTags(map[string]string{"host": "server"}),
			map[string]interface{}{"value": 1.0},
			time.Unix(1, 2),
		)

		err := sh.WritePoints([]models.Point{pt}, tsdb.NoopStatsTracker())
		if err == nil {
			t.Fatalf("expected shard disabled error")
		}
		if err != tsdb.ErrShardDisabled {
			t.Fatalf(err.Error())
		}
		m := &influxql.Measurement{Name: "cpu"}
		_, got := sh.CreateIterator(context.Background(), m, query.IteratorOptions{})
		if err == nil {
			t.Fatalf("expected shard disabled error")
		}
		if exp := tsdb.ErrShardDisabled; got != exp {
			t.Fatalf("got %v, expected %v", got, exp)
		}

		sh.SetEnabled(true)

		err = sh.WritePoints([]models.Point{pt}, tsdb.NoopStatsTracker())
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		m = &influxql.Measurement{Name: "cpu"}
		if _, err = sh.CreateIterator(context.Background(), m, query.IteratorOptions{}); err != nil {
			t.Fatalf("unexpected error: %v", got)
		}
	}

	for _, index := range tsdb.RegisteredIndexes() {
		t.Run(index, func(t *testing.T) { test(index) })
		sh.Close()
	}
}

func TestShard_Closed_Functions(t *testing.T) {
	var sh *Shard
	test := func(index string) {
		sh = NewShard(index)
		if err := sh.Open(); err != nil {
			t.Fatal(err)
		}

		pt := models.MustNewPoint(
			"cpu",
			models.NewTags(map[string]string{"host": "server"}),
			map[string]interface{}{"value": 1.0},
			time.Unix(1, 2),
		)

		if err := sh.WritePoints([]models.Point{pt}, tsdb.NoopStatsTracker()); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		sh.Close()

		// Should not panic.
		if exp, got := 0, sh.TagKeyCardinality([]byte("cpu"), []byte("host")); exp != got {
			t.Fatalf("got %d, expected %d", got, exp)
		}
	}

	for _, index := range tsdb.RegisteredIndexes() {
		t.Run(index, func(t *testing.T) { test(index) })
	}
}

func TestShard_FieldDimensions(t *testing.T) {
	var sh *Shard

	sfile := MustOpenSeriesFile()
	defer sfile.Close()

	setup := func(index string) {
		sh = NewShard(index)

		if err := sh.Open(); err != nil {
			t.Fatal(err)
		}

		sh.MustWritePointsString(`
cpu,host=serverA,region=uswest value=100 0
cpu,host=serverA,region=uswest value=50,val2=5  10
cpu,host=serverB,region=uswest value=25  0
mem,host=serverA value=25i 0
mem,host=serverB value=50i,val3=t 10
_reserved,region=uswest value="foo" 0
`)
	}

	for _, index := range tsdb.RegisteredIndexes() {
		setup(index)
		for _, tt := range []struct {
			sources []string
			f       map[string]influxql.DataType
			d       map[string]struct{}
		}{
			{
				sources: []string{"cpu"},
				f: map[string]influxql.DataType{
					"value": influxql.Float,
					"val2":  influxql.Float,
				},
				d: map[string]struct{}{
					"host":   {},
					"region": {},
				},
			},
			{
				sources: []string{"mem"},
				f: map[string]influxql.DataType{
					"value": influxql.Integer,
					"val3":  influxql.Boolean,
				},
				d: map[string]struct{}{
					"host": {},
				},
			},
			{
				sources: []string{"cpu", "mem"},
				f: map[string]influxql.DataType{
					"value": influxql.Float,
					"val2":  influxql.Float,
					"val3":  influxql.Boolean,
				},
				d: map[string]struct{}{
					"host":   {},
					"region": {},
				},
			},
			{
				sources: []string{"_fieldKeys"},
				f: map[string]influxql.DataType{
					"fieldKey":  influxql.String,
					"fieldType": influxql.String,
				},
				d: map[string]struct{}{},
			},
			{
				sources: []string{"_series"},
				f: map[string]influxql.DataType{
					"key": influxql.String,
				},
				d: map[string]struct{}{},
			},
			{
				sources: []string{"_tagKeys"},
				f: map[string]influxql.DataType{
					"tagKey": influxql.String,
				},
				d: map[string]struct{}{},
			},
			{
				sources: []string{"_reserved"},
				f: map[string]influxql.DataType{
					"value": influxql.String,
				},
				d: map[string]struct{}{
					"region": {},
				},
			},
			{
				sources: []string{"unknown"},
				f:       map[string]influxql.DataType{},
				d:       map[string]struct{}{},
			},
		} {
			name := fmt.Sprintf("%s_%s", strings.Join(tt.sources, ","), index)
			t.Run(name, func(t *testing.T) {
				f, d, err := sh.FieldDimensions(tt.sources)
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}

				if diff := cmp.Diff(tt.f, f, cmpopts.EquateEmpty()); diff != "" {
					t.Errorf("unexpected fields:\n%s", diff)
				}
				if diff := cmp.Diff(tt.d, d, cmpopts.EquateEmpty()); diff != "" {
					t.Errorf("unexpected dimensions:\n%s", diff)
				}
			})
		}
		sh.Close()
	}
}

func TestShard_ReadersBlocked(t *testing.T) {
	setup := func(index string) Shards {
		shards := NewShards(index, 2)
		shards.MustOpen()

		shards[0].MustWritePointsString(`cpu,host=serverA,region=uswest a=2.2,b=33.3,value=100 0`)

		shards[1].MustWritePointsString(`
			cpu,host=serverA,region=uswest a=2.2,c=12.3,value=100,z="hello" 0
			disk q=100 0
		`)

		shards[0].Shard.ScheduleFullCompaction()
		shards[1].Shard.ScheduleFullCompaction()

		return shards
	}

	shardInUse := func(sh *tsdb.Shard) bool {
		t.Helper()
		require.NoError(t, sh.SetNewReadersBlocked(true))
		defer sh.SetNewReadersBlocked(false)
		inUse, err := sh.InUse()
		require.NoError(t, err)
		return inUse
	}

	for _, index := range tsdb.RegisteredIndexes() {
		t.Run(fmt.Sprintf("%s_readers_blocked", index), func(t *testing.T) {
			shards := setup(index)
			defer shards.Close()

			s1 := shards[0].Shard
			m := influxql.Measurement{
				Database:        "db0",
				RetentionPolicy: "rp0",
				Name:            "cpu",
			}
			opts := query.IteratorOptions{
				Aux:        []influxql.VarRef{{Val: "a", Type: influxql.Float}, {Val: "b", Type: influxql.Float}},
				StartTime:  models.MinNanoTime,
				EndTime:    models.MaxNanoTime,
				Ascending:  false,
				Limit:      5,
				Ordered:    true,
				Authorizer: query.OpenAuthorizer,
			}

			// Block new readers. Due to internal interfaces, CreateIterator won't return an error but
			// it should return a faux iterator.
			require.NoError(t, s1.SetNewReadersBlocked(true))
			require.False(t, shardInUse(s1))
			it, err := s1.CreateIterator(context.Background(), &m, opts)
			require.NoError(t, err) // It would be great to get an error, but alas that's major internal replumbing.
			require.NotNil(t, it)
			require.False(t, shardInUse(s1)) // Remember, it isn't a real iterator.
			fit, ok := it.(query.FloatIterator)
			require.True(t, ok)
			p, err := fit.Next()
			require.NoError(t, err)
			require.Nil(t, p)
			require.NoError(t, fit.Close())
			require.NoError(t, s1.SetNewReadersBlocked(false))
			require.False(t, shardInUse(s1))

			// CreateIterator, make sure shard shows as in-use during iterator life.
			require.False(t, shardInUse(s1))
			it, err = s1.CreateIterator(context.Background(), &m, opts)
			require.NoError(t, err)
			fit, ok = it.(query.FloatIterator)
			require.True(t, ok)
			p, err = fit.Next()
			require.NoError(t, err)
			require.NotNil(t, p)
			require.True(t, shardInUse(s1))
			require.NoError(t, fit.Close())
			require.False(t, shardInUse(s1))
		})
	}
}

func TestShards_FieldKeysByMeasurement(t *testing.T) {
	var shards Shards

	setup := func(index string) {
		shards = NewShards(index, 2)
		shards.MustOpen()

		shards[0].MustWritePointsString(`cpu,host=serverA,region=uswest a=2.2,b=33.3,value=100 0`)

		shards[1].MustWritePointsString(`
			cpu,host=serverA,region=uswest a=2.2,c=12.3,value=100,z="hello" 0
			disk q=100 0
		`)
	}

	for _, index := range tsdb.RegisteredIndexes() {
		setup(index)
		t.Run(fmt.Sprintf("%s_single_shard", index), func(t *testing.T) {
			exp := []string{"a", "b", "value"}
			if got := (tsdb.Shards{shards[0].Shard}).FieldKeysByMeasurement([]byte("cpu")); !reflect.DeepEqual(got, exp) {
				shards.Close()
				t.Fatalf("got keys %v, expected %v", got, exp)
			}
		})

		t.Run(fmt.Sprintf("%s_multiple_shards", index), func(t *testing.T) {
			exp := []string{"a", "b", "c", "value", "z"}
			if got := shards.Shards().FieldKeysByMeasurement([]byte("cpu")); !reflect.DeepEqual(got, exp) {
				shards.Close()
				t.Fatalf("got keys %v, expected %v", got, exp)
			}
		})
		shards.Close()
	}
}

func TestShards_FieldDimensions(t *testing.T) {
	var shard1, shard2 *Shard

	setup := func(index string) {
		shard1 = NewShard(index)
		if err := shard1.Open(); err != nil {
			t.Fatal(err)
		}

		shard1.MustWritePointsString(`
cpu,host=serverA,region=uswest value=100 0
cpu,host=serverA,region=uswest value=50,val2=5  10
cpu,host=serverB,region=uswest value=25  0
`)

		shard2 = NewShard(index)
		if err := shard2.Open(); err != nil {
			t.Fatal(err)
		}

		shard2.MustWritePointsString(`
mem,host=serverA value=25i 0
mem,host=serverB value=50i,val3=t 10
_reserved,region=uswest value="foo" 0
`)
	}

	for _, index := range tsdb.RegisteredIndexes() {
		setup(index)
		sh := tsdb.Shards([]*tsdb.Shard{shard1.Shard, shard2.Shard})
		for _, tt := range []struct {
			sources []string
			f       map[string]influxql.DataType
			d       map[string]struct{}
		}{
			{
				sources: []string{"cpu"},
				f: map[string]influxql.DataType{
					"value": influxql.Float,
					"val2":  influxql.Float,
				},
				d: map[string]struct{}{
					"host":   {},
					"region": {},
				},
			},
			{
				sources: []string{"mem"},
				f: map[string]influxql.DataType{
					"value": influxql.Integer,
					"val3":  influxql.Boolean,
				},
				d: map[string]struct{}{
					"host": {},
				},
			},
			{
				sources: []string{"cpu", "mem"},
				f: map[string]influxql.DataType{
					"value": influxql.Float,
					"val2":  influxql.Float,
					"val3":  influxql.Boolean,
				},
				d: map[string]struct{}{
					"host":   {},
					"region": {},
				},
			},
			{
				sources: []string{"_fieldKeys"},
				f: map[string]influxql.DataType{
					"fieldKey":  influxql.String,
					"fieldType": influxql.String,
				},
				d: map[string]struct{}{},
			},
			{
				sources: []string{"_series"},
				f: map[string]influxql.DataType{
					"key": influxql.String,
				},
				d: map[string]struct{}{},
			},
			{
				sources: []string{"_tagKeys"},
				f: map[string]influxql.DataType{
					"tagKey": influxql.String,
				},
				d: map[string]struct{}{},
			},
			{
				sources: []string{"_reserved"},
				f: map[string]influxql.DataType{
					"value": influxql.String,
				},
				d: map[string]struct{}{
					"region": {},
				},
			},
			{
				sources: []string{"unknown"},
				f:       map[string]influxql.DataType{},
				d:       map[string]struct{}{},
			},
		} {
			name := fmt.Sprintf("%s_%s", index, strings.Join(tt.sources, ","))
			t.Run(name, func(t *testing.T) {
				f, d, err := sh.FieldDimensions(tt.sources)
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}

				if diff := cmp.Diff(tt.f, f, cmpopts.EquateEmpty()); diff != "" {
					t.Errorf("unexpected fields:\n%s", diff)
				}
				if diff := cmp.Diff(tt.d, d, cmpopts.EquateEmpty()); diff != "" {
					t.Errorf("unexpected dimensions:\n%s", diff)
				}
			})
		}
		shard1.Close()
		shard2.Close()
	}
}

func TestShards_MapType(t *testing.T) {
	var shard1, shard2 *Shard

	setup := func(index string) {
		shard1 = NewShard(index)
		if err := shard1.Open(); err != nil {
			t.Fatal(err)
		}

		shard1.MustWritePointsString(`
cpu,host=serverA,region=uswest value=100 0
cpu,host=serverA,region=uswest value=50,val2=5  10
cpu,host=serverB,region=uswest value=25  0
`)

		shard2 = NewShard(index)
		if err := shard2.Open(); err != nil {
			t.Fatal(err)
		}

		shard2.MustWritePointsString(`
mem,host=serverA value=25i 0
mem,host=serverB value=50i,val3=t 10
_reserved,region=uswest value="foo" 0
`)
	}

	for _, index := range tsdb.RegisteredIndexes() {
		setup(index)
		sh := tsdb.Shards([]*tsdb.Shard{shard1.Shard, shard2.Shard})
		for _, tt := range []struct {
			measurement string
			field       string
			typ         influxql.DataType
		}{
			{
				measurement: "cpu",
				field:       "value",
				typ:         influxql.Float,
			},
			{
				measurement: "cpu",
				field:       "host",
				typ:         influxql.Tag,
			},
			{
				measurement: "cpu",
				field:       "region",
				typ:         influxql.Tag,
			},
			{
				measurement: "cpu",
				field:       "val2",
				typ:         influxql.Float,
			},
			{
				measurement: "cpu",
				field:       "unknown",
				typ:         influxql.Unknown,
			},
			{
				measurement: "mem",
				field:       "value",
				typ:         influxql.Integer,
			},
			{
				measurement: "mem",
				field:       "val3",
				typ:         influxql.Boolean,
			},
			{
				measurement: "mem",
				field:       "host",
				typ:         influxql.Tag,
			},
			{
				measurement: "unknown",
				field:       "unknown",
				typ:         influxql.Unknown,
			},
			{
				measurement: "_fieldKeys",
				field:       "fieldKey",
				typ:         influxql.String,
			},
			{
				measurement: "_fieldKeys",
				field:       "fieldType",
				typ:         influxql.String,
			},
			{
				measurement: "_fieldKeys",
				field:       "unknown",
				typ:         influxql.Unknown,
			},
			{
				measurement: "_series",
				field:       "key",
				typ:         influxql.String,
			},
			{
				measurement: "_series",
				field:       "unknown",
				typ:         influxql.Unknown,
			},
			{
				measurement: "_tagKeys",
				field:       "tagKey",
				typ:         influxql.String,
			},
			{
				measurement: "_tagKeys",
				field:       "unknown",
				typ:         influxql.Unknown,
			},
			{
				measurement: "_reserved",
				field:       "value",
				typ:         influxql.String,
			},
			{
				measurement: "_reserved",
				field:       "region",
				typ:         influxql.Tag,
			},
		} {
			name := fmt.Sprintf("%s_%s_%s", index, tt.measurement, tt.field)
			t.Run(name, func(t *testing.T) {
				typ := sh.MapType(tt.measurement, tt.field)
				if have, want := typ, tt.typ; have != want {
					t.Errorf("unexpected data type: have=%#v want=%#v", have, want)
				}
			})
		}
		shard1.Close()
		shard2.Close()
	}
}

func TestShards_MeasurementsByRegex(t *testing.T) {
	var shard1, shard2 *Shard

	setup := func(index string) {
		shard1 = NewShard(index)
		if err := shard1.Open(); err != nil {
			t.Fatal(err)
		}

		shard1.MustWritePointsString(`
cpu,host=serverA,region=uswest value=100 0
cpu,host=serverA,region=uswest value=50,val2=5  10
cpu,host=serverB,region=uswest value=25  0
`)

		shard2 = NewShard(index)
		if err := shard2.Open(); err != nil {
			t.Fatal(err)
		}

		shard2.MustWritePointsString(`
mem,host=serverA value=25i 0
mem,host=serverB value=50i,val3=t 10
_reserved,region=uswest value="foo" 0
`)
	}

	for _, index := range tsdb.RegisteredIndexes() {
		setup(index)
		sh := tsdb.Shards([]*tsdb.Shard{shard1.Shard, shard2.Shard})
		for _, tt := range []struct {
			regex        string
			measurements []string
		}{
			{regex: `cpu`, measurements: []string{"cpu"}},
			{regex: `mem`, measurements: []string{"mem"}},
			{regex: `cpu|mem`, measurements: []string{"cpu", "mem"}},
			{regex: `gpu`, measurements: []string{}},
			{regex: `pu`, measurements: []string{"cpu"}},
			{regex: `p|m`, measurements: []string{"cpu", "mem"}},
		} {
			t.Run(tt.regex, func(t *testing.T) {
				re := regexp.MustCompile(tt.regex)
				measurements := sh.MeasurementsByRegex(re)
				sort.Strings(measurements)
				if diff := cmp.Diff(tt.measurements, measurements, cmpopts.EquateEmpty()); diff != "" {
					t.Errorf("unexpected measurements:\n%s", diff)
				}
			})
		}
		shard1.Close()
		shard2.Close()
	}
}

func TestMeasurementFieldSet_SaveLoad(t *testing.T) {
	const measurement = "cpu"
	const fieldName = "value"

	dir, cleanup := MustTempDir()
	defer cleanup()

	path := filepath.Join(dir, "fields.idx")
	mf, err := tsdb.NewMeasurementFieldSet(path, nil)
	if err != nil {
		t.Fatalf("NewMeasurementFieldSet error: %v", err)
	}
	defer checkMeasurementFieldSetClose(t, mf)
	fields := mf.CreateFieldsIfNotExists([]byte(measurement))
	if _, err := fields.CreateFieldIfNotExists(fieldName, influxql.Float); err != nil {
		t.Fatalf("create field error: %v", err)
	}
	change := tsdb.FieldChange{
		FieldCreate: tsdb.FieldCreate{
			Measurement: []byte(measurement),
			Field:       &tsdb.Field{Name: fieldName, Type: influxql.Float},
		},
		ChangeType: tsdb.AddMeasurementField,
	}

	if err := mf.Save(tsdb.FieldChanges{&change}); err != nil {
		t.Fatalf("save error: %v", err)
	}
	_, err = os.Stat(mf.ChangesPath())
	assert.NoError(t, err, "no field.idx change file")

	mf2, err := tsdb.NewMeasurementFieldSet(path, nil)
	if err != nil {
		t.Fatalf("NewMeasurementFieldSet error: %v", err)
	}
	_, err = os.Stat(mf.ChangesPath())
	assert2.Error(t, err, "file %s should have had this error: %s", mf.ChangesPath(), os.ErrNotExist)
	if !os.IsNotExist(err) {
		t.Fatalf("unexpected error for %s: got %s, expected %s", mf.ChangesPath(), err, os.ErrNotExist)
	}
	defer checkMeasurementFieldSetClose(t, mf2)
	fields = mf2.FieldsByString(measurement)
	field := fields.Field(fieldName)
	if field == nil {
		t.Fatalf("field is null")
	}

	if got, exp := field.Type, influxql.Float; got != exp {
		t.Fatalf("field type mismatch: got %v, exp %v", got, exp)
	}
}

func TestMeasurementFieldSet_Corrupt(t *testing.T) {
	dir, cleanup := MustTempDir()
	defer cleanup()

	path := filepath.Join(dir, "fields.idx")
	func() {
		mf, err := tsdb.NewMeasurementFieldSet(path, nil)
		if err != nil {
			t.Fatalf("NewMeasurementFieldSet error: %v", err)
		}
		defer checkMeasurementFieldSetClose(t, mf)
		measurement := []byte("cpu")
		fields := mf.CreateFieldsIfNotExists(measurement)
		fieldName := "value"
		if _, err := fields.CreateFieldIfNotExists(fieldName, influxql.Float); err != nil {
			t.Fatalf("create field error: %v", err)
		}
		change := tsdb.FieldChange{
			FieldCreate: tsdb.FieldCreate{
				Measurement: []byte(measurement),
				Field:       &tsdb.Field{Name: fieldName, Type: influxql.Float},
			},
			ChangeType: tsdb.AddMeasurementField,
		}

		if err := mf.Save(tsdb.FieldChanges{&change}); err != nil {
			t.Fatalf("save error: %v", err)
		}
	}()
	stat, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat error: %v", err)
	}
	// Truncate the file to simulate a corrupted file
	if err := os.Truncate(path, stat.Size()-3); err != nil {
		t.Fatalf("truncate error: %v", err)
	}
	mf, err := tsdb.NewMeasurementFieldSet(path, nil)
	if err == nil {
		t.Fatal("NewMeasurementFieldSet expected error")
	}
	defer checkMeasurementFieldSetClose(t, mf)

	fields := mf.FieldsByString("cpu")
	if fields != nil {
		t.Fatal("expecte fields to be nil")
	}
}

func TestMeasurementFieldSet_CorruptChangeFile(t *testing.T) {
	dir, cleanup := MustTempDir()
	defer cleanup()

	testFields := []struct {
		Measurement string
		Field       string
		FieldType   influxql.DataType
	}{
		{
			Measurement: "cpu",
			Field:       "value_1",
			FieldType:   influxql.Float,
		},
		{
			Measurement: "cpu",
			Field:       "value_2",
			FieldType:   influxql.String,
		},
		{
			Measurement: "cpu",
			Field:       "value_3",
			FieldType:   influxql.Integer,
		},
	}

	path := filepath.Join(dir, "fields.idx")
	var mf *tsdb.MeasurementFieldSet
	var err error
	mf, err = tsdb.NewMeasurementFieldSet(path, nil)
	if err != nil {
		t.Fatalf("NewMeasurementFieldSet error: %v", err)
	}
	defer checkMeasurementFieldSetClose(t, mf)
	for _, f := range testFields {
		fields := mf.CreateFieldsIfNotExists([]byte(f.Measurement))
		if _, err := fields.CreateFieldIfNotExists(f.Field, f.FieldType); err != nil {
			t.Fatalf("create field error: %v", err)
		}
		change := tsdb.FieldChange{
			FieldCreate: tsdb.FieldCreate{
				Measurement: []byte(f.Measurement),
				Field:       &tsdb.Field{Name: f.Field, Type: f.FieldType},
			},
			ChangeType: tsdb.AddMeasurementField,
		}

		if err := mf.Save(tsdb.FieldChanges{&change}); err != nil {
			t.Fatalf("save error: %v", err)
		}
	}
	changeFile := filepath.Join(dir, tsdb.FieldsChangeFile)
	stat, err := os.Stat(changeFile)
	if err != nil {
		t.Fatalf("stat error: %v", err)
	}
	// Truncate the file to simulate a corrupted file
	if err := os.Truncate(changeFile, stat.Size()-3); err != nil {
		t.Fatalf("truncate error: %v", err)
	}
	mf2, err := tsdb.NewMeasurementFieldSet(path, nil)
	assert.NoError(t, err, "failed creating second MeasurementFieldSet")
	defer checkMeasurementFieldSetClose(t, mf2)

	for i := 0; i < len(testFields)-1; i++ {
		fields := mf2.FieldsByString(testFields[i].Measurement)
		if fields == nil {
			t.Fatalf("nil fields map for %s", testFields[i].Measurement)
		} else if f := fields.Field(testFields[i].Field); f == nil {
			t.Fatalf("%s not found in %s fields", testFields[i].Field, testFields[i].Measurement)
		} else if f.Type != testFields[i].FieldType {
			t.Fatalf("%s.%s wrong type: expected %v, got %v", testFields[i].Measurement, testFields[i].Field, testFields[i].FieldType, f.Type)
		}
	}
	i := len(testFields) - 1
	fields := mf2.FieldsByString(testFields[i].Measurement)
	if fields == nil {
		t.Fatalf("nil fields map for %s", testFields[i].Measurement)
	} else if f := fields.Field(testFields[i].Field); f != nil {
		t.Fatalf("%s found in %s fields, should have not been present", testFields[i].Field, testFields[i].Measurement)
	}
}

func TestMeasurementFieldSet_DeleteEmpty(t *testing.T) {
	const measurement = "cpu"
	const fieldName = "value"

	dir, cleanup := MustTempDir()
	defer cleanup()

	path := filepath.Join(dir, "fields.idx")
	mf, err := tsdb.NewMeasurementFieldSet(path, nil)
	if err != nil {
		t.Fatalf("NewMeasurementFieldSet error: %v", err)
	}
	defer checkMeasurementFieldSetClose(t, mf)

	fields := mf.CreateFieldsIfNotExists([]byte(measurement))
	if _, err := fields.CreateFieldIfNotExists(fieldName, influxql.Float); err != nil {
		t.Fatalf("create field error: %v", err)
	}

	change := tsdb.FieldChange{
		FieldCreate: tsdb.FieldCreate{
			Measurement: []byte(measurement),
			Field:       &tsdb.Field{Name: fieldName, Type: influxql.Float},
		},
		ChangeType: tsdb.AddMeasurementField,
	}

	if err := mf.Save(tsdb.FieldChanges{&change}); err != nil {
		t.Fatalf("save error: %v", err)
	}
	mf2, err := tsdb.NewMeasurementFieldSet(path, nil)
	if err != nil {
		t.Fatalf("NewMeasurementFieldSet error: %v", err)
	}
	fields = mf2.FieldsByString(measurement)
	field := fields.Field(fieldName)
	if field == nil {
		t.Fatalf("field is null")
	}

	if got, exp := field.Type, influxql.Float; got != exp {
		t.Fatalf("field type mismatch: got %v, exp %v", got, exp)
	}

	mf2.Delete(measurement)

	if err := mf2.Save(tsdb.MeasurementsToFieldChangeDeletions([]string{measurement})); err != nil {
		t.Fatalf("save after delete error: %v", err)
	}
	_, err = os.Stat(mf.ChangesPath())
	assert.NoError(t, err, "no field.idx change file")
	assert.NoError(t, mf2.Close(), "failed closing MeasurementFieldSet")

	_, err = os.Stat(mf.ChangesPath())
	assert2.Error(t, err, "file %s should have had this error: %s", mf.ChangesPath(), os.ErrNotExist)
	if !os.IsNotExist(err) {
		t.Fatalf("unexpected error for %s: got %s, expected %s", mf.ChangesPath(), err, os.ErrNotExist)
	}

	if _, err = os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("got %v, not exist err", err)
	}
}

func checkMeasurementFieldSetClose(t *testing.T, fs *tsdb.MeasurementFieldSet) {
	assert.NoError(t, fs.Close(), "failed closing tsdb.MeasurementFieldSet")
	_, err := os.Stat(fs.ChangesPath())
	assert2.Error(t, err, "file %s should have had this error: %s", fs.ChangesPath(), os.ErrNotExist)
	if !os.IsNotExist(err) {
		t.Fatalf("unexpected error for %s: got %s, expected %s", fs.ChangesPath(), err, os.ErrNotExist)
	}
}

func TestMeasurementFieldSet_InvalidFormat(t *testing.T) {
	dir, cleanup := MustTempDir()
	defer cleanup()

	path := filepath.Join(dir, "fields.idx")

	if err := os.WriteFile(path, []byte{0, 0}, 0666); err != nil {
		t.Fatalf("error writing fields.index: %v", err)
	}

	mf, err := tsdb.NewMeasurementFieldSet(path, nil)
	if !errors.Is(err, tsdb.ErrUnknownFieldsFormat) {
		t.Fatalf("unexpected error: got %v, exp %v", err, tsdb.ErrUnknownFieldsFormat)
	}
	defer checkMeasurementFieldSetClose(t, mf)
}

func TestMeasurementFieldSet_ConcurrentSave(t *testing.T) {
	var iterations int
	dir, cleanup := MustTempDir()
	defer cleanup()

	if testing.Short() {
		iterations = 50
	} else {
		iterations = 200
	}

	mt := []string{"cpu", "dpu", "epu", "fpu"}
	ft := make([][]string, len(mt), len(mt))
	for mi, m := range mt {
		ft[mi] = make([]string, iterations, iterations)
		for i := 0; i < iterations; i += 1 {
			ft[mi][i] = fmt.Sprintf("%s_%s_%d", m, "value", i)
		}
	}

	path := filepath.Join(dir, "fields.idx")
	mfs, err := tsdb.NewMeasurementFieldSet(path, nil)
	if err != nil {
		t.Fatalf("NewMeasurementFieldSet error: %v", err)
	}
	defer checkMeasurementFieldSetClose(t, mfs)
	var wg sync.WaitGroup

	wg.Add(len(ft))
	for i, fs := range ft {
		go testFieldMaker(t, &wg, mfs, mt[i], fs)
	}
	wg.Wait()

	mfs2, err := tsdb.NewMeasurementFieldSet(path, nil)
	if err != nil {
		t.Fatalf("NewMeasurementFieldSet error: %v", err)
	}
	defer checkMeasurementFieldSetClose(t, mfs2)
	for i, fs := range ft {
		mf := mfs.Fields([]byte(mt[i]))
		mf2 := mfs2.Fields([]byte(mt[i]))
		for _, f := range fs {
			if mf2.Field(f) == nil {
				t.Fatalf("Created field not found on reloaded MeasurementFieldSet %s", f)
			}
			if mf.Field(f) == nil {
				t.Fatalf("Created field not found in original MeasureMentFieldSet: %s", f)
			}
		}
	}
}

func testFieldMaker(t *testing.T, wg *sync.WaitGroup, mf *tsdb.MeasurementFieldSet, measurement string, fieldNames []string) {
	defer wg.Done()
	fields := mf.CreateFieldsIfNotExists([]byte(measurement))

	for _, fieldName := range fieldNames {
		if _, err := fields.CreateFieldIfNotExists(fieldName, influxql.Float); err != nil {
			t.Logf("create field error: %v", err)
			t.Fail()
			return
		}

		change := tsdb.FieldChange{
			FieldCreate: tsdb.FieldCreate{
				Measurement: []byte(measurement),
				Field:       &tsdb.Field{Name: fieldName, Type: influxql.Float},
			},
			ChangeType: tsdb.AddMeasurementField,
		}

		err := mf.Save(tsdb.FieldChanges{&change})
		if err != nil {
			t.Logf("save error: %v", err)
			t.Fail()
			return
		}
		_, err = os.Stat(mf.ChangesPath())
		if err != nil {
			t.Logf("unexpected error for field.idxl change file %s: %s", mf.ChangesPath(), err)
			t.Fail()
		}
	}
}

func BenchmarkWritePoints_NewSeries_1K(b *testing.B)   { benchmarkWritePoints(b, 38, 3, 3, 1) }
func BenchmarkWritePoints_NewSeries_100K(b *testing.B) { benchmarkWritePoints(b, 32, 5, 5, 1) }
func BenchmarkWritePoints_NewSeries_250K(b *testing.B) { benchmarkWritePoints(b, 80, 5, 5, 1) }
func BenchmarkWritePoints_NewSeries_500K(b *testing.B) { benchmarkWritePoints(b, 160, 5, 5, 1) }
func BenchmarkWritePoints_NewSeries_1M(b *testing.B)   { benchmarkWritePoints(b, 320, 5, 5, 1) }

// Fix measurement and tag key cardinalities and vary tag value cardinality
func BenchmarkWritePoints_NewSeries_1_Measurement_1_TagKey_100_TagValues(b *testing.B) {
	benchmarkWritePoints(b, 1, 1, 100, 1)
}
func BenchmarkWritePoints_NewSeries_1_Measurement_1_TagKey_500_TagValues(b *testing.B) {
	benchmarkWritePoints(b, 1, 1, 500, 1)
}
func BenchmarkWritePoints_NewSeries_1_Measurement_1_TagKey_1000_TagValues(b *testing.B) {
	benchmarkWritePoints(b, 1, 1, 1000, 1)
}
func BenchmarkWritePoints_NewSeries_1_Measurement_1_TagKey_5000_TagValues(b *testing.B) {
	benchmarkWritePoints(b, 1, 1, 5000, 1)
}
func BenchmarkWritePoints_NewSeries_1_Measurement_1_TagKey_10000_TagValues(b *testing.B) {
	benchmarkWritePoints(b, 1, 1, 10000, 1)
}
func BenchmarkWritePoints_NewSeries_1_Measurement_1_TagKey_50000_TagValues(b *testing.B) {
	benchmarkWritePoints(b, 1, 1, 50000, 1)
}
func BenchmarkWritePoints_NewSeries_1_Measurement_1_TagKey_100000_TagValues(b *testing.B) {
	benchmarkWritePoints(b, 1, 1, 100000, 1)
}
func BenchmarkWritePoints_NewSeries_1_Measurement_1_TagKey_500000_TagValues(b *testing.B) {
	benchmarkWritePoints(b, 1, 1, 500000, 1)
}
func BenchmarkWritePoints_NewSeries_1_Measurement_1_TagKey_1000000_TagValues(b *testing.B) {
	benchmarkWritePoints(b, 1, 1, 1000000, 1)
}

// Fix tag key and tag values cardinalities and vary measurement cardinality
func BenchmarkWritePoints_NewSeries_100_Measurements_1_TagKey_1_TagValue(b *testing.B) {
	benchmarkWritePoints(b, 100, 1, 1, 1)
}
func BenchmarkWritePoints_NewSeries_500_Measurements_1_TagKey_1_TagValue(b *testing.B) {
	benchmarkWritePoints(b, 500, 1, 1, 1)
}
func BenchmarkWritePoints_NewSeries_1000_Measurement_1_TagKey_1_TagValue(b *testing.B) {
	benchmarkWritePoints(b, 1000, 1, 1, 1)
}

func BenchmarkWritePoints_NewSeries_5000_Measurement_1_TagKey_1_TagValue(b *testing.B) {
	benchmarkWritePoints(b, 5000, 1, 1, 1)
}
func BenchmarkWritePoints_NewSeries_10000_Measurement_1_TagKey_1_TagValue(b *testing.B) {
	benchmarkWritePoints(b, 10000, 1, 1, 1)
}

func BenchmarkWritePoints_NewSeries_1000_Measurement_10_TagKey_1_TagValue(b *testing.B) {
	benchmarkWritePoints(b, 1000, 10, 1, 1)
}

func BenchmarkWritePoints_NewSeries_50000_Measurement_1_TagKey_1_TagValue(b *testing.B) {
	benchmarkWritePoints(b, 50000, 1, 1, 1)
}
func BenchmarkWritePoints_NewSeries_100000_Measurement_1_TagKey_1_TagValue(b *testing.B) {
	benchmarkWritePoints(b, 100000, 1, 1, 1)
}

func BenchmarkWritePoints_NewSeries_500000_Measurement_1_TagKey_1_TagValue(b *testing.B) {
	benchmarkWritePoints(b, 500000, 1, 1, 1)
}
func BenchmarkWritePoints_NewSeries_1000000_Measurement_1_TagKey_1_TagValue(b *testing.B) {
	benchmarkWritePoints(b, 1000000, 1, 1, 1)
}

// Fix measurement and tag values cardinalities and vary tag key cardinality
func BenchmarkWritePoints_NewSeries_1_Measurement_2_TagKeys_1_TagValue(b *testing.B) {
	benchmarkWritePoints(b, 1, 1<<1, 1, 1)
}
func BenchmarkWritePoints_NewSeries_1_Measurements_4_TagKeys_1_TagValue(b *testing.B) {
	benchmarkWritePoints(b, 1, 1<<2, 1, 1)
}
func BenchmarkWritePoints_NewSeries_1_Measurements_8_TagKeys_1_TagValue(b *testing.B) {
	benchmarkWritePoints(b, 1, 1<<3, 1, 1)
}
func BenchmarkWritePoints_NewSeries_1_Measurement_16_TagKeys_1_TagValue(b *testing.B) {
	benchmarkWritePoints(b, 1, 1<<4, 1, 1)
}
func BenchmarkWritePoints_NewSeries_1_Measurement_32_TagKeys_1_TagValue(b *testing.B) {
	benchmarkWritePoints(b, 1, 1<<5, 1, 1)
}
func BenchmarkWritePoints_NewSeries_1_Measurement_64_TagKeys_1_TagValue(b *testing.B) {
	benchmarkWritePoints(b, 1, 1<<6, 1, 1)
}
func BenchmarkWritePoints_NewSeries_1_Measurement_128_TagKeys_1_TagValue(b *testing.B) {
	benchmarkWritePoints(b, 1, 1<<7, 1, 1)
}
func BenchmarkWritePoints_NewSeries_1_Measurement_256_TagKeys_1_TagValue(b *testing.B) {
	benchmarkWritePoints(b, 1, 1<<8, 1, 1)
}
func BenchmarkWritePoints_NewSeries_1_Measurement_512_TagKeys_1_TagValue(b *testing.B) {
	benchmarkWritePoints(b, 1, 1<<9, 1, 1)
}
func BenchmarkWritePoints_NewSeries_1_Measurement_1024_TagKeys_1_TagValue(b *testing.B) {
	benchmarkWritePoints(b, 1, 1<<10, 1, 1)
}

// Fix series cardinality and vary tag keys and value cardinalities
func BenchmarkWritePoints_NewSeries_1_Measurement_1_TagKey_65536_TagValue(b *testing.B) {
	benchmarkWritePoints(b, 1, 1, 1<<16, 1)
}
func BenchmarkWritePoints_NewSeries_1_Measurement_2_TagKeys_256_TagValue(b *testing.B) {
	benchmarkWritePoints(b, 1, 2, 1<<8, 1)
}
func BenchmarkWritePoints_NewSeries_1_Measurement_4_TagKeys_16_TagValue(b *testing.B) {
	benchmarkWritePoints(b, 1, 4, 1<<4, 1)
}
func BenchmarkWritePoints_NewSeries_1_Measurement_8_TagKeys_4_TagValue(b *testing.B) {
	benchmarkWritePoints(b, 1, 8, 1<<2, 1)
}
func BenchmarkWritePoints_NewSeries_1_Measurement_16_TagKeys_2_TagValue(b *testing.B) {
	benchmarkWritePoints(b, 1, 16, 1<<1, 1)
}

func BenchmarkWritePoints_ExistingSeries_1K(b *testing.B) {
	benchmarkWritePointsExistingSeries(b, 38, 3, 3, 1)
}
func BenchmarkWritePoints_ExistingSeries_100K(b *testing.B) {
	benchmarkWritePointsExistingSeries(b, 32, 5, 5, 1)
}

func BenchmarkWritePoints_ExistingSeries_250K(b *testing.B) {
	benchmarkWritePointsExistingSeries(b, 80, 5, 5, 1)
}
func BenchmarkWritePoints_ExistingSeries_500K(b *testing.B) {
	benchmarkWritePointsExistingSeries(b, 160, 5, 5, 1)
}
func BenchmarkWritePoints_ExistingSeries_1M(b *testing.B) {
	benchmarkWritePointsExistingSeries(b, 320, 5, 5, 1)
}

// The following two benchmarks measure time to write 10k points at a time for comparing performance with different measurement cardinalities.
func BenchmarkWritePoints_ExistingSeries_100K_1_1(b *testing.B) {
	benchmarkWritePointsExistingSeriesEqualBatches(b, 100000, 1, 1, 1)
}

func BenchmarkWritePoints_ExistingSeries_10K_10_1(b *testing.B) {
	benchmarkWritePointsExistingSeriesEqualBatches(b, 10000, 10, 1, 1)
}

func BenchmarkWritePoints_ExistingSeries_100K_1_1_Fields(b *testing.B) {
	benchmarkWritePointsExistingSeriesFields(b, 100000, 1, 1, 1)
}

func BenchmarkWritePoints_ExistingSeries_10K_10_1_Fields(b *testing.B) {
	benchmarkWritePointsExistingSeriesFields(b, 10000, 10, 1, 1)
}

// benchmarkWritePoints benchmarks writing new series to a shard.
// mCnt - measurement count
// tkCnt - tag key count
// tvCnt - tag value count (values per tag)
// pntCnt - points per series.  # of series = mCnt * (tvCnt ^ tkCnt)
func benchmarkWritePoints(b *testing.B, mCnt, tkCnt, tvCnt, pntCnt int) {
	// Generate test series (measurements + unique tag sets).
	series := genTestSeries(mCnt, tkCnt, tvCnt)
	// Generate point data to write to the shard.
	points := []models.Point{}
	for _, s := range series {
		for val := 0.0; val < float64(pntCnt); val++ {
			p := models.MustNewPoint(s.Measurement, s.Tags, map[string]interface{}{"value": val}, time.Now())
			points = append(points, p)
		}
	}

	// Stop & reset timers and mem-stats before the main benchmark loop.
	b.StopTimer()
	b.ResetTimer()

	sfile := MustOpenSeriesFile()
	defer sfile.Close()

	// Run the benchmark loop.
	for n := 0; n < b.N; n++ {
		shard, tmpDir, err := openShard(sfile)
		if err != nil {
			shard.Close()
			b.Fatal(err)
		}

		b.StartTimer()
		// Call the function being benchmarked.
		chunkedWrite(shard, points)

		b.StopTimer()
		shard.Close()
		os.RemoveAll(tmpDir)
	}
}

// benchmarkWritePointsExistingSeries benchmarks writing to existing series in a shard.
// mCnt - measurement count
// tkCnt - tag key count
// tvCnt - tag value count (values per tag)
// pntCnt - points per series.  # of series = mCnt * (tvCnt ^ tkCnt)
func benchmarkWritePointsExistingSeries(b *testing.B, mCnt, tkCnt, tvCnt, pntCnt int) {
	// Generate test series (measurements + unique tag sets).
	series := genTestSeries(mCnt, tkCnt, tvCnt)
	// Generate point data to write to the shard.
	points := []models.Point{}
	for _, s := range series {
		for val := 0.0; val < float64(pntCnt); val++ {
			p := models.MustNewPoint(s.Measurement, s.Tags, map[string]interface{}{"value": val}, time.Now())
			points = append(points, p)
		}
	}

	sfile := MustOpenSeriesFile()
	defer sfile.Close()

	shard, tmpDir, err := openShard(sfile)
	if err != nil {
		b.Fatal(err)
	}
	defer shard.Close()

	chunkedWrite(shard, points)

	// Reset timers and mem-stats before the main benchmark loop.
	b.ResetTimer()

	// Run the benchmark loop.
	for n := 0; n < b.N; n++ {
		b.StopTimer()

		for _, p := range points {
			p.SetTime(p.Time().Add(time.Second))
		}

		b.StartTimer()
		// Call the function being benchmarked.
		chunkedWrite(shard, points)
	}
	os.RemoveAll(tmpDir)
}

func benchmarkWritePointsExistingSeriesFields(b *testing.B, mCnt, tkCnt, tvCnt, pntCnt int) {
	// Generate test series (measurements + unique tag sets).
	series := genTestSeries(mCnt, tkCnt, tvCnt)
	// Generate point data to write to the shard.
	points := []models.Point{}
	for _, s := range series {
		i := 0
		for val := 0.0; val < float64(pntCnt); val++ {
			field := fmt.Sprintf("v%d", i%256)
			p := models.MustNewPoint(s.Measurement, s.Tags, map[string]interface{}{field: val}, time.Now())
			points = append(points, p)
			i++
		}
	}

	sfile := MustOpenSeriesFile()
	defer sfile.Close()

	shard, tmpDir, err := openShard(sfile)
	if err != nil {
		b.Fatal(err)
	}
	defer shard.Close()

	chunkedWrite(shard, points)

	// Reset timers and mem-stats before the main benchmark loop.
	b.ResetTimer()

	// Run the benchmark loop.
	for n := 0; n < b.N; n++ {
		b.StopTimer()

		for _, p := range points {
			p.SetTime(p.Time().Add(time.Second))
		}

		b.StartTimer()
		// Call the function being benchmarked.
		chunkedWrite(shard, points)
	}
	os.RemoveAll(tmpDir)
}

func benchmarkWritePointsExistingSeriesEqualBatches(b *testing.B, mCnt, tkCnt, tvCnt, pntCnt int) {
	// Generate test series (measurements + unique tag sets).
	series := genTestSeries(mCnt, tkCnt, tvCnt)
	// Generate point data to write to the shard.
	points := []models.Point{}
	for _, s := range series {
		for val := 0.0; val < float64(pntCnt); val++ {
			p := models.MustNewPoint(s.Measurement, s.Tags, map[string]interface{}{"value": val}, time.Now())
			points = append(points, p)
		}
	}

	sfile := MustOpenSeriesFile()
	defer sfile.Close()

	shard, tmpDir, err := openShard(sfile)
	if err != nil {
		b.Fatal(err)
	}
	defer shard.Close()

	chunkedWrite(shard, points)

	// Reset timers and mem-stats before the main benchmark loop.
	b.ResetTimer()

	// Run the benchmark loop.
	nPts := len(points)
	chunkSz := 10000
	start := 0
	end := chunkSz
	for n := 0; n < b.N; n++ {
		b.StopTimer()

		if end > nPts {
			end = nPts
		}
		if end-start == 0 {
			start = 0
			end = chunkSz
		}

		for _, p := range points[start:end] {
			p.SetTime(p.Time().Add(time.Second))
		}

		b.StartTimer()
		shard.WritePoints(points[start:end], tsdb.NoopStatsTracker())
		b.StopTimer()

		start = end
		end += chunkSz
	}
	os.RemoveAll(tmpDir)
}

func openShard(sfile *SeriesFile) (*tsdb.Shard, string, error) {
	tmpDir, _ := os.MkdirTemp("", "shard_test")
	tmpShard := filepath.Join(tmpDir, "shard")
	tmpWal := filepath.Join(tmpDir, "wal")
	opts := tsdb.NewEngineOptions()
	opts.Config.WALDir = tmpWal
	opts.InmemIndex = inmem.NewIndex(filepath.Base(tmpDir), sfile.SeriesFile)
	shard := tsdb.NewShard(1, tmpShard, tmpWal, sfile.SeriesFile, opts)
	err := shard.Open()
	return shard, tmpDir, err
}

func BenchmarkCreateIterator(b *testing.B) {
	// Generate test series (measurements + unique tag sets).
	series := genTestSeries(1, 6, 4)
	// Generate point data to write to the shard.
	points := make([]models.Point, 0, len(series))
	for _, s := range series {
		p := models.MustNewPoint(s.Measurement, s.Tags, map[string]interface{}{"v0": 1.0, "v1": 1.0}, time.Now())
		points = append(points, p)
	}

	setup := func(index string, shards Shards) {
		// Write all the points to all the shards.
		for _, sh := range shards {
			if err := sh.WritePoints(points, tsdb.NoopStatsTracker()); err != nil {
				b.Fatal(err)
			}
		}
	}

	for _, index := range tsdb.RegisteredIndexes() {
		var shards Shards
		for i := 1; i <= 5; i++ {
			name := fmt.Sprintf("%s_shards_%d", index, i)
			shards = NewShards(index, i)
			shards.MustOpen()

			setup(index, shards)
			b.Run(name, func(b *testing.B) {
				defer shards.Close()

				m := &influxql.Measurement{
					Database:        "db0",
					RetentionPolicy: "rp0",
					Name:            "measurement0",
				}

				opts := query.IteratorOptions{
					Aux:        []influxql.VarRef{{Val: "v0", Type: 1}, {Val: "v1", Type: 1}},
					StartTime:  models.MinNanoTime,
					EndTime:    models.MaxNanoTime,
					Ascending:  false,
					Limit:      5,
					Ordered:    true,
					Authorizer: query.OpenAuthorizer,
				}

				opts.Condition = &influxql.BinaryExpr{
					Op: 27,
					LHS: &influxql.BinaryExpr{
						Op:  29,
						LHS: &influxql.VarRef{Val: "tagKey1", Type: 7},
						RHS: &influxql.StringLiteral{Val: "tagValue1"},
					},
					RHS: &influxql.BinaryExpr{
						Op:  29,
						LHS: &influxql.VarRef{Val: "tagKey2", Type: 7},
						RHS: &influxql.StringLiteral{Val: "tagValue1"},
					},
				}
				for i := 0; i < b.N; i++ {
					shards.Shards().CreateIterator(context.Background(), m, opts)
				}
			})
		}
	}
}

func chunkedWrite(shard *tsdb.Shard, points []models.Point) {
	nPts := len(points)
	chunkSz := 10000
	start := 0
	end := chunkSz

	for {
		if end > nPts {
			end = nPts
		}
		if end-start == 0 {
			break
		}

		shard.WritePoints(points[start:end], tsdb.NoopStatsTracker())
		start = end
		end += chunkSz
	}
}

// Shard represents a test wrapper for tsdb.Shard.
type Shard struct {
	*tsdb.Shard
	sfile *SeriesFile
	path  string
}

type Shards []*Shard

// NewShard returns a new instance of Shard with temp paths.
func NewShard(index string) *Shard {
	return NewShards(index, 1)[0]
}

// MustNewOpenShard creates and opens a shard with the provided index.
func MustNewOpenShard(index string) *Shard {
	sh := NewShard(index)
	if err := sh.Open(); err != nil {
		panic(err)
	}
	return sh
}

// Close closes the shard and removes all underlying data.
func (sh *Shard) Close() error {
	// Will remove temp series file data.
	if err := sh.sfile.Close(); err != nil {
		return err
	}

	defer os.RemoveAll(sh.path)
	return sh.Shard.Close()
}

// NewShards create several shards all sharing the same
func NewShards(index string, n int) Shards {
	// Create temporary path for data and WAL.
	dir, err := os.MkdirTemp("", "influxdb-tsdb-")
	if err != nil {
		panic(err)
	}

	sfile := MustOpenSeriesFile()

	var shards []*Shard
	var idSets []*tsdb.SeriesIDSet
	for i := 0; i < n; i++ {
		idSets = append(idSets, tsdb.NewSeriesIDSet())
	}

	for i := 0; i < n; i++ {
		// Build engine options.
		opt := tsdb.NewEngineOptions()
		opt.IndexVersion = index
		opt.Config.WALDir = filepath.Join(dir, "wal")
		if index == tsdb.InmemIndexName {
			opt.InmemIndex = inmem.NewIndex(filepath.Base(dir), sfile.SeriesFile)
		}

		// Initialise series id sets. Need to do this as it's normally done at the
		// store level.
		opt.SeriesIDSets = seriesIDSets(idSets)

		sh := &Shard{
			Shard: tsdb.NewShard(uint64(i),
				filepath.Join(dir, "data", "db0", "rp0", fmt.Sprint(i)),
				filepath.Join(dir, "wal", "db0", "rp0", fmt.Sprint(i)),
				sfile.SeriesFile,
				opt,
			),
			sfile: sfile,
			path:  dir,
		}

		shards = append(shards, sh)
	}
	return Shards(shards)
}

// Open opens all the underlying shards.
func (a Shards) Open() error {
	for _, sh := range a {
		if err := sh.Open(); err != nil {
			return err
		}
	}
	return nil
}

// MustOpen opens all the shards, panicking if an error is encountered.
func (a Shards) MustOpen() {
	if err := a.Open(); err != nil {
		panic(err)
	}
}

// Shards returns the set of shards as a tsdb.Shards type.
func (a Shards) Shards() tsdb.Shards {
	var all tsdb.Shards
	for _, sh := range a {
		all = append(all, sh.Shard)
	}
	return all
}

// Close closes all shards and removes all underlying data.
func (a Shards) Close() error {
	if len(a) == 1 {
		return a[0].Close()
	}

	// Will remove temp series file data.
	if err := a[0].sfile.Close(); err != nil {
		return err
	}

	defer os.RemoveAll(a[0].path)
	for _, sh := range a {
		if err := sh.Shard.Close(); err != nil {
			return err
		}
	}
	return nil
}

// MustWritePointsString parses the line protocol (with second precision) and
// inserts the resulting points into the shard. Panic on error.
func (sh *Shard) MustWritePointsString(s string) {
	a, err := models.ParsePointsWithPrecision([]byte(strings.TrimSpace(s)), time.Time{}, "s")
	if err != nil {
		panic(err)
	}

	if err := sh.WritePoints(a, tsdb.NoopStatsTracker()); err != nil {
		panic(err)
	}
}

func MustTempDir() (string, func()) {
	dir, err := os.MkdirTemp("", "shard-test")
	if err != nil {
		panic(fmt.Sprintf("failed to create temp dir: %v", err))
	}
	return dir, func() { os.RemoveAll(dir) }
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

// Tests concurrently writing to the same shard with different field types which
// can trigger a panic when the shard is snapshotted to TSM files.
func TestShard_WritePoints_ForceFieldConflictConcurrent(t *testing.T) {
	const Runs = 50
	if testing.Short() || runtime.GOOS == "windows" {
		t.Skip("Skipping on short or windows")
	}
	for i := 0; i < Runs; i++ {
		conflictShard(t, i)
	}
}

func conflictShard(t *testing.T, run int) {
	const measurement = "cpu"
	const field = "value"
	const numTypes = 4 // float, int, bool, string
	const pointCopies = 10
	const trialsPerShard = 10

	tmpDir, _ := os.MkdirTemp("", "shard_test")
	defer func() {
		require.NoError(t, os.RemoveAll(tmpDir), "removing %s", tmpDir)
	}()
	tmpShard := filepath.Join(tmpDir, "shard")
	tmpWal := filepath.Join(tmpDir, "wal")

	sfile := MustOpenSeriesFile()
	defer func() {
		require.NoError(t, sfile.Close(), "closing series file")
		require.NoError(t, os.RemoveAll(sfile.Path()), "removing series file %s", sfile.Path())
	}()

	opts := tsdb.NewEngineOptions()
	opts.Config.WALDir = tmpWal
	opts.InmemIndex = inmem.NewIndex(filepath.Base(tmpDir), sfile.SeriesFile)
	opts.SeriesIDSets = seriesIDSets([]*tsdb.SeriesIDSet{})
	sh := tsdb.NewShard(1, tmpShard, tmpWal, sfile.SeriesFile, opts)
	require.NoError(t, sh.Open(), "opening shard: %s", sh.Path())
	defer func() {
		require.NoError(t, sh.Close(), "closing shard %s", tmpShard)
	}()
	var wg sync.WaitGroup
	mu := sync.RWMutex{}
	maxConcurrency := atomic.Int64{}

	currentTime := time.Now()

	points := make([]models.Point, 0, pointCopies*numTypes)

	for i := 0; i < pointCopies; i++ {
		points = append(points, models.MustNewPoint(
			measurement,
			models.NewTags(map[string]string{"host": "server"}),
			map[string]interface{}{field: 1.0},
			currentTime.Add(time.Duration(i)*time.Second),
		))
		points = append(points, models.MustNewPoint(
			measurement,
			models.NewTags(map[string]string{"host": "server"}),
			map[string]interface{}{field: int64(1)},
			currentTime.Add(time.Duration(i)*time.Second),
		))
		points = append(points, models.MustNewPoint(
			measurement,
			models.NewTags(map[string]string{"host": "server"}),
			map[string]interface{}{field: "one"},
			currentTime.Add(time.Duration(i)*time.Second),
		))
		points = append(points, models.MustNewPoint(
			measurement,
			models.NewTags(map[string]string{"host": "server"}),
			map[string]interface{}{field: true},
			currentTime.Add(time.Duration(i)*time.Second),
		))
	}
	concurrency := atomic.Int64{}

	for i := 0; i < trialsPerShard; i++ {
		mu.Lock()
		wg.Add(len(points))
		// Write points concurrently
		for i := 0; i < pointCopies; i++ {
			for j := 0; j < numTypes; j++ {
				concurrency.Add(1)
				go func(mp models.Point) {
					mu.RLock()
					defer concurrency.Add(-1)
					defer mu.RUnlock()
					defer wg.Done()
					if err := sh.WritePoints([]models.Point{mp}, tsdb.NoopStatsTracker()); err == nil {
						fs, err := mp.Fields()
						require.NoError(t, err, "getting fields")
						require.Equal(t,
							sh.MeasurementFields([]byte(measurement)).Field(field).Type,
							influxql.InspectDataType(fs[field]),
							"field types mismatch on run %d: types exp: %s, got: %s", run+1, sh.MeasurementFields([]byte(measurement)).Field(field).Type.String(), influxql.InspectDataType(fs[field]).String())
					} else {
						require.ErrorContains(t, err, tsdb.ErrFieldTypeConflict.Error(), "unexpected error")
					}
					if c := concurrency.Load(); maxConcurrency.Load() < c {
						maxConcurrency.Store(c)
					}
				}(points[i*numTypes+j])
			}
		}
		mu.Unlock()
		wg.Wait()
		dir, err := sh.CreateSnapshot(false)
		require.NoError(t, err, "creating snapshot: %s", sh.Path())
		require.NoError(t, os.RemoveAll(dir), "removing snapshot directory %s", dir)
	}
	keyType := map[string]byte{}
	files, err := os.ReadDir(tmpShard)
	require.NoError(t, err, "reading shard directory %s", tmpShard)
	for i, file := range files {
		if !strings.HasSuffix(path.Ext(file.Name()), tsm1.TSMFileExtension) {
			continue
		}
		ffile := path.Join(tmpShard, file.Name())
		fh, err := os.Open(ffile)
		require.NoError(t, err, "opening snapshot file %s", ffile)
		tr, err := tsm1.NewTSMReader(fh)
		require.NoError(t, err, "creating TSM reader for %s", ffile)
		key, typ := tr.KeyAt(0)
		if oldTyp, ok := keyType[string(key)]; ok {
			require.Equal(t, oldTyp, typ,
				"field type mismatch in run %d TSM file %d -- %q in %s\nfirst seen: %s, newest: %s, field type: %s",
				run+1,
				i+1,
				string(key),
				ffile,
				blockTypeString(oldTyp),
				blockTypeString(typ),
				sh.MeasurementFields([]byte(measurement)).Field(field).Type.String())
		} else {
			keyType[string(key)] = typ
		}
		// Must close after all uses of key (mapped memory)
		require.NoError(t, tr.Close(), "closing TSM reader")
	}
	// t.Logf("Type %s wins run %d with concurrency: %d", sh.MeasurementFields([]byte(measurement)).Field(field).Type.String(), run+1, maxConcurrency.Load())
}

func blockTypeString(typ byte) string {
	switch typ {
	case tsm1.BlockFloat64:
		return "float64"
	case tsm1.BlockInteger:
		return "int64"
	case tsm1.BlockBoolean:
		return "bool"
	case tsm1.BlockString:
		return "string"
	default:
		return "unknown"
	}
}
