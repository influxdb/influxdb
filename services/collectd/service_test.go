package collectd

import (
	"encoding/hex"
	"errors"
	"io/ioutil"
	"log"
	"net"
	"testing"
	"time"

	"github.com/influxdb/influxdb/cluster"
	"github.com/influxdb/influxdb/meta"
	"github.com/influxdb/influxdb/models"
	"github.com/influxdb/influxdb/toml"
)

// Test that the service checks / creates the target database on startup.
func TestService_CreatesDatabase(t *testing.T) {
	t.Parallel()

	s := newTestService(1, time.Second)

	createDatabaseCalled := false

	ms := &testMetaStore{}
	ms.CreateDatabaseIfNotExistsFn = func(name string) (*meta.DatabaseInfo, error) {
		if name != s.Config.Database {
			t.Errorf("\n\texp = %s\n\tgot = %s\n", s.Config.Database, name)
		}
		createDatabaseCalled = true
		return nil, nil
	}
	s.Service.MetaStore = ms

	s.Open()
	s.Close()

	if !createDatabaseCalled {
		t.Errorf("CreateDatabaseIfNotExists should have been called when the service opened.")
	}
}

// Test that the collectd service correctly batches points by BatchSize.
func TestService_BatchSize(t *testing.T) {
	t.Parallel()

	totalPoints := len(expPoints)

	// Batch sizes that totalTestPoints divide evenly by.
	batchSizes := []int{1, 2, 13}

	for _, batchSize := range batchSizes {
		func() {
			s := newTestService(batchSize, time.Second)

			pointCh := make(chan models.Point)
			s.MetaStore.CreateDatabaseIfNotExistsFn = func(name string) (*meta.DatabaseInfo, error) { return nil, nil }
			s.PointsWriter.WritePointsFn = func(req *cluster.WritePointsRequest) error {
				if len(req.Points) != batchSize {
					t.Errorf("\n\texp = %d\n\tgot = %d\n", batchSize, len(req.Points))
				}

				for _, p := range req.Points {
					pointCh <- p
				}
				return nil
			}

			if err := s.Open(); err != nil {
				t.Fatal(err)
			}
			defer func() { t.Log("closing service"); s.Close() }()

			// Get the address & port the service is listening on for collectd data.
			addr := s.Addr()
			conn, err := net.Dial("udp", addr.String())
			if err != nil {
				t.Fatal(err)
			}

			// Send the test data to the service.
			if n, err := conn.Write(testData); err != nil {
				t.Fatal(err)
			} else if n != len(testData) {
				t.Fatalf("only sent %d of %d bytes", n, len(testData))
			}

			points := []models.Point{}
		Loop:
			for {
				select {
				case p := <-pointCh:
					points = append(points, p)
					if len(points) == totalPoints {
						break Loop
					}
				case <-time.After(time.Second):
					t.Logf("exp %d points, got %d", totalPoints, len(points))
					t.Fatal("timed out waiting for points from collectd service")
				}
			}

			if len(points) != totalPoints {
				t.Fatalf("exp %d points, got %d", totalPoints, len(points))
			}

			for i, exp := range expPoints {
				got := points[i].String()
				if got != exp {
					t.Fatalf("\n\texp = %s\n\tgot = %s\n", exp, got)
				}
			}
		}()
	}
}

// Test that the collectd service correctly batches points using BatchDuration.
func TestService_BatchDuration(t *testing.T) {
	t.Parallel()

	totalPoints := len(expPoints)

	s := newTestService(5000, 250*time.Millisecond)

	pointCh := make(chan models.Point, 1000)
	s.MetaStore.CreateDatabaseIfNotExistsFn = func(name string) (*meta.DatabaseInfo, error) { return nil, nil }
	s.PointsWriter.WritePointsFn = func(req *cluster.WritePointsRequest) error {
		for _, p := range req.Points {
			pointCh <- p
		}
		return nil
	}

	if err := s.Open(); err != nil {
		t.Fatal(err)
	}
	defer func() { t.Log("closing service"); s.Close() }()

	// Get the address & port the service is listening on for collectd data.
	addr := s.Addr()
	conn, err := net.Dial("udp", addr.String())
	if err != nil {
		t.Fatal(err)
	}

	// Send the test data to the service.
	if n, err := conn.Write(testData); err != nil {
		t.Fatal(err)
	} else if n != len(testData) {
		t.Fatalf("only sent %d of %d bytes", n, len(testData))
	}

	points := []models.Point{}
Loop:
	for {
		select {
		case p := <-pointCh:
			points = append(points, p)
			if len(points) == totalPoints {
				break Loop
			}
		case <-time.After(time.Second):
			t.Logf("exp %d points, got %d", totalPoints, len(points))
			t.Fatal("timed out waiting for points from collectd service")
		}
	}

	if len(points) != totalPoints {
		t.Fatalf("exp %d points, got %d", totalPoints, len(points))
	}

	for i, exp := range expPoints {
		got := points[i].String()
		if got != exp {
			t.Fatalf("\n\texp = %s\n\tgot = %s\n", exp, got)
		}
	}
}

// Test that the service loads configured types.db file
func TestService_LoadSingleTypesDb(t *testing.T) {
	t.Parallel()

	s := newTestService(1, time.Second)

	createDatabaseCalled := false

	ms := &testMetaStore{}
	ms.CreateDatabaseIfNotExistsFn = func(name string) (*meta.DatabaseInfo, error) {
		if name != s.Config.Database {
			t.Errorf("\n\texp = %s\n\tgot = %s\n", s.Config.Database, name)
		}
		createDatabaseCalled = true
		return nil, nil
	}
	s.Service.MetaStore = ms

	s.Open()
	// validate test_types1.db was loaded
	if s.typesdb == nil {
		t.Fatal("types.db file should have been loaded.")
	}
	// a key present on test_types1.db should have been loaded
	if _, ok := s.typesdb["absolute"]; !ok {
		t.Fatal("key absolute should exist")
	}
	// a key not present on test_types1.db should not have been loaded
	if _, ok := s.typesdb["custom_metric"]; ok {
		t.Fatal("key absolute should exist")
	}
	s.Close()

	if !createDatabaseCalled {
		t.Errorf("CreateDatabaseIfNotExists should have been called when the service opened.")
	}
}

// Test that the service loads configured types.db multiple files
func TestService_LoadMultipleTypesDb(t *testing.T) {
	t.Parallel()

	s := newTestServiceWithTypesDB(1, time.Second, []string{"test_types1.db", "test_types2.db"})

	createDatabaseCalled := false

	ms := &testMetaStore{}
	ms.CreateDatabaseIfNotExistsFn = func(name string) (*meta.DatabaseInfo, error) {
		if name != s.Config.Database {
			t.Errorf("\n\texp = %s\n\tgot = %s\n", s.Config.Database, name)
		}
		createDatabaseCalled = true
		return nil, nil
	}
	s.Service.MetaStore = ms

	s.Open()
	// validate test_types1.db was loaded
	if s.typesdb == nil {
		t.Fatal("types.db file should have been loaded.")
	}
	// a key present on test_types1.db should have been loaded
	if _, ok := s.typesdb["absolute"]; !ok {
		t.Fatal("key absolute should exist")
	}
	// a key present on test_types2.db should have been loaded
	if _, ok := s.typesdb["custom_metric"]; !ok {
		t.Fatal("key absolute should exist")
	}
	s.Close()

	if !createDatabaseCalled {
		t.Errorf("CreateDatabaseIfNotExists should have been called when the service opened.")
	}
}

type testService struct {
	*Service
	MetaStore    testMetaStore
	PointsWriter testPointsWriter
}

func newTestService(batchSize int, batchDuration time.Duration) *testService {
	s := &testService{
		Service: NewService(Config{
			BindAddress:   "127.0.0.1:0",
			Database:      "collectd_test",
			BatchSize:     batchSize,
			BatchDuration: toml.Duration(batchDuration),
			TypesDB:       []string{"test_types1.db"},
		}),
	}
	s.Service.PointsWriter = &s.PointsWriter
	s.Service.MetaStore = &s.MetaStore

	if !testing.Verbose() {
		s.Logger = log.New(ioutil.Discard, "", log.LstdFlags)
	}

	return s
}

func newTestServiceWithTypesDB(batchSize int, batchDuration time.Duration, typesDbFiles []string) *testService {
	s := &testService{
		Service: NewService(Config{
			BindAddress:   "127.0.0.1:0",
			Database:      "collectd_test",
			BatchSize:     batchSize,
			BatchDuration: toml.Duration(batchDuration),
			TypesDB:       typesDbFiles,
		}),
	}
	s.Service.PointsWriter = &s.PointsWriter
	s.Service.MetaStore = &s.MetaStore

	if !testing.Verbose() {
		s.Logger = log.New(ioutil.Discard, "", log.LstdFlags)
	}

	return s
}

type testPointsWriter struct {
	WritePointsFn func(*cluster.WritePointsRequest) error
}

func (w *testPointsWriter) WritePoints(p *cluster.WritePointsRequest) error {
	return w.WritePointsFn(p)
}

type testMetaStore struct {
	CreateDatabaseIfNotExistsFn func(name string) (*meta.DatabaseInfo, error)
	//DatabaseFn func(name string) (*meta.DatabaseInfo, error)
}

func (ms *testMetaStore) CreateDatabaseIfNotExists(name string) (*meta.DatabaseInfo, error) {
	return ms.CreateDatabaseIfNotExistsFn(name)
}

func (ms *testMetaStore) WaitForLeader(d time.Duration) error {
	return nil
}

func wait(c chan struct{}, d time.Duration) (err error) {
	select {
	case <-c:
	case <-time.After(d):
		err = errors.New("timed out")
	}
	return
}

func waitInt(c chan int, d time.Duration) (i int, err error) {
	select {
	case i = <-c:
	case <-time.After(d):
		err = errors.New("timed out")
	}
	return
}

func check(err error) {
	if err != nil {
		panic(err)
	}
}

// Raw data sent by collectd, captured using Wireshark.
var testData = func() []byte {
	b, err := hex.DecodeString("000000167066312d36322d3231302d39342d313733000001000c00000000544928ff0007000c00000000000000050002000c656e74726f7079000004000c656e74726f7079000006000f0001010000000000007240000200086370750000030006310000040008637075000005000969646c65000006000f0001000000000000a674620005000977616974000006000f0001000000000000000000000200076466000003000500000400076466000005000d6c6976652d636f7700000600180002010100000000a090b641000000a0cb6a2742000200086370750000030006310000040008637075000005000e696e74657272757074000006000f00010000000000000000fe0005000c736f6674697271000006000f000100000000000000000000020007646600000300050000040007646600000500096c6976650000060018000201010000000000000000000000e0ec972742000200086370750000030006310000040008637075000005000a737465616c000006000f00010000000000000000000003000632000005000975736572000006000f0001000000000000005f36000500096e696365000006000f0001000000000000000ad80002000e696e746572666163650000030005000004000e69665f6f6374657473000005000b64756d6d79300000060018000200000000000000000000000000000000041a000200076466000004000764660000050008746d70000006001800020101000000000000f240000000a0ea972742000200086370750000030006320000040008637075000005000b73797374656d000006000f00010000000000000045d30002000e696e746572666163650000030005000004000f69665f7061636b657473000005000b64756d6d79300000060018000200000000000000000000000000000000000f000200086370750000030006320000040008637075000005000969646c65000006000f0001000000000000a66480000200076466000003000500000400076466000005000d72756e2d6c6f636b000006001800020101000000000000000000000000000054410002000e696e74657266616365000004000e69665f6572726f7273000005000b64756d6d793000000600180002000000000000000000000000000000000000000200086370750000030006320000040008637075000005000977616974000006000f00010000000000000000000005000e696e74657272757074000006000f0001000000000000000132")
	check(err)
	return b
}()

var expPoints = []string{
	"entropy_value,host=pf1-62-210-94-173,type=entropy value=288 1414080767000000000",
	"cpu_value,host=pf1-62-210-94-173,instance=1,type=cpu,type_instance=idle value=10908770 1414080767000000000",
	"cpu_value,host=pf1-62-210-94-173,instance=1,type=cpu,type_instance=wait value=0 1414080767000000000",
	"df_used,host=pf1-62-210-94-173,type=df,type_instance=live-cow value=378576896 1414080767000000000",
	"df_free,host=pf1-62-210-94-173,type=df,type_instance=live-cow value=50287988736 1414080767000000000",
	"cpu_value,host=pf1-62-210-94-173,instance=1,type=cpu,type_instance=interrupt value=254 1414080767000000000",
	"cpu_value,host=pf1-62-210-94-173,instance=1,type=cpu,type_instance=softirq value=0 1414080767000000000",
	"df_used,host=pf1-62-210-94-173,type=df,type_instance=live value=0 1414080767000000000",
	"df_free,host=pf1-62-210-94-173,type=df,type_instance=live value=50666565632 1414080767000000000",
	"cpu_value,host=pf1-62-210-94-173,instance=1,type=cpu,type_instance=steal value=0 1414080767000000000",
	"cpu_value,host=pf1-62-210-94-173,instance=2,type=cpu,type_instance=user value=24374 1414080767000000000",
	"cpu_value,host=pf1-62-210-94-173,instance=2,type=cpu,type_instance=nice value=2776 1414080767000000000",
	"interface_rx,host=pf1-62-210-94-173,type=if_octets,type_instance=dummy0 value=0 1414080767000000000",
	"interface_tx,host=pf1-62-210-94-173,type=if_octets,type_instance=dummy0 value=1050 1414080767000000000",
	"df_used,host=pf1-62-210-94-173,type=df,type_instance=tmp value=73728 1414080767000000000",
	"df_free,host=pf1-62-210-94-173,type=df,type_instance=tmp value=50666491904 1414080767000000000",
	"cpu_value,host=pf1-62-210-94-173,instance=2,type=cpu,type_instance=system value=17875 1414080767000000000",
	"interface_rx,host=pf1-62-210-94-173,type=if_packets,type_instance=dummy0 value=0 1414080767000000000",
	"interface_tx,host=pf1-62-210-94-173,type=if_packets,type_instance=dummy0 value=15 1414080767000000000",
	"cpu_value,host=pf1-62-210-94-173,instance=2,type=cpu,type_instance=idle value=10904704 1414080767000000000",
	"df_used,host=pf1-62-210-94-173,type=df,type_instance=run-lock value=0 1414080767000000000",
	"df_free,host=pf1-62-210-94-173,type=df,type_instance=run-lock value=5242880 1414080767000000000",
	"interface_rx,host=pf1-62-210-94-173,type=if_errors,type_instance=dummy0 value=0 1414080767000000000",
	"interface_tx,host=pf1-62-210-94-173,type=if_errors,type_instance=dummy0 value=0 1414080767000000000",
	"cpu_value,host=pf1-62-210-94-173,instance=2,type=cpu,type_instance=wait value=0 1414080767000000000",
	"cpu_value,host=pf1-62-210-94-173,instance=2,type=cpu,type_instance=interrupt value=306 1414080767000000000",
}
