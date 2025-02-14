package backend_test

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/influxdata/flux"
	"github.com/influxdata/flux/dependencies/url"
	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/authorization"
	icontext "github.com/influxdata/influxdb/v2/context"
	_ "github.com/influxdata/influxdb/v2/fluxinit/static"
	"github.com/influxdata/influxdb/v2/inmem"
	"github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/influxdata/influxdb/v2/kv"
	"github.com/influxdata/influxdb/v2/kv/migration/all"
	"github.com/influxdata/influxdb/v2/mock"
	"github.com/influxdata/influxdb/v2/query"
	"github.com/influxdata/influxdb/v2/query/control"
	"github.com/influxdata/influxdb/v2/query/fluxlang"
	stdlib "github.com/influxdata/influxdb/v2/query/stdlib/influxdata/influxdb"
	"github.com/influxdata/influxdb/v2/storage"
	storageflux "github.com/influxdata/influxdb/v2/storage/flux"
	"github.com/influxdata/influxdb/v2/task/backend"
	"github.com/influxdata/influxdb/v2/task/servicetest"
	"github.com/influxdata/influxdb/v2/task/taskmodel"
	"github.com/influxdata/influxdb/v2/tenant"
	"github.com/influxdata/influxdb/v2/v1/services/meta"
	storage2 "github.com/influxdata/influxdb/v2/v1/services/storage"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest"
)

func runTestWithTokenHashing(name string, testFunc func(bool, *testing.T), t *testing.T) {
	t.Helper()
	for _, useTokenHashing := range []bool{false, true} {
		t.Run(fmt.Sprintf("%s/TokenHashing=%t", name, useTokenHashing), func(t *testing.T) {
			testFunc(useTokenHashing, t)
		})
	}
}

func TestAnalyticalStore(t *testing.T) {
	runTestWithTokenHashing("TestAnalyticalStore", runTestAnalyticalStore, t)
}

func runTestAnalyticalStore(useTokenHashing bool, t *testing.T) {
	t.Skip("https://github.com/influxdata/influxdb/issues/22920")
	servicetest.TestTaskService(
		t,
		func(t *testing.T) (*servicetest.System, context.CancelFunc) {
			ctx, cancelFunc := context.WithCancel(context.Background())
			logger := zaptest.NewLogger(t)
			store := inmem.NewKVStore()
			if err := all.Up(ctx, logger, store); err != nil {
				t.Fatal(err)
			}

			tenantStore := tenant.NewStore(store)
			ts := tenant.NewService(tenantStore)

			authStore, err := authorization.NewStore(ctx, store, useTokenHashing)
			require.NoError(t, err)
			authSvc := authorization.NewService(authStore, ts)

			svc := kv.NewService(logger, store, ts, kv.ServiceConfig{
				FluxLanguageService: fluxlang.DefaultService,
			})

			metaClient := meta.NewClient(meta.NewConfig(), store)
			require.NoError(t, metaClient.Open())

			var (
				ab       = newAnalyticalBackend(t, ts.OrganizationService, ts.BucketService, metaClient)
				svcStack = backend.NewAnalyticalStorage(logger, svc, ts.BucketService, svc, ab.PointsWriter(), ab.QueryService())
			)

			ts.BucketService = storage.NewBucketService(logger, ts.BucketService, ab.storageEngine)

			authCtx := icontext.SetAuthorizer(ctx, &influxdb.Authorization{
				Permissions: influxdb.OperPermissions(),
			})

			return &servicetest.System{
					TaskControlService:         svcStack,
					TaskService:                svcStack,
					OrganizationService:        ts.OrganizationService,
					UserService:                ts.UserService,
					UserResourceMappingService: ts.UserResourceMappingService,
					AuthorizationService:       authSvc,
					Ctx:                        authCtx,
					CallFinishRun:              true,
				}, func() {
					cancelFunc()
					ab.Close(t)
				}
		},
	)
}

func TestDeduplicateRuns(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	logger := zaptest.NewLogger(t)
	store := inmem.NewKVStore()
	if err := all.Up(context.Background(), logger, store); err != nil {
		t.Fatal(err)
	}

	tenantStore := tenant.NewStore(store)
	ts := tenant.NewService(tenantStore)

	metaClient := meta.NewClient(meta.NewConfig(), store)
	require.NoError(t, metaClient.Open())

	_, err := metaClient.CreateDatabase(platform.ID(10).String())
	require.NoError(t, err)

	ab := newAnalyticalBackend(t, ts.OrganizationService, ts.BucketService, metaClient)
	defer ab.Close(t)

	mockTS := &mock.TaskService{
		FindTaskByIDFn: func(context.Context, platform.ID) (*taskmodel.Task, error) {
			return &taskmodel.Task{ID: 1, OrganizationID: 20}, nil
		},
		FindRunsFn: func(context.Context, taskmodel.RunFilter) ([]*taskmodel.Run, int, error) {
			return []*taskmodel.Run{
				{ID: 2, Status: "started"},
			}, 1, nil
		},
	}
	mockTCS := &mock.TaskControlService{
		FinishRunFn: func(ctx context.Context, taskID, runID platform.ID) (*taskmodel.Run, error) {
			return &taskmodel.Run{ID: 2, TaskID: 1, Status: "success", ScheduledFor: time.Now(), StartedAt: time.Now().Add(1), FinishedAt: time.Now().Add(2)}, nil
		},
	}
	mockBS := mock.NewBucketService()

	svcStack := backend.NewAnalyticalStorage(zaptest.NewLogger(t), mockTS, mockBS, mockTCS, ab.PointsWriter(), ab.QueryService())

	_, err = svcStack.FinishRun(context.Background(), 1, 2)
	if err != nil {
		t.Fatal(err)
	}

	runs, _, err := svcStack.FindRuns(context.Background(), taskmodel.RunFilter{Task: 1})
	if err != nil {
		t.Fatal(err)
	}

	if len(runs) != 1 {
		t.Fatalf("expected 1 run but got %d", len(runs))
	}

	if runs[0].Status != "success" {
		t.Fatalf("expected the deduped run to be 'success', got: %s", runs[0].Status)
	}
}

type analyticalBackend struct {
	queryController *control.Controller
	rootDir         string
	storageEngine   *storage.Engine
}

func (ab *analyticalBackend) PointsWriter() storage.PointsWriter {
	return ab.storageEngine
}

func (ab *analyticalBackend) QueryService() query.QueryService {
	return query.QueryServiceBridge{AsyncQueryService: ab.queryController}
}

func (ab *analyticalBackend) Close(t *testing.T) {
	if err := ab.queryController.Shutdown(context.Background()); err != nil {
		t.Error(err)
	}
	if err := ab.storageEngine.Close(); err != nil {
		t.Error(err)
	}
	if err := os.RemoveAll(ab.rootDir); err != nil {
		t.Error(err)
	}
}

func newAnalyticalBackend(t *testing.T, orgSvc influxdb.OrganizationService, bucketSvc influxdb.BucketService, metaClient storage.MetaClient) *analyticalBackend {
	// Mostly copied out of cmd/influxd/main.go.
	logger := zaptest.NewLogger(t)

	rootDir := t.TempDir()

	engine := storage.NewEngine(rootDir, storage.NewConfig(), storage.WithMetaClient(metaClient))
	engine.WithLogger(logger)

	if err := engine.Open(context.Background()); err != nil {
		t.Fatal(err)
	}

	defer func() {
		if t.Failed() {
			engine.Close()
		}
	}()

	const (
		concurrencyQuota         = 10
		memoryBytesQuotaPerQuery = 1e6
		queueSize                = 10
	)

	// TODO(adam): do we need a proper secret service here?
	storageStore := storage2.NewStore(engine.TSDBStore(), engine.MetaClient())
	readsReader := storageflux.NewReader(storageStore)

	deps, err := stdlib.NewDependencies(readsReader, engine, bucketSvc, orgSvc, nil, nil, stdlib.WithURLValidator(url.PassValidator{}))
	if err != nil {
		t.Fatal(err)
	}
	cc := control.Config{
		ExecutorDependencies:     []flux.Dependency{deps},
		ConcurrencyQuota:         concurrencyQuota,
		MemoryBytesQuotaPerQuery: int64(memoryBytesQuotaPerQuery),
		QueueSize:                queueSize,
	}

	queryController, err := control.New(cc, logger.With(zap.String("service", "storage-reads")))
	if err != nil {
		t.Fatal(err)
	}

	return &analyticalBackend{
		queryController: queryController,
		rootDir:         rootDir,
		storageEngine:   engine,
	}
}
