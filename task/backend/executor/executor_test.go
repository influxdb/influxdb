package executor

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/influxdata/flux"
	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/authorization"
	icontext "github.com/influxdata/influxdb/v2/context"
	"github.com/influxdata/influxdb/v2/inmem"
	"github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/influxdata/influxdb/v2/kit/prom"
	"github.com/influxdata/influxdb/v2/kit/prom/promtest"
	tracetest "github.com/influxdata/influxdb/v2/kit/tracing/testing"
	"github.com/influxdata/influxdb/v2/kv"
	"github.com/influxdata/influxdb/v2/kv/migration/all"
	"github.com/influxdata/influxdb/v2/query"
	"github.com/influxdata/influxdb/v2/query/fluxlang"
	"github.com/influxdata/influxdb/v2/task/backend"
	"github.com/influxdata/influxdb/v2/task/backend/executor/mock"
	"github.com/influxdata/influxdb/v2/task/backend/scheduler"
	"github.com/influxdata/influxdb/v2/task/taskmodel"
	"github.com/influxdata/influxdb/v2/tenant"
	"github.com/opentracing/opentracing-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber/jaeger-client-go"
	"go.uber.org/zap/zaptest"
)

func TestMain(m *testing.M) {
	var code int
	func() {
		defer tracetest.SetupInMemoryTracing("task_backend_tests")()

		code = m.Run()
	}()

	os.Exit(code)
}

type tes struct {
	svc     *fakeQueryService
	ex      *Executor
	metrics *ExecutorMetrics
	i       *kv.Service
	tcs     *taskControlService
	tc      testCreds
}

func runTestWithTokenHashing(name string, testFunc func(bool, *testing.T), t *testing.T) {
	t.Helper()
	for _, useTokenHashing := range []bool{false, true} {
		t.Run(fmt.Sprintf("%s/TokenHashing=%t", name, useTokenHashing), func(t *testing.T) {
			testFunc(useTokenHashing, t)
		})
	}
}

func taskExecutorSystem(useTokenHashing bool, t *testing.T) tes {
	var (
		aqs = newFakeQueryService()
		qs  = query.QueryServiceBridge{
			AsyncQueryService: aqs,
		}
		ctx    = context.Background()
		logger = zaptest.NewLogger(t)
		store  = inmem.NewKVStore()
	)

	if err := all.Up(ctx, logger, store); err != nil {
		t.Fatal(err)
	}
	ctrl := gomock.NewController(t)
	ps := mock.NewMockPermissionService(ctrl)
	ps.EXPECT().FindPermissionForUser(gomock.Any(), gomock.Any()).Return(influxdb.PermissionSet{}, nil).AnyTimes()

	tenantStore := tenant.NewStore(store)
	tenantSvc := tenant.NewService(tenantStore)

	authStore, err := authorization.NewStore(ctx, store, useTokenHashing)
	require.NoError(t, err)
	authSvc := authorization.NewService(authStore, tenantSvc)

	var (
		svc = kv.NewService(logger, store, tenantSvc, kv.ServiceConfig{
			FluxLanguageService: fluxlang.DefaultService,
		})

		tcs         = &taskControlService{TaskControlService: svc}
		ex, metrics = NewExecutor(zaptest.NewLogger(t), qs, ps, svc, tcs)
	)
	return tes{
		svc:     aqs,
		ex:      ex,
		metrics: metrics,
		i:       svc,
		tcs:     tcs,
		tc:      createCreds(t, tenantSvc, tenantSvc, authSvc),
	}
}

func TestTaskExecutor(t *testing.T) {

	runTestWithTokenHashing("QuerySuccess", testQuerySuccess, t)
	runTestWithTokenHashing("QueryFailure", testQueryFailure, t)
	runTestWithTokenHashing("ManualRun", testManualRun, t)
	runTestWithTokenHashing("ResumeRun", testResumingRun, t)
	runTestWithTokenHashing("WorkerLimit", testWorkerLimit, t)
	runTestWithTokenHashing("LimitFunc", testLimitFunc, t)
	runTestWithTokenHashing("Metrics", testMetrics, t)
	runTestWithTokenHashing("IteratorFailure", testIteratorFailure, t)
	runTestWithTokenHashing("ErrorHandling", testErrorHandling, t)
}

func testQuerySuccess(useTokenHashing bool, t *testing.T) {
	t.Parallel()

	tes := taskExecutorSystem(useTokenHashing, t)

	var (
		script = fmt.Sprintf(fmtTestScript, t.Name())
		ctx    = icontext.SetAuthorizer(context.Background(), tes.tc.Auth)
		span   = opentracing.GlobalTracer().StartSpan("test-span")
	)
	ctx = opentracing.ContextWithSpan(ctx, span)

	task, err := tes.i.CreateTask(ctx, taskmodel.TaskCreate{OrganizationID: tes.tc.OrgID, OwnerID: tes.tc.Auth.GetUserID(), Flux: script})
	if err != nil {
		t.Fatal(err)
	}

	promise, err := tes.ex.PromisedExecute(ctx, scheduler.ID(task.ID), time.Unix(123, 0), time.Unix(126, 0))
	if err != nil {
		t.Fatal(err)
	}
	promiseID := platform.ID(promise.ID())

	run, err := tes.i.FindRunByID(context.Background(), task.ID, promiseID)
	if err != nil {
		t.Fatal(err)
	}

	if run.ID != promiseID {
		t.Fatal("promise and run dont match")
	}

	if run.RunAt != time.Unix(126, 0).UTC() {
		t.Fatalf("did not correctly set RunAt value, got: %v", run.RunAt)
	}

	tes.svc.WaitForQueryLive(t, script)
	tes.svc.SucceedQuery(script)

	<-promise.Done()

	if got := promise.Error(); got != nil {
		t.Fatal(got)
	}

	// confirm run is removed from in-mem store
	run, err = tes.i.FindRunByID(context.Background(), task.ID, run.ID)
	if run != nil || err == nil || !strings.Contains(err.Error(), "run not found") {
		t.Fatal("run was returned when it should have been removed from kv")
	}

	// ensure the run returned by TaskControlService.FinishRun(...)
	// has run logs formatted as expected
	if run = tes.tcs.run; run == nil {
		t.Fatal("expected run returned by FinishRun to not be nil")
	}

	if len(run.Log) < 3 {
		t.Fatalf("expected 3 run logs, found %d", len(run.Log))
	}

	sctx := span.Context().(jaeger.SpanContext)
	expectedMessage := fmt.Sprintf("trace_id=%s is_sampled=true", sctx.TraceID())
	if expectedMessage != run.Log[1].Message {
		t.Errorf("expected %q, found %q", expectedMessage, run.Log[1].Message)
	}
}

func testQueryFailure(useTokenHashing bool, t *testing.T) {
	t.Parallel()
	tes := taskExecutorSystem(useTokenHashing, t)

	script := fmt.Sprintf(fmtTestScript, t.Name())
	ctx := icontext.SetAuthorizer(context.Background(), tes.tc.Auth)
	task, err := tes.i.CreateTask(ctx, taskmodel.TaskCreate{OrganizationID: tes.tc.OrgID, OwnerID: tes.tc.Auth.GetUserID(), Flux: script})
	if err != nil {
		t.Fatal(err)
	}

	promise, err := tes.ex.PromisedExecute(ctx, scheduler.ID(task.ID), time.Unix(123, 0), time.Unix(126, 0))
	if err != nil {
		t.Fatal(err)
	}
	promiseID := platform.ID(promise.ID())

	run, err := tes.i.FindRunByID(context.Background(), task.ID, promiseID)
	if err != nil {
		t.Fatal(err)
	}

	if run.ID != promiseID {
		t.Fatal("promise and run dont match")
	}

	tes.svc.WaitForQueryLive(t, script)
	tes.svc.FailQuery(script, errors.New("blargyblargblarg"))

	<-promise.Done()

	if got := promise.Error(); got == nil {
		t.Fatal("got no error when I should have")
	}
}

func testManualRun(useTokenHashing bool, t *testing.T) {
	t.Parallel()
	tes := taskExecutorSystem(useTokenHashing, t)

	script := fmt.Sprintf(fmtTestScript, t.Name())
	ctx := icontext.SetAuthorizer(context.Background(), tes.tc.Auth)
	task, err := tes.i.CreateTask(ctx, taskmodel.TaskCreate{OrganizationID: tes.tc.OrgID, OwnerID: tes.tc.Auth.GetUserID(), Flux: script})
	if err != nil {
		t.Fatal(err)
	}

	manualRun, err := tes.i.ForceRun(ctx, task.ID, 123)
	if err != nil {
		t.Fatal(err)
	}

	mrs, err := tes.i.ManualRuns(ctx, task.ID)
	if err != nil {
		t.Fatal(err)
	}

	if len(mrs) != 1 {
		t.Fatal("manual run not created by force run")
	}

	promise, err := tes.ex.ManualRun(ctx, task.ID, manualRun.ID)
	if err != nil {
		t.Fatal(err)
	}

	run, err := tes.i.FindRunByID(context.Background(), task.ID, promise.ID())
	if err != nil {
		t.Fatal(err)
	}

	if run.ID != promise.ID() || manualRun.ID != promise.ID() {
		t.Fatal("promise and run and manual run dont match")
	}

	tes.svc.WaitForQueryLive(t, script)
	tes.svc.SucceedQuery(script)

	if got := promise.Error(); got != nil {
		t.Fatal(got)
	}
}

func testResumingRun(useTokenHashing bool, t *testing.T) {
	t.Parallel()
	tes := taskExecutorSystem(useTokenHashing, t)

	script := fmt.Sprintf(fmtTestScript, t.Name())
	ctx := icontext.SetAuthorizer(context.Background(), tes.tc.Auth)
	task, err := tes.i.CreateTask(ctx, taskmodel.TaskCreate{OrganizationID: tes.tc.OrgID, OwnerID: tes.tc.Auth.GetUserID(), Flux: script})
	if err != nil {
		t.Fatal(err)
	}

	stalledRun, err := tes.i.CreateRun(ctx, task.ID, time.Unix(123, 0), time.Unix(126, 0))
	if err != nil {
		t.Fatal(err)
	}

	promise, err := tes.ex.ResumeCurrentRun(ctx, task.ID, stalledRun.ID)
	if err != nil {
		t.Fatal(err)
	}

	// ensure that it doesn't recreate a promise
	if _, err := tes.ex.ResumeCurrentRun(ctx, task.ID, stalledRun.ID); err != taskmodel.ErrRunNotFound {
		t.Fatal("failed to error when run has already been resumed")
	}

	run, err := tes.i.FindRunByID(context.Background(), task.ID, promise.ID())
	if err != nil {
		t.Fatal(err)
	}

	if run.ID != promise.ID() || stalledRun.ID != promise.ID() {
		t.Fatal("promise and run and manual run dont match")
	}

	tes.svc.WaitForQueryLive(t, script)
	tes.svc.SucceedQuery(script)

	if got := promise.Error(); got != nil {
		t.Fatal(got)
	}
}

func testWorkerLimit(useTokenHashing bool, t *testing.T) {
	t.Parallel()
	tes := taskExecutorSystem(useTokenHashing, t)

	script := fmt.Sprintf(fmtTestScript, t.Name())
	ctx := icontext.SetAuthorizer(context.Background(), tes.tc.Auth)
	task, err := tes.i.CreateTask(ctx, taskmodel.TaskCreate{OrganizationID: tes.tc.OrgID, OwnerID: tes.tc.Auth.GetUserID(), Flux: script})
	if err != nil {
		t.Fatal(err)
	}

	promise, err := tes.ex.PromisedExecute(ctx, scheduler.ID(task.ID), time.Unix(123, 0), time.Unix(126, 0))
	if err != nil {
		t.Fatal(err)
	}

	if len(tes.ex.workerLimit) != 1 {
		t.Fatal("expected a worker to be started")
	}

	tes.svc.WaitForQueryLive(t, script)
	tes.svc.FailQuery(script, errors.New("blargyblargblarg"))

	<-promise.Done()

	if got := promise.Error(); got == nil {
		t.Fatal("got no error when I should have")
	}
}

func testLimitFunc(useTokenHashing bool, t *testing.T) {
	t.Parallel()
	tes := taskExecutorSystem(useTokenHashing, t)

	script := fmt.Sprintf(fmtTestScript, t.Name())
	ctx := icontext.SetAuthorizer(context.Background(), tes.tc.Auth)
	task, err := tes.i.CreateTask(ctx, taskmodel.TaskCreate{OrganizationID: tes.tc.OrgID, OwnerID: tes.tc.Auth.GetUserID(), Flux: script})
	if err != nil {
		t.Fatal(err)
	}
	forcedErr := errors.New("forced")
	forcedQueryErr := taskmodel.ErrQueryError(forcedErr)
	tes.svc.FailNextQuery(forcedErr)

	count := 0
	tes.ex.SetLimitFunc(func(*taskmodel.Task, *taskmodel.Run) error {
		count++
		if count < 2 {
			return errors.New("not there yet")
		}
		return nil
	})

	promise, err := tes.ex.PromisedExecute(ctx, scheduler.ID(task.ID), time.Unix(123, 0), time.Unix(126, 0))
	if err != nil {
		t.Fatal(err)
	}

	<-promise.Done()

	if got := promise.Error(); got.Error() != forcedQueryErr.Error() {
		t.Fatal("failed to get failure from forced error")
	}

	if count != 2 {
		t.Fatalf("failed to call limitFunc enough times: %d", count)
	}
}

func testMetrics(useTokenHashing bool, t *testing.T) {
	t.Parallel()
	tes := taskExecutorSystem(useTokenHashing, t)
	metrics := tes.metrics
	reg := prom.NewRegistry(zaptest.NewLogger(t))
	reg.MustRegister(metrics.PrometheusCollectors()...)

	mg := promtest.MustGather(t, reg)
	m := promtest.MustFindMetric(t, mg, "task_executor_total_runs_active", nil)
	assert.EqualValues(t, 0, *m.Gauge.Value, "unexpected number of active runs")

	script := fmt.Sprintf(fmtTestScript, t.Name())
	ctx := icontext.SetAuthorizer(context.Background(), tes.tc.Auth)
	task, err := tes.i.CreateTask(ctx, taskmodel.TaskCreate{OrganizationID: tes.tc.OrgID, OwnerID: tes.tc.Auth.GetUserID(), Flux: script})
	assert.NoError(t, err)

	promise, err := tes.ex.PromisedExecute(ctx, scheduler.ID(task.ID), time.Unix(123, 0), time.Unix(126, 0))
	assert.NoError(t, err)
	promiseID := promise.ID()

	run, err := tes.i.FindRunByID(context.Background(), task.ID, promiseID)
	assert.NoError(t, err)
	assert.EqualValues(t, promiseID, run.ID, "promise and run dont match")

	tes.svc.WaitForQueryLive(t, script)

	mg = promtest.MustGather(t, reg)
	m = promtest.MustFindMetric(t, mg, "task_executor_total_runs_active", nil)
	assert.EqualValues(t, 1, *m.Gauge.Value, "unexpected number of active runs")

	tes.svc.SucceedQuery(script)
	<-promise.Done()

	// N.B. You might think the _runs_complete and _runs_active metrics are updated atomically,
	// but that's not the case. As a task run completes and is being cleaned up, there's a small
	// window where it can be counted under both metrics.
	//
	// Our CI is very good at hitting this window, causing failures when we assert on the metric
	// values below. We sleep a small amount before gathering metrics to avoid flaky errors.
	time.Sleep(500 * time.Millisecond)

	mg = promtest.MustGather(t, reg)
	m = promtest.MustFindMetric(t, mg, "task_executor_total_runs_complete", map[string]string{"task_type": "", "status": "success"})
	assert.EqualValues(t, 1, *m.Counter.Value, "unexpected number of successful runs")

	m = promtest.MustFindMetric(t, mg, "task_executor_total_runs_active", nil)
	assert.EqualValues(t, 0, *m.Gauge.Value, "unexpected number of active runs")

	assert.NoError(t, promise.Error())

	// manual runs metrics
	mt, err := tes.i.CreateTask(ctx, taskmodel.TaskCreate{OrganizationID: tes.tc.OrgID, OwnerID: tes.tc.Auth.GetUserID(), Flux: script})
	assert.NoError(t, err)

	scheduledFor := int64(123)

	r, err := tes.i.ForceRun(ctx, mt.ID, scheduledFor)
	assert.NoError(t, err)

	_, err = tes.ex.ManualRun(ctx, mt.ID, r.ID)
	assert.NoError(t, err)

	mg = promtest.MustGather(t, reg)
	m = promtest.MustFindMetric(t, mg, "task_executor_manual_runs_counter", map[string]string{"taskID": mt.ID.String()})
	assert.EqualValues(t, 1, *m.Counter.Value, "unexpected number of manual runs")

	m = promtest.MustFindMetric(t, mg, "task_executor_run_latency_seconds", map[string]string{"task_type": ""})
	assert.GreaterOrEqual(t, *m.Histogram.SampleCount, uint64(1), "run latency metric not found")
	assert.Greater(t, *m.Histogram.SampleSum, float64(100), "run latency metric unexpectedly small")
}

func testIteratorFailure(useTokenHashing bool, t *testing.T) {
	t.Parallel()
	tes := taskExecutorSystem(useTokenHashing, t)

	// replace iterator exhaust function with one which errors
	tes.ex.workerPool = sync.Pool{New: func() interface{} {
		return &worker{
			e: tes.ex,
			exhaustResultIterators: func(flux.Result) error {
				return errors.New("something went wrong exhausting iterator")
			},
			systemBuildCompiler:    NewASTCompiler,
			nonSystemBuildCompiler: NewASTCompiler,
		}
	}}

	script := fmt.Sprintf(fmtTestScript, t.Name())
	ctx := icontext.SetAuthorizer(context.Background(), tes.tc.Auth)
	task, err := tes.i.CreateTask(ctx, taskmodel.TaskCreate{OrganizationID: tes.tc.OrgID, OwnerID: tes.tc.Auth.GetUserID(), Flux: script})
	if err != nil {
		t.Fatal(err)
	}

	promise, err := tes.ex.PromisedExecute(ctx, scheduler.ID(task.ID), time.Unix(123, 0), time.Unix(126, 0))
	if err != nil {
		t.Fatal(err)
	}
	promiseID := platform.ID(promise.ID())

	run, err := tes.i.FindRunByID(context.Background(), task.ID, promiseID)
	if err != nil {
		t.Fatal(err)
	}

	if run.ID != promiseID {
		t.Fatal("promise and run dont match")
	}

	tes.svc.WaitForQueryLive(t, script)
	tes.svc.SucceedQuery(script)

	<-promise.Done()

	if got := promise.Error(); got == nil {
		t.Fatal("got no error when I should have")
	}
}

func testErrorHandling(useTokenHashing bool, t *testing.T) {
	t.Parallel()
	tes := taskExecutorSystem(useTokenHashing, t)

	metrics := tes.metrics
	reg := prom.NewRegistry(zaptest.NewLogger(t))
	reg.MustRegister(metrics.PrometheusCollectors()...)

	script := fmt.Sprintf(fmtTestScript, t.Name())
	ctx := icontext.SetAuthorizer(context.Background(), tes.tc.Auth)
	task, err := tes.i.CreateTask(ctx, taskmodel.TaskCreate{OrganizationID: tes.tc.OrgID, OwnerID: tes.tc.Auth.GetUserID(), Flux: script, Status: "active"})
	if err != nil {
		t.Fatal(err)
	}

	// encountering a bucket not found error should log an unrecoverable error in the metrics
	forcedErr := errors.New("could not find bucket")
	tes.svc.FailNextQuery(forcedErr)

	promise, err := tes.ex.PromisedExecute(ctx, scheduler.ID(task.ID), time.Unix(123, 0), time.Unix(126, 0))
	if err != nil {
		t.Fatal(err)
	}

	<-promise.Done()

	mg := promtest.MustGather(t, reg)

	m := promtest.MustFindMetric(t, mg, "task_executor_unrecoverable_counter", map[string]string{"taskID": task.ID.String(), "errorType": "internal error"})
	if got := *m.Counter.Value; got != 1 {
		t.Fatalf("expected 1 unrecoverable error, got %v", got)
	}

	// TODO (al): once user notification system is put in place, this code should be uncommented
	// encountering a bucket not found error should deactivate the task
	/*
		inactive, err := tes.i.FindTaskByID(context.Background(), task.ID)
		if err != nil {
			t.Fatal(err)
		}
		if inactive.Status != "inactive" {
			t.Fatal("expected task to be deactivated after permanent error")
		}
	*/
}

func TestPromiseFailure(t *testing.T) {
	runTestWithTokenHashing("TestPromiseFailure", testPromiseFailure, t)
}

func testPromiseFailure(useTokenHashing bool, t *testing.T) {
	t.Parallel()

	tes := taskExecutorSystem(useTokenHashing, t)

	var (
		script = fmt.Sprintf(fmtTestScript, t.Name())
		ctx    = icontext.SetAuthorizer(context.Background(), tes.tc.Auth)
		span   = opentracing.GlobalTracer().StartSpan("test-span")
	)
	ctx = opentracing.ContextWithSpan(ctx, span)

	task, err := tes.i.CreateTask(ctx, taskmodel.TaskCreate{OrganizationID: tes.tc.OrgID, OwnerID: tes.tc.Auth.GetUserID(), Flux: script})
	if err != nil {
		t.Fatal(err)
	}

	if err := tes.i.DeleteTask(ctx, task.ID); err != nil {
		t.Fatal(err)
	}

	promise, err := tes.ex.PromisedExecute(ctx, scheduler.ID(task.ID), time.Unix(123, 0), time.Unix(126, 0))
	if err == nil {
		t.Fatal("failed to error on promise create")
	}

	if promise != nil {
		t.Fatalf("expected no promise but received one: %+v", promise)
	}

	runs, _, err := tes.i.FindRuns(context.Background(), taskmodel.RunFilter{Task: task.ID})
	if err != nil {
		t.Fatal(err)
	}

	if len(runs) != 1 {
		t.Fatalf("expected 1 runs on failed promise: got: %d, %#v", len(runs), runs[0])
	}

	if runs[0].Status != "failed" {
		t.Fatal("failed to set failed state")
	}

}

type taskControlService struct {
	backend.TaskControlService

	run *taskmodel.Run
}

func (t *taskControlService) FinishRun(ctx context.Context, taskID platform.ID, runID platform.ID) (*taskmodel.Run, error) {
	// ensure auth set on context
	_, err := icontext.GetAuthorizer(ctx)
	if err != nil {
		panic(err)
	}

	t.run, err = t.TaskControlService.FinishRun(ctx, taskID, runID)
	return t.run, err
}
