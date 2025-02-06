package kv_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/google/go-cmp/cmp"
	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/authorization"
	icontext "github.com/influxdata/influxdb/v2/context"
	_ "github.com/influxdata/influxdb/v2/fluxinit/static"
	"github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/influxdata/influxdb/v2/kv"
	"github.com/influxdata/influxdb/v2/query/fluxlang"
	"github.com/influxdata/influxdb/v2/task/options"
	"github.com/influxdata/influxdb/v2/task/servicetest"
	"github.com/influxdata/influxdb/v2/task/taskmodel"
	"github.com/influxdata/influxdb/v2/tenant"
	itesting "github.com/influxdata/influxdb/v2/testing"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

func TestBoltTaskService(t *testing.T) {
	runTestWithTokenHashing("TestBoltTaskService", runTestBoltTaskService, t)
}

func runTestBoltTaskService(useTokenHashing bool, t *testing.T) {
	servicetest.TestTaskService(
		t,
		func(t *testing.T) (*servicetest.System, context.CancelFunc) {
			ctx, cancelFunc := context.WithCancel(context.Background())

			store, close := itesting.NewTestBoltStore(t)

			tenantStore := tenant.NewStore(store)
			ts := tenant.NewService(tenantStore)

			authStore, err := authorization.NewStore(ctx, store, useTokenHashing)
			require.NoError(t, err)
			authSvc := authorization.NewService(authStore, ts)

			service := kv.NewService(zaptest.NewLogger(t), store, ts, kv.ServiceConfig{
				FluxLanguageService: fluxlang.DefaultService,
			})

			go func() {
				<-ctx.Done()
				close()
			}()

			return &servicetest.System{
				TaskControlService:         service,
				TaskService:                service,
				OrganizationService:        ts.OrganizationService,
				UserService:                ts.UserService,
				UserResourceMappingService: ts.UserResourceMappingService,
				AuthorizationService:       authSvc,
				Ctx:                        ctx,
			}, cancelFunc
		},
		"transactional",
	)
}

type testService struct {
	Store   kv.Store
	Service *kv.Service
	Org     influxdb.Organization
	User    influxdb.User
	Auth    influxdb.Authorization
	Clock   clock.Clock
}

func newService(t *testing.T, ctx context.Context, c clock.Clock, useTokenHashing bool) *testService {
	t.Helper()

	if c == nil {
		c = clock.New()
	}

	var (
		ts    = &testService{}
		err   error
		store kv.SchemaStore
	)

	store = itesting.NewTestInmemStore(t)

	ts.Store = store

	tenantStore := tenant.NewStore(store)
	tenantSvc := tenant.NewService(tenantStore)

	authStore, err := authorization.NewStore(ctx, store, useTokenHashing)
	require.NoError(t, err)
	authSvc := authorization.NewService(authStore, tenantSvc)

	ts.Service = kv.NewService(zaptest.NewLogger(t), store, tenantSvc, kv.ServiceConfig{
		Clock:               c,
		FluxLanguageService: fluxlang.DefaultService,
	})

	ts.User = influxdb.User{Name: t.Name() + "-user"}
	if err := tenantSvc.CreateUser(ctx, &ts.User); err != nil {
		t.Fatal(err)
	}
	ts.Org = influxdb.Organization{Name: t.Name() + "-org"}
	if err := tenantSvc.CreateOrganization(ctx, &ts.Org); err != nil {
		t.Fatal(err)
	}

	if err := tenantSvc.CreateUserResourceMapping(ctx, &influxdb.UserResourceMapping{
		ResourceType: influxdb.OrgsResourceType,
		ResourceID:   ts.Org.ID,
		UserID:       ts.User.ID,
		UserType:     influxdb.Owner,
	}); err != nil {
		t.Fatal(err)
	}

	ts.Auth = influxdb.Authorization{
		OrgID:       ts.Org.ID,
		UserID:      ts.User.ID,
		Permissions: influxdb.OperPermissions(),
	}
	if err := authSvc.CreateAuthorization(context.Background(), &ts.Auth); err != nil {
		t.Fatal(err)
	}

	return ts
}

func TestRetrieveTaskWithBadAuth(t *testing.T) {
	runTestWithTokenHashing("TestRetrieveTaskWithBadAuth", runTestRetrieveTaskWithBadAuth, t)
}

func runTestRetrieveTaskWithBadAuth(useTokenHashing bool, t *testing.T) {
	ctx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()

	ts := newService(t, ctx, nil, useTokenHashing)

	ctx = icontext.SetAuthorizer(ctx, &ts.Auth)

	task, err := ts.Service.CreateTask(ctx, taskmodel.TaskCreate{
		Flux:           `option task = {name: "a task",every: 1h} from(bucket:"test") |> range(start:-1h)`,
		OrganizationID: ts.Org.ID,
		OwnerID:        ts.User.ID,
		Status:         string(taskmodel.TaskActive),
	})
	if err != nil {
		t.Fatal(err)
	}

	// convert task to old one with a bad auth
	err = ts.Store.Update(ctx, func(tx kv.Tx) error {
		b, err := tx.Bucket([]byte("tasksv1"))
		if err != nil {
			return err
		}
		bID, err := task.ID.Encode()
		if err != nil {
			return err
		}
		task.OwnerID = platform.ID(1)
		tbyte, err := json.Marshal(task)
		if err != nil {
			return err
		}
		// have to actually hack the bytes here because the system doesnt like us to encode bad id's.
		tbyte = bytes.Replace(tbyte, []byte(`,"ownerID":"0000000000000001"`), []byte{}, 1)
		if err := b.Put(bID, tbyte); err != nil {
			return err
		}

		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	// lets see if we can list and find the task
	newTask, err := ts.Service.FindTaskByID(ctx, task.ID)
	if err != nil {
		t.Fatal(err)
	}
	if newTask.ID != task.ID {
		t.Fatal("miss matching taskID's")
	}

	tasks, _, err := ts.Service.FindTasks(context.Background(), taskmodel.TaskFilter{})
	if err != nil {
		t.Fatal(err)
	}
	if len(tasks) != 1 {
		t.Fatal("failed to return task")
	}

	// test status filter
	active := string(taskmodel.TaskActive)
	tasksWithActiveFilter, _, err := ts.Service.FindTasks(context.Background(), taskmodel.TaskFilter{Status: &active})
	if err != nil {
		t.Fatal("could not find tasks")
	}
	if len(tasksWithActiveFilter) != 1 {
		t.Fatal("failed to find active task with filter")
	}
}

func TestService_UpdateTask_InactiveToActive(t *testing.T) {
	runTestWithTokenHashing("TestService_UpdateTask_InactiveToActive", runTestService_UpdateTask_InactiveToActive, t)
}

func runTestService_UpdateTask_InactiveToActive(useTokenHashing bool, t *testing.T) {
	ctx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()

	c := clock.NewMock()
	c.Set(time.Unix(1000, 0))

	ts := newService(t, ctx, c, useTokenHashing)

	ctx = icontext.SetAuthorizer(ctx, &ts.Auth)

	originalTask, err := ts.Service.CreateTask(ctx, taskmodel.TaskCreate{
		Flux:           `option task = {name: "a task",every: 1h} from(bucket:"test") |> range(start:-1h)`,
		OrganizationID: ts.Org.ID,
		OwnerID:        ts.User.ID,
		Status:         string(taskmodel.TaskActive),
	})
	if err != nil {
		t.Fatal("CreateTask", err)
	}

	v := taskmodel.TaskStatusInactive
	c.Add(1 * time.Second)
	exp := c.Now()
	updatedTask, err := ts.Service.UpdateTask(ctx, originalTask.ID, taskmodel.TaskUpdate{Status: &v, LatestCompleted: &exp, LatestScheduled: &exp})
	if err != nil {
		t.Fatal("UpdateTask", err)
	}

	if got := updatedTask.LatestScheduled; !got.Equal(exp) {
		t.Fatalf("unexpected -got/+exp\n%s", cmp.Diff(got.String(), exp.String()))
	}
	if got := updatedTask.LatestCompleted; !got.Equal(exp) {
		t.Fatalf("unexpected -got/+exp\n%s", cmp.Diff(got.String(), exp.String()))
	}

	c.Add(10 * time.Second)
	exp = c.Now()
	v = taskmodel.TaskStatusActive
	updatedTask, err = ts.Service.UpdateTask(ctx, originalTask.ID, taskmodel.TaskUpdate{Status: &v})
	if err != nil {
		t.Fatal("UpdateTask", err)
	}

	if got := updatedTask.LatestScheduled; !got.Equal(exp) {
		t.Fatalf("unexpected -got/+exp\n%s", cmp.Diff(got.String(), exp.String()))
	}
}

func TestTaskRunCancellation(t *testing.T) {
	runTestWithTokenHashing("TestTaskRunCancellation", runTestTaskRunCancellation, t)
}

func runTestTaskRunCancellation(useTokenHashing bool, t *testing.T) {
	store, closeSvc := itesting.NewTestBoltStore(t)
	defer closeSvc()

	ctx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()

	tenantStore := tenant.NewStore(store)
	tenantSvc := tenant.NewService(tenantStore)

	authStore, err := authorization.NewStore(ctx, store, useTokenHashing)
	require.NoError(t, err)
	authSvc := authorization.NewService(authStore, tenantSvc)

	service := kv.NewService(zaptest.NewLogger(t), store, tenantSvc, kv.ServiceConfig{
		FluxLanguageService: fluxlang.DefaultService,
	})

	u := &influxdb.User{Name: t.Name() + "-user"}
	if err := tenantSvc.CreateUser(ctx, u); err != nil {
		t.Fatal(err)
	}
	o := &influxdb.Organization{Name: t.Name() + "-org"}
	if err := tenantSvc.CreateOrganization(ctx, o); err != nil {
		t.Fatal(err)
	}

	if err := tenantSvc.CreateUserResourceMapping(ctx, &influxdb.UserResourceMapping{
		ResourceType: influxdb.OrgsResourceType,
		ResourceID:   o.ID,
		UserID:       u.ID,
		UserType:     influxdb.Owner,
	}); err != nil {
		t.Fatal(err)
	}

	authz := influxdb.Authorization{
		OrgID:       o.ID,
		UserID:      u.ID,
		Permissions: influxdb.OperPermissions(),
	}
	if err := authSvc.CreateAuthorization(context.Background(), &authz); err != nil {
		t.Fatal(err)
	}

	ctx = icontext.SetAuthorizer(ctx, &authz)

	task, err := service.CreateTask(ctx, taskmodel.TaskCreate{
		Flux:           `option task = {name: "a task",cron: "0 * * * *", offset: 20s} from(bucket:"test") |> range(start:-1h)`,
		OrganizationID: o.ID,
		OwnerID:        u.ID,
	})
	if err != nil {
		t.Fatal(err)
	}

	run, err := service.CreateRun(ctx, task.ID, time.Now().Add(time.Hour), time.Now().Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}

	if err := service.CancelRun(ctx, run.TaskID, run.ID); err != nil {
		t.Fatal(err)
	}

	canceled, err := service.FindRunByID(ctx, run.TaskID, run.ID)
	if err != nil {
		t.Fatal(err)
	}

	if canceled.Status != taskmodel.RunCanceled.String() {
		t.Fatalf("expected task run to be cancelled")
	}
}

func TestService_UpdateTask_RecordLatestSuccessAndFailure(t *testing.T) {
	runTestWithTokenHashing("TestService_UpdateTask_RecordLatestSuccessAndFailure", runTestService_UpdateTask_RecordLatestSuccessAndFailure, t)
}

func runTestService_UpdateTask_RecordLatestSuccessAndFailure(useTokenHashing bool, t *testing.T) {
	ctx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()

	c := clock.NewMock()
	c.Set(time.Unix(1000, 0))

	ts := newService(t, ctx, c, useTokenHashing)

	ctx = icontext.SetAuthorizer(ctx, &ts.Auth)

	originalTask, err := ts.Service.CreateTask(ctx, taskmodel.TaskCreate{
		Flux:           `option task = {name: "a task",every: 1h} from(bucket:"test") |> range(start:-1h)`,
		OrganizationID: ts.Org.ID,
		OwnerID:        ts.User.ID,
		Status:         string(taskmodel.TaskActive),
	})
	if err != nil {
		t.Fatal("CreateTask", err)
	}

	c.Add(1 * time.Second)
	exp := c.Now()
	updatedTask, err := ts.Service.UpdateTask(ctx, originalTask.ID, taskmodel.TaskUpdate{
		LatestCompleted: &exp,
		LatestScheduled: &exp,

		// These would be updated in a mutually exclusive manner, but we'll set
		// them both to demonstrate that they do change.
		LatestSuccess: &exp,
		LatestFailure: &exp,
	})
	if err != nil {
		t.Fatal("UpdateTask", err)
	}

	if got := updatedTask.LatestScheduled; !got.Equal(exp) {
		t.Fatalf("unexpected -got/+exp\n%s", cmp.Diff(got.String(), exp.String()))
	}
	if got := updatedTask.LatestCompleted; !got.Equal(exp) {
		t.Fatalf("unexpected -got/+exp\n%s", cmp.Diff(got.String(), exp.String()))
	}
	if got := updatedTask.LatestSuccess; !got.Equal(exp) {
		t.Fatalf("unexpected -got/+exp\n%s", cmp.Diff(got.String(), exp.String()))
	}
	if got := updatedTask.LatestFailure; !got.Equal(exp) {
		t.Fatalf("unexpected -got/+exp\n%s", cmp.Diff(got.String(), exp.String()))
	}

	c.Add(5 * time.Second)
	exp = c.Now()
	updatedTask, err = ts.Service.UpdateTask(ctx, originalTask.ID, taskmodel.TaskUpdate{
		LatestCompleted: &exp,
		LatestScheduled: &exp,

		// These would be updated in a mutually exclusive manner, but we'll set
		// them both to demonstrate that they do change.
		LatestSuccess: &exp,
		LatestFailure: &exp,
	})
	if err != nil {
		t.Fatal("UpdateTask", err)
	}

	if got := updatedTask.LatestScheduled; !got.Equal(exp) {
		t.Fatalf("unexpected -got/+exp\n%s", cmp.Diff(got.String(), exp.String()))
	}
	if got := updatedTask.LatestCompleted; !got.Equal(exp) {
		t.Fatalf("unexpected -got/+exp\n%s", cmp.Diff(got.String(), exp.String()))
	}
	if got := updatedTask.LatestSuccess; !got.Equal(exp) {
		t.Fatalf("unexpected -got/+exp\n%s", cmp.Diff(got.String(), exp.String()))
	}
	if got := updatedTask.LatestFailure; !got.Equal(exp) {
		t.Fatalf("unexpected -got/+exp\n%s", cmp.Diff(got.String(), exp.String()))
	}
}

type taskOptions struct {
	name        string
	every       string
	cron        string
	offset      string
	concurrency int64
	retry       int64
}

func TestExtractTaskOptions(t *testing.T) {
	tcs := []struct {
		name     string
		flux     string
		expected taskOptions
		errMsg   string
	}{
		{
			name: "all parameters",
			flux: `option task = {name: "whatever", every: 1s, offset: 0s, concurrency: 2, retry: 2}`,
			expected: taskOptions{
				name:        "whatever",
				every:       "1s",
				offset:      "0s",
				concurrency: 2,
				retry:       2,
			},
		},
		{
			name: "some extra whitespace and bad content around it",
			flux: `howdy()
			option     task    =     { name:"whatever",  cron:  "* * * * *"  }
			hello()
			`,
			expected: taskOptions{
				name:        "whatever",
				cron:        "* * * * *",
				concurrency: 1,
				retry:       1,
			},
		},
		{
			name:   "bad options",
			flux:   `option task = {name: "whatever", every: 1s, cron: "* * * * *"}`,
			errMsg: "cannot use both cron and every in task options",
		},
		{
			name:   "no options",
			flux:   `doesntexist()`,
			errMsg: "no task options defined",
		},
		{
			name: "multiple assignments",
			flux: `
			option task = {name: "whatever", every: 1s, offset: 0s, concurrency: 2, retry: 2}
			option task = {name: "whatever", every: 1s, offset: 0s, concurrency: 2, retry: 2}
			`,
			errMsg: "multiple task options defined",
		},
		{
			name: "with script calling tableFind",
			flux: `
			import "http"
			import "json"
			option task = {name: "Slack Metrics to #Community", cron: "0 9 * * 5"}
			all_slack_messages = from(bucket: "metrics")
				|> range(start: -7d, stop: now())
				|> filter(fn: (r) =>
					(r._measurement == "slack_channel_message"))
			total_messages = all_slack_messages
				|> group()
				|> count()
				|> tableFind(fn: (key) => true)
			all_slack_messages |> yield()
			`,
			expected: taskOptions{
				name:        "Slack Metrics to #Community",
				cron:        "0 9 * * 5",
				concurrency: 1,
				retry:       1,
			},
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {

			opts, err := options.FromScriptAST(fluxlang.DefaultService, tc.flux)
			if tc.errMsg != "" {
				require.Error(t, err)
				assert.Equal(t, tc.errMsg, err.Error())
				return
			}

			require.NoError(t, err)

			var offset options.Duration
			if opts.Offset != nil {
				offset = *opts.Offset
			}

			var concur int64
			if opts.Concurrency != nil {
				concur = *opts.Concurrency
			}

			var retry int64
			if opts.Retry != nil {
				retry = *opts.Retry
			}

			assert.Equal(t, tc.expected.name, opts.Name)
			assert.Equal(t, tc.expected.cron, opts.Cron)
			assert.Equal(t, tc.expected.every, opts.Every.String())
			assert.Equal(t, tc.expected.offset, offset.String())
			assert.Equal(t, tc.expected.concurrency, concur)
			assert.Equal(t, tc.expected.retry, retry)
		})
	}
}
