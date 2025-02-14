package authorizer_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/authorization"
	"github.com/influxdata/influxdb/v2/authorizer"
	pctx "github.com/influxdata/influxdb/v2/context"
	_ "github.com/influxdata/influxdb/v2/fluxinit/static"
	"github.com/influxdata/influxdb/v2/http"
	"github.com/influxdata/influxdb/v2/inmem"
	"github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/influxdata/influxdb/v2/kv"
	"github.com/influxdata/influxdb/v2/kv/migration/all"
	"github.com/influxdata/influxdb/v2/mock"
	"github.com/influxdata/influxdb/v2/task/taskmodel"
	"github.com/influxdata/influxdb/v2/tenant"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
)

func TestOnboardingValidation(t *testing.T) {
	for _, useHashedTokens := range []bool{false, true} {
		t.Run(fmt.Sprintf("TestOnboardingValidation/HashedTokens=%t", useHashedTokens), func(t *testing.T) {
			_, onboard := setup(t, useHashedTokens)

			ts := authorizer.NewTaskService(zaptest.NewLogger(t), mockTaskService(3, 2, 1))

			r, err := onboard.OnboardInitialUser(context.Background(), &influxdb.OnboardingRequest{
				User:                   "Setec Astronomy",
				Password:               "too many secrets",
				Org:                    "thing",
				Bucket:                 "holder",
				RetentionPeriodSeconds: 1,
			})
			require.NoError(t, err)

			ctx := pctx.SetAuthorizer(context.Background(), r.Auth)

			_, err = ts.CreateTask(ctx, taskmodel.TaskCreate{
				OrganizationID: r.Org.ID,
				OwnerID:        r.Auth.GetUserID(),
				Flux: `option task = {
 name: "my_task",
 every: 1s,
}
from(bucket:"holder") |> range(start:-5m) |> to(bucket:"holder", org:"thing")`,
			})
			require.NoError(t, err)
		})
	}
}

func mockTaskService(orgID, taskID, runID platform.ID) taskmodel.TaskService {
	task := taskmodel.Task{
		ID:             taskID,
		OrganizationID: orgID,
		Name:           "cows",
		Status:         string(taskmodel.TaskActive),
		Flux: `option task = {
 name: "my_task",
 every: 1s,
}
from(bucket:"holder") |> range(start:-5m) |> to(bucket:"holder", org:"thing")`,
		Every: "1s",
	}

	log := taskmodel.Log{Message: "howdy partner"}

	run := taskmodel.Run{
		ID:           runID,
		TaskID:       taskID,
		Status:       "completed",
		ScheduledFor: time.Now().UTC(),
		StartedAt:    time.Now().UTC().Add(time.Second * 3),
		FinishedAt:   time.Now().UTC().Add(time.Second * 10),
		Log:          []taskmodel.Log{log},
	}

	return &mock.TaskService{
		FindTaskByIDFn: func(context.Context, platform.ID) (*taskmodel.Task, error) {
			return &task, nil
		},
		FindTasksFn: func(context.Context, taskmodel.TaskFilter) ([]*taskmodel.Task, int, error) {
			return []*taskmodel.Task{&task}, 1, nil
		},
		CreateTaskFn: func(_ context.Context, tc taskmodel.TaskCreate) (*taskmodel.Task, error) {
			taskCopy := task
			return &taskCopy, nil
		},
		UpdateTaskFn: func(context.Context, platform.ID, taskmodel.TaskUpdate) (*taskmodel.Task, error) {
			return &task, nil
		},
		DeleteTaskFn: func(context.Context, platform.ID) error {
			return nil
		},
		FindLogsFn: func(context.Context, taskmodel.LogFilter) ([]*taskmodel.Log, int, error) {
			return []*taskmodel.Log{&log}, 1, nil
		},
		FindRunsFn: func(context.Context, taskmodel.RunFilter) ([]*taskmodel.Run, int, error) {
			return []*taskmodel.Run{&run}, 1, nil
		},
		FindRunByIDFn: func(context.Context, platform.ID, platform.ID) (*taskmodel.Run, error) {
			return &run, nil
		},
		CancelRunFn: func(context.Context, platform.ID, platform.ID) error {
			return nil
		},
		RetryRunFn: func(context.Context, platform.ID, platform.ID) (*taskmodel.Run, error) {
			return &run, nil
		},
		ForceRunFn: func(context.Context, platform.ID, int64) (*taskmodel.Run, error) {
			return &run, nil
		},
	}
}

func TestValidations(t *testing.T) {
	for _, useHashedTokens := range []bool{false, true} {
		runTestValidations(useHashedTokens, t)
	}
}

func runTestValidations(useHashedTokens bool, t *testing.T) {
	var (
		taskID   = platform.ID(0x7456)
		runID    = platform.ID(0x402)
		otherOrg = &influxdb.Organization{Name: "other_org"}
	)

	svc, onboard := setup(t, useHashedTokens)

	r, err := onboard.OnboardInitialUser(context.Background(), &influxdb.OnboardingRequest{
		User:                   "Setec Astronomy",
		Password:               "too many secrets",
		Org:                    "thing",
		Bucket:                 "holder",
		RetentionPeriodSeconds: 1,
	})
	require.NoError(t, err)

	err = svc.CreateOrganization(context.Background(), otherOrg)
	require.NoError(t, err)

	otherBucket := &influxdb.Bucket{
		Name:  "other_bucket",
		OrgID: otherOrg.ID,
	}

	err = svc.CreateBucket(context.Background(), otherBucket)
	require.NoError(t, err)

	var (
		orgID            = r.Org.ID
		validTaskService = authorizer.NewTaskService(zaptest.NewLogger(t), mockTaskService(orgID, taskID, runID))

		// Read all tasks in org.
		orgReadAllTaskPermissions = []influxdb.Permission{
			{Action: influxdb.ReadAction, Resource: influxdb.Resource{Type: influxdb.TasksResourceType, OrgID: &orgID}},
		}

		// Read all tasks in some other org.
		wrongOrgReadAllTaskPermissions = []influxdb.Permission{
			{Action: influxdb.ReadAction, Resource: influxdb.Resource{Type: influxdb.TasksResourceType, OrgID: &taskID}},
		}

		// Write all tasks in org, no specific bucket permissions.
		orgWriteAllTaskPermissions = []influxdb.Permission{
			{Action: influxdb.WriteAction, Resource: influxdb.Resource{Type: influxdb.TasksResourceType, OrgID: &orgID}},
		}

		// Write all tasks in org, and read/write the onboarding bucket.
		orgWriteAllTaskBucketPermissions = []influxdb.Permission{
			{Action: influxdb.WriteAction, Resource: influxdb.Resource{Type: influxdb.TasksResourceType, OrgID: &orgID}},
			{Action: influxdb.WriteAction, Resource: influxdb.Resource{Type: influxdb.BucketsResourceType, OrgID: &orgID, ID: &r.Bucket.ID}},
			{Action: influxdb.ReadAction, Resource: influxdb.Resource{Type: influxdb.BucketsResourceType, OrgID: &orgID, ID: &r.Bucket.ID}},
		}

		// Write the specific task, and read/write the onboarding bucket.
		orgWriteTaskBucketPermissions = []influxdb.Permission{
			{Action: influxdb.WriteAction, Resource: influxdb.Resource{Type: influxdb.TasksResourceType, OrgID: &orgID, ID: &taskID}},
			{Action: influxdb.WriteAction, Resource: influxdb.Resource{Type: influxdb.BucketsResourceType, OrgID: &orgID, ID: &r.Bucket.ID}},
			{Action: influxdb.ReadAction, Resource: influxdb.Resource{Type: influxdb.BucketsResourceType, OrgID: &orgID, ID: &r.Bucket.ID}},
		}

		// Permission only to specifically write the target task.
		orgWriteTaskPermissions = []influxdb.Permission{
			{Action: influxdb.WriteAction, Resource: influxdb.Resource{Type: influxdb.TasksResourceType, OrgID: &orgID, ID: &taskID}},
		}

		// Permission only to specifically read the target task.
		orgReadTaskPermissions = []influxdb.Permission{
			{Action: influxdb.ReadAction, Resource: influxdb.Resource{Type: influxdb.TasksResourceType, OrgID: &orgID, ID: &taskID}},
		}
	)

	tests := []struct {
		name  string
		check func(context.Context, taskmodel.TaskService) error
		auth  *influxdb.Authorization
	}{
		{
			name: "create failure",
			check: func(ctx context.Context, svc taskmodel.TaskService) error {
				_, err := svc.CreateTask(ctx, taskmodel.TaskCreate{
					OrganizationID: r.Org.ID,
					Flux: `option task = {
 name: "my_task",
 every: 1s,
}
from(bucket:"holder") |> range(start:-5m) |> to(bucket:"holder", org:"thing")`,
				})
				if err == nil {
					return errors.New("failed to error without permission")
				}
				return nil
			},
			auth: &influxdb.Authorization{},
		},
		{
			name: "create success",
			auth: r.Auth,
			check: func(ctx context.Context, svc taskmodel.TaskService) error {
				_, err := svc.CreateTask(ctx, taskmodel.TaskCreate{
					OrganizationID: r.Org.ID,
					OwnerID:        r.Auth.GetUserID(),
					Flux: `option task = {
 name: "my_task",
 every: 1s,
}
from(bucket:"holder") |> range(start:-5m) |> to(bucket:"holder", org:"thing")`,
				})
				return err
			},
		},
		{
			name: "FindTaskByID missing auth",
			auth: &influxdb.Authorization{Permissions: []influxdb.Permission{}},
			check: func(ctx context.Context, svc taskmodel.TaskService) error {
				_, err := svc.FindTaskByID(ctx, taskID)
				if err == nil {
					return errors.New("returned without error without permission")
				}
				return nil
			},
		},
		{
			name: "FindTaskByID with org auth",
			auth: &influxdb.Authorization{Status: "active", Permissions: orgReadAllTaskPermissions},
			check: func(ctx context.Context, svc taskmodel.TaskService) error {
				_, err := svc.FindTaskByID(ctx, taskID)
				return err
			},
		},
		{
			name: "FindTaskByID with task auth",
			auth: &influxdb.Authorization{Status: "active", Permissions: orgReadTaskPermissions},
			check: func(ctx context.Context, svc taskmodel.TaskService) error {
				_, err := svc.FindTaskByID(ctx, taskID)
				return err
			},
		},
		{
			name: "FindTasks with bad auth",
			auth: &influxdb.Authorization{Status: "active", Permissions: wrongOrgReadAllTaskPermissions},
			check: func(ctx context.Context, svc taskmodel.TaskService) error {
				ts, _, err := svc.FindTasks(ctx, taskmodel.TaskFilter{
					OrganizationID: &orgID,
				})
				if err == nil && len(ts) > 0 {
					return errors.New("returned no error with a invalid auth")
				}
				return nil
			},
		},
		{
			name: "FindTasks with org auth",
			auth: &influxdb.Authorization{Status: "active", Permissions: orgReadAllTaskPermissions},
			check: func(ctx context.Context, svc taskmodel.TaskService) error {
				_, _, err := svc.FindTasks(ctx, taskmodel.TaskFilter{
					OrganizationID: &orgID,
				})
				return err
			},
		},
		{
			name: "FindTasks with task auth",
			auth: &influxdb.Authorization{Status: "active", Permissions: orgReadTaskPermissions},
			check: func(ctx context.Context, svc taskmodel.TaskService) error {
				_, _, err := svc.FindTasks(ctx, taskmodel.TaskFilter{
					OrganizationID: &orgID,
				})
				return err
			},
		},
		{
			name: "FindTasks without org filter",
			auth: &influxdb.Authorization{Status: "active", Permissions: orgReadAllTaskPermissions},
			check: func(ctx context.Context, svc taskmodel.TaskService) error {
				_, _, err := svc.FindTasks(ctx, taskmodel.TaskFilter{})
				return err
			},
		},
		{
			name: "UpdateTask with readonly auth",
			auth: &influxdb.Authorization{Status: "active", Permissions: orgReadAllTaskPermissions},
			check: func(ctx context.Context, svc taskmodel.TaskService) error {
				flux := `option task = {
 name: "my_task",
 every: 1s,
}
from(bucket:"holder") |> range(start:-5m) |> to(bucket:"holder", org:"thing")`
				_, err := svc.UpdateTask(ctx, taskID, taskmodel.TaskUpdate{
					Flux: &flux,
				})
				if err == nil {
					return errors.New("returned no error with a invalid auth")
				}
				return nil
			},
		},
		{
			name: "UpdateTask with org auth",
			auth: &influxdb.Authorization{Status: "active", Permissions: orgWriteAllTaskBucketPermissions},
			check: func(ctx context.Context, svc taskmodel.TaskService) error {
				flux := `option task = {
		 name: "my_task",
		 every: 1s,
		}
		from(bucket:"holder") |> range(start:-5m) |> to(bucket:"holder", org:"thing")`
				_, err := svc.UpdateTask(ctx, taskID, taskmodel.TaskUpdate{
					Flux: &flux,
				})
				return err
			},
		},
		{
			name: "UpdateTask with task auth",
			auth: &influxdb.Authorization{Status: "active", Permissions: orgWriteTaskBucketPermissions},
			check: func(ctx context.Context, svc taskmodel.TaskService) error {
				flux := `option task = {
 name: "my_task",
 every: 1s,
}
from(bucket:"holder") |> range(start:-5m) |> to(bucket:"holder", org:"thing")`
				_, err := svc.UpdateTask(ctx, taskID, taskmodel.TaskUpdate{
					Flux: &flux,
				})
				return err
			},
		},
		{
			name: "DeleteTask missing auth",
			auth: &influxdb.Authorization{Permissions: []influxdb.Permission{}},
			check: func(ctx context.Context, svc taskmodel.TaskService) error {
				err := svc.DeleteTask(ctx, taskID)
				if err == nil {
					return errors.New("returned without error without permission")
				}
				return nil
			},
		},
		{
			name: "DeleteTask readonly auth",
			auth: &influxdb.Authorization{Permissions: orgReadAllTaskPermissions},
			check: func(ctx context.Context, svc taskmodel.TaskService) error {
				err := svc.DeleteTask(ctx, taskID)
				if err == nil {
					return errors.New("returned without error without permission")
				}
				return nil
			},
		},
		{
			name: "DeleteTask with org auth",
			auth: &influxdb.Authorization{Status: "active", Permissions: orgWriteAllTaskPermissions},
			check: func(ctx context.Context, svc taskmodel.TaskService) error {
				err := svc.DeleteTask(ctx, taskID)
				return err
			},
		},
		{
			name: "DeleteTask with task auth",
			auth: &influxdb.Authorization{Status: "active", Permissions: orgWriteTaskPermissions},
			check: func(ctx context.Context, svc taskmodel.TaskService) error {
				err := svc.DeleteTask(ctx, taskID)
				return err
			},
		},
		{
			name: "FindLogs with bad auth",
			auth: &influxdb.Authorization{Status: "active", Permissions: wrongOrgReadAllTaskPermissions},
			check: func(ctx context.Context, svc taskmodel.TaskService) error {
				_, _, err := svc.FindLogs(ctx, taskmodel.LogFilter{
					Task: taskID,
				})
				if err == nil {
					return errors.New("returned no error with a invalid auth")
				}
				return nil
			},
		},
		{
			name: "FindLogs with org auth",
			auth: &influxdb.Authorization{Status: "active", Permissions: orgReadAllTaskPermissions},
			check: func(ctx context.Context, svc taskmodel.TaskService) error {
				_, _, err := svc.FindLogs(ctx, taskmodel.LogFilter{
					Task: taskID,
				})
				return err
			},
		},
		{
			name: "FindLogs with task auth",
			auth: &influxdb.Authorization{Status: "active", Permissions: orgReadTaskPermissions},
			check: func(ctx context.Context, svc taskmodel.TaskService) error {
				_, _, err := svc.FindLogs(ctx, taskmodel.LogFilter{
					Task: taskID,
				})
				return err
			},
		},
		{
			name: "FindRuns with bad auth",
			auth: &influxdb.Authorization{Status: "active", Permissions: wrongOrgReadAllTaskPermissions},
			check: func(ctx context.Context, svc taskmodel.TaskService) error {
				_, _, err := svc.FindRuns(ctx, taskmodel.RunFilter{
					Task: taskID,
				})
				if err == nil {
					return errors.New("returned no error with a invalid auth")
				}
				return nil
			},
		},
		{
			name: "FindRuns with org auth",
			auth: &influxdb.Authorization{Status: "active", Permissions: orgReadAllTaskPermissions},
			check: func(ctx context.Context, svc taskmodel.TaskService) error {
				_, _, err := svc.FindRuns(ctx, taskmodel.RunFilter{
					Task: taskID,
				})
				return err
			},
		},
		{
			name: "FindRuns with task auth",
			auth: &influxdb.Authorization{Status: "active", Permissions: orgReadTaskPermissions},
			check: func(ctx context.Context, svc taskmodel.TaskService) error {
				_, _, err := svc.FindRuns(ctx, taskmodel.RunFilter{
					Task: taskID,
				})
				return err
			},
		},
		{
			name: "FindRunByID missing auth",
			auth: &influxdb.Authorization{Permissions: []influxdb.Permission{}},
			check: func(ctx context.Context, svc taskmodel.TaskService) error {
				_, err := svc.FindRunByID(ctx, taskID, 10)
				if err == nil {
					return errors.New("returned without error without permission")
				}
				return nil
			},
		},
		{
			name: "FindRunByID with org auth",
			auth: &influxdb.Authorization{Status: "active", Permissions: orgReadAllTaskPermissions},
			check: func(ctx context.Context, svc taskmodel.TaskService) error {
				_, err := svc.FindRunByID(ctx, taskID, 10)
				return err
			},
		},
		{
			name: "FindRunByID with task auth",
			auth: &influxdb.Authorization{Status: "active", Permissions: orgReadTaskPermissions},
			check: func(ctx context.Context, svc taskmodel.TaskService) error {
				_, err := svc.FindRunByID(ctx, taskID, 10)
				return err
			},
		},
		{
			name: "CancelRun with bad auth",
			auth: &influxdb.Authorization{Status: "active", Permissions: wrongOrgReadAllTaskPermissions},
			check: func(ctx context.Context, svc taskmodel.TaskService) error {
				err := svc.CancelRun(ctx, taskID, 10)
				if err == nil {
					return errors.New("returned no error with a invalid auth")
				}
				return nil
			},
		},
		{
			name: "CancelRun with org auth",
			auth: &influxdb.Authorization{Status: "active", Permissions: orgWriteAllTaskPermissions},
			check: func(ctx context.Context, svc taskmodel.TaskService) error {
				err := svc.CancelRun(ctx, taskID, 10)
				return err
			},
		},
		{
			name: "CancelRun with task auth",
			auth: &influxdb.Authorization{Status: "active", Permissions: orgWriteTaskPermissions},
			check: func(ctx context.Context, svc taskmodel.TaskService) error {
				err := svc.CancelRun(ctx, taskID, 10)
				return err
			},
		},
		{
			name: "RetryRun with bad auth",
			auth: &influxdb.Authorization{Status: "active", Permissions: wrongOrgReadAllTaskPermissions},
			check: func(ctx context.Context, svc taskmodel.TaskService) error {
				_, err := svc.RetryRun(ctx, taskID, 10)
				if err == nil {
					return errors.New("returned no error with a invalid auth")
				}
				return nil
			},
		},
		{
			name: "RetryRun with org auth",
			auth: &influxdb.Authorization{Status: "active", Permissions: orgWriteAllTaskPermissions},
			check: func(ctx context.Context, svc taskmodel.TaskService) error {
				_, err := svc.RetryRun(ctx, taskID, 10)
				return err
			},
		},
		{
			name: "RetryRun with task auth",
			auth: &influxdb.Authorization{Status: "active", Permissions: orgWriteTaskPermissions},
			check: func(ctx context.Context, svc taskmodel.TaskService) error {
				_, err := svc.RetryRun(ctx, taskID, 10)
				return err
			},
		},
		{
			name: "ForceRun with bad auth",
			auth: &influxdb.Authorization{Status: "active", Permissions: wrongOrgReadAllTaskPermissions},
			check: func(ctx context.Context, svc taskmodel.TaskService) error {
				_, err := svc.ForceRun(ctx, taskID, 10000)
				if err == nil {
					return errors.New("returned no error with a invalid auth")
				}
				return nil
			},
		},
		{
			name: "ForceRun with org auth",
			auth: &influxdb.Authorization{Status: "active", Permissions: orgWriteAllTaskPermissions},
			check: func(ctx context.Context, svc taskmodel.TaskService) error {
				_, err := svc.ForceRun(ctx, taskID, 10000)
				return err
			},
		},
		{
			name: "ForceRun with task auth",
			auth: &influxdb.Authorization{Status: "active", Permissions: orgWriteTaskPermissions},
			check: func(ctx context.Context, svc taskmodel.TaskService) error {
				_, err := svc.ForceRun(ctx, taskID, 10000)
				return err
			},
		},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("%s/HashedTokens=%t", test.name, useHashedTokens), func(t *testing.T) {
			ctx := pctx.SetAuthorizer(context.Background(), test.auth)
			if err := test.check(ctx, validTaskService); err != nil {
				if aerr, ok := err.(http.AuthzError); ok {
					t.Error(aerr.AuthzError())
				}
				t.Error(err)
			}
		})
	}
}

func setup(t *testing.T, useHashedTokens bool) (*tenant.Service, influxdb.OnboardingService) {
	t.Helper()

	store := newStore(t)

	svc := tenant.NewService(tenant.NewStore(store))

	authStore, err := authorization.NewStore(context.Background(), store, useHashedTokens)
	require.NoError(t, err)

	authSvc := authorization.NewService(authStore, svc)

	onboard := tenant.NewOnboardService(svc, authSvc)

	return svc, onboard
}

func newStore(t *testing.T) kv.Store {
	t.Helper()

	store := inmem.NewKVStore()

	err := all.Up(context.Background(), zaptest.NewLogger(t), store)
	require.NoError(t, err)

	return store
}
