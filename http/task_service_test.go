package http

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/influxdata/httprouter"
	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/authorization"
	pcontext "github.com/influxdata/influxdb/v2/context"
	_ "github.com/influxdata/influxdb/v2/fluxinit/static"
	"github.com/influxdata/influxdb/v2/kit/platform"
	errors2 "github.com/influxdata/influxdb/v2/kit/platform/errors"
	kithttp "github.com/influxdata/influxdb/v2/kit/transport/http"
	"github.com/influxdata/influxdb/v2/label"
	"github.com/influxdata/influxdb/v2/mock"
	"github.com/influxdata/influxdb/v2/task/taskmodel"
	"github.com/influxdata/influxdb/v2/tenant"
	influxdbtesting "github.com/influxdata/influxdb/v2/testing"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest"
)

// NewMockTaskBackend returns a TaskBackend with mock services.
func NewMockTaskBackend(t *testing.T) *TaskBackend {
	t.Helper()
	store := influxdbtesting.NewTestInmemStore(t)
	tenantService := tenant.NewService(tenant.NewStore(store))

	return &TaskBackend{
		log: zaptest.NewLogger(t).With(zap.String("handler", "task")),

		AlgoWProxy:           &NoopProxyHandler{},
		AuthorizationService: mock.NewAuthorizationService(),
		TaskService:          &mock.TaskService{},
		OrganizationService: &mock.OrganizationService{
			FindOrganizationByIDF: func(ctx context.Context, id platform.ID) (*influxdb.Organization, error) {
				return &influxdb.Organization{ID: id, Name: "test"}, nil
			},
			FindOrganizationF: func(ctx context.Context, filter influxdb.OrganizationFilter) (*influxdb.Organization, error) {
				org := &influxdb.Organization{}
				if filter.Name != nil {
					if *filter.Name == "non-existent-org" {
						return nil, &errors2.Error{
							Err:  errors.New("org not found or unauthorized"),
							Msg:  "org " + *filter.Name + " not found or unauthorized",
							Code: errors2.ENotFound,
						}
					}
					org.Name = *filter.Name
				}
				if filter.ID != nil {
					org.ID = *filter.ID
				}

				return org, nil
			},
		},
		UserResourceMappingService: tenantService,
		LabelService:               mock.NewLabelService(),
		UserService:                mock.NewUserService(),
	}
}

func TestTaskHandler_handleGetTasks(t *testing.T) {
	type fields struct {
		taskService  taskmodel.TaskService
		labelService influxdb.LabelService
	}
	type wants struct {
		statusCode  int
		contentType string
		body        string
	}

	tests := []struct {
		name      string
		getParams string
		fields    fields
		wants     wants
	}{
		{
			name: "get tasks",
			fields: fields{
				taskService: &mock.TaskService{
					FindTasksFn: func(ctx context.Context, f taskmodel.TaskFilter) ([]*taskmodel.Task, int, error) {
						tasks := []*taskmodel.Task{
							{
								ID:             1,
								Name:           "task1",
								Description:    "A little Task",
								OrganizationID: 1,
								OwnerID:        1,
								Organization:   "test",
							},
							{
								ID:             2,
								Name:           "task2",
								OrganizationID: 2,
								OwnerID:        2,
								Organization:   "test",
							},
						}
						return tasks, len(tasks), nil
					},
				},
				labelService: &mock.LabelService{
					FindResourceLabelsFn: func(ctx context.Context, f influxdb.LabelMappingFilter) ([]*influxdb.Label, error) {
						labels := []*influxdb.Label{
							{
								ID:   influxdbtesting.MustIDBase16("fc3dc670a4be9b9a"),
								Name: "label",
								Properties: map[string]string{
									"color": "fff000",
								},
							},
						}
						return labels, nil
					},
				},
			},
			wants: wants{
				statusCode:  http.StatusOK,
				contentType: "application/json; charset=utf-8",
				body: `
{
  "links": {
    "self": "/api/v2/tasks?limit=100"
  },
  "tasks": [
    {
      "links": {
        "self": "/api/v2/tasks/0000000000000001",
        "owners": "/api/v2/tasks/0000000000000001/owners",
        "members": "/api/v2/tasks/0000000000000001/members",
        "labels": "/api/v2/tasks/0000000000000001/labels",
        "runs": "/api/v2/tasks/0000000000000001/runs",
        "logs": "/api/v2/tasks/0000000000000001/logs"
      },
      "id": "0000000000000001",
      "name": "task1",
	  "description": "A little Task",
	  "labels": [
        {
          "id": "fc3dc670a4be9b9a",
          "name": "label",
          "properties": {
            "color": "fff000"
          }
        }
      ],
      "orgID": "0000000000000001",
      "ownerID": "0000000000000001",
      "org": "test",
      "status": "",
      "flux": ""
    },
    {
      "links": {
        "self": "/api/v2/tasks/0000000000000002",
        "owners": "/api/v2/tasks/0000000000000002/owners",
        "members": "/api/v2/tasks/0000000000000002/members",
        "labels": "/api/v2/tasks/0000000000000002/labels",
        "runs": "/api/v2/tasks/0000000000000002/runs",
        "logs": "/api/v2/tasks/0000000000000002/logs"
      },
      "id": "0000000000000002",
      "name": "task2",
			"labels": [
        {
          "id": "fc3dc670a4be9b9a",
          "name": "label",
          "properties": {
            "color": "fff000"
          }
        }
      ],
	  "orgID": "0000000000000002",
	  "ownerID": "0000000000000002",
	  "org": "test",
      "status": "",
      "flux": ""
    }
  ]
}`,
			},
		},
		{
			name:      "get tasks by after and limit",
			getParams: "after=0000000000000001&limit=1",
			fields: fields{
				taskService: &mock.TaskService{
					FindTasksFn: func(ctx context.Context, f taskmodel.TaskFilter) ([]*taskmodel.Task, int, error) {
						tasks := []*taskmodel.Task{
							{
								ID:             2,
								Name:           "task2",
								OrganizationID: 2,
								OwnerID:        2,
								Organization:   "test",
							},
						}
						return tasks, len(tasks), nil
					},
				},
				labelService: &mock.LabelService{
					FindResourceLabelsFn: func(ctx context.Context, f influxdb.LabelMappingFilter) ([]*influxdb.Label, error) {
						labels := []*influxdb.Label{
							{
								ID:   influxdbtesting.MustIDBase16("fc3dc670a4be9b9a"),
								Name: "label",
								Properties: map[string]string{
									"color": "fff000",
								},
							},
						}
						return labels, nil
					},
				},
			},
			wants: wants{
				statusCode:  http.StatusOK,
				contentType: "application/json; charset=utf-8",
				body: `
{
  "links": {
    "self": "/api/v2/tasks?after=0000000000000001&limit=1",
    "next": "/api/v2/tasks?after=0000000000000002&limit=1"
  },
  "tasks": [
    {
      "links": {
        "self": "/api/v2/tasks/0000000000000002",
        "owners": "/api/v2/tasks/0000000000000002/owners",
        "members": "/api/v2/tasks/0000000000000002/members",
        "labels": "/api/v2/tasks/0000000000000002/labels",
        "runs": "/api/v2/tasks/0000000000000002/runs",
        "logs": "/api/v2/tasks/0000000000000002/logs"
      },
      "id": "0000000000000002",
      "name": "task2",
			"labels": [
        {
          "id": "fc3dc670a4be9b9a",
          "name": "label",
          "properties": {
            "color": "fff000"
          }
        }
      ],
	  "orgID": "0000000000000002",
	  "ownerID": "0000000000000002",
      "org": "test",
      "status": "",
      "flux": ""
    }
  ]
}`,
			},
		},
		{
			name:      "get tasks by org name",
			getParams: "org=test2",
			fields: fields{
				taskService: &mock.TaskService{
					FindTasksFn: func(ctx context.Context, f taskmodel.TaskFilter) ([]*taskmodel.Task, int, error) {
						tasks := []*taskmodel.Task{
							{
								ID:             2,
								Name:           "task2",
								OrganizationID: 2,
								OwnerID:        2,
								Organization:   "test2",
							},
						}
						return tasks, len(tasks), nil
					},
				},
				labelService: &mock.LabelService{
					FindResourceLabelsFn: func(ctx context.Context, f influxdb.LabelMappingFilter) ([]*influxdb.Label, error) {
						labels := []*influxdb.Label{
							{
								ID:   influxdbtesting.MustIDBase16("fc3dc670a4be9b9a"),
								Name: "label",
								Properties: map[string]string{
									"color": "fff000",
								},
							},
						}
						return labels, nil
					},
				},
			},
			wants: wants{
				statusCode:  http.StatusOK,
				contentType: "application/json; charset=utf-8",
				body: `
{
  "links": {
    "self": "/api/v2/tasks?limit=100&org=test2"
  },
  "tasks": [
    {
      "links": {
        "self": "/api/v2/tasks/0000000000000002",
        "owners": "/api/v2/tasks/0000000000000002/owners",
        "members": "/api/v2/tasks/0000000000000002/members",
        "labels": "/api/v2/tasks/0000000000000002/labels",
        "runs": "/api/v2/tasks/0000000000000002/runs",
        "logs": "/api/v2/tasks/0000000000000002/logs"
      },
      "id": "0000000000000002",
      "name": "task2",
			"labels": [
        {
          "id": "fc3dc670a4be9b9a",
          "name": "label",
          "properties": {
            "color": "fff000"
          }
        }
      ],
	  "orgID": "0000000000000002",
	  "ownerID": "0000000000000002",
	  "org": "test2",
      "status": "",
      "flux": ""
    }
  ]
}`,
			},
		},
		{
			name:      "get tasks by org name bad",
			getParams: "org=non-existent-org",
			fields: fields{
				taskService: &mock.TaskService{
					FindTasksFn: func(ctx context.Context, f taskmodel.TaskFilter) ([]*taskmodel.Task, int, error) {
						tasks := []*taskmodel.Task{
							{
								ID:             1,
								Name:           "task1",
								OrganizationID: 1,
								OwnerID:        1,
								Organization:   "test2",
							},
							{
								ID:             2,
								Name:           "task2",
								OrganizationID: 2,
								OwnerID:        2,
								Organization:   "test2",
							},
						}
						return tasks, len(tasks), nil
					},
				},
				labelService: &mock.LabelService{
					FindResourceLabelsFn: func(ctx context.Context, f influxdb.LabelMappingFilter) ([]*influxdb.Label, error) {
						labels := []*influxdb.Label{
							{
								ID:   influxdbtesting.MustIDBase16("fc3dc670a4be9b9a"),
								Name: "label",
								Properties: map[string]string{
									"color": "fff000",
								},
							},
						}
						return labels, nil
					},
				},
			},
			wants: wants{
				statusCode:  http.StatusBadRequest,
				contentType: "application/json; charset=utf-8",
				body: `{
"code": "invalid",
"message": "failed to decode request: org non-existent-org not found or unauthorized: org not found or unauthorized"
}`,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := httptest.NewRequest("GET", "http://any.url?"+tt.getParams, nil)
			w := httptest.NewRecorder()

			taskBackend := NewMockTaskBackend(t)
			taskBackend.HTTPErrorHandler = kithttp.NewErrorHandler(zaptest.NewLogger(t))
			taskBackend.TaskService = tt.fields.taskService
			taskBackend.LabelService = tt.fields.labelService
			h := NewTaskHandler(zaptest.NewLogger(t), taskBackend)
			h.handleGetTasks(w, r)

			res := w.Result()
			content := res.Header.Get("Content-Type")
			body, _ := io.ReadAll(res.Body)

			if res.StatusCode != tt.wants.statusCode {
				t.Errorf("%q. handleGetTasks() = %v, want %v", tt.name, res.StatusCode, tt.wants.statusCode)
			}
			if tt.wants.contentType != "" && content != tt.wants.contentType {
				t.Errorf("%q. handleGetTasks() = %v, want %v", tt.name, content, tt.wants.contentType)
			}
			if tt.wants.body != "" {
				if eq, diff, err := jsonEqual(tt.wants.body, string(body)); err != nil {
					t.Errorf("%q, handleGetTasks(). error unmarshalling json %v", tt.name, err)
				} else if !eq {
					t.Errorf("%q. handleGetTasks() = ***%s***", tt.name, diff)
				}
			}
		})
	}
}

func TestTaskHandler_handlePostTasks(t *testing.T) {
	type args struct {
		taskCreate taskmodel.TaskCreate
	}
	type fields struct {
		taskService taskmodel.TaskService
	}
	type wants struct {
		statusCode  int
		contentType string
		body        string
	}

	tests := []struct {
		name   string
		args   args
		fields fields
		wants  wants
	}{
		{
			name: "create task",
			args: args{
				taskCreate: taskmodel.TaskCreate{
					OrganizationID: 1,
					Flux:           "abc",
				},
			},
			fields: fields{
				taskService: &mock.TaskService{
					CreateTaskFn: func(ctx context.Context, tc taskmodel.TaskCreate) (*taskmodel.Task, error) {
						return &taskmodel.Task{
							ID:             1,
							Name:           "task1",
							Description:    "Brand New Task",
							OrganizationID: 1,
							OwnerID:        1,
							Organization:   "test",
							Flux:           "abc",
						}, nil
					},
				},
			},
			wants: wants{
				statusCode:  http.StatusCreated,
				contentType: "application/json; charset=utf-8",
				body: `
{
  "links": {
    "self": "/api/v2/tasks/0000000000000001",
    "owners": "/api/v2/tasks/0000000000000001/owners",
    "members": "/api/v2/tasks/0000000000000001/members",
    "labels": "/api/v2/tasks/0000000000000001/labels",
    "runs": "/api/v2/tasks/0000000000000001/runs",
    "logs": "/api/v2/tasks/0000000000000001/logs"
  },
  "id": "0000000000000001",
  "name": "task1",
  "description": "Brand New Task",
  "labels": [],
  "orgID": "0000000000000001",
  "ownerID": "0000000000000001",
  "org": "test",
  "status": "",
  "flux": "abc"
}
`,
			},
		},
		{
			name: "create task - influxdb error creating task",
			args: args{
				taskCreate: taskmodel.TaskCreate{
					OrganizationID: 1,
					Flux:           "abc",
				},
			},
			fields: fields{
				taskService: &mock.TaskService{
					CreateTaskFn: func(ctx context.Context, tc taskmodel.TaskCreate) (*taskmodel.Task, error) {
						return nil, errors2.NewError(
							errors2.WithErrorErr(errors.New("something went wrong")),
							errors2.WithErrorMsg("something really went wrong"),
							errors2.WithErrorCode(errors2.EInvalid),
						)
					},
				},
			},
			wants: wants{
				statusCode:  http.StatusBadRequest,
				contentType: "application/json; charset=utf-8",
				body: `
{
    "code": "invalid",
    "message": "something really went wrong: something went wrong"
}
`,
			},
		},
		{
			name: "create task - error creating task",
			args: args{
				taskCreate: taskmodel.TaskCreate{
					OrganizationID: 1,
					Flux:           "abc",
				},
			},
			fields: fields{
				taskService: &mock.TaskService{
					CreateTaskFn: func(ctx context.Context, tc taskmodel.TaskCreate) (*taskmodel.Task, error) {
						return nil, errors.New("something bad happened")
					},
				},
			},
			wants: wants{
				statusCode:  http.StatusInternalServerError,
				contentType: "application/json; charset=utf-8",
				body: `
{
    "code": "internal error",
    "message": "failed to create task: something bad happened"
}
`,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b, err := json.Marshal(tt.args.taskCreate)
			if err != nil {
				t.Fatalf("failed to unmarshal task: %v", err)
			}

			r := httptest.NewRequest("POST", "http://any.url", bytes.NewReader(b))
			ctx := pcontext.SetAuthorizer(context.TODO(), new(influxdb.Authorization))
			r = r.WithContext(ctx)

			w := httptest.NewRecorder()

			taskBackend := NewMockTaskBackend(t)
			taskBackend.HTTPErrorHandler = kithttp.NewErrorHandler(zaptest.NewLogger(t))
			taskBackend.TaskService = tt.fields.taskService
			h := NewTaskHandler(zaptest.NewLogger(t), taskBackend)
			h.handlePostTask(w, r)

			res := w.Result()
			content := res.Header.Get("Content-Type")
			body, _ := io.ReadAll(res.Body)

			if res.StatusCode != tt.wants.statusCode {
				t.Errorf("%q. handlePostTask() = %v, want %v", tt.name, res.StatusCode, tt.wants.statusCode)
			}
			if tt.wants.contentType != "" && content != tt.wants.contentType {
				t.Errorf("%q. handlePostTask() = %v, want %v", tt.name, content, tt.wants.contentType)
			}
			if tt.wants.body != "" {
				if eq, diff, err := jsonEqual(tt.wants.body, string(body)); err != nil {
					t.Errorf("%q, handlePostTask(). error unmarshalling json %v", tt.name, err)
				} else if !eq {
					t.Errorf("%q. handlePostTask() = ***%s***", tt.name, diff)
				}
			}
		})
	}
}

func TestTaskHandler_handleGetRun(t *testing.T) {
	type fields struct {
		taskService taskmodel.TaskService
	}
	type args struct {
		taskID platform.ID
		runID  platform.ID
	}
	type wants struct {
		statusCode  int
		contentType string
		body        string
	}

	tests := []struct {
		name   string
		fields fields
		args   args
		wants  wants
	}{
		{
			name: "get a run by id",
			fields: fields{
				taskService: &mock.TaskService{
					FindRunByIDFn: func(ctx context.Context, taskID platform.ID, runID platform.ID) (*taskmodel.Run, error) {
						scheduledFor, _ := time.Parse(time.RFC3339, "2018-12-01T17:00:13Z")
						startedAt, _ := time.Parse(time.RFC3339Nano, "2018-12-01T17:00:03.155645Z")
						finishedAt, _ := time.Parse(time.RFC3339Nano, "2018-12-01T17:00:13.155645Z")
						requestedAt, _ := time.Parse(time.RFC3339, "2018-12-01T17:00:13Z")
						run := taskmodel.Run{
							ID:           runID,
							TaskID:       taskID,
							Status:       "success",
							ScheduledFor: scheduledFor,
							StartedAt:    startedAt,
							FinishedAt:   finishedAt,
							RequestedAt:  requestedAt,
						}
						return &run, nil
					},
				},
			},
			args: args{
				taskID: 1,
				runID:  2,
			},
			wants: wants{
				statusCode:  http.StatusOK,
				contentType: "application/json; charset=utf-8",
				body: `
{
  "links": {
    "self": "/api/v2/tasks/0000000000000001/runs/0000000000000002",
    "task": "/api/v2/tasks/0000000000000001",
    "retry": "/api/v2/tasks/0000000000000001/runs/0000000000000002/retry",
    "logs": "/api/v2/tasks/0000000000000001/runs/0000000000000002/logs"
  },
  "id": "0000000000000002",
  "taskID": "0000000000000001",
  "status": "success",
  "scheduledFor": "2018-12-01T17:00:13Z",
  "startedAt": "2018-12-01T17:00:03.155645Z",
  "finishedAt": "2018-12-01T17:00:13.155645Z",
  "requestedAt": "2018-12-01T17:00:13Z"
}`,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := httptest.NewRequest("GET", "http://any.url", nil)
			r = r.WithContext(context.WithValue(
				context.Background(),
				httprouter.ParamsKey,
				httprouter.Params{
					{
						Key:   "id",
						Value: tt.args.taskID.String(),
					},
					{
						Key:   "rid",
						Value: tt.args.runID.String(),
					},
				}))
			r = r.WithContext(pcontext.SetAuthorizer(r.Context(), &influxdb.Authorization{Permissions: influxdb.OperPermissions()}))
			w := httptest.NewRecorder()
			taskBackend := NewMockTaskBackend(t)
			taskBackend.HTTPErrorHandler = kithttp.NewErrorHandler(zaptest.NewLogger(t))
			taskBackend.TaskService = tt.fields.taskService
			h := NewTaskHandler(zaptest.NewLogger(t), taskBackend)
			h.handleGetRun(w, r)

			res := w.Result()
			content := res.Header.Get("Content-Type")
			body, _ := io.ReadAll(res.Body)

			if res.StatusCode != tt.wants.statusCode {
				t.Errorf("%q. handleGetRun() = %v, want %v", tt.name, res.StatusCode, tt.wants.statusCode)
			}
			if tt.wants.contentType != "" && content != tt.wants.contentType {
				t.Errorf("%q. handleGetRun() = %v, want %v", tt.name, content, tt.wants.contentType)
			}
			if tt.wants.body != "" {
				if eq, diff, err := jsonEqual(string(body), tt.wants.body); err != nil {
					t.Errorf("%q, handleGetRun(). error unmarshalling json %v", tt.name, err)
				} else if !eq {
					t.Errorf("%q. handleGetRun() = ***%s***", tt.name, diff)
				}
			}
		})
	}
}

func TestTaskHandler_handleGetRuns(t *testing.T) {
	type fields struct {
		taskService taskmodel.TaskService
	}
	type args struct {
		taskID platform.ID
	}
	type wants struct {
		statusCode  int
		contentType string
		body        string
	}

	tests := []struct {
		name   string
		fields fields
		args   args
		wants  wants
	}{
		{
			name: "get runs by task id",
			fields: fields{
				taskService: &mock.TaskService{
					FindRunsFn: func(ctx context.Context, f taskmodel.RunFilter) ([]*taskmodel.Run, int, error) {
						scheduledFor, _ := time.Parse(time.RFC3339, "2018-12-01T17:00:13Z")
						startedAt, _ := time.Parse(time.RFC3339Nano, "2018-12-01T17:00:03.155645Z")
						finishedAt, _ := time.Parse(time.RFC3339Nano, "2018-12-01T17:00:13.155645Z")
						requestedAt, _ := time.Parse(time.RFC3339, "2018-12-01T17:00:13Z")
						runs := []*taskmodel.Run{
							{
								ID:           platform.ID(2),
								TaskID:       f.Task,
								Status:       "success",
								ScheduledFor: scheduledFor,
								StartedAt:    startedAt,
								FinishedAt:   finishedAt,
								RequestedAt:  requestedAt,
							},
						}
						return runs, len(runs), nil
					},
				},
			},
			args: args{
				taskID: 1,
			},
			wants: wants{
				statusCode:  http.StatusOK,
				contentType: "application/json; charset=utf-8",
				body: `
{
  "links": {
    "self": "/api/v2/tasks/0000000000000001/runs",
    "task": "/api/v2/tasks/0000000000000001"
  },
  "runs": [
    {
      "links": {
        "self": "/api/v2/tasks/0000000000000001/runs/0000000000000002",
        "task": "/api/v2/tasks/0000000000000001",
        "retry": "/api/v2/tasks/0000000000000001/runs/0000000000000002/retry",
        "logs": "/api/v2/tasks/0000000000000001/runs/0000000000000002/logs"
      },
      "id": "0000000000000002",
      "taskID": "0000000000000001",
      "status": "success",
      "scheduledFor": "2018-12-01T17:00:13Z",
      "startedAt": "2018-12-01T17:00:03.155645Z",
      "finishedAt": "2018-12-01T17:00:13.155645Z",
      "requestedAt": "2018-12-01T17:00:13Z"
    }
  ]
}`,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := httptest.NewRequest("GET", "http://any.url", nil)
			r = r.WithContext(context.WithValue(
				context.Background(),
				httprouter.ParamsKey,
				httprouter.Params{
					{
						Key:   "id",
						Value: tt.args.taskID.String(),
					},
				}))
			r = r.WithContext(pcontext.SetAuthorizer(r.Context(), &influxdb.Authorization{Permissions: influxdb.OperPermissions()}))
			w := httptest.NewRecorder()
			taskBackend := NewMockTaskBackend(t)
			taskBackend.HTTPErrorHandler = kithttp.NewErrorHandler(zaptest.NewLogger(t))
			taskBackend.TaskService = tt.fields.taskService
			h := NewTaskHandler(zaptest.NewLogger(t), taskBackend)
			h.handleGetRuns(w, r)

			res := w.Result()
			content := res.Header.Get("Content-Type")
			body, _ := io.ReadAll(res.Body)

			if res.StatusCode != tt.wants.statusCode {
				t.Errorf("%q. handleGetRuns() = %v, want %v", tt.name, res.StatusCode, tt.wants.statusCode)
			}
			if tt.wants.contentType != "" && content != tt.wants.contentType {
				t.Errorf("%q. handleGetRuns() = %v, want %v", tt.name, content, tt.wants.contentType)
			}
			if tt.wants.body != "" {
				if eq, diff, err := jsonEqual(string(body), tt.wants.body); err != nil {
					t.Errorf("%q, handleGetRuns(). error unmarshalling json %v", tt.name, err)
				} else if !eq {
					t.Errorf("%q. handleGetRuns() = ***%s***", tt.name, diff)
				}
			}
		})
	}
}

func TestTaskHandler_NotFoundStatus(t *testing.T) {
	// Ensure that the HTTP handlers return 404s for missing resources, and OKs for matching.

	store := influxdbtesting.NewTestInmemStore(t)
	tenantService := tenant.NewService(tenant.NewStore(store))

	labelStore, _ := label.NewStore(store)
	labelService := label.NewService(labelStore)

	taskBackend := NewMockTaskBackend(t)
	taskBackend.HTTPErrorHandler = kithttp.NewErrorHandler(zaptest.NewLogger(t))
	h := NewTaskHandler(zaptest.NewLogger(t), taskBackend)
	h.UserResourceMappingService = tenantService
	h.LabelService = labelService
	h.UserService = tenantService
	h.OrganizationService = tenantService

	o := influxdb.Organization{Name: "o"}
	ctx := context.Background()
	if err := h.OrganizationService.CreateOrganization(ctx, &o); err != nil {
		t.Fatal(err)
	}

	// Create a session to associate with the contexts, so authorization checks pass.
	authz := &influxdb.Authorization{Permissions: influxdb.OperPermissions()}

	const taskID, runID = platform.ID(0xCCCCCC), platform.ID(0xAAAAAA)

	var (
		okTask    = []interface{}{taskID}
		okTaskRun = []interface{}{taskID, runID}

		notFoundTask = [][]interface{}{
			{taskID + 1},
		}
		notFoundTaskRun = [][]interface{}{
			{taskID, runID + 1},
			{taskID + 1, runID},
			{taskID + 1, runID + 1},
		}
	)

	tcs := []struct {
		name             string
		svc              *mock.TaskService
		method           string
		body             string
		pathFmt          string
		okPathArgs       []interface{}
		notFoundPathArgs [][]interface{}
	}{
		{
			name: "get task",
			svc: &mock.TaskService{
				FindTaskByIDFn: func(_ context.Context, id platform.ID) (*taskmodel.Task, error) {
					if id == taskID {
						return &taskmodel.Task{ID: taskID, Organization: "o"}, nil
					}

					return nil, taskmodel.ErrTaskNotFound
				},
			},
			method:           http.MethodGet,
			pathFmt:          "/tasks/%s",
			okPathArgs:       okTask,
			notFoundPathArgs: notFoundTask,
		},
		{
			name: "update task",
			svc: &mock.TaskService{
				UpdateTaskFn: func(_ context.Context, id platform.ID, _ taskmodel.TaskUpdate) (*taskmodel.Task, error) {
					if id == taskID {
						return &taskmodel.Task{ID: taskID, Organization: "o"}, nil
					}

					return nil, taskmodel.ErrTaskNotFound
				},
			},
			method:           http.MethodPatch,
			body:             `{"status": "active"}`,
			pathFmt:          "/tasks/%s",
			okPathArgs:       okTask,
			notFoundPathArgs: notFoundTask,
		},
		{
			name: "delete task",
			svc: &mock.TaskService{
				DeleteTaskFn: func(_ context.Context, id platform.ID) error {
					if id == taskID {
						return nil
					}

					return taskmodel.ErrTaskNotFound
				},
			},
			method:           http.MethodDelete,
			pathFmt:          "/tasks/%s",
			okPathArgs:       okTask,
			notFoundPathArgs: notFoundTask,
		},
		{
			name: "get task logs",
			svc: &mock.TaskService{
				FindLogsFn: func(_ context.Context, f taskmodel.LogFilter) ([]*taskmodel.Log, int, error) {
					if f.Task == taskID {
						return nil, 0, nil
					}

					return nil, 0, taskmodel.ErrTaskNotFound
				},
			},
			method:           http.MethodGet,
			pathFmt:          "/tasks/%s/logs",
			okPathArgs:       okTask,
			notFoundPathArgs: notFoundTask,
		},
		{
			name: "get run logs",
			svc: &mock.TaskService{
				FindLogsFn: func(_ context.Context, f taskmodel.LogFilter) ([]*taskmodel.Log, int, error) {
					if f.Task != taskID {
						return nil, 0, taskmodel.ErrTaskNotFound
					}
					if *f.Run != runID {
						return nil, 0, taskmodel.ErrNoRunsFound
					}

					return nil, 0, nil
				},
			},
			method:           http.MethodGet,
			pathFmt:          "/tasks/%s/runs/%s/logs",
			okPathArgs:       okTaskRun,
			notFoundPathArgs: notFoundTaskRun,
		},
		{
			name: "get runs: task not found",
			svc: &mock.TaskService{
				FindRunsFn: func(_ context.Context, f taskmodel.RunFilter) ([]*taskmodel.Run, int, error) {
					if f.Task != taskID {
						return nil, 0, taskmodel.ErrTaskNotFound
					}

					return nil, 0, nil
				},
			},
			method:           http.MethodGet,
			pathFmt:          "/tasks/%s/runs",
			okPathArgs:       okTask,
			notFoundPathArgs: notFoundTask,
		},
		{
			name: "get runs: task found but no runs found",
			svc: &mock.TaskService{
				FindRunsFn: func(_ context.Context, f taskmodel.RunFilter) ([]*taskmodel.Run, int, error) {
					if f.Task != taskID {
						return nil, 0, taskmodel.ErrNoRunsFound
					}

					return nil, 0, nil
				},
			},
			method:           http.MethodGet,
			pathFmt:          "/tasks/%s/runs",
			okPathArgs:       okTask,
			notFoundPathArgs: notFoundTask,
		},
		{
			name: "force run",
			svc: &mock.TaskService{
				ForceRunFn: func(_ context.Context, tid platform.ID, _ int64) (*taskmodel.Run, error) {
					if tid != taskID {
						return nil, taskmodel.ErrTaskNotFound
					}

					return &taskmodel.Run{ID: runID, TaskID: taskID, Status: taskmodel.RunScheduled.String()}, nil
				},
			},
			method:           http.MethodPost,
			body:             "{}",
			pathFmt:          "/tasks/%s/runs",
			okPathArgs:       okTask,
			notFoundPathArgs: notFoundTask,
		},
		{
			name: "get run",
			svc: &mock.TaskService{
				FindRunByIDFn: func(_ context.Context, tid, rid platform.ID) (*taskmodel.Run, error) {
					if tid != taskID {
						return nil, taskmodel.ErrTaskNotFound
					}
					if rid != runID {
						return nil, taskmodel.ErrRunNotFound
					}

					return &taskmodel.Run{ID: runID, TaskID: taskID, Status: taskmodel.RunScheduled.String()}, nil
				},
			},
			method:           http.MethodGet,
			pathFmt:          "/tasks/%s/runs/%s",
			okPathArgs:       okTaskRun,
			notFoundPathArgs: notFoundTaskRun,
		},
		{
			name: "retry run",
			svc: &mock.TaskService{
				RetryRunFn: func(_ context.Context, tid, rid platform.ID) (*taskmodel.Run, error) {
					if tid != taskID {
						return nil, taskmodel.ErrTaskNotFound
					}
					if rid != runID {
						return nil, taskmodel.ErrRunNotFound
					}

					return &taskmodel.Run{ID: runID, TaskID: taskID, Status: taskmodel.RunScheduled.String()}, nil
				},
			},
			method:           http.MethodPost,
			pathFmt:          "/tasks/%s/runs/%s/retry",
			okPathArgs:       okTaskRun,
			notFoundPathArgs: notFoundTaskRun,
		},
		{
			name: "cancel run",
			svc: &mock.TaskService{
				CancelRunFn: func(_ context.Context, tid, rid platform.ID) error {
					if tid != taskID {
						return taskmodel.ErrTaskNotFound
					}
					if rid != runID {
						return taskmodel.ErrRunNotFound
					}

					return nil
				},
			},
			method:           http.MethodDelete,
			pathFmt:          "/tasks/%s/runs/%s",
			okPathArgs:       okTaskRun,
			notFoundPathArgs: notFoundTaskRun,
		},
	}

	for _, tc := range tcs {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			h.TaskService = tc.svc

			okPath := fmt.Sprintf(tc.pathFmt, tc.okPathArgs...)
			t.Run("matching ID: "+tc.method+" "+okPath, func(t *testing.T) {
				w := httptest.NewRecorder()
				r := httptest.NewRequest(tc.method, "http://task.example/api/v2"+okPath, strings.NewReader(tc.body)).WithContext(
					pcontext.SetAuthorizer(context.Background(), authz),
				)

				h.ServeHTTP(w, r)

				res := w.Result()
				defer res.Body.Close()

				if res.StatusCode < 200 || res.StatusCode > 299 {
					t.Errorf("expected OK, got %d", res.StatusCode)
					b, _ := io.ReadAll(res.Body)
					t.Fatalf("body: %s", string(b))
				}
			})

			t.Run("mismatched ID", func(t *testing.T) {
				for _, nfa := range tc.notFoundPathArgs {
					path := fmt.Sprintf(tc.pathFmt, nfa...)
					t.Run(tc.method+" "+path, func(t *testing.T) {
						w := httptest.NewRecorder()
						r := httptest.NewRequest(tc.method, "http://task.example/api/v2"+path, strings.NewReader(tc.body)).WithContext(
							pcontext.SetAuthorizer(context.Background(), authz),
						)

						h.ServeHTTP(w, r)

						res := w.Result()
						defer res.Body.Close()

						if res.StatusCode != http.StatusNotFound {
							t.Errorf("expected Not Found, got %d", res.StatusCode)
							b, _ := io.ReadAll(res.Body)
							t.Fatalf("body: %s", string(b))
						}
					})
				}
			})
		})
	}
}

func TestService_handlePostTaskLabel(t *testing.T) {
	type fields struct {
		LabelService influxdb.LabelService
	}
	type args struct {
		labelMapping *influxdb.LabelMapping
		taskID       platform.ID
	}
	type wants struct {
		statusCode  int
		contentType string
		body        string
	}

	tests := []struct {
		name   string
		fields fields
		args   args
		wants  wants
	}{
		{
			name: "add label to task",
			fields: fields{
				LabelService: &mock.LabelService{
					FindLabelByIDFn: func(ctx context.Context, id platform.ID) (*influxdb.Label, error) {
						return &influxdb.Label{
							ID:   1,
							Name: "label",
							Properties: map[string]string{
								"color": "fff000",
							},
						}, nil
					},
					CreateLabelMappingFn: func(ctx context.Context, m *influxdb.LabelMapping) error { return nil },
				},
			},
			args: args{
				labelMapping: &influxdb.LabelMapping{
					ResourceID: 100,
					LabelID:    1,
				},
				taskID: 100,
			},
			wants: wants{
				statusCode:  http.StatusCreated,
				contentType: "application/json; charset=utf-8",
				body: `
{
  "label": {
    "id": "0000000000000001",
    "name": "label",
    "properties": {
      "color": "fff000"
    }
  },
  "links": {
    "self": "/api/v2/labels/0000000000000001"
  }
}
`,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			taskBE := NewMockTaskBackend(t)
			taskBE.LabelService = tt.fields.LabelService
			h := NewTaskHandler(zaptest.NewLogger(t), taskBE)

			b, err := json.Marshal(tt.args.labelMapping)
			if err != nil {
				t.Fatalf("failed to unmarshal label mapping: %v", err)
			}

			url := fmt.Sprintf("http://localhost:8086/api/v2/tasks/%s/labels", tt.args.taskID)
			r := httptest.NewRequest("POST", url, bytes.NewReader(b))
			w := httptest.NewRecorder()

			h.ServeHTTP(w, r)

			res := w.Result()
			content := res.Header.Get("Content-Type")
			body, _ := io.ReadAll(res.Body)

			if res.StatusCode != tt.wants.statusCode {
				t.Errorf("got %v, want %v", res.StatusCode, tt.wants.statusCode)
			}
			if tt.wants.contentType != "" && content != tt.wants.contentType {
				t.Errorf("got %v, want %v", content, tt.wants.contentType)
			}
			if tt.wants.body != "" {
				if eq, diff, err := jsonEqual(string(body), tt.wants.body); err != nil {
					t.Errorf("%q, handlePostTaskLabel(). error unmarshalling json %v", tt.name, err)
				} else if !eq {
					t.Errorf("%q. handlePostTaskLabel() = ***%s***", tt.name, diff)
				}
			}
		})
	}
}

func runTestWithTokenHashingOptions(name string, testFunc func(bool, *testing.T), t *testing.T) {
	t.Helper()
	for _, useTokenHashing := range []bool{false, true} {
		t.Run(fmt.Sprintf("%s/TokenHashing=%t", name, useTokenHashing), func(t *testing.T) {
			testFunc(useTokenHashing, t)
		})
	}
}

// Test that org name to org ID translation happens properly in the HTTP layer.
// Regression test for https://github.com/influxdata/influxdb/issues/12089.
func TestTaskHandler_CreateTaskWithOrgName(t *testing.T) {
	runTestWithTokenHashingOptions("TestTaskHandler_CreateTaskWithOrgName", runTestTaskHandler_CreateTaskWithOrgName, t)
}

func runTestTaskHandler_CreateTaskWithOrgName(useTokenHashing bool, t *testing.T) {
	ctx := context.Background()

	i := influxdbtesting.NewTestInmemStore(t)

	ts := tenant.NewService(tenant.NewStore(i))
	aStore, err := authorization.NewStore(ctx, i, useTokenHashing)
	require.NoError(t, err)
	as := authorization.NewService(aStore, ts)

	// Set up user and org.
	u := &influxdb.User{Name: "u"}
	if err := ts.CreateUser(ctx, u); err != nil {
		t.Fatal(err)
	}
	o := &influxdb.Organization{Name: "o"}
	if err := ts.CreateOrganization(ctx, o); err != nil {
		t.Fatal(err)
	}

	// Source and destination buckets for use in task.
	bSrc := influxdb.Bucket{OrgID: o.ID, Name: "b-src"}
	if err := ts.CreateBucket(ctx, &bSrc); err != nil {
		t.Fatal(err)
	}
	bDst := influxdb.Bucket{OrgID: o.ID, Name: "b-dst"}
	if err := ts.CreateBucket(ctx, &bDst); err != nil {
		t.Fatal(err)
	}

	authz := influxdb.Authorization{OrgID: o.ID, UserID: u.ID, Permissions: influxdb.OperPermissions()}
	if err := as.CreateAuthorization(ctx, &authz); err != nil {
		t.Fatal(err)
	}

	taskSvc := &mock.TaskService{
		CreateTaskFn: func(_ context.Context, tc taskmodel.TaskCreate) (*taskmodel.Task, error) {
			if tc.OrganizationID != o.ID {
				t.Fatalf("expected task to be created with org ID %s, got %s", o.ID, tc.OrganizationID)
			}

			return &taskmodel.Task{ID: 9, OrganizationID: o.ID, OwnerID: o.ID, Name: "x", Flux: tc.Flux}, nil
		},
	}

	lStore, _ := label.NewStore(i)
	h := NewTaskHandler(zaptest.NewLogger(t), &TaskBackend{
		log: zaptest.NewLogger(t),

		TaskService:                taskSvc,
		AuthorizationService:       as,
		OrganizationService:        ts,
		UserResourceMappingService: ts,
		LabelService:               label.NewService(lStore),
		UserService:                ts,
		BucketService:              ts,
	})

	const script = `option task = {name:"x", every:1m} from(bucket:"b-src") |> range(start:-1m) |> to(bucket:"b-dst", org:"o")`

	url := "http://localhost:8086/api/v2/tasks"

	b, err := json.Marshal(taskmodel.TaskCreate{
		Flux:         script,
		Organization: o.Name,
	})
	if err != nil {
		t.Fatal(err)
	}

	r := httptest.NewRequest("POST", url, bytes.NewReader(b)).WithContext(
		pcontext.SetAuthorizer(ctx, &authz),
	)
	w := httptest.NewRecorder()

	h.handlePostTask(w, r)

	res := w.Result()
	defer res.Body.Close()
	body, err := io.ReadAll(res.Body)
	if err != nil {
		t.Fatal(err)
	}
	if res.StatusCode != http.StatusCreated {
		t.Logf("response body: %s", body)
		t.Fatalf("expected status created, got %v", res.StatusCode)
	}

	// The task should have been created with a valid token.
	var createdTask taskmodel.Task
	if err := json.Unmarshal([]byte(body), &createdTask); err != nil {
		t.Fatal(err)
	}
	if createdTask.Flux != script {
		t.Fatalf("Unexpected script returned:\n got: %s\nwant: %s", createdTask.Flux, script)
	}
}

func TestTaskHandler_Sessions(t *testing.T) {
	runTestWithTokenHashingOptions("TestTaskHandler_Sessions", runTestTaskHandler_Sessions, t)
}

func runTestTaskHandler_Sessions(useTokenHashing bool, t *testing.T) {
	t.Skip("rework these")

	ctx := context.Background()

	// Common setup to get a working base for using tasks.
	st := influxdbtesting.NewTestInmemStore(t)

	tStore := tenant.NewStore(st)
	tSvc := tenant.NewService(tStore)

	labelStore, err := label.NewStore(st)
	if err != nil {
		t.Fatal(err)
	}
	labelService := label.NewService(labelStore)

	authStore, err := authorization.NewStore(ctx, st, useTokenHashing)
	if err != nil {
		t.Fatal(err)
	}
	authService := authorization.NewService(authStore, tSvc)

	// Set up user and org.
	u := &influxdb.User{Name: "u"}
	if err := tSvc.CreateUser(ctx, u); err != nil {
		t.Fatal(err)
	}
	o := &influxdb.Organization{Name: "o"}
	if err := tSvc.CreateOrganization(ctx, o); err != nil {
		t.Fatal(err)
	}

	// Map user to org.
	if err := tSvc.CreateUserResourceMapping(ctx, &influxdb.UserResourceMapping{
		ResourceType: influxdb.OrgsResourceType,
		ResourceID:   o.ID,
		UserID:       u.ID,
		UserType:     influxdb.Owner,
	}); err != nil {
		t.Fatal(err)
	}

	// Source and destination buckets for use in task.
	bSrc := influxdb.Bucket{OrgID: o.ID, Name: "b-src"}
	if err := tSvc.CreateBucket(ctx, &bSrc); err != nil {
		t.Fatal(err)
	}
	bDst := influxdb.Bucket{OrgID: o.ID, Name: "b-dst"}
	if err := tSvc.CreateBucket(ctx, &bDst); err != nil {
		t.Fatal(err)
	}

	sessionAllPermsCtx := pcontext.SetAuthorizer(context.Background(), &influxdb.Session{
		UserID:      u.ID,
		Permissions: influxdb.OperPermissions(),
		ExpiresAt:   time.Now().Add(24 * time.Hour),
	})

	newHandler := func(t *testing.T, ts *mock.TaskService) *TaskHandler {
		return NewTaskHandler(zaptest.NewLogger(t), &TaskBackend{
			HTTPErrorHandler: kithttp.NewErrorHandler(zaptest.NewLogger(t)),
			log:              zaptest.NewLogger(t),

			TaskService:                ts,
			AuthorizationService:       authService,
			OrganizationService:        tSvc,
			UserResourceMappingService: tSvc,
			LabelService:               labelService,
			UserService:                tSvc,
			BucketService:              tSvc,
		})
	}

	t.Run("get runs for a task", func(t *testing.T) {
		// Unique authorization to associate with our fake task.
		taskAuth := &influxdb.Authorization{OrgID: o.ID, UserID: u.ID}
		if err := authService.CreateAuthorization(ctx, taskAuth); err != nil {
			t.Fatal(err)
		}

		const taskID = platform.ID(12345)
		const runID = platform.ID(9876)

		var findRunsCtx context.Context
		ts := &mock.TaskService{
			FindRunsFn: func(ctx context.Context, f taskmodel.RunFilter) ([]*taskmodel.Run, int, error) {
				findRunsCtx = ctx
				if f.Task != taskID {
					t.Fatalf("expected task ID %v, got %v", taskID, f.Task)
				}

				return []*taskmodel.Run{
					{ID: runID, TaskID: taskID},
				}, 1, nil
			},

			FindTaskByIDFn: func(ctx context.Context, id platform.ID) (*taskmodel.Task, error) {
				if id != taskID {
					return nil, taskmodel.ErrTaskNotFound
				}

				return &taskmodel.Task{
					ID:             taskID,
					OrganizationID: o.ID,
				}, nil
			},
		}

		h := newHandler(t, ts)
		url := fmt.Sprintf("http://localhost:8086/api/v2/tasks/%s/runs", taskID)
		valCtx := context.WithValue(sessionAllPermsCtx, httprouter.ParamsKey, httprouter.Params{{Key: "id", Value: taskID.String()}})
		r := httptest.NewRequest("GET", url, nil).WithContext(valCtx)
		w := httptest.NewRecorder()
		h.handleGetRuns(w, r)

		res := w.Result()
		body, err := io.ReadAll(res.Body)
		if err != nil {
			t.Fatal(err)
		}
		if res.StatusCode != http.StatusOK {
			t.Logf("response body: %s", body)
			t.Fatalf("expected status OK, got %v", res.StatusCode)
		}

		authr, err := pcontext.GetAuthorizer(findRunsCtx)
		if err != nil {
			t.Fatal(err)
		}
		if authr.Kind() != influxdb.AuthorizationKind {
			t.Fatalf("expected context's authorizer to be of kind %q, got %q", influxdb.AuthorizationKind, authr.Kind())
		}

		orgID := authr.(*influxdb.Authorization).OrgID

		if orgID != o.ID {
			t.Fatalf("expected context's authorizer org ID to be %v, got %v", o.ID, orgID)
		}

		// Other user without permissions on the task or authorization should be disallowed.
		otherUser := &influxdb.User{Name: "other-" + t.Name()}
		if err := tSvc.CreateUser(ctx, otherUser); err != nil {
			t.Fatal(err)
		}

		valCtx = pcontext.SetAuthorizer(valCtx, &influxdb.Session{
			UserID:    otherUser.ID,
			ExpiresAt: time.Now().Add(24 * time.Hour),
		})

		r = httptest.NewRequest("GET", url, nil).WithContext(valCtx)
		w = httptest.NewRecorder()
		h.handleGetRuns(w, r)

		res = w.Result()
		body, err = io.ReadAll(res.Body)
		if err != nil {
			t.Fatal(err)
		}
		if res.StatusCode != http.StatusUnauthorized {
			t.Logf("response body: %s", body)
			t.Fatalf("expected status unauthorized, got %v", res.StatusCode)
		}
	})

	t.Run("get single run for a task", func(t *testing.T) {
		// Unique authorization to associate with our fake task.
		taskAuth := &influxdb.Authorization{OrgID: o.ID, UserID: u.ID}
		if err := authService.CreateAuthorization(ctx, taskAuth); err != nil {
			t.Fatal(err)
		}

		const taskID = platform.ID(12345)
		const runID = platform.ID(9876)

		var findRunByIDCtx context.Context
		ts := &mock.TaskService{
			FindRunByIDFn: func(ctx context.Context, tid, rid platform.ID) (*taskmodel.Run, error) {
				findRunByIDCtx = ctx
				if tid != taskID {
					t.Fatalf("expected task ID %v, got %v", taskID, tid)
				}
				if rid != runID {
					t.Fatalf("expected run ID %v, got %v", runID, rid)
				}

				return &taskmodel.Run{ID: runID, TaskID: taskID}, nil
			},

			FindTaskByIDFn: func(ctx context.Context, id platform.ID) (*taskmodel.Task, error) {
				if id != taskID {
					return nil, taskmodel.ErrTaskNotFound
				}

				return &taskmodel.Task{
					ID:             taskID,
					OrganizationID: o.ID,
				}, nil
			},
		}

		h := newHandler(t, ts)
		url := fmt.Sprintf("http://localhost:8086/api/v2/tasks/%s/runs/%s", taskID, runID)
		valCtx := context.WithValue(sessionAllPermsCtx, httprouter.ParamsKey, httprouter.Params{
			{Key: "id", Value: taskID.String()},
			{Key: "rid", Value: runID.String()},
		})
		r := httptest.NewRequest("GET", url, nil).WithContext(valCtx)
		w := httptest.NewRecorder()
		h.handleGetRun(w, r)

		res := w.Result()
		body, err := io.ReadAll(res.Body)
		if err != nil {
			t.Fatal(err)
		}
		if res.StatusCode != http.StatusOK {
			t.Logf("response body: %s", body)
			t.Fatalf("expected status OK, got %v", res.StatusCode)
		}

		// The context passed to TaskService.FindRunByID must be a valid authorization (not a session).
		authr, err := pcontext.GetAuthorizer(findRunByIDCtx)
		if err != nil {
			t.Fatal(err)
		}
		if authr.Kind() != influxdb.AuthorizationKind {
			t.Fatalf("expected context's authorizer to be of kind %q, got %q", influxdb.AuthorizationKind, authr.Kind())
		}
		if authr.Identifier() != taskAuth.ID {
			t.Fatalf("expected context's authorizer ID to be %v, got %v", taskAuth.ID, authr.Identifier())
		}

		// Other user without permissions on the task or authorization should be disallowed.
		otherUser := &influxdb.User{Name: "other-" + t.Name()}
		if err := tSvc.CreateUser(ctx, otherUser); err != nil {
			t.Fatal(err)
		}

		valCtx = pcontext.SetAuthorizer(valCtx, &influxdb.Session{
			UserID:    otherUser.ID,
			ExpiresAt: time.Now().Add(24 * time.Hour),
		})

		r = httptest.NewRequest("GET", url, nil).WithContext(valCtx)
		w = httptest.NewRecorder()
		h.handleGetRuns(w, r)

		res = w.Result()
		body, err = io.ReadAll(res.Body)
		if err != nil {
			t.Fatal(err)
		}
		if res.StatusCode != http.StatusUnauthorized {
			t.Logf("response body: %s", body)
			t.Fatalf("expected status unauthorized, got %v", res.StatusCode)
		}
	})

	t.Run("get logs for a run", func(t *testing.T) {
		// Unique authorization to associate with our fake task.
		taskAuth := &influxdb.Authorization{OrgID: o.ID, UserID: u.ID}
		if err := authService.CreateAuthorization(ctx, taskAuth); err != nil {
			t.Fatal(err)
		}

		const taskID = platform.ID(12345)
		const runID = platform.ID(9876)

		var findLogsCtx context.Context
		ts := &mock.TaskService{
			FindLogsFn: func(ctx context.Context, f taskmodel.LogFilter) ([]*taskmodel.Log, int, error) {
				findLogsCtx = ctx
				if f.Task != taskID {
					t.Fatalf("expected task ID %v, got %v", taskID, f.Task)
				}
				if *f.Run != runID {
					t.Fatalf("expected run ID %v, got %v", runID, *f.Run)
				}

				line := taskmodel.Log{Time: "time", Message: "a log line"}
				return []*taskmodel.Log{&line}, 1, nil
			},

			FindTaskByIDFn: func(ctx context.Context, id platform.ID) (*taskmodel.Task, error) {
				if id != taskID {
					return nil, taskmodel.ErrTaskNotFound
				}

				return &taskmodel.Task{
					ID:             taskID,
					OrganizationID: o.ID,
				}, nil
			},
		}

		h := newHandler(t, ts)
		url := fmt.Sprintf("http://localhost:8086/api/v2/tasks/%s/runs/%s/logs", taskID, runID)
		valCtx := context.WithValue(sessionAllPermsCtx, httprouter.ParamsKey, httprouter.Params{
			{Key: "id", Value: taskID.String()},
			{Key: "rid", Value: runID.String()},
		})
		r := httptest.NewRequest("GET", url, nil).WithContext(valCtx)
		w := httptest.NewRecorder()
		h.handleGetLogs(w, r)

		res := w.Result()
		body, err := io.ReadAll(res.Body)
		if err != nil {
			t.Fatal(err)
		}
		if res.StatusCode != http.StatusOK {
			t.Logf("response body: %s", body)
			t.Fatalf("expected status OK, got %v", res.StatusCode)
		}

		// The context passed to TaskService.FindLogs must be a valid authorization (not a session).
		authr, err := pcontext.GetAuthorizer(findLogsCtx)
		if err != nil {
			t.Fatal(err)
		}
		if authr.Kind() != influxdb.AuthorizationKind {
			t.Fatalf("expected context's authorizer to be of kind %q, got %q", influxdb.AuthorizationKind, authr.Kind())
		}
		if authr.Identifier() != taskAuth.ID {
			t.Fatalf("expected context's authorizer ID to be %v, got %v", taskAuth.ID, authr.Identifier())
		}

		// Other user without permissions on the task or authorization should be disallowed.
		otherUser := &influxdb.User{Name: "other-" + t.Name()}
		if err := tSvc.CreateUser(ctx, otherUser); err != nil {
			t.Fatal(err)
		}

		valCtx = pcontext.SetAuthorizer(valCtx, &influxdb.Session{
			UserID:    otherUser.ID,
			ExpiresAt: time.Now().Add(24 * time.Hour),
		})

		r = httptest.NewRequest("GET", url, nil).WithContext(valCtx)
		w = httptest.NewRecorder()
		h.handleGetRuns(w, r)

		res = w.Result()
		body, err = io.ReadAll(res.Body)
		if err != nil {
			t.Fatal(err)
		}
		if res.StatusCode != http.StatusUnauthorized {
			t.Logf("response body: %s", body)
			t.Fatalf("expected status unauthorized, got %v", res.StatusCode)
		}
	})

	t.Run("retry a run", func(t *testing.T) {
		// Unique authorization to associate with our fake task.
		taskAuth := &influxdb.Authorization{OrgID: o.ID, UserID: u.ID}
		if err := authService.CreateAuthorization(ctx, taskAuth); err != nil {
			t.Fatal(err)
		}

		const taskID = platform.ID(12345)
		const runID = platform.ID(9876)

		var retryRunCtx context.Context
		ts := &mock.TaskService{
			RetryRunFn: func(ctx context.Context, tid, rid platform.ID) (*taskmodel.Run, error) {
				retryRunCtx = ctx
				if tid != taskID {
					t.Fatalf("expected task ID %v, got %v", taskID, tid)
				}
				if rid != runID {
					t.Fatalf("expected run ID %v, got %v", runID, rid)
				}

				return &taskmodel.Run{ID: 10 * runID, TaskID: taskID}, nil
			},

			FindTaskByIDFn: func(ctx context.Context, id platform.ID) (*taskmodel.Task, error) {
				if id != taskID {
					return nil, taskmodel.ErrTaskNotFound
				}

				return &taskmodel.Task{
					ID:             taskID,
					OrganizationID: o.ID,
				}, nil
			},
		}

		h := newHandler(t, ts)
		url := fmt.Sprintf("http://localhost:8086/api/v2/tasks/%s/runs/%s/retry", taskID, runID)
		valCtx := context.WithValue(sessionAllPermsCtx, httprouter.ParamsKey, httprouter.Params{
			{Key: "id", Value: taskID.String()},
			{Key: "rid", Value: runID.String()},
		})
		r := httptest.NewRequest("POST", url, nil).WithContext(valCtx)
		w := httptest.NewRecorder()
		h.handleRetryRun(w, r)

		res := w.Result()
		body, err := io.ReadAll(res.Body)
		if err != nil {
			t.Fatal(err)
		}
		if res.StatusCode != http.StatusOK {
			t.Logf("response body: %s", body)
			t.Fatalf("expected status OK, got %v", res.StatusCode)
		}

		// The context passed to TaskService.RetryRun must be a valid authorization (not a session).
		authr, err := pcontext.GetAuthorizer(retryRunCtx)
		if err != nil {
			t.Fatal(err)
		}
		if authr.Kind() != influxdb.AuthorizationKind {
			t.Fatalf("expected context's authorizer to be of kind %q, got %q", influxdb.AuthorizationKind, authr.Kind())
		}
		if authr.Identifier() != taskAuth.ID {
			t.Fatalf("expected context's authorizer ID to be %v, got %v", taskAuth.ID, authr.Identifier())
		}

		// Other user without permissions on the task or authorization should be disallowed.
		otherUser := &influxdb.User{Name: "other-" + t.Name()}
		if err := tSvc.CreateUser(ctx, otherUser); err != nil {
			t.Fatal(err)
		}

		valCtx = pcontext.SetAuthorizer(valCtx, &influxdb.Session{
			UserID:    otherUser.ID,
			ExpiresAt: time.Now().Add(24 * time.Hour),
		})

		r = httptest.NewRequest("POST", url, nil).WithContext(valCtx)
		w = httptest.NewRecorder()
		h.handleGetRuns(w, r)

		res = w.Result()
		body, err = io.ReadAll(res.Body)
		if err != nil {
			t.Fatal(err)
		}
		if res.StatusCode != http.StatusUnauthorized {
			t.Logf("response body: %s", body)
			t.Fatalf("expected status unauthorized, got %v", res.StatusCode)
		}
	})
}
