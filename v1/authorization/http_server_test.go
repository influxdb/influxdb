package authorization

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sort"
	"testing"

	"github.com/go-chi/chi"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/influxdata/httprouter"
	"github.com/influxdata/influxdb/v2"
	icontext "github.com/influxdata/influxdb/v2/context"
	"github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/influxdata/influxdb/v2/kit/platform/errors"
	"github.com/influxdata/influxdb/v2/mock"
	itesting "github.com/influxdata/influxdb/v2/testing"
	"go.uber.org/zap/zaptest"
)

func TestService_handlePostAuthorization(t *testing.T) {
	type fields struct {
		AuthorizationService influxdb.AuthorizationService
		TenantService        TenantService
	}
	type args struct {
		session       *influxdb.Authorization
		authorization *influxdb.Authorization
	}
	type wants struct {
		statusCode  int
		contentType string
		body        string
		bodyErr     string
	}

	var (
		defaultFields = fields{
			AuthorizationService: &mock.AuthorizationService{
				CreateAuthorizationFn: func(ctx context.Context, c *influxdb.Authorization) error {
					c.ID = itesting.MustIDBase16("020f755c3c082000")
					return nil
				},
			},
			TenantService: &tenantService{
				FindUserByIDFn: func(ctx context.Context, id platform.ID) (*influxdb.User, error) {
					return &influxdb.User{
						ID:   id,
						Name: "u1",
					}, nil
				},
				FindOrganizationByIDF: func(ctx context.Context, id platform.ID) (*influxdb.Organization, error) {
					return &influxdb.Organization{
						ID:   id,
						Name: "o1",
					}, nil
				},
				FindBucketByIDFn: func(ctx context.Context, id platform.ID) (*influxdb.Bucket, error) {
					return &influxdb.Bucket{
						ID:   id,
						Name: "b1",
					}, nil
				},
			},
		}

		defaultSession = &influxdb.Authorization{
			Token:       "session-token",
			ID:          itesting.MustIDBase16("020f755c3c082000"),
			UserID:      itesting.MustIDBase16("aaaaaaaaaaaaaaaa"),
			OrgID:       itesting.MustIDBase16("020f755c3c083000"),
			Description: "can write to authorization resource",
			Permissions: []influxdb.Permission{
				{
					Action: influxdb.WriteAction,
					Resource: influxdb.Resource{
						Type:  influxdb.AuthorizationsResourceType,
						OrgID: itesting.IDPtr(itesting.MustIDBase16("020f755c3c083000")),
					},
				},
			},
		}
	)

	tests := []struct {
		name   string
		fields fields
		args   args
		wants  wants
	}{
		{
			name:   "create an authorization",
			fields: defaultFields,
			args: args{
				session: defaultSession,
				authorization: &influxdb.Authorization{
					ID:          itesting.MustIDBase16("020f755c3c082000"),
					OrgID:       itesting.MustIDBase16("020f755c3c083000"),
					Token:       "first:second",
					Description: "only read dashboards sucka",
					Permissions: []influxdb.Permission{
						{
							Action: influxdb.ReadAction,
							Resource: influxdb.Resource{
								Type:  influxdb.DashboardsResourceType,
								OrgID: itesting.IDPtr(itesting.MustIDBase16("020f755c3c083000")),
							},
						},
					},
				},
			},
			wants: wants{
				statusCode:  http.StatusCreated,
				contentType: "application/json; charset=utf-8",
				body: `
{
	"createdAt": "0001-01-01T00:00:00Z",
	"updatedAt": "0001-01-01T00:00:00Z",
  "description": "only read dashboards sucka",
  "id": "020f755c3c082000",
  "links": {
    "self": "/api/v2/authorizations/020f755c3c082000",
    "user": "/api/v2/users/aaaaaaaaaaaaaaaa"
  },
  "org": "o1",
  "orgID": "020f755c3c083000",
  "permissions": [
    {
      "action": "read",
			"resource": {
				"type": "dashboards",
				"orgID": "020f755c3c083000",
				"org": "o1"
			}
    }
  ],
  "status": "active",
  "authorizationType": "v1_user",
  "user": "u1",
  "userID": "aaaaaaaaaaaaaaaa"
}
`,
			},
		},
		{
			name:   "create an authorization with missing token",
			fields: defaultFields,
			args: args{
				session: defaultSession,
				authorization: &influxdb.Authorization{
					ID:          itesting.MustIDBase16("020f755c3c082000"),
					OrgID:       itesting.MustIDBase16("020f755c3c083000"),
					Description: "only read dashboards sucka",
					Permissions: []influxdb.Permission{
						{
							Action: influxdb.ReadAction,
							Resource: influxdb.Resource{
								Type:  influxdb.DashboardsResourceType,
								OrgID: itesting.IDPtr(itesting.MustIDBase16("020f755c3c083000")),
							},
						},
					},
				},
			},
			wants: wants{
				statusCode:  http.StatusBadRequest,
				contentType: "application/json; charset=utf-8",
				bodyErr: `
{
	"code": "invalid",
	"message": "token required for v1 user authorization type"
}
`,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Helper()

			s := itesting.NewTestInmemStore(t)
			storage, err := NewStore(s)
			if err != nil {
				t.Fatal(err)
			}

			svc := NewService(storage, tt.fields.TenantService)

			handler := NewHTTPAuthHandler(zaptest.NewLogger(t), svc, nil, tt.fields.TenantService)
			router := chi.NewRouter()
			router.Mount(handler.Prefix(), handler)

			newRequest := func(a *influxdb.Authorization) *postAuthorizationRequest {
				return &postAuthorizationRequest{
					OrgID:       a.OrgID,
					Description: a.Description,
					Permissions: a.Permissions,
					Status:      a.Status,
					Token:       a.Token,
				}
			}

			req := newRequest(tt.args.authorization)
			b, err := json.Marshal(req)
			if err != nil {
				t.Fatalf("failed to unmarshal authorization: %v", err)
			}

			r := httptest.NewRequest("GET", "http://any.url", bytes.NewReader(b))
			r = r.WithContext(context.WithValue(
				context.Background(),
				httprouter.ParamsKey,
				httprouter.Params{
					{
						Key:   "userID",
						Value: fmt.Sprintf("%d", tt.args.session.UserID),
					},
				}))

			w := httptest.NewRecorder()

			ctx := icontext.SetAuthorizer(context.Background(), tt.args.session)
			r = r.WithContext(ctx)

			handler.handlePostAuthorization(w, r)

			res := w.Result()
			content := res.Header.Get("Content-Type")
			body, _ := io.ReadAll(res.Body)

			if res.StatusCode != tt.wants.statusCode {
				t.Logf("headers: %v body: %s", res.Header, body)
				t.Errorf("%q. handlePostAuthorization() = %v, want %v", tt.name, res.StatusCode, tt.wants.statusCode)
			}
			if tt.wants.contentType != "" && content != tt.wants.contentType {
				t.Errorf("%q. handlePostAuthorization() = %v, want %v", tt.name, content, tt.wants.contentType)
			}
			if tt.wants.body != "" {
				if diff, err := jsonDiff(string(body), tt.wants.body); diff != "" {
					t.Errorf("%q. handlePostAuthorization() = ***%s***", tt.name, diff)
				} else if err != nil {
					t.Errorf("%q, handlePostAuthorization() error: %v", tt.name, err)
				}
			}
			if tt.wants.bodyErr != "" {
				if diff, err := jsonDiffErr(string(body), tt.wants.bodyErr); diff != "" {
					t.Errorf("%q. handlePostAuthorization() = ***%s***", tt.name, diff)
				} else if err != nil {
					t.Errorf("%q, handlePostAuthorization() error: %v", tt.name, err)
				}
			}

		})
	}
}

func TestService_handleGetAuthorization(t *testing.T) {
	type fields struct {
		AuthorizationService influxdb.AuthorizationService
		TenantService        TenantService
	}
	type args struct {
		id string
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
			name: "get a authorization by id",
			fields: fields{
				AuthorizationService: &mock.AuthorizationService{
					FindAuthorizationByIDFn: func(ctx context.Context, id platform.ID) (*influxdb.Authorization, error) {
						if id == itesting.MustIDBase16("020f755c3c082000") {
							return &influxdb.Authorization{
								ID:     itesting.MustIDBase16("020f755c3c082000"),
								UserID: itesting.MustIDBase16("020f755c3c082000"),
								OrgID:  itesting.MustIDBase16("020f755c3c083000"),
								Permissions: []influxdb.Permission{
									{
										Action: influxdb.ReadAction,
										Resource: influxdb.Resource{
											Type:  influxdb.BucketsResourceType,
											OrgID: itesting.IDPtr(itesting.MustIDBase16("020f755c3c083000")),
											ID: func() *platform.ID {
												id := itesting.MustIDBase16("020f755c3c084000")
												return &id
											}(),
										},
									},
								},
								Token: "hello",
							}, nil
						}

						return nil, fmt.Errorf("not found")
					},
				},
				TenantService: &tenantService{
					FindUserByIDFn: func(ctx context.Context, id platform.ID) (*influxdb.User, error) {
						return &influxdb.User{
							ID:   id,
							Name: "u1",
						}, nil
					},
					FindOrganizationByIDF: func(ctx context.Context, id platform.ID) (*influxdb.Organization, error) {
						return &influxdb.Organization{
							ID:   id,
							Name: "o1",
						}, nil
					},
					FindBucketByIDFn: func(ctx context.Context, id platform.ID) (*influxdb.Bucket, error) {
						return &influxdb.Bucket{
							ID:   id,
							Name: "b1",
						}, nil
					},
				},
			},
			args: args{
				id: "020f755c3c082000",
			},
			wants: wants{
				statusCode:  http.StatusOK,
				contentType: "application/json; charset=utf-8",
				body: `
{
	"createdAt": "0001-01-01T00:00:00Z",
	"updatedAt": "0001-01-01T00:00:00Z",
  "description": "",
  "id": "020f755c3c082000",
  "links": {
    "self": "/api/v2/authorizations/020f755c3c082000",
    "user": "/api/v2/users/020f755c3c082000"
  },
  "org": "o1",
  "orgID": "020f755c3c083000",
  "permissions": [
    {
      "action": "read",
      "resource": {
				"type": "buckets",
				"orgID": "020f755c3c083000",
				"id": "020f755c3c084000",
				"name": "b1",
				"org": "o1"
			}
    }
  ],
  "status": "",
  "authorizationType": "plain",
  "token": "hello",
  "user": "u1",
  "userID": "020f755c3c082000"
}
`,
			},
		},
		{
			name: "not found",
			fields: fields{
				AuthorizationService: &mock.AuthorizationService{
					FindAuthorizationByIDFn: func(ctx context.Context, id platform.ID) (*influxdb.Authorization, error) {
						return nil, &errors.Error{
							Code: errors.ENotFound,
							Msg:  "authorization not found",
						}
					},
				},
				TenantService: &tenantService{},
			},
			args: args{
				id: "020f755c3c082000",
			},
			wants: wants{
				statusCode: http.StatusNotFound,
				body:       `{"code":"not found","message":"authorization not found"}`,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Helper()

			handler := NewHTTPAuthHandler(zaptest.NewLogger(t), tt.fields.AuthorizationService, nil, tt.fields.TenantService)
			router := chi.NewRouter()
			router.Mount(handler.Prefix(), handler)

			w := httptest.NewRecorder()

			r := httptest.NewRequest("GET", "http://any.url", nil)
			rctx := chi.NewRouteContext()
			rctx.URLParams.Add("id", tt.args.id)
			r = r.WithContext(context.WithValue(r.Context(), chi.RouteCtxKey, rctx))

			handler.handleGetAuthorization(w, r)

			res := w.Result()
			content := res.Header.Get("Content-Type")
			body, _ := io.ReadAll(res.Body)

			if res.StatusCode != tt.wants.statusCode {
				t.Logf("headers: %v body: %s", res.Header, body)
				t.Errorf("%q. handleGetAuthorization() = %v, want %v", tt.name, res.StatusCode, tt.wants.statusCode)
			}
			if tt.wants.contentType != "" && content != tt.wants.contentType {
				t.Errorf("%q. handleGetAuthorization() = %v, want %v", tt.name, content, tt.wants.contentType)
			}
			if diff, err := jsonDiff(string(body), tt.wants.body); err != nil {
				t.Errorf("%q, handleGetAuthorization. error unmarshaling json %v", tt.name, err)
			} else if tt.wants.body != "" && diff != "" {
				t.Errorf("%q. handleGetAuthorization() = -got/+want %s**", tt.name, diff)
			}
		})
	}
}

func TestService_handleGetAuthorizations(t *testing.T) {
	type fields struct {
		AuthorizationService influxdb.AuthorizationService
		TenantService        TenantService
	}

	type args struct {
		queryParams map[string][]string
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
			name: "get all authorizations",
			fields: fields{
				&mock.AuthorizationService{
					FindAuthorizationsFn: func(ctx context.Context, filter influxdb.AuthorizationFilter, opts ...influxdb.FindOptions) ([]*influxdb.Authorization, int, error) {
						return []*influxdb.Authorization{
							{
								ID:          itesting.MustIDBase16("0d0a657820696e74"),
								Token:       "hello",
								UserID:      itesting.MustIDBase16("2070616e656d2076"),
								OrgID:       itesting.MustIDBase16("3070616e656d2076"),
								Description: "t1",
								Permissions: influxdb.OperPermissions(),
							},
							{
								ID:          itesting.MustIDBase16("6669646573207375"),
								Token:       "example",
								UserID:      itesting.MustIDBase16("6c7574652c206f6e"),
								OrgID:       itesting.MustIDBase16("9d70616e656d2076"),
								Description: "t2",
								Permissions: influxdb.OperPermissions(),
							},
						}, 2, nil
					},
				},
				&tenantService{
					FindUserByIDFn: func(ctx context.Context, id platform.ID) (*influxdb.User, error) {
						return &influxdb.User{
							ID:   id,
							Name: id.String(),
						}, nil
					},

					FindOrganizationByIDF: func(ctx context.Context, id platform.ID) (*influxdb.Organization, error) {
						return &influxdb.Organization{
							ID:   id,
							Name: id.String(),
						}, nil
					},
				},
			},
			args: args{},
			wants: wants{
				statusCode:  http.StatusOK,
				contentType: "application/json; charset=utf-8",
				body: fmt.Sprintf(`
{
  "links": {
    "self": "/api/v2/authorizations"
  },
  "authorizations": [
    {
      "links": {
        "user": "/api/v2/users/2070616e656d2076",
        "self": "/api/v2/authorizations/0d0a657820696e74"
      },
      "id": "0d0a657820696e74",
	  "userID": "2070616e656d2076",
	  "user": "2070616e656d2076",
	  "org": "3070616e656d2076",
	  "orgID": "3070616e656d2076",
      "status": "",
	  "token": "hello",
	  "description": "t1",
		"permissions": %s,
		"createdAt": "0001-01-01T00:00:00Z",
		"updatedAt": "0001-01-01T00:00:00Z"
    },
    {
      "links": {
        "user": "/api/v2/users/6c7574652c206f6e",
        "self": "/api/v2/authorizations/6669646573207375"
      },
      "id": "6669646573207375",
      "userID": "6c7574652c206f6e",
      "user": "6c7574652c206f6e",
	  "org": "9d70616e656d2076",
	  "orgID": "9d70616e656d2076",
      "status": "",
      "token": "example",
	  "description": "t2",
		"permissions": %s,
		"createdAt": "0001-01-01T00:00:00Z",
		"updatedAt": "0001-01-01T00:00:00Z"
    }
  ]
}
`,
					MustMarshal(influxdb.OperPermissions()),
					MustMarshal(influxdb.OperPermissions())),
			},
		},
		{
			name: "skip authorizations with no org",
			fields: fields{
				&mock.AuthorizationService{
					FindAuthorizationsFn: func(ctx context.Context, filter influxdb.AuthorizationFilter, opts ...influxdb.FindOptions) ([]*influxdb.Authorization, int, error) {
						return []*influxdb.Authorization{
							{
								ID:          itesting.MustIDBase16("0d0a657820696e74"),
								Token:       "hello",
								UserID:      itesting.MustIDBase16("2070616e656d2076"),
								OrgID:       itesting.MustIDBase16("3070616e656d2076"),
								Description: "t1",
								Permissions: influxdb.OperPermissions(),
							},
							{
								ID:          itesting.MustIDBase16("6669646573207375"),
								Token:       "example",
								UserID:      itesting.MustIDBase16("6c7574652c206f6e"),
								OrgID:       itesting.MustIDBase16("9d70616e656d2076"),
								Description: "t2",
								Permissions: influxdb.OperPermissions(),
							},
						}, 2, nil
					},
				},
				&tenantService{
					FindUserByIDFn: func(ctx context.Context, id platform.ID) (*influxdb.User, error) {
						if id.String() == "2070616e656d2076" {
							return &influxdb.User{
								ID:   id,
								Name: id.String(),
							}, nil
						}
						return nil, &errors.Error{}
					},
					FindOrganizationByIDF: func(ctx context.Context, id platform.ID) (*influxdb.Organization, error) {
						return &influxdb.Organization{
							ID:   id,
							Name: id.String(),
						}, nil
					},
				},
			},
			args: args{},
			wants: wants{
				statusCode:  http.StatusOK,
				contentType: "application/json; charset=utf-8",
				body: fmt.Sprintf(`
{
  "links": {
    "self": "/api/v2/authorizations"
  },
  "authorizations": [
    {
      "links": {
        "user": "/api/v2/users/2070616e656d2076",
        "self": "/api/v2/authorizations/0d0a657820696e74"
      },
      "id": "0d0a657820696e74",
	  "userID": "2070616e656d2076",
	  "user": "2070616e656d2076",
	  "org": "3070616e656d2076",
	  "orgID": "3070616e656d2076",
      "status": "",
	  "token": "hello",
	  "description": "t1",
		"permissions": %s,
		"createdAt": "0001-01-01T00:00:00Z",
		"updatedAt": "0001-01-01T00:00:00Z"
    }
  ]
}
`,
					MustMarshal(influxdb.OperPermissions())),
			},
		},
		{
			name: "skip authorizations with no user",
			fields: fields{
				&mock.AuthorizationService{
					FindAuthorizationsFn: func(ctx context.Context, filter influxdb.AuthorizationFilter, opts ...influxdb.FindOptions) ([]*influxdb.Authorization, int, error) {
						return []*influxdb.Authorization{
							{
								ID:          itesting.MustIDBase16("0d0a657820696e74"),
								Token:       "hello",
								UserID:      itesting.MustIDBase16("2070616e656d2076"),
								OrgID:       itesting.MustIDBase16("3070616e656d2076"),
								Description: "t1",
								Permissions: influxdb.OperPermissions(),
							},
							{
								ID:          itesting.MustIDBase16("6669646573207375"),
								Token:       "example",
								UserID:      itesting.MustIDBase16("6c7574652c206f6e"),
								OrgID:       itesting.MustIDBase16("9d70616e656d2076"),
								Description: "t2",
								Permissions: influxdb.OperPermissions(),
							},
						}, 2, nil
					},
				},
				&tenantService{
					FindUserByIDFn: func(ctx context.Context, id platform.ID) (*influxdb.User, error) {
						return &influxdb.User{
							ID:   id,
							Name: id.String(),
						}, nil
					},
					FindOrganizationByIDF: func(ctx context.Context, id platform.ID) (*influxdb.Organization, error) {
						if id.String() == "3070616e656d2076" {
							return &influxdb.Organization{
								ID:   id,
								Name: id.String(),
							}, nil
						}
						return nil, &errors.Error{}
					},
				},
			},
			args: args{},
			wants: wants{
				statusCode:  http.StatusOK,
				contentType: "application/json; charset=utf-8",
				body: fmt.Sprintf(`
{
  "links": {
    "self": "/api/v2/authorizations"
  },
  "authorizations": [
    {
      "links": {
        "user": "/api/v2/users/2070616e656d2076",
        "self": "/api/v2/authorizations/0d0a657820696e74"
      },
      "id": "0d0a657820696e74",
	  "userID": "2070616e656d2076",
	  "user": "2070616e656d2076",
	  "org": "3070616e656d2076",
	  "orgID": "3070616e656d2076",
      "status": "",
	  "token": "hello",
	  "description": "t1",
		"permissions": %s,
		"createdAt": "0001-01-01T00:00:00Z",
		"updatedAt": "0001-01-01T00:00:00Z"
    }
  ]
}
`,
					MustMarshal(influxdb.OperPermissions())),
			},
		},
		{
			name: "get all authorizations when there are none",
			fields: fields{
				AuthorizationService: &mock.AuthorizationService{
					FindAuthorizationsFn: func(ctx context.Context, filter influxdb.AuthorizationFilter, opts ...influxdb.FindOptions) ([]*influxdb.Authorization, int, error) {
						return []*influxdb.Authorization{}, 0, nil
					},
				},
			},
			args: args{},
			wants: wants{
				statusCode:  http.StatusOK,
				contentType: "application/json; charset=utf-8",
				body: `
{
  "links": {
    "self": "/api/v2/authorizations"
  },
  "authorizations": []
}`,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Helper()

			s := itesting.NewTestInmemStore(t)
			storage, err := NewStore(s)
			if err != nil {
				t.Fatal(err)
			}

			svc := NewService(storage, tt.fields.TenantService)

			handler := NewHTTPAuthHandler(zaptest.NewLogger(t), svc, nil, tt.fields.TenantService)
			router := chi.NewRouter()
			router.Mount(handler.Prefix(), handler)

			r := httptest.NewRequest("GET", "http://any.url", nil)

			qp := r.URL.Query()
			for k, vs := range tt.args.queryParams {
				for _, v := range vs {
					qp.Add(k, v)
				}
			}
			r.URL.RawQuery = qp.Encode()

			w := httptest.NewRecorder()

			handler.handleGetAuthorizations(w, r)

			res := w.Result()
			content := res.Header.Get("Content-Type")
			body, _ := io.ReadAll(res.Body)

			if res.StatusCode != tt.wants.statusCode {
				t.Errorf("%q. handleGetAuthorizations() = %v, want %v", tt.name, res.StatusCode, tt.wants.statusCode)
			}
			if tt.wants.contentType != "" && content != tt.wants.contentType {
				t.Errorf("%q. handleGetAuthorizations() = %v, want %v", tt.name, content, tt.wants.contentType)
			}
			if diff, err := jsonDiff(string(body), tt.wants.body); diff != "" {
				t.Errorf("%q. handleGetAuthorizations() = ***%s***", tt.name, diff)
			} else if err != nil {
				t.Errorf("%q, handleGetAuthorizations() error: %v", tt.name, err)
			}

		})
	}
}

func TestService_handleDeleteAuthorization(t *testing.T) {
	type fields struct {
		AuthorizationService influxdb.AuthorizationService
		TenantService        TenantService
	}
	type args struct {
		id string
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
			name: "remove a authorization by id",
			fields: fields{
				&mock.AuthorizationService{
					DeleteAuthorizationFn: func(ctx context.Context, id platform.ID) error {
						if id == itesting.MustIDBase16("020f755c3c082000") {
							return nil
						}

						return fmt.Errorf("wrong id")
					},
				},
				&tenantService{},
			},
			args: args{
				id: "020f755c3c082000",
			},
			wants: wants{
				statusCode: http.StatusNoContent,
			},
		},
		{
			name: "authorization not found",
			fields: fields{
				&mock.AuthorizationService{
					DeleteAuthorizationFn: func(ctx context.Context, id platform.ID) error {
						return &errors.Error{
							Code: errors.ENotFound,
							Msg:  "authorization not found",
						}
					},
				},
				&tenantService{},
			},
			args: args{
				id: "020f755c3c082000",
			},
			wants: wants{
				statusCode: http.StatusNotFound,
				body:       `{"code":"not found","message":"authorization not found"}`,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Helper()

			handler := NewHTTPAuthHandler(zaptest.NewLogger(t), tt.fields.AuthorizationService, nil, tt.fields.TenantService)
			router := chi.NewRouter()
			router.Mount(handler.Prefix(), handler)

			w := httptest.NewRecorder()

			r := httptest.NewRequest("GET", "http://any.url", nil)
			rctx := chi.NewRouteContext()
			rctx.URLParams.Add("id", tt.args.id)
			r = r.WithContext(context.WithValue(r.Context(), chi.RouteCtxKey, rctx))

			handler.handleDeleteAuthorization(w, r)

			res := w.Result()
			content := res.Header.Get("Content-Type")
			body, _ := io.ReadAll(res.Body)

			if res.StatusCode != tt.wants.statusCode {
				t.Errorf("%q. handleDeleteAuthorization() = %v, want %v", tt.name, res.StatusCode, tt.wants.statusCode)
			}
			if tt.wants.contentType != "" && content != tt.wants.contentType {
				t.Errorf("%q. handleDeleteAuthorization() = %v, want %v", tt.name, content, tt.wants.contentType)
			}

			if tt.wants.body != "" {
				if diff, err := jsonDiff(string(body), tt.wants.body); err != nil {
					t.Errorf("%q, handleDeleteAuthorization(). error unmarshaling json %v", tt.name, err)
				} else if diff != "" {
					t.Errorf("%q. handleDeleteAuthorization() = ***%s***", tt.name, diff)
				}
			}
		})
	}
}

func jsonDiff(s1, s2 string) (diff string, err error) {
	if s1 == s2 {
		return "", nil
	}

	if s1 == "" {
		return s2, fmt.Errorf("s1 is empty")
	}

	if s2 == "" {
		return s1, fmt.Errorf("s2 is empty")
	}

	var o1 influxdb.Authorization
	if err = json.Unmarshal([]byte(s1), &o1); err != nil {
		return
	}

	var o2 influxdb.Authorization
	if err = json.Unmarshal([]byte(s2), &o2); err != nil {
		return
	}

	return cmp.Diff(o1, o2, authorizationCmpOptions...), err
}

func jsonDiffErr(s1, s2 string) (diff string, err error) {
	if s1 == s2 {
		return "", nil
	}

	if s1 == "" {
		return s2, fmt.Errorf("s1 is empty")
	}

	if s2 == "" {
		return s1, fmt.Errorf("s2 is empty")
	}

	var o1 errors.Error
	if err = json.Unmarshal([]byte(s1), &o1); err != nil {
		return
	}

	var o2 errors.Error
	if err = json.Unmarshal([]byte(s2), &o2); err != nil {
		return
	}

	return cmp.Diff(o1, o2), err
}

var authorizationCmpOptions = cmp.Options{
	cmpopts.EquateEmpty(),
	cmpopts.IgnoreFields(influxdb.Authorization{}, "ID", "Token", "CreatedAt", "UpdatedAt"),
	cmp.Comparer(func(x, y []byte) bool {
		return bytes.Equal(x, y)
	}),
	cmp.Transformer("Sort", func(in []*influxdb.Authorization) []*influxdb.Authorization {
		out := append([]*influxdb.Authorization(nil), in...) // Copy input to avoid mutating it
		sort.Slice(out, func(i, j int) bool {
			return out[i].ID.String() > out[j].ID.String()
		})
		return out
	}),
}

func MustMarshal(o interface{}) []byte {
	b, _ := json.Marshal(o)
	return b
}
