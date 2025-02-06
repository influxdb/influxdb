package http

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/influxdata/httprouter"
	"github.com/influxdata/influxdb/v2"
	platform "github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/authorization"
	pcontext "github.com/influxdata/influxdb/v2/context"
	platform2 "github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/influxdata/influxdb/v2/kit/platform/errors"
	kithttp "github.com/influxdata/influxdb/v2/kit/transport/http"
	"github.com/influxdata/influxdb/v2/kv"
	"github.com/influxdata/influxdb/v2/mock"
	"github.com/influxdata/influxdb/v2/tenant"
	platformtesting "github.com/influxdata/influxdb/v2/testing"
	"go.uber.org/zap/zaptest"
)

// NewMockAuthorizationBackend returns a AuthorizationBackend with mock services.
func NewMockAuthorizationBackend(t *testing.T) *AuthorizationBackend {
	return &AuthorizationBackend{
		log: zaptest.NewLogger(t),

		AuthorizationService: mock.NewAuthorizationService(),
		OrganizationService:  mock.NewOrganizationService(),
		UserService:          mock.NewUserService(),
		LookupService:        mock.NewLookupService(),
	}
}

func TestService_handleGetAuthorizations(t *testing.T) {
	type fields struct {
		AuthorizationService platform.AuthorizationService
		UserService          platform.UserService
		OrganizationService  platform.OrganizationService
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
					FindAuthorizationsFn: func(ctx context.Context, filter platform.AuthorizationFilter, opts ...platform.FindOptions) ([]*platform.Authorization, int, error) {
						return []*platform.Authorization{
							{
								ID:          platformtesting.MustIDBase16("0d0a657820696e74"),
								Token:       "hello",
								UserID:      platformtesting.MustIDBase16("2070616e656d2076"),
								OrgID:       platformtesting.MustIDBase16("3070616e656d2076"),
								Description: "t1",
								Permissions: platform.OperPermissions(),
							},
							{
								ID:          platformtesting.MustIDBase16("6669646573207375"),
								Token:       "example",
								UserID:      platformtesting.MustIDBase16("6c7574652c206f6e"),
								OrgID:       platformtesting.MustIDBase16("9d70616e656d2076"),
								Description: "t2",
								Permissions: platform.OperPermissions(),
							},
						}, 2, nil
					},
				},
				&mock.UserService{
					FindUserByIDFn: func(ctx context.Context, id platform2.ID) (*platform.User, error) {
						return &platform.User{
							ID:   id,
							Name: id.String(),
						}, nil
					},
				},
				&mock.OrganizationService{
					FindOrganizationByIDF: func(ctx context.Context, id platform2.ID) (*platform.Organization, error) {
						return &platform.Organization{
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
					MustMarshal(platform.OperPermissions()),
					MustMarshal(platform.OperPermissions())),
			},
		},
		{
			name: "skip authorizations with no org",
			fields: fields{
				&mock.AuthorizationService{
					FindAuthorizationsFn: func(ctx context.Context, filter platform.AuthorizationFilter, opts ...platform.FindOptions) ([]*platform.Authorization, int, error) {
						return []*platform.Authorization{
							{
								ID:          platformtesting.MustIDBase16("0d0a657820696e74"),
								Token:       "hello",
								UserID:      platformtesting.MustIDBase16("2070616e656d2076"),
								OrgID:       platformtesting.MustIDBase16("3070616e656d2076"),
								Description: "t1",
								Permissions: platform.OperPermissions(),
							},
							{
								ID:          platformtesting.MustIDBase16("6669646573207375"),
								Token:       "example",
								UserID:      platformtesting.MustIDBase16("6c7574652c206f6e"),
								OrgID:       platformtesting.MustIDBase16("9d70616e656d2076"),
								Description: "t2",
								Permissions: platform.OperPermissions(),
							},
						}, 2, nil
					},
				},
				&mock.UserService{
					FindUserByIDFn: func(ctx context.Context, id platform2.ID) (*platform.User, error) {
						if id.String() == "2070616e656d2076" {
							return &platform.User{
								ID:   id,
								Name: id.String(),
							}, nil
						}
						return nil, &errors.Error{}
					},
				},
				&mock.OrganizationService{
					FindOrganizationByIDF: func(ctx context.Context, id platform2.ID) (*platform.Organization, error) {
						return &platform.Organization{
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
					MustMarshal(platform.OperPermissions())),
			},
		},
		{
			name: "skip authorizations with no user",
			fields: fields{
				&mock.AuthorizationService{
					FindAuthorizationsFn: func(ctx context.Context, filter platform.AuthorizationFilter, opts ...platform.FindOptions) ([]*platform.Authorization, int, error) {
						return []*platform.Authorization{
							{
								ID:          platformtesting.MustIDBase16("0d0a657820696e74"),
								Token:       "hello",
								UserID:      platformtesting.MustIDBase16("2070616e656d2076"),
								OrgID:       platformtesting.MustIDBase16("3070616e656d2076"),
								Description: "t1",
								Permissions: platform.OperPermissions(),
							},
							{
								ID:          platformtesting.MustIDBase16("6669646573207375"),
								Token:       "example",
								UserID:      platformtesting.MustIDBase16("6c7574652c206f6e"),
								OrgID:       platformtesting.MustIDBase16("9d70616e656d2076"),
								Description: "t2",
								Permissions: platform.OperPermissions(),
							},
						}, 2, nil
					},
				},
				&mock.UserService{
					FindUserByIDFn: func(ctx context.Context, id platform2.ID) (*platform.User, error) {
						return &platform.User{
							ID:   id,
							Name: id.String(),
						}, nil
					},
				},
				&mock.OrganizationService{
					FindOrganizationByIDF: func(ctx context.Context, id platform2.ID) (*platform.Organization, error) {
						if id.String() == "3070616e656d2076" {
							return &platform.Organization{
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
					MustMarshal(platform.OperPermissions())),
			},
		},
		{
			name: "get all authorizations when there are none",
			fields: fields{
				AuthorizationService: &mock.AuthorizationService{
					FindAuthorizationsFn: func(ctx context.Context, filter platform.AuthorizationFilter, opts ...platform.FindOptions) ([]*platform.Authorization, int, error) {
						return []*platform.Authorization{}, 0, nil
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
			authorizationBackend := NewMockAuthorizationBackend(t)
			authorizationBackend.HTTPErrorHandler = kithttp.NewErrorHandler(zaptest.NewLogger(t))
			authorizationBackend.AuthorizationService = tt.fields.AuthorizationService
			authorizationBackend.UserService = tt.fields.UserService
			authorizationBackend.OrganizationService = tt.fields.OrganizationService
			h := NewAuthorizationHandler(zaptest.NewLogger(t), authorizationBackend)

			r := httptest.NewRequest("GET", "http://any.url", nil)

			qp := r.URL.Query()
			for k, vs := range tt.args.queryParams {
				for _, v := range vs {
					qp.Add(k, v)
				}
			}
			r.URL.RawQuery = qp.Encode()

			w := httptest.NewRecorder()

			h.handleGetAuthorizations(w, r)

			res := w.Result()
			content := res.Header.Get("Content-Type")
			body, _ := io.ReadAll(res.Body)

			if res.StatusCode != tt.wants.statusCode {
				t.Errorf("%q. handleGetAuthorizations() = %v, want %v", tt.name, res.StatusCode, tt.wants.statusCode)
			}
			if tt.wants.contentType != "" && content != tt.wants.contentType {
				t.Errorf("%q. handleGetAuthorizations() = %v, want %v", tt.name, content, tt.wants.contentType)
			}
			if eq, diff, err := jsonEqual(string(body), tt.wants.body); err != nil {
				t.Errorf("%q, handleGetAuthorizations(). error unmarshalling json %v", tt.name, err)
			} else if tt.wants.body != "" && !eq {
				t.Errorf("%q. handleGetAuthorizations() = ***%s***", tt.name, diff)
			}

		})
	}
}

func TestService_handleGetAuthorization(t *testing.T) {
	type fields struct {
		AuthorizationService platform.AuthorizationService
		UserService          platform.UserService
		OrganizationService  platform.OrganizationService
		LookupService        platform.LookupService
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
					FindAuthorizationByIDFn: func(ctx context.Context, id platform2.ID) (*platform.Authorization, error) {
						if id == platformtesting.MustIDBase16("020f755c3c082000") {
							return &platform.Authorization{
								ID:     platformtesting.MustIDBase16("020f755c3c082000"),
								UserID: platformtesting.MustIDBase16("020f755c3c082000"),
								OrgID:  platformtesting.MustIDBase16("020f755c3c083000"),
								Permissions: []platform.Permission{
									{
										Action: platform.ReadAction,
										Resource: platform.Resource{
											Type:  platform.BucketsResourceType,
											OrgID: platformtesting.IDPtr(platformtesting.MustIDBase16("020f755c3c083000")),
											ID: func() *platform2.ID {
												id := platformtesting.MustIDBase16("020f755c3c084000")
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
				UserService: &mock.UserService{
					FindUserByIDFn: func(ctx context.Context, id platform2.ID) (*platform.User, error) {
						return &platform.User{
							ID:   id,
							Name: "u1",
						}, nil
					},
				},
				OrganizationService: &mock.OrganizationService{
					FindOrganizationByIDF: func(ctx context.Context, id platform2.ID) (*platform.Organization, error) {
						return &platform.Organization{
							ID:   id,
							Name: "o1",
						}, nil
					},
				},
				LookupService: &mock.LookupService{
					NameFn: func(ctx context.Context, resource platform.ResourceType, id platform2.ID) (string, error) {
						switch resource {
						case platform.BucketsResourceType:
							return "b1", nil
						case platform.OrgsResourceType:
							return "o1", nil
						}
						return "", fmt.Errorf("bad resource type %s", resource)
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
					FindAuthorizationByIDFn: func(ctx context.Context, id platform2.ID) (*platform.Authorization, error) {
						return nil, &errors.Error{
							Code: errors.ENotFound,
							Msg:  "authorization not found",
						}
					},
				},
				UserService:         &mock.UserService{},
				OrganizationService: &mock.OrganizationService{},
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
			authorizationBackend := NewMockAuthorizationBackend(t)
			authorizationBackend.HTTPErrorHandler = kithttp.NewErrorHandler(zaptest.NewLogger(t))
			authorizationBackend.AuthorizationService = tt.fields.AuthorizationService
			authorizationBackend.UserService = tt.fields.UserService
			authorizationBackend.OrganizationService = tt.fields.OrganizationService
			authorizationBackend.LookupService = tt.fields.LookupService
			h := NewAuthorizationHandler(zaptest.NewLogger(t), authorizationBackend)

			r := httptest.NewRequest("GET", "http://any.url", nil)

			r = r.WithContext(context.WithValue(
				context.Background(),
				httprouter.ParamsKey,
				httprouter.Params{
					{
						Key:   "id",
						Value: tt.args.id,
					},
				}))

			w := httptest.NewRecorder()

			h.handleGetAuthorization(w, r)

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
			if eq, diff, err := jsonEqual(string(body), tt.wants.body); err != nil {
				t.Errorf("%q, handleGetAuthorization. error unmarshalling json %v", tt.name, err)
			} else if tt.wants.body != "" && !eq {
				t.Errorf("%q. handleGetAuthorization() = -got/+want %s**", tt.name, diff)
			}
		})
	}
}

func TestService_handlePostAuthorization(t *testing.T) {
	type fields struct {
		AuthorizationService platform.AuthorizationService
		UserService          platform.UserService
		OrganizationService  platform.OrganizationService
		LookupService        platform.LookupService
	}
	type args struct {
		session       *platform.Authorization
		authorization *platform.Authorization
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
			name: "create a new authorization",
			fields: fields{
				AuthorizationService: &mock.AuthorizationService{
					CreateAuthorizationFn: func(ctx context.Context, c *platform.Authorization) error {
						c.ID = platformtesting.MustIDBase16("020f755c3c082000")
						c.Token = "new-test-token"
						return nil
					},
				},
				LookupService: &mock.LookupService{
					NameFn: func(ctx context.Context, resource platform.ResourceType, id platform2.ID) (string, error) {
						switch resource {
						case platform.BucketsResourceType:
							return "b1", nil
						case platform.OrgsResourceType:
							return "o1", nil
						}
						return "", fmt.Errorf("bad resource type %s", resource)
					},
				},
				UserService: &mock.UserService{
					FindUserByIDFn: func(ctx context.Context, id platform2.ID) (*platform.User, error) {
						if !id.Valid() {
							return nil, platform2.ErrInvalidID
						}
						return &platform.User{
							ID:   id,
							Name: "u1",
						}, nil
					},
				},
				OrganizationService: &mock.OrganizationService{
					FindOrganizationByIDF: func(ctx context.Context, id platform2.ID) (*platform.Organization, error) {
						if !id.Valid() {
							return nil, platform2.ErrInvalidID
						}
						return &platform.Organization{
							ID:   id,
							Name: "o1",
						}, nil
					},
				},
			},
			args: args{
				session: &platform.Authorization{
					Token:       "session-token",
					ID:          platformtesting.MustIDBase16("020f755c3c082000"),
					UserID:      platformtesting.MustIDBase16("aaaaaaaaaaaaaaaa"),
					OrgID:       platformtesting.MustIDBase16("020f755c3c083000"),
					Description: "can write to authorization resource",
					Permissions: []platform.Permission{
						{
							Action: platform.WriteAction,
							Resource: platform.Resource{
								Type:  platform.AuthorizationsResourceType,
								OrgID: platformtesting.IDPtr(platformtesting.MustIDBase16("020f755c3c083000")),
							},
						},
					},
				},
				authorization: &platform.Authorization{
					ID:          platformtesting.MustIDBase16("020f755c3c082000"),
					OrgID:       platformtesting.MustIDBase16("020f755c3c083000"),
					Description: "only read dashboards sucka",
					Permissions: []platform.Permission{
						{
							Action: platform.ReadAction,
							Resource: platform.Resource{
								Type:  platform.DashboardsResourceType,
								OrgID: platformtesting.IDPtr(platformtesting.MustIDBase16("020f755c3c083000")),
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
  "token": "new-test-token",
  "user": "u1",
  "userID": "aaaaaaaaaaaaaaaa"
}
`,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			authorizationBackend := NewMockAuthorizationBackend(t)
			authorizationBackend.HTTPErrorHandler = kithttp.NewErrorHandler(zaptest.NewLogger(t))
			authorizationBackend.AuthorizationService = tt.fields.AuthorizationService
			authorizationBackend.UserService = tt.fields.UserService
			authorizationBackend.OrganizationService = tt.fields.OrganizationService
			authorizationBackend.LookupService = tt.fields.LookupService
			h := NewAuthorizationHandler(zaptest.NewLogger(t), authorizationBackend)

			req, err := newPostAuthorizationRequest(tt.args.authorization)
			if err != nil {
				t.Fatalf("failed to create new authorization request: %v", err)
			}
			b, err := json.Marshal(req)
			if err != nil {
				t.Fatalf("failed to unmarshal authorization: %v", err)
			}

			r := httptest.NewRequest("GET", "http://any.url", bytes.NewReader(b))
			w := httptest.NewRecorder()

			ctx := pcontext.SetAuthorizer(context.Background(), tt.args.session)
			r = r.WithContext(ctx)

			h.handlePostAuthorization(w, r)

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
			if eq, diff, err := jsonEqual(string(body), tt.wants.body); err != nil {
				t.Errorf("%q, handlePostAuthorization(). error unmarshalling json %v", tt.name, err)
			} else if tt.wants.body != "" && !eq {
				t.Errorf("%q. handlePostAuthorization() = ***%s***", tt.name, diff)
			}
		})
	}
}

func TestService_handleDeleteAuthorization(t *testing.T) {
	type fields struct {
		AuthorizationService platform.AuthorizationService
		UserService          platform.UserService
		OrganizationService  platform.OrganizationService
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
					DeleteAuthorizationFn: func(ctx context.Context, id platform2.ID) error {
						if id == platformtesting.MustIDBase16("020f755c3c082000") {
							return nil
						}

						return fmt.Errorf("wrong id")
					},
				},
				&mock.UserService{},
				&mock.OrganizationService{},
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
					DeleteAuthorizationFn: func(ctx context.Context, id platform2.ID) error {
						return &errors.Error{
							Code: errors.ENotFound,
							Msg:  "authorization not found",
						}
					},
				},
				&mock.UserService{},
				&mock.OrganizationService{},
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
			authorizationBackend := NewMockAuthorizationBackend(t)
			authorizationBackend.HTTPErrorHandler = kithttp.NewErrorHandler(zaptest.NewLogger(t))
			authorizationBackend.AuthorizationService = tt.fields.AuthorizationService
			authorizationBackend.UserService = tt.fields.UserService
			authorizationBackend.OrganizationService = tt.fields.OrganizationService
			h := NewAuthorizationHandler(zaptest.NewLogger(t), authorizationBackend)

			r := httptest.NewRequest("GET", "http://any.url", nil)

			r = r.WithContext(context.WithValue(
				context.Background(),
				httprouter.ParamsKey,
				httprouter.Params{
					{
						Key:   "id",
						Value: tt.args.id,
					},
				}))

			w := httptest.NewRecorder()

			h.handleDeleteAuthorization(w, r)

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
				if eq, diff, err := jsonEqual(string(body), tt.wants.body); err != nil {
					t.Errorf("%q, handleDeleteAuthorization(). error unmarshalling json %v", tt.name, err)
				} else if !eq {
					t.Errorf("%q. handleDeleteAuthorization() = ***%s***", tt.name, diff)
				}
			}
		})
	}
}

func initAuthorizationService(f platformtesting.AuthorizationFields, useTokenHashing bool, t *testing.T) (platform.AuthorizationService, string, func()) {
	t.Helper()
	if t.Name() == "TestAuthorizationService_FindAuthorizations/find_authorization_by_token" {
		/*
			TODO(goller): need a secure way to communicate get
			 authorization by token string via headers or something
		*/
		t.Skip("TestAuthorizationService_FindAuthorizations/find_authorization_by_token skipped because user tokens cannot be queried")
	}

	if t.Name() == "TestAuthorizationService_CreateAuthorization/providing_a_non_existing_user_is_invalid" {
		t.Skip("HTTP authorization service does not required a user id on the authentication struct.  We get the user from the session token.")
	}

	ctx := context.Background()

	store := platformtesting.NewTestInmemStore(t)
	tenantStore := tenant.NewStore(store)
	tenantStore.OrgIDGen = f.OrgIDGenerator
	tenantService := tenant.NewService(tenantStore)

	authStore, err := authorization.NewStore(ctx, store, useTokenHashing)
	if err != nil {
		t.Fatal(err)
	}
	authService := authorization.NewService(authStore, tenantService)

	svc := kv.NewService(zaptest.NewLogger(t), store, tenantService)
	svc.IDGenerator = f.IDGenerator
	svc.TokenGenerator = f.TokenGenerator
	svc.TimeGenerator = f.TimeGenerator

	for _, u := range f.Users {
		if err := tenantService.CreateUser(ctx, u); err != nil {
			t.Fatalf("failed to populate users")
		}
	}

	for _, o := range f.Orgs {
		if err := tenantService.CreateOrganization(ctx, o); err != nil {
			t.Fatalf("failed to populate orgs")
		}
	}

	var token string

	for _, a := range f.Authorizations {
		if err := authService.CreateAuthorization(ctx, a); err != nil {
			t.Fatalf("failed to populate authorizations")
		}

		token = a.Token
	}

	mus := &mock.UserService{
		FindUserByIDFn: func(ctx context.Context, id platform2.ID) (*platform.User, error) {
			return &platform.User{}, nil
		},
	}

	authorizationBackend := NewMockAuthorizationBackend(t)
	authorizationBackend.HTTPErrorHandler = kithttp.NewErrorHandler(zaptest.NewLogger(t))
	authorizationBackend.AuthorizationService = authService
	authorizationBackend.UserService = mus
	authorizationBackend.OrganizationService = tenantService
	authorizationBackend.LookupService = &mock.LookupService{
		NameFn: func(ctx context.Context, resource platform.ResourceType, id platform2.ID) (string, error) {
			switch resource {
			case platform.BucketsResourceType:
				return "b1", nil
			case platform.OrgsResourceType:
				return "o1", nil
			}
			return "", fmt.Errorf("bad resource type %s", resource)
		},
	}

	authZ := NewAuthorizationHandler(zaptest.NewLogger(t), authorizationBackend)
	authN := NewAuthenticationHandler(zaptest.NewLogger(t), kithttp.NewErrorHandler(zaptest.NewLogger(t)))
	authN.AuthorizationService = authService
	authN.Handler = authZ
	authN.UserService = mus

	server := httptest.NewServer(authN)

	httpClient, err := NewHTTPClient(server.URL, token, false)
	if err != nil {
		t.Fatal(err)
	}

	done := server.Close

	return &AuthorizationService{Client: httpClient}, "", done
}

func runAuthorizationServiceTest(
	name string,
	tf func(init func(platformtesting.AuthorizationFields, *testing.T) (influxdb.AuthorizationService, string, func()), t *testing.T),
	initWithHashing func(f platformtesting.AuthorizationFields, useTokenHashing bool, t *testing.T) (platform.AuthorizationService, string, func()),
	t *testing.T,
) {
	t.Helper()
	for _, useHashedTokens := range []bool{false, true} {
		init := func(f platformtesting.AuthorizationFields, t *testing.T) (platform.AuthorizationService, string, func()) {
			return initWithHashing(f, useHashedTokens, t)
		}
		t.Run(fmt.Sprintf("%s/TokenHashing=%t", name, useHashedTokens), func(t *testing.T) {
			tf(init, t)
		})
	}
}

func TestAuthorizationService_CreateAuthorization(t *testing.T) {
	runAuthorizationServiceTest("TestAuthorizationService_CreateAuthorization", platformtesting.CreateAuthorization, initAuthorizationService, t)
}

func TestAuthorizationService_FindAuthorizationByID(t *testing.T) {
	runAuthorizationServiceTest("TestAuthorizationService_FindAuthorizationByID", platformtesting.FindAuthorizationByID, initAuthorizationService, t)
}

func TestAuthorizationService_FindAuthorizationByToken(t *testing.T) {
	/*
		TODO(goller): need a secure way to communicate get
		authorization by token string via headers or something
	*/
	t.Skip()
	runAuthorizationServiceTest("TestAuthorizationService_FindAuthorizationByToken", platformtesting.FindAuthorizationByToken, initAuthorizationService, t)
}

func TestAuthorizationService_FindAuthorizations(t *testing.T) {
	runAuthorizationServiceTest("TestAuthorizationService_FindAuthorizations", platformtesting.FindAuthorizations, initAuthorizationService, t)
}

func TestAuthorizationService_DeleteAuthorization(t *testing.T) {
	runAuthorizationServiceTest("TestAuthorizationService_DeleteAuthorization", platformtesting.DeleteAuthorization, initAuthorizationService, t)
}

func TestAuthorizationService_UpdateAuthorization(t *testing.T) {
	runAuthorizationServiceTest("TestAuthorizationService_UpdateAuthorization", platformtesting.UpdateAuthorization, initAuthorizationService, t)
}

func MustMarshal(o interface{}) []byte {
	b, _ := json.Marshal(o)
	return b
}
