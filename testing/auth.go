package testing

import (
	"bytes"
	"context"
	"sort"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/influxdata/influxdb/v2/kit/platform/errors"
	"github.com/influxdata/influxdb/v2/mock"
)

const (
	authZeroID  = "020f755c3c081000"
	authOneID   = "020f755c3c082000"
	authTwoID   = "020f755c3c082001"
	authThreeID = "020f755c3c082002"
)

var authorizationCmpOptions = cmp.Options{
	cmpopts.EquateEmpty(),
	cmpopts.IgnoreFields(influxdb.Authorization{}, "ID", "Token", "HashedToken", "CreatedAt", "UpdatedAt"),
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

type AuthTestOpts struct {
	WithoutFindByToken bool
}

// WithoutFindByToken allows the Find By Token test case to be skipped when we are testing the http server,
// since finding by token is not supported by the HTTP API
func WithoutFindByToken() AuthTestOpts {
	return AuthTestOpts{
		WithoutFindByToken: true,
	}
}

// AuthorizationFields will include the IDGenerator, and authorizations
type AuthorizationFields struct {
	IDGenerator    platform.IDGenerator
	OrgIDGenerator platform.IDGenerator
	TokenGenerator influxdb.TokenGenerator
	TimeGenerator  influxdb.TimeGenerator
	Authorizations []*influxdb.Authorization
	Users          []*influxdb.User
	Orgs           []*influxdb.Organization
}

// AuthorizationService tests all the service functions.
func AuthorizationService(
	init func(AuthorizationFields, *testing.T) (influxdb.AuthorizationService, string, func()),
	t *testing.T,
	opts ...AuthTestOpts) {
	tests := []struct {
		name string
		fn   func(init func(AuthorizationFields, *testing.T) (influxdb.AuthorizationService, string, func()),
			t *testing.T)
	}{
		{
			name: "CreateAuthorization",
			fn:   CreateAuthorization,
		},
		{
			name: "FindAuthorizationByID",
			fn:   FindAuthorizationByID,
		},
		{
			name: "FindAuthorizationByToken",
			fn:   FindAuthorizationByToken,
		},
		{
			name: "UpdateAuthorization",
			fn:   UpdateAuthorization,
		},
		{
			name: "FindAuthorizations",
			fn:   FindAuthorizations,
		},
		{
			name: "DeleteAuthorization",
			fn:   DeleteAuthorization,
		},
	}
	for _, tt := range tests {
		if tt.name == "FindAuthorizationByToken" && len(opts) > 0 && opts[0].WithoutFindByToken {
			continue
		}
		t.Run(tt.name, func(t *testing.T) {
			tt := tt
			t.Parallel()
			tt.fn(init, t)
		})
	}
}

// CreateAuthorization testing
func CreateAuthorization(
	init func(AuthorizationFields, *testing.T) (influxdb.AuthorizationService, string, func()),
	t *testing.T,
) {
	type args struct {
		authorization *influxdb.Authorization
	}
	type wants struct {
		err            error
		authorizations []*influxdb.Authorization
	}

	tests := []struct {
		name   string
		fields AuthorizationFields
		args   args
		wants  wants
	}{
		{
			name: "basic create authorization",
			fields: AuthorizationFields{
				OrgIDGenerator: mock.NewIncrementingIDGenerator(1),
				IDGenerator:    mock.NewIDGenerator(authTwoID, t),
				TimeGenerator: &mock.TimeGenerator{
					FakeValue: time.Date(2009, time.November, 10, 23, 0, 0, 0, time.UTC),
				},
				TokenGenerator: &mock.TokenGenerator{
					TokenFn: func() (string, error) {
						return "rand", nil
					},
				},
				Users: []*influxdb.User{
					{
						Name: "cooluser",
						ID:   MustIDBase16(userOneID),
					},
				},
				Orgs: []*influxdb.Organization{
					{
						Name: "o1",
					},
				},
				Authorizations: []*influxdb.Authorization{
					{
						ID:          MustIDBase16(authOneID),
						UserID:      MustIDBase16(userOneID),
						OrgID:       idOne,
						Token:       "supersecret",
						Permissions: allUsersPermission(idOne),
						Description: "already existing auth",
					},
				},
			},
			args: args{
				authorization: &influxdb.Authorization{
					OrgID:       idOne,
					UserID:      MustIDBase16(userOneID),
					Permissions: createUsersPermission(idOne),
					Description: "new auth",
				},
			},
			wants: wants{
				authorizations: []*influxdb.Authorization{
					{
						ID:          MustIDBase16(authOneID),
						UserID:      MustIDBase16(userOneID),
						OrgID:       idOne,
						Status:      influxdb.Active,
						Token:       "supersecret",
						Permissions: allUsersPermission(idOne),
						Description: "already existing auth",
					},
					{
						ID:          MustIDBase16(authTwoID),
						UserID:      MustIDBase16(userOneID),
						OrgID:       idOne,
						Token:       "rand",
						Status:      influxdb.Active,
						Permissions: createUsersPermission(idOne),
						Description: "new auth",
						CRUDLog: influxdb.CRUDLog{
							CreatedAt: time.Date(2009, time.November, 10, 23, 0, 0, 0, time.UTC),
							UpdatedAt: time.Date(2009, time.November, 10, 23, 0, 0, 0, time.UTC),
						},
					},
				},
			},
		},
		{
			name: "providing a non existing user is invalid",
			fields: AuthorizationFields{
				OrgIDGenerator: mock.NewIncrementingIDGenerator(1),
				IDGenerator:    mock.NewIDGenerator(authTwoID, t),
				TimeGenerator: &mock.TimeGenerator{
					FakeValue: time.Date(2009, time.November, 10, 23, 0, 0, 0, time.UTC),
				},
				TokenGenerator: &mock.TokenGenerator{
					TokenFn: func() (string, error) {
						return "rand", nil
					},
				},
				Users: []*influxdb.User{
					{
						Name: "cooluser",
						ID:   MustIDBase16(userOneID),
					},
				},
				Orgs: []*influxdb.Organization{
					{
						Name: "o1",
					},
				},
				Authorizations: []*influxdb.Authorization{
					{
						ID:          MustIDBase16(authOneID),
						UserID:      MustIDBase16(userOneID),
						OrgID:       idOne,
						Token:       "supersecret",
						Permissions: allUsersPermission(idOne),
						Description: "already existing auth",
					},
				},
			},
			args: args{
				authorization: &influxdb.Authorization{
					OrgID:       idOne,
					UserID:      MustIDBase16(userTwoID),
					Permissions: createUsersPermission(idOne),
					Description: "auth with non-existent user",
				},
			},
			wants: wants{
				authorizations: []*influxdb.Authorization{
					{
						ID:          MustIDBase16(authOneID),
						UserID:      MustIDBase16(userOneID),
						OrgID:       idOne,
						Status:      influxdb.Active,
						Token:       "supersecret",
						Permissions: allUsersPermission(idOne),
						Description: "already existing auth",
					},
				},
				err: influxdb.ErrUnableToCreateToken,
			},
		},
		{
			name: "providing a non existing org is invalid",
			fields: AuthorizationFields{
				OrgIDGenerator: mock.NewIncrementingIDGenerator(1),
				IDGenerator:    mock.NewIDGenerator(authTwoID, t),
				TimeGenerator: &mock.TimeGenerator{
					FakeValue: time.Date(2009, time.November, 10, 23, 0, 0, 0, time.UTC),
				},
				TokenGenerator: &mock.TokenGenerator{
					TokenFn: func() (string, error) {
						return "rand", nil
					},
				},
				Users: []*influxdb.User{
					{
						Name: "cooluser",
						ID:   MustIDBase16(userOneID),
					},
				},
				Orgs: []*influxdb.Organization{
					{
						Name: "o1",
					},
				},
				Authorizations: []*influxdb.Authorization{
					{
						ID:          MustIDBase16(authOneID),
						UserID:      MustIDBase16(userOneID),
						OrgID:       idOne,
						Token:       "supersecret",
						Permissions: allUsersPermission(idOne),
						Description: "already existing auth",
					},
				},
			},
			args: args{
				authorization: &influxdb.Authorization{
					OrgID:       idTwo,
					UserID:      MustIDBase16(userOneID),
					Permissions: createUsersPermission(idTwo),
					Description: "auth with non-existent org",
				},
			},
			wants: wants{
				authorizations: []*influxdb.Authorization{
					{
						ID:          MustIDBase16(authOneID),
						UserID:      MustIDBase16(userOneID),
						OrgID:       idOne,
						Status:      influxdb.Active,
						Token:       "supersecret",
						Permissions: allUsersPermission(idOne),
						Description: "already existing auth",
					},
				},
				err: influxdb.ErrUnableToCreateToken,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, _, done := init(tt.fields, t)
			defer done()
			ctx := context.Background()
			err := s.CreateAuthorization(ctx, tt.args.authorization)
			if (err != nil) != (tt.wants.err != nil) {
				t.Fatalf("expected error '%v' got '%v'", tt.wants.err, err)
			}

			diffPlatformErrors(tt.name, err, tt.wants.err, false, false, t)

			defer s.DeleteAuthorization(ctx, tt.args.authorization.ID)

			authorizations, _, err := s.FindAuthorizations(ctx, influxdb.AuthorizationFilter{})
			if err != nil {
				t.Fatalf("failed to retrieve authorizations: %v", err)
			}
			if diff := cmp.Diff(authorizations, tt.wants.authorizations, authorizationCmpOptions...); diff != "" {
				t.Errorf("authorizations are different -got/+want\ndiff %s", diff)
			}
		})
	}
}

// FindAuthorizationByID testing
func FindAuthorizationByID(
	init func(AuthorizationFields, *testing.T) (influxdb.AuthorizationService, string, func()),
	t *testing.T,
) {
	type wants struct {
		err            error
		authorizations []*influxdb.Authorization
	}

	tests := []struct {
		name   string
		fields AuthorizationFields
		wants  wants
	}{
		{
			name: "basic find authorization by id",
			fields: AuthorizationFields{
				OrgIDGenerator: mock.NewIncrementingIDGenerator(1),
				Users: []*influxdb.User{
					{
						Name: "cooluser",
						ID:   MustIDBase16(userOneID),
					},
					{
						Name: "regularuser",
						ID:   MustIDBase16(userTwoID),
					},
				},
				Authorizations: []*influxdb.Authorization{
					{
						ID:          MustIDBase16(authOneID),
						UserID:      MustIDBase16(userOneID),
						OrgID:       idOne,
						Token:       "rand1",
						Permissions: allUsersPermission(idOne),
					},
					{
						ID:          MustIDBase16(authTwoID),
						UserID:      MustIDBase16(userTwoID),
						OrgID:       idOne,
						Token:       "rand2",
						Permissions: createUsersPermission(idOne),
					},
				},
				Orgs: []*influxdb.Organization{
					{
						// ID(1)
						Name: "o1",
					},
				},
			},
			wants: wants{
				authorizations: []*influxdb.Authorization{
					{
						ID:          MustIDBase16(authOneID),
						UserID:      MustIDBase16(userOneID),
						OrgID:       idOne,
						Token:       "rand1",
						Status:      "active",
						Permissions: allUsersPermission(idOne),
					},
					{
						ID:          MustIDBase16(authTwoID),
						UserID:      MustIDBase16(userTwoID),
						OrgID:       idOne,
						Token:       "rand2",
						Status:      "active",
						Permissions: createUsersPermission(idOne),
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, _, done := init(tt.fields, t)
			defer done()
			ctx := context.Background()

			for i := range tt.fields.Authorizations {
				authorization, err := s.FindAuthorizationByID(ctx, tt.fields.Authorizations[i].ID)
				diffPlatformErrors(tt.name, err, tt.wants.err, false, false, t)

				if diff := cmp.Diff(authorization, tt.wants.authorizations[i], authorizationCmpOptions...); diff != "" {
					t.Errorf("authorization is different -got/+want\ndiff %s", diff)
				}
			}

		})
	}
}

func stringPtr(s string) *string {
	return &s
}

// UpdateAuthorization testing
func UpdateAuthorization(
	init func(AuthorizationFields, *testing.T) (influxdb.AuthorizationService, string, func()),
	t *testing.T,
) {
	type args struct {
		id  platform.ID
		upd *influxdb.AuthorizationUpdate
	}
	type wants struct {
		err           error
		authorization *influxdb.Authorization
	}
	tests := []struct {
		name   string
		fields AuthorizationFields
		args   args
		wants  wants
	}{
		{
			name: "regular update",
			fields: AuthorizationFields{
				OrgIDGenerator: mock.NewIncrementingIDGenerator(1),
				TimeGenerator: &mock.TimeGenerator{
					FakeValue: time.Date(2009, time.November, 10, 23, 0, 0, 0, time.UTC),
				},
				Users: []*influxdb.User{
					{
						Name: "cooluser",
						ID:   MustIDBase16(userOneID),
					},
					{
						Name: "regularuser",
						ID:   MustIDBase16(userTwoID),
					},
				},
				Orgs: []*influxdb.Organization{
					{
						Name: "o1",
					},
					{
						Name: "o2",
					},
				},
				Authorizations: []*influxdb.Authorization{
					{
						ID:          MustIDBase16(authOneID),
						UserID:      MustIDBase16(userOneID),
						Token:       "rand1",
						Status:      influxdb.Inactive,
						OrgID:       idTwo,
						Permissions: allUsersPermission(idTwo),
					},
					{
						ID:          MustIDBase16(authZeroID),
						UserID:      MustIDBase16(userOneID),
						Token:       "rand0",
						OrgID:       idOne,
						Permissions: allUsersPermission(idOne),
					},
					{
						ID:          MustIDBase16(authTwoID),
						UserID:      MustIDBase16(userTwoID),
						OrgID:       idOne,
						Token:       "rand2",
						Permissions: createUsersPermission(idOne),
					},
					{
						ID:          MustIDBase16(authThreeID),
						UserID:      MustIDBase16(userOneID),
						OrgID:       idOne,
						Token:       "rand3",
						Permissions: allUsersPermission(idOne),
					},
				},
			},
			args: args{
				id: MustIDBase16(authTwoID),
				upd: &influxdb.AuthorizationUpdate{
					Status:      influxdb.Inactive.Ptr(),
					Description: stringPtr("desc1"),
				},
			},
			wants: wants{
				authorization: &influxdb.Authorization{
					ID:          MustIDBase16(authTwoID),
					UserID:      MustIDBase16(userTwoID),
					OrgID:       idOne,
					Token:       "rand2",
					Permissions: createUsersPermission(idOne),
					Status:      influxdb.Inactive,
					Description: "desc1",
					CRUDLog: influxdb.CRUDLog{
						UpdatedAt: time.Date(2009, time.November, 10, 23, 0, 0, 0, time.UTC),
					},
				},
			},
		},
		{
			name: "update with id not found",
			fields: AuthorizationFields{
				OrgIDGenerator: mock.NewIncrementingIDGenerator(1),
				Users: []*influxdb.User{
					{
						Name: "cooluser",
						ID:   MustIDBase16(userOneID),
					},
					{
						Name: "regularuser",
						ID:   MustIDBase16(userTwoID),
					},
				},
				Orgs: []*influxdb.Organization{
					{
						Name: "o1",
					},
					{
						Name: "o2",
					},
				},
				Authorizations: []*influxdb.Authorization{
					{
						ID:          MustIDBase16(authOneID),
						UserID:      MustIDBase16(userOneID),
						Token:       "rand1",
						Status:      influxdb.Inactive,
						OrgID:       idTwo,
						Permissions: allUsersPermission(idTwo),
					},
					{
						ID:          MustIDBase16(authZeroID),
						UserID:      MustIDBase16(userOneID),
						Token:       "rand0",
						OrgID:       idOne,
						Permissions: allUsersPermission(idOne),
					},
					{
						ID:          MustIDBase16(authTwoID),
						UserID:      MustIDBase16(userTwoID),
						OrgID:       idOne,
						Token:       "rand2",
						Permissions: createUsersPermission(idOne),
					},
				},
			},
			args: args{
				id: MustIDBase16(authThreeID),
				upd: &influxdb.AuthorizationUpdate{
					Status: influxdb.Inactive.Ptr(),
				},
			},
			wants: wants{
				err: &errors.Error{
					Code: errors.ENotFound,
					Op:   influxdb.OpUpdateAuthorization,
					Msg:  "authorization not found",
				},
			},
		},
		{
			name: "update with unknown status",
			fields: AuthorizationFields{
				OrgIDGenerator: mock.NewIncrementingIDGenerator(1),
				TimeGenerator: &mock.TimeGenerator{
					FakeValue: time.Date(2009, time.November, 10, 23, 0, 0, 0, time.UTC),
				},
				Users: []*influxdb.User{
					{
						Name: "cooluser",
						ID:   MustIDBase16(userOneID),
					},
					{
						Name: "regularuser",
						ID:   MustIDBase16(userTwoID),
					},
				},
				Orgs: []*influxdb.Organization{
					{
						Name: "o1",
					},
					{
						Name: "o2",
					},
				},
				Authorizations: []*influxdb.Authorization{
					{
						ID:          MustIDBase16(authOneID),
						UserID:      MustIDBase16(userOneID),
						Token:       "rand1",
						Status:      influxdb.Inactive,
						OrgID:       idTwo,
						Permissions: allUsersPermission(idTwo),
					},
					{
						ID:          MustIDBase16(authZeroID),
						UserID:      MustIDBase16(userOneID),
						Token:       "rand0",
						OrgID:       idOne,
						Permissions: allUsersPermission(idOne),
					},
					{
						ID:          MustIDBase16(authTwoID),
						UserID:      MustIDBase16(userTwoID),
						OrgID:       idOne,
						Token:       "rand2",
						Permissions: createUsersPermission(idOne),
					},
					{
						ID:          MustIDBase16(authThreeID),
						UserID:      MustIDBase16(userOneID),
						OrgID:       idOne,
						Token:       "rand3",
						Permissions: allUsersPermission(idOne),
					},
				},
			},
			args: args{
				id: MustIDBase16(authTwoID),
				upd: &influxdb.AuthorizationUpdate{
					Status: influxdb.Status("unknown").Ptr(),
				},
			},
			wants: wants{
				err: &errors.Error{
					Code: errors.EInvalid,
					Op:   influxdb.OpUpdateAuthorization,
					Msg:  "unknown authorization status",
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, _, done := init(tt.fields, t)
			defer done()
			ctx := context.Background()

			updatedAuth, err := s.UpdateAuthorization(ctx, tt.args.id, tt.args.upd)
			diffPlatformErrors(tt.name, err, tt.wants.err, false, false, t)

			if tt.wants.err == nil {
				authorization, err := s.FindAuthorizationByID(ctx, tt.args.id)
				if err != nil {
					t.Errorf("%s failed, got error %s", tt.name, err.Error())
				}
				if diff := cmp.Diff(authorization, tt.wants.authorization, authorizationCmpOptions...); diff != "" {
					t.Errorf("authorization is different -got/+want\ndiff %s", diff)
				}
				if diff := cmp.Diff(authorization, updatedAuth, authorizationCmpOptions...); diff != "" {
					t.Errorf("authorization is different -got/+want\ndiff %s", diff)
				}
			}
		})
	}
}

// FindAuthorizationByToken testing
func FindAuthorizationByToken(
	init func(AuthorizationFields, *testing.T) (influxdb.AuthorizationService, string, func()),
	t *testing.T,
) {
	type args struct {
		token string
	}
	type wants struct {
		err           error
		authorization *influxdb.Authorization
	}

	// VALIS: Add tests to make sure look-up by hashed token does /not/ work
	tests := []struct {
		name   string
		fields AuthorizationFields
		args   args
		wants  wants
	}{
		{
			name: "basic find authorization by token",
			fields: AuthorizationFields{
				OrgIDGenerator: mock.NewIncrementingIDGenerator(1),
				Users: []*influxdb.User{
					{
						Name: "cooluser",
						ID:   MustIDBase16(userOneID),
					},
					{
						Name: "regularuser",
						ID:   MustIDBase16(userTwoID),
					},
				},
				Orgs: []*influxdb.Organization{
					{
						Name: "o1",
					},
					{
						Name: "o2",
					},
				},
				Authorizations: []*influxdb.Authorization{
					{
						ID:          MustIDBase16(authOneID),
						UserID:      MustIDBase16(userOneID),
						Token:       "rand1",
						Status:      influxdb.Inactive,
						OrgID:       idTwo,
						Permissions: allUsersPermission(idTwo),
					},
					{
						ID:          MustIDBase16(authZeroID),
						UserID:      MustIDBase16(userOneID),
						Token:       "rand0",
						OrgID:       idOne,
						Permissions: allUsersPermission(idOne),
					},
					{
						ID:          MustIDBase16(authTwoID),
						UserID:      MustIDBase16(userTwoID),
						OrgID:       idOne,
						Token:       "rand2",
						Permissions: createUsersPermission(idOne),
					},
					{
						ID:          MustIDBase16(authThreeID),
						UserID:      MustIDBase16(userOneID),
						OrgID:       idOne,
						Token:       "rand3",
						Permissions: allUsersPermission(idOne),
					},
				},
			},
			args: args{
				token: "rand1",
			},
			wants: wants{
				authorization: &influxdb.Authorization{
					ID:          MustIDBase16(authOneID),
					UserID:      MustIDBase16(userOneID),
					OrgID:       idTwo,
					Status:      influxdb.Inactive,
					Token:       "rand1",
					Permissions: allUsersPermission(idTwo),
				},
			},
		},
		{
			name: "find authorization by token",
			fields: AuthorizationFields{
				OrgIDGenerator: mock.NewIncrementingIDGenerator(1),
				Users: []*influxdb.User{
					{
						Name: "cooluser",
						ID:   MustIDBase16(userOneID),
					},
					{
						Name: "regularuser",
						ID:   MustIDBase16(userTwoID),
					},
				},
				Orgs: []*influxdb.Organization{
					{
						Name: "o1",
					},
				},
				Authorizations: []*influxdb.Authorization{
					{
						ID:          MustIDBase16(authZeroID),
						UserID:      MustIDBase16(userOneID),
						OrgID:       idOne,
						Token:       "rand1",
						Permissions: deleteUsersPermission(idOne),
					},
					{
						ID:          MustIDBase16(authTwoID),
						UserID:      MustIDBase16(userTwoID),
						OrgID:       idOne,
						Token:       "rand2",
						Permissions: createUsersPermission(idOne),
					},
					{
						ID:          MustIDBase16(authOneID),
						UserID:      MustIDBase16(userOneID),
						OrgID:       idOne,
						Token:       "rand3",
						Permissions: allUsersPermission(idOne),
					},
					{
						ID:          MustIDBase16(authThreeID),
						UserID:      MustIDBase16(userOneID),
						OrgID:       idOne,
						Token:       "rand4",
						Permissions: deleteUsersPermission(idOne),
					},
				},
			},
			args: args{
				token: "rand2",
			},
			wants: wants{
				authorization: &influxdb.Authorization{
					ID:          MustIDBase16(authTwoID),
					UserID:      MustIDBase16(userTwoID),
					OrgID:       idOne,
					Token:       "rand2",
					Status:      influxdb.Active,
					Permissions: createUsersPermission(idOne),
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, _, done := init(tt.fields, t)
			defer done()
			ctx := context.Background()

			authorization, err := s.FindAuthorizationByToken(ctx, tt.args.token)
			diffPlatformErrors(tt.name, err, tt.wants.err, false, false, t)

			if diff := cmp.Diff(authorization, tt.wants.authorization, authorizationCmpOptions...); diff != "" {
				t.Errorf("authorization is different -got/+want\ndiff %s", diff)
			}
		})
	}
}

// FindAuthorizations testing
func FindAuthorizations(
	init func(AuthorizationFields, *testing.T) (influxdb.AuthorizationService, string, func()),
	t *testing.T,
) {
	type args struct {
		ID     platform.ID
		UserID platform.ID
		OrgID  platform.ID
		token  string
	}

	// VALIS: Do we need tests that set HashedToken, or tests with Token and HashedToken set?
	type wants struct {
		authorizations []*influxdb.Authorization
		err            error
	}
	tests := []struct {
		name   string
		fields AuthorizationFields
		args   args
		wants  wants
	}{
		{
			name: "find all authorizations",
			fields: AuthorizationFields{
				OrgIDGenerator: mock.NewIncrementingIDGenerator(1),
				Users: []*influxdb.User{
					{
						Name: "cooluser",
						ID:   MustIDBase16(userOneID),
					},
					{
						Name: "regularuser",
						ID:   MustIDBase16(userTwoID),
					},
				},
				Orgs: []*influxdb.Organization{
					{
						Name: "o1",
					},
				},
				Authorizations: []*influxdb.Authorization{
					{
						ID:          MustIDBase16(authOneID),
						UserID:      MustIDBase16(userOneID),
						OrgID:       idOne,
						Token:       "rand1",
						Permissions: allUsersPermission(idOne),
					},
					{
						ID:          MustIDBase16(authTwoID),
						UserID:      MustIDBase16(userTwoID),
						OrgID:       idOne,
						Token:       "rand2",
						Permissions: createUsersPermission(idOne),
					},
				},
			},
			args: args{},
			wants: wants{
				authorizations: []*influxdb.Authorization{
					{
						ID:          MustIDBase16(authOneID),
						UserID:      MustIDBase16(userOneID),
						OrgID:       idOne,
						Token:       "rand1",
						Status:      influxdb.Active,
						Permissions: allUsersPermission(idOne),
					},
					{
						ID:          MustIDBase16(authTwoID),
						UserID:      MustIDBase16(userTwoID),
						OrgID:       idOne,
						Token:       "rand2",
						Status:      influxdb.Active,
						Permissions: createUsersPermission(idOne),
					},
				},
			},
		},
		{
			name: "find authorization by user id",
			fields: AuthorizationFields{
				OrgIDGenerator: mock.NewIncrementingIDGenerator(1),
				Users: []*influxdb.User{
					{
						Name: "cooluser",
						ID:   MustIDBase16(userOneID),
					},
					{
						Name: "regularuser",
						ID:   MustIDBase16(userTwoID),
					},
				},
				Orgs: []*influxdb.Organization{
					{
						Name: "o1",
					},
				},
				Authorizations: []*influxdb.Authorization{
					{
						ID:          MustIDBase16(authOneID),
						UserID:      MustIDBase16(userOneID),
						OrgID:       idOne,
						Token:       "rand1",
						Status:      influxdb.Active,
						Permissions: allUsersPermission(idOne),
					},
					{
						ID:          MustIDBase16(authTwoID),
						UserID:      MustIDBase16(userTwoID),
						OrgID:       idOne,
						Token:       "rand2",
						Permissions: createUsersPermission(idOne),
					},
					{
						ID:          MustIDBase16(authThreeID),
						UserID:      MustIDBase16(userOneID),
						OrgID:       idOne,
						Token:       "rand3",
						Permissions: deleteUsersPermission(idOne),
					},
				},
			},
			args: args{
				UserID: MustIDBase16(userOneID),
			},
			wants: wants{
				authorizations: []*influxdb.Authorization{
					{
						ID:          MustIDBase16(authOneID),
						UserID:      MustIDBase16(userOneID),
						OrgID:       idOne,
						Status:      influxdb.Active,
						Token:       "rand1",
						Permissions: allUsersPermission(idOne),
					},
					{
						ID:          MustIDBase16(authThreeID),
						UserID:      MustIDBase16(userOneID),
						OrgID:       idOne,
						Status:      influxdb.Active,
						Token:       "rand3",
						Permissions: deleteUsersPermission(idOne),
					},
				},
			},
		},
		{
			name: "find authorization by org id",
			fields: AuthorizationFields{
				OrgIDGenerator: mock.NewIncrementingIDGenerator(1),
				Users: []*influxdb.User{
					{
						Name: "cooluser",
						ID:   MustIDBase16(userOneID),
					},
				},
				Orgs: []*influxdb.Organization{
					{
						Name: "o1",
					},
					{
						Name: "o2",
					},
				},
				Authorizations: []*influxdb.Authorization{
					{
						ID:          MustIDBase16(authOneID),
						UserID:      MustIDBase16(userOneID),
						OrgID:       idOne,
						Status:      influxdb.Active,
						Token:       "rand1",
						Permissions: createUsersPermission(idOne),
					},
					{
						ID:          MustIDBase16(authTwoID),
						UserID:      MustIDBase16(userOneID),
						OrgID:       idOne,
						Status:      influxdb.Active,
						Token:       "rand2",
						Permissions: deleteUsersPermission(idOne),
					},
					{
						ID:          MustIDBase16(authThreeID),
						UserID:      MustIDBase16(userOneID),
						OrgID:       idTwo,
						Status:      influxdb.Active,
						Token:       "rand3",
						Permissions: allUsersPermission(idTwo),
					},
				},
			},
			args: args{
				OrgID: idOne,
			},
			wants: wants{
				authorizations: []*influxdb.Authorization{
					{
						ID:          MustIDBase16(authOneID),
						UserID:      MustIDBase16(userOneID),
						OrgID:       idOne,
						Status:      influxdb.Active,
						Token:       "rand1",
						Permissions: createUsersPermission(idOne),
					},
					{
						ID:          MustIDBase16(authTwoID),
						UserID:      MustIDBase16(userOneID),
						OrgID:       idOne,
						Status:      influxdb.Active,
						Token:       "rand2",
						Permissions: deleteUsersPermission(idOne),
					},
				},
			},
		},
		{
			name: "find authorization by org id and user id",
			fields: AuthorizationFields{
				OrgIDGenerator: mock.NewIncrementingIDGenerator(1),
				Users: []*influxdb.User{
					{
						Name: "cooluser",
						ID:   MustIDBase16(userOneID),
					},
					{
						Name: "regularuser",
						ID:   MustIDBase16(userTwoID),
					},
				},
				Orgs: []*influxdb.Organization{
					{
						Name: "o1",
					},
					{
						Name: "o2",
						ID:   idTwo,
					},
				},
				Authorizations: []*influxdb.Authorization{
					{
						ID:          MustIDBase16(authOneID),
						UserID:      MustIDBase16(userOneID),
						OrgID:       idOne,
						Status:      influxdb.Active,
						Token:       "rand1",
						Permissions: allUsersPermission(idOne),
					},
					{
						ID:          MustIDBase16(authTwoID),
						UserID:      MustIDBase16(userOneID),
						OrgID:       idTwo,
						Status:      influxdb.Active,
						Token:       "rand2",
						Permissions: allUsersPermission(idTwo),
					},
					{
						ID:          MustIDBase16(authThreeID),
						UserID:      MustIDBase16(userTwoID),
						OrgID:       idOne,
						Status:      influxdb.Active,
						Token:       "rand3",
						Permissions: allUsersPermission(idOne),
					},
					{
						ID:          MustIDBase16(authThreeID),
						UserID:      MustIDBase16(userTwoID),
						OrgID:       idTwo,
						Status:      influxdb.Active,
						Token:       "rand4",
						Permissions: allUsersPermission(idTwo),
					},
				},
			},
			args: args{
				UserID: MustIDBase16(userOneID),
				OrgID:  idTwo,
			},
			wants: wants{
				authorizations: []*influxdb.Authorization{
					{
						ID:          MustIDBase16(authTwoID),
						UserID:      MustIDBase16(userOneID),
						OrgID:       idTwo,
						Status:      influxdb.Active,
						Token:       "rand2",
						Permissions: allUsersPermission(idTwo),
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, _, done := init(tt.fields, t)
			defer done()
			ctx := context.Background()

			filter := influxdb.AuthorizationFilter{}
			if tt.args.ID.Valid() {
				filter.ID = &tt.args.ID
			}
			if tt.args.UserID.Valid() {
				filter.UserID = &tt.args.UserID
			}
			if tt.args.OrgID.Valid() {
				filter.OrgID = &tt.args.OrgID
			}
			if tt.args.token != "" {
				filter.Token = &tt.args.token
			}

			authorizations, _, err := s.FindAuthorizations(ctx, filter)
			diffPlatformErrors(tt.name, err, tt.wants.err, false, false, t)
			if diff := cmp.Diff(authorizations, tt.wants.authorizations, authorizationCmpOptions...); diff != "" {
				t.Errorf("authorizations are different -got/+want\ndiff %s", diff)
			}
		})
	}
}

// DeleteAuthorization testing
func DeleteAuthorization(
	init func(AuthorizationFields, *testing.T) (influxdb.AuthorizationService, string, func()),
	t *testing.T,
) {
	type args struct {
		ID platform.ID
	}
	type wants struct {
		err            error
		authorizations []*influxdb.Authorization
	}

	tests := []struct {
		name   string
		fields AuthorizationFields
		args   args
		wants  wants
	}{
		{
			name: "delete authorizations using exist id",
			fields: AuthorizationFields{
				OrgIDGenerator: mock.NewIncrementingIDGenerator(1),
				Users: []*influxdb.User{
					{
						Name: "cooluser",
						ID:   MustIDBase16(userOneID),
					},
					{
						Name: "regularuser",
						ID:   MustIDBase16(userTwoID),
					},
				},
				Orgs: []*influxdb.Organization{
					{
						Name: "o1",
					},
				},
				Authorizations: []*influxdb.Authorization{
					{
						ID:          MustIDBase16(authOneID),
						UserID:      MustIDBase16(userOneID),
						OrgID:       idOne,
						Token:       "rand1",
						Permissions: allUsersPermission(idOne),
					},
					{
						ID:          MustIDBase16(authTwoID),
						UserID:      MustIDBase16(userTwoID),
						OrgID:       idOne,
						Token:       "rand2",
						Permissions: createUsersPermission(idOne),
					},
				},
			},
			args: args{
				ID: MustIDBase16(authOneID),
			},
			wants: wants{
				authorizations: []*influxdb.Authorization{
					{
						ID:          MustIDBase16(authTwoID),
						UserID:      MustIDBase16(userTwoID),
						OrgID:       idOne,
						Status:      influxdb.Active,
						Token:       "rand2",
						Permissions: createUsersPermission(idOne),
					},
				},
			},
		},
		{
			name: "delete authorizations using id that does not exist",
			fields: AuthorizationFields{
				OrgIDGenerator: mock.NewIncrementingIDGenerator(1),
				Users: []*influxdb.User{
					{
						Name: "cooluser",
						ID:   MustIDBase16(userOneID),
					},
					{
						Name: "regularuser",
						ID:   MustIDBase16(userTwoID),
					},
				},
				Orgs: []*influxdb.Organization{
					{
						Name: "o1",
					},
				},
				Authorizations: []*influxdb.Authorization{
					{
						ID:          MustIDBase16(authOneID),
						UserID:      MustIDBase16(userOneID),
						OrgID:       idOne,
						Token:       "rand1",
						Permissions: allUsersPermission(idOne),
					},
					{
						ID:          MustIDBase16(authTwoID),
						OrgID:       idOne,
						UserID:      MustIDBase16(userTwoID),
						Token:       "rand2",
						Permissions: createUsersPermission(idOne),
					},
				},
			},
			args: args{
				ID: MustIDBase16(authThreeID),
			},
			wants: wants{
				err: &errors.Error{
					Code: errors.ENotFound,
					Msg:  "authorization not found",
					Op:   influxdb.OpDeleteAuthorization,
				},
				authorizations: []*influxdb.Authorization{
					{
						ID:          MustIDBase16(authOneID),
						UserID:      MustIDBase16(userOneID),
						Token:       "rand1",
						Status:      influxdb.Active,
						OrgID:       idOne,
						Permissions: allUsersPermission(idOne),
					},
					{
						ID:          MustIDBase16(authTwoID),
						UserID:      MustIDBase16(userTwoID),
						OrgID:       idOne,
						Token:       "rand2",
						Status:      influxdb.Active,
						Permissions: createUsersPermission(idOne),
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, _, done := init(tt.fields, t)
			defer done()
			ctx := context.Background()
			err := s.DeleteAuthorization(ctx, tt.args.ID)
			diffPlatformErrors(tt.name, err, tt.wants.err, false, false, t)

			filter := influxdb.AuthorizationFilter{}
			authorizations, _, err := s.FindAuthorizations(ctx, filter)
			if err != nil {
				t.Fatalf("failed to retrieve authorizations: %v", err)
			}
			if diff := cmp.Diff(authorizations, tt.wants.authorizations, authorizationCmpOptions...); diff != "" {
				t.Errorf("authorizations are different -got/+want\ndiff %s", diff)
			}
		})
	}
}

func allUsersPermission(orgID platform.ID) []influxdb.Permission {
	return []influxdb.Permission{
		{Action: influxdb.WriteAction, Resource: influxdb.Resource{Type: influxdb.UsersResourceType, OrgID: &orgID}},
		{Action: influxdb.ReadAction, Resource: influxdb.Resource{Type: influxdb.UsersResourceType, OrgID: &orgID}},
	}
}

func createUsersPermission(orgID platform.ID) []influxdb.Permission {
	return []influxdb.Permission{
		{Action: influxdb.WriteAction, Resource: influxdb.Resource{Type: influxdb.UsersResourceType, OrgID: &orgID}},
	}
}

func deleteUsersPermission(orgID platform.ID) []influxdb.Permission {
	return []influxdb.Permission{
		{Action: influxdb.WriteAction, Resource: influxdb.Resource{Type: influxdb.UsersResourceType, OrgID: &orgID}},
	}
}
