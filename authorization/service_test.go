package authorization_test

import (
	"context"
	"testing"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/authorization"
	"github.com/influxdata/influxdb/v2/kv"
	"github.com/influxdata/influxdb/v2/tenant"
	influxdbtesting "github.com/influxdata/influxdb/v2/testing"
)

func initBoltAuthService(f influxdbtesting.AuthorizationFields, useHashedTokens bool, t *testing.T) (influxdb.AuthorizationService, string, func()) {
	s, closeBolt := influxdbtesting.NewTestBoltStore(t)
	svc, closeSvc := initAuthService(s, f, useHashedTokens, t)
	return svc, "service_auth", func() {
		closeSvc()
		closeBolt()
	}
}

func initAuthService(s kv.Store, f influxdbtesting.AuthorizationFields, useHashedTokens bool, t *testing.T) (influxdb.AuthorizationService, func()) {
	st := tenant.NewStore(s)
	if f.OrgIDGenerator != nil {
		st.OrgIDGen = f.OrgIDGenerator
	}

	ts := tenant.NewService(st)

	storage, err := authorization.NewStore(context.Background(), s, useHashedTokens)
	if err != nil {
		t.Fatal(err)
	}

	svc := authorization.NewService(storage, ts)

	for _, u := range f.Users {
		if err := ts.CreateUser(context.Background(), u); err != nil {
			t.Fatalf("error populating users: %v", err)
		}
	}

	for _, o := range f.Orgs {
		if err := ts.CreateOrganization(context.Background(), o); err != nil {
			t.Fatalf("failed to populate organizations: %s", err)
		}
	}

	for _, m := range f.Authorizations {
		if err := svc.CreateAuthorization(context.Background(), m); err != nil {
			t.Fatalf("failed to populate authorizations: %v", err)
		}
	}

	return svc, func() {
		for _, m := range f.Authorizations {
			if err := svc.DeleteAuthorization(context.Background(), m.ID); err != nil {
				t.Logf("failed to remove authorization token: %v", err)
			}
		}
	}
}

func TestBoltAuthService(t *testing.T) {
	t.Parallel()
	for _, useHashedTokens := range []bool{true} {
		init := func(f influxdbtesting.AuthorizationFields, t *testing.T) (influxdb.AuthorizationService, string, func()) {
			return initBoltAuthService(f, useHashedTokens, t)
		}
		influxdbtesting.AuthorizationService(init, t)
	}
}
