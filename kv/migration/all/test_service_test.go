package all

import (
	"context"
	"fmt"
	"testing"

	"github.com/benbjohnson/clock"
	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/authorization"
	"github.com/influxdata/influxdb/v2/inmem"
	"github.com/influxdata/influxdb/v2/kv"
	"github.com/influxdata/influxdb/v2/kv/migration"
	"github.com/influxdata/influxdb/v2/tenant"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
)

type testService struct {
	Store   kv.SchemaStore
	Service *kv.Service
	Org     influxdb.Organization
	User    influxdb.User
	Auth    influxdb.Authorization
	Clock   clock.Clock
}

func runTestWithTokenHashing(name string, testFunc func(bool, *testing.T), t *testing.T) {
	t.Helper()
	for _, useTokenHashing := range []bool{false, true} {
		t.Run(fmt.Sprintf("%s/TokenHashing=%t", name, useTokenHashing), func(t *testing.T) {
			testFunc(useTokenHashing, t)
		})
	}
}

func newService(t *testing.T, ctx context.Context, endMigration int, useTokenHashing bool) *testService {
	t.Helper()

	var (
		ts = &testService{
			Store: inmem.NewKVStore(),
		}
		logger = zaptest.NewLogger(t)
	)

	// apply migrations up to (but not including) this one
	migrator, err := migration.NewMigrator(logger, ts.Store, Migrations[:endMigration]...)
	if err != nil {
		t.Fatal(err)
	}

	if err := migrator.Up(ctx); err != nil {
		t.Fatal(err)
	}

	store := tenant.NewStore(ts.Store)
	tenantSvc := tenant.NewService(store)

	var ignoreMissingHashIndex bool
	if endMigration <= 21 {
		ignoreMissingHashIndex = true
	}
	missingHashIndexOption := authorization.WithIgnoreMissingHashIndex(ignoreMissingHashIndex)
	authStore, err := authorization.NewStore(ctx, ts.Store, useTokenHashing, missingHashIndexOption)
	if useTokenHashing && ignoreMissingHashIndex {
		require.ErrorIs(t, err, kv.ErrBucketNotFound)
		t.Skipf("migrationLevel=%d with useTokenHashing=%t is not a valid combination", endMigration, useTokenHashing)
	}
	require.NoError(t, err)

	authSvc := authorization.NewService(authStore, tenantSvc)

	ts.Service = kv.NewService(logger, ts.Store, tenantSvc)

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
