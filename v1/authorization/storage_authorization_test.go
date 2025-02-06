package authorization

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/inmem"
	"github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/influxdata/influxdb/v2/kv"
	"github.com/influxdata/influxdb/v2/kv/migration/all"
	"github.com/influxdata/influxdb/v2/pkg/pointer"
	"github.com/influxdata/influxdb/v2/tenant"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
)

func TestAuth(t *testing.T) {
	setup := func(t *testing.T, store *Store, tx kv.Tx) {
		for i := 1; i <= 10; i++ {
			err := store.CreateAuthorization(context.Background(), tx, &influxdb.Authorization{
				ID:     platform.ID(i),
				Token:  fmt.Sprintf("randomtoken%d", i),
				OrgID:  platform.ID(i),
				UserID: platform.ID(i),
				Status: influxdb.Active,
			})

			if err != nil {
				t.Fatal(err)
			}
		}
	}

	tt := []struct {
		name    string
		setup   func(*testing.T, *Store, kv.Tx)
		update  func(*testing.T, *Store, kv.Tx)
		results func(*testing.T, *Store, kv.Tx)
	}{
		{
			name:  "create",
			setup: setup,
			results: func(t *testing.T, store *Store, tx kv.Tx) {
				auths, err := store.ListAuthorizations(context.Background(), tx, influxdb.AuthorizationFilter{})
				if err != nil {
					t.Fatal(err)
				}

				if len(auths) != 10 {
					t.Fatalf("expected 10 authorizations, got: %d", len(auths))
				}

				expected := []*influxdb.Authorization{}
				for i := 1; i <= 10; i++ {
					expected = append(expected, &influxdb.Authorization{
						ID:     platform.ID(i),
						Token:  fmt.Sprintf("randomtoken%d", i),
						OrgID:  platform.ID(i),
						UserID: platform.ID(i),
						Status: "active",
					})
				}
				if !reflect.DeepEqual(auths, expected) {
					t.Fatalf("expected identical authorizations: \n%+v\n%+v", auths, expected)
				}

				// should not be able to create two authorizations with identical tokens
				err = store.CreateAuthorization(context.Background(), tx, &influxdb.Authorization{
					ID:     platform.ID(1),
					Token:  fmt.Sprintf("randomtoken%d", 1),
					OrgID:  platform.ID(1),
					UserID: platform.ID(1),
				})
				if err == nil {
					t.Fatalf("expected to be unable to create authorizations with identical tokens")
				}
			},
		},
		{
			// VALIS: add tests for hashed tokens
			name:  "read",
			setup: setup,
			results: func(t *testing.T, store *Store, tx kv.Tx) {
				for i := 1; i <= 10; i++ {
					expectedAuth := &influxdb.Authorization{
						ID:     platform.ID(i),
						Token:  fmt.Sprintf("randomtoken%d", i),
						OrgID:  platform.ID(i),
						UserID: platform.ID(i),
						Status: influxdb.Active,
					}

					authByID, err := store.GetAuthorizationByID(context.Background(), tx, platform.ID(i))
					if err != nil {
						t.Fatalf("Unexpectedly could not acquire Authorization by ID [Error]: %v", err)
					}

					if !reflect.DeepEqual(authByID, expectedAuth) {
						t.Fatalf("ID TEST: expected identical authorizations:\n[Expected]: %+#v\n[Got]: %+#v", expectedAuth, authByID)
					}

					authByToken, err := store.GetAuthorizationByToken(context.Background(), tx, fmt.Sprintf("randomtoken%d", i))
					if err != nil {
						t.Fatalf("cannot get authorization by Token [Error]: %v", err)
					}

					if !reflect.DeepEqual(authByToken, expectedAuth) {
						t.Fatalf("TOKEN TEST: expected identical authorizations:\n[Expected]: %+#v\n[Got]: %+#v", expectedAuth, authByToken)
					}
				}

			},
		},
		{
			name:  "update",
			setup: setup,
			update: func(t *testing.T, store *Store, tx kv.Tx) {
				for i := 1; i <= 10; i++ {
					auth, err := store.GetAuthorizationByID(context.Background(), tx, platform.ID(i))
					if err != nil {
						t.Fatalf("Could not get authorization [Error]: %v", err)
					}

					auth.Status = influxdb.Inactive

					_, err = store.UpdateAuthorization(context.Background(), tx, platform.ID(i), auth)
					if err != nil {
						t.Fatalf("Could not get updated authorization [Error]: %v", err)
					}
				}
			},
			results: func(t *testing.T, store *Store, tx kv.Tx) {

				for i := 1; i <= 10; i++ {
					auth, err := store.GetAuthorizationByID(context.Background(), tx, platform.ID(i))
					if err != nil {
						t.Fatalf("Could not get authorization [Error]: %v", err)
					}

					expectedAuth := &influxdb.Authorization{
						ID:     platform.ID(i),
						Token:  fmt.Sprintf("randomtoken%d", i),
						OrgID:  platform.ID(i),
						UserID: platform.ID(i),
						Status: influxdb.Inactive,
					}

					if !reflect.DeepEqual(auth, expectedAuth) {
						t.Fatalf("expected identical authorizations:\n[Expected] %+#v\n[Got] %+#v", expectedAuth, auth)
					}
				}
			},
		},
		{
			name:  "delete",
			setup: setup,
			update: func(t *testing.T, store *Store, tx kv.Tx) {
				for i := 1; i <= 10; i++ {
					err := store.DeleteAuthorization(context.Background(), tx, platform.ID(i))
					if err != nil {
						t.Fatalf("Could not delete authorization [Error]: %v", err)
					}
				}
			},
			results: func(t *testing.T, store *Store, tx kv.Tx) {
				for i := 1; i <= 10; i++ {
					_, err := store.GetAuthorizationByID(context.Background(), tx, platform.ID(i))
					if err == nil {
						t.Fatal("Authorization was not deleted correctly")
					}
				}
			},
		},
	}

	for _, testScenario := range tt {
		t.Run(testScenario.name, func(t *testing.T) {
			store := inmem.NewKVStore()
			if err := all.Up(context.Background(), zaptest.NewLogger(t), store); err != nil {
				t.Fatal(err)
			}

			ts, err := NewStore(store)
			if err != nil {
				t.Fatal(err)
			}

			// setup
			if testScenario.setup != nil {
				err := ts.Update(context.Background(), func(tx kv.Tx) error {
					testScenario.setup(t, ts, tx)
					return nil
				})

				if err != nil {
					t.Fatal(err)
				}
			}

			// update
			if testScenario.update != nil {
				err := ts.Update(context.Background(), func(tx kv.Tx) error {
					testScenario.update(t, ts, tx)
					return nil
				})

				if err != nil {
					t.Fatal(err)
				}
			}

			// results
			if testScenario.results != nil {
				err := ts.View(context.Background(), func(tx kv.Tx) error {
					testScenario.results(t, ts, tx)
					return nil
				})

				if err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}

func TestAuthBucketNotExists(t *testing.T) {
	store := inmem.NewKVStore()
	if err := all.Up(context.Background(), zaptest.NewLogger(t), store); err != nil {
		t.Fatal(err)
	}

	ts, err := NewStore(store)
	require.NoError(t, err)

	bucketID := platform.ID(1)
	tenant := tenant.NewStore(store)
	err = tenant.Update(context.Background(), func(tx kv.Tx) error {
		err := tenant.CreateBucket(context.Background(), tx, &influxdb.Bucket{
			ID:    bucketID,
			OrgID: platform.ID(10),
			Name:  "testbucket",
		})
		if err != nil {
			return err
		}

		b, err := tenant.GetBucketByName(context.Background(), tx, platform.ID(10), "testbucket")
		if err != nil {
			return err
		}

		bucketID = b.ID

		return nil
	})
	require.NoError(t, err)

	perm1, err := influxdb.NewPermissionAtID(
		bucketID,
		influxdb.ReadAction,
		influxdb.BucketsResourceType,
		platform.ID(10),
	)
	require.NoError(t, err)

	perm2, err := influxdb.NewPermissionAtID(
		platform.ID(2),
		influxdb.ReadAction,
		influxdb.BucketsResourceType,
		platform.ID(10),
	)
	require.NoError(t, err)

	err = ts.Update(context.Background(), func(tx kv.Tx) error {
		err = ts.CreateAuthorization(context.Background(), tx, &influxdb.Authorization{
			ID:     platform.ID(1),
			Token:  "buckettoken",
			OrgID:  platform.ID(10),
			UserID: platform.ID(4),
			Status: influxdb.Active,
			Permissions: []influxdb.Permission{
				*perm1,
			},
		})

		return err
	})

	require.NoErrorf(t, err, "Authorization creating should have succeeded")

	err = ts.Update(context.Background(), func(tx kv.Tx) error {
		err = ts.CreateAuthorization(context.Background(), tx, &influxdb.Authorization{
			ID:     platform.ID(1),
			Token:  "buckettoken",
			OrgID:  platform.ID(10),
			UserID: platform.ID(4),
			Status: influxdb.Active,
			Permissions: []influxdb.Permission{
				*perm2,
			},
		})

		return err
	})

	if err == nil || !errors.Is(err, ErrBucketNotFound) {
		t.Fatalf("Authorization creating should have failed with ErrBucketNotFound [Error]: %v", err)
	}
}

func Test_filterAuthorizationsFn(t *testing.T) {
	var (
		otherID = platform.ID(999)
	)

	auth := influxdb.Authorization{
		ID:     1000,
		Token:  "foo",
		Status: influxdb.Active,
		OrgID:  2000,
		UserID: 3000,
	}
	tests := []struct {
		name string
		filt influxdb.AuthorizationFilter
		auth influxdb.Authorization
		exp  bool
	}{
		{
			name: "default is true",
			filt: influxdb.AuthorizationFilter{},
			auth: auth,
			exp:  true,
		},
		{
			name: "match id",
			filt: influxdb.AuthorizationFilter{
				ID: &auth.ID,
			},
			auth: auth,
			exp:  true,
		},
		{
			name: "no match id",
			filt: influxdb.AuthorizationFilter{
				ID: &otherID,
			},
			auth: auth,
			exp:  false,
		},
		{
			name: "match token",
			filt: influxdb.AuthorizationFilter{
				Token: &auth.Token,
			},
			auth: auth,
			exp:  true,
		},
		{
			name: "no match token",
			filt: influxdb.AuthorizationFilter{
				Token: pointer.String("2"),
			},
			auth: auth,
			exp:  false,
		},
		{
			name: "match org",
			filt: influxdb.AuthorizationFilter{
				OrgID: &auth.OrgID,
			},
			auth: auth,
			exp:  true,
		},
		{
			name: "no match org",
			filt: influxdb.AuthorizationFilter{
				OrgID: &otherID,
			},
			auth: auth,
			exp:  false,
		},
		{
			name: "match user",
			filt: influxdb.AuthorizationFilter{
				UserID: &auth.UserID,
			},
			auth: auth,
			exp:  true,
		},
		{
			name: "no match user",
			filt: influxdb.AuthorizationFilter{
				UserID: &otherID,
			},
			auth: auth,
			exp:  false,
		},
		{
			name: "match org and user",
			filt: influxdb.AuthorizationFilter{
				OrgID:  &auth.OrgID,
				UserID: &auth.UserID,
			},
			auth: auth,
			exp:  true,
		},
		{
			name: "no match org and user",
			filt: influxdb.AuthorizationFilter{
				OrgID:  &otherID,
				UserID: &auth.UserID,
			},
			auth: auth,
			exp:  false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			pred := filterAuthorizationsFn(tc.filt)
			got := pred(&tc.auth)
			assert.Equal(t, tc.exp, got)
		})
	}
}
