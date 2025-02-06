package authorization_test

import (
	"context"
	"fmt"
	"reflect"
	"testing"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/authorization"
	"github.com/influxdata/influxdb/v2/inmem"
	"github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/influxdata/influxdb/v2/kv"
	"github.com/influxdata/influxdb/v2/kv/migration/all"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
)

func TestAuth(t *testing.T) {
	setup := func(t *testing.T, store *authorization.Store, tx kv.Tx) {
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
		setup   func(*testing.T, *authorization.Store, kv.Tx)
		update  func(*testing.T, *authorization.Store, kv.Tx)
		results func(*testing.T, bool, *authorization.Store, *authorization.AuthorizationHasher, kv.Tx)
	}{
		{
			name:  "create",
			setup: setup,
			results: func(t *testing.T, useHashedTokens bool, store *authorization.Store, hasher *authorization.AuthorizationHasher, tx kv.Tx) {
				auths, err := store.ListAuthorizations(context.Background(), tx, influxdb.AuthorizationFilter{})
				if err != nil {
					t.Fatal(err)
				}

				if len(auths) != 10 {
					t.Fatalf("expected 10 authorizations, got: %d", len(auths))
				}

				expected := []*influxdb.Authorization{}
				for i := 1; i <= 10; i++ {
					a := &influxdb.Authorization{
						ID:     platform.ID(i),
						Token:  fmt.Sprintf("randomtoken%d", i),
						OrgID:  platform.ID(i),
						UserID: platform.ID(i),
						Status: "active",
					}
					if useHashedTokens {
						hashedToken, err := hasher.Hash(a.Token)
						require.NoError(t, err)
						a.HashedToken = hashedToken
						a.Token = ""
					}
					expected = append(expected, a)
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
			name:  "read",
			setup: setup,
			results: func(t *testing.T, useHashedTokens bool, store *authorization.Store, hasher *authorization.AuthorizationHasher, tx kv.Tx) {
				for i := 1; i <= 10; i++ {
					expectedAuth := &influxdb.Authorization{
						ID:     platform.ID(i),
						Token:  fmt.Sprintf("randomtoken%d", i),
						OrgID:  platform.ID(i),
						UserID: platform.ID(i),
						Status: influxdb.Active,
					}
					if useHashedTokens {
						hashedToken, err := hasher.Hash(expectedAuth.Token)
						require.NoError(t, err)
						expectedAuth.HashedToken = hashedToken
						expectedAuth.Token = ""
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
			update: func(t *testing.T, store *authorization.Store, tx kv.Tx) {
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
			results: func(t *testing.T, useHashedTokens bool, store *authorization.Store, hasher *authorization.AuthorizationHasher, tx kv.Tx) {

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
					if useHashedTokens {
						hashedToken, err := hasher.Hash(expectedAuth.Token)
						require.NoError(t, err)
						expectedAuth.HashedToken = hashedToken
						expectedAuth.Token = ""
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
			update: func(t *testing.T, store *authorization.Store, tx kv.Tx) {
				for i := 1; i <= 10; i++ {
					err := store.DeleteAuthorization(context.Background(), tx, platform.ID(i))
					if err != nil {
						t.Fatalf("Could not delete authorization [Error]: %v", err)
					}
				}
			},
			results: func(t *testing.T, useHashedTokens bool, store *authorization.Store, hasher *authorization.AuthorizationHasher, tx kv.Tx) {
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
		for _, useHashedTokens := range []bool{false, true} {

			t.Run(testScenario.name, func(t *testing.T) {
				store := inmem.NewKVStore()
				if err := all.Up(context.Background(), zaptest.NewLogger(t), store); err != nil {
					t.Fatal(err)
				}

				hasher, err := authorization.NewAuthorizationHasher()
				require.NoError(t, err)

				ts, err := authorization.NewStore(context.Background(), store, useHashedTokens, authorization.WithAuthorizationHasher(hasher))
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
						testScenario.results(t, useHashedTokens, ts, hasher, tx)
						return nil
					})

					if err != nil {
						t.Fatal(err)
					}
				}
			})
		}
	}
}
