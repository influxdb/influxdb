package authorization_test

import (
	"context"
	"fmt"
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
	checkIndexCounts := func(t *testing.T, tx kv.Tx, expAuthIndexCount, expHashedAuthIndexCount int) {
		t.Helper()

		const (
			authIndexName       = "authorizationindexv1"
			hashedAuthIndexName = "authorizationhashedindexv1"
		)

		indexCount := make(map[string]int)
		for _, indexName := range []string{authIndexName, hashedAuthIndexName} {
			index, err := tx.Bucket([]byte(indexName))
			require.NoError(t, err)
			cur, err := index.Cursor()
			require.NoError(t, err)
			for k, _ := cur.First(); k != nil; k, _ = cur.Next() {
				indexCount[indexName]++
			}
		}

		require.Equal(t, expAuthIndexCount, indexCount[authIndexName])
		require.Equal(t, expHashedAuthIndexCount, indexCount[hashedAuthIndexName])
	}

	setup := func(t *testing.T, useHashedTokens bool, store *authorization.Store, hasher *authorization.AuthorizationHasher, tx kv.Tx) {
		for i := 1; i <= 10; i++ {
			err := store.CreateAuthorization(context.Background(), tx, &influxdb.Authorization{
				ID:     platform.ID(i),
				Token:  fmt.Sprintf("randomtoken%d", i),
				OrgID:  platform.ID(i),
				UserID: platform.ID(i),
				Status: influxdb.Active,
			})
			require.NoError(t, err)
		}

		// Perform sanity checks on Token vs HashedToken and indices.
		for i := 1; i <= 10; i++ {
			expToken := fmt.Sprintf("randomtoken%d", i)
			a, err := store.GetAuthorizationByToken(context.Background(), tx, expToken)
			require.NoError(t, err)
			if useHashedTokens {
				require.Empty(t, a.Token)
				hashedToken, err := hasher.Hash(expToken)
				require.NoError(t, err)
				require.Equal(t, hashedToken, a.HashedToken)
			} else {
				require.Equal(t, expToken, a.Token)
				require.Empty(t, a.HashedToken)
			}
		}

		var expAuthIndexCount, expHashedAuthIndexCount int
		if useHashedTokens {
			expHashedAuthIndexCount = 10
		} else {
			expAuthIndexCount = 10
		}
		checkIndexCounts(t, tx, expAuthIndexCount, expHashedAuthIndexCount)
	}

	tt := []struct {
		name    string
		setup   func(*testing.T, bool, *authorization.Store, *authorization.AuthorizationHasher, kv.Tx)
		update  func(*testing.T, *authorization.Store, kv.Tx)
		results func(*testing.T, bool, *authorization.Store, *authorization.AuthorizationHasher, kv.Tx)
	}{
		{
			name:  "create duplicate token",
			setup: setup,
			update: func(t *testing.T, store *authorization.Store, tx kv.Tx) {
				// should not be able to create two authorizations with identical tokens
				err := store.CreateAuthorization(context.Background(), tx, &influxdb.Authorization{
					ID:     platform.ID(1),
					Token:  fmt.Sprintf("randomtoken%d", 1),
					OrgID:  platform.ID(1),
					UserID: platform.ID(1),
				})
				require.ErrorIs(t, err, influxdb.ErrUnableToCreateToken)
			},
			results: func(t *testing.T, useHashedTokens bool, store *authorization.Store, hasher *authorization.AuthorizationHasher, tx kv.Tx) {
				auths, err := store.ListAuthorizations(context.Background(), tx, influxdb.AuthorizationFilter{})
				require.NoError(t, err)
				require.Len(t, auths, 10)

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
				require.Equal(t, auths, expected)

				var expAuthIndexCount, expHashedAuthIndexCount int
				if useHashedTokens {
					expHashedAuthIndexCount = 10
				} else {
					expAuthIndexCount = 10
				}
				checkIndexCounts(t, tx, expAuthIndexCount, expHashedAuthIndexCount)
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
					require.NoError(t, err)
					require.Equal(t, expectedAuth, authByID)

					authByToken, err := store.GetAuthorizationByToken(context.Background(), tx, fmt.Sprintf("randomtoken%d", i))
					require.NoError(t, err)
					require.Equal(t, expectedAuth, authByToken)
				}

				var expAuthIndexCount, expHashedAuthIndexCount int
				if useHashedTokens {
					expHashedAuthIndexCount = 10
				} else {
					expAuthIndexCount = 10
				}
				checkIndexCounts(t, tx, expAuthIndexCount, expHashedAuthIndexCount)
			},
		},
		{
			name:  "update",
			setup: setup,
			update: func(t *testing.T, store *authorization.Store, tx kv.Tx) {
				for i := 1; i <= 10; i++ {
					auth, err := store.GetAuthorizationByID(context.Background(), tx, platform.ID(i))
					require.NoError(t, err)

					auth.Status = influxdb.Inactive
					copyAuth := *auth

					updatedAuth, err := store.UpdateAuthorization(context.Background(), tx, platform.ID(i), auth)
					require.NoError(t, err)
					require.Equal(t, auth, updatedAuth) /* should be the same pointer */
					require.Equal(t, copyAuth, *auth)   /* should be the same contents */
				}
			},
			results: func(t *testing.T, useHashedTokens bool, store *authorization.Store, hasher *authorization.AuthorizationHasher, tx kv.Tx) {

				for i := 1; i <= 10; i++ {
					auth, err := store.GetAuthorizationByID(context.Background(), tx, platform.ID(i))
					require.NoError(t, err)

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

					require.Equal(t, expectedAuth, auth)
				}
				var expAuthIndexCount, expHashedAuthIndexCount int
				if useHashedTokens {
					expHashedAuthIndexCount = 10
				} else {
					expAuthIndexCount = 10
				}
				checkIndexCounts(t, tx, expAuthIndexCount, expHashedAuthIndexCount)
			},
		},
		{
			name:  "delete",
			setup: setup,
			update: func(t *testing.T, store *authorization.Store, tx kv.Tx) {
				for i := 1; i <= 10; i++ {
					err := store.DeleteAuthorization(context.Background(), tx, platform.ID(i))
					require.NoError(t, err)
				}
			},
			results: func(t *testing.T, useHashedTokens bool, store *authorization.Store, hasher *authorization.AuthorizationHasher, tx kv.Tx) {
				for i := 1; i <= 10; i++ {
					a, err := store.GetAuthorizationByID(context.Background(), tx, platform.ID(i))
					require.ErrorIs(t, err, authorization.ErrAuthNotFound)
					require.Nil(t, a)
				}
				checkIndexCounts(t, tx, 0, 0)
			},
		},
	}

	for _, testScenario := range tt {
		for _, useHashedTokens := range []bool{false, true} {

			t.Run(testScenario.name, func(t *testing.T) {
				store := inmem.NewKVStore()
				err := all.Up(context.Background(), zaptest.NewLogger(t), store)
				require.NoError(t, err)

				hasher, err := authorization.NewAuthorizationHasher()
				require.NoError(t, err)
				require.NotNil(t, hasher)

				ts, err := authorization.NewStore(context.Background(), store, useHashedTokens, authorization.WithAuthorizationHasher(hasher))
				require.NoError(t, err)
				require.NotNil(t, ts)

				// setup
				if testScenario.setup != nil {
					err := ts.Update(context.Background(), func(tx kv.Tx) error {
						testScenario.setup(t, useHashedTokens, ts, hasher, tx)
						return nil
					})
					require.NoError(t, err)
				}

				// update
				if testScenario.update != nil {
					err := ts.Update(context.Background(), func(tx kv.Tx) error {
						testScenario.update(t, ts, tx)
						return nil
					})
					require.NoError(t, err)
				}

				// results
				if testScenario.results != nil {
					err := ts.View(context.Background(), func(tx kv.Tx) error {
						testScenario.results(t, useHashedTokens, ts, hasher, tx)
						return nil
					})
					require.NoError(t, err)
				}
			})
		}
	}
}
