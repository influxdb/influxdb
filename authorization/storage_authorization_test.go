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
	influxdb2_algo "github.com/influxdata/influxdb/v2/pkg/crypt/algorithm/influxdb2"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
)

const (
	authIndexName       = "authorizationindexv1"
	hashedAuthIndexName = "authorizationhashedindexv1"
)

func TestAuth(t *testing.T) {
	checkIndexCounts := func(t *testing.T, tx kv.Tx, expAuthIndexCount, expHashedAuthIndexCount int) {
		t.Helper()

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

func TestAuthorizationStore_HashingConfigChanges(t *testing.T) {
	sha256, err := influxdb2_algo.New(influxdb2_algo.WithVariant(influxdb2_algo.VariantSHA256))
	require.NoError(t, err)
	sha512, err := influxdb2_algo.New(influxdb2_algo.WithVariant(influxdb2_algo.VariantSHA512))
	require.NoError(t, err)

	type authData struct {
		ID          platform.ID
		Token       string
		HashedToken string
	}
	type testConfig struct {
		enabled bool
		algo    string
	}
	type testCase struct {
		desc         string
		config       testConfig
		action       func(t *testing.T, ctx context.Context, store *authorization.Store, tx kv.Tx)
		exp          []authData
		hashedTokens []string // tokens which only exists as hashes
	}
	cases := []testCase{
		{
			desc:   "initial unhashed",
			config: testConfig{enabled: false},
			action: func(t *testing.T, ctx context.Context, store *authorization.Store, tx kv.Tx) {
				a := &influxdb.Authorization{
					ID:     platform.ID(1),
					OrgID:  platform.ID(1),
					UserID: platform.ID(1),
					Token:  "Token#1",
				}
				require.NoError(t, store.CreateAuthorization(ctx, tx, a))
			},
			exp: []authData{
				{ID: platform.ID(1), Token: "Token#1"},
			},
		},
		{
			desc:   "upgrade hashed #1", // update hash and indices
			config: testConfig{enabled: true, algo: influxdb2_algo.VariantIdentifierSHA256},
			exp: []authData{
				{ID: platform.ID(1), HashedToken: sha256.MustHash("Token#1").Encode()},
			},
			hashedTokens: []string{"Token#1"},
		},
		{
			desc:   "downgrade hashed #1", // can't unhash
			config: testConfig{enabled: false, algo: influxdb2_algo.VariantIdentifierSHA256},
			action: func(t *testing.T, ctx context.Context, store *authorization.Store, tx kv.Tx) {
				a := &influxdb.Authorization{
					ID:     platform.ID(2),
					OrgID:  platform.ID(2),
					UserID: platform.ID(2),
					Token:  "Token#2",
				}
				require.NoError(t, store.CreateAuthorization(ctx, tx, a))
			},
			exp: []authData{
				{ID: platform.ID(1), HashedToken: sha256.MustHash("Token#1").Encode()},
				{ID: platform.ID(2), Token: "Token#2"},
			},
			hashedTokens: []string{"Token#1"},
		},
		{
			desc:   "upgrade hashed sha512", // can't rehash existing, use new algo for new auths
			config: testConfig{enabled: true, algo: influxdb2_algo.VariantIdentifierSHA512},
			action: func(t *testing.T, ctx context.Context, store *authorization.Store, tx kv.Tx) {
				a := &influxdb.Authorization{
					ID:     platform.ID(3),
					OrgID:  platform.ID(3),
					UserID: platform.ID(3),
					Token:  "Token#3",
				}
				require.NoError(t, store.CreateAuthorization(ctx, tx, a))
			},
			exp: []authData{
				{ID: platform.ID(1), HashedToken: sha256.MustHash("Token#1").Encode()},
				{ID: platform.ID(2), HashedToken: sha512.MustHash("Token#2").Encode()},
				{ID: platform.ID(3), HashedToken: sha512.MustHash("Token#3").Encode()},
			},
			hashedTokens: []string{"Token#1", "Token#2", "Token#3"},
		},
	}

	ctx := context.Background()

	// The underlying kv store persists across tests cases. This allows for testing how opening with
	// new authentication configurations impacts the data.
	kvStore := inmem.NewKVStore()
	err = all.Up(ctx, zaptest.NewLogger(t), kvStore)
	require.NoError(t, err)

	for _, tc := range cases {
		t.Run(tc.desc, func(t *testing.T) {
			// Create new authorization.Store for test cases using existing kvStore.
			variantName := tc.config.algo
			if variantName == "" {
				if !tc.config.enabled {
					variantName = authorization.DefaultHashVariantName
				} else {
					require.Fail(t, "Must specific algo if hashing is enabled for test case")
				}
			}
			store, err := authorization.NewStore(ctx, kvStore, tc.config.enabled, authorization.WithAuthorizationHashVariantName(variantName))
			require.NoError(t, err)
			require.NotNil(t, store)

			// Execute action, if given. Simply opening the store with a different configuration may be the "action".
			if tc.action != nil {
				err = kvStore.Update(ctx, func(tx kv.Tx) error {
					tc.action(t, ctx, store, tx)
					return nil
				})
				require.NoError(t, err)
			}

			// Check results.
			err = kvStore.View(ctx, func(tx kv.Tx) error {
				// Collect all authorization data from store.
				storedAuths, err := store.ListAuthorizations(ctx, tx, influxdb.AuthorizationFilter{})
				require.NoError(t, err)

				// Collect auth data from data currently in store
				actualAuthData := make([]authData, 0, len(storedAuths))
				for _, sa := range storedAuths {
					ad := authData{ID: sa.ID, Token: sa.Token, HashedToken: sa.HashedToken}
					actualAuthData = append(actualAuthData, ad)
				}

				// Check that authData matches exp.
				require.ElementsMatch(t, tc.exp, actualAuthData)

				// Collect data from kvStore's token index.
				collectIndex := func(indexName string) map[string]platform.ID {
					indexMap := make(map[string]platform.ID)
					index, err := tx.Bucket([]byte(indexName))
					require.NoError(t, err)
					cursor, err := index.Cursor()
					require.NoError(t, err)
					for k, v := cursor.First(); k != nil; k, v = cursor.Next() {
						var id platform.ID
						require.NoError(t, id.Decode(v))
						indexMap[string(k)] = id
					}
					return indexMap
				}
				actualTokenIndex := collectIndex(authIndexName)
				actualHashedIndex := collectIndex(hashedAuthIndexName)

				// Collect expected token and hashed indices.
				expTokenIndex := make(map[string]platform.ID)
				expHashedIndex := make(map[string]platform.ID)
				for _, d := range tc.exp {
					if d.Token != "" {
						expTokenIndex[d.Token] = d.ID
					}
					if d.HashedToken != "" {
						expHashedIndex[d.HashedToken] = d.ID
					}
				}

				// Compare indices.
				require.Equal(t, expTokenIndex, actualTokenIndex)
				require.Equal(t, expHashedIndex, actualHashedIndex)

				// Make sure we can lookup all tokens.
				var allTokens []string
				for _, d := range tc.exp {
					if d.Token != "" {
						allTokens = append(allTokens, d.Token)
					}
				}
				allTokens = append(allTokens, tc.hashedTokens...)

				for _, token := range allTokens {
					auth, err := store.GetAuthorizationByToken(ctx, tx, token)
					require.NoError(t, err, "error looking up token %q", token)
					require.NotNil(t, auth)
				}

				return nil
			})
			require.NoError(t, err)
		})

	}
}
