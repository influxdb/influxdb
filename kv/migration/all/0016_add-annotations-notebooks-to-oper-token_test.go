package all

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kv"
	"github.com/influxdata/influxdb/v2/snowflake"
	"github.com/stretchr/testify/require"
)

func TestMigration_AnnotationsNotebooksOperToken(t *testing.T) {
	runTestWithTokenHashing("TestMigration_AnnotationsNotebooksOperToken", runTestMigration_AnnotationsNotebooksOperToken, t)
}

func runTestMigration_AnnotationsNotebooksOperToken(useTokenHashing bool, t *testing.T) {
	ctx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()

	// Run up to migration 15.
	ts := newService(t, ctx, 15, useTokenHashing)

	// Auth bucket contains the authorizations AKA tokens
	authBucket := []byte("authorizationsv1")

	// The store returned by newService will include an operator token with the
	// current system's entire list of resources already, so remove that before
	// proceeding with the tests.
	err := ts.Store.Update(context.Background(), func(tx kv.Tx) error {
		bkt, err := tx.Bucket(authBucket)
		require.NoError(t, err)

		cursor, err := bkt.ForwardCursor(nil)
		require.NoError(t, err)

		return kv.WalkCursor(ctx, cursor, func(k, _ []byte) (bool, error) {
			err := bkt.Delete(k)
			require.NoError(t, err)
			return true, nil
		})
	})
	require.NoError(t, err)

	// Verify that running the migration in the absence of an operator token will
	// not crash influxdb.
	require.NoError(t, Migration0016_AddAnnotationsNotebooksToOperToken.Up(context.Background(), ts.Store))

	// Seed some authorizations
	id1 := snowflake.NewIDGenerator().ID()
	id2 := snowflake.NewIDGenerator().ID()
	OrgID := ts.Org.ID
	UserID := ts.User.ID

	auths := []influxdb.Authorization{
		{
			ID:          id1, // a non-operator token
			OrgID:       OrgID,
			UserID:      UserID,
			Permissions: permsShouldNotChange(),
		},
		{
			ID:          id2, // an operator token
			OrgID:       OrgID,
			UserID:      UserID,
			Permissions: preNotebooksAnnotationsOpPerms(),
		},
	}

	for _, a := range auths {
		js, err := json.Marshal(a)
		require.NoError(t, err)
		idBytes, err := a.ID.Encode()
		require.NoError(t, err)

		err = ts.Store.Update(context.Background(), func(tx kv.Tx) error {
			bkt, err := tx.Bucket(authBucket)
			require.NoError(t, err)
			return bkt.Put(idBytes, js)
		})
		require.NoError(t, err)
	}

	encoded1, err := id1.Encode()
	require.NoError(t, err)
	encoded2, err := id2.Encode()
	require.NoError(t, err)

	checkPerms := func(expectedOpPerms []influxdb.Permission) {
		// the first item should never change
		err = ts.Store.View(context.Background(), func(tx kv.Tx) error {
			bkt, err := tx.Bucket(authBucket)
			require.NoError(t, err)

			b, err := bkt.Get(encoded1)
			require.NoError(t, err)

			var token influxdb.Authorization
			require.NoError(t, json.Unmarshal(b, &token))
			require.Equal(t, auths[0], token)

			return nil
		})
		require.NoError(t, err)

		// the second item is the 2.0.x operator token and should have been updated to match our expectations
		err = ts.Store.View(context.Background(), func(tx kv.Tx) error {
			bkt, err := tx.Bucket(authBucket)
			require.NoError(t, err)

			b, err := bkt.Get(encoded2)
			require.NoError(t, err)

			var token influxdb.Authorization
			require.NoError(t, json.Unmarshal(b, &token))

			require.ElementsMatch(t, expectedOpPerms, token.Permissions)
			return nil
		})
		require.NoError(t, err)
	}

	// Test applying the migration for the 1st time.
	require.NoError(t, Migration0016_AddAnnotationsNotebooksToOperToken.Up(context.Background(), ts.Store))
	checkPerms(append(preNotebooksAnnotationsOpPerms(), notebooksAndAnnotationsPerms(0)...))

	// Downgrade the migration.
	require.NoError(t, Migration0016_AddAnnotationsNotebooksToOperToken.Down(context.Background(), ts.Store))
	checkPerms(preNotebooksAnnotationsOpPerms())

	// Test re-applying the migration after a downgrade.
	require.NoError(t, Migration0016_AddAnnotationsNotebooksToOperToken.Up(context.Background(), ts.Store))
	checkPerms(append(preNotebooksAnnotationsOpPerms(), notebooksAndAnnotationsPerms(0)...))
}

func Test_PermListsMatch(t *testing.T) {
	tests := []struct {
		name string
		l1   []influxdb.Permission
		l2   []influxdb.Permission
		want bool
	}{
		{
			"empty lists",
			[]influxdb.Permission{},
			[]influxdb.Permission{},
			true,
		},
		{
			"not matching",
			[]influxdb.Permission{
				{
					Action: influxdb.ReadAction,
					Resource: influxdb.Resource{
						Type: influxdb.ChecksResourceType,
					},
				},
			},
			[]influxdb.Permission{
				{
					Action: influxdb.WriteAction,
					Resource: influxdb.Resource{
						Type: influxdb.ChecksResourceType,
					},
				},
			},
			false,
		},
		{
			"matches same order",
			[]influxdb.Permission{
				{
					Action: influxdb.ReadAction,
					Resource: influxdb.Resource{
						Type: influxdb.ChecksResourceType,
					},
				},
				{
					Action: influxdb.WriteAction,
					Resource: influxdb.Resource{
						Type: influxdb.DashboardsResourceType,
					},
				},
			},
			[]influxdb.Permission{
				{
					Action: influxdb.ReadAction,
					Resource: influxdb.Resource{
						Type: influxdb.ChecksResourceType,
					},
				},
				{
					Action: influxdb.WriteAction,
					Resource: influxdb.Resource{
						Type: influxdb.DashboardsResourceType,
					},
				},
			},
			true,
		},
		{
			"matches different order",
			[]influxdb.Permission{
				{
					Action: influxdb.ReadAction,
					Resource: influxdb.Resource{
						Type: influxdb.ChecksResourceType,
					},
				},
				{
					Action: influxdb.WriteAction,
					Resource: influxdb.Resource{
						Type: influxdb.ChecksResourceType,
					},
				},
				{
					Action: influxdb.WriteAction,
					Resource: influxdb.Resource{
						Type: influxdb.DBRPResourceType,
					},
				},
				{
					Action: influxdb.ReadAction,
					Resource: influxdb.Resource{
						Type: influxdb.DBRPResourceType,
					},
				},
			},
			[]influxdb.Permission{
				{
					Action: influxdb.WriteAction,
					Resource: influxdb.Resource{
						Type: influxdb.DBRPResourceType,
					},
				},
				{
					Action: influxdb.ReadAction,
					Resource: influxdb.Resource{
						Type: influxdb.DBRPResourceType,
					},
				},
				{
					Action: influxdb.WriteAction,
					Resource: influxdb.Resource{
						Type: influxdb.ChecksResourceType,
					},
				},
				{
					Action: influxdb.ReadAction,
					Resource: influxdb.Resource{
						Type: influxdb.ChecksResourceType,
					},
				},
			},
			true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := permListsMatch(tt.l1, tt.l2)
			require.Equal(t, tt.want, got)
		})
	}
}

// This set of permissions shouldn't change - it doesn't match the operator
// token.
func permsShouldNotChange() []influxdb.Permission {
	return []influxdb.Permission{
		{
			Action: influxdb.ReadAction,
			Resource: influxdb.Resource{
				Type: influxdb.ChecksResourceType,
			},
		},
		{
			Action: influxdb.WriteAction,
			Resource: influxdb.Resource{
				Type: influxdb.ChecksResourceType,
			},
		},
	}
}
