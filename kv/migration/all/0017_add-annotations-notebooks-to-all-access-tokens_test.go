package all

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/influxdata/influxdb/v2/kv"
	"github.com/influxdata/influxdb/v2/snowflake"
	"github.com/stretchr/testify/require"
)

func TestMigration_AnnotationsNotebooksAllAccessToken(t *testing.T) {
	runTestWithTokenHashing("TestMigration_AnnotationsNotebooksAllAccessToken", runTestMigration_AnnotationsNotebooksAllAccessToken, t)
}

func runTestMigration_AnnotationsNotebooksAllAccessToken(useTokenHashing bool, t *testing.T) {
	ctx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()

	// Run up to migration 16.
	ts := newService(t, ctx, 16, useTokenHashing)

	// Auth bucket contains the authorizations AKA tokens
	authBucket := []byte("authorizationsv1")

	// Verify that running the migration in the absence of an all-access token will
	// not crash influxdb.
	require.NoError(t, Migration0017_AddAnnotationsNotebooksToAllAccessTokens.Up(context.Background(), ts.Store))

	// Seed some authorizations
	id1 := snowflake.NewIDGenerator().ID()
	id2 := snowflake.NewIDGenerator().ID()
	OrgID := ts.Org.ID
	UserID := ts.User.ID

	auths := []influxdb.Authorization{
		{
			ID:          id1, // a non-all-access token
			OrgID:       OrgID,
			UserID:      UserID,
			Permissions: orgPermsShouldNotChange(OrgID),
		},
		{
			ID:          id2, // an all-access token
			OrgID:       OrgID,
			UserID:      UserID,
			Permissions: preNotebooksAnnotationsAllAccessPerms(OrgID, UserID),
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

	checkPerms := func(expectedAllPerms []influxdb.Permission) {
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

		// the second item is a 2.0.x all-access token and should have been updated to match our expectations
		err = ts.Store.View(context.Background(), func(tx kv.Tx) error {
			bkt, err := tx.Bucket(authBucket)
			require.NoError(t, err)

			b, err := bkt.Get(encoded2)
			require.NoError(t, err)

			var token influxdb.Authorization
			require.NoError(t, json.Unmarshal(b, &token))

			require.ElementsMatch(t, expectedAllPerms, token.Permissions)
			return nil
		})
		require.NoError(t, err)
	}

	// Test applying the migration for the 1st time.
	require.NoError(t, Migration0017_AddAnnotationsNotebooksToAllAccessTokens.Up(context.Background(), ts.Store))
	checkPerms(append(preNotebooksAnnotationsAllAccessPerms(OrgID, UserID), notebooksAndAnnotationsPerms(OrgID)...))

	// Downgrade the migration.
	require.NoError(t, Migration0017_AddAnnotationsNotebooksToAllAccessTokens.Down(context.Background(), ts.Store))
	checkPerms(preNotebooksAnnotationsAllAccessPerms(OrgID, UserID))

	// Test re-applying the migration after a downgrade.
	require.NoError(t, Migration0017_AddAnnotationsNotebooksToAllAccessTokens.Up(context.Background(), ts.Store))
	checkPerms(append(preNotebooksAnnotationsAllAccessPerms(OrgID, UserID), notebooksAndAnnotationsPerms(OrgID)...))
}

// This set of permissions shouldn't change - it doesn't match an all-access token.
func orgPermsShouldNotChange(orgId platform.ID) []influxdb.Permission {
	return []influxdb.Permission{
		{
			Action: influxdb.ReadAction,
			Resource: influxdb.Resource{
				Type:  influxdb.ChecksResourceType,
				OrgID: &orgId,
			},
		},
		{
			Action: influxdb.WriteAction,
			Resource: influxdb.Resource{
				Type:  influxdb.ChecksResourceType,
				OrgID: &orgId,
			},
		},
	}
}
