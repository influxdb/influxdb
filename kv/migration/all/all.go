package all

import (
	"github.com/influxdata/influxdb/v2/kv/migration"
)

// Migrations contains all the migrations required for the entire of the
// kv store backing influxdb's metadata.
var Migrations = [...]migration.Spec{
	// initial migrations
	Migration0001_InitialMigration,
	// add index user resource mappings by user id
	Migration0002_AddURMByUserIndex,
	// add index for tasks with missing owner IDs
	Migration0003_TaskOwnerIDUpMigration,
	// add dbrp buckets
	Migration0004_AddDbrpBuckets,
	// add pkger buckets
	Migration0005_AddPkgerBuckets,
	// delete bucket sessionsv1
	Migration0006_DeleteBucketSessionsv1,
	// CreateMetaDataBucket
	Migration0007_CreateMetaDataBucket,
	// LegacyAuthBuckets
	Migration0008_LegacyAuthBuckets,
	// LegacyAuthPasswordBuckets
	Migration0009_LegacyAuthPasswordBuckets,
	// add index telegraf by org
	Migration0010_AddIndexTelegrafByOrg,
	// populate dashboards owner id
	Migration0011_PopulateDashboardsOwnerId,
	// Populate the DBRP service ByOrg index
	Migration0012_DBRPByOrgIndex,
	// repair DBRP owner and bucket IDs
	Migration0013_RepairDBRPOwnerAndBucketIDs,
	// reindex DBRPs
	Migration0014_ReindexDBRPs,
	// record shard group durations in bucket metadata
	Migration0015_RecordShardGroupDurationsInBucketMetadata,
	// add annotations and notebooks resource types to the operator token
	Migration0016_AddAnnotationsNotebooksToOperToken,
	// add annotations and notebooks resource types to all-access tokens
	Migration0017_AddAnnotationsNotebooksToAllAccessTokens,
	// repair missing shard group durations
	Migration0018_RepairMissingShardGroupDurations,
	// add remotes and replications resource types to operator and all-access tokens
	Migration0019_AddRemotesReplicationsToTokens,
	// add_remotes_replications_metrics_buckets
	Migration0020_Add_remotes_replications_metrics_buckets,
	// create bucket for hashed token index
	Migration0021_CreateAuthorizationHashedIndexv1,
	// {{ do_not_edit . }}
}
