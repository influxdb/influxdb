package all

import "github.com/influxdata/influxdb/v2/kv/migration"

var Migration0021_CreateAuthorizationHashedIndexv1 = migration.CreateBuckets(
	"create authorizationhashedindexv1 bucket",
	[]byte("authorizationhashedindexv1"),
)
