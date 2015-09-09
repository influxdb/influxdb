package opentsdb

import (
	"time"

	"github.com/influxdb/influxdb/toml"
)

const (
	// DefaultBindAddress is the default address that the service binds to.
	DefaultBindAddress = ":4242"

	// DefaultDatabase is the default database used for writes.
	DefaultDatabase = "opentsdb"

	// DefaultRetentionPolicy is the default retention policy used for writes.
	DefaultRetentionPolicy = ""

	// DefaultConsistencyLevel is the default write consistency level.
	DefaultConsistencyLevel = "one"

	DefaultBatchSize = 5000

	DefaultBatchDuration = toml.Duration(10 * time.Second)
)

type Config struct {
	Enabled          bool          `toml:"enabled"`
	BindAddress      string        `toml:"bind-address"`
	Database         string        `toml:"database"`
	RetentionPolicy  string        `toml:"retention-policy"`
	ConsistencyLevel string        `toml:"consistency-level"`
	TLSEnabled       bool          `toml:"tls-enabled"`
	Certificate      string        `toml:"certificate"`
	BatchSize        int           `toml:"batch-size"`
	BatchDuration    toml.Duration `toml:"batch-timeout"`
}

func NewConfig() Config {
	return Config{
		BindAddress:      DefaultBindAddress,
		Database:         DefaultDatabase,
		RetentionPolicy:  DefaultRetentionPolicy,
		ConsistencyLevel: DefaultConsistencyLevel,
		TLSEnabled:       false,
		Certificate:      "/etc/ssl/influxdb.pem",
		BatchSize:        DefaultBatchSize,
		BatchDuration:    DefaultBatchDuration,
	}
}
