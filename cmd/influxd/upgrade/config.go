package upgrade

// Configuration file upgrade implementation.
// The strategy is to transform only those entries for which rule exists.

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/BurntSushi/toml"
	"go.uber.org/zap"
	"golang.org/x/text/encoding/unicode"
	"golang.org/x/text/transform"
)

// passthroughConfigRules maps v1 config key-names to corresponding v2 config key-names.
// The values of these configs will not be modified during the upgrade process.
var passthroughConfigRules = map[string]string{
	"reporting-disabled":                                   "reporting-disabled",
	"data.dir":                                             "engine-path",
	"data.wal-fsync-delay":                                 "storage-wal-fsync-delay",
	"data.validate-keys":                                   "storage-validate-keys",
	"data.cache-max-memory-size":                           "storage-cache-max-memory-size",
	"data.cache-snapshot-memory-size":                      "storage-cache-snapshot-memory-size",
	"data.cache-snapshot-write-cold-duration":              "storage-cache-snapshot-write-cold-duration",
	"data.compact-full-write-cold-duration":                "storage-compact-full-write-cold-duration",
	"data.compact-throughput":                              "storage-compact-throughput",
	"data.compact-throughput-burst":                        "storage-compact-throughput-burst",
	"data.max-concurrent-compactions":                      "storage-max-concurrent-compactions",
	"data.max-index-log-file-size":                         "storage-max-index-log-file-size",
	"data.series-id-set-cache-size":                        "storage-series-id-set-cache-size",
	"data.series-file-max-concurrent-snapshot-compactions": "storage-series-file-max-concurrent-snapshot-compactions",
	"data.tsm-use-madv-willneed":                           "storage-tsm-use-madv-willneed",
	"retention.check-interval":                             "storage-retention-check-interval",
	"shard-precreation.check-interval":                     "storage-shard-precreator-check-interval",
	"shard-precreation.advance-period":                     "storage-shard-precreator-advance-period",
	"coordinator.max-concurrent-queries":                   "query-concurrency",
	"coordinator.max-select-point":                         "influxql-max-select-point",
	"coordinator.max-select-series":                        "influxql-max-select-series",
	"coordinator.max-select-buckets":                       "influxql-max-select-buckets",
	"logging.level":                                        "log-level",
	"http.bind-address":                                    "http-bind-address",
	"http.https-certificate":                               "tls-cert",
	"http.https-private-key":                               "tls-key",
}

func loadV1Config(configFile string) (*configV1, *map[string]interface{}, error) {
	_, err := os.Stat(configFile)
	if err != nil {
		return nil, nil, fmt.Errorf("1.x config file '%s' does not exist", configFile)
	}

	// load 1.x config content into byte array
	bs, err := load(configFile)
	if err != nil {
		return nil, nil, err
	}

	// parse it into simplified v1 config used as return value
	var configV1 configV1
	_, err = toml.Decode(string(bs), &configV1)
	if err != nil {
		return nil, nil, err
	}

	// parse into a generic config map
	var cAny map[string]interface{}
	_, err = toml.Decode(string(bs), &cAny)
	if err != nil {
		return nil, nil, err
	}

	return &configV1, &cAny, nil
}

func load(path string) ([]byte, error) {
	bs, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	// From master-1.x/cmd/influxd/run/config.go:
	// Handle any potential Byte-Order-Marks that may be in the config file.
	// This is for Windows compatibility only.
	// See https://github.com/influxdata/telegraf/issues/1378 and
	// https://github.com/influxdata/influxdb/issues/8965.
	bom := unicode.BOMOverride(transform.Nop)
	bs, _, err = transform.Bytes(bom, bs)

	return bs, err
}

// upgradeConfig upgrades existing 1.x configuration file to 2.x influxdb.toml file.
func upgradeConfig(v1Config map[string]interface{}, targetOptions optionsV2, log *zap.Logger) error {
	// create and initialize helper
	cu := &configUpgrader{
		rules: passthroughConfigRules,
		log:   log,
	}

	// rewrite config options from V1 to V2 paths
	cTransformed := cu.transform(v1Config)

	// update new config with upgrade command options
	cu.updateV2Config(cTransformed, targetOptions)

	// write the ugpraded config to disk
	return cu.save(cTransformed, targetOptions.configPath)
}

// configUpgrader is a helper used by `upgrade-config` command.
type configUpgrader struct {
	rules map[string]string
	log   *zap.Logger
}

func (cu *configUpgrader) updateV2Config(config map[string]interface{}, targetOptions optionsV2) {
	if targetOptions.enginePath != "" {
		config["engine-path"] = targetOptions.enginePath
	}
	if targetOptions.boltPath != "" {
		config["bolt-path"] = targetOptions.boltPath
	}
}

func (cu *configUpgrader) save(config map[string]interface{}, path string) error {
	// Open the target file, creating parent directories if needed.
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return err
	}
	outFile, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer outFile.Close()

	// Encode the config directly into the file as TOML.
	return toml.NewEncoder(outFile).Encode(&config)
}

// Credits: @rogpeppe (Roger Peppe)

func (cu *configUpgrader) transform(v1Config map[string]interface{}) map[string]interface{} {
	res := make(map[string]interface{})
	for old, new := range cu.rules {
		if val, ok := cu.lookup(v1Config, old); ok {
			res[new] = val
		}
	}

	// Special case: flip the value for pprof.
	if val, ok := cu.lookup(v1Config, "http.pprof-enabled"); ok {
		if b, ok := val.(bool); ok {
			res["pprof-disabled"] = !b
		}
	}

	// Special case: ensure query settings are valid.
	fixQueryLimits(res)

	return res
}

// fixQueryLimits ensures that all query-related config settings are compatible
// with the upgraded value of the 'query-concurrency' setting.
func fixQueryLimits(v2Config map[string]interface{}) {
	concurrencyVal, ok := v2Config["query-concurrency"]
	if !ok {
		return
	}
	var concurrency int64
	switch c := concurrencyVal.(type) {
	case int:
		concurrency = int64(c)
	case int32:
		concurrency = int64(c)
	case int64:
		concurrency = c
	default:
		concurrency = 0
	}
	if concurrency == 0 {
		// The upgrade process doesn't generate a value for query-queue-size, so if
		// query-concurrency is 0 / unset then it's safe to leave query-queue-size unset.
		return
	}

	// When query-concurrency is > 0, query-queue-size must also be > 0.
	v2Config["query-queue-size"] = concurrency
}

func (cu *configUpgrader) lookup(v1Config map[string]interface{}, path string) (interface{}, bool) {
	for {
		elem, rest, _ := strings.Cut(path, ".")
		val, ok := v1Config[elem]
		if rest == "" {
			return val, ok
		}
		child, ok := val.(map[string]interface{})
		if !ok {
			return nil, false
		}
		path, v1Config = rest, child
	}
}
