package launcher

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/influxdata/influxdb/v2/bolt"
	"github.com/influxdata/influxdb/v2/fluxinit"
	"github.com/influxdata/influxdb/v2/internal/fs"
	"github.com/influxdata/influxdb/v2/kit/cli"
	"github.com/influxdata/influxdb/v2/kit/signals"
	influxlogger "github.com/influxdata/influxdb/v2/logger"
	"github.com/influxdata/influxdb/v2/pprof"
	"github.com/influxdata/influxdb/v2/sqlite"
	"github.com/influxdata/influxdb/v2/storage"
	"github.com/influxdata/influxdb/v2/v1/coordinator"
	"github.com/influxdata/influxdb/v2/vault"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"go.uber.org/zap/zapcore"
)

func errInvalidFlags(flags []string, configFile string) error {
	return fmt.Errorf(
		"error: found flags from an InfluxDB 1.x configuration in config file at %s - see https://docs.influxdata.com/influxdb/latest/reference/config-options/ for flags supported on this version of InfluxDB: %s",
		configFile,
		strings.Join(flags, ","),
	)
}

// NewInfluxdCommand constructs the root of the influxd CLI, along with a `run` subcommand.
// The `run` subcommand is set as the default to execute.
func NewInfluxdCommand(ctx context.Context, v *viper.Viper) (*cobra.Command, error) {
	o := NewOpts(v)
	cliOpts := o.BindCliOpts()

	prog := cli.Program{
		Name: "influxd",
		Run:  cmdRunE(ctx, o),
	}
	cmd, err := cli.NewCommand(o.Viper, &prog)
	if err != nil {
		return nil, err
	}

	// Error out if invalid flags are found in the config file. This may indicate trying to launch 2.x using a 1.x config.
	if invalidFlags := invalidFlags(v); len(invalidFlags) > 0 {
		return nil, errInvalidFlags(invalidFlags, v.ConfigFileUsed())
	}

	runCmd := &cobra.Command{
		Use:  "run",
		RunE: cmd.RunE,
		Args: cobra.NoArgs,
	}
	for _, c := range []*cobra.Command{cmd, runCmd} {
		setCmdDescriptions(c)
		if err := cli.BindOptions(o.Viper, c, cliOpts); err != nil {
			return nil, err
		}
	}
	cmd.AddCommand(runCmd)
	printCmd, err := NewInfluxdPrintConfigCommand(v, cliOpts)
	if err != nil {
		return nil, err
	}
	cmd.AddCommand(printCmd)

	return cmd, nil
}

func invalidFlags(v *viper.Viper) []string {
	var invalid []string
	for _, k := range v.AllKeys() {
		if inOneDotExFlagsList(k) {
			invalid = append(invalid, k)
		}
	}

	return invalid
}

func setCmdDescriptions(cmd *cobra.Command) {
	cmd.Short = "Start the influxd server"
	cmd.Long = `
	Start up the daemon configured with flags/env vars/config file.

	The order of precedence for config options are as follows (1 highest, 3 lowest):
		1. flags
		2. env vars
		3. config file

	A config file can be provided via the INFLUXD_CONFIG_PATH env var. If a file is
	not provided via an env var, influxd will look in the current directory for a
	config.{json|toml|yaml|yml} file. If one does not exist, then it will continue unchanged.
`
}

func cmdRunE(ctx context.Context, o *InfluxdOpts) func() error {
	return func() error {
		// Set this as early as possible, since it affects global profiling rates.
		pprof.SetGlobalProfiling(!o.ProfilingDisabled)

		fluxinit.FluxInit()

		l := NewLauncher()

		// Create top level logger
		logconf := &influxlogger.Config{
			Format: "auto",
			Level:  o.LogLevel,
		}
		logger, err := logconf.New(os.Stdout)
		if err != nil {
			return err
		}
		l.log = logger

		// Start the launcher and wait for it to exit on SIGINT or SIGTERM.
		if err := l.run(signals.WithStandardSignals(ctx), o); err != nil {
			return err
		}
		<-l.Done()

		// Tear down the launcher, allowing it a few seconds to finish any
		// in-progress requests.
		shutdownCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		defer cancel()
		return l.Shutdown(shutdownCtx)
	}
}

// InfluxdOpts captures all arguments for running the InfluxDB server.
type InfluxdOpts struct {
	Testing                 bool
	TestingAlwaysAllowSetup bool

	LogLevel          zapcore.Level
	FluxLogEnabled    bool
	TracingType       string
	ReportingDisabled bool

	PIDFile          string
	OverwritePIDFile bool

	AssetsPath string
	BoltPath   string
	SqLitePath string
	EnginePath string

	StoreType   string
	SecretStore string
	VaultConfig vault.Config

	InstanceID string

	HttpBindAddress       string
	HttpReadHeaderTimeout time.Duration
	HttpReadTimeout       time.Duration
	HttpWriteTimeout      time.Duration
	HttpIdleTimeout       time.Duration
	HttpTLSCert           string
	HttpTLSKey            string
	HttpTLSMinVersion     string
	HttpTLSStrictCiphers  bool
	SessionLength         int // in minutes
	SessionRenewDisabled  bool

	ProfilingDisabled bool
	MetricsDisabled   bool
	UIDisabled        bool

	NatsPort            int
	NatsMaxPayloadBytes int

	NoTasks      bool
	FeatureFlags map[string]string

	// Query options.
	ConcurrencyQuota                int32
	InitialMemoryBytesQuotaPerQuery int64
	MemoryBytesQuotaPerQuery        int64
	MaxMemoryBytes                  int64
	QueueSize                       int32
	CoordinatorConfig               coordinator.Config

	// Storage options.
	StorageConfig storage.Config

	Viper *viper.Viper

	// HardeningEnabled toggles multiple best-practice hardening options on.
	HardeningEnabled bool
	// TemplateFileUrlsDisabled disables file protocol URIs in templates.
	TemplateFileUrlsDisabled bool
	StrongPasswords          bool
	UseHashedTokens          bool
}

// NewOpts constructs options with default values.
func NewOpts(viper *viper.Viper) *InfluxdOpts {
	dir, err := fs.InfluxDir()
	if err != nil {
		panic(fmt.Errorf("failed to determine influx directory: %v", err))
	}

	return &InfluxdOpts{
		Viper:             viper,
		StorageConfig:     storage.NewConfig(),
		CoordinatorConfig: coordinator.NewConfig(),

		LogLevel:          zapcore.InfoLevel,
		FluxLogEnabled:    false,
		ReportingDisabled: false,

		PIDFile:          "",
		OverwritePIDFile: false,

		BoltPath:   filepath.Join(dir, bolt.DefaultFilename),
		SqLitePath: filepath.Join(dir, sqlite.DefaultFilename),
		EnginePath: filepath.Join(dir, "engine"),

		HttpBindAddress:       ":8086",
		HttpReadHeaderTimeout: 10 * time.Second,
		HttpIdleTimeout:       3 * time.Minute,
		HttpTLSMinVersion:     "1.2",
		HttpTLSStrictCiphers:  false,
		SessionLength:         60, // 60 minutes
		SessionRenewDisabled:  false,

		ProfilingDisabled: false,
		MetricsDisabled:   false,
		UIDisabled:        false,

		StoreType:   DiskStore,
		SecretStore: BoltStore,

		NatsPort:            0,
		NatsMaxPayloadBytes: 0,

		NoTasks: false,

		ConcurrencyQuota:                1024,
		InitialMemoryBytesQuotaPerQuery: 0,
		MemoryBytesQuotaPerQuery:        0,
		MaxMemoryBytes:                  0,
		QueueSize:                       1024,

		Testing:                 false,
		TestingAlwaysAllowSetup: false,

		HardeningEnabled:         false,
		TemplateFileUrlsDisabled: false,
		StrongPasswords:          false,
	}
}

// BindCliOpts returns a list of options which can be added to a cobra command
// in order to set options over the CLI.
func (o *InfluxdOpts) BindCliOpts() []cli.Opt {
	return []cli.Opt{
		{
			DestP:   &o.LogLevel,
			Flag:    "log-level",
			Default: o.LogLevel,
			Desc:    "supported log levels are debug, info, and error",
		},
		{
			DestP:   &o.FluxLogEnabled,
			Flag:    "flux-log-enabled",
			Default: o.FluxLogEnabled,
			Desc:    "enables detailed logging for flux queries",
		},
		{
			DestP: &o.TracingType,
			Flag:  "tracing-type",
			Desc:  fmt.Sprintf("supported tracing types are %s, %s", LogTracing, JaegerTracing),
		},
		{
			DestP:   &o.BoltPath,
			Flag:    "bolt-path",
			Default: o.BoltPath,
			Desc:    "path to boltdb database",
		},
		{
			DestP: &o.SqLitePath,
			Flag:  "sqlite-path",
			Desc:  fmt.Sprintf("path to sqlite database. if not set, sqlite database will be stored in the bolt-path directory as %q.", sqlite.DefaultFilename),
		},
		{
			DestP: &o.AssetsPath,
			Flag:  "assets-path",
			Desc:  "override default assets by serving from a specific directory (developer mode)",
		},
		{
			DestP:   &o.StoreType,
			Flag:    "store",
			Default: o.StoreType,
			Desc:    "backing store for REST resources (disk or memory)",
		},
		{
			DestP:   &o.Testing,
			Flag:    "e2e-testing",
			Default: o.Testing,
			Desc:    "add /debug/flush endpoint to clear stores; used for end-to-end tests",
		},
		{
			DestP:   &o.TestingAlwaysAllowSetup,
			Flag:    "testing-always-allow-setup",
			Default: o.TestingAlwaysAllowSetup,
			Desc:    "ensures the /api/v2/setup endpoint always returns true to allow onboarding",
		},
		{
			DestP:   &o.EnginePath,
			Flag:    "engine-path",
			Default: o.EnginePath,
			Desc:    "path to persistent engine files",
		},
		{
			DestP:   &o.SecretStore,
			Flag:    "secret-store",
			Default: o.SecretStore,
			Desc:    "data store for secrets (bolt or vault)",
		},
		{
			DestP:   &o.ReportingDisabled,
			Flag:    "reporting-disabled",
			Default: o.ReportingDisabled,
			Desc:    "disable sending telemetry data to https://telemetry.influxdata.com every 8 hours",
		},
		{
			DestP:   &o.PIDFile,
			Flag:    "pid-file",
			Default: o.PIDFile,
			Desc:    "write process ID to a file",
		},
		{
			DestP:   &o.OverwritePIDFile,
			Flag:    "overwrite-pid-file",
			Default: o.OverwritePIDFile,
			Desc:    "overwrite PID file if it already exists instead of exiting",
		},
		{
			DestP:   &o.SessionLength,
			Flag:    "session-length",
			Default: o.SessionLength,
			Desc:    "ttl in minutes for newly created sessions",
		},
		{
			DestP:   &o.SessionRenewDisabled,
			Flag:    "session-renew-disabled",
			Default: o.SessionRenewDisabled,
			Desc:    "disables automatically extending session ttl on request",
		},
		{
			DestP: &o.VaultConfig.Address,
			Flag:  "vault-addr",
			Desc:  "address of the Vault server expressed as a URL and port, for example: https://127.0.0.1:8200/.",
		},
		{
			DestP: &o.VaultConfig.ClientTimeout,
			Flag:  "vault-client-timeout",
			Desc:  "timeout variable. The default value is 60s.",
		},
		{
			DestP: &o.VaultConfig.MaxRetries,
			Flag:  "vault-max-retries",
			Desc:  "maximum number of retries when a 5xx error code is encountered. The default is 2, for three total attempts. Set this to 0 or less to disable retrying.",
		},
		{
			DestP: &o.VaultConfig.CACert,
			Flag:  "vault-cacert",
			Desc:  "path to a PEM-encoded CA certificate file on the local disk. This file is used to verify the Vault server's SSL certificate. This environment variable takes precedence over VAULT_CAPATH.",
		},
		{
			DestP: &o.VaultConfig.CAPath,
			Flag:  "vault-capath",
			Desc:  "path to a directory of PEM-encoded CA certificate files on the local disk. These certificates are used to verify the Vault server's SSL certificate.",
		},
		{
			DestP: &o.VaultConfig.ClientCert,
			Flag:  "vault-client-cert",
			Desc:  "path to a PEM-encoded client certificate on the local disk. This file is used for TLS communication with the Vault server.",
		},
		{
			DestP: &o.VaultConfig.ClientKey,
			Flag:  "vault-client-key",
			Desc:  "path to an unencrypted, PEM-encoded private key on disk which corresponds to the matching client certificate.",
		},
		{
			DestP: &o.VaultConfig.InsecureSkipVerify,
			Flag:  "vault-skip-verify",
			Desc:  "do not verify Vault's presented certificate before communicating with it. Setting this variable is not recommended and voids Vault's security model.",
		},
		{
			DestP: &o.VaultConfig.TLSServerName,
			Flag:  "vault-tls-server-name",
			Desc:  "name to use as the SNI host when connecting via TLS.",
		},
		{
			DestP: &o.VaultConfig.Token,
			Flag:  "vault-token",
			Desc:  "vault authentication token",
		},

		// HTTP options
		{
			DestP:   &o.HttpBindAddress,
			Flag:    "http-bind-address",
			Default: o.HttpBindAddress,
			Desc:    "bind address for the REST HTTP API",
		},
		{
			DestP:   &o.HttpReadHeaderTimeout,
			Flag:    "http-read-header-timeout",
			Default: o.HttpReadHeaderTimeout,
			Desc:    "max duration the server should spend trying to read HTTP headers for new requests. Set to 0 for no timeout",
		},
		{
			DestP:   &o.HttpReadTimeout,
			Flag:    "http-read-timeout",
			Default: o.HttpReadTimeout,
			Desc:    "max duration the server should spend trying to read the entirety of new requests. Set to 0 for no timeout",
		},
		{
			DestP:   &o.HttpWriteTimeout,
			Flag:    "http-write-timeout",
			Default: o.HttpWriteTimeout,
			Desc:    "max duration the server should spend on processing+responding to requests. Set to 0 for no timeout",
		},
		{
			DestP:   &o.HttpIdleTimeout,
			Flag:    "http-idle-timeout",
			Default: o.HttpIdleTimeout,
			Desc:    "max duration the server should keep established connections alive while waiting for new requests. Set to 0 for no timeout",
		},
		{
			DestP: &o.HttpTLSCert,
			Flag:  "tls-cert",
			Desc:  "TLS certificate for HTTPs",
		},
		{
			DestP: &o.HttpTLSKey,
			Flag:  "tls-key",
			Desc:  "TLS key for HTTPs",
		},
		{
			DestP:   &o.HttpTLSMinVersion,
			Flag:    "tls-min-version",
			Default: o.HttpTLSMinVersion,
			Desc:    "Minimum accepted TLS version",
		},
		{
			DestP:   &o.HttpTLSStrictCiphers,
			Flag:    "tls-strict-ciphers",
			Default: o.HttpTLSStrictCiphers,
			Desc:    "Restrict accept ciphers to: ECDHE_ECDSA_WITH_AES_128_GCM_SHA256, ECDHE_RSA_WITH_AES_128_GCM_SHA256, ECDHE_ECDSA_WITH_AES_256_GCM_SHA384, ECDHE_RSA_WITH_AES_256_GCM_SHA384, ECDHE_ECDSA_WITH_CHACHA20_POLY1305, ECDHE_RSA_WITH_CHACHA20_POLY1305",
		},

		{
			DestP:   &o.NoTasks,
			Flag:    "no-tasks",
			Default: o.NoTasks,
			Desc:    "disables the task scheduler",
		},
		{
			DestP:   &o.ConcurrencyQuota,
			Flag:    "query-concurrency",
			Default: o.ConcurrencyQuota,
			Desc:    "the number of queries that are allowed to execute concurrently. Set to 0 to allow an unlimited number of concurrent queries",
		},
		{
			DestP:   &o.InitialMemoryBytesQuotaPerQuery,
			Flag:    "query-initial-memory-bytes",
			Default: o.InitialMemoryBytesQuotaPerQuery,
			Desc:    "the initial number of bytes allocated for a query when it is started. If this is unset, then query-memory-bytes will be used",
		},
		{
			DestP:   &o.MemoryBytesQuotaPerQuery,
			Flag:    "query-memory-bytes",
			Default: o.MemoryBytesQuotaPerQuery,
			Desc:    "maximum number of bytes a query is allowed to use at any given time. This must be greater or equal to query-initial-memory-bytes",
		},
		{
			DestP:   &o.MaxMemoryBytes,
			Flag:    "query-max-memory-bytes",
			Default: o.MaxMemoryBytes,
			Desc:    "the maximum amount of memory used for queries. Can only be set when query-concurrency is limited. If this is unset, then this number is query-concurrency * query-memory-bytes",
		},
		{
			DestP:   &o.QueueSize,
			Flag:    "query-queue-size",
			Default: o.QueueSize,
			Desc:    "the number of queries that are allowed to be awaiting execution before new queries are rejected. Must be > 0 if query-concurrency is not unlimited",
		},
		{
			DestP: &o.FeatureFlags,
			Flag:  "feature-flags",
			Desc:  "feature flag overrides",
		},
		{
			DestP:   &o.InstanceID,
			Flag:    "instance-id",
			Default: "",
			Desc:    "add an instance id for replications to prevent collisions and allow querying by edge node",
		},

		// storage configuration
		{
			DestP:   &o.StorageConfig.WriteTimeout,
			Flag:    "storage-write-timeout",
			Default: o.StorageConfig.WriteTimeout,
			Desc:    "The max amount of time the engine will spend completing a write request before cancelling with a timeout.",
		},
		{
			DestP: &o.StorageConfig.Data.WALFsyncDelay,
			Flag:  "storage-wal-fsync-delay",
			Desc:  "The amount of time that a write will wait before fsyncing. A duration greater than 0 can be used to batch up multiple fsync calls. This is useful for slower disks or when WAL write contention is seen.",
		},
		{
			DestP: &o.StorageConfig.Data.WALMaxConcurrentWrites,
			Flag:  "storage-wal-max-concurrent-writes",
			Desc:  "The max number of writes that will attempt to write to the WAL at a time. (default <nprocs> * 2)",
		},
		{
			DestP:   &o.StorageConfig.Data.WALMaxWriteDelay,
			Flag:    "storage-wal-max-write-delay",
			Default: o.StorageConfig.Data.WALMaxWriteDelay,
			Desc:    "The max amount of time a write will wait when the WAL already has `storage-wal-max-concurrent-writes` active writes. Set to 0 to disable the timeout.",
		},
		{
			DestP: &o.StorageConfig.Data.WALFlushOnShutdown,
			Flag:  "storage-wal-flush-on-shutdown",
			Desc:  "Flushes and clears the WAL on shutdown",
		},
		{
			DestP: &o.StorageConfig.Data.ValidateKeys,
			Flag:  "storage-validate-keys",
			Desc:  "Validates incoming writes to ensure keys only have valid unicode characters.",
		},
		{
			DestP: &o.StorageConfig.Data.SkipFieldSizeValidation,
			Flag:  "storage-no-validate-field-size",
			Desc:  "Skip field-size validation on incoming writes.",
		},
		{
			DestP: &o.StorageConfig.Data.CacheMaxMemorySize,
			Flag:  "storage-cache-max-memory-size",
			Desc:  "The maximum size a shard's cache can reach before it starts rejecting writes.",
		},
		{
			DestP: &o.StorageConfig.Data.CacheSnapshotMemorySize,
			Flag:  "storage-cache-snapshot-memory-size",
			Desc:  "The size at which the engine will snapshot the cache and write it to a TSM file, freeing up memory.",
		},
		{
			DestP: &o.StorageConfig.Data.CacheSnapshotWriteColdDuration,
			Flag:  "storage-cache-snapshot-write-cold-duration",
			Desc:  "The length of time at which the engine will snapshot the cache and write it to a new TSM file if the shard hasn't received writes or deletes.",
		},
		{
			DestP: &o.StorageConfig.Data.CompactFullWriteColdDuration,
			Flag:  "storage-compact-full-write-cold-duration",
			Desc:  "The duration at which the engine will compact all TSM files in a shard if it hasn't received a write or delete.",
		},
		{
			DestP: &o.StorageConfig.Data.CompactThroughputBurst,
			Flag:  "storage-compact-throughput-burst",
			Desc:  "The rate limit in bytes per second that we will allow TSM compactions to write to disk.",
		},
		// limits
		{
			DestP: &o.StorageConfig.Data.MaxConcurrentCompactions,
			Flag:  "storage-max-concurrent-compactions",
			Desc:  "The maximum number of concurrent full and level compactions that can run at one time.  A value of 0 results in 50% of runtime.GOMAXPROCS(0) used at runtime.  Any number greater than 0 limits compactions to that value.  This setting does not apply to cache snapshotting.",
		},
		{
			DestP: &o.StorageConfig.Data.MaxIndexLogFileSize,
			Flag:  "storage-max-index-log-file-size",
			Desc:  "The threshold, in bytes, when an index write-ahead log file will compact into an index file. Lower sizes will cause log files to be compacted more quickly and result in lower heap usage at the expense of write throughput.",
		},
		{
			DestP: &o.StorageConfig.Data.SeriesIDSetCacheSize,
			Flag:  "storage-series-id-set-cache-size",
			Desc:  "The size of the internal cache used in the TSI index to store previously calculated series results.",
		},
		{
			DestP: &o.StorageConfig.Data.SeriesFileMaxConcurrentSnapshotCompactions,
			Flag:  "storage-series-file-max-concurrent-snapshot-compactions",
			Desc:  "The maximum number of concurrent snapshot compactions that can be running at one time across all series partitions in a database.",
		},
		{
			DestP: &o.StorageConfig.Data.TSMWillNeed,
			Flag:  "storage-tsm-use-madv-willneed",
			Desc:  "Controls whether we hint to the kernel that we intend to page in mmap'd sections of TSM files.",
		},
		{
			DestP: &o.StorageConfig.RetentionService.CheckInterval,
			Flag:  "storage-retention-check-interval",
			Desc:  "The interval of time when retention policy enforcement checks run.",
		},
		{
			DestP: &o.StorageConfig.PrecreatorConfig.CheckInterval,
			Flag:  "storage-shard-precreator-check-interval",
			Desc:  "The interval of time when the check to pre-create new shards runs.",
		},
		{
			DestP: &o.StorageConfig.PrecreatorConfig.AdvancePeriod,
			Flag:  "storage-shard-precreator-advance-period",
			Desc:  "The default period ahead of the endtime of a shard group that its successor group is created.",
		},

		// InfluxQL Coordinator Config
		{
			DestP: &o.CoordinatorConfig.MaxSelectPointN,
			Flag:  "influxql-max-select-point",
			Desc:  "The maximum number of points a SELECT can process. A value of 0 will make the maximum point count unlimited. This will only be checked every second so queries will not be aborted immediately when hitting the limit.",
		},
		{
			DestP: &o.CoordinatorConfig.MaxSelectSeriesN,
			Flag:  "influxql-max-select-series",
			Desc:  "The maximum number of series a SELECT can run. A value of 0 will make the maximum series count unlimited.",
		},
		{
			DestP: &o.CoordinatorConfig.MaxSelectBucketsN,
			Flag:  "influxql-max-select-buckets",
			Desc:  "The maximum number of group by time bucket a SELECT can create. A value of zero will max the maximum number of buckets unlimited.",
		},

		// NATS config
		{
			DestP:   &o.NatsPort,
			Flag:    "nats-port",
			Desc:    "deprecated: nats has been replaced",
			Default: o.NatsPort,
			Hidden:  true,
		},
		{
			DestP:   &o.NatsMaxPayloadBytes,
			Flag:    "nats-max-payload-bytes",
			Desc:    "deprecated: nats has been replaced",
			Default: o.NatsMaxPayloadBytes,
			Hidden:  true,
		},

		// Pprof config
		{
			DestP:   &o.ProfilingDisabled,
			Flag:    "pprof-disabled",
			Desc:    "Don't expose debugging information over HTTP at /debug/pprof",
			Default: o.ProfilingDisabled,
		},

		// Metrics config
		{
			DestP:   &o.MetricsDisabled,
			Flag:    "metrics-disabled",
			Desc:    "Don't expose metrics over HTTP at /metrics",
			Default: o.MetricsDisabled,
		},
		// UI Config
		{
			DestP:   &o.UIDisabled,
			Flag:    "ui-disabled",
			Default: o.UIDisabled,
			Desc:    "Disable the InfluxDB UI",
		},

		// hardening options
		// --hardening-enabled is meant to enable all hardening
		// options in one go. Today it enables the IP validator for
		// flux and pkger templates HTTP requests, and disables file://
		// protocol for pkger templates. In the future,
		// --hardening-enabled might be used to enable other security
		// features, at which point we can add per-feature flags so
		// that users can either opt into all features
		// (--hardening-enabled) or to precisely the features they
		// require. Since today there is but one feature, there is no
		// need to introduce --hardening-ip-validation-enabled (or
		// similar).
		{
			DestP:   &o.HardeningEnabled,
			Flag:    "hardening-enabled",
			Default: o.HardeningEnabled,
			Desc:    "enable hardening options (disallow private IPs within flux and templates HTTP requests; disable file URLs in templates)",
		},

		// --template-file-urls-disabled prevents file protocol URIs
		// from being used for templates.
		{
			DestP:   &o.TemplateFileUrlsDisabled,
			Flag:    "template-file-urls-disabled",
			Default: o.TemplateFileUrlsDisabled,
			Desc:    "disable template file URLs",
		},
		{
			DestP:   &o.StrongPasswords,
			Flag:    "strong-passwords",
			Default: o.StrongPasswords,
			Desc:    "enable password strength enforcement",
		},
		{
			DestP:   &o.UseHashedTokens,
			Flag:    "use-hashed-tokens",
			Default: o.UseHashedTokens,
			Desc:    "enable token hashing",
		},
	}
}

var (
	oneDotExFlagsList = []string{
		// "reporting-disabled" is valid in both 1x and 2x configs
		"bind-address", // global setting is called "http-bind-address" on 2x

		// Remaining flags, when parsed from a 1.x config file, will be in sub-sections prefixed by these headers:
		"collectd.",
		"continuous_queries.",
		"coordinator.",
		"data.",
		"graphite.",
		"http.",
		"logging.",
		"meta.",
		"monitor.",
		"opentsdb.",
		"retention.",
		"shard-precreation.",
		"subscriber.",
		"tls.",
		"udp.",
	}
)

// compareFlags checks if a given flag from the read configuration matches one from the list. If the value from the list
// ends in a ".", the given flag is check for that prefix. Otherwise, the flag is checked for equality.
func compareFlags(key, fromList string) bool {
	if strings.HasSuffix(fromList, ".") {
		return strings.HasPrefix(key, fromList)
	}

	return strings.EqualFold(key, fromList)
}

func inOneDotExFlagsList(key string) bool {
	for _, f := range oneDotExFlagsList {
		if compareFlags(key, f) {
			return true
		}
	}

	return false
}
