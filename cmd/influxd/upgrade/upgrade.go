package upgrade

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"os/user"
	"path/filepath"
	"strings"

	"github.com/influxdata/influx-cli/v2/clients"
	"github.com/influxdata/influx-cli/v2/pkg/stdio"
	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/authorization"
	"github.com/influxdata/influxdb/v2/bolt"
	"github.com/influxdata/influxdb/v2/dbrp"
	"github.com/influxdata/influxdb/v2/internal/fs"
	"github.com/influxdata/influxdb/v2/kit/cli"
	"github.com/influxdata/influxdb/v2/kit/metric"
	"github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/influxdata/influxdb/v2/kit/prom"
	"github.com/influxdata/influxdb/v2/kv"
	"github.com/influxdata/influxdb/v2/kv/migration"
	"github.com/influxdata/influxdb/v2/kv/migration/all"
	"github.com/influxdata/influxdb/v2/storage"
	"github.com/influxdata/influxdb/v2/tenant"
	authv1 "github.com/influxdata/influxdb/v2/v1/authorization"
	"github.com/influxdata/influxdb/v2/v1/services/meta"
	"github.com/influxdata/influxdb/v2/v1/services/meta/filestore"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// Simplified 1.x config.
type configV1 struct {
	Meta struct {
		Dir string `toml:"dir"`
	} `toml:"meta"`
	Data struct {
		Dir    string `toml:"dir"`
		WALDir string `toml:"wal-dir"`
	} `toml:"data"`
	Http struct {
		BindAddress  string `toml:"bind-address"`
		HttpsEnabled bool   `toml:"https-enabled"`
		AuthEnabled  bool   `toml:"auth-enabled"`
	} `toml:"http"`
}

func (c *configV1) dbURL() string {
	address := c.Http.BindAddress
	if address == "" { // fallback to default
		address = ":8086"
	}
	var url url.URL
	if c.Http.HttpsEnabled {
		url.Scheme = "https"
	} else {
		url.Scheme = "http"
	}
	if strings.HasPrefix(address, ":") { // address is just :port
		url.Host = "localhost" + address
	} else {
		url.Host = address
	}
	return url.String()
}

type optionsV1 struct {
	metaDir string
	walDir  string
	dataDir string
	dbURL   string
	// cmd option
	dbDir      string
	configFile string
}

// populateDirs sets values for expected sub-directories of o.dbDir
func (o *optionsV1) populateDirs() {
	o.metaDir = filepath.Join(o.dbDir, "meta")
	o.dataDir = filepath.Join(o.dbDir, "data")
	o.walDir = filepath.Join(o.dbDir, "wal")
}

type optionsV2 struct {
	boltPath        string
	cliConfigsPath  string
	enginePath      string
	cqPath          string
	configPath      string
	rmConflicts     bool
	useHashedTokens bool

	userName  string
	password  string
	orgName   string
	bucket    string
	orgID     platform.ID
	userID    platform.ID
	token     string
	retention string
}

type options struct {
	// flags for source InfluxDB
	source optionsV1

	// flags for target InfluxDB
	target optionsV2

	force bool
}

type logOptions struct {
	logLevel zapcore.Level
	logPath  string
}

func NewCommand(ctx context.Context, v *viper.Viper) (*cobra.Command, error) {

	// target flags
	v2dir, err := fs.InfluxDir()
	if err != nil {
		return nil, fmt.Errorf("error fetching default InfluxDB 2.0 dir: %w", err)
	}

	// DEPRECATED in favor of log-level=debug, but left for backwards-compatibility
	verbose := false
	logOptions := &logOptions{}
	options := &options{}

	cmd := &cobra.Command{
		Use:   "upgrade",
		Short: "Upgrade a 1.x version of InfluxDB",
		Long: `
    Upgrades a 1.x version of InfluxDB by performing the following actions:
      1. Reads the 1.x config file and creates a 2.x config file with matching options. Unsupported 1.x options are reported.
      2. Copies 1.x database files.
      3. Creates influx CLI configurations.
      4. Exports any 1.x continuous queries to disk.

    If --config-file is not passed, 1.x db folder (--v1-dir options) is taken as an input. If neither option is given,
    the CLI will search for config under ${HOME}/.influxdb/ and /etc/influxdb/. If config can't be found, the CLI assumes
    a standard V1 directory structure under ${HOME}/.influxdb/.

    Target 2.x database dir is specified by the --engine-path option. If changed, the bolt path should be changed as well.
`,
		RunE: func(cmd *cobra.Command, _ []string) error {
			logger, err := buildLogger(logOptions, verbose)
			if err != nil {
				return err
			}
			return runUpgradeE(ctx, clients.CLI{StdIO: stdio.TerminalStdio}, options, logger)
		},
		Args: cobra.NoArgs,
	}

	opts := []cli.Opt{
		{
			DestP: &options.source.dbDir,
			Flag:  "v1-dir",
			Desc:  "path to source 1.x db directory containing meta, data and wal sub-folders",
		},
		{
			DestP:   &verbose,
			Flag:    "verbose",
			Default: false,
			Desc:    "DEPRECATED: use --log-level=debug instead",
			Short:   'v',
			Hidden:  true,
		},
		{
			DestP:   &options.target.boltPath,
			Flag:    "bolt-path",
			Default: filepath.Join(v2dir, bolt.DefaultFilename),
			Desc:    "path for boltdb database",
			Short:   'm',
		},
		{
			DestP:   &options.target.cliConfigsPath,
			Flag:    "influx-configs-path",
			Default: filepath.Join(v2dir, "configs"),
			Desc:    "path for 2.x CLI configurations file",
			Short:   'c',
		},
		{
			DestP:   &options.target.enginePath,
			Flag:    "engine-path",
			Default: filepath.Join(v2dir, "engine"),
			Desc:    "path for persistent engine files",
			Short:   'e',
		},
		{
			DestP:   &options.target.cqPath,
			Flag:    "continuous-query-export-path",
			Default: filepath.Join(homeOrAnyDir(), "continuous_queries.txt"),
			Desc:    "path for exported 1.x continuous queries",
		},
		{
			DestP:   &options.target.useHashedTokens,
			Flag:    "use-hashed-tokens",
			Default: options.target.useHashedTokens,
			Desc:    "enable token hashing",
		},
		{
			DestP:   &options.target.userName,
			Flag:    "username",
			Default: "",
			Desc:    "primary username",
			Short:   'u',
		},
		{
			DestP:   &options.target.password,
			Flag:    "password",
			Default: "",
			Desc:    "password for username",
			Short:   'p',
		},
		{
			DestP:   &options.target.orgName,
			Flag:    "org",
			Default: "",
			Desc:    "primary organization name",
			Short:   'o',
		},
		{
			DestP:   &options.target.bucket,
			Flag:    "bucket",
			Default: "",
			Desc:    "primary bucket name",
			Short:   'b',
		},
		{
			DestP:   &options.target.retention,
			Flag:    "retention",
			Default: "",
			Desc:    "optional: duration bucket will retain data (i.e '1w' or '72h'). Default is infinite.",
			Short:   'r',
		},
		{
			DestP:   &options.target.token,
			Flag:    "token",
			Default: "",
			Desc:    "optional: token for username, else auto-generated",
			Short:   't',
		},
		{
			DestP: &options.source.configFile,
			Flag:  "config-file",
			Desc:  "optional: Custom InfluxDB 1.x config file path, else the default config file",
		},
		{
			DestP:   &options.target.configPath,
			Flag:    "v2-config-path",
			Default: filepath.Join(v2dir, "config.toml"),
			Desc:    "optional: Custom path where upgraded 2.x config should be written",
		},
		{
			DestP:   &logOptions.logLevel,
			Flag:    "log-level",
			Default: zapcore.InfoLevel,
			Desc:    "supported log levels are debug, info, warn and error",
		},
		{
			DestP:   &logOptions.logPath,
			Flag:    "log-path",
			Default: filepath.Join(homeOrAnyDir(), "upgrade.log"),
			Desc:    "optional: custom log file path",
		},
		{
			DestP:   &options.force,
			Flag:    "force",
			Default: false,
			Desc:    "skip the confirmation prompt",
			Short:   'f',
		},
		{
			DestP:   &options.target.rmConflicts,
			Flag:    "overwrite-existing-v2",
			Default: false,
			Desc:    "if files are present at an output path, overwrite them instead of aborting the upgrade process",
		},
	}

	if err := cli.BindOptions(v, cmd, opts); err != nil {
		return nil, err
	}
	// add sub commands
	cmd.AddCommand(v1DumpMetaCommand)
	cmd.AddCommand(v2DumpMetaCommand)
	return cmd, nil
}

type influxDBv1 struct {
	meta *meta.Client
}

type influxDBv2 struct {
	log         *zap.Logger
	boltClient  *bolt.Client
	store       *bolt.KVStore
	kvStore     kv.SchemaStore
	tenantStore *tenant.Store
	ts          *tenant.Service
	dbrpSvc     influxdb.DBRPMappingService
	bucketSvc   influxdb.BucketService
	onboardSvc  influxdb.OnboardingService
	authSvc     *authv1.Service
	authSvcV2   influxdb.AuthorizationService
	meta        *meta.Client
}

func (i *influxDBv2) close() error {
	err := i.meta.Close()
	if err != nil {
		return err
	}
	err = i.boltClient.Close()
	if err != nil {
		return err
	}
	err = i.store.Close()
	if err != nil {
		return err
	}
	return nil
}

func buildLogger(options *logOptions, verbose bool) (*zap.Logger, error) {
	config := zap.NewProductionConfig()

	config.Level = zap.NewAtomicLevelAt(options.logLevel)
	if verbose {
		config.Level.SetLevel(zap.DebugLevel)
	}
	logPath, err := options.zapSafeLogPath()
	if err != nil {
		return nil, err
	}

	config.OutputPaths = append(config.OutputPaths, logPath)
	config.ErrorOutputPaths = append(config.ErrorOutputPaths, logPath)

	log, err := config.Build()
	if err != nil {
		return nil, err
	}
	if verbose {
		log.Warn("--verbose is deprecated, use --log-level=debug instead")
	}
	return log, nil
}

func runUpgradeE(ctx context.Context, cli clients.CLI, options *options, log *zap.Logger) error {
	if options.source.configFile != "" && options.source.dbDir != "" {
		return errors.New("only one of --v1-dir or --config-file may be specified")
	}

	if options.source.configFile == "" && options.source.dbDir == "" {
		// Try finding config at usual paths
		options.source.configFile = influxConfigPathV1()
		// If not found, try loading a V1 dir under HOME.
		if options.source.configFile == "" {
			v1dir, err := influxDirV1()
			if err != nil {
				return fmt.Errorf("error fetching default InfluxDB 1.x dir: %w", err)
			}
			options.source.dbDir = v1dir
		}
	}

	v1Config := &configV1{}
	var genericV1ops *map[string]interface{}
	var err error

	if options.source.configFile != "" {
		// If config is present, use it to set data paths.
		v1Config, genericV1ops, err = loadV1Config(options.source.configFile)
		if err != nil {
			return err
		}
		options.source.metaDir = v1Config.Meta.Dir
		options.source.dataDir = v1Config.Data.Dir
		options.source.walDir = v1Config.Data.WALDir
	} else {
		// Otherwise, assume a standard directory layout
		// and the default port on localhost.
		options.source.populateDirs()
	}

	options.source.dbURL = v1Config.dbURL()
	if err := options.source.validatePaths(); err != nil {
		return err
	}
	checkV2paths := options.target.validatePaths
	if options.target.rmConflicts {
		checkV2paths = options.target.clearPaths
	}
	if err := checkV2paths(); err != nil {
		return err
	}

	log.Info("Starting InfluxDB 1.x upgrade")

	if genericV1ops != nil {
		log.Info("Upgrading config file", zap.String("file", options.source.configFile))
		if err := upgradeConfig(*genericV1ops, options.target, log); err != nil {
			return err
		}
		log.Info(
			"Config file upgraded.",
			zap.String("1.x config", options.source.configFile),
			zap.String("2.x config", options.target.configPath),
		)

	} else {
		log.Info("No InfluxDB 1.x config file specified, skipping its upgrade")
	}

	log.Info("Upgrade source paths", zap.String("meta", options.source.metaDir), zap.String("data", options.source.dataDir))
	log.Info("Upgrade target paths", zap.String("bolt", options.target.boltPath), zap.String("engine", options.target.enginePath))

	v1, err := newInfluxDBv1(&options.source)
	if err != nil {
		return err
	}

	v2, err := newInfluxDBv2(ctx, &options.target, log)
	if err != nil {
		return err
	}

	defer func() {
		if err := v2.close(); err != nil {
			log.Error("Failed to close 2.0 services", zap.Error(err))
		}
	}()

	canOnboard, err := v2.onboardSvc.IsOnboarding(ctx)
	if err != nil {
		return err
	}

	if !canOnboard {
		return errors.New("InfluxDB has been already set up")
	}

	req, err := onboardingRequest(cli, options)
	if err != nil {
		return err
	}
	or, err := setupAdmin(ctx, v2, req)
	if err != nil {
		return err
	}

	options.target.orgID = or.Org.ID
	options.target.userID = or.User.ID
	options.target.token = or.Auth.Token

	err = saveLocalConfig(&options.source, &options.target, log)
	if err != nil {
		return err
	}

	db2BucketIds, err := upgradeDatabases(ctx, cli, v1, v2, options, or.Org.ID, log)
	if err != nil {
		// remove all files
		log.Error("Database upgrade error, removing data", zap.Error(err))
		if e := os.Remove(options.target.boltPath); e != nil {
			log.Error("Unable to remove bolt database", zap.Error(e))
		}

		if e := os.RemoveAll(options.target.enginePath); e != nil {
			log.Error("Unable to remove time series data", zap.Error(e))
		}
		return err
	}

	usersUpgraded, err := upgradeUsers(ctx, v1, v2, &options.target, db2BucketIds, log)
	if err != nil {
		return err
	}
	if usersUpgraded > 0 && !v1Config.Http.AuthEnabled {
		log.Warn(
			"1.x users were upgraded, but 1.x auth was not enabled. Existing clients will fail authentication against 2.x if using invalid credentials",
		)
	}

	log.Info(
		"Upgrade successfully completed. Start the influxd service now, then log in",
		zap.String("login_url", options.source.dbURL),
	)

	return nil
}

// validatePaths ensures that all paths pointing to V1 inputs are usable by the upgrade command.
func (o *optionsV1) validatePaths() error {
	if o.dbDir != "" {
		fi, err := os.Stat(o.dbDir)
		if err != nil {
			return fmt.Errorf("1.x DB dir '%s' does not exist", o.dbDir)
		}
		if !fi.IsDir() {
			return fmt.Errorf("1.x DB dir '%s' is not a directory", o.dbDir)
		}
	}

	metaDb := filepath.Join(o.metaDir, "meta.db")
	_, err := os.Stat(metaDb)
	if err != nil {
		return fmt.Errorf("1.x meta.db '%s' does not exist: %w", metaDb, err)
	}

	return nil
}

// validatePaths ensures that none of the paths pointing to V2 outputs refer to existing files.
func (o *optionsV2) validatePaths() error {
	if o.configPath != "" {
		if _, err := os.Stat(o.configPath); err == nil {
			return fmt.Errorf("file present at target path for upgraded 2.x config file %q", o.configPath)
		} else if !os.IsNotExist(err) {
			return fmt.Errorf("error checking for existing file at %q: %w", o.configPath, err)
		}
	}

	if _, err := os.Stat(o.boltPath); err == nil {
		return fmt.Errorf("file present at target path for upgraded 2.x bolt DB: %q", o.boltPath)
	} else if !os.IsNotExist(err) {
		return fmt.Errorf("error checking for existing file at %q: %w", o.boltPath, err)
	}

	if fi, err := os.Stat(o.enginePath); err == nil {
		if !fi.IsDir() {
			return fmt.Errorf("upgraded 2.x engine path %q is not a directory", o.enginePath)
		}
		entries, err := os.ReadDir(o.enginePath)
		if err != nil {
			return fmt.Errorf("error checking contents of existing engine directory %q: %w", o.enginePath, err)
		}
		if len(entries) > 0 {
			return fmt.Errorf("upgraded 2.x engine directory %q must be empty", o.enginePath)
		}
	} else if !os.IsNotExist(err) {
		return fmt.Errorf("error checking for existing file at %q: %w", o.enginePath, err)
	}

	if _, err := os.Stat(o.cliConfigsPath); err == nil {
		return fmt.Errorf("file present at target path for 2.x CLI configs %q", o.cliConfigsPath)
	} else if !os.IsNotExist(err) {
		return fmt.Errorf("error checking for existing file at %q: %w", o.cliConfigsPath, err)
	}

	if _, err := os.Stat(o.cqPath); err == nil {
		return fmt.Errorf("file present at target path for exported continuous queries %q", o.cqPath)
	} else if !os.IsNotExist(err) {
		return fmt.Errorf("error checking for existing file at %q: %w", o.cqPath, err)
	}

	return nil
}

// clearPaths deletes any files already present at the specified V2 output paths.
func (o *optionsV2) clearPaths() error {
	if o.configPath != "" {
		if err := os.RemoveAll(o.configPath); err != nil {
			return fmt.Errorf("couldn't delete existing file at %q: %w", o.configPath, err)
		}
	}

	if err := os.RemoveAll(o.boltPath); err != nil {
		return fmt.Errorf("couldn't delete existing file at %q: %w", o.boltPath, err)
	}

	if err := os.RemoveAll(o.enginePath); err != nil {
		return fmt.Errorf("couldn't delete existing file at %q: %w", o.enginePath, err)
	}

	if err := os.RemoveAll(o.cliConfigsPath); err != nil {
		return fmt.Errorf("couldn't delete existing file at %q: %w", o.cliConfigsPath, err)
	}

	if err := os.RemoveAll(o.cqPath); err != nil {
		return fmt.Errorf("couldn't delete existing file at %q: %w", o.cqPath, err)
	}

	return nil
}

func newInfluxDBv1(opts *optionsV1) (svc *influxDBv1, err error) {
	svc = &influxDBv1{}
	svc.meta, err = openV1Meta(opts.metaDir)
	if err != nil {
		return nil, fmt.Errorf("error opening 1.x meta.db: %w", err)
	}

	return svc, nil
}

func newInfluxDBv2(ctx context.Context, opts *optionsV2, log *zap.Logger) (svc *influxDBv2, err error) {
	reg := prom.NewRegistry(log.With(zap.String("service", "prom_registry")))

	svc = &influxDBv2{}
	svc.log = log

	// Create BoltDB store and K/V service
	svc.boltClient = bolt.NewClient(log.With(zap.String("service", "bolt")))
	svc.boltClient.Path = opts.boltPath
	if err := svc.boltClient.Open(ctx); err != nil {
		log.Error("Failed opening bolt", zap.Error(err))
		return nil, err
	}

	svc.store = bolt.NewKVStore(log.With(zap.String("service", "kvstore-bolt")), opts.boltPath)
	svc.store.WithDB(svc.boltClient.DB())
	svc.kvStore = svc.store

	// ensure migrator is run
	migrator, err := migration.NewMigrator(
		log.With(zap.String("service", "migrations")),
		svc.kvStore,
		all.Migrations[:]...,
	)
	if err != nil {
		log.Error("Failed to initialize kv migrator", zap.Error(err))
		return nil, err
	}

	// apply migrations to metadata store
	if err := migrator.Up(ctx); err != nil {
		log.Error("Failed to apply migrations", zap.Error(err))
		return nil, err
	}

	// Create Tenant service (orgs, buckets, )
	svc.tenantStore = tenant.NewStore(svc.kvStore)
	svc.ts = tenant.NewSystem(svc.tenantStore, log.With(zap.String("store", "new")), reg, false, metric.WithSuffix("new"))

	svc.meta = meta.NewClient(meta.NewConfig(), svc.kvStore)
	if err := svc.meta.Open(); err != nil {
		return nil, err
	}

	// DB/RP service
	svc.dbrpSvc = dbrp.NewService(ctx, svc.ts.BucketService, svc.kvStore)
	svc.bucketSvc = svc.ts.BucketService

	engine := storage.NewEngine(
		opts.enginePath,
		storage.NewConfig(),
		storage.WithMetaClient(svc.meta),
	)

	svc.ts.BucketService = storage.NewBucketService(log, svc.ts.BucketService, engine)

	hashVariantName := authorization.DefaultHashVariantName // In the future this could come from opts.
	authStoreV2, err := authorization.NewStore(ctx, svc.store, opts.useHashedTokens, authorization.WithAuthorizationHashVariantName(hashVariantName))
	if err != nil {
		return nil, err
	}

	svc.authSvcV2 = authorization.NewService(authStoreV2, svc.ts)

	// on-boarding service (influx setup)
	svc.onboardSvc = tenant.NewOnboardService(svc.ts, svc.authSvcV2)

	// v1 auth service
	authStoreV1, err := authv1.NewStore(svc.kvStore)
	if err != nil {
		return nil, err
	}

	svc.authSvc = authv1.NewService(authStoreV1, svc.ts)

	return svc, nil
}

func openV1Meta(dir string) (*meta.Client, error) {
	cfg := meta.NewConfig()
	cfg.Dir = dir
	store := filestore.New(cfg.Dir, string(meta.BucketName), "meta.db")
	c := meta.NewClient(cfg, store)
	if err := c.Open(); err != nil {
		return nil, err
	}

	return c, nil
}

// influxDirV1 retrieves the influxdb directory.
func influxDirV1() (string, error) {
	var dir string
	// By default, store meta and data files in current users home directory
	u, err := user.Current()
	if err == nil {
		dir = u.HomeDir
	} else if home := os.Getenv("HOME"); home != "" {
		dir = home
	} else {
		wd, err := os.Getwd()
		if err != nil {
			return "", err
		}
		dir = wd
	}
	dir = filepath.Join(dir, ".influxdb")

	return dir, nil
}

// influxConfigPathV1 returns default 1.x config file path or empty path if not found.
func influxConfigPathV1() string {
	if envVar := os.Getenv("INFLUXDB_CONFIG_PATH"); envVar != "" {
		return envVar
	}
	for _, path := range []string{
		os.ExpandEnv("${HOME}/.influxdb/influxdb.conf"),
		"/etc/influxdb/influxdb.conf",
	} {
		if _, err := os.Stat(path); err == nil {
			return path
		}
	}

	return ""
}

// homeOrAnyDir retrieves user's home directory, current working one or just none.
func homeOrAnyDir() string {
	var dir string
	u, err := user.Current()
	if err == nil {
		dir = u.HomeDir
	} else if home := os.Getenv("HOME"); home != "" {
		dir = home
	} else if home := os.Getenv("USERPROFILE"); home != "" {
		dir = home
	} else {
		wd, err := os.Getwd()
		if err != nil {
			dir = ""
		} else {
			dir = wd
		}
	}

	return dir
}
