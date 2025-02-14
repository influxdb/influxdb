package downgrade

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/bolt"
	"github.com/influxdata/influxdb/v2/internal/fs"
	"github.com/influxdata/influxdb/v2/kit/cli"
	"github.com/influxdata/influxdb/v2/kv/migration"
	"github.com/influxdata/influxdb/v2/kv/migration/all"
	influxlogger "github.com/influxdata/influxdb/v2/logger"
	"github.com/influxdata/influxdb/v2/sqlite"
	sqliteMigrations "github.com/influxdata/influxdb/v2/sqlite/migrations"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

const (
	backupPathFormat = "%s.%s-pre-%s-downgrade.backup"
)

type migrationTarget struct {
	kvMigration, sqlMigration int
}

// migrationTarget int values mean "migrate to this migration number". For example, kvMigration of 15 will result in the
// downgraded database including migration 15, but migration 15 will not be undone.
var downgradeMigrationTargets = map[string]migrationTarget{
	"2.0": {kvMigration: 15, sqlMigration: 0},
	"2.1": {kvMigration: 18, sqlMigration: 3},
	"2.3": {kvMigration: 20, sqlMigration: 5},
	"2.4": {kvMigration: 20, sqlMigration: 7},
}

func NewCommand(ctx context.Context, v *viper.Viper) (*cobra.Command, error) {
	v2dir, err := fs.InfluxDir()
	if err != nil {
		return nil, fmt.Errorf("error fetching default InfluxDB 2.0 dir: %w", err)
	}

	var validDowngradeTargets []string
	for k := range downgradeMigrationTargets {
		validDowngradeTargets = append(validDowngradeTargets, k)
	}
	var validTargetsHelp string
	if len(validDowngradeTargets) == 1 {
		validTargetsHelp = validDowngradeTargets[0]
	} else {
		validTargetsHelp = fmt.Sprintf("<%s>", strings.Join(validDowngradeTargets, "|"))
	}

	var sqlitePath string
	var boltPath string
	var logLevel zapcore.Level

	cmd := &cobra.Command{
		Use:   fmt.Sprintf("downgrade [flags] %s", validTargetsHelp),
		Short: "Downgrade metadata schema used by influxd to match the expectations of an older release",
		Long: `Run this command prior to downgrading the influxd binary.

influxd does not guarantee backwards-compatibility with older releases in its embedded
metadata stores. Attempting to boot up an older influxd on a BoltDB/SQLite file that has
been migrated to a newer schema will result in a startup error. This command downgrades
those metadata schemas to match the expectations of an older release, allowing the older
influxd binary to boot successfully.

The target version of the downgrade must be specified, i.e. "influxd downgrade 2.0".
`,
		ValidArgs: validDowngradeTargets,
		Args:      cobra.MatchAll(cobra.ExactArgs(1), cobra.OnlyValidArgs),
		RunE: func(cmd *cobra.Command, args []string) error {
			logconf := &influxlogger.Config{
				Format: "auto",
				Level:  logLevel,
			}
			logger, err := logconf.New(os.Stdout)
			if err != nil {
				return err
			}

			return downgrade(ctx, boltPath, sqlitePath, args[0], logger)
		},
	}

	opts := []cli.Opt{
		{
			DestP:   &boltPath,
			Flag:    "bolt-path",
			Default: filepath.Join(v2dir, bolt.DefaultFilename),
			Desc:    "path for boltdb database",
		},
		{
			DestP: &sqlitePath,
			Flag:  "sqlite-path",
			Desc:  fmt.Sprintf("path to sqlite database. if not set, the database is assumed to be in the bolt-path directory as %q", sqlite.DefaultFilename),
		},
		{
			DestP:   &logLevel,
			Flag:    "log-level",
			Default: zapcore.InfoLevel,
			Desc:    "supported log levels are debug, info, warn and error",
		},
	}
	if err := cli.BindOptions(v, cmd, opts); err != nil {
		return nil, err
	}

	if sqlitePath == "" {
		sqlitePath = filepath.Join(filepath.Dir(boltPath), sqlite.DefaultFilename)
	}

	return cmd, nil
}

func downgrade(ctx context.Context, boltPath, sqlitePath, targetVersion string, log *zap.Logger) error {
	info := influxdb.GetBuildInfo()

	n, err := compareVersionStrings(targetVersion, "2.4.0")
	if n < 0 || err != nil {
		errStr := "if the target version is less than 2.4.0, any replications using bucket names rather than ids will be deleted"
		log.Warn("downgrade warning", zap.String("targetVersion", errStr))
	}

	// Files must exist at the specified paths for the downgrade to work properly. The bolt and sqlite "open" methods will
	// create files if they do not exist, so their existence must be verified here.
	if _, err := os.Stat(boltPath); err != nil {
		return fmt.Errorf("invalid bolt path %q: %w", boltPath, err)
	}

	if _, err := os.Stat(sqlitePath); err != nil {
		return fmt.Errorf("invalid sqlite path %q: %w", sqlitePath, err)
	}

	// Initialize both migrators prior to attempting any migrations so that we can error out prior to mutating either DB
	// if there are errors initializing either migrator.
	boltClient := bolt.NewClient(log.With(zap.String("service", "bolt")))
	boltClient.Path = boltPath

	if err := boltClient.Open(ctx); err != nil {
		return fmt.Errorf("failed to open bolt DB: %w", err)
	}
	defer boltClient.Close()

	kvStore := bolt.NewKVStore(log.With(zap.String("service", "kvstore-bolt")), boltPath)
	kvStore.WithDB(boltClient.DB())

	kvMigrator, err := migration.NewMigrator(log.With(zap.String("service", "kv-migrator")), kvStore)
	if err != nil {
		return fmt.Errorf("failed to initialize KV migrator: %w", err)
	}

	kvMigrator.SetBackupPath(fmt.Sprintf(backupPathFormat, boltPath, info.Version, targetVersion))
	kvMigrator.AddMigrations(all.Migrations[:]...)

	sqlStore, err := sqlite.NewSqlStore(sqlitePath, log.With(zap.String("service", "sqlstore-sqlite")))
	if err != nil {
		return fmt.Errorf("failed to initialize SQL migrator: %w", err)
	}
	defer sqlStore.Close()

	sqlMigrator := sqlite.NewMigrator(sqlStore, log.With(zap.String("service", "sql-migrator")))
	sqlMigrator.SetBackupPath(fmt.Sprintf(backupPathFormat, sqlitePath, info.Version, targetVersion))

	log.Info("Downgrading KV metadata to target version", zap.String("version", targetVersion))
	if err := kvMigrator.Down(ctx, downgradeMigrationTargets[targetVersion].kvMigration); err != nil {
		return fmt.Errorf("failed to tear down KV migrations: %w", err)
	}

	log.Info("Downgrading SQL metadata to target version", zap.String("version", targetVersion))
	if err := sqlMigrator.Down(ctx, downgradeMigrationTargets[targetVersion].sqlMigration, sqliteMigrations.AllDown); err != nil {
		return fmt.Errorf("failed to tear down SQL migrations: %w", err)
	}

	log.Info("Metadata successfully downgraded, you can now safely replace this `influxd` with the target older version",
		zap.String("version", targetVersion))
	return nil
}

func compareVersionStrings(left string, right string) (int, error) {
	l := strings.Split(left, ".")
	r := strings.Split(right, ".")
	loop := len(r)
	if len(l) > len(r) {
		loop = len(l)
	}
	for i := 0; i < loop; i++ {
		var x, y string
		if len(l) > i {
			x = l[i]
		}
		if len(r) > i {
			y = r[i]
		}
		lefti, err := strconv.Atoi(x)
		if err != nil {
			return 0, err
		}
		righti, err := strconv.Atoi(y)
		if err != nil {
			return 0, err
		}

		if lefti > righti {
			return 1, nil
		} else if lefti < righti {
			return -1, nil
		}
	}
	return 0, nil
}
