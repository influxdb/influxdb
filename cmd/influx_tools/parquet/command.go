package parquet

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"

	"go.uber.org/zap"

	"github.com/influxdata/influxdb/cmd/influx_tools/server"
	internal_errors "github.com/influxdata/influxdb/pkg/errors"
)

// Command represents the program execution for "store query".
type Command struct {
	// Standard input/output, overridden for testing.
	Stderr io.Writer
	Logger *zap.Logger

	server server.Interface
}

// NewCommand returns a new instance of the export Command.
func NewCommand(server server.Interface) *Command {
	return &Command{
		Stderr: os.Stderr,
		server: server,
	}
}

// Run executes the export command using the specified args.
func (cmd *Command) Run(args []string) (err error) {
	var (
		configPath      string
		database        string
		rp              string
		measurements    string
		typeResolutions string
		nameResolutions string
		output          string
		dryRun          bool
	)

	cwd, err := os.Getwd()
	if err != nil {
		return fmt.Errorf("getting current working directory failed: %w", err)
	}

	flags := flag.NewFlagSet("export", flag.ContinueOnError)
	flags.StringVar(&configPath, "config", "", "Config file of the InfluxDB v1 instance")
	flags.StringVar(&database, "database", "", "Database to export")
	flags.StringVar(&rp, "rp", "", "Retention policy in the database to export")
	flags.StringVar(&measurements, "measurements", "*", "Comma-separated list of measurements to export")
	flags.StringVar(&typeResolutions, "resolve-types", "", "Comma-separated list of field type resolutions in the form <measurements>.<field>=<type>")
	flags.StringVar(&nameResolutions, "resolve-names", "", "Comma-separated list of field renamings in the form <measurements>.<field>=<new name>")
	flags.StringVar(&output, "output", cwd, "Output directory for exported parquet files")
	flags.BoolVar(&dryRun, "dry-run", false, "Print plan and exit")

	if err := flags.Parse(args); err != nil {
		return fmt.Errorf("parsing flags failed: %w", err)
	}

	if database == "" {
		return errors.New("database is required")
	}

	loggerCfg := zap.NewDevelopmentConfig()
	loggerCfg.DisableStacktrace = true
	loggerCfg.DisableCaller = true
	cmd.Logger, err = loggerCfg.Build()
	if err != nil {
		return fmt.Errorf("creating logger failed: %w", err)
	}

	if err := cmd.server.Open(configPath); err != nil {
		return fmt.Errorf("opening server failed: %w", err)
	}
	defer cmd.server.Close()

	cfg := &config{
		Database:        database,
		RP:              rp,
		Measurements:    measurements,
		TypeResolutions: typeResolutions,
		NameResolutions: nameResolutions,
		Output:          output,
	}
	exp, err := newExporter(cmd.server, cfg, cmd.Logger)
	if err != nil {
		return err
	}

	ctx := context.Background()
	if err := exp.open(ctx); err != nil {
		return fmt.Errorf("opening exporter failed: %w", err)
	}
	defer internal_errors.Capture(&err, exp.close)

	exp.printPlan(cmd.Stderr)

	if dryRun {
		return nil
	}

	return exp.export(ctx)
}
