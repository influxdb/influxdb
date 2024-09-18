// The influx_tools command displays detailed information about InfluxDB data files.
package main

import (
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/influxdata/influxdb/cmd"
	"github.com/influxdata/influxdb/cmd/influx_tools/compact"
	"github.com/influxdata/influxdb/cmd/influx_tools/export"
	genexec "github.com/influxdata/influxdb/cmd/influx_tools/generate/exec"
	geninit "github.com/influxdata/influxdb/cmd/influx_tools/generate/init"
	"github.com/influxdata/influxdb/cmd/influx_tools/help"
	"github.com/influxdata/influxdb/cmd/influx_tools/importer"
	"github.com/influxdata/influxdb/cmd/influx_tools/parquet"
	"github.com/influxdata/influxdb/cmd/influx_tools/server"
	"github.com/influxdata/influxdb/cmd/influxd/run"
	"github.com/influxdata/influxdb/services/meta"
	"github.com/influxdata/influxdb/tsdb"
	_ "github.com/influxdata/influxdb/tsdb/engine"
	"go.uber.org/zap"
)

func main() {
	m := NewMain()
	if err := m.Run(os.Args[1:]...); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

// Main represents the program execution.
type Main struct {
	Stdin  io.Reader
	Stdout io.Writer
	Stderr io.Writer
}

// NewMain returns a new instance of Main.
func NewMain() *Main {
	return &Main{
		Stdin:  os.Stdin,
		Stdout: os.Stdout,
		Stderr: os.Stderr,
	}
}

// Run determines and runs the command specified by the CLI args.
func (m *Main) Run(args ...string) error {
	name, args := cmd.ParseCommandName(args)

	// Extract name from args.
	switch name {
	case "", "help":
		if err := help.NewCommand().Run(args...); err != nil {
			return fmt.Errorf("help failed: %s", err)
		}
	case "compact-shard":
		c := compact.NewCommand()
		if err := c.Run(args); err != nil {
			return fmt.Errorf("compact-shard failed: %s", err)
		}
	case "export":
		c := export.NewCommand(&ossServer{logger: zap.NewNop()})
		if err := c.Run(args); err != nil {
			return fmt.Errorf("export failed: %s", err)
		}
	case "export-parquet":
		c := parquet.NewCommand(&ossServer{logger: zap.NewNop()})
		if err := c.Run(args); err != nil {
			return fmt.Errorf("export failed: %s", err)
		}
	case "import":
		c := importer.NewCommand(&ossServer{logger: zap.NewNop()})
		if err := c.Run(args); err != nil {
			return fmt.Errorf("import failed: %s", err)
		}
	case "gen-init":
		c := geninit.NewCommand(&ossServer{logger: zap.NewNop()})
		if err := c.Run(args); err != nil {
			return fmt.Errorf("gen-init failed: %s", err)
		}
	case "gen-exec":
		deps := genexec.Dependencies{Server: &ossServer{logger: zap.NewNop()}}
		c := genexec.NewCommand(deps)
		if err := c.Run(args); err != nil {
			return fmt.Errorf("gen-exec failed: %s", err)
		}
	default:
		return fmt.Errorf(`unknown command "%s"`+"\n"+`Run 'influx-tools help' for usage`+"\n\n", name)
	}

	return nil
}

type ossServer struct {
	logger   *zap.Logger
	config   *run.Config
	noClient bool
	client   *meta.Client
	mc       server.MetaClient
}

func (s *ossServer) Open(path string) (err error) {
	s.config, err = s.parseConfig(path)
	if err != nil {
		return err
	}

	// Validate the configuration.
	if err = s.config.Validate(); err != nil {
		return fmt.Errorf("validate config: %s", err)
	}

	if s.noClient {
		return nil
	}

	s.client = meta.NewClient(s.config.Meta)
	if err = s.client.Open(); err != nil {
		s.client = nil
		return err
	}
	s.mc = &ossMetaClient{s.client}
	return nil
}

func (s *ossServer) Close() {
	if s.client != nil {
		s.client.Close()
		s.client = nil
	}
}

func (s *ossServer) MetaClient() server.MetaClient { return s.mc }
func (s *ossServer) TSDBConfig() tsdb.Config       { return s.config.Data }
func (s *ossServer) Logger() *zap.Logger           { return s.logger }
func (s *ossServer) NodeID() uint64                { return 0 }

// ParseConfig parses the config at path.
// It returns a demo configuration if path is blank.
func (s *ossServer) parseConfig(path string) (*run.Config, error) {
	path = s.resolvePath(path)
	// Use demo configuration if no config path is specified.
	if path == "" {
		return nil, errors.New("missing config file")
	}

	config := run.NewConfig()
	if err := config.FromTomlFile(path); err != nil {
		return nil, err
	}

	return config, nil
}

func (s *ossServer) resolvePath(path string) string {
	if path != "" {
		if path == os.DevNull {
			return ""
		}
		return path
	}

	for _, p := range []string{
		os.ExpandEnv("${HOME}/.influxdb/influxdb.conf"),
		"/etc/influxdb/influxdb.conf",
	} {
		if _, err := os.Stat(p); err == nil {
			return p
		}
	}
	return ""
}

type ossMetaClient struct {
	*meta.Client
}

func (*ossMetaClient) NodeID() uint64 { return 0 }

func (c *ossMetaClient) NodeShardGroupsByTimeRange(database, policy string, min, max time.Time) (a []meta.ShardGroupInfo, err error) {
	return c.ShardGroupsByTimeRange(database, policy, min, max)
}
