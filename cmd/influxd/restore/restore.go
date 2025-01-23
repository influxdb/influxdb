// Package restore is the restore subcommand for the influxd command,
// for restoring from a backup.
package restore

import (
	"archive/tar"
	"bytes"
	"encoding/binary"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/influxdata/influxdb/cmd/influxd/backup_util"
	tarstream "github.com/influxdata/influxdb/pkg/tar"
	"github.com/influxdata/influxdb/services/meta"
	"github.com/influxdata/influxdb/services/snapshotter"
	gzip "github.com/klauspost/pgzip"
)

// Command represents the program execution for "influxd restore".
type Command struct {
	// The logger passed to the ticker during execution.
	StdoutLogger *log.Logger
	StderrLogger *log.Logger

	// Standard input/output, overridden for testing.
	Stderr io.Writer
	Stdout io.Writer

	host   string
	client *snapshotter.Client

	backupFilesPath     string
	metadir             string
	datadir             string
	destinationDatabase string
	sourceDatabase      string
	backupRetention     string
	restoreRetention    string
	shard               uint64
	portable            bool
	online              bool
	manifestMeta        *backup_util.MetaEntry
	manifestFiles       map[uint64]*backup_util.Entry

	// TODO: when the new meta stuff is done this should not be exported or be gone
	MetaConfig *meta.Config

	shardIDMap map[uint64]uint64
}

// NewCommand returns a new instance of Command with default settings.
func NewCommand() *Command {
	return &Command{
		Stdout:     os.Stdout,
		Stderr:     os.Stderr,
		MetaConfig: meta.NewConfig(),
	}
}

// Run executes the program.
func (cmd *Command) Run(args ...string) error {
	// Set up logger.
	cmd.StdoutLogger = log.New(cmd.Stdout, "", log.LstdFlags)
	cmd.StderrLogger = log.New(cmd.Stderr, "", log.LstdFlags)
	if err := cmd.parseFlags(args); err != nil {
		return err
	}

	if cmd.portable {
		return cmd.runOnlinePortable()
	} else if cmd.online {
		return cmd.runOnlineLegacy()
	} else {
		return cmd.runOffline()
	}
}

func (cmd *Command) runOffline() error {
	if cmd.metadir != "" {
		if err := cmd.unpackMeta(); err != nil {
			return err
		}
	}

	if cmd.shard != 0 {
		return cmd.unpackShard(cmd.shard)
	} else if cmd.restoreRetention != "" {
		return cmd.unpackRetention()
	} else if cmd.datadir != "" {
		return cmd.unpackDatabase()
	}
	return nil
}

func (cmd *Command) runOnlinePortable() error {
	err := cmd.updateMetaPortable()
	if err != nil {
		cmd.StderrLogger.Printf("error updating meta: %v", err)
		return err
	}
	err = cmd.uploadShardsPortable()
	if err != nil {
		cmd.StderrLogger.Printf("error updating shards: %v", err)
		return err
	}
	return nil
}

func (cmd *Command) runOnlineLegacy() error {
	err := cmd.updateMetaLegacy()
	if err != nil {
		cmd.StderrLogger.Printf("error updating meta: %v", err)
		return err
	}
	err = cmd.uploadShardsLegacy()
	if err != nil {
		cmd.StderrLogger.Printf("error updating shards: %v", err)
		return err
	}
	return nil
}

// parseFlags parses and validates the command line arguments.
func (cmd *Command) parseFlags(args []string) error {
	fs := flag.NewFlagSet("", flag.ContinueOnError)
	fs.StringVar(&cmd.host, "host", "localhost:8088", "")
	fs.StringVar(&cmd.metadir, "metadir", "", "")
	fs.StringVar(&cmd.datadir, "datadir", "", "")

	fs.StringVar(&cmd.sourceDatabase, "database", "", "")
	fs.StringVar(&cmd.sourceDatabase, "db", "", "")
	fs.StringVar(&cmd.destinationDatabase, "newdb", "", "")

	fs.StringVar(&cmd.backupRetention, "retention", "", "")
	fs.StringVar(&cmd.backupRetention, "rp", "", "")
	fs.StringVar(&cmd.restoreRetention, "newrp", "", "")

	fs.Uint64Var(&cmd.shard, "shard", 0, "")
	fs.BoolVar(&cmd.online, "online", false, "")
	fs.BoolVar(&cmd.portable, "portable", false, "")
	fs.SetOutput(cmd.Stdout)
	fs.Usage = cmd.printUsage
	if err := fs.Parse(args); err != nil {
		return err
	}

	cmd.MetaConfig = meta.NewConfig()
	cmd.MetaConfig.Dir = cmd.metadir
	cmd.client = snapshotter.NewClient(cmd.host)

	// Require output path.
	cmd.backupFilesPath = fs.Arg(0)
	if cmd.backupFilesPath == "" {
		return fmt.Errorf("path with backup files required")
	}

	fi, err := os.Stat(cmd.backupFilesPath)
	if err != nil || !fi.IsDir() {
		return fmt.Errorf("backup path should be a valid directory: %s", cmd.backupFilesPath)
	}

	if cmd.destinationDatabase != "" && cmd.sourceDatabase == "" {
		return fmt.Errorf("must specify a database to be restored into new database %s", cmd.destinationDatabase)
	}

	if cmd.portable || cmd.online {
		// validate the arguments

		if cmd.metadir != "" {
			return fmt.Errorf("offline parameter metadir found, not compatible with -portable")
		}

		if cmd.datadir != "" {
			return fmt.Errorf("offline parameter datadir found, not compatible with -portable")
		}

		if cmd.restoreRetention == "" {
			cmd.restoreRetention = cmd.backupRetention
		}

		if cmd.portable {
			var err error

			cmd.manifestMeta, cmd.manifestFiles, err = backup_util.LoadIncremental(cmd.backupFilesPath)
			if err != nil {
				return fmt.Errorf("restore failed while processing manifest files: %s", err.Error())
			} else if cmd.manifestMeta == nil {
				// No manifest files found.
				return fmt.Errorf("No manifest files found in: %s\n", cmd.backupFilesPath)

			}
		}
	} else {
		// validate the arguments
		if cmd.destinationDatabase == "" {
			cmd.destinationDatabase = cmd.sourceDatabase
		}

		if cmd.metadir == "" && cmd.destinationDatabase == "" {
			return fmt.Errorf("-metadir or -newdb are required to restore")
		}

		if cmd.destinationDatabase != "" && cmd.datadir == "" {
			return fmt.Errorf("-datadir is required to restore")
		}

		if cmd.shard != 0 {
			if cmd.destinationDatabase == "" {
				return fmt.Errorf("-newdb is required to restore shard")
			}
			if cmd.backupRetention == "" {
				return fmt.Errorf("-retention is required to restore shard")
			}
		} else if cmd.backupRetention != "" && cmd.destinationDatabase == "" {
			return fmt.Errorf("-newdb is required to restore retention policy")
		}
	}

	return nil
}

// unpackMeta reads the metadata from the backup directory and initializes a raft
// cluster and replaces the root metadata.
func (cmd *Command) unpackMeta() error {
	// find the meta file
	metaFiles, err := filepath.Glob(filepath.Join(cmd.backupFilesPath, backup_util.Metafile+".*"))
	if err != nil {
		return err
	}

	if len(metaFiles) == 0 {
		return fmt.Errorf("no metastore backups in %s", cmd.backupFilesPath)
	}

	latest := metaFiles[len(metaFiles)-1]

	fmt.Fprintf(cmd.Stdout, "Using metastore snapshot: %v\n", latest)
	// Read the metastore backup
	f, err := os.Open(latest)
	if err != nil {
		return err
	}

	var buf bytes.Buffer
	if _, err := io.Copy(&buf, f); err != nil {
		return fmt.Errorf("copy: %s", err)
	}

	b := buf.Bytes()
	var i int

	// Make sure the file is actually a meta store backup file
	magic := binary.BigEndian.Uint64(b[:8])
	if magic != snapshotter.BackupMagicHeader {
		return fmt.Errorf("invalid metadata file")
	}
	i += 8

	// Size of the meta store bytes
	length := int(binary.BigEndian.Uint64(b[i : i+8]))
	i += 8
	metaBytes := b[i : i+length]
	i += int(length)

	// Size of the node.json bytes
	length = int(binary.BigEndian.Uint64(b[i : i+8]))
	i += 8
	nodeBytes := b[i : i+length]

	// Unpack into metadata.
	var data meta.Data
	if err := data.UnmarshalBinary(metaBytes); err != nil {
		return fmt.Errorf("unmarshal: %s", err)
	}

	// Copy meta config and remove peers so it starts in single mode.
	c := cmd.MetaConfig
	c.Dir = cmd.metadir

	// Create the meta dir
	if err := os.MkdirAll(c.Dir, 0700); err != nil {
		return err
	}

	// Write node.json back to meta dir
	if err := os.WriteFile(filepath.Join(c.Dir, "node.json"), nodeBytes, 0655); err != nil {
		return err
	}

	client := meta.NewClient(c)
	if err := client.Open(); err != nil {
		return err
	}
	defer client.Close()

	// Force set the full metadata.
	if err := client.SetData(&data); err != nil {
		return fmt.Errorf("set data: %s", err)
	}

	// remove the raft.db file if it exists
	err = os.Remove(filepath.Join(cmd.metadir, "raft.db"))
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	// remove the node.json file if it exists
	err = os.Remove(filepath.Join(cmd.metadir, "node.json"))
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	return nil
}

func (cmd *Command) updateMetaPortable() error {
	var metaBytes []byte
	fileName := filepath.Join(cmd.backupFilesPath, cmd.manifestMeta.FileName)

	fileBytes, err := os.ReadFile(fileName)
	if err != nil {
		return err
	}

	var ep backup_util.PortablePacker
	ep.UnmarshalBinary(fileBytes)

	metaBytes = ep.Data

	req := &snapshotter.Request{
		Type:                   snapshotter.RequestMetaStoreUpdate,
		BackupDatabase:         cmd.sourceDatabase,
		RestoreDatabase:        cmd.destinationDatabase,
		BackupRetentionPolicy:  cmd.backupRetention,
		RestoreRetentionPolicy: cmd.restoreRetention,
		UploadSize:             int64(len(metaBytes)),
	}

	shardIDMap, err := cmd.client.UpdateMeta(req, bytes.NewReader(metaBytes))
	cmd.shardIDMap = shardIDMap
	return err

}

// updateMetaLive takes a metadata backup and sends it to the influx server
// for a live merger of metadata.
func (cmd *Command) updateMetaLegacy() error {

	var metaBytes []byte

	// find the meta file
	metaFiles, err := filepath.Glob(filepath.Join(cmd.backupFilesPath, backup_util.Metafile+".*"))
	if err != nil {
		return err
	}

	if len(metaFiles) == 0 {
		return fmt.Errorf("no metastore backups in %s", cmd.backupFilesPath)
	}

	fileName := metaFiles[len(metaFiles)-1]
	cmd.StdoutLogger.Printf("Using metastore snapshot: %v\n", fileName)
	metaBytes, err = backup_util.GetMetaBytes(fileName)
	if err != nil {
		return err
	}

	req := &snapshotter.Request{
		Type:                   snapshotter.RequestMetaStoreUpdate,
		BackupDatabase:         cmd.sourceDatabase,
		RestoreDatabase:        cmd.destinationDatabase,
		BackupRetentionPolicy:  cmd.backupRetention,
		RestoreRetentionPolicy: cmd.restoreRetention,
		UploadSize:             int64(len(metaBytes)),
	}

	shardIDMap, err := cmd.client.UpdateMeta(req, bytes.NewReader(metaBytes))
	cmd.shardIDMap = shardIDMap
	return err
}

func (cmd *Command) uploadShardsPortable() error {
	for _, file := range cmd.manifestFiles {
		if cmd.sourceDatabase == "" || cmd.sourceDatabase == file.Database {
			if cmd.backupRetention == "" || cmd.backupRetention == file.Policy {
				if cmd.shard == 0 || cmd.shard == file.ShardID {
					oldID := file.ShardID
					// if newID not found then this shard's metadata was NOT imported
					// and should be skipped
					newID, ok := cmd.shardIDMap[oldID]
					if !ok {
						cmd.StdoutLogger.Printf("Meta info not found for shard %d on database %s. Skipping shard file %s", oldID, file.Database, file.FileName)
						continue
					}
					cmd.StdoutLogger.Printf("Restoring shard %d live from backup %s\n", file.ShardID, file.FileName)
					f, err := os.Open(filepath.Join(cmd.backupFilesPath, file.FileName))
					if err != nil {
						f.Close()
						return err
					}
					gr, err := gzip.NewReader(f)
					if err != nil {
						f.Close()
						return err
					}
					tr := tar.NewReader(gr)
					targetDB := cmd.destinationDatabase
					if targetDB == "" {
						targetDB = file.Database
					}

					if err := cmd.client.UploadShard(oldID, newID, targetDB, cmd.restoreRetention, tr); err != nil {
						f.Close()
						return err
					}
					f.Close()
				}
			}
		}
	}
	return nil
}

// unpackFiles will look for backup files matching the pattern and restore them to the data dir
func (cmd *Command) uploadShardsLegacy() error {
	// find the destinationDatabase backup files
	pat := fmt.Sprintf("%s.*", filepath.Join(cmd.backupFilesPath, cmd.sourceDatabase))
	cmd.StdoutLogger.Printf("Restoring live from backup %s\n", pat)
	backupFiles, err := filepath.Glob(pat)
	if err != nil {
		return err
	}
	if len(backupFiles) == 0 {
		return fmt.Errorf("no backup files in %s", cmd.backupFilesPath)
	}

	for _, fn := range backupFiles {
		parts := strings.Split(filepath.Base(fn), ".")

		if len(parts) != 4 {
			cmd.StderrLogger.Printf("Skipping mis-named backup file: %s", fn)
		}
		shardID, err := strconv.ParseUint(parts[2], 10, 64)
		if err != nil {
			return err
		}

		// if newID not found then this shard's metadata was NOT imported
		// and should be skipped
		newID, ok := cmd.shardIDMap[shardID]
		if !ok {
			cmd.StdoutLogger.Printf("Meta info not found for shard %d. Skipping shard file %s", shardID, fn)
			continue
		}
		f, err := os.Open(fn)
		if err != nil {
			return err
		}
		tr := tar.NewReader(f)
		if err := cmd.client.UploadShard(shardID, newID, cmd.destinationDatabase, cmd.restoreRetention, tr); err != nil {
			f.Close()
			return err
		}
		f.Close()
	}

	return nil
}

// unpackDatabase will look for all backup files in the path matching this destinationDatabase
// and restore them to the data dir
func (cmd *Command) unpackDatabase() error {
	// make sure the shard isn't already there so we don't clobber anything
	restorePath := filepath.Join(cmd.datadir, cmd.sourceDatabase)
	if _, err := os.Stat(restorePath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("database already present: %s", restorePath)
	}

	// find the database backup files
	pat := filepath.Join(cmd.backupFilesPath, cmd.sourceDatabase)
	return cmd.unpackFiles(pat + ".*")
}

// unpackRetention will look for all backup files in the path matching this retention
// and restore them to the data dir
func (cmd *Command) unpackRetention() error {
	// make sure the shard isn't already there so we don't clobber anything
	restorePath := filepath.Join(cmd.datadir, cmd.sourceDatabase, cmd.backupRetention)
	if _, err := os.Stat(restorePath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("retention already present: %s", restorePath)
	}

	// find the retention backup files
	pat := filepath.Join(cmd.backupFilesPath, cmd.sourceDatabase)
	return cmd.unpackFiles(fmt.Sprintf("%s.%s.*", pat, cmd.backupRetention))
}

// unpackShard will look for all backup files in the path matching this shard ID
// and restore them to the data dir
func (cmd *Command) unpackShard(shard uint64) error {
	shardID := strconv.FormatUint(shard, 10)
	// make sure the shard isn't already there so we don't clobber anything
	restorePath := filepath.Join(cmd.datadir, cmd.sourceDatabase, cmd.backupRetention, shardID)
	if _, err := os.Stat(restorePath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("shard already present: %s", restorePath)
	}

	id, err := strconv.ParseUint(shardID, 10, 64)
	if err != nil {
		return err
	}

	// find the shard backup files
	pat := filepath.Join(cmd.backupFilesPath, fmt.Sprintf(backup_util.BackupFilePattern, cmd.sourceDatabase, cmd.backupRetention, id))
	return cmd.unpackFiles(pat + ".*")
}

// unpackFiles will look for backup files matching the pattern and restore them to the data dir
func (cmd *Command) unpackFiles(pat string) error {
	cmd.StdoutLogger.Printf("Restoring offline from backup %s\n", pat)

	backupFiles, err := filepath.Glob(pat)
	if err != nil {
		return err
	}

	if len(backupFiles) == 0 {
		return fmt.Errorf("no backup files for %s in %s", pat, cmd.backupFilesPath)
	}

	for _, fn := range backupFiles {
		if err := cmd.unpackTar(fn); err != nil {
			return err
		}
	}

	return nil
}

// unpackTar will restore a single tar archive to the data dir
func (cmd *Command) unpackTar(tarFile string) error {
	f, err := os.Open(tarFile)
	if err != nil {
		return err
	}
	defer f.Close()

	// should get us ["db","rp", "00001", "00"]
	pathParts := strings.Split(filepath.Base(tarFile), ".")
	if len(pathParts) != 4 {
		return fmt.Errorf("backup tarfile name incorrect format")
	}

	shardPath := filepath.Join(cmd.datadir, pathParts[0], pathParts[1], strings.TrimLeft(pathParts[2], "0"))
	os.MkdirAll(shardPath, 0755)

	return tarstream.Restore(f, shardPath)
}

// printUsage prints the usage message to STDERR.
func (cmd *Command) printUsage() {
	fmt.Fprintf(cmd.Stdout, `
Uses backup copies from the specified PATH to restore databases or specific shards from InfluxDB OSS
  or InfluxDB Enterprise to an InfluxDB OSS instance.

Usage: influxd restore -portable [options] PATH

Note: Restore using the '-portable' option consumes files in an improved Enterprise-compatible 
  format that includes a file manifest.

Options:
    -portable 
            Required to activate the portable restore mode. If not specified, the legacy restore mode is used.
    -host  <host:port>
            InfluxDB OSS host to connect to where the data will be restored. Defaults to '127.0.0.1:8088'.
    -db    <name>
            Name of database to be restored from the backup (InfluxDB OSS or InfluxDB Enterprise)
    -newdb <name>
            Name of the InfluxDB OSS database into which the archived data will be imported on the target system. 
            Optional. If not given, then the value of '-db <db_name>' is used.  The new database name must be unique 
            to the target system.
    -rp    <name>
            Name of retention policy from the backup that will be restored. Optional. 
            Requires that '-db <db_name>' is specified.
    -newrp <name>
            Name of the retention policy to be created on the target system. Optional. Requires that '-rp <rp_name>' 
            is set. If not given, the '-rp <rp_name>' value is used.
    -shard <id>
            Identifier of the shard to be restored. Optional. If specified, then '-db <db_name>' and '-rp <rp_name>' are
            required.
    PATH
            Path to directory containing the backup files.

`)
}
