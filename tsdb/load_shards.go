package tsdb

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"sync"
	"time"

	"github.com/influxdata/influxdb/logger"
	"github.com/influxdata/influxdb/pkg/limiter"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// res holds the result from opening each shard in a goroutine
type res struct {
	s   *Shard
	err error
}

func (s *Store) loadShards() error {
	// Limit the number of concurrent TSM files to be opened to the number of cores.
	s.EngineOptions.OpenLimiter = limiter.NewFixed(runtime.GOMAXPROCS(0))

	// Setup a shared limiter for compactions
	lim := s.EngineOptions.Config.MaxConcurrentCompactions
	if lim == 0 {
		lim = runtime.GOMAXPROCS(0) / 2 // Default to 50% of cores for compactions

		if lim < 1 {
			lim = 1
		}
	}

	// Don't allow more compactions to run than cores.
	if lim > runtime.GOMAXPROCS(0) {
		lim = runtime.GOMAXPROCS(0)
	}

	s.EngineOptions.CompactionLimiter = limiter.NewFixed(lim)

	compactionSettings := []zapcore.Field{zap.Int("max_concurrent_compactions", lim)}
	throughput := int(s.EngineOptions.Config.CompactThroughput)
	throughputBurst := int(s.EngineOptions.Config.CompactThroughputBurst)
	if throughput > 0 {
		if throughputBurst < throughput {
			throughputBurst = throughput
		}

		compactionSettings = append(
			compactionSettings,
			zap.Int("throughput_bytes_per_second", throughput),
			zap.Int("throughput_bytes_per_second_burst", throughputBurst),
		)
		s.EngineOptions.CompactionThroughputLimiter = limiter.NewRate(throughput, throughputBurst)
	} else {
		compactionSettings = append(
			compactionSettings,
			zap.String("throughput_bytes_per_second", "unlimited"),
			zap.String("throughput_bytes_per_second_burst", "unlimited"),
		)
	}

	s.Logger.Info("Compaction settings", compactionSettings...)

	log, logEnd := logger.NewOperation(s.Logger, "Open store", "tsdb_open")
	defer logEnd()

	shardLoaderWg := new(sync.WaitGroup)
	t := limiter.NewFixed(runtime.GOMAXPROCS(0))
	resC := make(chan *res)

	// Determine how many shards we need to open by checking the store path.
	dbDirs, err := os.ReadDir(s.path)
	if err != nil {
		return err
	}

	walkShardsAndProcess := func(fn func(sfile *SeriesFile, idx interface{}, sh os.DirEntry, db os.DirEntry, rp os.DirEntry) error) error {
		for _, db := range dbDirs {
			rpDirs, err := s.getRetentionPolicyDirs(db, log)
			if err != nil {
				return err
			} else if rpDirs == nil {
				continue
			}

			// Load series file.
			sfile, err := s.openSeriesFile(db.Name())
			if err != nil {
				return err
			}

			// Retrieve database index.
			idx, err := s.createIndexIfNotExists(db.Name())
			if err != nil {
				return err
			}

			for _, rp := range rpDirs {
				shardDirs, err := s.getShards(rp, db, log)
				if err != nil {
					return err
				} else if shardDirs == nil {
					continue
				}

				for _, sh := range shardDirs {
					// Series file should not be in a retention policy but skip just in case.
					if sh.Name() == SeriesFileDirectory {
						log.Warn("Skipping series file in retention policy dir", zap.String("path", filepath.Join(s.path, db.Name(), rp.Name())))
						continue
					}

					if err := fn(sfile, idx, sh, db, rp); err != nil {
						return err
					}
				}
			}
		}

		return nil
	}

	if s.startupProgressMetrics != nil {
		err := walkShardsAndProcess(func(sfile *SeriesFile, idx interface{}, sh os.DirEntry, db os.DirEntry, rp os.DirEntry) error {
			s.startupProgressMetrics.AddShard()
			return nil
		})
		if err != nil {
			return err
		}
	}

	err = walkShardsAndProcess(func(sfile *SeriesFile, idx interface{}, sh os.DirEntry, db os.DirEntry, rp os.DirEntry) error {
		shardLoaderWg.Add(1)

		go func(db, rp, sh string) {
			defer shardLoaderWg.Done()

			t.Take()
			defer t.Release()

			start := time.Now()
			path := filepath.Join(s.path, db, rp, sh)
			walPath := filepath.Join(s.EngineOptions.Config.WALDir, db, rp, sh)

			// Shard file names are numeric shardIDs
			shardID, err := strconv.ParseUint(sh, 10, 64)
			if err != nil {
				log.Info("invalid shard ID found at path", zap.String("path", path))
				resC <- &res{err: fmt.Errorf("%s is not a valid ID. Skipping shard.", sh)}
				if s.startupProgressMetrics != nil {
					s.startupProgressMetrics.CompletedShard()
				}
				return
			}

			if s.EngineOptions.ShardFilter != nil && !s.EngineOptions.ShardFilter(db, rp, shardID) {
				log.Info("skipping shard", zap.String("path", path), logger.Shard(shardID))
				resC <- &res{}
				if s.startupProgressMetrics != nil {
					s.startupProgressMetrics.CompletedShard()
				}
				return
			}

			// Copy options and assign shared index.
			opt := s.EngineOptions
			opt.InmemIndex = idx

			// Provide an implementation of the ShardIDSets
			opt.SeriesIDSets = shardSet{store: s, db: db}

			// Existing shards should continue to use inmem index.
			if _, err := os.Stat(filepath.Join(path, "index")); os.IsNotExist(err) {
				opt.IndexVersion = InmemIndexName
			}

			// Open engine.
			shard := NewShard(shardID, path, walPath, sfile, opt)

			// Disable compactions, writes and queries until all shards are loaded
			shard.EnableOnOpen = false
			shard.CompactionDisabled = s.EngineOptions.CompactionDisabled
			shard.WithLogger(s.baseLogger)

			err = s.OpenShard(shard, false)
			if err != nil {
				log.Error("Failed to open shard", logger.Shard(shardID), zap.Error(err))
				resC <- &res{err: fmt.Errorf("failed to open shard: %d: %w", shardID, err)}
				if s.startupProgressMetrics != nil {
					s.startupProgressMetrics.CompletedShard()
				}
				return
			}

			resC <- &res{s: shard}
			log.Info("Opened shard", zap.String("index_version", shard.IndexType()), zap.String("path", path), zap.Duration("duration", time.Since(start)))
			if s.startupProgressMetrics != nil {
				s.startupProgressMetrics.CompletedShard()
			}
		}(db.Name(), rp.Name(), sh.Name())

		return nil
	})

	if err := s.enableShards(shardLoaderWg, resC); err != nil {
		return err
	}

	return nil
}

func (s *Store) enableShards(wg *sync.WaitGroup, resC chan *res) error {
	go func() {
		wg.Wait()
		close(resC)
	}()

	for res := range resC {
		if res.s == nil || res.err != nil {
			continue
		}
		s.shards[res.s.id] = res.s
		s.epochs[res.s.id] = newEpochTracker()
		if _, ok := s.databases[res.s.database]; !ok {
			s.databases[res.s.database] = new(databaseState)
		}
		s.databases[res.s.database].addIndexType(res.s.IndexType())
	}

	// Check if any databases are running multiple index types.
	for db, state := range s.databases {
		if state.hasMultipleIndexTypes() {
			var fields []zapcore.Field
			for idx, cnt := range state.indexTypes {
				fields = append(fields, zap.Int(fmt.Sprintf("%s_count", idx), cnt))
			}
			s.Logger.Warn("Mixed shard index types", append(fields, logger.Database(db))...)
		}
	}

	// Enable all shards
	for _, sh := range s.shards {
		sh.SetEnabled(true)
		if isIdle, _ := sh.IsIdle(); isIdle {
			if err := sh.Free(); err != nil {
				return err
			}
		}
	}

	return nil
}

func (s *Store) getRetentionPolicyDirs(db os.DirEntry, log *zap.Logger) ([]os.DirEntry, error) {
	dbPath := filepath.Join(s.path, db.Name())
	if !db.IsDir() {
		log.Info("Skipping database dir", zap.String("name", db.Name()), zap.String("reason", "not a directory"))
		return nil, nil
	}

	if s.EngineOptions.DatabaseFilter != nil && !s.EngineOptions.DatabaseFilter(db.Name()) {
		log.Info("Skipping database dir", logger.Database(db.Name()), zap.String("reason", "failed database filter"))
		return nil, nil
	}

	// Load each retention policy within the database directory.
	rpDirs, err := os.ReadDir(dbPath)
	if err != nil {
		return nil, err
	}

	return rpDirs, nil
}

func (s *Store) getShards(rpDir os.DirEntry, dbDir os.DirEntry, log *zap.Logger) ([]os.DirEntry, error) {
	rpPath := filepath.Join(s.path, dbDir.Name(), rpDir.Name())
	if !rpDir.IsDir() {
		log.Info("Skipping retention policy dir", zap.String("name", rpDir.Name()), zap.String("reason", "not a directory"))
		return nil, nil
	}

	// The .series directory is not a retention policy.
	if rpDir.Name() == SeriesFileDirectory {
		return nil, nil
	}

	if s.EngineOptions.RetentionPolicyFilter != nil && !s.EngineOptions.RetentionPolicyFilter(dbDir.Name(), rpDir.Name()) {
		log.Info("Skipping retention policy dir", logger.RetentionPolicy(rpDir.Name()), zap.String("reason", "failed retention policy filter"))
		return nil, nil
	}

	shardDirs, err := os.ReadDir(rpPath)
	if err != nil {
		return nil, err
	}

	return shardDirs, nil
}
