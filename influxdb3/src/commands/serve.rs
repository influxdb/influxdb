//! Entrypoint for InfluxDB 3 Core Server

use anyhow::{bail, Context};
use datafusion_util::config::register_iox_object_store;
use influxdb3_cache::{
    distinct_cache::DistinctCacheProvider,
    last_cache::{self, LastCacheProvider},
    parquet_cache::create_cached_obj_store_and_oracle,
};
use influxdb3_clap_blocks::plugins::{PackageManager, ProcessingEngineConfig};
use influxdb3_clap_blocks::{
    datafusion::IoxQueryDatafusionConfig,
    memory_size::MemorySize,
    object_store::{ObjectStoreConfig, ObjectStoreType},
    socket_addr::SocketAddr,
    tokio::TokioDatafusionConfig,
};
use influxdb3_process::{
    build_malloc_conf, setup_metric_registry, INFLUXDB3_GIT_HASH, INFLUXDB3_VERSION, PROCESS_UUID,
};
use influxdb3_processing_engine::environment::{
    DisabledManager, PipManager, PythonEnvironmentManager, UVManager,
};
use influxdb3_processing_engine::plugins::ProcessingEngineEnvironmentManager;
use influxdb3_processing_engine::ProcessingEngineManagerImpl;
use influxdb3_server::{
    auth::AllOrNothingAuthorizer,
    builder::ServerBuilder,
    query_executor::{CreateQueryExecutorArgs, QueryExecutorImpl},
    serve, CommonServerState,
};
use influxdb3_sys_events::SysEventStore;
use influxdb3_telemetry::store::TelemetryStore;
use influxdb3_wal::{Gen1Duration, WalConfig};
use influxdb3_write::{
    persister::Persister,
    write_buffer::{
        check_mem_and_force_snapshot_loop, persisted_files::PersistedFiles, WriteBufferImpl,
        WriteBufferImplArgs,
    },
    WriteBuffer,
};
use iox_query::exec::{DedicatedExecutor, Executor, ExecutorConfig};
use iox_time::SystemProvider;
use object_store::ObjectStore;
use observability_deps::tracing::*;
use panic_logging::SendPanicsToTracing;
use parquet_file::storage::{ParquetStorage, StorageId};
use std::env;
use std::process::Command;
use std::{num::NonZeroUsize, sync::Arc, time::Duration};
use std::{
    path::{Path, PathBuf},
    str::FromStr,
};
use thiserror::Error;
use tokio::net::TcpListener;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use trace_exporters::TracingConfig;
use trace_http::ctx::TraceHeaderParser;
use trogging::cli::LoggingConfig;

/// The default name of the influxdb data directory
#[allow(dead_code)]
pub const DEFAULT_DATA_DIRECTORY_NAME: &str = ".influxdb3";

/// The default bind address for the HTTP API.
pub const DEFAULT_HTTP_BIND_ADDR: &str = "0.0.0.0:8181";

pub const DEFAULT_TELEMETRY_ENDPOINT: &str = "https://telemetry.v3.influxdata.com";

#[derive(Debug, Error)]
pub enum Error {
    #[error("Cannot parse object store config: {0}")]
    ObjectStoreParsing(#[from] influxdb3_clap_blocks::object_store::ParseError),

    #[error("Tracing config error: {0}")]
    TracingConfig(#[from] trace_exporters::Error),

    #[error("Error initializing tokio runtime: {0}")]
    TokioRuntime(#[source] std::io::Error),

    #[error("Failed to bind address")]
    BindAddress(#[source] std::io::Error),

    #[error("Server error: {0}")]
    Server(#[from] influxdb3_server::Error),

    #[error("Write buffer error: {0}")]
    WriteBuffer(#[from] influxdb3_write::write_buffer::Error),

    #[error("invalid token: {0}")]
    InvalidToken(#[from] hex::FromHexError),

    #[error("failed to initialized write buffer: {0}")]
    WriteBufferInit(#[source] anyhow::Error),

    #[error("failed to initialize from persisted catalog: {0}")]
    InitializePersistedCatalog(#[source] influxdb3_write::persister::Error),

    #[error("failed to initialize last cache: {0}")]
    InitializeLastCache(#[source] last_cache::Error),

    #[error("failed to initialize distinct cache: {0:#}")]
    InitializeDistinctCache(#[source] influxdb3_cache::distinct_cache::ProviderError),
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, clap::Parser)]
pub struct Config {
    /// object store options
    #[clap(flatten)]
    object_store_config: ObjectStoreConfig,

    /// logging options
    #[clap(flatten)]
    pub(crate) logging_config: LoggingConfig,

    /// tracing options
    #[clap(flatten)]
    pub(crate) tracing_config: TracingConfig,

    /// tokio datafusion config
    #[clap(flatten)]
    pub(crate) tokio_datafusion_config: TokioDatafusionConfig,

    /// iox_query extended DataFusion config
    #[clap(flatten)]
    pub(crate) iox_query_datafusion_config: IoxQueryDatafusionConfig,

    /// Maximum size of HTTP requests.
    #[clap(
    long = "max-http-request-size",
    env = "INFLUXDB3_MAX_HTTP_REQUEST_SIZE",
    default_value = "10485760", // 10 MiB
    action,
    )]
    pub max_http_request_size: usize,

    /// The address on which InfluxDB will serve HTTP API requests
    #[clap(
    long = "http-bind",
    env = "INFLUXDB3_HTTP_BIND_ADDR",
    default_value = DEFAULT_HTTP_BIND_ADDR,
    action,
    )]
    pub http_bind_address: SocketAddr,

    /// Size of the RAM cache used to store data in bytes.
    ///
    /// Can be given as absolute value or in percentage of the total available memory (e.g. `10%`).
    #[clap(
    long = "ram-pool-data-bytes",
    env = "INFLUXDB3_RAM_POOL_DATA_BYTES",
    default_value = "1073741824",  // 1GB
    action
    )]
    pub ram_pool_data_bytes: MemorySize,

    /// Size of memory pool used during query exec, in bytes.
    ///
    /// Can be given as absolute value or in percentage of the total available memory (e.g. `10%`).
    #[clap(
    long = "exec-mem-pool-bytes",
    env = "INFLUXDB3_EXEC_MEM_POOL_BYTES",
    default_value = "8589934592",  // 8GB
    action
    )]
    pub exec_mem_pool_bytes: MemorySize,

    /// bearer token to be set for requests
    #[clap(long = "bearer-token", env = "INFLUXDB3_BEARER_TOKEN", action)]
    pub bearer_token: Option<String>,

    /// Duration that the Parquet files get arranged into. The data timestamps will land each
    /// row into a file of this duration. 1m, 5m, and 10m are supported. These are known as
    /// "generation 1" files. The compactor in Pro can compact these into larger and longer
    /// generations.
    #[clap(
        long = "gen1-duration",
        env = "INFLUXDB3_GEN1_DURATION",
        default_value = "10m",
        action
    )]
    pub gen1_duration: Gen1Duration,

    /// Interval to flush buffered data to a wal file. Writes that wait for wal confirmation will
    /// take as long as this interval to complete.
    #[clap(
        long = "wal-flush-interval",
        env = "INFLUXDB3_WAL_FLUSH_INTERVAL",
        default_value = "1s",
        action
    )]
    pub wal_flush_interval: humantime::Duration,

    /// The number of WAL files to attempt to remove in a snapshot. This times the interval will
    /// determine how often snapshot is taken.
    #[clap(
        long = "wal-snapshot-size",
        env = "INFLUXDB3_WAL_SNAPSHOT_SIZE",
        default_value = "600",
        action
    )]
    pub wal_snapshot_size: usize,

    /// The maximum number of writes requests that can be buffered before a flush must be run
    /// and succeed.
    #[clap(
        long = "wal-max-write-buffer-size",
        env = "INFLUXDB3_WAL_MAX_WRITE_BUFFER_SIZE",
        default_value = "100000",
        action
    )]
    pub wal_max_write_buffer_size: usize,

    /// Number of snapshotted wal files to retain in object store, wal flush does not clear
    /// the wal files immediately instead they are only deleted when snapshotted and num wal files
    /// count exceeds this size
    #[clap(
        long = "snapshotted-wal-files-to-keep",
        env = "INFLUXDB3_NUM_WAL_FILES_TO_KEEP",
        default_value = "300",
        action
    )]
    pub snapshotted_wal_files_to_keep: u64,

    // TODO - tune this default:
    /// The size of the query log. Up to this many queries will remain in the log before
    /// old queries are evicted to make room for new ones.
    #[clap(
        long = "query-log-size",
        env = "INFLUXDB3_QUERY_LOG_SIZE",
        default_value = "1000",
        action
    )]
    pub query_log_size: usize,

    // TODO - make this default to 70% of available memory:
    /// The size limit of the buffered data. If this limit is passed a snapshot will be forced.
    #[clap(
        long = "buffer-mem-limit-mb",
        env = "INFLUXDB3_BUFFER_MEM_LIMIT_MB",
        default_value = "5000",
        action
    )]
    pub buffer_mem_limit_mb: usize,

    /// The node idendifier used as a prefix in all object store file paths. This should be unique
    /// for any InfluxDB 3 Core servers that share the same object store configuration, i.e., the
    /// same bucket.
    #[clap(
        long = "node-id",
        // TODO: deprecate this alias in future version
        alias = "host-id",
        env = "INFLUXDB3_NODE_IDENTIFIER_PREFIX",
        action
    )]
    pub node_identifier_prefix: String,

    /// The size of the in-memory Parquet cache in megabytes (MB).
    #[clap(
        long = "parquet-mem-cache-size-mb",
        env = "INFLUXDB3_PARQUET_MEM_CACHE_SIZE_MB",
        default_value = "1000",
        action
    )]
    pub parquet_mem_cache_size: ParquetCacheSizeMb,

    /// The percentage of entries to prune during a prune operation on the in-memory Parquet cache.
    ///
    /// This must be a number between 0 and 1.
    #[clap(
        long = "parquet-mem-cache-prune-percentage",
        env = "INFLUXDB3_PARQUET_MEM_CACHE_PRUNE_PERCENTAGE",
        default_value = "0.1",
        action
    )]
    pub parquet_mem_cache_prune_percentage: ParquetCachePrunePercent,

    /// The interval on which to check if the in-memory Parquet cache needs to be pruned.
    ///
    /// Enter as a human-readable time, e.g., "1s", "100ms", "1m", etc.
    #[clap(
        long = "parquet-mem-cache-prune-interval",
        env = "INFLUXDB3_PARQUET_MEM_CACHE_PRUNE_INTERVAL",
        default_value = "1s",
        action
    )]
    pub parquet_mem_cache_prune_interval: humantime::Duration,

    /// Disable the in-memory Parquet cache. By default, the cache is enabled.
    #[clap(
        long = "disable-parquet-mem-cache",
        env = "INFLUXDB3_DISABLE_PARQUET_MEM_CACHE",
        default_value_t = false,
        action
    )]
    pub disable_parquet_mem_cache: bool,

    /// The duration from `now` to check if parquet files pulled in query path requires caching
    /// Enter as a human-readable time, e.g., "5h", "3d"
    #[clap(
        long = "parquet-mem-cache-query-path-duration",
        env = "INFLUXDB3_PARQUET_MEM_CACHE_QUERY_PATH_DURATION",
        default_value = "5h",
        action
    )]
    pub parquet_mem_cache_query_path_duration: humantime::Duration,

    /// The interval on which to evict expired entries from the Last-N-Value cache, expressed as a
    /// human-readable time, e.g., "20s", "1m", "1h".
    #[clap(
        long = "last-cache-eviction-interval",
        env = "INFLUXDB3_LAST_CACHE_EVICTION_INTERVAL",
        default_value = "10s",
        action
    )]
    pub last_cache_eviction_interval: humantime::Duration,

    /// The interval on which to evict expired entries from the Distinct Value cache, expressed as a
    /// human-readable time, e.g., "20s", "1m", "1h".
    #[clap(
        long = "distinct-cache-eviction-interval",
        env = "INFLUXDB3_DISTINCT_CACHE_EVICTION_INTERVAL",
        default_value = "10s",
        action
    )]
    pub distinct_cache_eviction_interval: humantime::Duration,

    /// The processing engine config.
    #[clap(flatten)]
    pub processing_engine_config: ProcessingEngineConfig,

    /// Threshold for internal buffer, can be either percentage or absolute value.
    /// eg: 70% or 100000
    #[clap(
        long = "force-snapshot-mem-threshold",
        env = "INFLUXDB3_FORCE_SNAPSHOT_MEM_THRESHOLD",
        default_value = "70%",
        action
    )]
    pub force_snapshot_mem_threshold: MemorySize,

    /// Disable sending telemetry data to telemetry.v3.influxdata.com.
    #[clap(
        long = "disable-telemetry-upload",
        env = "INFLUXDB3_TELEMETRY_DISABLE_UPLOAD",
        default_value_t = false,
        hide = true,
        action
    )]
    pub disable_telemetry_upload: bool,

    /// Send telemetry data to the specified endpoint.
    #[clap(
        long = "telemetry-endpoint",
        env = "INFLUXDB3_TELEMETRY_ENDPOINT",
        default_value = DEFAULT_TELEMETRY_ENDPOINT,
        hide = true,
        action
    )]
    pub telemetry_endpoint: String,

    /// Set the limit for number of parquet files allowed in a query. Defaults
    /// to 432 which is about 3 days worth of files using default settings.
    /// This number can be increased to allow more files to be queried, but
    /// query performance will likely suffer, RAM usage will spike, and the
    /// process might be OOM killed as a result. It would be better to specify
    /// smaller time ranges if possible in a query.
    #[clap(long = "query-file-limit", env = "INFLUXDB3_QUERY_FILE_LIMIT", action)]
    pub query_file_limit: Option<usize>,
}

/// Specified size of the Parquet cache in megabytes (MB)
#[derive(Debug, Clone, Copy)]
pub struct ParquetCacheSizeMb(usize);

impl ParquetCacheSizeMb {
    /// Express this cache size in terms of bytes (B)
    fn as_num_bytes(&self) -> usize {
        self.0 * 1_000 * 1_000
    }
}

impl FromStr for ParquetCacheSizeMb {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> std::prelude::v1::Result<Self, Self::Err> {
        s.parse()
            .context("failed to parse parquet cache size value as an unsigned integer")
            .map(Self)
    }
}

#[derive(Debug, Clone, Copy)]
pub struct ParquetCachePrunePercent(f64);

impl From<ParquetCachePrunePercent> for f64 {
    fn from(value: ParquetCachePrunePercent) -> Self {
        value.0
    }
}

impl FromStr for ParquetCachePrunePercent {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> std::prelude::v1::Result<Self, Self::Err> {
        let p = s
            .parse::<f64>()
            .context("failed to parse prune percent as f64")?;
        if p <= 0.0 || p >= 1.0 {
            bail!("prune percent must be between 0 and 1");
        }
        Ok(Self(p))
    }
}

/// If `p` does not exist, try to create it as a directory.
///
/// panic's if the directory does not exist and can not be created
#[allow(dead_code)]
fn ensure_directory_exists(p: &Path) {
    if !p.exists() {
        info!(
            p=%p.display(),
            "Creating directory",
        );
        std::fs::create_dir_all(p).expect("Could not create default directory");
    }
}

pub async fn command(config: Config) -> Result<()> {
    let startup_timer = Instant::now();
    let num_cpus = num_cpus::get();
    let build_malloc_conf = build_malloc_conf();
    info!(
        node_id = %config.node_identifier_prefix,
        git_hash = %INFLUXDB3_GIT_HASH as &str,
        version = %INFLUXDB3_VERSION.as_ref() as &str,
        uuid = %PROCESS_UUID.as_ref() as &str,
        num_cpus,
        "InfluxDB 3 Core server starting",
    );
    debug!(%build_malloc_conf, "build configuration");

    let metrics = setup_metric_registry();

    // Install custom panic handler and forget about it.
    //
    // This leaks the handler and prevents it from ever being dropped during the
    // lifetime of the program - this is actually a good thing, as it prevents
    // the panic handler from being removed while unwinding a panic (which in
    // turn, causes a panic - see #548)
    let f = SendPanicsToTracing::new_with_metrics(&metrics);
    std::mem::forget(f);

    // Construct a token to trigger clean shutdown
    let frontend_shutdown = CancellationToken::new();

    let time_provider = Arc::new(SystemProvider::new());
    let sys_events_store = Arc::new(SysEventStore::new(Arc::clone(&time_provider) as _));
    let object_store: Arc<dyn ObjectStore> = config
        .object_store_config
        .make_object_store()
        .map_err(Error::ObjectStoreParsing)?;

    let (object_store, parquet_cache) = if !config.disable_parquet_mem_cache {
        let (object_store, parquet_cache) = create_cached_obj_store_and_oracle(
            object_store,
            Arc::clone(&time_provider) as _,
            Arc::clone(&metrics),
            config.parquet_mem_cache_size.as_num_bytes(),
            config.parquet_mem_cache_query_path_duration.into(),
            config.parquet_mem_cache_prune_percentage.into(),
            config.parquet_mem_cache_prune_interval.into(),
        );
        (object_store, Some(parquet_cache))
    } else {
        (object_store, None)
    };

    let trace_exporter = config.tracing_config.build()?;

    let parquet_store =
        ParquetStorage::new(Arc::clone(&object_store), StorageId::from("influxdb3"));

    let mut tokio_datafusion_config = config.tokio_datafusion_config;
    tokio_datafusion_config.num_threads = tokio_datafusion_config
        .num_threads
        .or_else(|| NonZeroUsize::new(num_cpus::get()))
        .or_else(|| NonZeroUsize::new(1));
    info!(
        num_threads = tokio_datafusion_config.num_threads.map(|n| n.get()),
        "Creating shared query executor"
    );

    let exec = Arc::new(Executor::new_with_config_and_executor(
        ExecutorConfig {
            target_query_partitions: tokio_datafusion_config.num_threads.unwrap(),
            object_stores: [&parquet_store]
                .into_iter()
                .map(|store| (store.id(), Arc::clone(store.object_store())))
                .collect(),
            metric_registry: Arc::clone(&metrics),
            mem_pool_size: config.exec_mem_pool_bytes.bytes(),
        },
        DedicatedExecutor::new(
            "datafusion",
            tokio_datafusion_config
                .builder()
                .map_err(Error::TokioRuntime)?,
            Arc::clone(&metrics),
        ),
    ));
    let runtime_env = exec.new_context().inner().runtime_env();
    register_iox_object_store(runtime_env, parquet_store.id(), Arc::clone(&object_store));

    let trace_header_parser = TraceHeaderParser::new()
        .with_jaeger_trace_context_header_name(
            config
                .tracing_config
                .traces_jaeger_trace_context_header_name,
        )
        .with_jaeger_debug_name(config.tracing_config.traces_jaeger_debug_name);

    let persister = Arc::new(Persister::new(
        Arc::clone(&object_store),
        config.node_identifier_prefix,
        Arc::clone(&time_provider) as _,
    ));
    let wal_config = WalConfig {
        gen1_duration: config.gen1_duration,
        max_write_buffer_size: config.wal_max_write_buffer_size,
        flush_interval: config.wal_flush_interval.into(),
        snapshot_size: config.wal_snapshot_size,
    };

    let catalog = Arc::new(
        persister
            .load_or_create_catalog()
            .await
            .map_err(Error::InitializePersistedCatalog)?,
    );
    info!(instance_id = ?catalog.instance_id(), "catalog initialized");

    let last_cache = LastCacheProvider::new_from_catalog_with_background_eviction(
        Arc::clone(&catalog) as _,
        config.last_cache_eviction_interval.into(),
    )
    .map_err(Error::InitializeLastCache)?;

    let distinct_cache = DistinctCacheProvider::new_from_catalog_with_background_eviction(
        Arc::clone(&time_provider) as _,
        Arc::clone(&catalog),
        config.distinct_cache_eviction_interval.into(),
    )
    .map_err(Error::InitializeDistinctCache)?;

    let write_buffer_impl = WriteBufferImpl::new(WriteBufferImplArgs {
        persister: Arc::clone(&persister),
        catalog: Arc::clone(&catalog),
        last_cache,
        distinct_cache,
        time_provider: Arc::<SystemProvider>::clone(&time_provider),
        executor: Arc::clone(&exec),
        wal_config,
        parquet_cache,
        metric_registry: Arc::clone(&metrics),
        snapshotted_wal_files_to_keep: config.snapshotted_wal_files_to_keep,
        query_file_limit: config.query_file_limit,
    })
    .await
    .map_err(|e| Error::WriteBufferInit(e.into()))?;

    info!("setting up background mem check for query buffer");
    background_buffer_checker(
        config.force_snapshot_mem_threshold.bytes(),
        &write_buffer_impl,
    )
    .await;

    info!("setting up telemetry store");
    let telemetry_store = setup_telemetry_store(
        &config.object_store_config,
        catalog.instance_id(),
        num_cpus,
        Some(Arc::clone(&write_buffer_impl.persisted_files())),
        config.telemetry_endpoint.as_str(),
        config.disable_telemetry_upload,
    )
    .await;

    let write_buffer: Arc<dyn WriteBuffer> = write_buffer_impl;

    let common_state = CommonServerState::new(
        Arc::clone(&metrics),
        trace_exporter,
        trace_header_parser,
        Arc::clone(&telemetry_store),
    )?;

    let query_executor = Arc::new(QueryExecutorImpl::new(CreateQueryExecutorArgs {
        catalog: write_buffer.catalog(),
        write_buffer: Arc::clone(&write_buffer),
        exec: Arc::clone(&exec),
        metrics: Arc::clone(&metrics),
        datafusion_config: Arc::new(config.iox_query_datafusion_config.build()),
        query_log_size: config.query_log_size,
        telemetry_store: Arc::clone(&telemetry_store),
        sys_events_store: Arc::clone(&sys_events_store),
    }));

    let listener = TcpListener::bind(*config.http_bind_address)
        .await
        .map_err(Error::BindAddress)?;

    let processing_engine = ProcessingEngineManagerImpl::new(
        setup_processing_engine_env_manager(&config.processing_engine_config),
        write_buffer.catalog(),
        Arc::clone(&write_buffer),
        Arc::clone(&query_executor) as _,
        Arc::clone(&time_provider) as _,
        write_buffer.wal(),
        sys_events_store,
    );

    let builder = ServerBuilder::new(common_state)
        .max_request_size(config.max_http_request_size)
        .write_buffer(write_buffer)
        .query_executor(query_executor)
        .time_provider(time_provider)
        .persister(persister)
        .tcp_listener(listener)
        .processing_engine(processing_engine);

    let server = if let Some(token) = config.bearer_token.map(hex::decode).transpose()? {
        builder
            .authorizer(Arc::new(AllOrNothingAuthorizer::new(token)))
            .build()
            .await
    } else {
        builder.build().await
    };
    serve(server, frontend_shutdown, startup_timer).await?;

    Ok(())
}

pub(crate) fn setup_processing_engine_env_manager(
    config: &ProcessingEngineConfig,
) -> ProcessingEngineEnvironmentManager {
    let package_manager: Arc<dyn PythonEnvironmentManager> = match config.package_manager {
        PackageManager::Discover => determine_package_manager(),
        PackageManager::Pip => Arc::new(PipManager),
        PackageManager::UV => Arc::new(UVManager),
    };
    ProcessingEngineEnvironmentManager {
        plugin_dir: config.plugin_dir.clone(),
        virtual_env_location: config.virtual_env_location.clone(),
        package_manager,
    }
}

fn determine_package_manager() -> Arc<dyn PythonEnvironmentManager> {
    // Check for uv first (highest preference)
    if let Ok(output) = Command::new("uv").arg("--version").output() {
        if output.status.success() {
            return Arc::new(UVManager);
        }
    }

    // Check for pip second
    // XXX: put this somewhere common
    let python_exe_bn = if cfg!(windows) {
        "python.exe"
    } else {
        "python3"
    };
    let python_exe = if let Ok(v) = env::var("PYTHONHOME") {
        // honor PYTHONHOME (set earlier for python standalone). python build
        // standalone has bin/python3 on OSX/Linx and python.exe on Windows
        let mut path = PathBuf::from(v);
        if !cfg!(windows) {
            path.push("bin");
        }
        path.push(python_exe_bn);
        path
    } else {
        PathBuf::from(python_exe_bn)
    };

    if let Ok(output) = Command::new(python_exe)
        .args(["-m", "pip", "--version"])
        .output()
    {
        if output.status.success() {
            return Arc::new(PipManager);
        }
    }

    // If neither is available, return DisabledManager
    Arc::new(DisabledManager)
}

async fn setup_telemetry_store(
    object_store_config: &ObjectStoreConfig,
    instance_id: Arc<str>,
    num_cpus: usize,
    persisted_files: Option<Arc<PersistedFiles>>,
    telemetry_endpoint: &str,
    disable_upload: bool,
) -> Arc<TelemetryStore> {
    let os = std::env::consts::OS;
    let influxdb_pkg_version = env!("CARGO_PKG_VERSION");
    let influxdb_pkg_name = env!("CARGO_PKG_NAME");
    // Following should show influxdb3-0.1.0
    let influx_version = format!("{}-{}", influxdb_pkg_name, influxdb_pkg_version);
    let obj_store_type = object_store_config
        .object_store
        .unwrap_or(ObjectStoreType::Memory);
    let storage_type = obj_store_type.as_str();

    if disable_upload {
        debug!("Initializing TelemetryStore with upload disabled.");
        TelemetryStore::new_without_background_runners(persisted_files.map(|p| p as _))
    } else {
        debug!("Initializing TelemetryStore with upload enabled for {telemetry_endpoint}.");
        TelemetryStore::new(
            instance_id,
            Arc::from(os),
            Arc::from(influx_version),
            Arc::from(storage_type),
            num_cpus,
            persisted_files.map(|p| p as _),
            telemetry_endpoint.to_string(),
        )
        .await
    }
}

async fn background_buffer_checker(
    mem_threshold_bytes: usize,
    write_buffer_impl: &Arc<WriteBufferImpl>,
) {
    debug!(mem_threshold_bytes, "setting up background buffer checker");
    check_mem_and_force_snapshot_loop(
        Arc::clone(write_buffer_impl),
        mem_threshold_bytes,
        Duration::from_secs(10),
    )
    .await;
}
