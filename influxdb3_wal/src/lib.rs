//! This crate provides a Write Ahead Log (WAL) for InfluxDB 3 Core. The WAL is used to buffer writes
//! in memory and persist them as individual files in an object store. The WAL is used to make
//! writes durable until they can be written in larger batches as Parquet files and other snapshot and
//! index files in object storage.

pub mod create;
pub mod object_store;
pub mod serialize;
mod snapshot_tracker;

use async_trait::async_trait;
use cron::Schedule;
use data_types::Timestamp;
use hashbrown::HashMap;
use humantime::{format_duration, parse_duration};
use indexmap::IndexMap;
use influxdb3_id::{ColumnId, DbId, SerdeVecMap, TableId};
use influxdb_line_protocol::v3::SeriesValue;
use influxdb_line_protocol::FieldValue;
use iox_time::Time;
use miette::Diagnostic;
use schema::{InfluxColumnType, InfluxFieldType};
use serde::{Deserialize, Serialize};
use serde_with::serde_as;
use std::cmp::Ordering;
use std::fmt::{Debug, Display};
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use std::{any::Any, num::ParseIntError};
use thiserror::Error;
use tokio::sync::{oneshot, OwnedSemaphorePermit};

#[derive(Debug, Diagnostic, Error)]
#[diagnostic(
    code(influxdb3_wal::lib),
    url("https://github.com/influxdata/influxdb/issues/new?template=bug_report.md")
)]
pub enum Error {
    #[error("wal buffer full with {0} ops")]
    BufferFull(usize),

    #[error("error writing wal file: {0}")]
    WriteError(String),

    #[error("deserialize error")]
    Serialize(#[from] crate::serialize::Error),

    #[error("join error")]
    Join(#[from] tokio::task::JoinError),

    #[error("object store error")]
    ObjectStoreError(#[from] ::object_store::Error),

    #[error("wal is shutdown and not accepting writes")]
    Shutdown,

    #[error("invalid gen1 duration {0}. Must be one of 1m, 5m, 10m")]
    InvalidGen1Duration(String),

    #[error("last cache size must be from 1 to 10")]
    InvalidLastCacheSize,

    #[error("invalid WAL file path")]
    InvalidWalFilePath,

    #[error("failed to parse trigger from {trigger_spec}{}", .context.as_ref().map(|context| format!(": {context}")).unwrap_or_default())]
    TriggerSpecificationParseError {
        trigger_spec: String,
        context: Option<String>,
    },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[async_trait]
pub trait Wal: Debug + Send + Sync + 'static {
    /// Buffer writes ops into the buffer, but returns before the operation is persisted to the WAL.
    async fn write_ops_unconfirmed(&self, op: Vec<WalOp>) -> Result<(), Error>;

    /// Writes the ops into the buffer and waits until the WAL file is persisted. When this returns
    /// the operations are durable in the configured object store and the file notifier has been
    /// called, which puts it into the queryable memory buffer.
    async fn write_ops(&self, ops: Vec<WalOp>) -> Result<(), Error>;

    /// Flushes all buffered writes to a single WAL file and calls the file notifier with the contents.
    /// If it is time for a snapshot, it will tell the notifier to start the snapshot and return
    /// a receiver that will be signalled when the snapshot is complete along with the semaphore
    /// permit to release when done. The caller is responsible for cleaning up the wal.
    async fn flush_buffer(
        &self,
    ) -> Option<(
        oneshot::Receiver<SnapshotDetails>,
        SnapshotDetails,
        OwnedSemaphorePermit,
    )>;

    /// This is similar to flush buffer but it allows for snapshot to be done immediately rather
    /// than waiting for wal periods to stack up in [`snapshot_tracker::SnapshotTracker`].
    /// Because forcing flush buffer can happen with no ops in wal buffer this call will add
    /// a [`WalOp::Noop`] into the wal buffer if it is empty and then carry on with snapshotting.
    async fn force_flush_buffer(
        &self,
    ) -> Option<(
        oneshot::Receiver<SnapshotDetails>,
        SnapshotDetails,
        OwnedSemaphorePermit,
    )>;

    /// Removes any snapshot wal files
    async fn cleanup_snapshot(
        &self,
        snapshot_details: SnapshotDetails,
        snapshot_permit: OwnedSemaphorePermit,
    );

    /// Returns the last persisted wal file sequence number
    async fn last_wal_sequence_number(&self) -> WalFileSequenceNumber;

    /// Returns the last persisted wal file sequence number
    async fn last_snapshot_sequence_number(&self) -> SnapshotSequenceNumber;

    /// Stop all writes to the WAL and flush the buffer to a WAL file.
    async fn shutdown(&self);

    /// Adds a new file notifier listener to the WAL (for use by the processing engine). The WAL
    /// will send new file notifications to the listener, but ignore any snapshot receiver.
    /// Only the notifier passed in the WAL constructor should be used for snapshots (i.e. the
    /// `QueryableBuffer`).
    fn add_file_notifier(&self, notifier: Arc<dyn WalFileNotifier>);
}

/// When the WAL persists a file with buffered ops, the contents are sent to this
/// notifier so that the data can be loaded into the in memory buffer and caches.
#[async_trait]
pub trait WalFileNotifier: Debug + Send + Sync + 'static {
    /// Notify the handler that a new WAL file has been persisted with the given contents.
    async fn notify(&self, write: Arc<WalContents>);

    /// Notify the handler that a new WAL file has been persisted with the given contents and tell
    /// it to snapshot the data. The returned receiver will be signalled when the snapshot is complete.
    async fn notify_and_snapshot(
        &self,
        write: Arc<WalContents>,
        snapshot_details: SnapshotDetails,
    ) -> oneshot::Receiver<SnapshotDetails>;

    fn as_any(&self) -> &dyn Any;
}

/// The configuration for the WAL
#[derive(Debug, Clone, Copy)]
pub struct WalConfig {
    /// The duration of time of chunks to be persisted as Parquet files
    pub gen1_duration: Gen1Duration,
    /// The maximum number of writes that can be buffered before we must flush to a wal file
    pub max_write_buffer_size: usize,
    /// The interval at which to flush the buffer to a wal file
    pub flush_interval: Duration,
    /// The number of wal files to snapshot at a time
    pub snapshot_size: usize,
}

impl WalConfig {
    pub fn test_config() -> Self {
        Self {
            gen1_duration: Gen1Duration::new_5m(),
            max_write_buffer_size: 1000,
            flush_interval: Duration::from_millis(10),
            snapshot_size: 100,
        }
    }
}

impl Default for WalConfig {
    fn default() -> Self {
        Self {
            gen1_duration: Default::default(),
            max_write_buffer_size: 100_000,
            flush_interval: Duration::from_secs(1),
            snapshot_size: 600,
        }
    }
}

/// The duration of data timestamps, grouped into files persisted into object storage.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Gen1Duration(Duration);

impl Gen1Duration {
    pub fn duration_seconds(&self) -> i64 {
        self.0.as_secs() as i64
    }

    /// Returns the time of the chunk the given timestamp belongs to based on the duration
    pub fn chunk_time_for_timestamp(&self, t: Timestamp) -> i64 {
        t.get() - (t.get() % self.0.as_nanos() as i64)
    }

    /// Given a time, returns the start time of the gen1 chunk that contains the time.
    pub fn start_time(&self, timestamp_seconds: i64) -> Time {
        let duration_seconds = self.duration_seconds();
        let rounded_seconds = (timestamp_seconds / duration_seconds) * duration_seconds;
        Time::from_timestamp(rounded_seconds, 0).unwrap()
    }

    pub fn as_duration(&self) -> Duration {
        self.0
    }

    pub fn as_nanos(&self) -> i64 {
        self.0.as_nanos() as i64
    }

    pub fn new_1m() -> Self {
        Self(Duration::from_secs(60))
    }

    pub fn new_5m() -> Self {
        Self(Duration::from_secs(300))
    }
}

impl FromStr for Gen1Duration {
    type Err = Error;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s {
            "1m" => Ok(Self(Duration::from_secs(60))),
            "5m" => Ok(Self(Duration::from_secs(300))),
            "10m" => Ok(Self(Duration::from_secs(600))),
            _ => Err(Error::InvalidGen1Duration(s.to_string())),
        }
    }
}

impl Default for Gen1Duration {
    fn default() -> Self {
        Self(Duration::from_secs(600))
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
pub struct NoopDetails {
    timestamp_ns: i64,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub enum WalOp {
    Write(WriteBatch),
    Catalog(OrderedCatalogBatch),
    Noop(NoopDetails),
}

impl PartialOrd for WalOp {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for WalOp {
    fn cmp(&self, other: &Self) -> Ordering {
        match (self, other) {
            // Catalog ops come before Write ops
            (WalOp::Catalog(_), WalOp::Write(_)) => Ordering::Less,
            (WalOp::Write(_), WalOp::Catalog(_)) => Ordering::Greater,

            // For two Catalog ops, compare by database_sequence_number
            (WalOp::Catalog(a), WalOp::Catalog(b)) => {
                a.database_sequence_number.cmp(&b.database_sequence_number)
            }

            // For two Write ops, consider them equal
            (WalOp::Write(_), WalOp::Write(_)) => Ordering::Equal,
            // all noops should stay where they are no need to reorder them
            // the noop at the moment at least should appear only in cases
            // when there are no other ops in wal buffer.
            (_, WalOp::Noop(_)) => Ordering::Equal,
            (WalOp::Noop(_), _) => Ordering::Equal,
        }
    }
}

impl WalOp {
    pub fn as_write(&self) -> Option<&WriteBatch> {
        match self {
            WalOp::Write(w) => Some(w),
            WalOp::Catalog(_) => None,
            WalOp::Noop(_) => None,
        }
    }

    pub fn as_catalog(&self) -> Option<&CatalogBatch> {
        match self {
            WalOp::Write(_) => None,
            WalOp::Catalog(c) => Some(&c.catalog),
            WalOp::Noop(_) => None,
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct CatalogBatch {
    pub database_id: DbId,
    pub database_name: Arc<str>,
    pub time_ns: i64,
    pub ops: Vec<CatalogOp>,
}

/// A catalog batch that has been processed by the catalog and given a sequence number.
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct OrderedCatalogBatch {
    catalog: CatalogBatch,
    database_sequence_number: u32,
}

impl OrderedCatalogBatch {
    pub fn new(catalog: CatalogBatch, database_sequence_number: u32) -> Self {
        Self {
            catalog,
            database_sequence_number,
        }
    }

    pub fn sequence_number(&self) -> u32 {
        self.database_sequence_number
    }

    pub fn batch(&self) -> &CatalogBatch {
        &self.catalog
    }

    pub fn into_batch(self) -> CatalogBatch {
        self.catalog
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub enum CatalogOp {
    CreateDatabase(DatabaseDefinition),
    CreateTable(WalTableDefinition),
    AddFields(FieldAdditions),
    CreateDistinctCache(DistinctCacheDefinition),
    DeleteDistinctCache(DistinctCacheDelete),
    CreateLastCache(LastCacheDefinition),
    DeleteLastCache(LastCacheDelete),
    DeleteDatabase(DeleteDatabaseDefinition),
    DeleteTable(DeleteTableDefinition),
    CreateTrigger(TriggerDefinition),
    DeleteTrigger(DeleteTriggerDefinition),
    EnableTrigger(TriggerIdentifier),
    DisableTrigger(TriggerIdentifier),
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct DatabaseDefinition {
    pub database_id: DbId,
    pub database_name: Arc<str>,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct DeleteDatabaseDefinition {
    pub database_id: DbId,
    pub database_name: Arc<str>,
    pub deletion_time: i64,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct DeleteTableDefinition {
    pub database_id: DbId,
    pub database_name: Arc<str>,
    pub table_id: TableId,
    pub table_name: Arc<str>,
    pub deletion_time: i64,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct WalTableDefinition {
    pub database_id: DbId,
    pub database_name: Arc<str>,
    pub table_name: Arc<str>,
    pub table_id: TableId,
    pub field_definitions: Vec<FieldDefinition>,
    pub key: Vec<ColumnId>,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct FieldAdditions {
    pub database_name: Arc<str>,
    pub database_id: DbId,
    pub table_name: Arc<str>,
    pub table_id: TableId,
    pub field_definitions: Vec<FieldDefinition>,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct FieldDefinition {
    pub name: Arc<str>,
    pub id: ColumnId,
    pub data_type: FieldDataType,
}

impl FieldDefinition {
    pub fn new(
        id: ColumnId,
        name: impl Into<Arc<str>>,
        data_type: impl Into<FieldDataType>,
    ) -> Self {
        Self {
            id,
            name: name.into(),
            data_type: data_type.into(),
        }
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
pub enum FieldDataType {
    String,
    Integer,
    UInteger,
    Float,
    Boolean,
    Timestamp,
    Tag,
    Key,
}

// FieldDataType from an InfluxColumnType
impl From<&InfluxColumnType> for FieldDataType {
    fn from(influx_column_type: &InfluxColumnType) -> Self {
        match influx_column_type {
            InfluxColumnType::Tag => FieldDataType::Tag,
            InfluxColumnType::Timestamp => FieldDataType::Timestamp,
            InfluxColumnType::Field(InfluxFieldType::String) => FieldDataType::String,
            InfluxColumnType::Field(InfluxFieldType::Integer) => FieldDataType::Integer,
            InfluxColumnType::Field(InfluxFieldType::UInteger) => FieldDataType::UInteger,
            InfluxColumnType::Field(InfluxFieldType::Float) => FieldDataType::Float,
            InfluxColumnType::Field(InfluxFieldType::Boolean) => FieldDataType::Boolean,
        }
    }
}

impl From<FieldDataType> for InfluxColumnType {
    fn from(field_data_type: FieldDataType) -> Self {
        match field_data_type {
            FieldDataType::Tag => InfluxColumnType::Tag,
            FieldDataType::Timestamp => InfluxColumnType::Timestamp,
            FieldDataType::String => InfluxColumnType::Field(InfluxFieldType::String),
            FieldDataType::Integer => InfluxColumnType::Field(InfluxFieldType::Integer),
            FieldDataType::UInteger => InfluxColumnType::Field(InfluxFieldType::UInteger),
            FieldDataType::Float => InfluxColumnType::Field(InfluxFieldType::Float),
            FieldDataType::Boolean => InfluxColumnType::Field(InfluxFieldType::Boolean),
            FieldDataType::Key => panic!("Key is not a valid InfluxDB column type"),
        }
    }
}

/// Defines a last cache in a given table and database
#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone)]
pub struct LastCacheDefinition {
    /// The table id the cache is associated with
    pub table_id: TableId,
    /// The table name the cache is associated with
    pub table: Arc<str>,
    /// Given name of the cache
    pub name: Arc<str>,
    /// Columns intended to be used as predicates in the cache
    pub key_columns: Vec<ColumnId>,
    /// Columns that store values in the cache
    pub value_columns: LastCacheValueColumnsDef,
    /// The number of last values to hold in the cache
    pub count: LastCacheSize,
    /// The time-to-live (TTL) in seconds for entries in the cache
    pub ttl: u64,
}

impl LastCacheDefinition {
    /// Create a new [`LastCacheDefinition`] with explicit value columns
    ///
    /// This is intended for tests and expects that the column id for the time
    /// column is included in the value columns argument.
    pub fn new_with_explicit_value_columns(
        table_id: TableId,
        table: impl Into<Arc<str>>,
        name: impl Into<Arc<str>>,
        key_columns: Vec<ColumnId>,
        value_columns: Vec<ColumnId>,
        count: usize,
        ttl: u64,
    ) -> Result<Self, Error> {
        Ok(Self {
            table_id,
            table: table.into(),
            name: name.into(),
            key_columns,
            value_columns: LastCacheValueColumnsDef::Explicit {
                columns: value_columns,
            },
            count: count.try_into()?,
            ttl,
        })
    }

    /// Create a new [`LastCacheDefinition`] with explicit value columns
    ///
    /// This is intended for tests.
    pub fn new_all_non_key_value_columns(
        table_id: TableId,
        table: impl Into<Arc<str>>,
        name: impl Into<Arc<str>>,
        key_columns: Vec<ColumnId>,
        count: usize,
        ttl: u64,
    ) -> Result<Self, Error> {
        Ok(Self {
            table_id,
            table: table.into(),
            name: name.into(),
            key_columns,
            value_columns: LastCacheValueColumnsDef::AllNonKeyColumns,
            count: count.try_into()?,
            ttl,
        })
    }
}

/// A last cache will either store values for an explicit set of columns, or will accept all
/// non-key columns
#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone)]
#[serde(rename_all = "snake_case")]
pub enum LastCacheValueColumnsDef {
    /// Explicit list of column names
    Explicit { columns: Vec<ColumnId> },
    /// Stores all non-key columns
    AllNonKeyColumns,
}

/// The maximum allowed size for a last cache
pub const LAST_CACHE_MAX_SIZE: usize = 10;

/// The size of the last cache
///
/// Must be between 1 and [`LAST_CACHE_MAX_SIZE`]
#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone, Copy)]
pub struct LastCacheSize(usize);

impl LastCacheSize {
    pub fn new(size: usize) -> Result<Self, Error> {
        if size == 0 || size > LAST_CACHE_MAX_SIZE {
            Err(Error::InvalidLastCacheSize)
        } else {
            Ok(Self(size))
        }
    }
}

impl Default for LastCacheSize {
    fn default() -> Self {
        Self(1)
    }
}

impl TryFrom<usize> for LastCacheSize {
    type Error = Error;

    fn try_from(value: usize) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl From<LastCacheSize> for usize {
    fn from(value: LastCacheSize) -> Self {
        value.0
    }
}

impl From<LastCacheSize> for u64 {
    fn from(value: LastCacheSize) -> Self {
        value
            .0
            .try_into()
            .expect("usize fits into a 64 bit unsigned integer")
    }
}

impl PartialEq<usize> for LastCacheSize {
    fn eq(&self, other: &usize) -> bool {
        self.0.eq(other)
    }
}

impl PartialEq<LastCacheSize> for usize {
    fn eq(&self, other: &LastCacheSize) -> bool {
        self.eq(&other.0)
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct LastCacheDelete {
    pub table_name: Arc<str>,
    pub table_id: TableId,
    pub name: Arc<str>,
}

/// Defines a distinct value cache in a given table and database
#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone)]
pub struct DistinctCacheDefinition {
    /// The id of the associated table
    pub table_id: TableId,
    /// The name of the associated table
    pub table_name: Arc<str>,
    /// The name of the cache, is unique within the associated table
    pub cache_name: Arc<str>,
    /// The ids of columns tracked by this distinct value cache, in the defined order
    pub column_ids: Vec<ColumnId>,
    /// The maximum number of distinct value combintions the cache will hold
    pub max_cardinality: usize,
    /// The maximum age in seconds, similar to a time-to-live (TTL), for entries in the cache
    pub max_age_seconds: u64,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct DistinctCacheDelete {
    pub table_name: Arc<str>,
    pub table_id: TableId,
    pub cache_name: Arc<str>,
}

#[derive(Debug, Eq, PartialEq, Clone, Serialize, Deserialize, Copy)]
#[serde(rename_all = "snake_case")]
pub enum PluginType {
    WalRows,
    Schedule,
    Request,
}

impl Display for PluginType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Debug::fmt(self, f)
    }
}

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone)]
pub struct TriggerDefinition {
    pub trigger_name: String,
    pub plugin_filename: String,
    pub database_name: String,
    pub trigger: TriggerSpecificationDefinition,
    pub trigger_arguments: Option<HashMap<String, String>>,
    pub disabled: bool,
}

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone)]
pub struct DeleteTriggerDefinition {
    pub trigger_name: String,
    #[serde(default)]
    pub force: bool,
}

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone)]
pub struct TriggerIdentifier {
    pub db_name: String,
    pub trigger_name: String,
}

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone)]
#[serde(rename_all = "snake_case")]
pub enum TriggerSpecificationDefinition {
    SingleTableWalWrite { table_name: String },
    AllTablesWalWrite,
    Schedule { schedule: String },
    RequestPath { path: String },
    Every { duration: Duration },
}

impl TriggerSpecificationDefinition {
    pub fn from_string_rep(spec_str: &str) -> Result<TriggerSpecificationDefinition, Error> {
        let spec_str = spec_str.trim();
        match spec_str {
            s if s.starts_with("table:") => {
                let table_name = s.trim_start_matches("table:").trim();
                if table_name.is_empty() {
                    return Err(Error::TriggerSpecificationParseError {
                        trigger_spec: spec_str.to_string(),
                        context: Some("table name is empty".to_string()),
                    });
                }
                Ok(TriggerSpecificationDefinition::SingleTableWalWrite {
                    table_name: table_name.to_string(),
                })
            }
            "all_tables" => Ok(TriggerSpecificationDefinition::AllTablesWalWrite),
            s if s.starts_with("cron:") => {
                let cron_schedule = s.trim_start_matches("cron:").trim();
                if cron_schedule.is_empty() || Schedule::from_str(cron_schedule).is_err() {
                    return Err(Error::TriggerSpecificationParseError {
                        trigger_spec: spec_str.to_string(),
                        context: None,
                    });
                }
                Ok(TriggerSpecificationDefinition::Schedule {
                    schedule: cron_schedule.to_string(),
                })
            }
            s if s.starts_with("every:") => {
                let duration_str = s.trim_start_matches("every:").trim();
                let Ok(duration) = parse_duration(duration_str) else {
                    return Err(Error::TriggerSpecificationParseError {
                        trigger_spec: spec_str.to_string(),
                        context: Some("couldn't parse to duration".to_string()),
                    });
                };
                if duration > parse_duration("1 year").unwrap() {
                    return Err(Error::TriggerSpecificationParseError {
                        trigger_spec: spec_str.to_string(),
                        context: Some("don't support every schedules of over 1 year".to_string()),
                    });
                }
                Ok(TriggerSpecificationDefinition::Every { duration })
            }
            s if s.starts_with("request:") => {
                let path = s.trim_start_matches("request:").trim();
                if path.is_empty() {
                    return Err(Error::TriggerSpecificationParseError {
                        trigger_spec: spec_str.to_string(),
                        context: None,
                    });
                }
                Ok(TriggerSpecificationDefinition::RequestPath {
                    path: path.to_string(),
                })
            }
            _ => Err(Error::TriggerSpecificationParseError {
                trigger_spec: spec_str.to_string(),
                context: Some("expect one of the following prefixes: 'table:', 'all_tables:', 'cron:', 'every:', or 'request:'".to_string()),
            }),
        }
    }

    pub fn string_rep(&self) -> String {
        match self {
            TriggerSpecificationDefinition::SingleTableWalWrite { table_name } => {
                format!("table:{}", table_name)
            }
            TriggerSpecificationDefinition::AllTablesWalWrite => "all_tables".to_string(),
            TriggerSpecificationDefinition::Schedule { schedule } => {
                format!("cron:{}", schedule)
            }
            TriggerSpecificationDefinition::Every { duration } => {
                format!("every:{}", format_duration(*duration))
            }
            TriggerSpecificationDefinition::RequestPath { path } => {
                format!("request:{}", path)
            }
        }
    }

    pub fn plugin_type(&self) -> PluginType {
        match self {
            TriggerSpecificationDefinition::SingleTableWalWrite { .. }
            | TriggerSpecificationDefinition::AllTablesWalWrite => PluginType::WalRows,
            TriggerSpecificationDefinition::Schedule { .. }
            | TriggerSpecificationDefinition::Every { .. } => PluginType::Schedule,
            TriggerSpecificationDefinition::RequestPath { .. } => PluginType::Request,
        }
    }
}

#[serde_as]
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct WriteBatch {
    pub database_id: DbId,
    pub database_name: Arc<str>,
    pub table_chunks: SerdeVecMap<TableId, TableChunks>,
    pub min_time_ns: i64,
    pub max_time_ns: i64,
}

impl WriteBatch {
    pub fn new(
        database_id: DbId,
        database_name: Arc<str>,
        table_chunks: IndexMap<TableId, TableChunks>,
    ) -> Self {
        // find the min and max times across the table chunks
        let (min_time_ns, max_time_ns) = table_chunks.values().fold(
            (i64::MAX, i64::MIN),
            |(min_time_ns, max_time_ns), table_chunks| {
                (
                    min_time_ns.min(table_chunks.min_time),
                    max_time_ns.max(table_chunks.max_time),
                )
            },
        );

        Self {
            database_id,
            database_name,
            table_chunks: table_chunks.into(),
            min_time_ns,
            max_time_ns,
        }
    }

    pub fn add_write_batch(
        &mut self,
        new_table_chunks: SerdeVecMap<TableId, TableChunks>,
        min_time_ns: i64,
        max_time_ns: i64,
    ) {
        self.min_time_ns = self.min_time_ns.min(min_time_ns);
        self.max_time_ns = self.max_time_ns.max(max_time_ns);

        for (table_name, new_chunks) in new_table_chunks {
            let chunks = self.table_chunks.entry(table_name).or_default();
            for (chunk_time, new_chunk) in new_chunks.chunk_time_to_chunk {
                let chunk = chunks.chunk_time_to_chunk.entry(chunk_time).or_default();
                for r in &new_chunk.rows {
                    chunks.min_time = chunks.min_time.min(r.time);
                    chunks.max_time = chunks.max_time.max(r.time);
                }
                chunk.rows.extend(new_chunk.rows);
            }
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct TableChunks {
    pub min_time: i64,
    pub max_time: i64,
    pub chunk_time_to_chunk: HashMap<i64, TableChunk>,
}

impl Default for TableChunks {
    fn default() -> Self {
        Self {
            min_time: i64::MAX,
            max_time: i64::MIN,
            chunk_time_to_chunk: Default::default(),
        }
    }
}

impl TableChunks {
    pub fn push_row(&mut self, chunk_time: i64, row: Row) {
        self.min_time = self.min_time.min(row.time);
        self.max_time = self.max_time.max(row.time);
        let chunk = self.chunk_time_to_chunk.entry(chunk_time).or_default();
        chunk.rows.push(row);
    }

    pub fn row_count(&self) -> usize {
        self.chunk_time_to_chunk
            .values()
            .map(|c| c.rows.len())
            .sum()
    }
}

#[derive(Debug, Default, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct TableChunk {
    pub rows: Vec<Row>,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct Field {
    pub id: ColumnId,
    pub value: FieldData,
}

impl Field {
    pub fn new(id: ColumnId, value: impl Into<FieldData>) -> Self {
        Self {
            id,
            value: value.into(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Row {
    pub time: i64,
    pub fields: Vec<Field>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum FieldData {
    Timestamp(i64),
    Key(String),
    Tag(String),
    String(String),
    Integer(i64),
    UInteger(u64),
    Float(f64),
    Boolean(bool),
}

impl PartialEq for FieldData {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (FieldData::Timestamp(a), FieldData::Timestamp(b)) => a == b,
            (FieldData::Tag(a), FieldData::Tag(b)) => a == b,
            (FieldData::Key(a), FieldData::Key(b)) => a == b,
            (FieldData::String(a), FieldData::String(b)) => a == b,
            (FieldData::Integer(a), FieldData::Integer(b)) => a == b,
            (FieldData::UInteger(a), FieldData::UInteger(b)) => a == b,
            (FieldData::Float(a), FieldData::Float(b)) => a == b,
            (FieldData::Boolean(a), FieldData::Boolean(b)) => a == b,
            _ => false,
        }
    }
}

impl Eq for FieldData {}

impl<'a> From<&SeriesValue<'a>> for FieldData {
    fn from(sk: &SeriesValue<'a>) -> Self {
        match sk {
            SeriesValue::String(s) => Self::Key(s.to_string()),
        }
    }
}

impl<'a> From<FieldValue<'a>> for FieldData {
    fn from(value: FieldValue<'a>) -> Self {
        match value {
            FieldValue::I64(v) => Self::Integer(v),
            FieldValue::U64(v) => Self::UInteger(v),
            FieldValue::F64(v) => Self::Float(v),
            FieldValue::String(v) => Self::String(v.to_string()),
            FieldValue::Boolean(v) => Self::Boolean(v),
        }
    }
}

impl<'a> From<&FieldValue<'a>> for FieldData {
    fn from(value: &FieldValue<'a>) -> Self {
        match value {
            FieldValue::I64(v) => Self::Integer(*v),
            FieldValue::U64(v) => Self::UInteger(*v),
            FieldValue::F64(v) => Self::Float(*v),
            FieldValue::String(v) => Self::String(v.to_string()),
            FieldValue::Boolean(v) => Self::Boolean(*v),
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct WalContents {
    /// The time at which this WAL file is being persisted
    pub persist_timestamp_ms: i64,
    /// The min timestamp from any writes in the WAL file
    pub min_timestamp_ns: i64,
    /// The max timestamp from any writes in the WAL file
    pub max_timestamp_ns: i64,
    /// A number that increments for every generated WAL file
    pub wal_file_number: WalFileSequenceNumber,
    /// The operations contained in the WAL file
    pub ops: Vec<WalOp>,
    /// If present, the buffer should be snapshot after the contents of this file are loaded.
    pub snapshot: Option<SnapshotDetails>,
}

impl WalContents {
    pub fn is_empty(&self) -> bool {
        self.ops.is_empty() && self.snapshot.is_none()
    }
}

#[derive(
    Debug, Default, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
pub struct WalFileSequenceNumber(u64);

impl WalFileSequenceNumber {
    pub fn new(id: u64) -> Self {
        Self(id)
    }

    pub fn next(&self) -> Self {
        Self(self.0 + 1)
    }

    pub fn as_u64(&self) -> u64 {
        self.0
    }
}

impl std::fmt::Display for WalFileSequenceNumber {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl FromStr for WalFileSequenceNumber {
    type Err = ParseIntError;

    fn from_str(s: &str) -> std::prelude::v1::Result<Self, Self::Err> {
        s.parse::<u64>().map(Self)
    }
}

#[derive(
    Debug, Default, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
pub struct SnapshotSequenceNumber(u64);

impl SnapshotSequenceNumber {
    pub fn new(number: u64) -> Self {
        Self(number)
    }

    pub fn next(&self) -> Self {
        Self(self.0 + 1)
    }

    pub fn as_u64(&self) -> u64 {
        self.0
    }
}

impl std::fmt::Display for SnapshotSequenceNumber {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Details about a snapshot of the WAL
#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
pub struct SnapshotDetails {
    /// The sequence number for this snapshot
    pub snapshot_sequence_number: SnapshotSequenceNumber,
    /// All chunks with data before this time can be snapshot and persisted
    pub end_time_marker: i64,
    /// All wal files with a sequence number >= to this can be deleted once snapshotting is complete
    pub first_wal_sequence_number: WalFileSequenceNumber,
    /// All wal files with a sequence number <= to this can be deleted once snapshotting is complete
    pub last_wal_sequence_number: WalFileSequenceNumber,
    // both forced and 3 times snapshot size should set this flag
    pub forced: bool,
}

pub fn background_wal_flush<W: Wal>(
    wal: Arc<W>,
    flush_interval: Duration,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(flush_interval);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            interval.tick().await;

            let cleanup_after_snapshot = wal.flush_buffer().await;

            // handle snapshot cleanup outside of the flush loop
            if let Some((snapshot_complete, snapshot_info, snapshot_permit)) =
                cleanup_after_snapshot
            {
                let snapshot_wal = Arc::clone(&wal);
                tokio::spawn(async move {
                    let snapshot_details = snapshot_complete.await.expect("snapshot failed");
                    assert_eq!(snapshot_info, snapshot_details);

                    snapshot_wal
                        .cleanup_snapshot(snapshot_info, snapshot_permit)
                        .await;
                });
            }
        }
    })
}
