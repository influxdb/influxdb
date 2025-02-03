//! This package contains definitions for writing data into InfluxDB3. The logical data model is the standard
//! InfluxDB model of Database > Table > Row. As the data arrives into the server it is written into the wal
//! and buffered in memory in a queryable format. Periodically the WAL is snapshot, which converts the in memory
//! data into parquet files that are persisted to object storage. A snapshot file is written that contains the
//! metadata of the parquet files that were written in that snapshot.

pub mod chunk;
pub mod paths;
pub mod persister;
pub mod write_buffer;

use anyhow::Context;
use async_trait::async_trait;
use data_types::{NamespaceName, TimestampMinMax};
use datafusion::{
    catalog::Session,
    common::{Column, DFSchema},
    error::DataFusionError,
    execution::context::ExecutionProps,
    logical_expr::interval_arithmetic::Interval,
    physical_expr::{analyze, create_physical_expr, AnalysisContext, ExprBoundaries},
    prelude::Expr,
    scalar::ScalarValue,
};
use influxdb3_cache::{
    distinct_cache::{CreateDistinctCacheArgs, DistinctCacheProvider},
    last_cache::LastCacheProvider,
};
use influxdb3_catalog::catalog::{Catalog, CatalogSequenceNumber, DatabaseSchema, TableDefinition};
use influxdb3_id::{ColumnId, DbId, ParquetFileId, SerdeVecMap, TableId};
use influxdb3_wal::{
    DistinctCacheDefinition, LastCacheDefinition, SnapshotSequenceNumber, Wal,
    WalFileSequenceNumber,
};
use iox_query::QueryChunk;
use iox_time::Time;
use miette::Diagnostic;
use observability_deps::tracing::debug;
use schema::TIME_COLUMN_NAME;
use serde::{Deserialize, Serialize};
use std::{fmt::Debug, sync::Arc, time::Duration};
use thiserror::Error;

#[derive(Debug, Diagnostic, Error)]
#[diagnostic(
    code(influxdb3_write::lib),
    url("https://github.com/influxdata/influxdb/issues/new?template=bug_report.md")
)]
pub enum Error {
    #[error("object store path error")]
    ObjStorePath(#[from] object_store::path::Error),

    #[error("write buffer error")]
    WriteBuffer(#[from] write_buffer::Error),

    #[error("persister error")]
    Persister(#[from] persister::Error),

    #[error(transparent)]
    Anyhow(#[from] anyhow::Error),
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

pub trait WriteBuffer:
    Bufferer + ChunkContainer + DistinctCacheManager + LastCacheManager + DatabaseManager
{
}

/// Database manager - supports only delete operation
#[async_trait::async_trait]
pub trait DatabaseManager: Debug + Send + Sync + 'static {
    async fn create_database(&self, name: String) -> Result<(), write_buffer::Error>;
    async fn soft_delete_database(&self, name: String) -> Result<(), write_buffer::Error>;
    async fn create_table(
        &self,
        db: String,
        table: String,
        tags: Vec<String>,
        fields: Vec<(String, String)>,
    ) -> Result<(), write_buffer::Error>;
    async fn soft_delete_table(
        &self,
        db_name: String,
        table_name: String,
    ) -> Result<(), write_buffer::Error>;
}

/// The buffer is for buffering data in memory and in the wal before it is persisted as parquet files in storage.
#[async_trait]
pub trait Bufferer: Debug + Send + Sync + 'static {
    /// Validates the line protocol, writes it into the WAL if configured, writes it into the in memory buffer
    /// and returns the result with any lines that had errors and summary statistics.
    async fn write_lp(
        &self,
        database: NamespaceName<'static>,
        lp: &str,
        ingest_time: Time,
        accept_partial: bool,
        precision: Precision,
        no_sync: bool,
    ) -> write_buffer::Result<BufferedWriteRequest>;

    /// Returns the database schema provider
    fn catalog(&self) -> Arc<Catalog>;

    /// Reutrns the WAL this bufferer is using
    fn wal(&self) -> Arc<dyn Wal>;

    /// Returns the parquet files for a given database and table
    fn parquet_files(&self, db_id: DbId, table_id: TableId) -> Vec<ParquetFile> {
        self.parquet_files_filtered(db_id, table_id, &ChunkFilter::default())
    }

    /// Returns the parquet files for a given database and table that satisfy the given filter
    fn parquet_files_filtered(
        &self,
        db_id: DbId,
        table_id: TableId,
        filter: &ChunkFilter,
    ) -> Vec<ParquetFile>;

    /// A channel to watch for when new persisted snapshots are created
    fn watch_persisted_snapshots(&self) -> tokio::sync::watch::Receiver<Option<PersistedSnapshot>>;
}

/// ChunkContainer is used by the query engine to get chunks for a given table. Chunks will generally be in the
/// `Bufferer` for those in memory from buffered writes or the `Persister` for parquet files that have been persisted.
pub trait ChunkContainer: Debug + Send + Sync + 'static {
    fn get_table_chunks(
        &self,
        db_schema: Arc<DatabaseSchema>,
        table_def: Arc<TableDefinition>,
        filter: &ChunkFilter,
        projection: Option<&Vec<usize>>,
        ctx: &dyn Session,
    ) -> Result<Vec<Arc<dyn QueryChunk>>, DataFusionError>;
}

/// [`DistinctCacheManager`] is used to manage interaction with a [`DistinctCacheProvider`]. This enables
/// cache creation, deletion, and getting access to existing
#[async_trait::async_trait]
pub trait DistinctCacheManager: Debug + Send + Sync + 'static {
    /// Get a reference to the distinct value cache provider
    fn distinct_cache_provider(&self) -> Arc<DistinctCacheProvider>;

    /// Create a new distinct value cache
    async fn create_distinct_cache(
        &self,
        db_schema: Arc<DatabaseSchema>,
        cache_name: Option<String>,
        args: CreateDistinctCacheArgs,
    ) -> Result<Option<DistinctCacheDefinition>, write_buffer::Error>;

    /// Delete a distinct value cache
    async fn delete_distinct_cache(
        &self,
        db_id: &DbId,
        tbl_id: &TableId,
        cache_name: &str,
    ) -> Result<(), write_buffer::Error>;
}

/// [`LastCacheManager`] is used to manage interaction with a last-n-value cache provider. This enables
/// cache creation, deletion, and getting access to existing caches in underlying [`LastCacheProvider`].
/// It is important that the state of the cache is also maintained in the catalog.
#[async_trait::async_trait]
pub trait LastCacheManager: Debug + Send + Sync + 'static {
    /// Get a reference to the last cache provider
    fn last_cache_provider(&self) -> Arc<LastCacheProvider>;
    /// Create a new last-n-value cache
    ///
    /// This should handle updating the catalog with the cache information, so that it will be
    /// preserved on server restarts.
    #[allow(clippy::too_many_arguments)]
    async fn create_last_cache(
        &self,
        db_id: DbId,
        tbl_id: TableId,
        cache_name: Option<&str>,
        count: Option<usize>,
        ttl: Option<Duration>,
        key_columns: Option<Vec<ColumnId>>,
        value_columns: Option<Vec<ColumnId>>,
    ) -> Result<Option<LastCacheDefinition>, write_buffer::Error>;
    /// Delete a last-n-value cache
    ///
    /// This should handle removal of the cache's information from the catalog as well
    async fn delete_last_cache(
        &self,
        db_id: DbId,
        tbl_id: TableId,
        cache_name: &str,
    ) -> Result<(), write_buffer::Error>;
}

/// A single write request can have many lines in it. A writer can request to accept all lines that are valid, while
/// returning an error for any invalid lines. This is the error information for a single invalid line.
#[derive(Debug, Serialize)]
pub struct WriteLineError {
    pub original_line: String,
    pub line_number: usize,
    pub error_message: String,
}

/// A write that has been validated against the catalog schema, written to the WAL (if configured), and buffered in
/// memory. This is the summary information for the write along with any errors that were encountered.
#[derive(Debug)]
pub struct BufferedWriteRequest {
    pub db_name: NamespaceName<'static>,
    pub invalid_lines: Vec<WriteLineError>,
    pub line_count: usize,
    pub field_count: usize,
    pub index_count: usize,
}

/// The collection of Parquet files that were persisted in a snapshot
#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone)]
pub struct PersistedSnapshot {
    /// The node identifier that persisted this snapshot
    // TODO: deprecate this alias
    #[serde(alias = "writer_id")]
    pub node_id: String,
    /// The next file id to be used with `ParquetFile`s when the snapshot is loaded
    pub next_file_id: ParquetFileId,
    /// The next db id to be used for databases when the snapshot is loaded
    pub next_db_id: DbId,
    /// The next table id to be used for tables when the snapshot is loaded
    pub next_table_id: TableId,
    /// The next column id to be used for columns when the snapshot is loaded
    pub next_column_id: ColumnId,
    /// The snapshot sequence number associated with this snapshot
    pub snapshot_sequence_number: SnapshotSequenceNumber,
    /// The wal file sequence number that triggered this snapshot
    pub wal_file_sequence_number: WalFileSequenceNumber,
    /// The catalog sequence number associated with this snapshot
    pub catalog_sequence_number: CatalogSequenceNumber,
    /// The size of the snapshot parquet files in bytes.
    pub parquet_size_bytes: u64,
    /// The number of rows across all parquet files in the snapshot.
    pub row_count: u64,
    /// The min time from all parquet files in the snapshot.
    pub min_time: i64,
    /// The max time from all parquet files in the snapshot.
    pub max_time: i64,
    /// The collection of databases that had tables persisted in this snapshot. The tables will then have their
    /// name and the parquet file.
    pub databases: SerdeVecMap<DbId, DatabaseTables>,
}

impl PersistedSnapshot {
    pub fn new(
        node_id: String,
        snapshot_sequence_number: SnapshotSequenceNumber,
        wal_file_sequence_number: WalFileSequenceNumber,
        catalog_sequence_number: CatalogSequenceNumber,
    ) -> Self {
        Self {
            node_id,
            next_file_id: ParquetFileId::next_id(),
            next_db_id: DbId::next_id(),
            next_table_id: TableId::next_id(),
            next_column_id: ColumnId::next_id(),
            snapshot_sequence_number,
            wal_file_sequence_number,
            catalog_sequence_number,
            parquet_size_bytes: 0,
            row_count: 0,
            min_time: i64::MAX,
            max_time: i64::MIN,
            databases: SerdeVecMap::new(),
        }
    }

    fn add_parquet_file(
        &mut self,
        database_id: DbId,
        table_id: TableId,
        parquet_file: ParquetFile,
    ) {
        // Update the next_file_id field, as we likely have a new file
        self.next_file_id = ParquetFileId::next_id();
        self.parquet_size_bytes += parquet_file.size_bytes;
        self.row_count += parquet_file.row_count;
        self.min_time = self.min_time.min(parquet_file.min_time);
        self.max_time = self.max_time.max(parquet_file.max_time);

        self.databases
            .entry(database_id)
            .or_default()
            .tables
            .entry(table_id)
            .or_default()
            .push(parquet_file);
    }

    pub fn db_table_and_file_count(&self) -> (u64, u64, u64) {
        let mut db_count = 0;
        let mut table_count = 0;
        let mut file_count = 0;
        for (_, db_tables) in &self.databases {
            db_count += 1;
            table_count += db_tables.tables.len() as u64;
            file_count += db_tables.tables.values().fold(0, |mut acc, files| {
                acc += files.len() as u64;
                acc
            });
        }
        (db_count, table_count, file_count)
    }

    pub fn overall_db_table_file_counts(host_snapshots: &[PersistedSnapshot]) -> (u64, u64, u64) {
        let overall_counts = host_snapshots.iter().fold((0, 0, 0), |mut acc, item| {
            let (db_count, table_count, file_count) = item.db_table_and_file_count();
            acc.0 += db_count;
            acc.1 += table_count;
            acc.2 += file_count;
            acc
        });
        overall_counts
    }
}

#[derive(Debug, Serialize, Deserialize, Default, Eq, PartialEq, Clone)]
pub struct DatabaseTables {
    pub tables: SerdeVecMap<TableId, Vec<ParquetFile>>,
}

/// The summary data for a persisted parquet file in a snapshot.
#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone)]
pub struct ParquetFile {
    pub id: ParquetFileId,
    pub path: String,
    pub size_bytes: u64,
    pub row_count: u64,
    /// chunk time nanos
    pub chunk_time: i64,
    /// min time nanos
    pub min_time: i64,
    /// max time nanos
    pub max_time: i64,
}

impl ParquetFile {
    pub fn timestamp_min_max(&self) -> TimestampMinMax {
        TimestampMinMax {
            min: self.min_time,
            max: self.max_time,
        }
    }
}

#[cfg(test)]
impl ParquetFile {
    pub(crate) fn create_for_test(path: impl Into<String>) -> Self {
        Self {
            id: ParquetFileId::new(),
            path: path.into(),
            size_bytes: 1024,
            row_count: 1,
            chunk_time: 0,
            min_time: 0,
            max_time: 1,
        }
    }
}

/// The precision of the timestamp
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum Precision {
    Auto,
    Second,
    Millisecond,
    Microsecond,
    Nanosecond,
}

impl Default for Precision {
    fn default() -> Self {
        Self::Auto
    }
}

impl From<iox_http::write::Precision> for Precision {
    fn from(legacy: iox_http::write::Precision) -> Self {
        match legacy {
            iox_http::write::Precision::Second => Precision::Second,
            iox_http::write::Precision::Millisecond => Precision::Millisecond,
            iox_http::write::Precision::Microsecond => Precision::Microsecond,
            iox_http::write::Precision::Nanosecond => Precision::Nanosecond,
        }
    }
}

/// Guess precision based off of a given timestamp.
// Note that this will fail in June 2128, but that's not our problem
pub(crate) fn guess_precision(timestamp: i64) -> Precision {
    const NANO_SECS_PER_SEC: i64 = 1_000_000_000;
    // Get the absolute value of the timestamp so we can work with negative
    // numbers
    let val = timestamp.abs() / NANO_SECS_PER_SEC;

    if val < 5 {
        // If the time sent to us is in seconds then this will be a number less than
        // 5 so for example if the time in seconds is 1_708_976_567 then it will be
        // 1 (due to integer truncation) and be less than 5
        Precision::Second
    } else if val < 5_000 {
        // If however the value is milliseconds and not seconds than the same number
        // for time but now in milliseconds 1_708_976_567_000 when divided will now
        // be 1708 which is bigger than the previous if statement but less than this
        // one and so we return milliseconds
        Precision::Millisecond
    } else if val < 5_000_000 {
        // If we do the same thing here by going up another order of magnitude then
        // 1_708_976_567_000_000 when divided will be 1708976 which is large enough
        // for this if statement
        Precision::Microsecond
    } else {
        // Anything else we can assume is large enough of a number that it must
        // be nanoseconds
        Precision::Nanosecond
    }
}

/// A derived set of filters that are used to prune data in the buffer when serving queries
#[derive(Debug, Default)]
pub struct ChunkFilter<'a> {
    time_lower_bound_ns: Option<i64>,
    time_upper_bound_ns: Option<i64>,
    filters: &'a [Expr],
}

impl<'a> ChunkFilter<'a> {
    /// Create a new `ChunkFilter` given a [`TableDefinition`] and set of filter [`Expr`]s from
    /// a logical query plan.
    ///
    /// This method analyzes the incoming `exprs` to determine if there are any filters on the
    /// `time` column and attempt to derive the boundaries on `time` from the query.
    pub fn new(table_def: &Arc<TableDefinition>, exprs: &'a [Expr]) -> Result<Self> {
        debug!(input = ?exprs, ">>> creating chunk filter");
        let mut time_interval: Option<Interval> = None;
        let arrow_schema = table_def.schema.as_arrow();
        let time_col_index = arrow_schema
            .fields()
            .iter()
            .position(|f| f.name() == TIME_COLUMN_NAME)
            .context("table should have a time column")?;

        // DF schema and execution properties used for handling physical expressions:
        let df_schema = DFSchema::try_from(Arc::clone(&arrow_schema))
            .context("table schema was not able to convert to datafusion schema")?;
        let props = ExecutionProps::new();

        for expr in exprs.iter().filter(|e| {
            // NOTE: filter out most expression types, as they are not relevant to time bound
            // analysis:
            matches!(e, Expr::BinaryExpr(_) | Expr::Not(_) | Expr::Between(_))
            // Check if the expression refers to the `time` column:
                && e.column_refs()
                    .contains(&Column::new_unqualified(TIME_COLUMN_NAME))
        }) {
            let Ok(physical_expr) = create_physical_expr(expr, &df_schema, &props) else {
                continue;
            };
            // Determine time bounds, if provided:
            let boundaries = ExprBoundaries::try_new_unbounded(&arrow_schema)
                .context("unable to create unbounded expr boundaries on incoming expression")?;
            let mut analysis = analyze(
                &physical_expr,
                AnalysisContext::new(boundaries),
                &arrow_schema,
            )
            .context("unable to analyze provided filters for a boundary on the time column")?;

            // Set the boundaries on the time column using the evaluated interval, if it exisxts
            // If an interval was already derived from a previous expression, we take their
            // intersection, or produce an error if:
            // - the derived intervals are not compatible (different types)
            // - the derived intervals do not intersect, this should be a user error, i.e., a
            //   poorly formed query
            if let Some(ExprBoundaries { interval, .. }) = (time_col_index
                < analysis.boundaries.len())
            .then_some(analysis.boundaries.remove(time_col_index))
            {
                if let Some(existing) = time_interval.take() {
                    let intersection = existing.intersect(interval).context(
                                "failed to derive a time interval from provided filters",
                            )?.context("provided filters on time column did not produce a valid set of boundaries")?;
                    time_interval.replace(intersection);
                } else {
                    time_interval.replace(interval);
                }
            }
        }

        // Determine the lower and upper bound from the derived interval on time:
        // TODO: we may open this up more to other scalar types, e.g., other timestamp types
        //       depending on how users define time bounds.
        let (time_lower_bound_ns, time_upper_bound_ns) = if let Some(i) = time_interval {
            let low = if let ScalarValue::TimestampNanosecond(Some(l), _) = i.lower() {
                Some(*l)
            } else {
                None
            };
            let high = if let ScalarValue::TimestampNanosecond(Some(h), _) = i.upper() {
                Some(*h)
            } else {
                None
            };

            (low, high)
        } else {
            (None, None)
        };

        Ok(Self {
            time_lower_bound_ns,
            time_upper_bound_ns,
            filters: exprs,
        })
    }

    /// Test a `min` and `max` time against this filter to check if the range they define overlaps
    /// with the range defined by the bounds in this filter.
    pub fn test_time_stamp_min_max(&self, min_time_ns: i64, max_time_ns: i64) -> bool {
        match (self.time_lower_bound_ns, self.time_upper_bound_ns) {
            (None, None) => true,
            (None, Some(u)) => min_time_ns <= u,
            (Some(l), None) => max_time_ns >= l,
            (Some(l), Some(u)) => min_time_ns <= u && max_time_ns >= l,
        }
    }

    pub fn original_filters(&self) -> &[Expr] {
        self.filters
    }
}

pub mod test_helpers {
    use crate::ChunkFilter;
    use crate::WriteBuffer;
    use arrow::array::RecordBatch;
    use datafusion::prelude::Expr;
    use iox_query::exec::IOxSessionContext;

    /// Helper trait for getting [`RecordBatch`]es from a [`WriteBuffer`] implementation in tests
    #[async_trait::async_trait]
    pub trait WriteBufferTester {
        /// Get record batches for the given database and table, using the provided filter `Expr`s
        async fn get_record_batches_filtered_unchecked(
            &self,
            database_name: &str,
            table_name: &str,
            filters: &[Expr],
            ctx: &IOxSessionContext,
        ) -> Vec<RecordBatch>;

        /// Get record batches for the given database and table
        async fn get_record_batches_unchecked(
            &self,
            database_name: &str,
            table_name: &str,
            ctx: &IOxSessionContext,
        ) -> Vec<RecordBatch>;
    }

    #[async_trait::async_trait]
    impl<T> WriteBufferTester for T
    where
        T: WriteBuffer,
    {
        async fn get_record_batches_filtered_unchecked(
            &self,
            database_name: &str,
            table_name: &str,
            filters: &[Expr],
            ctx: &IOxSessionContext,
        ) -> Vec<RecordBatch> {
            let db_schema = self
                .catalog()
                .db_schema(database_name)
                .expect("database should exist");
            let table_def = db_schema
                .table_definition(table_name)
                .expect("table should exist");
            let filter =
                ChunkFilter::new(&table_def, filters).expect("filter expressions should be valid");
            let chunks = self
                .get_table_chunks(db_schema, table_def, &filter, None, &ctx.inner().state())
                .expect("should get query chunks");
            let mut batches = vec![];
            for chunk in chunks {
                batches.extend(
                    chunk
                        .data()
                        .read_to_batches(chunk.schema(), ctx.inner())
                        .await,
                );
            }
            batches
        }

        async fn get_record_batches_unchecked(
            &self,
            database_name: &str,
            table_name: &str,
            ctx: &IOxSessionContext,
        ) -> Vec<RecordBatch> {
            self.get_record_batches_filtered_unchecked(database_name, table_name, &[], ctx)
                .await
        }
    }
}

#[cfg(test)]
mod tests {
    use influxdb3_catalog::catalog::CatalogSequenceNumber;
    use influxdb3_id::{ColumnId, DbId, ParquetFileId, SerdeVecMap, TableId};
    use influxdb3_wal::{SnapshotSequenceNumber, WalFileSequenceNumber};

    use crate::{DatabaseTables, ParquetFile, PersistedSnapshot};

    #[test]
    fn test_overall_counts() {
        let host = "host_id";
        // db 1 setup
        let db_id_1 = DbId::from(0);
        let mut dbs_1 = SerdeVecMap::new();
        let table_id_1 = TableId::from(0);
        let mut tables_1 = SerdeVecMap::new();
        let parquet_files_1 = vec![
            ParquetFile {
                id: ParquetFileId::from(1),
                path: "some_path".to_string(),
                size_bytes: 100_000,
                row_count: 200,
                chunk_time: 1123456789,
                min_time: 11234567777,
                max_time: 11234567788,
            },
            ParquetFile {
                id: ParquetFileId::from(2),
                path: "some_path".to_string(),
                size_bytes: 100_000,
                row_count: 200,
                chunk_time: 1123456789,
                min_time: 11234567777,
                max_time: 11234567788,
            },
        ];
        tables_1.insert(table_id_1, parquet_files_1);
        dbs_1.insert(db_id_1, DatabaseTables { tables: tables_1 });

        // add dbs_1 to snapshot
        let persisted_snapshot_1 = PersistedSnapshot {
            node_id: host.to_string(),
            next_file_id: ParquetFileId::from(0),
            next_db_id: DbId::from(1),
            next_table_id: TableId::from(1),
            next_column_id: ColumnId::from(1),
            snapshot_sequence_number: SnapshotSequenceNumber::new(124),
            wal_file_sequence_number: WalFileSequenceNumber::new(100),
            catalog_sequence_number: CatalogSequenceNumber::new(100),
            databases: dbs_1,
            min_time: 0,
            max_time: 1,
            row_count: 0,
            parquet_size_bytes: 0,
        };

        // db 2 setup
        let db_id_2 = DbId::from(2);
        let mut dbs_2 = SerdeVecMap::new();
        let table_id_2 = TableId::from(2);
        let mut tables_2 = SerdeVecMap::new();
        let parquet_files_2 = vec![
            ParquetFile {
                id: ParquetFileId::from(4),
                path: "some_path".to_string(),
                size_bytes: 100_000,
                row_count: 200,
                chunk_time: 1123456789,
                min_time: 11234567777,
                max_time: 11234567788,
            },
            ParquetFile {
                id: ParquetFileId::from(5),
                path: "some_path".to_string(),
                size_bytes: 100_000,
                row_count: 200,
                chunk_time: 1123456789,
                min_time: 11234567777,
                max_time: 11234567788,
            },
        ];
        tables_2.insert(table_id_2, parquet_files_2);
        dbs_2.insert(db_id_2, DatabaseTables { tables: tables_2 });

        // add dbs_2 to snapshot
        let persisted_snapshot_2 = PersistedSnapshot {
            node_id: host.to_string(),
            next_file_id: ParquetFileId::from(5),
            next_db_id: DbId::from(2),
            next_table_id: TableId::from(22),
            next_column_id: ColumnId::from(22),
            snapshot_sequence_number: SnapshotSequenceNumber::new(124),
            wal_file_sequence_number: WalFileSequenceNumber::new(100),
            catalog_sequence_number: CatalogSequenceNumber::new(100),
            databases: dbs_2,
            min_time: 0,
            max_time: 1,
            row_count: 0,
            parquet_size_bytes: 0,
        };

        let overall_counts = PersistedSnapshot::overall_db_table_file_counts(&[
            persisted_snapshot_1,
            persisted_snapshot_2,
        ]);
        assert_eq!((2, 2, 4), overall_counts);
    }

    #[test]
    fn test_overall_counts_zero() {
        // db 1 setup
        let db_id_1 = DbId::from(0);
        let mut dbs_1 = SerdeVecMap::new();
        let table_id_1 = TableId::from(0);
        let mut tables_1 = SerdeVecMap::new();
        let parquet_files_1 = vec![
            ParquetFile {
                id: ParquetFileId::from(1),
                path: "some_path".to_string(),
                size_bytes: 100_000,
                row_count: 200,
                chunk_time: 1123456789,
                min_time: 11234567777,
                max_time: 11234567788,
            },
            ParquetFile {
                id: ParquetFileId::from(2),
                path: "some_path".to_string(),
                size_bytes: 100_000,
                row_count: 200,
                chunk_time: 1123456789,
                min_time: 11234567777,
                max_time: 11234567788,
            },
        ];
        tables_1.insert(table_id_1, parquet_files_1);
        dbs_1.insert(db_id_1, DatabaseTables { tables: tables_1 });

        // db 2 setup
        let db_id_2 = DbId::from(2);
        let mut dbs_2 = SerdeVecMap::new();
        let table_id_2 = TableId::from(2);
        let mut tables_2 = SerdeVecMap::new();
        let parquet_files_2 = vec![
            ParquetFile {
                id: ParquetFileId::from(4),
                path: "some_path".to_string(),
                size_bytes: 100_000,
                row_count: 200,
                chunk_time: 1123456789,
                min_time: 11234567777,
                max_time: 11234567788,
            },
            ParquetFile {
                id: ParquetFileId::from(5),
                path: "some_path".to_string(),
                size_bytes: 100_000,
                row_count: 200,
                chunk_time: 1123456789,
                min_time: 11234567777,
                max_time: 11234567788,
            },
        ];
        tables_2.insert(table_id_2, parquet_files_2);
        dbs_2.insert(db_id_2, DatabaseTables { tables: tables_2 });

        // add dbs_2 to snapshot
        let overall_counts = PersistedSnapshot::overall_db_table_file_counts(&[]);
        assert_eq!((0, 0, 0), overall_counts);
    }
}
