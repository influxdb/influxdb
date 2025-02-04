//! The compactor crate contains code for compacting data and for downstream writers to consume
//! the compacted data. The compactor writes data into an object store location identified
//! by the compactor_id, similar to writers that write data. Writers write generation 1 (gen1)
//! files. The compactor picks up those files and rewrites them into larger gen2 blocks.
//!
//! The compactor also keeps its own catalog along with the compacted data. The catalog is the
//! union of the catalog of all writers that are getting compacted together. The compacted catalog
//! also keeps a mapping of writer ids to the compacted catalog id.
//!
//! This module is split between the producer (which produces the compacted data view), the
//! consumer, which can poll object storage for updated compacted data views, the catalog, and
//! the compacted data view itself.

use arrow::array::as_largestring_array;
use arrow::array::AsArray;
use arrow::array::RecordBatch;
use arrow::compute::cast_with_options;
use arrow::compute::partition;
use arrow::compute::CastOptions;
use arrow::compute::Partitions;
use arrow::datatypes::TimestampNanosecondType;
use arrow::util::display::FormatOptions;
use arrow_schema::ArrowError;
use arrow_schema::DataType;
use arrow_util::util::ensure_schema;
use bytes::{Bytes, BytesMut};
use data_types::ChunkId;
use data_types::ChunkOrder;
use data_types::PartitionHashId;
use data_types::PartitionKey;
use data_types::TableId;
use datafusion::datasource::object_store::ObjectStoreUrl;
use datafusion::error::DataFusionError;
use datafusion::execution::SendableRecordBatchStream;
use futures_util::future::BoxFuture;
use futures_util::StreamExt;
use influxdb3_catalog::catalog::TableDefinition;
use influxdb3_catalog::catalog::TIME_COLUMN_NAME;
use influxdb3_enterprise_data_layout::{CompactedFilePath, Generation};
use influxdb3_enterprise_index::FileIndex;
use influxdb3_enterprise_parquet_cache::ParquetCachePreFetcher;
use influxdb3_id::ColumnId;
use influxdb3_id::ParquetFileId;
use influxdb3_write::chunk::ParquetChunk;
use influxdb3_write::persister::ROW_GROUP_WRITE_SIZE;
use influxdb3_write::ParquetFile;
use iox_query::chunk_statistics::create_chunk_statistics;
use iox_query::chunk_statistics::NoColumnRanges;
use iox_query::exec::Executor;
use iox_query::frontend::reorg;
use iox_query::frontend::reorg::ReorgPlanner;
use iox_query::QueryChunk;
use object_store::path::Path as ObjPath;
use object_store::MultipartUpload;
use object_store::ObjectStore;
use object_store::PutPayload;
use object_store::PutResult;
use observability_deps::tracing::{debug, error};
use parquet::arrow::arrow_writer::ArrowWriterOptions;
use parquet::arrow::async_writer::AsyncFileWriter;
use parquet::arrow::AsyncArrowWriter;
use parquet::errors::ParquetError;
use parquet::file::properties::WriterProperties;
use parquet_file::storage::ParquetExecInput;
use schema::sort::SortKey;
use schema::Schema;
use std::collections::HashMap;
use std::fmt::Debug;
use std::future::Future;
use std::mem;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;

pub mod catalog;
pub mod compacted_data;
pub mod consumer;
pub mod planner;
pub mod producer;
pub mod sys_events;

#[derive(Debug, thiserror::Error)]
pub enum CompactorError {
    #[error("The db to be compacted does not exist")]
    MissingDB,
    #[error("The table to be compacted does not exist and no schema can be found")]
    MissingSchema,
    #[error("No files were compacted when executing the compactor")]
    NoCompactedFiles,
    #[error("Failed to put data into obj store: {0}")]
    FailedPut(object_store::Error),
    #[error("Failed metadata request to object store: {0}")]
    FailedHead(object_store::Error),
    #[error("Failed get to obj store: {0}")]
    FailedGet(object_store::Error),
    #[error("Failed to create a new AsyncArrowWriter: {0}")]
    NewArrowWriter(ParquetError),
    #[error("Failed to write into an AsyncArrowWriter: {0}")]
    WriteArrowWriter(ParquetError),
    #[error("Failed to close an AsyncArrowWriter: {0}")]
    CloseArrowWriter(ParquetError),
    #[error("Failed to flush an AsyncArrowWriter: {0}")]
    FlushArrowWriter(ParquetError),
    #[error("Failed to ensure schema: {0}")]
    EnsureSchema(ArrowError),
    #[error("Failed to produce a RecordBatch stream: {0}")]
    RecordStream(DataFusionError),
    #[error("Failed to produce a RecordBatch: {0}")]
    FailedRecordStream(DataFusionError),
    #[error("'{0}' was not a column name in this table")]
    MissingColumnName(Arc<str>),
    #[error("'{0}' was not a column id in this table")]
    MissingColumnId(ColumnId),
    #[error("Failed to partition RecordBatch: {0}")]
    FailedPartition(ArrowError),
    #[error("Failed to create a reorg plan: {0}")]
    ReorgError(reorg::Error),
    #[error("Failed to execute a LogicalPlan: {0}")]
    FailedLogicalPlan(DataFusionError),
    #[error("Failed to cast an array while indexing data: {0}")]
    CastError(ArrowError),
}

#[derive(Debug)]
pub struct CompactorOutput {
    pub output_paths: Vec<ObjPath>,
    pub file_index: FileIndex,
    pub file_metadata: Vec<ParquetFile>,
}

#[derive(Debug)]
pub struct CompactFilesArgs {
    pub compactor_id: Arc<str>,
    pub table_def: Arc<TableDefinition>,
    pub paths: Vec<ObjPath>,
    pub limit: usize,
    pub generation: Generation,
    pub index_columns: Vec<ColumnId>,
    pub object_store: Arc<dyn ObjectStore>,
    pub object_store_url: ObjectStoreUrl,
    pub exec: Arc<Executor>,
    pub parquet_cache_prefetcher: Option<Arc<ParquetCachePreFetcher>>,
    pub datafusion_config: Arc<HashMap<String, String>>,
}

/// Compact `paths` together into one or more parquet files
///
/// The limit is the maximum number of rows that can be in a single file.
/// This number can be exceeded if a single series is larger than the limit
pub async fn compact_files(
    CompactFilesArgs {
        compactor_id,
        table_def,
        paths,
        limit,
        generation,
        index_columns,
        object_store,
        object_store_url,
        exec,
        parquet_cache_prefetcher,
        datafusion_config,
    }: CompactFilesArgs,
) -> Result<CompactorOutput, CompactorError> {
    let dedupe_key: Vec<_> = table_def.schema.primary_key();
    let dedupe_key = SortKey::from_columns(dedupe_key);

    let records = record_stream(
        Arc::clone(&table_def),
        dedupe_key,
        paths,
        Arc::clone(&object_store),
        object_store_url,
        exec,
        datafusion_config,
    )
    .await?;

    let mut series_writer = SeriesWriter::new(
        table_def,
        object_store,
        limit,
        records,
        compactor_id,
        generation,
        index_columns,
        parquet_cache_prefetcher.clone(),
    );

    loop {
        // If there is nothing left exit the loop
        let Some(batch) = series_writer.next_batch().await? else {
            break;
        };
        series_writer.push_batch(batch).await?;
    }

    series_writer.finish().await
}

/// Get a stream of `RecordBatch` formed from a set of input paths that get merged and deduplicated
/// into a single stream.
async fn record_stream(
    table_def: Arc<TableDefinition>,
    sort_key: SortKey,
    paths: Vec<ObjPath>,
    object_store: Arc<dyn ObjectStore>,
    object_store_url: ObjectStoreUrl,
    exec: Arc<Executor>,
    datafusion_config: Arc<HashMap<String, String>>,
) -> Result<SendableRecordBatchStream, CompactorError> {
    // sort, but to dedupe data. We use the same PartitionKey for every file to accomplish this.
    // This is a concept for IOx and the query planner can be clever about deduping data if data
    // is in separate partitions. Mainly that it won't due to assuming that different partitions
    // will have non overlapping data.
    let partition_id = data_types::TransitionPartitionId::Deterministic(PartitionHashId::new(
        TableId::new(0),
        &PartitionKey::from("synthetic-key"),
    ));

    let mut chunks = Vec::new();
    for (id, location) in paths.iter().enumerate() {
        let object_meta = object_store
            .head(location)
            .await
            .map_err(CompactorError::FailedHead)?;
        let parquet_exec = ParquetExecInput {
            object_store_url: object_store_url.clone(),
            object_meta,
            object_store: Arc::clone(&object_store),
        };

        let chunk_stats = create_chunk_statistics(None, &table_def.schema, None, &NoColumnRanges);

        let parquet_chunk: Arc<dyn QueryChunk> = Arc::new(ParquetChunk {
            partition_id: partition_id.clone(),
            schema: table_def.schema.clone(),
            stats: Arc::new(chunk_stats),
            sort_key: Some(sort_key.clone()),
            id: ChunkId::new_id(id as u128),
            chunk_order: ChunkOrder::new(id as i64),
            parquet_exec,
        });

        chunks.push(parquet_chunk);
    }

    // Create the plan and execute it over all of the ParquetChunks
    let reorg = ReorgPlanner::new();
    let plan = reorg
        .compact_plan(
            TableId::new(0),
            Arc::clone(&table_def.table_name),
            &table_def.schema,
            chunks,
            sort_key,
        )
        .map_err(CompactorError::ReorgError)?;
    let ctx = {
        let mut cfg = exec.new_session_config();
        for (k, v) in datafusion_config.iter() {
            cfg = cfg.with_config_option(k, v);
        }
        cfg.build()
    };
    ctx.inner()
        .execute_logical_plan(plan)
        .await
        .map_err(CompactorError::FailedLogicalPlan)?
        .execute_stream()
        .await
        .map_err(CompactorError::RecordStream)
}

/// Handles writing RecordBatches to a file in the object store
/// ensuring each series is written to the same file
struct SeriesWriter {
    table_def: Arc<TableDefinition>,
    object_store: Arc<dyn ObjectStore>,
    /// the target size of each output file
    limit: usize,
    /// The number of rows written to the current file
    current_row_count: usize,
    /// Rows from the previous record batch that were in the last series, if any
    last_batch: Option<RecordBatch>,
    /// The currently open writer
    writer: Option<AsyncArrowWriter<AsyncMultiPart>>,
    /// Used to get the next `RecordBatch` to process
    record_batches: RecordBatchHolder,
    /// Series key (e.g. tags) to separate data out by
    series_key: Vec<Arc<str>>,
    /// Files we have created
    output_paths: Vec<ObjPath>,
    /// Metadata for the files we have created
    file_metadata: Vec<ParquetFile>,
    /// The ID of the compactor
    compactor_id: Arc<str>,
    /// What generation the data is being compacted into
    generation: Generation,
    /// The current file id to use for this file we are compacting into
    current_file_id: ParquetFileId,
    /// What columns we are indexing on for the `FileIndex`
    index_columns: Vec<ColumnId>,
    /// The `FileIndex` for this table that we are compacting
    file_index: FileIndex,
    /// Min time for the current file
    min_time: i64,
    /// Max time for the current file
    max_time: i64,
    /// Parquet cache oracle to prefetch parquet file into cache
    parquet_cache_prefetcher: Option<Arc<ParquetCachePreFetcher>>,
}

impl SeriesWriter {
    /// Create a new `SeriesWriter` which maintains all of the state for writing out series for
    /// compactions
    #[allow(clippy::too_many_arguments)]
    fn new(
        table_def: Arc<TableDefinition>,
        object_store: Arc<dyn ObjectStore>,
        limit: usize,
        stream: SendableRecordBatchStream,
        compactor_id: Arc<str>,
        generation: Generation,
        index_columns: Vec<ColumnId>,
        parquet_cache_prefetcher: Option<Arc<ParquetCachePreFetcher>>,
    ) -> Self {
        // TODO: this should come directly from a method on the table definition, and it is being
        // somewhat confused with the "series key" from the v3 line protocol.
        let series_key = table_def
            .schema
            .primary_key()
            .iter()
            .filter(|f| **f != TIME_COLUMN_NAME)
            .map(|f| Arc::from(*f))
            .collect::<Vec<_>>();

        Self {
            record_batches: RecordBatchHolder::new(stream, Arc::new(table_def.schema.clone())),
            table_def,
            object_store,
            limit,
            current_row_count: 0,
            last_batch: None,
            writer: None,
            output_paths: Vec::new(),
            file_metadata: Vec::new(),
            compactor_id,
            generation,
            current_file_id: ParquetFileId::new(),
            index_columns: index_columns.into_iter().map(Into::into).collect(),
            file_index: FileIndex::new(),
            min_time: i64::MAX,
            max_time: i64::MIN,
            series_key,
            parquet_cache_prefetcher,
        }
    }

    /// Close out any leftover writers and return the paths
    async fn finish(mut self) -> Result<CompactorOutput, CompactorError> {
        // close the remaining writer, if any
        if let Some(writer) = self.writer.take() {
            self.close(writer).await?;
        }

        // This should never happen, but if it is we should throw an error
        if self.output_paths.is_empty() {
            Err(CompactorError::NoCompactedFiles)
        } else {
            // prefetch compacted files into cache
            if let Some(pre_fetcher) = self.parquet_cache_prefetcher {
                pre_fetcher.prefetch_all(&self.file_metadata).await;
            }

            Ok(CompactorOutput {
                output_paths: self.output_paths,
                file_index: self.file_index,
                file_metadata: self.file_metadata,
            })
        }
    }

    /// Push a new batch of data into the writer
    async fn push_batch(&mut self, batch: RecordBatch) -> Result<(), CompactorError> {
        let mut writer = self.get_writer().await?;
        let time = batch
            .column_by_name(TIME_COLUMN_NAME)
            .expect("RecordBatch has a time column");
        let min =
            arrow::compute::min(time.as_primitive::<TimestampNanosecondType>()).unwrap_or(i64::MAX);
        let max =
            arrow::compute::max(time.as_primitive::<TimestampNanosecondType>()).unwrap_or(i64::MIN);
        if self.min_time > min {
            self.min_time = min;
        }
        if self.max_time < max {
            self.max_time = max
        }

        // if there is space remaining within the limit for this writer, write the whole batch.
        if self.current_row_count + batch.num_rows() <= self.limit {
            writer
                .write(&batch)
                .await
                .map_err(CompactorError::WriteArrowWriter)?;
            self.index_and_update_last_batch(batch)?;
            self.writer = Some(writer);
            return Ok(());
        }

        // Check if this current batch starts with the same series the last batch ended with
        let mut same_series = true;
        if let Some(last_batch) = self.last_batch.take() {
            let last_batch_slice = last_batch.slice(last_batch.num_rows() - 1, 1);
            let batch_slice = batch.slice(0, 1);
            for key in self.series_key.iter() {
                same_series = same_series
                    && batch_slice
                        .column_by_name(key)
                        .eq(&last_batch_slice.column_by_name(key));
                if !same_series {
                    break;
                }
            }
        }

        // Find where the series are
        let partitions = self.partition_by_series(&batch)?;

        let ranges = partitions.ranges();
        let partition = &ranges[0];
        let len = partition.end - partition.start;

        match partitions.len() {
            // If there is only one partition and we're the same series write out the whole
            // batch. We might still have more records for this series and we will close it
            // later.
            1 if same_series => {
                writer
                    .write(&batch)
                    .await
                    .map_err(CompactorError::WriteArrowWriter)?;
                self.index_and_update_last_batch(batch)?;
                self.writer = Some(writer);
                return Ok(());
            }
            // If there's only one partition, but it's not the same series, then we check
            // to see if we're over or at the limit. If we are close it, if not then in
            // the next iteration of the loop we'll write out the batch into the writer.
            1 if !same_series => {
                self.set_leftover_batch(batch);
                if self.current_row_count >= self.limit {
                    self.close(writer).await?;
                    return Ok(());
                }
                self.last_batch = None;
                self.writer = Some(writer);
            }
            // If there are more than one partitions then we need to write out the slice
            // and close the writer if we go over the limit
            _ => {
                let next_partition = &ranges[1];
                let next_len = next_partition.end - next_partition.start;
                let slice = batch.slice(0, len);
                writer
                    .write(&slice)
                    .await
                    .map_err(CompactorError::WriteArrowWriter)?;

                self.index_and_update_last_batch(slice)?;

                let batch_slice = batch.slice(len, batch.num_rows() - len);
                if batch_slice.num_rows() > 0 {
                    self.set_leftover_batch(batch_slice);
                }

                if self.current_row_count + next_len > self.limit {
                    self.close(writer).await?;
                    return Ok(());
                }

                self.writer = Some(writer);
            }
        }

        Ok(())
    }

    /// Helper for updating the index, last record batch, and incrementing the current row count
    fn index_and_update_last_batch(&mut self, batch: RecordBatch) -> Result<(), CompactorError> {
        self.index_record_batch(&batch)?;
        self.current_row_count += batch.num_rows();
        self.last_batch = Some(batch);
        Ok(())
    }

    /// Get the next batch to push if it exists
    async fn next_batch(&mut self) -> Result<Option<RecordBatch>, CompactorError> {
        self.record_batches.next().await
    }

    /// Get a writer for the next file, if there is no writer available create a new one and
    /// push a new path into our outputs that will contain all of the written data eventually
    async fn get_writer(&mut self) -> Result<AsyncArrowWriter<AsyncMultiPart>, CompactorError> {
        match self.writer.take() {
            Some(writer) => Ok(writer),
            None => {
                self.current_file_id = ParquetFileId::new();
                let path = CompactedFilePath::new(
                    &self.compactor_id,
                    self.generation.id,
                    self.current_file_id,
                );
                let obj_store_multipart = self
                    .object_store
                    .put_multipart(path.as_object_store_path())
                    .await
                    .map_err(CompactorError::FailedPut)?;
                let options = ArrowWriterOptions::new().with_properties(
                    WriterProperties::builder()
                        .set_compression(parquet::basic::Compression::ZSTD(Default::default()))
                        .set_max_row_group_size(ROW_GROUP_WRITE_SIZE)
                        .build(),
                );
                let writer = AsyncArrowWriter::try_new_with_options(
                    AsyncMultiPart::new(
                        path.as_object_store_path(),
                        obj_store_multipart,
                        self.parquet_cache_prefetcher.clone(),
                    ),
                    self.record_batches.schema.as_arrow(),
                    options,
                )
                .map_err(CompactorError::NewArrowWriter)?;
                self.output_paths.push(path.as_object_store_path().clone());
                self.min_time = i64::MAX;
                self.max_time = i64::MIN;
                Ok(writer)
            }
        }
    }

    /// Find where the series are in the record batch
    fn partition_by_series(&self, batch: &RecordBatch) -> Result<Partitions, CompactorError> {
        // We want to partition by the series key columns in the batch
        let columns = self
            .series_key
            .iter()
            .map(|name| {
                Ok(Arc::clone(batch.column_by_name(name).ok_or_else(|| {
                    CompactorError::MissingColumnName(Arc::clone(name))
                })?))
            })
            .collect::<Result<Vec<_>, _>>()?;

        partition(&columns).map_err(CompactorError::FailedPartition)
    }

    /// Sets the leftover record batch in the `RecordBatchHolder`
    fn set_leftover_batch(&mut self, batch: RecordBatch) {
        self.record_batches.leftover_batch = Some(batch)
    }

    /// Index a record batch for the current output file
    fn index_record_batch(&mut self, batch: &RecordBatch) -> Result<(), CompactorError> {
        for column_id in self.index_columns.iter() {
            let column_name = self
                .table_def
                .column_id_to_name(column_id)
                .ok_or_else(|| CompactorError::MissingColumnId(*column_id))?;
            let array = batch.column_by_name(&column_name).unwrap();
            // If the cast fails use null for the value. We might lose out on the indexing,
            // but this way we can handle most things
            let casted = cast_with_options(
                &array,
                &DataType::LargeUtf8,
                &CastOptions {
                    safe: true,
                    format_options: FormatOptions::new().with_null("null"),
                },
            )
            .map_err(CompactorError::CastError)?;
            let downcasted = as_largestring_array(&casted);
            for value in downcasted.iter().map(|s| s.unwrap_or("null")) {
                self.file_index
                    .insert(column_name.as_ref(), value, &self.current_file_id);
            }
        }
        Ok(())
    }

    /// Flush and close the writer and reset the current row count
    async fn close(
        &mut self,
        mut writer: AsyncArrowWriter<AsyncMultiPart>,
    ) -> Result<(), CompactorError> {
        writer
            .flush()
            .await
            .map_err(CompactorError::FlushArrowWriter)?;

        let metadata = writer
            .close()
            .await
            .map_err(CompactorError::CloseArrowWriter)?;

        // we need the size of the file written into object storage and that doesn't seem to
        // be available from the writer. The bytes_written method on the writer returns a number
        // smaller than the actual file size. So we'll get the metadata from object storage to
        // see what we wrote.
        let size_bytes = self
            .object_store
            .head(self.output_paths.last().unwrap())
            .await
            .map_err(CompactorError::FailedGet)?
            .size;

        self.file_metadata.push(ParquetFile {
            id: self.current_file_id,
            path: self.output_paths.last().unwrap().to_string(),
            size_bytes: size_bytes as u64,
            // i64 to u64 cast
            row_count: metadata.num_rows as u64,
            chunk_time: self.min_time,
            min_time: self.min_time,
            max_time: self.max_time,
        });

        self.current_row_count = 0;

        Ok(())
    }
}

/// Maintains the state for what is the next batch to use
pub struct RecordBatchHolder {
    /// The stream of sorted record batches from all of the parquet files to iterate over
    stream: SendableRecordBatchStream,
    /// If we partition a batch store the leftover batch here
    leftover_batch: Option<RecordBatch>,
    schema: Arc<Schema>,
}

impl RecordBatchHolder {
    /// Create a new `RecordBatchHolder`
    fn new(stream: SendableRecordBatchStream, schema: Arc<Schema>) -> Self {
        RecordBatchHolder {
            stream,
            schema,
            leftover_batch: None,
        }
    }

    /// Get the next batch. We prioritize the leftover batch as this was what's left
    /// from splitting the prior `RecordBatch`. If there is nothing leftover we attempt
    /// to grab the next batch from the stream if there is one. We also make sure that
    /// the record that we read from the stream matches the schema via `ensure_schema`
    async fn next(&mut self) -> Result<Option<RecordBatch>, CompactorError> {
        match self.leftover_batch.take() {
            Some(record) => Ok(Some(record)),
            None => match self.stream.next().await {
                Some(Ok(record)) => ensure_schema(&self.schema.as_arrow(), &record)
                    .map(Some)
                    .map_err(CompactorError::EnsureSchema),
                Some(Err(err)) => Err(CompactorError::FailedRecordStream(err)),
                None => Ok(None),
            },
        }
    }
}

impl Debug for RecordBatchHolder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RecordBatchHolder")
            .field(
                "stream",
                &"Peekable<SendableRecordBatchStream>".to_string() as &dyn Debug,
            )
            .field("leftover_batch", &self.leftover_batch)
            .field("schema", &self.schema)
            .finish()
    }
}

struct AsyncMultiPart {
    path: Arc<ObjPath>,
    multi_part: Box<dyn MultipartUpload>,
    bytes_buffer: Vec<u8>,
    // every time bytes_buffer is flushed we add them to
    // overall_buffer and finally when complete this is
    // cleared.
    overall_buffer: Vec<Bytes>,
    cache_prefetcher: Option<Arc<ParquetCachePreFetcher>>,
}

impl AsyncMultiPart {
    fn new(
        path: &ObjPath,
        multi_part: Box<dyn MultipartUpload>,
        parquet_cache_prefetcher: Option<Arc<ParquetCachePreFetcher>>,
    ) -> Self {
        Self {
            path: Arc::from(path.clone()),
            multi_part,
            bytes_buffer: Vec::new(),
            overall_buffer: Vec::new(),
            cache_prefetcher: parquet_cache_prefetcher,
        }
    }
}

struct AsyncMultiPartWrite {
    payload: Pin<Box<dyn Future<Output = Result<(), object_store::Error>> + Send + 'static>>,
}

impl Future for AsyncMultiPartWrite {
    type Output = parquet::errors::Result<()>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match self.payload.as_mut().poll(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Ok(())) => Poll::Ready(Ok(())),
            Poll::Ready(Err(err)) => {
                Poll::Ready(Err(parquet::errors::ParquetError::General(err.to_string())))
            }
        }
    }
}

struct AsyncMultiPartComplete<'complete> {
    path: Arc<ObjPath>,
    overall_buffer: Vec<Bytes>,
    cache_prefetcher: Option<Arc<ParquetCachePreFetcher>>,
    complete:
        Pin<Box<dyn Future<Output = Result<PutResult, object_store::Error>> + Send + 'complete>>,
}

impl Future for AsyncMultiPartComplete<'_> {
    type Output = parquet::errors::Result<()>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match self.complete.as_mut().poll(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Ok(res)) => {
                if let Some(prefetcher) = &self.cache_prefetcher {
                    let overall_bytes = self
                        .overall_buffer
                        .iter()
                        .fold(BytesMut::new(), |mut acc, bytes| {
                            acc.extend_from_slice(bytes);
                            acc
                        })
                        .freeze();
                    debug!(path = ?self.path, ">> caching path");
                    prefetcher.add_to_cache(Arc::clone(&self.path), overall_bytes, res);
                }
                Poll::Ready(Ok(()))
            }
            Poll::Ready(Err(err)) => {
                Poll::Ready(Err(parquet::errors::ParquetError::General(err.to_string())))
            }
        }
    }
}

/// s3 and other Object Stores require an 5mb minimum for all parts and in some cases like with R2 each
/// part must be 5mb. We need to handle that use case
const MULTIPART_UPLOAD_MINIMUM: usize = 5242880; // 5mb as bytes

impl AsyncFileWriter for AsyncMultiPart {
    fn write(&mut self, bs: Bytes) -> BoxFuture<'_, parquet::errors::Result<()>> {
        self.bytes_buffer.extend_from_slice(bs.as_ref());
        if self.bytes_buffer.len() < MULTIPART_UPLOAD_MINIMUM {
            Box::pin(async { Ok(()) })
        } else {
            // this buffer is what we need
            let buffer = self
                .bytes_buffer
                .drain(0..MULTIPART_UPLOAD_MINIMUM)
                .collect::<Vec<u8>>();
            let bytes = Bytes::from(buffer);
            if self.cache_prefetcher.is_some() {
                self.overall_buffer.push(bytes.clone());
            }
            Box::pin(AsyncMultiPartWrite {
                payload: self.multi_part.put_part(PutPayload::from_bytes(bytes)),
            })
        }
    }

    fn complete(&mut self) -> BoxFuture<'_, parquet::errors::Result<()>> {
        Box::pin(async {
            if !self.bytes_buffer.is_empty() {
                // this buffer is what we need
                let bytes = Bytes::from(mem::take(&mut self.bytes_buffer));
                if self.cache_prefetcher.is_some() {
                    self.overall_buffer.push(bytes.clone());
                }

                Box::pin(AsyncMultiPartWrite {
                    payload: self
                        .multi_part
                        // Since we're completing the upload grab the rest of the bytes and write them out
                        // to object store before completing the upload
                        .put_part(PutPayload::from_bytes(bytes)),
                })
                .await?;
            }

            // Now that we have written out any residual data we can complete the upload
            AsyncMultiPartComplete {
                path: Arc::clone(&self.path),
                overall_buffer: self.overall_buffer.drain(..).collect(),
                complete: self.multi_part.complete(),
                cache_prefetcher: self.cache_prefetcher.clone(),
            }
            .await
        })
    }
}

#[cfg(test)]
mod test_helpers {
    use bytes::Bytes;
    use datafusion_util::config::register_iox_object_store;
    use executor::{register_current_runtime_for_io, DedicatedExecutor};
    use influxdb3_cache::distinct_cache::DistinctCacheProvider;
    use influxdb3_cache::{
        last_cache::LastCacheProvider, parquet_cache::test_cached_obj_store_and_oracle,
    };
    use influxdb3_catalog::catalog::Catalog;
    use influxdb3_enterprise_parquet_cache::ParquetCachePreFetcher;
    use influxdb3_test_helpers::object_store::RequestCountedObjectStore;
    use influxdb3_wal::{
        Gen1Duration, SnapshotDetails, SnapshotSequenceNumber, WalContents, WalFileNotifier,
        WalFileSequenceNumber, WalOp, WriteBatch,
    };
    use influxdb3_write::persister::Persister;
    use influxdb3_write::write_buffer::persisted_files::PersistedFiles;
    use influxdb3_write::write_buffer::queryable_buffer::{QueryableBuffer, QueryableBufferArgs};
    use influxdb3_write::write_buffer::validator::WriteValidator;
    use influxdb3_write::{ParquetFile, PersistedSnapshot, Precision};
    use iox_query::exec::{Executor, ExecutorConfig};
    use iox_time::{MockProvider, SystemProvider, Time, TimeProvider};
    use object_store::{memory::InMemory, path::Path as ObjPath, ObjectStore};
    use parquet::arrow::async_writer::AsyncFileWriter;
    use parquet_file::storage::{ParquetStorage, StorageId};
    use pretty_assertions::assert_eq;
    use std::sync::Arc;
    use std::{num::NonZeroUsize, str::FromStr};

    use crate::AsyncMultiPart;

    pub(crate) struct TestWriter {
        pub(crate) exec: Arc<Executor>,
        pub(crate) catalog: Arc<Catalog>,
        pub(crate) persister: Arc<Persister>,
        pub(crate) persisted_files: Arc<PersistedFiles>,
        pub(crate) wal_file_sequence_number: WalFileSequenceNumber,
        pub(crate) snapshot_sequence_number: SnapshotSequenceNumber,
        pub(crate) time_provider: Arc<dyn TimeProvider>,
    }

    impl TestWriter {
        pub(crate) fn new(node_id: &str, object_store: Arc<dyn ObjectStore>) -> Self {
            let metrics = Arc::new(metric::Registry::default());

            let parquet_store =
                ParquetStorage::new(Arc::clone(&object_store), StorageId::from("influxdb3"));
            let exec = Arc::new(Executor::new_with_config_and_executor(
                ExecutorConfig {
                    target_query_partitions: NonZeroUsize::new(1).unwrap(),
                    object_stores: [&parquet_store]
                        .into_iter()
                        .map(|store| (store.id(), Arc::clone(store.object_store())))
                        .collect(),
                    metric_registry: Arc::clone(&metrics),
                    // Default to 1gb
                    mem_pool_size: 1024 * 1024 * 1024, // 1024 (b/kb) * 1024 (kb/mb) * 1024 (mb/gb)
                },
                DedicatedExecutor::new_testing(),
            ));
            let runtime_env = exec.new_context().inner().runtime_env();
            register_iox_object_store(runtime_env, parquet_store.id(), Arc::clone(&object_store));
            register_current_runtime_for_io();

            let catalog = Arc::new(Catalog::new(node_id.into(), "foo".into()));
            let time_provider: Arc<dyn TimeProvider> =
                Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));
            let persister = Arc::new(Persister::new(
                Arc::clone(&object_store),
                node_id,
                Arc::clone(&time_provider),
            ));

            Self {
                exec,
                catalog,
                persister,
                persisted_files: Arc::new(PersistedFiles::new_from_persisted_snapshots(vec![])),
                wal_file_sequence_number: WalFileSequenceNumber::new(0),
                snapshot_sequence_number: SnapshotSequenceNumber::new(0),
                time_provider,
            }
        }

        pub(crate) fn get_files(&self, table_name: &str) -> Vec<ParquetFile> {
            let db = self.catalog.db_schema("testdb").unwrap();
            let table = db.table_definition(table_name).unwrap();
            self.persisted_files.get_files(db.id, table.table_id)
        }

        pub(crate) async fn persist_lp_and_snapshot(
            &mut self,
            lp: &str,
            default_time: i64,
        ) -> PersistedSnapshot {
            let db = data_types::NamespaceName::new("testdb").unwrap();
            let val =
                WriteValidator::initialize(db, Arc::clone(&self.catalog), default_time).unwrap();
            let lines = val
                .parse_lines_and_update_schema(
                    lp,
                    false,
                    self.time_provider.now(),
                    Precision::Nanosecond,
                )
                .unwrap()
                .convert_lines_to_buffer(Gen1Duration::new_1m());
            let batch: WriteBatch = lines.into();
            let wal_contents = WalContents {
                persist_timestamp_ms: 0,
                min_timestamp_ns: batch.min_time_ns,
                max_timestamp_ns: batch.max_time_ns,
                wal_file_number: self.wal_file_sequence_number.next(),
                ops: vec![WalOp::Write(batch)],
                snapshot: None,
            };
            let end_time = wal_contents.max_timestamp_ns
                + Gen1Duration::new_1m().as_duration().as_nanos() as i64;

            let queryable_buffer_args = QueryableBufferArgs {
                executor: Arc::clone(&self.exec),
                catalog: Arc::clone(&self.catalog),
                persister: Arc::clone(&self.persister),
                last_cache_provider: LastCacheProvider::new_from_catalog(Arc::clone(&self.catalog))
                    .unwrap(),
                distinct_cache_provider: DistinctCacheProvider::new_from_catalog(
                    Arc::clone(&self.time_provider),
                    Arc::clone(&self.catalog),
                )
                .unwrap(),
                persisted_files: Arc::new(PersistedFiles::new_from_persisted_snapshots(vec![])),
                parquet_cache: None,
            };
            let queryable_buffer = QueryableBuffer::new(queryable_buffer_args);

            // write the lp into the buffer
            queryable_buffer.notify(Arc::new(wal_contents)).await;
            self.snapshot_sequence_number = self.snapshot_sequence_number.next();
            let snapshot_details = SnapshotDetails {
                snapshot_sequence_number: self.snapshot_sequence_number,
                end_time_marker: end_time,
                last_wal_sequence_number: self.wal_file_sequence_number,
                first_wal_sequence_number: self.wal_file_sequence_number,
                forced: false,
            };

            // now force a snapshot, persisting the data to parquet files and writing a persisted snapshot file
            let details = queryable_buffer
                .notify_and_snapshot(
                    Arc::new(WalContents {
                        persist_timestamp_ms: 0,
                        min_timestamp_ns: 0,
                        max_timestamp_ns: 0,
                        wal_file_number: self.wal_file_sequence_number,
                        ops: vec![],
                        snapshot: Some(snapshot_details),
                    }),
                    snapshot_details,
                )
                .await;
            let details = details.await.unwrap();

            let persisted_snapshot = self
                .persister
                .load_snapshots(1)
                .await
                .unwrap()
                .pop()
                .unwrap();
            assert_eq!(
                persisted_snapshot.snapshot_sequence_number,
                details.snapshot_sequence_number
            );
            self.persisted_files
                .add_persisted_snapshot_files(persisted_snapshot.clone());

            persisted_snapshot
        }
    }

    #[test_log::test(tokio::test)]
    async fn test_async_multi_part_with_caching() {
        let obj_store = Arc::new(RequestCountedObjectStore::new(Arc::new(InMemory::new())));
        let path = ObjPath::from("/foo.txt");

        let multipart_upload = obj_store.put_multipart(&path).await.unwrap();
        let (prefetcher, cache) = build_parquet_cache_prefetcher(&obj_store);
        let mut multi_part_writer =
            AsyncMultiPart::new(&ObjPath::from("/foo.txt"), multipart_upload, prefetcher);

        multi_part_writer
            .write(Bytes::from_static(b"hello"))
            .await
            .unwrap();
        multi_part_writer
            .write(Bytes::from_static(b"world"))
            .await
            .unwrap();
        multi_part_writer.complete().await.unwrap();

        let res = cache.get(&path).await.unwrap().bytes().await.unwrap();
        assert_eq!(Bytes::from_static(b"helloworld"), res);
        assert_eq!(0, obj_store.total_read_request_count(&path));
    }

    fn build_parquet_cache_prefetcher(
        obj_store: &Arc<RequestCountedObjectStore>,
    ) -> (Option<Arc<ParquetCachePreFetcher>>, Arc<dyn ObjectStore>) {
        let time_provider: Arc<dyn TimeProvider> = Arc::new(SystemProvider::new());
        let (mem_store, parquet_cache) = test_cached_obj_store_and_oracle(
            Arc::clone(obj_store) as _,
            Arc::clone(&time_provider),
            Default::default(),
        );
        let mock_time_provider = Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));

        (
            Some(Arc::new(ParquetCachePreFetcher::new(
                parquet_cache,
                humantime::Duration::from_str("1d").unwrap(),
                mock_time_provider,
            ))),
            mem_store,
        )
    }
}
