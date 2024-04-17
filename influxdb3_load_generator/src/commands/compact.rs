use std::collections::HashMap;
use std::io::Write;
use std::num::NonZeroUsize;
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Context;
use arrow_array::builder::{
    BooleanBuilder, FixedSizeBinaryBuilder, StringBuilder, TimestampNanosecondBuilder,
};
use arrow_array::{ArrayRef, RecordBatch, StructArray};
use arrow_schema::{DataType, Field, Schema as ArrowSchema, SchemaRef, TimeUnit::Nanosecond};
use chrono::{DateTime, Utc};
use clap::Parser;
use clap_blocks::object_store::{make_object_store, ObjectStoreConfig};
use data_types::{ChunkId, ChunkOrder, PartitionKey, TableId, TransitionPartitionId};
use datafusion::execution::memory_pool::{MemoryPool, UnboundedMemoryPool};
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion_util::config::register_iox_object_store;
use influxdb3_process::setup_metric_registry;
use influxdb3_write::chunk::ParquetChunk;
use influxdb3_write::{ParquetFile, DEFAULT_OBJECT_STORE_URL};
use iox_query::chunk_statistics::create_chunk_statistics;
use iox_query::exec::{Executor, IOxSessionContext};
use iox_query::frontend::reorg::ReorgPlanner;
use iox_query::QueryChunk;
use itertools::Itertools;
use object_store::path::Path as ObjStorePath;
use object_store::{ObjectMeta, ObjectStore};
use parquet::file::properties::WriterProperties;
use parquet::schema::types::ColumnPath;
use parquet_file::serialize::ROW_GROUP_WRITE_SIZE;
use parquet_file::storage::{ParquetExecInput, ParquetStorage, StorageId};
use parquet_file::writer::TrackedMemoryArrowWriter;
use rand::rngs::SmallRng;
use rand::seq::{IteratorRandom, SliceRandom};
use rand::SeedableRng;
use schema::sort::SortKeyBuilder;
use schema::{Schema, SERIES_ID_COLUMN_NAME, TIME_COLUMN_NAME};
use sha2::Digest;
use sha2::Sha256;

use super::common::SamplingInterval;

#[derive(Debug, Parser, Clone)]
pub struct Config {
    #[clap(flatten)]
    object_store: ObjectStoreConfig,
    /// The number of input files (N) that will be generated to perform the compaction.
    #[clap(short = 'N', long = "num-input-files", default_value_t = 1)]
    num_input_files: usize,

    /// The number of rows per generated input file.
    #[clap(long = "row-count", default_value_t = 1_000_000)]
    row_count: usize,

    /// The number of tags in the generated data set.
    #[clap(short = 'T', long = "num-tags", default_value_t = 1)]
    num_tags: usize,

    /// The number of fields in the generated data set. These will be boolean fields, to
    /// keep the size of generated data to a minimum.
    #[clap(short = 'F', long = "num-fields", default_value_t = 1)]
    num_fields: usize,

    /// The maximum cardinality of the generated data.
    ///
    /// This will be the cardinality of the highest-cardinality tag.
    #[clap(short = 'c', default_value_t = 1_000)]
    cardinality: u32,

    // /// The number of output files (M) to compact to.
    // ///
    // /// Defaults to the same as `num-input-files`
    // #[clap(short = 'M', long = "num-output-files")]
    // num_output_files: Option<usize>,
    /// Generate a `_series_id` columnfor each row, and use it to perform sort/dedupe.
    #[clap(long = "series-id", default_value_t = false)]
    series_id: bool,

    /// Use dictionary tags; the alternative is string tags
    ///
    /// Currently, this is not suported.
    #[clap(long = "use-dict-tags", default_value_t = false)]
    use_dict_tags: bool,

    /// The duplication factor in generated parquet data.
    ///
    /// A duplication factor of 1 means that every row in generated data has a duplicate.
    #[clap(long = "duplication-factor", default_value_t = 1)]
    duplication_factor: usize,

    /// The seed for the random number generator. Default is 0.
    #[clap(long = "seed", default_value_t = 0)]
    rng_seed: u64,

    /// The timestamp to use as the starting point for generated row data
    ///
    /// Defaults to now.
    #[clap(long = "start-time")]
    start_time: Option<DateTime<Utc>>,

    /// The sampling interval that determines the duration between timestamps in generated
    /// row data.
    #[clap(short = 'i', long = "sampling-interval", default_value = "1s")]
    sampling_interval: SamplingInterval,

    /// Do not write anything to disk, but print out info about files that would be written
    #[clap(short = 'n', long = "dry-run", default_value_t = false)]
    dry_run: bool,

    /// The number of threads to run the executor on.
    #[clap(long = "num-threads", default_value = "1")]
    num_threads: NumThreads,

    /// The size of the memory pool made available to the executor in bytes (B).
    #[clap(long = "mem-pool-size", default_value_t = 8_589_934_592)]
    mem_pool_size: usize,

    /// Save the compacted parquet data into a new set of files
    #[clap(long = "inspect", default_value_t = false)]
    inspect_compacted: bool,
}

#[derive(Debug, Clone, Copy)]
struct NumThreads(NonZeroUsize);

impl FromStr for NumThreads {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let n = usize::from_str(s)?;
        Ok(Self(
            NonZeroUsize::new(n).context("num_threads must be greater than 0")?,
        ))
    }
}

fn object_store_url() -> ObjectStoreUrl {
    ObjectStoreUrl::parse(DEFAULT_OBJECT_STORE_URL).unwrap()
}

pub(crate) async fn command(config: Config) -> Result<(), anyhow::Error> {
    println!("Running Compaction Test");
    // println!("{config:#?}");
    let object_store = make_object_store(&config.object_store).expect("initialize object store");
    let parquet_store =
        ParquetStorage::new(Arc::clone(&object_store), StorageId::from("influxdb3"));
    let exec = Arc::new(Executor::new(
        "compact_test",
        config.num_threads.0,
        config.mem_pool_size,
        setup_metric_registry(),
    ));

    let ctx = exec.new_context();
    let runtime_env = ctx.inner().runtime_env();
    register_iox_object_store(runtime_env, parquet_store.id(), Arc::clone(&object_store));

    // Generate the input data
    println!("Generate duplicated and unsorted data");
    let mut generator = RowGenerator::from(&config);
    let schema = create_schema(&config);
    let iox_schema = Schema::try_from(Arc::clone(&schema)).unwrap();
    let mem_pool = Arc::new(UnboundedMemoryPool::default());
    let mut chunk_order = 0;
    let mut chunks: Vec<Arc<dyn QueryChunk>> = Vec::new();
    let mut total_rows = 0;
    for f in 0..config.num_input_files {
        let path = ObjStorePath::parse(format!("source/{f}.parquet")).expect("valid path");
        let parquet_file = generate_file(
            &mut generator,
            Arc::clone(&schema),
            &config,
            path.clone(),
            Arc::clone(&mem_pool) as _,
            Arc::clone(&object_store),
        )
        .await;
        let parquet_exec = ParquetExecInput {
            object_store_url: object_store_url(),
            object_store: Arc::clone(&object_store),
            object_meta: ObjectMeta {
                location: path,
                last_modified: Default::default(),
                size: parquet_file.size_bytes as usize,
                e_tag: None,
                version: None,
            },
        };
        let chunk_stats = create_chunk_statistics(
            Some(parquet_file.row_count as usize),
            &iox_schema,
            Some(parquet_file.timestamp_min_max()),
            None,
        );
        let partition_key = PartitionKey::from(parquet_file.path.clone());
        let partition_id = TransitionPartitionId::new(TableId::new(0), &partition_key);
        let parquet_chunk = ParquetChunk {
            schema: iox_schema.clone(),
            stats: Arc::new(chunk_stats),
            partition_id,
            sort_key: None,
            id: ChunkId::new(),
            chunk_order: ChunkOrder::new(chunk_order),
            parquet_exec,
        };
        chunk_order += 1;
        total_rows += parquet_file.row_count as usize;
        chunks.push(Arc::new(parquet_chunk));
    }

    // Create system stats reporter and start tracking stats

    // Perform the sort/dedupe operation
    let start = Instant::now();
    println!("Starting compaction...");
    let batches = compact(&ctx, &config, &iox_schema, chunks).await;
    let elapsed_ms = start.elapsed().as_millis();
    println!("Finished compaction in {elapsed_ms} ms.");
    let n: usize = batches.iter().map(|b| b.num_rows()).sum();
    println!("Compacted {total_rows} down to {n}");

    if config.inspect_compacted {
        println!("Produce compacted files...");
        let _paths =
            persist_compacted_batches(&config, schema, mem_pool, object_store, batches).await;
    }

    // Stop tracking stats, record time elapsed

    // Clean up generated data?

    Ok(())
}

async fn persist_compacted_batches(
    config: &Config,
    schema: SchemaRef,
    mem_pool: Arc<dyn MemoryPool>,
    object_store: Arc<dyn ObjectStore>,
    batches: Vec<RecordBatch>,
) -> Vec<ObjStorePath> {
    let mut paths = Vec::new();
    let batch_groups: Vec<Vec<RecordBatch>> = batches
        .into_iter()
        .batching(|it| {
            let mut num_rows = 0;
            let mut group = Vec::new();
            while num_rows < config.row_count {
                match it.next() {
                    Some(batch) => {
                        num_rows += batch.num_rows();
                        group.push(batch)
                    }
                    None => break,
                }
            }
            if group.is_empty() {
                None
            } else {
                Some(group)
            }
        })
        .collect();
    for (i, group) in batch_groups.into_iter().enumerate() {
        let mut sink = Vec::new();
        let mut writer = create_writer(
            &mut sink,
            Arc::clone(&schema),
            mem_pool.clone(),
            config.series_id,
        );
        for batch in group {
            writer.write(batch).expect("write comapcted record batch");
        }
        let meta = writer.close().expect("close parquet writer");
        let path = ObjStorePath::parse(format!("compacted/{i}.parquet")).unwrap();
        let size_bytes = sink.len();
        if !config.dry_run {
            object_store
                .put(&path, sink.into())
                .await
                .expect("put compacted file into object store");
        }
        println!(
            "compacted file: {path}, rows: {n}, size (MB): {s:.3}",
            n = meta.num_rows,
            s = size_bytes as f64 / 1024.0 / 1024.0
        );
        paths.push(path);
    }
    paths
}

async fn compact(
    ctx: &IOxSessionContext,
    config: &Config,
    schema: &Schema,
    chunks: Vec<Arc<dyn QueryChunk>>,
) -> Vec<RecordBatch> {
    let mut sort_key_builder = SortKeyBuilder::new();
    let sort_key = if config.series_id {
        sort_key_builder.with_col(SERIES_ID_COLUMN_NAME)
    } else {
        for t in 0..config.num_tags {
            sort_key_builder = sort_key_builder.with_col(format!("tag_{t}"));
        }
        sort_key_builder
    }
    .with_col(TIME_COLUMN_NAME)
    .build();
    let logical_plan = ReorgPlanner::new()
        .compact_plan(Arc::from("no_table"), schema, chunks, sort_key)
        .unwrap();
    let physical_plan = ctx
        .inner()
        .state()
        .create_physical_plan(&logical_plan)
        .await
        .expect("create physical plan");
    ctx.collect(physical_plan).await.expect("collect data")
}

async fn generate_file(
    generator: &mut RowGenerator,
    schema: SchemaRef,
    config: &Config,
    path: ObjStorePath,
    mem_pool: Arc<dyn MemoryPool>,
    object_store: Arc<dyn ObjectStore>,
) -> ParquetFile {
    let min_time = generator.current_time();
    let mut sink = Vec::new();
    let mut writer = create_writer(
        &mut sink,
        Arc::clone(&schema),
        mem_pool.clone(),
        config.series_id,
    );
    let mut num_rows = 0;
    while num_rows < config.row_count {
        let batch = generator.generate_record_batch();
        if batch.num_rows() == 0 {
            panic!("generator produced no rows");
        }
        num_rows += batch.num_rows();
        writer.write(batch).expect("write RecordBatch");
    }
    let max_time = generator.current_time();
    let meta = writer.close().expect("close parquet writer");
    let size_bytes = sink.len() as u64;
    let parquet_file = ParquetFile {
        path: path.to_string(),
        size_bytes,
        row_count: meta.num_rows as u64,
        min_time: min_time.timestamp_nanos_opt().unwrap(),
        max_time: max_time.timestamp_nanos_opt().unwrap(),
    };
    if !config.dry_run {
        object_store
            .put(&path, sink.into())
            .await
            .expect("write to object store");
    }
    println!("generated file: {path}, rows: {num_rows}, size (MB): {s:.3}, min time: {min_time}, max_time: {max_time}", s = size_bytes as f64 / 1024.0 / 1024.0);
    return parquet_file;
}

enum Metadata {
    Time,
    Tag,
    Field,
    SeriesId,
}

impl Metadata {
    fn create(self) -> HashMap<String, String> {
        match self {
            Metadata::Time => [("iox::column::type", "iox::column_type::timestamp")],
            Metadata::Tag => [("iox::column::type", "iox::column_type::tag")],
            Metadata::Field => [("iox::column::type", "iox::column_type::field::boolean")],
            Metadata::SeriesId => [("iox::column::type", "iox::column_type::sid")],
        }
        .into_iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect()
    }
}

/// Create the Arrow Schema for the data that will be generated.
fn create_schema(config: &Config) -> SchemaRef {
    let mut schema_fields = Vec::new();
    // add the time column:
    schema_fields.push(
        Field::new(
            TIME_COLUMN_NAME,
            DataType::Timestamp(Nanosecond, Some("UTC".into())),
            false,
        )
        .with_metadata(Metadata::Time.create()),
    );
    // add tag columns:
    for t in 0..config.num_tags {
        schema_fields.push(
            Field::new(
                format!("tag_{t}"),
                if config.use_dict_tags {
                    DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8))
                } else {
                    DataType::Utf8
                },
                true,
            )
            .with_metadata(Metadata::Tag.create()),
        );
    }
    // add field columns:
    for f in 0..config.num_fields {
        schema_fields.push(
            Field::new(format!("field_{f}"), DataType::Boolean, true)
                .with_metadata(Metadata::Field.create()),
        );
    }
    // _series_id if specified:
    if config.series_id {
        schema_fields.push(
            Field::new(SERIES_ID_COLUMN_NAME, DataType::FixedSizeBinary(32), false)
                .with_metadata(Metadata::SeriesId.create()),
        );
    }

    SchemaRef::new(ArrowSchema::new(schema_fields))
}

fn create_writer<W: Write + Send>(
    sink: W,
    schema: SchemaRef,
    mem_pool: Arc<dyn MemoryPool>,
    series_id: bool,
) -> TrackedMemoryArrowWriter<W> {
    let mut builder = WriterProperties::builder()
        .set_compression(parquet::basic::Compression::ZSTD(Default::default()))
        .set_max_row_group_size(ROW_GROUP_WRITE_SIZE);
    if series_id {
        builder = builder.set_column_encoding(
            ColumnPath::from(SERIES_ID_COLUMN_NAME),
            parquet::basic::Encoding::DELTA_BYTE_ARRAY,
        );
    }

    let props = builder.build();
    TrackedMemoryArrowWriter::try_new(sink, schema, props, mem_pool).expect("create writer")
}

/// Trait for generating values, much like an Iterator, but with the
/// optional ability to be reset.
trait Generator {
    type Value;

    fn generate(&mut self) -> Option<Self::Value>;

    fn reset(&mut self) {}
}

type Rng = SmallRng;

/// Generate rows accross all cardinalities, i.e., tags, for a given
/// timestamp, before resetting the tag generator, and incrementing the time.
struct RowGenerator {
    time: TimeGenerator,
    tags: Vec<TagGenerator>,
    series_id: bool,
    fields: Vec<FieldGenerator>,
    duplication_factor: usize,
    rng: SmallRng,
}

impl From<&Config> for RowGenerator {
    fn from(config: &Config) -> Self {
        let mut rng = SmallRng::seed_from_u64(config.rng_seed);
        let time = TimeGenerator::new(
            config.start_time.unwrap_or(Utc::now()),
            config.sampling_interval.into(),
        );
        let mut tags = Vec::new();
        for _ in 0..config.num_tags {
            tags.push(TagGenerator::new(&mut rng, config.cardinality).with_base("value-"));
        }
        let mut fields = Vec::new();
        for _ in 0..config.num_fields {
            fields.push(FieldGenerator::new(&rng));
        }

        Self {
            time,
            tags,
            series_id: config.series_id,
            duplication_factor: config.duplication_factor,
            fields,
            rng: SmallRng::seed_from_u64(config.rng_seed),
        }
    }
}

impl RowGenerator {
    fn generate_record_batch(&mut self) -> RecordBatch {
        let mut rows = Vec::new();
        while let Some(row) = self.generate() {
            rows.push(row.clone());
            for _ in 0..self.duplication_factor {
                rows.push(row.clone());
            }
        }
        self.reset();
        if self.duplication_factor > 0 {
            rows.shuffle(&mut self.rng);
        }
        let mut builder = RowBuilder::new(self.tags.len(), self.fields.len());
        builder.extend(rows.as_slice());
        RecordBatch::from(&builder.finish())
    }

    fn current_time(&self) -> DateTime<Utc> {
        self.time.current()
    }
}

impl Generator for RowGenerator {
    type Value = Row;

    fn generate(&mut self) -> Option<Self::Value> {
        let time = self.time.current();
        let tags = self
            .tags
            .iter_mut()
            .map(|t| t.generate())
            .enumerate()
            .map(|(i, val)| val.map(|t| Tag(format!("tag_{i}"), t)))
            .collect::<Option<Vec<Tag>>>()?;
        let fields = self
            .fields
            .iter_mut()
            .map(|f| f.generate())
            .enumerate()
            .map(|(i, val)| val.map(|f| FieldEntry(format!("field_{i}"), f)))
            .collect::<Option<Vec<FieldEntry>>>()?;
        let series_id = self.series_id.then(|| {
            let tags_str = tags
                .iter()
                .map(|t| format!("{}={}", t.0, t.1))
                .collect::<Vec<String>>()
                .join(",");
            Sha256::digest(tags_str).into()
        });

        Some(Row {
            time,
            tags,
            fields,
            series_id,
        })
    }

    fn reset(&mut self) {
        self.time.generate().expect("can increment time");
        self.tags.iter_mut().for_each(|t| t.reset());
    }
}

#[derive(Debug, Clone)]
struct Row {
    time: DateTime<Utc>,
    tags: Vec<Tag>,
    fields: Vec<FieldEntry>,
    series_id: Option<[u8; 32]>,
}

#[derive(Debug, Clone)]
struct Tag(String, String);

#[allow(dead_code)]
#[derive(Debug, Clone)]
struct FieldEntry(String, bool);

#[derive(Debug)]
struct RowBuilder {
    time: TimestampNanosecondBuilder,
    tags: Vec<StringBuilder>,
    fields: Vec<BooleanBuilder>,
    series_id: Option<FixedSizeBinaryBuilder>,
}

impl RowBuilder {
    fn new(num_tags: usize, num_fields: usize) -> Self {
        Self {
            time: TimestampNanosecondBuilder::new().with_timezone(Arc::from("UTC")),
            tags: (0..num_tags).map(|_| Default::default()).collect(),
            fields: (0..num_fields).map(|_| Default::default()).collect(),
            series_id: None,
        }
    }

    fn append(&mut self, row: &Row) {
        self.time
            .append_value(row.time.timestamp_nanos_opt().unwrap());
        if let Some(series_id) = row.series_id {
            let b = self
                .series_id
                .get_or_insert_with(|| FixedSizeBinaryBuilder::new(32));
            b.append_value(series_id).unwrap();
        }
        for (tag, b) in row.tags.iter().zip(self.tags.iter_mut()) {
            b.append_value(&tag.1);
        }
        for (field, b) in row.fields.iter().zip(self.fields.iter_mut()) {
            b.append_value(field.1);
        }
    }

    fn finish(&mut self) -> StructArray {
        let mut struct_fields = Vec::new();
        struct_fields.push((
            Arc::new(Field::new(
                "time",
                DataType::Timestamp(Nanosecond, Some("UTC".into())),
                false,
            )),
            Arc::new(self.time.finish()) as ArrayRef,
        ));
        for (i, tag) in self.tags.iter_mut().enumerate() {
            struct_fields.push((
                Arc::new(Field::new(format!("tag_{i}"), DataType::Utf8, false)),
                Arc::new(tag.finish()) as ArrayRef,
            ));
        }
        for (i, field) in self.fields.iter_mut().enumerate() {
            struct_fields.push((
                Arc::new(Field::new(format!("field_{i}"), DataType::Boolean, false)),
                Arc::new(field.finish()) as ArrayRef,
            ));
        }
        if let Some(ref mut series_id) = self.series_id {
            struct_fields.push((
                Arc::new(Field::new(
                    "_series_id",
                    DataType::FixedSizeBinary(32),
                    false,
                )),
                Arc::new(series_id.finish()) as ArrayRef,
            ));
        }

        StructArray::from(struct_fields)
    }
}

impl<'a> Extend<&'a Row> for RowBuilder {
    fn extend<T: IntoIterator<Item = &'a Row>>(&mut self, iter: T) {
        iter.into_iter().for_each(|row| self.append(row))
    }
}

struct TimeGenerator {
    current: DateTime<Utc>,
    interval: Duration,
}

impl TimeGenerator {
    fn new(start: DateTime<Utc>, interval: Duration) -> Self {
        Self {
            current: start,
            interval,
        }
    }

    fn current(&self) -> DateTime<Utc> {
        self.current
    }
}

impl Generator for TimeGenerator {
    type Value = DateTime<Utc>;

    fn generate(&mut self) -> Option<Self::Value> {
        let time = self.current;
        self.current = self.current + self.interval;
        Some(time)
    }
}

struct FieldGenerator {
    rng: Rng,
}

impl FieldGenerator {
    fn new(rng: &Rng) -> Self {
        Self { rng: rng.clone() }
    }
}

impl Generator for FieldGenerator {
    type Value = bool;

    fn generate(&mut self) -> Option<Self::Value> {
        [true, false].iter().choose_stable(&mut self.rng).copied()
    }
}

struct TagGenerator {
    base: Option<String>,
    cardinality: CardinalityGenerator,
}

impl TagGenerator {
    fn new(rng: &mut Rng, cardinality: u32) -> Self {
        Self {
            base: None,
            cardinality: CardinalityGenerator::new(rng, cardinality),
        }
    }

    fn with_base<S: Into<String>>(mut self, base: S) -> Self {
        self.base = Some(base.into());
        self
    }
}

impl Generator for TagGenerator {
    type Value = String;

    fn generate(&mut self) -> Option<Self::Value> {
        let card = self.cardinality.generate()?;
        let mut buf = Vec::new();
        if let Some(base) = &self.base {
            write!(&mut buf, "{base}").unwrap();
        }
        write!(&mut buf, "{card}").unwrap();
        Some(String::from_utf8(buf).unwrap())
    }

    fn reset(&mut self) {
        self.cardinality.reset();
    }
}

/// Generate cardinality values at random, in the form of integers.
///
/// Values are generated from a set, at random, but will always be generated in the same order
/// given a particular `seed`.
struct CardinalityGenerator {
    available: Vec<u32>,
    current: usize,
}

impl CardinalityGenerator {
    fn new(rng: &mut Rng, cardinality: u32) -> Self {
        let mut available: Vec<u32> = (0..cardinality).into_iter().collect();
        available.shuffle(rng);
        Self {
            available,
            current: 0,
        }
    }
}

impl Generator for CardinalityGenerator {
    type Value = u32;

    fn generate(&mut self) -> Option<Self::Value> {
        let v = *self.available.get(self.current)?;
        self.current += 1;
        Some(v)
    }

    fn reset(&mut self) {
        self.current = 0;
    }
}
