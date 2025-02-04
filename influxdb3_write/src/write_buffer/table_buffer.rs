//! The in memory buffer of a table that can be quickly added to and queried

use arrow::array::{
    Array, ArrayRef, BooleanBuilder, Float64Builder, Int64Builder, StringBuilder,
    StringDictionaryBuilder, TimestampNanosecondBuilder, UInt64Builder,
};
use arrow::datatypes::Int32Type;
use arrow::record_batch::RecordBatch;
use data_types::TimestampMinMax;
use hashbrown::{HashMap, HashSet};
use influxdb3_catalog::catalog::TableDefinition;
use influxdb3_id::ColumnId;
use influxdb3_wal::{FieldData, Row};
use observability_deps::tracing::error;
use schema::sort::SortKey;
use schema::{InfluxColumnType, InfluxFieldType, Schema, SchemaBuilder};
use std::collections::btree_map::Entry;
use std::collections::BTreeMap;
use std::mem::size_of;
use std::sync::Arc;
use thiserror::Error;

use crate::ChunkFilter;

#[derive(Debug, Error)]
pub enum Error {
    #[error("Field not found in table buffer: {0}")]
    FieldNotFound(String),

    #[error("Error creating record batch: {0}")]
    RecordBatchError(#[from] arrow::error::ArrowError),
}

pub(crate) type Result<T, E = Error> = std::result::Result<T, E>;

pub struct TableBuffer {
    chunk_time_to_chunks: BTreeMap<i64, MutableTableChunk>,
    snapshotting_chunks: Vec<SnapshotChunk>,
    pub(crate) sort_key: SortKey,
}

impl TableBuffer {
    pub fn new(sort_key: SortKey) -> Self {
        Self {
            chunk_time_to_chunks: BTreeMap::default(),
            snapshotting_chunks: vec![],
            sort_key,
        }
    }

    pub fn buffer_chunk(&mut self, chunk_time: i64, rows: &[Row]) {
        let buffer_chunk = self
            .chunk_time_to_chunks
            .entry(chunk_time)
            .or_insert_with(|| MutableTableChunk {
                timestamp_min: i64::MAX,
                timestamp_max: i64::MIN,
                data: Default::default(),
                row_count: 0,
            });

        buffer_chunk.add_rows(rows);
    }

    /// Produce a partitioned set of record batches along with their min/max timestamp
    ///
    /// The partitions are stored and returned in a `HashMap`, keyed on the generation time.
    ///
    /// This uses the provided `filter` to prune out chunks from the buffer that do not fall in
    /// the filter's time boundaries. If the filter contains literal guarantees on tag columns
    /// that are in the buffer index, this will also leverage those to prune rows in the resulting
    /// chunks that do not satisfy the guarantees specified in the filter.
    pub fn partitioned_record_batches(
        &self,
        table_def: Arc<TableDefinition>,
        filter: &ChunkFilter<'_>,
    ) -> Result<HashMap<i64, (TimestampMinMax, Vec<RecordBatch>)>> {
        let mut batches = HashMap::new();
        let schema = table_def.schema.as_arrow();
        for sc in self.snapshotting_chunks.iter().filter(|sc| {
            filter.test_time_stamp_min_max(sc.timestamp_min_max.min, sc.timestamp_min_max.max)
        }) {
            let cols: std::result::Result<Vec<_>, _> = schema
                .fields()
                .iter()
                .map(|f| {
                    let col = sc
                        .record_batch
                        .column_by_name(f.name())
                        .ok_or(Error::FieldNotFound(f.name().to_string()));
                    col.cloned()
                })
                .collect();
            let cols = cols?;
            let rb = RecordBatch::try_new(Arc::clone(&schema), cols)?;
            let (ts, v) = batches
                .entry(sc.chunk_time)
                .or_insert_with(|| (sc.timestamp_min_max, Vec::new()));
            *ts = ts.union(&sc.timestamp_min_max);
            v.push(rb);
        }
        for (t, c) in self
            .chunk_time_to_chunks
            .iter()
            .filter(|(_, c)| filter.test_time_stamp_min_max(c.timestamp_min, c.timestamp_max))
        {
            let ts_min_max = TimestampMinMax::new(c.timestamp_min, c.timestamp_max);
            let (ts, v) = batches
                .entry(*t)
                .or_insert_with(|| (ts_min_max, Vec::new()));
            *ts = ts.union(&ts_min_max);
            v.push(c.record_batch(Arc::clone(&table_def))?);
        }
        Ok(batches)
    }

    pub fn timestamp_min_max(&self) -> TimestampMinMax {
        let (min, max) = if self.chunk_time_to_chunks.is_empty() {
            (0, 0)
        } else {
            self.chunk_time_to_chunks
                .values()
                .map(|c| (c.timestamp_min, c.timestamp_max))
                .fold((i64::MAX, i64::MIN), |(a_min, b_min), (a_max, b_max)| {
                    (a_min.min(b_min), a_max.max(b_max))
                })
        };
        let mut timestamp_min_max = TimestampMinMax::new(min, max);

        for sc in &self.snapshotting_chunks {
            timestamp_min_max = timestamp_min_max.union(&sc.timestamp_min_max);
        }

        timestamp_min_max
    }

    /// Returns an estimate of the size of this table buffer based on the data and index sizes.
    #[allow(dead_code)]
    pub fn computed_size(&self) -> usize {
        let mut size = size_of::<Self>();

        for c in self.chunk_time_to_chunks.values() {
            for builder in c.data.values() {
                size += size_of::<ColumnId>() + size_of::<String>() + builder.size();
            }
        }

        size
    }

    pub fn snapshot(
        &mut self,
        table_def: Arc<TableDefinition>,
        older_than_chunk_time: i64,
    ) -> Vec<SnapshotChunk> {
        let keys_to_remove = self
            .chunk_time_to_chunks
            .keys()
            .filter(|k| **k < older_than_chunk_time)
            .copied()
            .collect::<Vec<_>>();
        self.snapshotting_chunks = keys_to_remove
            .into_iter()
            .map(|chunk_time| {
                let chunk = self.chunk_time_to_chunks.remove(&chunk_time).unwrap();
                let timestamp_min_max = chunk.timestamp_min_max();
                let (schema, record_batch) = chunk.into_schema_record_batch(Arc::clone(&table_def));

                SnapshotChunk {
                    chunk_time,
                    timestamp_min_max,
                    record_batch,
                    schema,
                }
            })
            .collect::<Vec<_>>();

        self.snapshotting_chunks.clone()
    }

    pub fn clear_snapshots(&mut self) {
        self.snapshotting_chunks.clear();
    }
}

#[derive(Debug, Clone)]
pub struct SnapshotChunk {
    pub(crate) chunk_time: i64,
    pub(crate) timestamp_min_max: TimestampMinMax,
    pub(crate) record_batch: RecordBatch,
    pub(crate) schema: Schema,
}

// Debug implementation for TableBuffer
impl std::fmt::Debug for TableBuffer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let (min_time, max_time, row_count) = self
            .chunk_time_to_chunks
            .values()
            .map(|c| (c.timestamp_min, c.timestamp_max, c.row_count))
            .fold(
                (i64::MAX, i64::MIN, 0),
                |(a_min, a_max, a_count), (b_min, b_max, b_count)| {
                    (a_min.min(b_min), a_max.max(b_max), a_count + b_count)
                },
            );
        f.debug_struct("TableBuffer")
            .field("chunk_count", &self.chunk_time_to_chunks.len())
            .field("timestamp_min", &min_time)
            .field("timestamp_max", &max_time)
            .field("row_count", &row_count)
            .finish()
    }
}

struct MutableTableChunk {
    timestamp_min: i64,
    timestamp_max: i64,
    data: BTreeMap<ColumnId, Builder>,
    row_count: usize,
}

impl MutableTableChunk {
    fn add_rows(&mut self, rows: &[Row]) {
        let new_row_count = rows.len();

        for (row_index, r) in rows.iter().enumerate() {
            let mut value_added = HashSet::with_capacity(r.fields.len());

            for f in &r.fields {
                value_added.insert(f.id);

                match &f.value {
                    FieldData::Timestamp(v) => {
                        self.timestamp_min = self.timestamp_min.min(*v);
                        self.timestamp_max = self.timestamp_max.max(*v);

                        let b = self.data.entry(f.id).or_insert_with(|| {
                            let mut time_builder = TimestampNanosecondBuilder::new();
                            // append nulls for all previous rows
                            time_builder.append_nulls(row_index + self.row_count);
                            Builder::Time(time_builder)
                        });
                        if let Builder::Time(b) = b {
                            b.append_value(*v);
                        } else {
                            panic!("unexpected field type");
                        }
                    }
                    FieldData::Tag(v) => {
                        if let Entry::Vacant(e) = self.data.entry(f.id) {
                            let mut tag_builder = StringDictionaryBuilder::new();
                            // append nulls for all previous rows
                            for _ in 0..(row_index + self.row_count) {
                                tag_builder.append_value("");
                            }
                            e.insert(Builder::Tag(tag_builder));
                        }
                        let b = self.data.get_mut(&f.id).expect("tag builder should exist");
                        if let Builder::Tag(b) = b {
                            b.append(v)
                                .expect("shouldn't be able to overflow 32 bit dictionary");
                        } else {
                            panic!("unexpected field type");
                        }
                    }
                    FieldData::Key(v) => {
                        if let Entry::Vacant(e) = self.data.entry(f.id) {
                            let key_builder = StringDictionaryBuilder::new();
                            if self.row_count > 0 {
                                panic!("series key columns must be passed in the very first write for a table");
                            }
                            e.insert(Builder::Key(key_builder));
                        }
                        let b = self.data.get_mut(&f.id).expect("key builder should exist");
                        let Builder::Key(b) = b else {
                            panic!("unexpected field type");
                        };
                        b.append_value(v);
                    }
                    FieldData::String(v) => {
                        let b = self.data.entry(f.id).or_insert_with(|| {
                            let mut string_builder = StringBuilder::new();
                            // append nulls for all previous rows
                            for _ in 0..(row_index + self.row_count) {
                                string_builder.append_null();
                            }
                            Builder::String(string_builder)
                        });
                        if let Builder::String(b) = b {
                            b.append_value(v);
                        } else {
                            panic!("unexpected field type");
                        }
                    }
                    FieldData::Integer(v) => {
                        let b = self.data.entry(f.id).or_insert_with(|| {
                            let mut int_builder = Int64Builder::new();
                            // append nulls for all previous rows
                            int_builder.append_nulls(row_index + self.row_count);
                            Builder::I64(int_builder)
                        });
                        if let Builder::I64(b) = b {
                            b.append_value(*v);
                        } else {
                            panic!("unexpected field type");
                        }
                    }
                    FieldData::UInteger(v) => {
                        let b = self.data.entry(f.id).or_insert_with(|| {
                            let mut uint_builder = UInt64Builder::new();
                            // append nulls for all previous rows
                            uint_builder.append_nulls(row_index + self.row_count);
                            Builder::U64(uint_builder)
                        });
                        if let Builder::U64(b) = b {
                            b.append_value(*v);
                        } else {
                            panic!("unexpected field type");
                        }
                    }
                    FieldData::Float(v) => {
                        let b = self.data.entry(f.id).or_insert_with(|| {
                            let mut float_builder = Float64Builder::new();
                            // append nulls for all previous rows
                            float_builder.append_nulls(row_index + self.row_count);
                            Builder::F64(float_builder)
                        });
                        if let Builder::F64(b) = b {
                            b.append_value(*v);
                        } else {
                            panic!("unexpected field type");
                        }
                    }
                    FieldData::Boolean(v) => {
                        let b = self.data.entry(f.id).or_insert_with(|| {
                            let mut bool_builder = BooleanBuilder::new();
                            // append nulls for all previous rows
                            bool_builder.append_nulls(row_index + self.row_count);
                            Builder::Bool(bool_builder)
                        });
                        if let Builder::Bool(b) = b {
                            b.append_value(*v);
                        } else {
                            panic!("unexpected field type");
                        }
                    }
                }
            }

            // add nulls for any columns not present
            for (column_id, builder) in &mut self.data {
                if !value_added.contains(column_id) {
                    match builder {
                        Builder::Bool(b) => b.append_null(),
                        Builder::F64(b) => b.append_null(),
                        Builder::I64(b) => b.append_null(),
                        Builder::U64(b) => b.append_null(),
                        Builder::String(b) => b.append_null(),
                        Builder::Tag(b) | Builder::Key(b) => {
                            // NOTE: we use an empty string "" for tags that are omitted
                            b.append_value("");
                        }
                        Builder::Time(b) => b.append_null(),
                    }
                }
            }
        }

        self.row_count += new_row_count;
    }

    fn timestamp_min_max(&self) -> TimestampMinMax {
        TimestampMinMax::new(self.timestamp_min, self.timestamp_max)
    }

    fn record_batch(&self, table_def: Arc<TableDefinition>) -> Result<RecordBatch> {
        let schema = table_def.schema.as_arrow();

        let mut cols = Vec::with_capacity(schema.fields().len());

        for f in schema.fields() {
            let column_def = table_def
                .column_definition(f.name().as_ref())
                .expect("a valid column name");
            let b = match self.data.get(&column_def.id) {
                Some(b) => b.as_arrow(),
                None => array_ref_nulls_for_type(column_def.data_type, self.row_count),
            };

            cols.push(b);
        }

        Ok(RecordBatch::try_new(schema, cols)?)
    }

    fn into_schema_record_batch(self, table_def: Arc<TableDefinition>) -> (Schema, RecordBatch) {
        let mut cols = Vec::with_capacity(self.data.len());
        let mut schema_builder = SchemaBuilder::new();
        let mut cols_in_batch = HashSet::new();
        for (col_id, builder) in self.data.into_iter() {
            cols_in_batch.insert(col_id);
            let (col_type, col) = builder.into_influxcol_and_arrow();
            schema_builder.influx_column(
                table_def
                    .column_id_to_name(&col_id)
                    .expect("valid column id")
                    .as_ref(),
                col_type,
            );
            cols.push(col);
        }

        // ensure that every series key column is present in the batch
        for col_id in &table_def.series_key {
            if !cols_in_batch.contains(col_id) {
                let col_name = table_def
                    .column_id_to_name(col_id)
                    .expect("valid column id");
                schema_builder.influx_column(col_name.as_ref(), InfluxColumnType::Tag);
                let mut tag_builder: StringDictionaryBuilder<Int32Type> =
                    StringDictionaryBuilder::new();
                for _ in 0..self.row_count {
                    tag_builder.append_value("");
                }

                cols.push(Arc::new(tag_builder.finish()));
                cols_in_batch.insert(*col_id);
            }
        }

        // ensure that every field column is present in the batch
        for (col_id, col_def) in &table_def.columns {
            if !cols_in_batch.contains(col_id) {
                schema_builder.influx_column(col_def.name.as_ref(), col_def.data_type);
                let col = array_ref_nulls_for_type(col_def.data_type, self.row_count);

                cols.push(col);
            }
        }
        let schema = schema_builder
            .build()
            .expect("should always be able to build schema");
        let arrow_schema = schema.as_arrow();

        (
            schema,
            RecordBatch::try_new(arrow_schema, cols)
                .expect("should always be able to build record batch"),
        )
    }
}

fn array_ref_nulls_for_type(data_type: InfluxColumnType, len: usize) -> ArrayRef {
    match data_type {
        InfluxColumnType::Field(InfluxFieldType::Boolean) => {
            let mut builder = BooleanBuilder::new();
            builder.append_nulls(len);
            Arc::new(builder.finish())
        }
        InfluxColumnType::Timestamp => {
            let mut builder = TimestampNanosecondBuilder::new();
            builder.append_nulls(len);
            Arc::new(builder.finish())
        }
        InfluxColumnType::Tag => {
            let mut builder: StringDictionaryBuilder<Int32Type> = StringDictionaryBuilder::new();
            for _ in 0..len {
                builder.append_value("");
            }
            Arc::new(builder.finish())
        }
        InfluxColumnType::Field(InfluxFieldType::Integer) => {
            let mut builder = Int64Builder::new();
            builder.append_nulls(len);
            Arc::new(builder.finish())
        }
        InfluxColumnType::Field(InfluxFieldType::Float) => {
            let mut builder = Float64Builder::new();
            builder.append_nulls(len);
            Arc::new(builder.finish())
        }
        InfluxColumnType::Field(InfluxFieldType::String) => {
            let mut builder = StringBuilder::new();
            for _ in 0..len {
                builder.append_null();
            }
            Arc::new(builder.finish())
        }
        InfluxColumnType::Field(InfluxFieldType::UInteger) => {
            let mut builder = UInt64Builder::new();
            builder.append_nulls(len);
            Arc::new(builder.finish())
        }
    }
}

// Debug implementation for TableBuffer
impl std::fmt::Debug for MutableTableChunk {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MutableTableChunk")
            .field("timestamp_min", &self.timestamp_min)
            .field("timestamp_max", &self.timestamp_max)
            .field("row_count", &self.row_count)
            .finish()
    }
}

pub(super) enum Builder {
    Bool(BooleanBuilder),
    I64(Int64Builder),
    F64(Float64Builder),
    U64(UInt64Builder),
    String(StringBuilder),
    Tag(StringDictionaryBuilder<Int32Type>),
    // For now we use a string dict to be consistent with tags, but in future
    // keys, like fields may support different data types.
    Key(StringDictionaryBuilder<Int32Type>),
    Time(TimestampNanosecondBuilder),
}

impl Builder {
    fn as_arrow(&self) -> ArrayRef {
        match self {
            Self::Bool(b) => Arc::new(b.finish_cloned()),
            Self::I64(b) => Arc::new(b.finish_cloned()),
            Self::F64(b) => Arc::new(b.finish_cloned()),
            Self::U64(b) => Arc::new(b.finish_cloned()),
            Self::String(b) => Arc::new(b.finish_cloned()),
            Self::Tag(b) => Arc::new(b.finish_cloned()),
            Self::Key(b) => Arc::new(b.finish_cloned()),
            Self::Time(b) => Arc::new(b.finish_cloned()),
        }
    }

    fn into_influxcol_and_arrow(self) -> (InfluxColumnType, ArrayRef) {
        match self {
            Self::Bool(mut b) => (
                InfluxColumnType::Field(InfluxFieldType::Boolean),
                Arc::new(b.finish()),
            ),
            Self::I64(mut b) => (
                InfluxColumnType::Field(InfluxFieldType::Integer),
                Arc::new(b.finish()),
            ),
            Self::F64(mut b) => (
                InfluxColumnType::Field(InfluxFieldType::Float),
                Arc::new(b.finish()),
            ),
            Self::U64(mut b) => (
                InfluxColumnType::Field(InfluxFieldType::UInteger),
                Arc::new(b.finish()),
            ),
            Self::String(mut b) => (
                InfluxColumnType::Field(InfluxFieldType::String),
                Arc::new(b.finish()),
            ),
            Self::Tag(mut b) => (InfluxColumnType::Tag, Arc::new(b.finish())),
            Self::Key(mut b) => (InfluxColumnType::Tag, Arc::new(b.finish())),
            Self::Time(mut b) => (InfluxColumnType::Timestamp, Arc::new(b.finish())),
        }
    }

    fn size(&self) -> usize {
        let data_size = match self {
            Self::Bool(b) => b.capacity() + b.validity_slice().map(|s| s.len()).unwrap_or(0),
            Self::I64(b) => {
                size_of::<i64>() * b.capacity() + b.validity_slice().map(|s| s.len()).unwrap_or(0)
            }
            Self::F64(b) => {
                size_of::<f64>() * b.capacity() + b.validity_slice().map(|s| s.len()).unwrap_or(0)
            }
            Self::U64(b) => {
                size_of::<u64>() * b.capacity() + b.validity_slice().map(|s| s.len()).unwrap_or(0)
            }
            Self::String(b) => {
                b.values_slice().len()
                    + b.offsets_slice().len()
                    + b.validity_slice().map(|s| s.len()).unwrap_or(0)
            }
            Self::Tag(b) | Self::Key(b) => {
                let b = b.finish_cloned();
                b.keys().len() * size_of::<i32>() + b.values().get_array_memory_size()
            }
            Self::Time(b) => size_of::<i64>() * b.capacity(),
        };
        size_of::<Self>() + data_size
    }
}

#[cfg(test)]
mod tests {
    use crate::{write_buffer::validator::WriteValidator, Precision};

    use super::*;
    use arrow_util::assert_batches_sorted_eq;
    use data_types::NamespaceName;
    use datafusion::prelude::{col, lit_timestamp_nano, Expr};
    use influxdb3_catalog::catalog::{Catalog, DatabaseSchema};
    use iox_time::Time;

    struct TestWriter {
        catalog: Arc<Catalog>,
    }

    impl TestWriter {
        const DB_NAME: &str = "test-db";

        fn new() -> Self {
            let catalog = Arc::new(Catalog::new("test-node".into(), "test-instance".into()));
            Self { catalog }
        }

        fn write_to_rows(&self, lp: impl AsRef<str>, ingest_time_sec: i64) -> Vec<Row> {
            let db = NamespaceName::try_from(Self::DB_NAME).unwrap();
            let ingest_time_ns = ingest_time_sec * 1_000_000_000;
            let validator =
                WriteValidator::initialize(db, Arc::clone(&self.catalog), ingest_time_ns).unwrap();
            validator
                .parse_lines_and_update_schema(
                    lp.as_ref(),
                    false,
                    Time::from_timestamp_nanos(ingest_time_ns),
                    Precision::Nanosecond,
                )
                .map(|r| r.into_inner().to_rows())
                .unwrap()
        }

        fn db_schema(&self) -> Arc<DatabaseSchema> {
            self.catalog.db_schema(Self::DB_NAME).unwrap()
        }
    }

    #[test]
    fn test_partitioned_table_buffer_batches() {
        let writer = TestWriter::new();

        let mut row_batches = Vec::new();
        for t in 0..10 {
            let offset = t * 10;
            let rows = writer.write_to_rows(
                format!(
                    "\
            tbl,tag=a val=\"thing {t}-1\" {o1}\n\
            tbl,tag=b val=\"thing {t}-2\" {o2}\n\
            ",
                    o1 = offset + 1,
                    o2 = offset + 2,
                ),
                offset,
            );
            row_batches.push((rows, offset));
        }

        let table_def = writer.db_schema().table_definition("tbl").unwrap();

        let mut table_buffer = TableBuffer::new(SortKey::empty());
        for (rows, offset) in row_batches {
            table_buffer.buffer_chunk(offset, &rows);
        }

        let partitioned_batches = table_buffer
            .partitioned_record_batches(Arc::clone(&table_def), &ChunkFilter::default())
            .unwrap();

        assert_eq!(10, partitioned_batches.len());

        for t in 0..10 {
            let offset = t * 10;
            let (ts_min_max, batches) = partitioned_batches.get(&offset).unwrap();
            assert_eq!(TimestampMinMax::new(offset + 1, offset + 2), *ts_min_max);
            assert_batches_sorted_eq!(
                [
                    "+-----+--------------------------------+-----------+",
                    "| tag | time                           | val       |",
                    "+-----+--------------------------------+-----------+",
                    format!(
                        "| a   | 1970-01-01T00:00:00.{:0>9}Z | thing {t}-1 |",
                        offset + 1
                    )
                    .as_str(),
                    format!(
                        "| b   | 1970-01-01T00:00:00.{:0>9}Z | thing {t}-2 |",
                        offset + 2
                    )
                    .as_str(),
                    "+-----+--------------------------------+-----------+",
                ],
                batches
            );
        }
    }

    #[test]
    fn test_computed_size_of_buffer() {
        let writer = TestWriter::new();

        let rows = writer.write_to_rows(
            "\
            tbl,tag=a value=1i 1\n\
            tbl,tag=b value=2i 2\n\
            tbl,tag=this\\ is\\ a\\ long\\ tag\\ value\\ to\\ store value=3i 3\n\
            ",
            0,
        );

        let mut table_buffer = TableBuffer::new(SortKey::empty());
        table_buffer.buffer_chunk(0, &rows);

        let size = table_buffer.computed_size();
        assert_eq!(size, 17769);
    }

    #[test]
    fn timestamp_min_max_works_when_empty() {
        let table_buffer = TableBuffer::new(SortKey::empty());
        let timestamp_min_max = table_buffer.timestamp_min_max();
        assert_eq!(timestamp_min_max.min, 0);
        assert_eq!(timestamp_min_max.max, 0);
    }

    #[test_log::test]
    fn test_time_filters() {
        let writer = TestWriter::new();

        let mut row_batches = Vec::new();
        for offset in 0..100 {
            let rows = writer.write_to_rows(
                format!(
                    "\
                tbl,tag=a val={}\n\
                tbl,tag=b val={}\n\
                ",
                    offset + 1,
                    offset + 2
                ),
                offset,
            );
            row_batches.push((offset, rows));
        }
        let table_def = writer.db_schema().table_definition("tbl").unwrap();
        let mut table_buffer = TableBuffer::new(SortKey::empty());

        for (offset, rows) in row_batches {
            table_buffer.buffer_chunk(offset, &rows);
        }

        struct TestCase<'a> {
            filter: &'a [Expr],
            expected_output: &'a [&'a str],
        }

        let test_cases = [
            TestCase {
                filter: &[col("time").gt(lit_timestamp_nano(97_000_000_000i64))],
                expected_output: &[
                    "+-----+----------------------+-------+",
                    "| tag | time                 | val   |",
                    "+-----+----------------------+-------+",
                    "| a   | 1970-01-01T00:01:38Z | 99.0  |",
                    "| a   | 1970-01-01T00:01:39Z | 100.0 |",
                    "| b   | 1970-01-01T00:01:38Z | 100.0 |",
                    "| b   | 1970-01-01T00:01:39Z | 101.0 |",
                    "+-----+----------------------+-------+",
                ],
            },
            TestCase {
                filter: &[col("time").lt(lit_timestamp_nano(3_000_000_000i64))],
                expected_output: &[
                    "+-----+----------------------+-----+",
                    "| tag | time                 | val |",
                    "+-----+----------------------+-----+",
                    "| a   | 1970-01-01T00:00:00Z | 1.0 |",
                    "| a   | 1970-01-01T00:00:01Z | 2.0 |",
                    "| a   | 1970-01-01T00:00:02Z | 3.0 |",
                    "| b   | 1970-01-01T00:00:00Z | 2.0 |",
                    "| b   | 1970-01-01T00:00:01Z | 3.0 |",
                    "| b   | 1970-01-01T00:00:02Z | 4.0 |",
                    "+-----+----------------------+-----+",
                ],
            },
            TestCase {
                filter: &[col("time")
                    .gt(lit_timestamp_nano(3_000_000_000i64))
                    .and(col("time").lt(lit_timestamp_nano(6_000_000_000i64)))],
                expected_output: &[
                    "+-----+----------------------+-----+",
                    "| tag | time                 | val |",
                    "+-----+----------------------+-----+",
                    "| a   | 1970-01-01T00:00:04Z | 5.0 |",
                    "| a   | 1970-01-01T00:00:05Z | 6.0 |",
                    "| b   | 1970-01-01T00:00:04Z | 6.0 |",
                    "| b   | 1970-01-01T00:00:05Z | 7.0 |",
                    "+-----+----------------------+-----+",
                ],
            },
        ];

        for t in test_cases {
            let filter = ChunkFilter::new(&table_def, t.filter).unwrap();
            let batches = table_buffer
                .partitioned_record_batches(Arc::clone(&table_def), &filter)
                .unwrap()
                .into_values()
                .flat_map(|(_, batches)| batches)
                .collect::<Vec<RecordBatch>>();
            assert_batches_sorted_eq!(t.expected_output, &batches);
        }
    }
}
