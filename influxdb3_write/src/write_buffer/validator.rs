use std::{borrow::Cow, sync::Arc};

use crate::{write_buffer::Result, Precision, WriteLineError};
use data_types::{NamespaceName, Timestamp};
use indexmap::IndexMap;
use influxdb3_catalog::catalog::{
    influx_column_type_from_field_value, Catalog, DatabaseSchema, TableDefinition,
};

use influxdb3_id::{ColumnId, TableId};
use influxdb3_wal::{
    CatalogBatch, CatalogOp, Field, FieldAdditions, FieldData, FieldDefinition, Gen1Duration,
    OrderedCatalogBatch, Row, TableChunks, WriteBatch,
};
use influxdb_line_protocol::{parse_lines, ParsedLine};
use iox_time::Time;
use schema::{InfluxColumnType, TIME_COLUMN_NAME};

use super::Error;

/// Type state for the [`WriteValidator`] after it has been initialized
/// with the catalog.
#[derive(Debug)]
pub struct WithCatalog {
    catalog: Arc<Catalog>,
    db_schema: Arc<DatabaseSchema>,
    time_now_ns: i64,
}

/// Type state for the [`WriteValidator`] after it has parsed v1 or v3
/// line protocol.
#[derive(Debug)]
pub struct LinesParsed {
    catalog: WithCatalog,
    lines: Vec<QualifiedLine>,
    bytes: u64,
    catalog_batch: Option<OrderedCatalogBatch>,
    errors: Vec<WriteLineError>,
}

impl LinesParsed {
    /// Convert this set of parsed and qualified lines into a set of rows
    ///
    /// This is useful for testing when you need to use the write validator to parse line protocol
    /// and get the raw row data for the WAL.
    pub fn to_rows(self) -> Vec<Row> {
        self.lines.into_iter().map(|line| line.row).collect()
    }
}

/// A state machine for validating v1 or v3 line protocol and updating
/// the [`Catalog`] with new tables or schema changes.
#[derive(Debug)]
pub struct WriteValidator<State> {
    state: State,
}

impl WriteValidator<WithCatalog> {
    /// Initialize the [`WriteValidator`] by getting a handle to, or creating
    /// a handle to the [`DatabaseSchema`] for the given namespace name `db_name`.
    pub fn initialize(
        db_name: NamespaceName<'static>,
        catalog: Arc<Catalog>,
        time_now_ns: i64,
    ) -> Result<WriteValidator<WithCatalog>> {
        let db_schema = catalog.db_or_create(db_name.as_str())?;
        Ok(WriteValidator {
            state: WithCatalog {
                catalog,
                db_schema,
                time_now_ns,
            },
        })
    }

    /// Parse the incoming lines of line protocol and update the
    /// [`DatabaseSchema`] if:
    ///
    /// * A new table is being added
    /// * New fields or tags are being added to an existing table
    ///
    /// # Implementation Note
    ///
    /// If this function succeeds, then the catalog will receive an update, so
    /// steps following this should be infallible.
    pub fn parse_lines_and_update_schema(
        mut self,
        lp: &str,
        accept_partial: bool,
        ingest_time: Time,
        precision: Precision,
    ) -> Result<WriteValidator<LinesParsed>> {
        let mut errors = vec![];
        let mut lp_lines = lp.lines();
        let mut lines = vec![];
        let mut bytes = 0;
        let mut catalog_updates = vec![];
        let mut schema = Cow::Borrowed(self.state.db_schema.as_ref());

        for (line_idx, maybe_line) in parse_lines(lp).enumerate() {
            let (qualified_line, catalog_op) = match maybe_line
                .map_err(|e| WriteLineError {
                    // This unwrap is fine because we're moving line by line
                    // alongside the output from parse_lines
                    original_line: lp_lines.next().unwrap().to_string(),
                    line_number: line_idx + 1,
                    error_message: e.to_string(),
                })
                .and_then(|l| {
                    let raw_line = lp_lines.next().unwrap();
                    validate_and_qualify_line(&mut schema, line_idx, l, ingest_time, precision)
                        .inspect(|_| bytes += raw_line.len() as u64)
                }) {
                Ok((qualified_line, catalog_op)) => (qualified_line, catalog_op),
                Err(e) => {
                    if !accept_partial {
                        return Err(Error::ParseError(e));
                    } else {
                        errors.push(e);
                    }
                    continue;
                }
            };
            if let Some(op) = catalog_op {
                catalog_updates.push(op);
            }
            // This unwrap is fine because we're moving line by line
            // alongside the output from parse_lines
            lines.push(qualified_line);
        }

        // All lines are parsed and validated, so all steps after this
        // are infallible, therefore, update the catalog if changes were
        // made to the schema:
        let catalog_batch = if catalog_updates.is_empty() {
            None
        } else {
            let catalog_batch = CatalogBatch {
                database_id: self.state.db_schema.id,
                time_ns: self.state.time_now_ns,
                database_name: Arc::clone(&self.state.db_schema.name),
                ops: catalog_updates,
            };
            let catalog_batch = self.state.catalog.apply_catalog_batch(&catalog_batch)?;
            // Update the schema used since we have applied new batches
            self.state.db_schema = self
                .state
                .catalog
                .db_schema_by_id(&self.state.db_schema.id)
                .expect("schema should exist");
            catalog_batch
        };

        let schema = self.state.db_schema.as_ref();

        // We now need to make sure lines are filled in with non null values for tags
        for line in lines.iter_mut() {
            // The table should exist if applied to the schema
            let table_schema = schema
                .table_definition_by_id(&line.table_id)
                .expect("table should exist in schema by this point");
            for tag_id in table_schema.series_key.iter() {
                if !line.row.fields.iter().any(|field| field.id == *tag_id) {
                    line.row.fields.push(Field {
                        id: *tag_id,
                        value: FieldData::Tag(String::new()),
                    });
                }
            }
        }

        Ok(WriteValidator {
            state: LinesParsed {
                catalog: self.state,
                lines,
                errors,
                bytes,
                catalog_batch,
            },
        })
    }
}

/// Type alias for storing new columns added by a write
type ColumnTracker = Vec<(ColumnId, Arc<str>, InfluxColumnType)>;

/// Validate a line of line protocol against the given schema definition
///
/// This is for scenarios where a write comes in for a table that exists, but may have
/// invalid field types, based on the pre-existing schema.
fn validate_and_qualify_line(
    db_schema: &mut Cow<'_, DatabaseSchema>,
    line_number: usize,
    line: ParsedLine<'_>,
    ingest_time: Time,
    precision: Precision,
) -> Result<(QualifiedLine, Option<CatalogOp>), WriteLineError> {
    let mut catalog_op = None;
    let table_name = line.series.measurement.as_str();
    let mut fields = Vec::with_capacity(line.column_count());
    let mut index_count = 0;
    let mut field_count = 0;
    let qualified = if let Some(table_def) = db_schema.table_definition(table_name) {
        // This table already exists, so update with any new columns if present:
        let mut columns = ColumnTracker::with_capacity(line.column_count() + 1);
        if let Some(tag_set) = &line.series.tag_set {
            for (tag_key, tag_val) in tag_set {
                if let Some(col_id) = table_def.column_name_to_id(tag_key.as_str()) {
                    fields.push(Field::new(col_id, FieldData::Tag(tag_val.to_string())));
                } else {
                    let col_id = ColumnId::new();
                    fields.push(Field::new(col_id, FieldData::Tag(tag_val.to_string())));
                    columns.push((col_id, tag_key.as_str().into(), InfluxColumnType::Tag));
                }
                index_count += 1;
            }
        }
        for (field_name, field_val) in line.field_set.iter() {
            // This field already exists, so check the incoming type matches existing type:
            if let Some((col_id, col_def)) = table_def.column_id_and_definition(field_name.as_str())
            {
                let field_col_type = influx_column_type_from_field_value(field_val);
                let existing_col_type = col_def.data_type;
                if field_col_type != existing_col_type {
                    let field_name = field_name.to_string();
                    return Err(WriteLineError {
                        original_line: line.to_string(),
                        line_number: line_number + 1,
                        error_message: format!(
                            "invalid field value in line protocol for field '{field_name}' on line \
                            {line_number}: expected type {expected}, but got {got}",
                            expected = existing_col_type,
                            got = field_col_type,
                        ),
                    });
                }
                fields.push(Field::new(col_id, field_val));
            } else {
                let col_id = ColumnId::new();
                columns.push((
                    col_id,
                    Arc::from(field_name.as_str()),
                    influx_column_type_from_field_value(field_val),
                ));
                fields.push(Field::new(col_id, field_val));
            }
            field_count += 1;
        }

        let time_col_id = table_def
            .column_name_to_id(TIME_COLUMN_NAME)
            .unwrap_or_else(|| {
                let col_id = ColumnId::new();
                columns.push((
                    col_id,
                    Arc::from(TIME_COLUMN_NAME),
                    InfluxColumnType::Timestamp,
                ));
                col_id
            });
        let timestamp_ns = line
            .timestamp
            .map(|ts| apply_precision_to_timestamp(precision, ts))
            .unwrap_or(ingest_time.timestamp_nanos());

        fields.push(Field::new(time_col_id, FieldData::Timestamp(timestamp_ns)));

        // if we have new columns defined, add them to the db_schema table so that subsequent lines
        // won't try to add the same definitions. Collect these additions into a catalog op, which
        // will be applied to the catalog with any other ops after all lines in the write request
        // have been parsed and validated.
        if !columns.is_empty() {
            let database_name = Arc::clone(&db_schema.name);
            let database_id = db_schema.id;
            let table_name: Arc<str> = Arc::clone(&table_def.table_name);
            let table_id = table_def.table_id;

            let mut field_definitions = Vec::with_capacity(columns.len());
            for (id, name, influx_type) in &columns {
                field_definitions.push(FieldDefinition::new(*id, Arc::clone(name), influx_type));
            }

            let db_schema = db_schema.to_mut();
            let mut new_table_def = db_schema
                .tables
                .get_mut(&table_id)
                // unwrap is safe due to the surrounding if let condition:
                .unwrap()
                .as_ref()
                .clone();
            new_table_def
                .add_columns(columns)
                .map_err(|e| WriteLineError {
                    original_line: line.to_string(),
                    line_number: line_number + 1,
                    error_message: e.to_string(),
                })?;
            db_schema
                .insert_table(table_id, Arc::new(new_table_def))
                .map_err(|e| WriteLineError {
                    original_line: line.to_string(),
                    line_number: line_number + 1,
                    error_message: e.to_string(),
                })?;

            catalog_op = Some(CatalogOp::AddFields(FieldAdditions {
                database_name,
                database_id,
                table_id,
                table_name,
                field_definitions,
            }));
        }
        QualifiedLine {
            table_id: table_def.table_id,
            row: Row {
                time: timestamp_ns,
                fields,
            },
            index_count,
            field_count,
        }
    } else {
        let table_id = TableId::new();
        // This is a new table, so build up its columns:
        let mut columns = Vec::new();
        let mut key = Vec::new();
        if let Some(tag_set) = &line.series.tag_set {
            for (tag_key, tag_val) in tag_set {
                let col_id = ColumnId::new();
                fields.push(Field::new(col_id, FieldData::Tag(tag_val.to_string())));
                columns.push((col_id, Arc::from(tag_key.as_str()), InfluxColumnType::Tag));
                // Build up the series key from the tags
                key.push(col_id);
                index_count += 1;
            }
        }
        for (field_name, field_val) in &line.field_set {
            let col_id = ColumnId::new();
            columns.push((
                col_id,
                Arc::from(field_name.as_str()),
                influx_column_type_from_field_value(field_val),
            ));
            fields.push(Field::new(col_id, field_val));
            field_count += 1;
        }
        // Always add time last on new table:
        let time_col_id = ColumnId::new();
        columns.push((
            time_col_id,
            Arc::from(TIME_COLUMN_NAME),
            InfluxColumnType::Timestamp,
        ));
        let timestamp_ns = line
            .timestamp
            .map(|ts| apply_precision_to_timestamp(precision, ts))
            .unwrap_or(ingest_time.timestamp_nanos());
        fields.push(Field::new(time_col_id, FieldData::Timestamp(timestamp_ns)));

        let table_name = table_name.into();
        let mut field_definitions = Vec::with_capacity(columns.len());

        for (id, name, influx_type) in &columns {
            field_definitions.push(FieldDefinition::new(*id, Arc::clone(name), influx_type));
        }
        catalog_op = Some(CatalogOp::CreateTable(influxdb3_wal::WalTableDefinition {
            table_id,
            database_id: db_schema.id,
            database_name: Arc::clone(&db_schema.name),
            table_name: Arc::clone(&table_name),
            field_definitions,
            key: key.clone(),
        }));

        let table = TableDefinition::new(table_id, Arc::clone(&table_name), columns, key).unwrap();

        let db_schema = db_schema.to_mut();
        db_schema
            .insert_table(table_id, Arc::new(table))
            .map_err(|e| WriteLineError {
                original_line: line.to_string(),
                line_number: line_number + 1,
                error_message: e.to_string(),
            })?
            .map_or_else(
                || Ok(()),
                |_| {
                    Err(WriteLineError {
                        original_line: line.to_string(),
                        line_number: line_number + 1,
                        error_message: "unexpected overwrite of existing table".to_string(),
                    })
                },
            )?;
        QualifiedLine {
            table_id,
            row: Row {
                time: timestamp_ns,
                fields,
            },
            index_count,
            field_count,
        }
    };

    Ok((qualified, catalog_op))
}

/// Result of conversion from line protocol to valid chunked data
/// for the buffer.
#[derive(Debug)]
pub struct ValidatedLines {
    /// Number of lines passed in
    pub(crate) line_count: usize,
    /// Number of bytes of all valid lines written
    pub(crate) valid_bytes_count: u64,
    /// Number of fields passed in
    pub(crate) field_count: usize,
    /// Number of index columns passed in, whether tags (v1) or series keys (v3)
    pub(crate) index_count: usize,
    /// Any errors that occurred while parsing the lines
    pub errors: Vec<WriteLineError>,
    /// Only valid lines will be converted into a WriteBatch
    pub valid_data: WriteBatch,
    /// If any catalog updates were made, they will be included here
    pub(crate) catalog_updates: Option<OrderedCatalogBatch>,
}

impl From<ValidatedLines> for WriteBatch {
    fn from(value: ValidatedLines) -> Self {
        value.valid_data
    }
}

impl WriteValidator<LinesParsed> {
    /// Convert this into the inner [`LinesParsed`]
    ///
    /// This is mainly used for testing
    pub fn into_inner(self) -> LinesParsed {
        self.state
    }

    /// Convert a set of valid parsed lines to a [`ValidatedLines`] which will
    /// be buffered and written to the WAL, if configured.
    ///
    /// This involves splitting out the writes into different batches for each chunk, which will
    /// map to the `Gen1Duration`. This function should be infallible, because
    /// the schema for incoming writes has been fully validated.
    pub fn convert_lines_to_buffer(self, gen1_duration: Gen1Duration) -> ValidatedLines {
        let mut table_chunks = IndexMap::new();
        let line_count = self.state.lines.len();
        let mut field_count = 0;
        let mut index_count = 0;

        for line in self.state.lines.into_iter() {
            field_count += line.field_count;
            index_count += line.index_count;

            convert_qualified_line(line, &mut table_chunks, gen1_duration);
        }

        let write_batch = WriteBatch::new(
            self.state.catalog.db_schema.id,
            Arc::clone(&self.state.catalog.db_schema.name),
            table_chunks,
        );

        ValidatedLines {
            line_count,
            valid_bytes_count: self.state.bytes,
            field_count,
            index_count,
            errors: self.state.errors,
            valid_data: write_batch,
            catalog_updates: self.state.catalog_batch,
        }
    }
}

fn convert_qualified_line(
    line: QualifiedLine,
    table_chunk_map: &mut IndexMap<TableId, TableChunks>,
    gen1_duration: Gen1Duration,
) {
    // Add the row into the correct chunk in the table
    let chunk_time = gen1_duration.chunk_time_for_timestamp(Timestamp::new(line.row.time));
    let table_chunks = table_chunk_map.entry(line.table_id).or_default();
    table_chunks.push_row(chunk_time, line.row);
}

#[derive(Debug)]
struct QualifiedLine {
    table_id: TableId,
    row: Row,
    index_count: usize,
    field_count: usize,
}

fn apply_precision_to_timestamp(precision: Precision, ts: i64) -> i64 {
    let multiplier = match precision {
        Precision::Auto => match crate::guess_precision(ts) {
            Precision::Second => 1_000_000_000,
            Precision::Millisecond => 1_000_000,
            Precision::Microsecond => 1_000,
            Precision::Nanosecond => 1,

            Precision::Auto => unreachable!(),
        },
        Precision::Second => 1_000_000_000,
        Precision::Millisecond => 1_000_000,
        Precision::Microsecond => 1_000,
        Precision::Nanosecond => 1,
    };

    ts * multiplier
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::WriteValidator;
    use crate::{write_buffer::Error, Precision};

    use data_types::NamespaceName;
    use influxdb3_catalog::catalog::Catalog;
    use influxdb3_id::TableId;
    use influxdb3_wal::Gen1Duration;
    use iox_time::Time;

    #[test]
    fn write_validator_v1() -> Result<(), Error> {
        let node_id = Arc::from("sample-host-id");
        let instance_id = Arc::from("sample-instance-id");
        let namespace = NamespaceName::new("test").unwrap();
        let catalog = Arc::new(Catalog::new(node_id, instance_id));
        let result = WriteValidator::initialize(namespace.clone(), Arc::clone(&catalog), 0)
            .unwrap()
            .parse_lines_and_update_schema(
                "cpu,tag1=foo val1=\"bar\" 1234",
                false,
                Time::from_timestamp_nanos(0),
                Precision::Auto,
            )
            .unwrap()
            .convert_lines_to_buffer(Gen1Duration::new_5m());

        assert_eq!(result.line_count, 1);
        assert_eq!(result.field_count, 1);
        assert_eq!(result.index_count, 1);
        assert!(result.errors.is_empty());

        assert_eq!(result.valid_data.database_name.as_ref(), namespace.as_str());
        // cpu table
        let batch = result
            .valid_data
            .table_chunks
            .get(&TableId::from(0))
            .unwrap();
        assert_eq!(batch.row_count(), 1);

        // Validate another write, the result should be very similar, but now the catalog
        // has the table/columns added, so it will excercise a different code path:
        let result = WriteValidator::initialize(namespace.clone(), Arc::clone(&catalog), 0)
            .unwrap()
            .parse_lines_and_update_schema(
                "cpu,tag1=foo val1=\"bar\" 1235",
                false,
                Time::from_timestamp_nanos(0),
                Precision::Auto,
            )
            .unwrap()
            .convert_lines_to_buffer(Gen1Duration::new_5m());

        println!("result: {result:?}");
        assert_eq!(result.line_count, 1);
        assert_eq!(result.field_count, 1);
        assert_eq!(result.index_count, 1);
        assert!(result.errors.is_empty());

        // Validate another write, this time adding a new field:
        let result = WriteValidator::initialize(namespace.clone(), Arc::clone(&catalog), 0)
            .unwrap()
            .parse_lines_and_update_schema(
                "cpu,tag1=foo val1=\"bar\",val2=false 1236",
                false,
                Time::from_timestamp_nanos(0),
                Precision::Auto,
            )
            .unwrap()
            .convert_lines_to_buffer(Gen1Duration::new_5m());

        println!("result: {result:?}");
        assert_eq!(result.line_count, 1);
        assert_eq!(result.field_count, 2);
        assert_eq!(result.index_count, 1);
        assert!(result.errors.is_empty());

        Ok(())
    }

    #[test]
    fn tags_are_filled_in() -> Result<(), Error> {
        let node_id = Arc::from("sample-host-id");
        let instance_id = Arc::from("sample-instance-id");
        let namespace = NamespaceName::new("test").unwrap();
        let catalog = Arc::new(Catalog::new(node_id, instance_id));
        let result = WriteValidator::initialize(namespace.clone(), Arc::clone(&catalog), 0)
            .unwrap()
            .parse_lines_and_update_schema(
                "cpu,tag1=foo val=0\n\
                cpu,tag2=bar val=1\n\
                cpu,tag1=baz,tag2=baq val=1\n",
                false,
                Time::from_timestamp_nanos(0),
                Precision::Auto,
            )
            .unwrap();
        // Check that the tag exists for every line
        for line in result.state.lines {
            let Some(table_schema) = result
                .state
                .catalog
                .db_schema
                .table_definition_by_id(&line.table_id)
            else {
                panic!("Table not found: {}", line.table_id);
            };
            for tag_id in table_schema.series_key.iter() {
                if !line.row.fields.iter().any(|field| field.id == *tag_id) {
                    panic!("Tag not found: {tag_id}");
                }
            }
        }
        Ok(())
    }
}
