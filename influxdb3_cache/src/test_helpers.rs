use std::sync::Arc;

use influxdb3_catalog::catalog::{Catalog, DatabaseSchema, TableDefinition};
use influxdb3_id::ColumnId;
use influxdb3_wal::{Gen1Duration, Row, WriteBatch};
use influxdb3_write::write_buffer::validator::WriteValidator;
use iox_time::Time;

#[derive(Debug)]
pub(crate) struct TestWriter {
    catalog: Arc<Catalog>,
}

impl TestWriter {
    const DB_NAME: &str = "test_db";

    pub(crate) fn new() -> Self {
        Self {
            catalog: Arc::new(Catalog::new("test-host".into(), "test-instance".into())),
        }
    }

    pub(crate) fn write_lp_to_rows(&self, lp: impl AsRef<str>, time_ns: i64) -> Vec<Row> {
        let lines_parsed = WriteValidator::initialize(
            Self::DB_NAME.try_into().unwrap(),
            Arc::clone(&self.catalog),
            time_ns,
        )
        .expect("initialize write validator")
        .parse_lines_and_update_schema(
            lp.as_ref(),
            false,
            Time::from_timestamp_nanos(time_ns),
            influxdb3_write::Precision::Nanosecond,
        )
        .expect("parse and validate v1 line protocol");

        lines_parsed.into_inner().to_rows()
    }

    pub(crate) fn write_lp_to_write_batch(&self, lp: impl AsRef<str>, time_ns: i64) -> WriteBatch {
        WriteValidator::initialize(
            Self::DB_NAME.try_into().unwrap(),
            Arc::clone(&self.catalog),
            time_ns,
        )
        .expect("initialize write validator")
        .parse_lines_and_update_schema(
            lp.as_ref(),
            false,
            Time::from_timestamp_nanos(time_ns),
            influxdb3_write::Precision::Nanosecond,
        )
        .expect("parse and validate v1 line protocol")
        .convert_lines_to_buffer(Gen1Duration::new_1m())
        .into()
    }

    pub(crate) fn catalog(&self) -> Arc<Catalog> {
        Arc::clone(&self.catalog)
    }

    pub(crate) fn db_schema(&self) -> Arc<DatabaseSchema> {
        self.catalog
            .db_schema(Self::DB_NAME)
            .expect("db schema should be initialized")
    }
}

/// Convert a list of column names to their respective [`ColumnId`]s in the given [`TableDefinition`]
pub(crate) fn column_ids_for_names(
    names: impl IntoIterator<Item: AsRef<str>>,
    table_def: &TableDefinition,
) -> Vec<ColumnId> {
    names
        .into_iter()
        .map(|name| table_def.column_name_to_id_unchecked(name.as_ref()))
        .collect()
}
