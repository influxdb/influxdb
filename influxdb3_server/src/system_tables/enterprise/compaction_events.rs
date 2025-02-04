use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::{Schema, SchemaRef};
use datafusion::{error::DataFusionError, prelude::Expr};
use influxdb3_enterprise_compactor::sys_events::{CompactionEvent, CompactionEventStore};
use influxdb3_sys_events::{SysEventStore, ToRecordBatch};
use iox_system_tables::IoxSystemTable;
use observability_deps::tracing::debug;

#[derive(Debug)]
pub(crate) struct CompactionEventsSysTable {
    compaction_events_store: Arc<dyn CompactionEventStore>,
    schema: Arc<Schema>,
}

impl CompactionEventsSysTable {
    pub(crate) fn new(sys_events_store: Arc<SysEventStore>) -> Self {
        Self {
            compaction_events_store: sys_events_store as Arc<dyn CompactionEventStore>,
            schema: Arc::new(CompactionEvent::schema()),
        }
    }
}

#[async_trait::async_trait]
impl IoxSystemTable for CompactionEventsSysTable {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    async fn scan(
        &self,
        _filters: Option<Vec<Expr>>,
        _limit: Option<usize>,
    ) -> Result<RecordBatch, DataFusionError> {
        let maybe_rec_batch = self
            .compaction_events_store
            .compaction_events_as_record_batch();

        debug!(
            ?maybe_rec_batch,
            "System table for snapshot fetched events query"
        );
        maybe_rec_batch
            .unwrap_or_else(|| Ok(RecordBatch::new_empty(Arc::clone(&self.schema))))
            .map_err(|err| DataFusionError::ArrowError(err, None))
    }
}
