//! Implementation of the Catalog that sits entirely in memory.

use crate::catalog::Error::{
    CatalogUpdatedElsewhere, ProcessingEngineCallExists, ProcessingEngineTriggerExists,
    ProcessingEngineTriggerRunning, TableNotFound,
};
use bimap::{BiHashMap, Overwritten};
use hashbrown::HashMap;
use indexmap::IndexMap;
use influxdb3_id::{ColumnId, DbId, SerdeVecMap, TableId};
use influxdb3_wal::{
    CatalogBatch, CatalogOp, DeleteDatabaseDefinition, DeletePluginDefinition,
    DeleteTableDefinition, DeleteTriggerDefinition, DistinctCacheDefinition, DistinctCacheDelete,
    FieldAdditions, FieldDefinition, LastCacheDefinition, LastCacheDelete, OrderedCatalogBatch,
    PluginDefinition, TriggerDefinition, TriggerIdentifier,
};
use influxdb_line_protocol::FieldValue;
use iox_time::Time;
use observability_deps::tracing::{debug, info, warn};
use parking_lot::RwLock;
use schema::{InfluxColumnType, InfluxFieldType, Schema, SchemaBuilder};
use serde::{Deserialize, Serialize};
use std::borrow::Cow;
use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::sync::Arc;
use thiserror::Error;

const SOFT_DELETION_TIME_FORMAT: &str = "%Y%m%dT%H%M%S";

#[derive(Debug, Error, Clone)]
pub enum Error {
    #[error("catalog updated elsewhere")]
    CatalogUpdatedElsewhere,

    #[error(
        "Update to schema would exceed number of columns per table limit of {} columns",
        Catalog::NUM_COLUMNS_PER_TABLE_LIMIT - 1
    )]
    TooManyColumns,

    #[error(
        "Update to schema would exceed number of tables limit of {} tables",
        Catalog::NUM_TABLES_LIMIT
    )]
    TooManyTables,

    #[error(
        "Adding a new database would exceed limit of {} databases",
        Catalog::NUM_DBS_LIMIT
    )]
    TooManyDbs,

    #[error("Table {} not in DB schema for {}", table_name, db_name)]
    TableNotFound {
        db_name: Arc<str>,
        table_name: Arc<str>,
    },

    #[error(
        "Field type mismatch on table {} column {}. Existing column is {} but attempted to add {}",
        table_name,
        column_name,
        existing,
        attempted
    )]
    FieldTypeMismatch {
        table_name: String,
        column_name: String,
        existing: InfluxColumnType,
        attempted: InfluxColumnType,
    },

    #[error(
        "Series key mismatch on table {}. Existing table has {}",
        table_name,
        existing
    )]
    SeriesKeyMismatch {
        table_name: String,
        existing: String,
    },

    #[error(
        "Cannot overwrite Processing Engine Call {} in Database {}",
        call_name,
        database_name
    )]
    ProcessingEngineCallExists {
        database_name: String,
        call_name: String,
    },
    #[error(
        "Cannot overwrite Processing Engine Trigger {} in Database {}",
        trigger_name,
        database_name
    )]
    ProcessingEngineTriggerExists {
        database_name: String,
        trigger_name: String,
    },

    #[error(
        "Cannot delete running plugin {}. Disable it first or use --force.",
        trigger_name
    )]
    ProcessingEngineTriggerRunning { trigger_name: String },

    #[error(
        "Cannot delete plugin {} in database {} because it is used by trigger {}",
        plugin_name,
        database_name,
        trigger_name
    )]
    ProcessingEnginePluginInUse {
        database_name: String,
        plugin_name: String,
        trigger_name: String,
    },

    #[error(
        "Processing Engine Plugin {} not in DB schema for {}",
        plugin_name,
        database_name
    )]
    ProcessingEnginePluginNotFound {
        plugin_name: String,
        database_name: String,
    },
    #[error("Processing Engine Unimplemented: {}", feature_description)]
    ProcessingEngineUnimplemented { feature_description: String },

    #[error(
        "Processing Engine Trigger {} not in DB {}",
        trigger_name,
        database_name
    )]
    ProcessingEngineTriggerNotFound {
        database_name: String,
        trigger_name: String,
    },
    #[error("failed to parse trigger from {}", trigger_spec)]
    ProcessingEngineTriggerSpecParseError { trigger_spec: String },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

pub const TIME_COLUMN_NAME: &str = "time";

/// The sequence number of a batch of WAL operations.
#[derive(
    Debug, Default, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
pub struct CatalogSequenceNumber(u32);

impl CatalogSequenceNumber {
    pub fn new(id: u32) -> Self {
        Self(id)
    }

    pub fn next(&self) -> Self {
        Self(self.0 + 1)
    }

    pub fn as_u32(&self) -> u32 {
        self.0
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub struct Catalog {
    #[serde(flatten)]
    inner: RwLock<InnerCatalog>,
}

impl PartialEq for Catalog {
    fn eq(&self, other: &Self) -> bool {
        self.inner.read().eq(&other.inner.read())
    }
}

impl Catalog {
    /// Limit for the number of Databases that InfluxDB 3 Core OSS can have
    pub(crate) const NUM_DBS_LIMIT: usize = 5;
    /// Limit for the number of columns per table that InfluxDB 3 Core OSS can have
    pub(crate) const NUM_COLUMNS_PER_TABLE_LIMIT: usize = 500;
    /// Limit for the number of tables across all DBs that InfluxDB 3 Core OSS can have
    pub(crate) const NUM_TABLES_LIMIT: usize = 2000;

    pub fn new(host_id: Arc<str>, instance_id: Arc<str>) -> Self {
        Self {
            inner: RwLock::new(InnerCatalog::new(host_id, instance_id)),
        }
    }

    pub fn from_inner(inner: InnerCatalog) -> Self {
        Self {
            inner: RwLock::new(inner),
        }
    }

    pub fn apply_catalog_batch(
        &self,
        catalog_batch: &CatalogBatch,
    ) -> Result<Option<OrderedCatalogBatch>> {
        self.inner.write().apply_catalog_batch(catalog_batch)
    }

    // Checks the sequence number to see if it needs to be applied.
    pub fn apply_ordered_catalog_batch(
        &self,
        batch: &OrderedCatalogBatch,
    ) -> Result<Option<CatalogBatch>> {
        if batch.sequence_number() >= self.sequence_number().as_u32() {
            if let Some(catalog_batch) = self.apply_catalog_batch(batch.batch())? {
                return Ok(Some(catalog_batch.batch().clone()));
            }
        }
        Ok(None)
    }

    pub fn db_or_create(&self, db_name: &str) -> Result<Arc<DatabaseSchema>> {
        let db = match self.db_schema(db_name) {
            Some(db) => db,
            None => {
                let mut inner = self.inner.write();

                if inner.database_count() >= Self::NUM_DBS_LIMIT {
                    return Err(Error::TooManyDbs);
                }

                info!("return new db {}", db_name);
                let db_id = DbId::new();
                let db_name = db_name.into();
                let db = Arc::new(DatabaseSchema::new(db_id, Arc::clone(&db_name)));
                inner.databases.insert(db.id, Arc::clone(&db));
                inner.sequence = inner.sequence.next();
                inner.updated = true;
                inner.db_map.insert(db_id, db_name);
                db
            }
        };

        Ok(db)
    }

    pub fn db_name_to_id(&self, db_name: &str) -> Option<DbId> {
        self.inner.read().db_map.get_by_right(db_name).copied()
    }

    pub fn db_id_to_name(&self, db_id: &DbId) -> Option<Arc<str>> {
        self.inner.read().db_map.get_by_left(db_id).map(Arc::clone)
    }

    pub fn db_schema(&self, db_name: &str) -> Option<Arc<DatabaseSchema>> {
        self.db_id_and_schema(db_name).map(|(_, schema)| schema)
    }

    pub fn db_schema_by_id(&self, db_id: &DbId) -> Option<Arc<DatabaseSchema>> {
        self.inner.read().databases.get(db_id).cloned()
    }

    pub fn db_id_and_schema(&self, db_name: &str) -> Option<(DbId, Arc<DatabaseSchema>)> {
        let inner = self.inner.read();
        let db_id = inner.db_map.get_by_right(db_name)?;
        inner
            .databases
            .get(db_id)
            .map(|db| (*db_id, Arc::clone(db)))
    }

    /// List names of databases that have not been deleted
    pub fn db_names(&self) -> Vec<String> {
        self.inner
            .read()
            .databases
            .values()
            .filter(|db| !db.deleted)
            .map(|db| db.name.to_string())
            .collect()
    }

    pub fn list_db_schema(&self) -> Vec<Arc<DatabaseSchema>> {
        self.inner.read().databases.values().cloned().collect()
    }

    pub fn sequence_number(&self) -> CatalogSequenceNumber {
        self.inner.read().sequence
    }

    pub fn clone_inner(&self) -> InnerCatalog {
        self.inner.read().clone()
    }

    pub fn instance_id(&self) -> Arc<str> {
        Arc::clone(&self.inner.read().instance_id)
    }

    pub fn host_id(&self) -> Arc<str> {
        Arc::clone(&self.inner.read().host_id)
    }

    #[cfg(test)]
    pub fn db_exists(&self, db_id: DbId) -> bool {
        self.inner.read().db_exists(db_id)
    }

    pub fn insert_database(&self, db: DatabaseSchema) {
        let mut inner = self.inner.write();
        inner.upsert_db(db);
    }

    pub fn is_updated(&self) -> bool {
        self.inner.read().updated
    }

    /// After the catalog has been persisted, mark it as not updated, if the sequence number
    /// matches. If it doesn't then the catalog was updated while persistence was running and
    /// will need to be persisted on the next snapshot.
    pub fn set_updated_false_if_sequence_matches(&self, sequence_number: CatalogSequenceNumber) {
        let mut inner = self.inner.write();
        if inner.sequence == sequence_number {
            inner.updated = false;
        }
    }

    pub fn triggers(&self) -> Vec<(String, String)> {
        let inner = self.inner.read();
        let result = inner
            .databases
            .values()
            .flat_map(|schema| {
                schema
                    .processing_engine_triggers
                    .iter()
                    .filter_map(move |(key, trigger)| {
                        if trigger.disabled {
                            None
                        } else {
                            Some((schema.name.to_string(), key.to_string()))
                        }
                    })
            })
            .collect();
        result
    }

    pub fn inner(&self) -> &RwLock<InnerCatalog> {
        &self.inner
    }
}

#[serde_with::serde_as]
#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone, Default)]
pub struct InnerCatalog {
    /// The catalog is a map of databases with their table schemas
    databases: SerdeVecMap<DbId, Arc<DatabaseSchema>>,
    sequence: CatalogSequenceNumber,
    /// The host_id is the prefix that is passed in when starting up (`host_identifier_prefix`)
    host_id: Arc<str>,
    /// The instance_id uniquely identifies the instance that generated the catalog
    instance_id: Arc<str>,
    /// If true, the catalog has been updated since the last time it was serialized
    #[serde(skip)]
    updated: bool,
    #[serde_as(as = "DbMapAsArray")]
    db_map: BiHashMap<DbId, Arc<str>>,
}

serde_with::serde_conv!(
    DbMapAsArray,
    BiHashMap<DbId, Arc<str>>,
    |map: &BiHashMap<DbId, Arc<str>>| {
        map.iter().fold(Vec::new(), |mut acc, (id, name)| {
            acc.push(DbMap {
                db_id: *id,
                name: Arc::clone(&name)
            });
            acc
        })
    },
    |vec: Vec<DbMap>| -> Result<_, std::convert::Infallible> {
        Ok(vec.into_iter().fold(BiHashMap::new(), |mut acc, db| {
            acc.insert(db.db_id, db.name);
            acc
        }))
    }
);

#[derive(Debug, Serialize, Deserialize)]
struct DbMap {
    db_id: DbId,
    name: Arc<str>,
}

#[derive(Debug, Serialize, Deserialize)]
struct TableMap {
    table_id: TableId,
    name: Arc<str>,
}

serde_with::serde_conv!(
    TableMapAsArray,
    BiHashMap<TableId, Arc<str>>,
    |map: &BiHashMap<TableId, Arc<str>>| {
        map.iter().fold(Vec::new(), |mut acc, (table_id, name)| {
            acc.push(TableMap {
                table_id: *table_id,
                name: Arc::clone(&name)
            });
            acc
        })
    },
    |vec: Vec<TableMap>| -> Result<_, std::convert::Infallible> {
        let mut map = BiHashMap::new();
        for item in vec {
            map.insert(item.table_id, item.name);
        }
        Ok(map)
    }
);

impl InnerCatalog {
    pub(crate) fn new(host_id: Arc<str>, instance_id: Arc<str>) -> Self {
        Self {
            databases: SerdeVecMap::new(),
            sequence: CatalogSequenceNumber::new(0),
            host_id,
            instance_id,
            updated: false,
            db_map: BiHashMap::new(),
        }
    }

    pub fn sequence_number(&self) -> CatalogSequenceNumber {
        self.sequence
    }

    pub fn database_count(&self) -> usize {
        self.databases.iter().filter(|db| !db.1.deleted).count()
    }

    pub fn table_count(&self) -> usize {
        self.databases.values().map(|db| db.table_count()).sum()
    }

    /// Applies the `CatalogBatch` while validating that all updates are compatible. If updates
    /// have already been applied, the sequence number and updated tracker are not updated.
    pub fn apply_catalog_batch(
        &mut self,
        catalog_batch: &CatalogBatch,
    ) -> Result<Option<OrderedCatalogBatch>> {
        let table_count = self.table_count();

        if let Some(db) = self.databases.get(&catalog_batch.database_id) {
            if let Some(new_db) = DatabaseSchema::new_if_updated_from_batch(db, catalog_batch)? {
                check_overall_table_count(Some(db), &new_db, table_count)?;
                self.upsert_db(new_db);
            } else {
                return Ok(None);
            }
        } else {
            if self.database_count() >= Catalog::NUM_DBS_LIMIT {
                return Err(Error::TooManyDbs);
            }
            let new_db = DatabaseSchema::new_from_batch(catalog_batch)?;
            check_overall_table_count(None, &new_db, table_count)?;
            self.upsert_db(new_db);
        }
        Ok(Some(OrderedCatalogBatch::new(
            catalog_batch.clone(),
            self.sequence.0,
        )))
    }

    pub fn db_exists(&self, db_id: DbId) -> bool {
        self.databases.contains_key(&db_id)
    }

    pub fn upsert_db(&mut self, db: DatabaseSchema) {
        let name = Arc::clone(&db.name);
        let id = db.id;
        self.databases.insert(id, Arc::new(db));
        self.set_updated_and_increment_sequence();
        self.db_map.insert(id, name);
    }

    fn set_updated_and_increment_sequence(&mut self) {
        self.sequence = self.sequence.next();
        self.updated = true;
    }
}

fn check_overall_table_count(
    existing_db: Option<&Arc<DatabaseSchema>>,
    new_db: &DatabaseSchema,
    current_table_count: usize,
) -> Result<()> {
    let existing_table_count = if let Some(existing_db) = existing_db {
        existing_db.table_count()
    } else {
        0
    };
    let new_table_count = new_db.table_count();
    match new_table_count.cmp(&existing_table_count) {
        Ordering::Less | Ordering::Equal => Ok(()),
        Ordering::Greater => {
            let newly_added_table_count = new_db.table_count() - existing_table_count;
            if current_table_count + newly_added_table_count > Catalog::NUM_TABLES_LIMIT {
                Err(Error::TooManyTables)
            } else {
                Ok(())
            }
        }
    }
}

#[derive(Debug, Eq, PartialEq, Clone)]
pub struct DatabaseSchema {
    pub id: DbId,
    pub name: Arc<str>,
    /// The database is a map of tables
    pub tables: SerdeVecMap<TableId, Arc<TableDefinition>>,
    pub table_map: BiHashMap<TableId, Arc<str>>,
    pub processing_engine_plugins: HashMap<String, PluginDefinition>,
    // TODO: care about performance of triggers
    pub processing_engine_triggers: HashMap<String, TriggerDefinition>,
    pub deleted: bool,
}

impl DatabaseSchema {
    pub fn new(id: DbId, name: Arc<str>) -> Self {
        Self {
            id,
            name,
            tables: Default::default(),
            table_map: BiHashMap::new(),
            processing_engine_plugins: HashMap::new(),
            processing_engine_triggers: HashMap::new(),
            deleted: false,
        }
    }

    pub fn table_count(&self) -> usize {
        self.tables.iter().filter(|table| !table.1.deleted).count()
    }

    /// Validates the updates in the `CatalogBatch` are compatible with this schema. If
    /// everything is compatible and there are no updates to the existing schema, None will be
    /// returned, otherwise a new `DatabaseSchema` will be returned with the updates applied.
    pub fn new_if_updated_from_batch(
        db_schema: &DatabaseSchema,
        catalog_batch: &CatalogBatch,
    ) -> Result<Option<Self>> {
        debug!(name = ?db_schema.name, deleted = ?db_schema.deleted, full_batch = ?catalog_batch, "Updating / adding to catalog");

        let mut schema = Cow::Borrowed(db_schema);

        for catalog_op in &catalog_batch.ops {
            schema = catalog_op.update_schema(schema)?;
        }
        // If there were updates then it will have become owned, so we should return the new schema.
        if let Cow::Owned(schema) = schema {
            Ok(Some(schema))
        } else {
            Ok(None)
        }
    }

    pub fn new_from_batch(catalog_batch: &CatalogBatch) -> Result<Self> {
        let db_schema = Self::new(
            catalog_batch.database_id,
            Arc::clone(&catalog_batch.database_name),
        );

        // We need to special case when we create a DB via the commandline as
        // this will only contain one op to create a database. If we don't
        // startup of the database will fail
        if catalog_batch.ops.len() == 1 {
            if let CatalogOp::CreateDatabase(_) = catalog_batch.ops[0] {
                return Ok(db_schema);
            }
        }

        let new_db = DatabaseSchema::new_if_updated_from_batch(&db_schema, catalog_batch)?
            .expect("database must be new");
        Ok(new_db)
    }

    /// Insert a [`TableDefinition`] to the `tables` map and also update the `table_map`
    pub fn insert_table(
        &mut self,
        table_id: TableId,
        table_def: Arc<TableDefinition>,
    ) -> Result<Option<Arc<TableDefinition>>> {
        match self
            .table_map
            .insert(table_id, Arc::clone(&table_def.table_name))
        {
            Overwritten::Left(_, _) | Overwritten::Right(_, _) | Overwritten::Both(_, _) => {
                // This will happen if another table was inserted with the same name between checking
                // for existence and insertion.
                // We'd like this to be automatically handled by the system,
                // but for now it is better to error than get into an inconsistent state.
                return Err(CatalogUpdatedElsewhere);
            }
            Overwritten::Neither | Overwritten::Pair(_, _) => {}
        }
        Ok(self.tables.insert(table_id, table_def))
    }

    pub fn table_schema(&self, table_name: impl Into<Arc<str>>) -> Option<Schema> {
        self.table_id_and_schema(table_name)
            .map(|(_, schema)| schema.clone())
    }

    pub fn table_schema_by_id(&self, table_id: &TableId) -> Option<Schema> {
        self.tables
            .get(table_id)
            .map(|table| table.influx_schema())
            .cloned()
    }

    pub fn table_id_and_schema(
        &self,
        table_name: impl Into<Arc<str>>,
    ) -> Option<(TableId, Schema)> {
        self.table_map
            .get_by_right(&table_name.into())
            .and_then(|table_id| {
                self.tables
                    .get(table_id)
                    .map(|table_def| (*table_id, table_def.influx_schema().clone()))
            })
    }

    pub fn table_definition(
        &self,
        table_name: impl Into<Arc<str>>,
    ) -> Option<Arc<TableDefinition>> {
        self.table_map
            .get_by_right(&table_name.into())
            .and_then(|table_id| self.tables.get(table_id).cloned())
    }

    pub fn table_definition_by_id(&self, table_id: &TableId) -> Option<Arc<TableDefinition>> {
        self.tables.get(table_id).cloned()
    }

    pub fn table_id_and_definition(
        &self,
        table_name: impl Into<Arc<str>>,
    ) -> Option<(TableId, Arc<TableDefinition>)> {
        let table_id = self.table_map.get_by_right(&table_name.into())?;
        self.tables
            .get(table_id)
            .map(|table_def| (*table_id, Arc::clone(table_def)))
    }

    pub fn table_ids(&self) -> Vec<TableId> {
        self.tables.keys().cloned().collect()
    }

    pub fn table_names(&self) -> Vec<Arc<str>> {
        self.tables
            .values()
            .map(|td| Arc::clone(&td.table_name))
            .collect()
    }

    pub fn table_exists(&self, table_id: &TableId) -> bool {
        self.tables.contains_key(table_id)
    }

    pub fn tables(&self) -> impl Iterator<Item = Arc<TableDefinition>> + use<'_> {
        self.tables.values().map(Arc::clone)
    }

    pub fn table_name_to_id(&self, table_name: impl Into<Arc<str>>) -> Option<TableId> {
        self.table_map.get_by_right(&table_name.into()).copied()
    }

    pub fn table_id_to_name(&self, table_id: &TableId) -> Option<Arc<str>> {
        self.table_map.get_by_left(table_id).map(Arc::clone)
    }
}

trait UpdateDatabaseSchema {
    fn update_schema<'a>(&self, schema: Cow<'a, DatabaseSchema>)
        -> Result<Cow<'a, DatabaseSchema>>;
}

impl UpdateDatabaseSchema for CatalogOp {
    fn update_schema<'a>(
        &self,
        schema: Cow<'a, DatabaseSchema>,
    ) -> Result<Cow<'a, DatabaseSchema>> {
        match &self {
            CatalogOp::CreateDatabase(_) => Ok(schema),
            CatalogOp::CreateTable(create_table) => create_table.update_schema(schema),
            CatalogOp::AddFields(field_additions) => field_additions.update_schema(schema),
            CatalogOp::CreateDistinctCache(distinct_cache_definition) => {
                distinct_cache_definition.update_schema(schema)
            }
            CatalogOp::DeleteDistinctCache(delete_distinct_cache) => {
                delete_distinct_cache.update_schema(schema)
            }
            CatalogOp::CreateLastCache(create_last_cache) => {
                create_last_cache.update_schema(schema)
            }
            CatalogOp::DeleteLastCache(delete_last_cache) => {
                delete_last_cache.update_schema(schema)
            }
            CatalogOp::DeleteDatabase(delete_database) => delete_database.update_schema(schema),
            CatalogOp::DeleteTable(delete_table) => delete_table.update_schema(schema),
            CatalogOp::DeletePlugin(delete_plugin) => delete_plugin.update_schema(schema),
            CatalogOp::CreatePlugin(create_plugin) => create_plugin.update_schema(schema),
            CatalogOp::CreateTrigger(create_trigger) => create_trigger.update_schema(schema),
            CatalogOp::DeleteTrigger(delete_trigger) => delete_trigger.update_schema(schema),
            CatalogOp::EnableTrigger(trigger_identifier) => {
                EnableTrigger(trigger_identifier.clone()).update_schema(schema)
            }
            CatalogOp::DisableTrigger(trigger_identifier) => {
                DisableTrigger(trigger_identifier.clone()).update_schema(schema)
            }
        }
    }
}

impl UpdateDatabaseSchema for influxdb3_wal::TableDefinition {
    fn update_schema<'a>(
        &self,
        mut database_schema: Cow<'a, DatabaseSchema>,
    ) -> Result<Cow<'a, DatabaseSchema>> {
        match database_schema.tables.get(&self.table_id) {
            Some(existing_table) => {
                if let Cow::Owned(updated_table) = existing_table.check_and_add_new_fields(self)? {
                    database_schema
                        .to_mut()
                        .insert_table(self.table_id, Arc::new(updated_table))?;
                }
            }
            None => {
                let new_table = TableDefinition::new_from_op(self);
                database_schema
                    .to_mut()
                    .insert_table(new_table.table_id, Arc::new(new_table))?;
            }
        }
        Ok(database_schema)
    }
}

impl UpdateDatabaseSchema for DeleteDatabaseDefinition {
    fn update_schema<'a>(
        &self,
        mut schema: Cow<'a, DatabaseSchema>,
    ) -> Result<Cow<'a, DatabaseSchema>> {
        let deletion_time = Time::from_timestamp_nanos(self.deletion_time);
        let owned = schema.to_mut();
        owned.name = make_new_name_using_deleted_time(&self.database_name, deletion_time);
        owned.deleted = true;
        Ok(schema)
    }
}

impl UpdateDatabaseSchema for DeleteTableDefinition {
    fn update_schema<'a>(
        &self,
        mut schema: Cow<'a, DatabaseSchema>,
    ) -> Result<Cow<'a, DatabaseSchema>> {
        // unlike other table ops, this is not an error.
        if !schema.tables.contains_key(&self.table_id) {
            return Ok(schema);
        }
        let mut_schema = schema.to_mut();
        if let Some(deleted_table) = mut_schema.tables.get_mut(&self.table_id) {
            let deletion_time = Time::from_timestamp_nanos(self.deletion_time);
            let table_name = make_new_name_using_deleted_time(&self.table_name, deletion_time);
            let new_table_def = Arc::make_mut(deleted_table);
            new_table_def.deleted = true;
            new_table_def.table_name = table_name;
            mut_schema.table_map.insert(
                new_table_def.table_id,
                Arc::clone(&new_table_def.table_name),
            );
        }
        Ok(schema)
    }
}

impl UpdateDatabaseSchema for DeletePluginDefinition {
    fn update_schema<'a>(
        &self,
        mut schema: Cow<'a, DatabaseSchema>,
    ) -> Result<Cow<'a, DatabaseSchema>> {
        // check that there aren't any triggers with this name.
        for (trigger_name, trigger) in &schema.processing_engine_triggers {
            if trigger.plugin_name == self.plugin_name {
                return Err(Error::ProcessingEnginePluginInUse {
                    database_name: schema.name.to_string(),
                    plugin_name: self.plugin_name.to_string(),
                    trigger_name: trigger_name.to_string(),
                });
            }
        }
        schema
            .to_mut()
            .processing_engine_plugins
            .remove(&self.plugin_name);
        Ok(schema)
    }
}

impl UpdateDatabaseSchema for PluginDefinition {
    fn update_schema<'a>(
        &self,
        mut schema: Cow<'a, DatabaseSchema>,
    ) -> Result<Cow<'a, DatabaseSchema>> {
        match schema.processing_engine_plugins.get(&self.plugin_name) {
            Some(current) if self.eq(current) => {}
            Some(_) => {
                return Err(ProcessingEngineCallExists {
                    database_name: schema.name.to_string(),
                    call_name: self.plugin_name.to_string(),
                })
            }
            None => {
                schema
                    .to_mut()
                    .processing_engine_plugins
                    .insert(self.plugin_name.to_string(), self.clone());
            }
        }

        Ok(schema)
    }
}

struct EnableTrigger(TriggerIdentifier);
struct DisableTrigger(TriggerIdentifier);

impl UpdateDatabaseSchema for EnableTrigger {
    fn update_schema<'a>(
        &self,
        mut schema: Cow<'a, DatabaseSchema>,
    ) -> Result<Cow<'a, DatabaseSchema>> {
        let Some(trigger) = schema.processing_engine_triggers.get(&self.0.trigger_name) else {
            return Err(Error::ProcessingEngineTriggerNotFound {
                database_name: self.0.db_name.to_string(),
                trigger_name: self.0.trigger_name.to_string(),
            });
        };
        if !trigger.disabled {
            return Ok(schema);
        }
        let mut_trigger = schema
            .to_mut()
            .processing_engine_triggers
            .get_mut(&self.0.trigger_name)
            .expect("already checked containment");
        mut_trigger.disabled = false;
        Ok(schema)
    }
}

impl UpdateDatabaseSchema for DisableTrigger {
    fn update_schema<'a>(
        &self,
        mut schema: Cow<'a, DatabaseSchema>,
    ) -> Result<Cow<'a, DatabaseSchema>> {
        let Some(trigger) = schema.processing_engine_triggers.get(&self.0.trigger_name) else {
            return Err(Error::ProcessingEngineTriggerNotFound {
                database_name: self.0.db_name.to_string(),
                trigger_name: self.0.trigger_name.to_string(),
            });
        };
        if trigger.disabled {
            return Ok(schema);
        }
        let mut_trigger = schema
            .to_mut()
            .processing_engine_triggers
            .get_mut(&self.0.trigger_name)
            .expect("already checked containment");
        mut_trigger.disabled = true;
        Ok(schema)
    }
}

impl UpdateDatabaseSchema for TriggerDefinition {
    fn update_schema<'a>(
        &self,
        mut schema: Cow<'a, DatabaseSchema>,
    ) -> Result<Cow<'a, DatabaseSchema>> {
        if let Some(current) = schema.processing_engine_triggers.get(&self.trigger_name) {
            if current == self {
                return Ok(schema);
            }
            return Err(ProcessingEngineTriggerExists {
                database_name: schema.name.to_string(),
                trigger_name: self.trigger_name.to_string(),
            });
        }
        schema
            .to_mut()
            .processing_engine_triggers
            .insert(self.trigger_name.to_string(), self.clone());
        Ok(schema)
    }
}

impl UpdateDatabaseSchema for DeleteTriggerDefinition {
    fn update_schema<'a>(
        &self,
        mut schema: Cow<'a, DatabaseSchema>,
    ) -> Result<Cow<'a, DatabaseSchema>> {
        let Some(trigger) = schema.processing_engine_triggers.get(&self.trigger_name) else {
            // deleting a non-existent trigger is a no-op to make it idempotent.
            return Ok(schema);
        };
        if !trigger.disabled && !self.force {
            if self.force {
                warn!("deleting running trigger {}", self.trigger_name);
            } else {
                return Err(ProcessingEngineTriggerRunning {
                    trigger_name: self.trigger_name.to_string(),
                });
            }
        }
        schema
            .to_mut()
            .processing_engine_triggers
            .remove(&self.trigger_name);

        Ok(schema)
    }
}

fn make_new_name_using_deleted_time(name: &str, deletion_time: Time) -> Arc<str> {
    Arc::from(format!(
        "{}-{}",
        name,
        deletion_time.date_time().format(SOFT_DELETION_TIME_FORMAT)
    ))
}

#[derive(Debug, Eq, PartialEq, Clone)]
pub struct TableDefinition {
    pub table_id: TableId,
    pub table_name: Arc<str>,
    pub schema: Schema,
    pub columns: IndexMap<ColumnId, ColumnDefinition>,
    pub column_map: BiHashMap<ColumnId, Arc<str>>,
    pub series_key: Vec<ColumnId>,
    pub series_key_names: Vec<Arc<str>>,
    pub last_caches: HashMap<Arc<str>, LastCacheDefinition>,
    pub distinct_caches: HashMap<Arc<str>, DistinctCacheDefinition>,
    pub deleted: bool,
}

impl TableDefinition {
    /// Create a new [`TableDefinition`]
    ///
    /// Ensures the provided columns will be ordered before constructing the schema.
    pub fn new(
        table_id: TableId,
        table_name: Arc<str>,
        columns: Vec<(ColumnId, Arc<str>, InfluxColumnType)>,
        series_key: Vec<ColumnId>,
    ) -> Result<Self> {
        // ensure we're under the column limit
        if columns.len() > Catalog::NUM_COLUMNS_PER_TABLE_LIMIT {
            return Err(Error::TooManyColumns);
        }

        // Use a BTree to ensure that the columns are ordered:
        let mut ordered_columns = BTreeMap::new();
        for (col_id, name, column_type) in &columns {
            ordered_columns.insert(name.as_ref(), (col_id, column_type));
        }
        let mut schema_builder = SchemaBuilder::with_capacity(columns.len());
        schema_builder.measurement(table_name.as_ref());
        let mut columns = IndexMap::with_capacity(ordered_columns.len());
        let mut column_map = BiHashMap::with_capacity(ordered_columns.len());
        for (name, (col_id, column_type)) in ordered_columns {
            schema_builder.influx_column(name, *column_type);
            let not_nullable =
                matches!(column_type, InfluxColumnType::Timestamp) || series_key.contains(col_id);
            columns.insert(
                *col_id,
                ColumnDefinition::new(*col_id, name, *column_type, !not_nullable),
            );
            column_map.insert(*col_id, name.into());
        }
        let series_key_names = series_key
            .clone()
            .into_iter()
            .map(|id| {
                column_map
                    .get_by_left(&id)
                    .cloned()
                    // NOTE: should this be an error instead of panic?
                    .expect("invalid column id in series key definition")
            })
            .collect::<Vec<Arc<str>>>();
        schema_builder.with_series_key(&series_key_names);
        let schema = schema_builder.build().expect("schema should be valid");

        Ok(Self {
            table_id,
            table_name,
            schema,
            columns,
            column_map,
            series_key,
            series_key_names,
            last_caches: HashMap::new(),
            distinct_caches: HashMap::new(),
            deleted: false,
        })
    }

    /// Create a new table definition from a catalog op
    pub fn new_from_op(table_definition: &influxdb3_wal::TableDefinition) -> Self {
        let mut columns = Vec::with_capacity(table_definition.field_definitions.len());
        for field_def in &table_definition.field_definitions {
            columns.push((
                field_def.id,
                Arc::clone(&field_def.name),
                field_def.data_type.into(),
            ));
        }
        Self::new(
            table_definition.table_id,
            Arc::clone(&table_definition.table_name),
            columns,
            table_definition.key.clone(),
        )
        .expect("tables defined from ops should not exceed column limits")
    }

    pub(crate) fn check_and_add_new_fields(
        &self,
        table_definition: &influxdb3_wal::TableDefinition,
    ) -> Result<Cow<'_, Self>> {
        // validate the series key is the same
        if table_definition.key != self.series_key {
            return Err(Error::SeriesKeyMismatch {
                table_name: self.table_name.to_string(),
                existing: self.schema.series_key().unwrap_or_default().join("/"),
            });
        }
        Self::add_fields(Cow::Borrowed(self), &table_definition.field_definitions)
    }

    pub(crate) fn add_fields<'a>(
        mut table: Cow<'a, Self>,
        fields: &Vec<FieldDefinition>,
    ) -> Result<Cow<'a, Self>> {
        let mut new_fields: Vec<(ColumnId, Arc<str>, InfluxColumnType)> =
            Vec::with_capacity(fields.len());
        for field_def in fields {
            if let Some(existing_type) = table.columns.get(&field_def.id).map(|def| def.data_type) {
                if existing_type != field_def.data_type.into() {
                    return Err(Error::FieldTypeMismatch {
                        table_name: table.table_name.to_string(),
                        column_name: field_def.name.to_string(),
                        existing: existing_type,
                        attempted: field_def.data_type.into(),
                    });
                }
            } else {
                new_fields.push((
                    field_def.id,
                    Arc::clone(&field_def.name),
                    field_def.data_type.into(),
                ));
            }
        }

        if !new_fields.is_empty() {
            table.to_mut().add_columns(new_fields)?;
        }
        Ok(table)
    }

    /// Check if the column exists in the [`TableDefinition`]
    pub fn column_exists(&self, column: impl Into<Arc<str>>) -> bool {
        self.column_map.get_by_right(&column.into()).is_some()
    }

    /// Add the columns to this [`TableDefinition`]
    ///
    /// This ensures that the resulting schema has its columns ordered
    pub fn add_columns(
        &mut self,
        columns: Vec<(ColumnId, Arc<str>, InfluxColumnType)>,
    ) -> Result<()> {
        // Use BTree to insert existing and new columns, and use that to generate the
        // resulting schema, to ensure column order is consistent:
        let mut cols = BTreeMap::new();
        for (_, col_def) in self.columns.drain(..) {
            cols.insert(Arc::clone(&col_def.name), col_def);
        }
        for (id, name, column_type) in columns {
            assert!(
                cols.insert(
                    Arc::clone(&name),
                    // any new column added by this function must be nullable:
                    ColumnDefinition::new(id, name, column_type, true)
                )
                .is_none(),
                "attempted to add existing column"
            );
        }

        // ensure we don't go over the column limit
        if cols.len() > Catalog::NUM_COLUMNS_PER_TABLE_LIMIT {
            return Err(Error::TooManyColumns);
        }

        let mut schema_builder = SchemaBuilder::with_capacity(cols.len());
        // TODO: may need to capture some schema-level metadata, currently, this causes trouble in
        // tests, so I am omitting this for now:
        // schema_builder.measurement(&self.name);
        for (name, col_def) in &cols {
            schema_builder.influx_column(name.as_ref(), col_def.data_type);
        }
        let schema = schema_builder.build().expect("schema should be valid");
        self.schema = schema;

        self.columns = cols
            .into_iter()
            .inspect(|(_, def)| {
                self.column_map.insert(def.id, Arc::clone(&def.name));
            })
            .map(|(_, def)| (def.id, def))
            .collect();

        Ok(())
    }

    pub fn index_column_ids(&self) -> Vec<ColumnId> {
        self.columns
            .iter()
            .filter_map(|(id, def)| match def.data_type {
                InfluxColumnType::Tag => Some(*id),
                InfluxColumnType::Field(_) | InfluxColumnType::Timestamp => None,
            })
            .collect()
    }

    pub fn influx_schema(&self) -> &Schema {
        &self.schema
    }

    pub fn num_columns(&self) -> usize {
        self.influx_schema().len()
    }

    pub fn field_type_by_name(&self, name: impl Into<Arc<str>>) -> Option<InfluxColumnType> {
        self.column_name_to_id(name)
            .and_then(|id| self.columns.get(&id))
            .map(|def| def.data_type)
    }

    /// Add the given [`DistinctCacheDefinition`] to this table
    pub fn add_distinct_cache(&mut self, distinct_cache: DistinctCacheDefinition) {
        self.distinct_caches
            .insert(Arc::clone(&distinct_cache.cache_name), distinct_cache);
    }

    /// Remove the distinct cache with the given name
    pub fn remove_distinct_cache(&mut self, cache_name: &str) {
        self.distinct_caches.remove(cache_name);
    }

    /// Add a new last cache to this table definition
    pub fn add_last_cache(&mut self, last_cache: LastCacheDefinition) {
        self.last_caches
            .insert(Arc::clone(&last_cache.name), last_cache);
    }

    /// Remove a last cache from the table definition by its name
    pub fn remove_last_cache(&mut self, cache_name: &str) {
        self.last_caches.remove(cache_name);
    }

    pub fn last_caches(&self) -> impl Iterator<Item = (Arc<str>, &LastCacheDefinition)> {
        self.last_caches
            .iter()
            .map(|(name, def)| (Arc::clone(name), def))
    }

    pub fn distinct_caches(&self) -> impl Iterator<Item = (Arc<str>, &DistinctCacheDefinition)> {
        self.distinct_caches
            .iter()
            .map(|(name, def)| (Arc::clone(name), def))
    }

    pub fn column_name_to_id(&self, name: impl Into<Arc<str>>) -> Option<ColumnId> {
        self.column_map.get_by_right(&name.into()).copied()
    }

    pub fn column_id_to_name(&self, id: &ColumnId) -> Option<Arc<str>> {
        self.column_map.get_by_left(id).cloned()
    }

    pub fn column_name_to_id_unchecked(&self, name: impl Into<Arc<str>>) -> ColumnId {
        *self
            .column_map
            .get_by_right(&name.into())
            .expect("Column exists in mapping")
    }

    pub fn column_id_to_name_unchecked(&self, id: &ColumnId) -> Arc<str> {
        Arc::clone(
            self.column_map
                .get_by_left(id)
                .expect("Column exists in mapping"),
        )
    }

    pub fn column_definition(&self, name: impl Into<Arc<str>>) -> Option<&ColumnDefinition> {
        self.column_map
            .get_by_right(&name.into())
            .and_then(|id| self.columns.get(id))
    }

    pub fn column_definition_by_id(&self, id: &ColumnId) -> Option<&ColumnDefinition> {
        self.columns.get(id)
    }

    pub fn column_id_and_definition(
        &self,
        name: impl Into<Arc<str>>,
    ) -> Option<(ColumnId, &ColumnDefinition)> {
        self.column_map
            .get_by_right(&name.into())
            .and_then(|id| self.columns.get(id).map(|def| (*id, def)))
    }

    pub fn series_key_ids(&self) -> &[ColumnId] {
        &self.series_key
    }

    pub fn series_key_names(&self) -> &[Arc<str>] {
        &self.series_key_names
    }
}

trait TableUpdate {
    fn table_id(&self) -> TableId;
    fn table_name(&self) -> Arc<str>;
    fn update_table<'a>(&self, table: Cow<'a, TableDefinition>)
        -> Result<Cow<'a, TableDefinition>>;
}

impl<T: TableUpdate> UpdateDatabaseSchema for T {
    fn update_schema<'a>(
        &self,
        mut schema: Cow<'a, DatabaseSchema>,
    ) -> Result<Cow<'a, DatabaseSchema>> {
        let Some(table) = schema.tables.get(&self.table_id()) else {
            return Err(TableNotFound {
                db_name: Arc::clone(&schema.name),
                table_name: Arc::clone(&self.table_name()),
            });
        };
        if let Cow::Owned(new_table) = self.update_table(Cow::Borrowed(table.as_ref()))? {
            schema
                .to_mut()
                .insert_table(new_table.table_id, Arc::new(new_table))?;
        }
        Ok(schema)
    }
}

impl TableUpdate for FieldAdditions {
    fn table_id(&self) -> TableId {
        self.table_id
    }
    fn table_name(&self) -> Arc<str> {
        Arc::clone(&self.table_name)
    }
    fn update_table<'a>(
        &self,
        table: Cow<'a, TableDefinition>,
    ) -> Result<Cow<'a, TableDefinition>> {
        TableDefinition::add_fields(table, &self.field_definitions)
    }
}

impl TableUpdate for DistinctCacheDefinition {
    fn table_id(&self) -> TableId {
        self.table_id
    }
    fn table_name(&self) -> Arc<str> {
        Arc::clone(&self.table_name)
    }
    fn update_table<'a>(
        &self,
        mut table: Cow<'a, TableDefinition>,
    ) -> Result<Cow<'a, TableDefinition>> {
        if !table.distinct_caches.contains_key(&self.cache_name) {
            table.to_mut().add_distinct_cache(self.clone());
        }
        Ok(table)
    }
}

impl TableUpdate for DistinctCacheDelete {
    fn table_id(&self) -> TableId {
        self.table_id
    }
    fn table_name(&self) -> Arc<str> {
        Arc::clone(&self.table_name)
    }
    fn update_table<'a>(
        &self,
        mut table: Cow<'a, TableDefinition>,
    ) -> Result<Cow<'a, TableDefinition>> {
        if table.distinct_caches.contains_key(&self.cache_name) {
            table.to_mut().distinct_caches.remove(&self.cache_name);
        }
        Ok(table)
    }
}

impl TableUpdate for LastCacheDefinition {
    fn table_id(&self) -> TableId {
        self.table_id
    }

    fn table_name(&self) -> Arc<str> {
        Arc::clone(&self.table)
    }

    fn update_table<'a>(
        &self,
        mut table: Cow<'a, TableDefinition>,
    ) -> Result<Cow<'a, TableDefinition>> {
        if !table.last_caches.contains_key(&self.name) {
            table.to_mut().add_last_cache(self.clone());
        }
        Ok(table)
    }
}

impl TableUpdate for LastCacheDelete {
    fn table_id(&self) -> TableId {
        self.table_id
    }
    fn table_name(&self) -> Arc<str> {
        Arc::clone(&self.table_name)
    }

    fn update_table<'a>(
        &self,
        mut table: Cow<'a, TableDefinition>,
    ) -> Result<Cow<'a, TableDefinition>> {
        if table.last_caches.contains_key(&self.name) {
            table.to_mut().last_caches.remove(&self.name);
        }
        Ok(table)
    }
}

#[derive(Debug, Eq, PartialEq, Clone)]
pub struct ColumnDefinition {
    pub id: ColumnId,
    pub name: Arc<str>,
    pub data_type: InfluxColumnType,
    pub nullable: bool,
}

impl ColumnDefinition {
    pub fn new(
        id: ColumnId,
        name: impl Into<Arc<str>>,
        data_type: InfluxColumnType,
        nullable: bool,
    ) -> Self {
        Self {
            id,
            name: name.into(),
            data_type,
            nullable,
        }
    }
}

pub fn influx_column_type_from_field_value(fv: &FieldValue<'_>) -> InfluxColumnType {
    match fv {
        FieldValue::I64(_) => InfluxColumnType::Field(InfluxFieldType::Integer),
        FieldValue::U64(_) => InfluxColumnType::Field(InfluxFieldType::UInteger),
        FieldValue::F64(_) => InfluxColumnType::Field(InfluxFieldType::Float),
        FieldValue::String(_) => InfluxColumnType::Field(InfluxFieldType::String),
        FieldValue::Boolean(_) => InfluxColumnType::Field(InfluxFieldType::Boolean),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use influxdb3_wal::CatalogOp::CreateTable;
    use influxdb3_wal::{create, DatabaseDefinition, FieldDataType, WalOp};
    use pretty_assertions::assert_eq;
    use test_helpers::assert_contains;

    #[test]
    fn catalog_serialization() {
        let host_id = Arc::from("sample-host-id");
        let instance_id = Arc::from("instance-id");
        let cloned_instance_id = Arc::clone(&instance_id);
        let catalog = Catalog::new(host_id, cloned_instance_id);
        let mut database = DatabaseSchema {
            id: DbId::from(0),
            name: "test_db".into(),
            tables: SerdeVecMap::new(),
            table_map: {
                let mut map = BiHashMap::new();
                map.insert(TableId::from(1), "test_table_1".into());
                map.insert(TableId::from(2), "test_table_2".into());
                map
            },
            processing_engine_plugins: Default::default(),
            processing_engine_triggers: Default::default(),
            deleted: false,
        };
        use InfluxColumnType::*;
        use InfluxFieldType::*;
        database.tables.insert(
            TableId::from(1),
            Arc::new(
                TableDefinition::new(
                    TableId::from(1),
                    "test_table_1".into(),
                    vec![
                        (ColumnId::new(), "tag_1".into(), Tag),
                        (ColumnId::new(), "tag_2".into(), Tag),
                        (ColumnId::new(), "tag_3".into(), Tag),
                        (ColumnId::new(), "time".into(), Timestamp),
                        (ColumnId::new(), "string_field".into(), Field(String)),
                        (ColumnId::new(), "bool_field".into(), Field(Boolean)),
                        (ColumnId::new(), "i64_field".into(), Field(Integer)),
                        (ColumnId::new(), "u64_field".into(), Field(UInteger)),
                        (ColumnId::new(), "f64_field".into(), Field(Float)),
                    ],
                    vec![0.into(), 1.into(), 2.into()],
                )
                .unwrap(),
            ),
        );
        database.tables.insert(
            TableId::from(2),
            Arc::new(
                TableDefinition::new(
                    TableId::from(2),
                    "test_table_2".into(),
                    vec![
                        (ColumnId::new(), "tag_1".into(), Tag),
                        (ColumnId::new(), "tag_2".into(), Tag),
                        (ColumnId::new(), "tag_3".into(), Tag),
                        (ColumnId::new(), "time".into(), Timestamp),
                        (ColumnId::new(), "string_field".into(), Field(String)),
                        (ColumnId::new(), "bool_field".into(), Field(Boolean)),
                        (ColumnId::new(), "i64_field".into(), Field(Integer)),
                        (ColumnId::new(), "u64_field".into(), Field(UInteger)),
                        (ColumnId::new(), "f64_field".into(), Field(Float)),
                    ],
                    vec![9.into(), 10.into(), 11.into()],
                )
                .unwrap(),
            ),
        );
        catalog
            .inner
            .write()
            .databases
            .insert(database.id, Arc::new(database));

        insta::with_settings!({
            sort_maps => true,
            description => "catalog serialization to help catch breaking changes"
        }, {
            insta::assert_json_snapshot!(catalog);
        });

        // Serialize/deserialize to ensure roundtrip to/from JSON
        let serialized = serde_json::to_string(&catalog).unwrap();
        let deserialized_inner: InnerCatalog = serde_json::from_str(&serialized).unwrap();
        let deserialized = Catalog::from_inner(deserialized_inner);
        assert_eq!(catalog, deserialized);
        assert_eq!(instance_id, deserialized.instance_id());
    }

    #[test]
    fn invalid_catalog_deserialization() {
        // Duplicate databases
        {
            let json = r#"{
                "databases": [
                    [
                        0,
                        {
                            "id": 0,
                            "name": "db1",
                            "tables": [],
                            "table_map": [],
                            "deleted": false
                        }
                    ],
                    [
                        0,
                        {
                            "id": 0,
                            "name": "db1",
                            "tables": [],
                            "table_map": [],
                            "deleted": false
                        }
                    ]
                ],
                "sequence": 0,
                "host_id": "test",
                "instance_id": "test",
                "db_map": []
            }"#;
            let err = serde_json::from_str::<InnerCatalog>(json).unwrap_err();
            assert_contains!(err.to_string(), "duplicate key found");
        }
        // Duplicate tables
        {
            let json = r#"{
                "databases": [
                    [
                        0,
                        {
                            "id": 0,
                            "name": "db1",
                            "tables": [
                                [
                                    0,
                                    {
                                        "table_id": 0,
                                        "table_name": "tbl1",
                                        "cols": [],
                                        "next_column_id": 0,
                                        "deleted": false,
                                        "key": []

                                    }
                                ],
                                [
                                    0,
                                    {
                                        "table_id": 0,
                                        "table_name": "tbl1",
                                        "cols": [],
                                        "next_column_id": 0,
                                        "deleted": false,
                                        "key": []
                                    }
                                ]
                            ],
                            "deleted": false
                        }
                    ]
                ],
                "sequence": 0,
                "host_id": "test",
                "instance_id": "test",
                "db_map": []
            }"#;
            let err = serde_json::from_str::<InnerCatalog>(json).unwrap_err();
            assert_contains!(err.to_string(), "duplicate key found");
        }
        // Duplicate columns
        {
            let json = r#"{
                "databases": [
                    [
                        0,
                        {
                            "id": 0,
                            "name": "db1",
                            "tables": [
                                [
                                    0,
                                    {
                                        "table_id": 0,
                                        "table_name": "tbl1",
                                        "cols": [
                                            [
                                                0,
                                                {
                                                    "id": 0,
                                                    "name": "col",
                                                    "type": "i64",
                                                    "influx_type": "field",
                                                    "nullable": true
                                                }
                                            ],
                                            [
                                                0,
                                                {
                                                    "id": 0,
                                                    "name": "col",
                                                    "type": "u64",
                                                    "influx_type": "field",
                                                    "nullable": true
                                                }
                                            ]
                                        ],
                                        "deleted": false

                                    }
                                ]
                            ],
                            "deleted": false
                        }
                    ]
                ]
            }"#;
            let err = serde_json::from_str::<InnerCatalog>(json).unwrap_err();
            assert_contains!(err.to_string(), "duplicate key found");
        }
    }

    #[test]
    fn add_columns_updates_schema_and_column_map() {
        let mut database = DatabaseSchema {
            id: DbId::from(0),
            name: "test".into(),
            tables: SerdeVecMap::new(),
            table_map: BiHashMap::new(),
            processing_engine_plugins: Default::default(),
            processing_engine_triggers: Default::default(),
            deleted: false,
        };
        database.tables.insert(
            TableId::from(0),
            Arc::new(
                TableDefinition::new(
                    TableId::from(0),
                    "test".into(),
                    vec![(
                        ColumnId::from(0),
                        "test".into(),
                        InfluxColumnType::Field(InfluxFieldType::String),
                    )],
                    vec![],
                )
                .unwrap(),
            ),
        );

        let table = database.tables.get_mut(&TableId::from(0)).unwrap();
        println!("table: {table:#?}");
        assert_eq!(table.column_map.len(), 1);
        assert_eq!(table.column_id_to_name_unchecked(&0.into()), "test".into());

        Arc::make_mut(table)
            .add_columns(vec![(
                ColumnId::from(1),
                "test2".into(),
                InfluxColumnType::Tag,
            )])
            .unwrap();
        let schema = table.influx_schema();
        assert_eq!(
            schema.field(0).0,
            InfluxColumnType::Field(InfluxFieldType::String)
        );
        assert_eq!(schema.field(1).0, InfluxColumnType::Tag);

        println!("table: {table:#?}");
        assert_eq!(table.column_map.len(), 2);
        assert_eq!(table.column_name_to_id_unchecked("test2"), 1.into());
    }

    #[test]
    fn serialize_series_keys() {
        let host_id = Arc::from("sample-host-id");
        let instance_id = Arc::from("instance-id");
        let catalog = Catalog::new(host_id, instance_id);
        let mut database = DatabaseSchema {
            id: DbId::from(0),
            name: "test_db".into(),
            tables: SerdeVecMap::new(),
            table_map: {
                let mut map = BiHashMap::new();
                map.insert(TableId::from(1), "test_table_1".into());
                map
            },
            processing_engine_plugins: Default::default(),
            processing_engine_triggers: Default::default(),
            deleted: false,
        };
        use InfluxColumnType::*;
        use InfluxFieldType::*;
        database.tables.insert(
            TableId::from(1),
            Arc::new(
                TableDefinition::new(
                    TableId::from(1),
                    "test_table_1".into(),
                    vec![
                        (ColumnId::from(0), "tag_1".into(), Tag),
                        (ColumnId::from(1), "tag_2".into(), Tag),
                        (ColumnId::from(2), "tag_3".into(), Tag),
                        (ColumnId::from(3), "time".into(), Timestamp),
                        (ColumnId::from(4), "field".into(), Field(String)),
                    ],
                    vec![ColumnId::from(0), ColumnId::from(1), ColumnId::from(2)],
                )
                .unwrap(),
            ),
        );
        catalog
            .inner
            .write()
            .databases
            .insert(database.id, Arc::new(database));

        insta::with_settings!({
            sort_maps => true,
            description => "catalog serialization to help catch breaking changes"
        }, {
            insta::assert_json_snapshot!(catalog);
        });

        let serialized = serde_json::to_string(&catalog).unwrap();
        let deserialized_inner: InnerCatalog = serde_json::from_str(&serialized).unwrap();
        let deserialized = Catalog::from_inner(deserialized_inner);
        assert_eq!(catalog, deserialized);
    }

    #[test]
    fn serialize_last_cache() {
        let host_id = Arc::from("sample-host-id");
        let instance_id = Arc::from("instance-id");
        let catalog = Catalog::new(host_id, instance_id);
        let mut database = DatabaseSchema {
            id: DbId::from(0),
            name: "test_db".into(),
            tables: SerdeVecMap::new(),
            table_map: {
                let mut map = BiHashMap::new();
                map.insert(TableId::from(0), "test".into());
                map
            },
            processing_engine_plugins: Default::default(),
            processing_engine_triggers: Default::default(),
            deleted: false,
        };
        use InfluxColumnType::*;
        use InfluxFieldType::*;
        let mut table_def = TableDefinition::new(
            TableId::from(0),
            "test".into(),
            vec![
                (ColumnId::from(0), "tag_1".into(), Tag),
                (ColumnId::from(1), "tag_2".into(), Tag),
                (ColumnId::from(2), "tag_3".into(), Tag),
                (ColumnId::from(3), "time".into(), Timestamp),
                (ColumnId::from(4), "field".into(), Field(String)),
            ],
            vec![0.into(), 1.into(), 2.into()],
        )
        .unwrap();
        table_def.add_last_cache(
            LastCacheDefinition::new_with_explicit_value_columns(
                TableId::from(0),
                "test",
                "test_table_last_cache",
                vec![ColumnId::from(1), ColumnId::from(2)],
                vec![ColumnId::from(4)],
                1,
                600,
            )
            .unwrap(),
        );
        database
            .tables
            .insert(TableId::from(0), Arc::new(table_def));
        catalog
            .inner
            .write()
            .databases
            .insert(database.id, Arc::new(database));

        insta::with_settings!({
            sort_maps => true,
            description => "catalog serialization to help catch breaking changes"
        }, {
            insta::assert_json_snapshot!(catalog);
        });

        let serialized = serde_json::to_string(&catalog).unwrap();
        let deserialized_inner: InnerCatalog = serde_json::from_str(&serialized).unwrap();
        let deserialized = Catalog::from_inner(deserialized_inner);
        assert_eq!(catalog, deserialized);
    }

    #[test]
    fn catalog_instance_and_host_ids() {
        let host_id = Arc::from("sample-host-id");
        let instance_id = Arc::from("sample-instance-id");
        let cloned_host_id = Arc::clone(&host_id);
        let cloned_instance_id = Arc::clone(&instance_id);
        let catalog = Catalog::new(cloned_host_id, cloned_instance_id);
        assert_eq!(instance_id, catalog.instance_id());
        assert_eq!(host_id, catalog.host_id());
    }

    /// See: https://github.com/influxdata/influxdb/issues/25524
    #[test]
    fn apply_catalog_batch_fails_for_add_fields_on_nonexist_table() {
        let catalog = Catalog::new(Arc::from("host"), Arc::from("instance"));
        catalog.insert_database(DatabaseSchema::new(DbId::new(), Arc::from("foo")));
        let db_id = catalog.db_name_to_id("foo").unwrap();
        let catalog_batch = create::catalog_batch(
            db_id,
            "foo",
            0,
            [create::add_fields_op(
                db_id,
                "foo",
                TableId::new(),
                "banana",
                [create::field_def(
                    ColumnId::new(),
                    "papaya",
                    FieldDataType::String,
                )],
            )],
        );
        let err = catalog
            .apply_catalog_batch(&catalog_batch)
            .expect_err("should fail to apply AddFields operation for non-existent table");
        assert_contains!(err.to_string(), "Table banana not in DB schema for foo");
    }

    #[test]
    fn test_check_and_mark_table_as_deleted() {
        let db_id = DbId::from(0);

        let mut database = DatabaseSchema {
            id: db_id,
            name: "test".into(),
            tables: SerdeVecMap::new(),
            table_map: BiHashMap::new(),
            processing_engine_plugins: Default::default(),
            processing_engine_triggers: Default::default(),
            deleted: false,
        };
        let deleted_table_id = TableId::new();
        let table_name = Arc::from("boo");
        let deleted_table_defn = DeleteTableDefinition {
            database_id: db_id,
            database_name: Arc::from("foo"),
            table_id: deleted_table_id,
            table_name: Arc::clone(&table_name),
            deletion_time: 0,
        };
        let table_defn = Arc::new(
            TableDefinition::new(
                deleted_table_id,
                Arc::clone(&table_name),
                vec![
                    (ColumnId::from(0), "tag_1".into(), InfluxColumnType::Tag),
                    (ColumnId::from(1), "tag_2".into(), InfluxColumnType::Tag),
                    (ColumnId::from(2), "tag_3".into(), InfluxColumnType::Tag),
                    (
                        ColumnId::from(3),
                        "time".into(),
                        InfluxColumnType::Timestamp,
                    ),
                    (
                        ColumnId::from(4),
                        "field".into(),
                        InfluxColumnType::Field(InfluxFieldType::String),
                    ),
                ],
                vec![ColumnId::from(0), ColumnId::from(1), ColumnId::from(2)],
            )
            .unwrap(),
        );

        database
            .insert_table(deleted_table_id, table_defn)
            .expect("should be able to insert");
        let new_db = DatabaseSchema::new_if_updated_from_batch(
            &database,
            &CatalogBatch {
                database_id: database.id,
                database_name: Arc::clone(&database.name),
                time_ns: 0,
                ops: vec![CatalogOp::DeleteTable(deleted_table_defn)],
            },
        )
        .unwrap()
        .expect("should mutate db");

        // check that table is not under the old name
        assert!(new_db.table_definition(table_name).is_none());

        let deleted_table = new_db.table_definition("boo-19700101T000000").unwrap();

        assert_eq!(deleted_table.table_id, deleted_table_id);
        assert_eq!(&*deleted_table.table_name, "boo-19700101T000000");
        assert!(deleted_table.deleted);
        assert!(!deleted_table.series_key.is_empty());
    }

    // tests that sorting catalog ops by the sequence number returned from apply_catalog_batch
    // fixes potential ordering issues.
    #[test]
    fn test_out_of_order_ops() -> Result<()> {
        let catalog = Catalog::new(Arc::from("host"), Arc::from("instance"));
        let db_id = DbId::new();
        let db_name = Arc::from("foo");
        let table_id = TableId::new();
        let table_name = Arc::from("bar");
        let table_definition = influxdb3_wal::TableDefinition {
            database_id: db_id,
            database_name: Arc::clone(&db_name),
            table_name: Arc::clone(&table_name),
            table_id,
            field_definitions: vec![
                FieldDefinition::new(ColumnId::from(0), "tag_1", FieldDataType::Tag),
                FieldDefinition::new(ColumnId::from(1), "time", FieldDataType::Timestamp),
                FieldDefinition::new(ColumnId::from(2), "field", FieldDataType::String),
            ],
            key: vec![ColumnId::from(0)],
        };
        let create_op = CatalogBatch {
            database_id: db_id,
            database_name: Arc::clone(&db_name),
            time_ns: 0,
            ops: vec![CreateTable(table_definition.clone())],
        };
        let add_column_op = CatalogBatch {
            database_id: db_id,
            database_name: Arc::clone(&db_name),
            time_ns: 0,
            ops: vec![CatalogOp::AddFields(FieldAdditions {
                database_name: Arc::clone(&db_name),
                database_id: db_id,
                table_name,
                table_id,
                field_definitions: vec![FieldDefinition::new(
                    ColumnId::from(3),
                    "tag_2",
                    FieldDataType::Tag,
                )],
            })],
        };
        let create_ordered_op = catalog
            .apply_catalog_batch(&create_op)?
            .expect("should be able to create");
        let add_column_op = catalog
            .apply_catalog_batch(&add_column_op)?
            .expect("should produce operation");
        let mut ops = vec![
            WalOp::Catalog(add_column_op),
            WalOp::Catalog(create_ordered_op),
        ];
        ops.sort();

        let replayed_catalog = Catalog::new(Arc::from("host"), Arc::from("instance"));
        for op in ops {
            let WalOp::Catalog(catalog_batch) = op else {
                panic!("should produce operation");
            };
            replayed_catalog.apply_catalog_batch(catalog_batch.batch())?;
        }
        let original_table = catalog
            .db_schema_by_id(&db_id)
            .unwrap()
            .table_definition_by_id(&table_id)
            .unwrap();
        let replayed_table = catalog
            .db_schema_by_id(&db_id)
            .unwrap()
            .table_definition_by_id(&table_id)
            .unwrap();

        assert_eq!(original_table, replayed_table);
        Ok(())
    }

    #[test]
    fn deleted_dbs_dont_count() {
        let catalog = Catalog::new(Arc::from("test"), Arc::from("test-instance"));

        let mut dbs: Vec<(DbId, String)> = vec![];
        for _ in 0..Catalog::NUM_DBS_LIMIT {
            let db_id = DbId::new();
            let db_name = format!("test-db-{db_id}");
            catalog
                .apply_catalog_batch(&influxdb3_wal::create::catalog_batch(
                    db_id,
                    db_name.as_str(),
                    0,
                    [influxdb3_wal::create::create_table_op(
                        db_id,
                        db_name.as_str(),
                        TableId::new(),
                        "test-table",
                        [
                            influxdb3_wal::create::field_def(
                                ColumnId::new(),
                                "field",
                                FieldDataType::String,
                            ),
                            influxdb3_wal::create::field_def(
                                ColumnId::new(),
                                "time",
                                FieldDataType::Timestamp,
                            ),
                        ],
                        [],
                    )],
                ))
                .unwrap();
            dbs.push((db_id, db_name));
        }
        // check the count of databases:
        assert_eq!(5, catalog.inner.read().database_count());
        // now create another database, this should NOT be allowed:
        let db_id = DbId::new();
        let db_name = "a-database-too-far";
        catalog
            .apply_catalog_batch(&influxdb3_wal::create::catalog_batch(
                db_id,
                db_name,
                0,
                [influxdb3_wal::create::create_table_op(
                    db_id,
                    db_name,
                    TableId::new(),
                    "test-table",
                    [
                        influxdb3_wal::create::field_def(
                            ColumnId::new(),
                            "field",
                            FieldDataType::String,
                        ),
                        influxdb3_wal::create::field_def(
                            ColumnId::new(),
                            "time",
                            FieldDataType::Timestamp,
                        ),
                    ],
                    [],
                )],
            ))
            .expect_err("should not be able to create more than 5 databases");
        // now delete a database:
        let (db_id, db_name) = dbs.pop().unwrap();
        catalog
            .apply_catalog_batch(&influxdb3_wal::create::catalog_batch(
                db_id,
                db_name.as_str(),
                1,
                [CatalogOp::DeleteDatabase(DeleteDatabaseDefinition {
                    database_id: db_id,
                    database_name: db_name.as_str().into(),
                    deletion_time: 1,
                })],
            ))
            .unwrap();
        // check again, count should have gone down:
        assert_eq!(4, catalog.inner.read().database_count());
        // now create another database (using same name as the deleted one), this should be allowed:
        let db_id = DbId::new();
        catalog
            .apply_catalog_batch(&influxdb3_wal::create::catalog_batch(
                db_id,
                db_name.as_str(),
                0,
                [influxdb3_wal::create::create_table_op(
                    db_id,
                    db_name.as_str(),
                    TableId::new(),
                    "test-table",
                    [
                        influxdb3_wal::create::field_def(
                            ColumnId::new(),
                            "field",
                            FieldDataType::String,
                        ),
                        influxdb3_wal::create::field_def(
                            ColumnId::new(),
                            "time",
                            FieldDataType::Timestamp,
                        ),
                    ],
                    [],
                )],
            ))
            .expect("can create a database again");
        // check new count:
        assert_eq!(5, catalog.inner.read().database_count());
    }

    #[test]
    fn deleted_tables_dont_count() {
        let catalog = Catalog::new(Arc::from("test"), Arc::from("test-instance"));
        // create a database:
        let db_id = DbId::new();
        let db_name = "test-db";
        catalog
            .apply_catalog_batch(&influxdb3_wal::create::catalog_batch(
                db_id,
                db_name,
                0,
                [CatalogOp::CreateDatabase(DatabaseDefinition {
                    database_id: db_id,
                    database_name: db_name.into(),
                })],
            ))
            .unwrap();
        let mut tables: Vec<(TableId, Arc<str>)> = vec![];
        for i in 0..Catalog::NUM_TABLES_LIMIT {
            let table_id = TableId::new();
            let table_name = Arc::<str>::from(format!("test-table-{i}").as_str());
            catalog
                .apply_catalog_batch(&influxdb3_wal::create::catalog_batch(
                    db_id,
                    db_name,
                    0,
                    [influxdb3_wal::create::create_table_op(
                        db_id,
                        db_name,
                        table_id,
                        Arc::clone(&table_name),
                        [
                            influxdb3_wal::create::field_def(
                                ColumnId::new(),
                                "field",
                                FieldDataType::String,
                            ),
                            influxdb3_wal::create::field_def(
                                ColumnId::new(),
                                "time",
                                FieldDataType::Timestamp,
                            ),
                        ],
                        [],
                    )],
                ))
                .unwrap();
            tables.push((table_id, table_name));
        }
        assert_eq!(2_000, catalog.inner.read().table_count());
        // should not be able to create another table:
        let table_id = TableId::new();
        let table_name = Arc::<str>::from("a-table-too-far");
        catalog
            .apply_catalog_batch(&influxdb3_wal::create::catalog_batch(
                db_id,
                db_name,
                0,
                [influxdb3_wal::create::create_table_op(
                    db_id,
                    db_name,
                    table_id,
                    Arc::clone(&table_name),
                    [
                        influxdb3_wal::create::field_def(
                            ColumnId::new(),
                            "field",
                            FieldDataType::String,
                        ),
                        influxdb3_wal::create::field_def(
                            ColumnId::new(),
                            "time",
                            FieldDataType::Timestamp,
                        ),
                    ],
                    [],
                )],
            ))
            .expect_err("should not be able to exceed table limit");
        // delete a table
        let (table_id, table_name) = tables.pop().unwrap();
        catalog
            .apply_catalog_batch(&influxdb3_wal::create::catalog_batch(
                db_id,
                db_name,
                0,
                [CatalogOp::DeleteTable(DeleteTableDefinition {
                    database_id: db_id,
                    database_name: db_name.into(),
                    table_id,
                    table_name: Arc::clone(&table_name),
                    deletion_time: 0,
                })],
            ))
            .unwrap();
        assert_eq!(1_999, catalog.inner.read().table_count());
        // now create it again, this should be allowed:
        catalog
            .apply_catalog_batch(&influxdb3_wal::create::catalog_batch(
                db_id,
                db_name,
                0,
                [influxdb3_wal::create::create_table_op(
                    db_id,
                    db_name,
                    TableId::new(),
                    Arc::clone(&table_name),
                    [
                        influxdb3_wal::create::field_def(
                            ColumnId::new(),
                            "field",
                            FieldDataType::String,
                        ),
                        influxdb3_wal::create::field_def(
                            ColumnId::new(),
                            "time",
                            FieldDataType::Timestamp,
                        ),
                    ],
                    [],
                )],
            ))
            .unwrap();
        assert_eq!(2_000, catalog.inner.read().table_count());
    }
}
