use std::{
    collections::{BTreeMap, BTreeSet, BinaryHeap},
    num::NonZeroUsize,
    sync::Arc,
    time::Duration,
};

use anyhow::Context;
use arrow::{
    array::{ArrayRef, RecordBatch, StringViewBuilder},
    datatypes::{DataType, Field, SchemaBuilder, SchemaRef},
    error::ArrowError,
};
use indexmap::IndexMap;
use influxdb3_catalog::catalog::TableDefinition;
use influxdb3_id::{ColumnId, TableId};
use influxdb3_wal::{DistinctCacheDefinition, FieldData, Row};
use iox_time::TimeProvider;
use miette::Diagnostic;
use schema::{InfluxColumnType, InfluxFieldType};
use serde::Deserialize;
use thiserror::Error;

#[derive(Debug, Diagnostic, Error)]
#[diagnostic(
    code(influxdb3_cache::distinct_cache::cache),
    url("https://github.com/influxdata/influxdb/issues/new?template=bug_report.md")
)]
pub enum CacheError {
    #[error("must pass a non-empty set of column ids")]
    EmptyColumnSet,
    #[error(
        "cannot use a column of type {attempted} in a distinct value cache, only \
                    tags and string fields can be used"
    )]
    NonTagOrStringColumn { attempted: InfluxColumnType },
    #[error("cannot overwrite an an existing cache: {message}")]
    ConfigurationMismatch { message: String },
    #[error("unexpected error")]
    Unexpected(#[from] anyhow::Error),
}

/// A cache for storing distinct values for a set of columns in a table
#[derive(Debug)]
pub(crate) struct DistinctCache {
    time_provider: Arc<dyn TimeProvider>,
    /// The maximum number of unique value combinations in the cache
    max_cardinality: usize,
    /// The maximum age for entries in the cache
    max_age: Duration,
    /// The fixed Arrow schema used to produce record batches from the cache
    schema: SchemaRef,
    /// Holds current state of the cache
    state: DistinctCacheState,
    /// The identifiers of the columns used in the cache
    column_ids: Vec<ColumnId>,
    /// The cache data, stored in a tree
    data: Node,
}

/// Type for tracking the current state of a [`DistinctCache`]
#[derive(Debug, Default)]
struct DistinctCacheState {
    /// The current number of unique value combinations in the cache
    cardinality: usize,
}

/// Arguments to create a new [`DistinctCache`]
#[derive(Debug)]
pub struct CreateDistinctCacheArgs {
    pub table_def: Arc<TableDefinition>,
    pub max_cardinality: MaxCardinality,
    pub max_age: MaxAge,
    pub column_ids: Vec<ColumnId>,
}

#[derive(Debug, Clone, Copy, Deserialize)]
pub struct MaxCardinality(NonZeroUsize);

impl TryFrom<usize> for MaxCardinality {
    type Error = anyhow::Error;

    fn try_from(value: usize) -> Result<Self, Self::Error> {
        Ok(Self(
            NonZeroUsize::try_from(value).context("invalid size provided")?,
        ))
    }
}

const DEFAULT_MAX_CARDINALITY: usize = 100_000;

impl Default for MaxCardinality {
    fn default() -> Self {
        Self(NonZeroUsize::new(DEFAULT_MAX_CARDINALITY).unwrap())
    }
}

impl From<MaxCardinality> for usize {
    fn from(value: MaxCardinality) -> Self {
        value.0.into()
    }
}

const DEFAULT_MAX_AGE: Duration = Duration::from_secs(24 * 60 * 60);

#[derive(Debug, Clone, Copy, Deserialize)]
pub struct MaxAge(Duration);

impl Default for MaxAge {
    fn default() -> Self {
        Self(DEFAULT_MAX_AGE)
    }
}

impl From<MaxAge> for Duration {
    fn from(value: MaxAge) -> Self {
        value.0
    }
}

impl From<Duration> for MaxAge {
    fn from(duration: Duration) -> Self {
        Self(duration)
    }
}

impl From<u64> for MaxAge {
    fn from(seconds: u64) -> Self {
        Self(Duration::new(seconds, 0))
    }
}

impl MaxAge {
    pub(crate) fn as_seconds(&self) -> u64 {
        self.0.as_secs()
    }
}

impl DistinctCache {
    /// Create a new [`DistinctCache`]
    ///
    /// Must pass a non-empty set of [`ColumnId`]s which correspond to valid columns in the provided
    /// [`TableDefinition`].
    pub(crate) fn new(
        time_provider: Arc<dyn TimeProvider>,
        CreateDistinctCacheArgs {
            table_def,
            max_cardinality,
            max_age,
            column_ids,
        }: CreateDistinctCacheArgs,
    ) -> Result<Self, CacheError> {
        if column_ids.is_empty() {
            return Err(CacheError::EmptyColumnSet);
        }
        let _ = table_def.index_column_ids();
        let mut builder = SchemaBuilder::new();
        for id in &column_ids {
            let col = table_def.columns.get(id).with_context(|| {
                format!("invalid column id ({id}) encountered while creating distinct value cache")
            })?;
            let data_type = match col.data_type {
                InfluxColumnType::Tag | InfluxColumnType::Field(InfluxFieldType::String) => {
                    DataType::Utf8View
                }
                attempted => return Err(CacheError::NonTagOrStringColumn { attempted }),
            };

            builder.push(Arc::new(Field::new(col.name.as_ref(), data_type, false)));
        }
        Ok(Self {
            time_provider,
            max_cardinality: max_cardinality.into(),
            max_age: max_age.into(),
            state: DistinctCacheState::default(),
            schema: Arc::new(builder.finish()),
            column_ids,
            data: Node::default(),
        })
    }

    /// Push a [`Row`] from the WAL into the cache, if the row contains all of the cached columns.
    pub(crate) fn push(&mut self, row: &Row) {
        let mut values = Vec::with_capacity(self.column_ids.len());
        for id in &self.column_ids {
            let Some(value) = row
                .fields
                .iter()
                .find(|f| &f.id == id)
                .map(|f| Value::from(&f.value))
            else {
                // ignore the row if it does not contain all columns in the cache:
                return;
            };
            values.push(value);
        }
        let mut target = &mut self.data;
        let mut val_iter = values.into_iter().peekable();
        let mut is_new = false;
        while let (Some(value), peek) = (val_iter.next(), val_iter.peek()) {
            let (last_seen, node) = target.0.entry(value).or_insert_with(|| {
                is_new = true;
                (row.time, peek.is_some().then(Node::default))
            });
            *last_seen = row.time;
            if let Some(node) = node {
                target = node;
            } else {
                break;
            }
        }
        if is_new {
            self.state.cardinality += 1;
        }
    }

    /// Gather a record batch from a cache given the set of predicates
    ///
    /// This assumes the predicates are well behaved, and validated before being passed in. For example,
    /// there cannot be multiple predicates on a single column; the caller needs to validate/transform
    /// the incoming predicates from Datafusion before calling.
    ///
    /// Entries in the cache that have not been seen since before the `max_age` of the cache will be
    /// filtered out of the result.
    pub(crate) fn to_record_batch(
        &self,
        schema: SchemaRef,
        predicates: &IndexMap<ColumnId, Predicate>,
        projection: Option<&[usize]>,
        limit: Option<usize>,
    ) -> Result<RecordBatch, ArrowError> {
        let n_columns = projection
            .as_ref()
            .and_then(|p| p.iter().max().copied())
            .unwrap_or(self.column_ids.len())
            + 1;

        // predicates may not be provided for all columns in the cache, or not be provided in the
        // order of columns in the cache. This re-orders them to the correct order, and fills in any
        // gaps with None.
        let predicates: Vec<Option<&Predicate>> = self
            .column_ids
            .iter()
            .map(|id| predicates.get(id))
            // if a projection was included, we only want to take as many columns as necessary:
            .take(n_columns)
            .collect();

        // Uses a [`StringViewBuilder`] to compose the set of [`RecordBatch`]es. This is convenient for
        // the sake of nested caches, where a predicate on a higher branch in the cache will need to have
        // its value in the outputted batches duplicated.
        let mut builders: Vec<Option<StringViewBuilder>> = (0..self.column_ids.len())
            .map(|i| {
                projection
                    .map(|p| p.contains(&i).then(StringViewBuilder::new))
                    .unwrap_or_else(|| Some(StringViewBuilder::new()))
            })
            .take(n_columns)
            .collect();

        let expired_time_ns = self.expired_time_ns();
        // a limit of usize::MAX would never be reached and therefore considered as no limit
        let limit = limit.unwrap_or(usize::MAX);
        let _ = self.data.evaluate_predicates(
            expired_time_ns,
            predicates.as_slice(),
            limit,
            &mut builders,
        );

        RecordBatch::try_new(
            schema,
            builders
                .into_iter()
                .flatten()
                .map(|mut builder| Arc::new(builder.finish()) as ArrayRef)
                .collect(),
        )
    }

    /// Prune nodes from within the cache
    ///
    /// This first prunes entries that are older than the `max_age` of the cache. If the cardinality
    /// of the cache is still over its `max_cardinality`, it will do another pass to bring the cache
    /// size down.
    pub(crate) fn prune(&mut self) {
        let before_time_ns = self.expired_time_ns();
        let _ = self.data.remove_before(before_time_ns);
        self.state.cardinality = self.data.cardinality();
        if self.state.cardinality > self.max_cardinality {
            let n_to_remove = self.state.cardinality - self.max_cardinality;
            self.data.remove_n_oldest(n_to_remove);
            self.state.cardinality = self.data.cardinality();
        }
    }

    /// Get the nanosecond timestamp as an `i64`, before which, entries that have not been seen
    /// since are considered expired.
    fn expired_time_ns(&self) -> i64 {
        self.time_provider
            .now()
            .checked_sub(self.max_age)
            .expect("max age on cache should not cause an overflow")
            .timestamp_nanos()
    }

    /// Get the arrow [`SchemaRef`] for this cache
    pub(crate) fn arrow_schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    /// Compare the configuration of a given cache, producing a helpful error message if they differ
    pub(crate) fn compare_config(&self, other: &Self) -> Result<(), CacheError> {
        if self.max_cardinality != other.max_cardinality {
            let message = format!(
                "incompatible `max_cardinality`, expected {}, got {}",
                self.max_cardinality, other.max_cardinality
            );
            return Err(CacheError::ConfigurationMismatch { message });
        }
        if self.max_age != other.max_age {
            let message = format!(
                "incompatible `max_age`, expected {}, got {}",
                self.max_age.as_secs(),
                other.max_age.as_secs()
            );
            return Err(CacheError::ConfigurationMismatch { message });
        }
        if self.column_ids != other.column_ids {
            let message = format!(
                "incompatible column id selection, expected {}, got {}",
                self.column_ids
                    .iter()
                    .map(|id| id.to_string())
                    .collect::<Vec<_>>()
                    .join(","),
                other
                    .column_ids
                    .iter()
                    .map(|id| id.to_string())
                    .collect::<Vec<_>>()
                    .join(","),
            );
            return Err(CacheError::ConfigurationMismatch { message });
        }

        Ok(())
    }

    /// Create a [`DistinctCacheDefinition`] from this cache along with the given args
    pub(super) fn to_definition(
        &self,
        table_id: TableId,
        table_name: Arc<str>,
        cache_name: Arc<str>,
    ) -> DistinctCacheDefinition {
        DistinctCacheDefinition {
            table_id,
            table_name,
            cache_name,
            column_ids: self.column_ids.to_vec(),
            max_cardinality: self.max_cardinality,
            max_age_seconds: self.max_age.as_secs(),
        }
    }
}

/// A node in the `data` tree of a [`DistinctCache`]
///
/// Recursive struct holding a [`BTreeMap`] whose keys are the values nested under this node, and
/// whose values hold the last seen time as an [`i64`] of each value, and an optional reference to
/// the node in the next level of the tree.
#[derive(Debug, Default)]
struct Node(BTreeMap<Value, (i64, Option<Node>)>);

impl Node {
    /// Remove all elements before the given nanosecond timestamp returning `true` if the resulting
    /// node is empty.
    fn remove_before(&mut self, time_ns: i64) -> bool {
        self.0.retain(|_, (last_seen, node)| {
            // Note that for a branch node, the `last_seen` time will be the greatest of
            // all its nodes, so an entire branch can be removed if its value has not been seen since
            // before `time_ns`, hence the short-curcuit here:
            *last_seen > time_ns
                || node
                    .as_mut()
                    .is_some_and(|node| node.remove_before(time_ns))
        });
        self.0.is_empty()
    }

    /// Remove the `n_to_remove` oldest entries from the cache
    fn remove_n_oldest(&mut self, n_to_remove: usize) {
        let mut times = BinaryHeap::with_capacity(n_to_remove);
        self.find_n_oldest(n_to_remove, &mut times);
        self.remove_before(*times.peek().unwrap());
    }

    /// Use a binary heap to find the time before which all nodes should be removed
    fn find_n_oldest(&self, n_to_remove: usize, times_heap: &mut BinaryHeap<i64>) {
        self.0
            .values()
            // do not need to add the last_seen time for a branch node to the
            // heap since it will be equal to the greatest of that of its leaves
            .for_each(|(last_seen, node)| {
                if let Some(node) = node {
                    node.find_n_oldest(n_to_remove, times_heap)
                } else if times_heap.len() < n_to_remove {
                    times_heap.push(*last_seen);
                } else if times_heap.peek().is_some_and(|newest| last_seen < newest) {
                    times_heap.pop();
                    times_heap.push(*last_seen);
                }
            });
    }

    /// Get the total count of unique value combinations nested under this node
    ///
    /// Note that this includes expired elements, which still contribute to the total size of the
    /// cache until they are pruned.
    fn cardinality(&self) -> usize {
        self.0
            .values()
            .map(|(_, node)| node.as_ref().map_or(1, |node| node.cardinality()))
            .sum()
    }

    /// Evaluate the set of provided predicates against this node, adding values to the provided
    /// [`StringViewBuilder`]s. Predicates and builders are provided as slices, as this is called
    /// recursively down the cache tree.
    ///
    /// Returns the number of values that were added to the arrow builders.
    ///
    /// # Panics
    ///
    /// This will panic if invalid sized `predicates` and `builders` slices are passed in. When
    /// called from the root [`Node`], their size should be that of the depth of the cache, i.e.,
    /// the number of columns in the cache.
    fn evaluate_predicates(
        &self,
        expired_time_ns: i64,
        predicates: &[Option<&Predicate>],
        mut limit: usize,
        builders: &mut [Option<StringViewBuilder>],
    ) -> usize {
        let mut total_count = 0;
        let Some((predicate, next_predicates)) = predicates.split_first() else {
            return total_count;
        };
        // if there is a predicate, evaluate it, otherwise, just grab everything from the node:
        let values_and_nodes = if let Some(predicate) = predicate {
            self.evaluate_predicate(expired_time_ns, predicate, limit)
        } else {
            self.0
                .iter()
                .filter(|&(_, (t, _))| (t > &expired_time_ns))
                .map(|(v, (_, n))| (v.clone(), n.as_ref()))
                .take(limit)
                .collect()
        };
        let (builder, next_builders) = builders
            .split_first_mut()
            .expect("builders should not be empty");
        // iterate over the resulting set of values and next nodes (if this is a branch), and add
        // the values to the arrow builders:
        for (value, node) in values_and_nodes {
            if let Some(node) = node {
                let count = node.evaluate_predicates(
                    expired_time_ns,
                    next_predicates,
                    limit,
                    next_builders,
                );
                if count > 0 {
                    if let Some(builder) = builder {
                        // we are not on a terminal node in the cache, so create a block, as this value
                        // repeated `count` times, i.e., depending on how many values come out of
                        // subsequent nodes:
                        let block = builder.append_block(value.0.as_bytes().into());
                        for _ in 0..count {
                            builder
                                .try_append_view(block, 0u32, value.0.len() as u32)
                                .expect("append view for known valid block, offset and length");
                        }
                    }
                    total_count += count;
                } else if next_predicates.is_empty() && next_builders.is_empty() {
                    if let Some(builder) = builder {
                        builder.append_value(value.0);
                    }
                }
                if let Some(new_limit) = limit.checked_sub(count) {
                    limit = new_limit;
                } else {
                    break;
                }
            } else {
                if let Some(builder) = builder {
                    builder.append_value(value.0);
                }
                total_count += 1;
            }
        }
        total_count
    }

    /// Evaluate a predicate against a [`Node`], producing a list of [`Value`]s and, if this is a
    /// branch node in the cache tree, a reference to the next [`Node`].
    fn evaluate_predicate(
        &self,
        expired_time_ns: i64,
        predicate: &Predicate,
        limit: usize,
    ) -> Vec<(Value, Option<&Node>)> {
        match &predicate {
            Predicate::In(in_list) => in_list
                .iter()
                .filter_map(|v| {
                    self.0.get_key_value(v).and_then(|(v, (t, n))| {
                        (t > &expired_time_ns).then(|| (v.clone(), n.as_ref()))
                    })
                })
                .take(limit)
                .collect(),
            Predicate::NotIn(not_in_set) => self
                .0
                .iter()
                .filter(|(v, (t, _))| t > &expired_time_ns && !not_in_set.contains(v))
                .map(|(v, (_, n))| (v.clone(), n.as_ref()))
                .take(limit)
                .collect(),
        }
    }
}

/// A cache value, which for now, only holds strings
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Clone, Hash)]
pub(crate) struct Value(Arc<str>);

impl From<&FieldData> for Value {
    fn from(field: &FieldData) -> Self {
        match field {
            FieldData::Key(s) => Self(Arc::from(s.as_str())),
            FieldData::Tag(s) => Self(Arc::from(s.as_str())),
            FieldData::String(s) => Self(Arc::from(s.as_str())),
            FieldData::Timestamp(_)
            | FieldData::Integer(_)
            | FieldData::UInteger(_)
            | FieldData::Float(_)
            | FieldData::Boolean(_) => panic!("unexpected field type for distinct value cache"),
        }
    }
}

impl From<String> for Value {
    fn from(value: String) -> Self {
        Self(Arc::from(value.as_str()))
    }
}

/// A predicate that can be applied when gathering [`RecordBatch`]es from a [`DistinctCache`]
///
/// This is intended to be derived from a set of filter expressions in Datafusion by analyzing
/// them with a `LiteralGuarantee`.
///
/// This uses a `BTreeSet` to store the values so that they are iterated over in sorted order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Predicate {
    In(BTreeSet<Value>),
    NotIn(BTreeSet<Value>),
}

impl std::fmt::Display for Predicate {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Predicate::In(_) => write!(f, "IN (")?,
            Predicate::NotIn(_) => write!(f, "NOT IN (")?,
        }
        let mut values = self.values();
        while let Some(v) = values.next() {
            write!(f, "{}", v.0)?;
            if values.size_hint().0 > 0 {
                write!(f, ",")?;
            }
        }

        write!(f, ")")
    }
}

impl Predicate {
    pub(crate) fn new_in(in_vals: impl IntoIterator<Item: Into<Arc<str>>>) -> Self {
        Self::In(in_vals.into_iter().map(Into::into).map(Value).collect())
    }

    pub(crate) fn new_not_in(in_vals: impl IntoIterator<Item: Into<Arc<str>>>) -> Self {
        Self::NotIn(in_vals.into_iter().map(Into::into).map(Value).collect())
    }

    fn values(&self) -> impl Iterator<Item = &Value> {
        match self {
            Predicate::In(vals) => vals.iter(),
            Predicate::NotIn(vals) => vals.iter(),
        }
    }
}
