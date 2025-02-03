use std::{
    collections::{HashMap, VecDeque},
    ops::{Deref, DerefMut},
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use anyhow::{bail, Context as AnyhowContext};
use arrow::{
    array::{as_string_array, ArrayRef, AsArray},
    compute::{cast_with_options, CastOptions},
    datatypes::{
        DataType, Float16Type, Float32Type, Float64Type, Int16Type, Int32Type, Int64Type, Int8Type,
        TimeUnit, TimestampMicrosecondType, TimestampMillisecondType, TimestampNanosecondType,
        TimestampSecondType, UInt16Type, UInt32Type, UInt64Type, UInt8Type,
    },
    record_batch::RecordBatch,
};
use arrow_schema::{Field, SchemaRef};
use bytes::Bytes;
use chrono::{format::SecondsFormat, DateTime};
use datafusion::physical_plan::SendableRecordBatchStream;
use futures::{ready, stream::Fuse, Stream, StreamExt};
use hyper::http::HeaderValue;
use hyper::{header::ACCEPT, header::CONTENT_TYPE, Body, Request, Response, StatusCode};
use influxdb_influxql_parser::select::{Dimension, GroupByClause};
use iox_time::TimeProvider;
use miette::Diagnostic;
use observability_deps::tracing::info;
use regex::Regex;
use schema::{InfluxColumnType, INFLUXQL_MEASUREMENT_COLUMN_NAME, TIME_COLUMN_NAME};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::{Error, HttpApi, Result};

const DEFAULT_CHUNK_SIZE: usize = 10_000;

impl<T> HttpApi<T>
where
    T: TimeProvider,
{
    /// Implements the v1 query API for InfluxDB
    ///
    /// Accepts the URL parameters, defined by [`QueryParams`]), and returns a stream
    /// of [`QueryResponse`]s. If the `chunked` parameter is set to `true`, then the
    /// response stream will be chunked into chunks of size `chunk_size`, if provided,
    /// or 10,000. For InfluxQL queries that select from multiple measurements, chunks
    /// will be split on the `chunk_size`, or series, whichever comes first.
    pub(super) async fn v1_query(&self, req: Request<Body>) -> Result<Response<Body>> {
        // extract params first from URI:
        let uri_params = QueryParams::from_request_uri(&req)?;
        // determine the format from the request headers now because we need to consume req to get
        // the body:
        let mut format = QueryFormat::from_request(&req)?;
        // now get the parameters provided in the body:
        let body = self.read_body(req).await?;
        let body_params = serde_urlencoded::from_bytes::<QueryParams>(&body)?;
        // now combine them, overwriting parameters from the uri with those from the body:
        let combined_params = body_params.combine(uri_params);
        // qualify the combination to ensure there is a query string:
        let qualified_params = combined_params.qualify()?;
        info!(?qualified_params, "handle v1 query API");

        let QualifiedQueryParams {
            chunk_size,
            database,
            epoch,
            pretty,
            query,
        } = qualified_params;

        if pretty {
            format = format.to_pretty();
        }

        // TODO - Currently not supporting parameterized queries, see
        //        https://github.com/influxdata/influxdb/issues/24805
        let (stream, group_by) = self.query_influxql_inner(database, &query, None).await?;
        let stream = QueryResponseStream::new(0, stream, chunk_size, format, epoch, group_by)
            .map_err(QueryError)?;
        let body = Body::wrap_stream(stream);

        Ok(Response::builder()
            .status(StatusCode::OK)
            .header(CONTENT_TYPE, format.as_content_type())
            .body(body)
            .unwrap())
    }
}

/// Query parameters for the v1 /query API
///
/// The original API supports a `u` parameter, for "username", as well as a `p`, for "password".
/// The password is extracted upstream, and username is ignored.
///
/// This makes all fields optional, so that the params can be parsed from both the request URI as
/// well as the request body, and then combined. This must be qualified via [`QueryParams::qualify`]
/// to ensure validity of the incoming request.
#[derive(Debug, Deserialize)]
struct QueryParams {
    /// Chunk the response into chunks of size `chunk_size`, or 10,000, or by series
    chunked: Option<bool>,
    /// Define the number of records that will go into a chunk
    chunk_size: Option<usize>,
    /// Database to perform the query against
    ///
    /// This is optional because the query string may specify the database
    #[serde(rename = "db")]
    database: Option<String>,
    /// Map timestamps to UNIX epoch time, with the given precision
    #[allow(dead_code)]
    epoch: Option<Precision>,
    /// Format the JSON outputted in pretty format
    pretty: Option<bool>,
    /// The InfluxQL query string
    ///
    /// This is optional only for deserialization, since we first extract from the request URI
    /// then from the body and combine the two sources.
    #[serde(rename = "q")]
    query: Option<String>,
}

impl QueryParams {
    /// Extract [`QueryParams`] from an HTTP [`Request`]
    fn from_request_uri(req: &Request<Body>) -> Result<Self> {
        let query = req.uri().query().unwrap_or_default();
        serde_urlencoded::from_str(query).map_err(Into::into)
    }

    /// Combine two [`QueryParams`] objects, prioritizing values in `self` over `other`.
    fn combine(self, other: Self) -> Self {
        Self {
            chunked: self.chunked.or(other.chunked),
            chunk_size: self.chunk_size.or(other.chunk_size),
            database: self.database.or(other.database),
            epoch: self.epoch.or(other.epoch),
            pretty: self.pretty.or(other.pretty),
            query: self.query.or(other.query),
        }
    }

    /// Qualify this [`QueryParams`] to determine chunk size and ensure a query string was provided
    fn qualify(self) -> Result<QualifiedQueryParams> {
        let chunk_size = self
            .chunked
            .unwrap_or_default()
            .then(|| self.chunk_size.unwrap_or(DEFAULT_CHUNK_SIZE));
        let query = self.query.ok_or(Error::MissingQueryV1Params)?;
        Ok(QualifiedQueryParams {
            chunk_size,
            database: self.database,
            epoch: self.epoch,
            pretty: self.pretty.unwrap_or_default(),
            query,
        })
    }
}

/// Qualified version of [`QueryParams`]
#[derive(Debug)]
struct QualifiedQueryParams {
    chunk_size: Option<usize>,
    database: Option<String>,
    epoch: Option<Precision>,
    pretty: bool,
    query: String,
}

/// Enum representing the query format for the v1/query API.
///
/// The original API supports CSV, JSON, and "pretty" JSON formats.
#[derive(Debug, Deserialize, Clone, Copy)]
#[serde(rename_all = "snake_case")]
pub(crate) enum QueryFormat {
    Csv,
    Json,
    JsonPretty,
}

impl QueryFormat {
    /// Returns the content type as a string slice for the query format.
    ///
    /// Maps the `QueryFormat` variants to their corresponding MIME types as strings.
    /// This is useful for setting the `Content-Type` header in HTTP responses.
    fn as_content_type(&self) -> &str {
        match self {
            Self::Csv => "application/csv",
            Self::Json | Self::JsonPretty => "application/json",
        }
    }

    /// Checks if the query format is 'JsonPretty'.
    ///
    /// Determines if the `QueryFormat` is `JsonPretty`, which indicates that the JSON
    /// output should be formatted in a human-readable way. Returns `true` if the
    /// format is `JsonPretty`, otherwise returns `false`.
    fn is_pretty(&self) -> bool {
        match self {
            Self::Csv | Self::Json => false,
            Self::JsonPretty => true,
        }
    }

    /// Extracts the [`QueryFormat`] from an HTTP [`Request`].
    ///
    /// The function inspects the `Accept` header of the request to determine the
    /// format, defaulting to JSON if no specific format is requested. If the format
    /// is invalid or non-UTF8, an error is returned.
    fn from_request(req: &Request<Body>) -> Result<Self> {
        let mime_type = req.headers().get(ACCEPT).map(HeaderValue::as_bytes);
        match mime_type {
            Some(b"application/csv" | b"text/csv") => Ok(Self::Csv),
            // the default is JSON:
            Some(b"application/json" | b"*/*") | None => Ok(Self::Json),
            Some(mime_type) => match String::from_utf8(mime_type.to_vec()) {
                Ok(s) => Err(Error::InvalidMimeType(s)),
                Err(e) => Err(Error::NonUtf8MimeType(e)),
            },
        }
    }

    /// Convert this from JSON to pretty printed JSON
    fn to_pretty(self) -> Self {
        match self {
            Self::Csv => Self::Csv,
            Self::Json | Self::JsonPretty => Self::JsonPretty,
        }
    }
}

/// UNIX epoch precision
#[derive(Debug, Deserialize, Clone, Copy)]
enum Precision {
    #[serde(rename = "ns")]
    Nanoseconds,
    #[serde(rename = "u", alias = "µ")]
    Microseconds,
    #[serde(rename = "ms")]
    Milliseconds,
    #[serde(rename = "s")]
    Seconds,
    #[serde(rename = "m")]
    Minutes,
    #[serde(rename = "h")]
    Hours,
}

/// Error type for the v1 API
///
/// This is used to catch errors that occur during the streaming process.
/// [`anyhow::Error`] is used as a catch-all because if anything fails during
/// that process it will result in a 500 INTERNAL ERROR.
#[derive(Debug, Diagnostic, thiserror::Error)]
#[diagnostic(
    code(influxdb3_cache::distinct_cache::cache),
    url("https://github.com/influxdata/influxdb/issues/new?template=bug_report.md")
)]
#[error("unexpected query error")]
pub struct QueryError(#[from] anyhow::Error);

/// The response structure returned by the v1 query API
///
/// The `pretty` parameter is used during serizliaztion to determine if JSON
/// is pretty formatted or not.
#[derive(Debug, Serialize)]
struct QueryResponse {
    results: Vec<StatementResponse>,
    #[serde(skip_serializing)]
    format: QueryFormat,
}

/// Convert `QueryResponse` to [`Bytes`] for `hyper`'s [`Body::wrap_stream`] method
impl From<QueryResponse> for Bytes {
    fn from(s: QueryResponse) -> Self {
        /// Convert a [`QueryResponse`] to a JSON byte vector.
        ///
        /// This function serializes the `QueryResponse` to JSON. If the format is
        /// `JsonPretty`, it will produce human-readable JSON, otherwise it produces
        /// compact JSON.
        fn to_json(s: QueryResponse) -> Vec<u8> {
            if s.format.is_pretty() {
                serde_json::to_vec_pretty(&s)
                    .expect("Failed to serialize QueryResponse to pretty JSON")
            } else {
                serde_json::to_vec(&s).expect("Failed to serialize QueryResponse to JSON")
            }
        }

        /// Convert a [`QueryResponse`] to a CSV byte vector.
        ///
        /// This function serializes the `QueryResponse` to CSV format. It dynamically
        /// extracts column names from the first series and writes the header and data
        /// rows to the CSV writer.
        fn to_csv(s: QueryResponse) -> Vec<u8> {
            let mut wtr = csv::WriterBuilder::new()
                .quote_style(csv::QuoteStyle::Never)
                .from_writer(vec![]);
            // Extract column names dynamically from the first series
            let mut headers = vec!["name", "tags"];
            if let Some(first_statement) = s.results.first() {
                if let Some(first_series) = first_statement.series.first() {
                    headers.extend(first_series.columns.iter().map(|s| s.as_str()));
                }
            }
            // Write the header
            wtr.write_record(&headers)
                .expect("Failed to write CSV header");

            // Iterate through the hierarchical structure of QueryResponse to write data
            // to the CSV writer. The loop processes each statement, series, and row to
            // build and write CSV records. Each record is initialized with the series name
            // and an empty tag field, followed by the string representations of the row's values.
            // Finally, the record is written to the CSV writer
            for statement in s.results {
                for series in statement.series {
                    for row in series.values {
                        let mut record = vec![series.name.clone(), "".to_string()];
                        for v in row.0 {
                            record.push(match v {
                                Value::String(s) => s.clone(),
                                _ => v.to_string(),
                            });
                        }
                        wtr.write_record(&record)
                            .expect("Failed to write CSV record");
                    }
                }
            }

            // Flush the CSV writer to ensure all data is written
            wtr.flush().expect("flush csv writer");

            wtr.into_inner().expect("into_inner from csv writer")
        }
        /// Extend a byte vector with CRLF and convert it to [`Bytes`].
        ///
        /// This function appends a CRLF (`\r\n`) sequence to the given byte vector
        /// and converts it to a `Bytes` object.
        fn extend_with_crlf(mut bytes: Vec<u8>) -> Bytes {
            bytes.extend_from_slice(b"\r\n");
            Bytes::from(bytes)
        }

        match s.format {
            QueryFormat::Json | QueryFormat::JsonPretty => extend_with_crlf(to_json(s)),
            QueryFormat::Csv => extend_with_crlf(to_csv(s)),
        }
    }
}

/// The response to an individual InfluxQL query
#[derive(Debug, Serialize)]
struct StatementResponse {
    statement_id: usize,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    series: Vec<Series>,
}

/// The records produced for a single time series (measurement)
#[derive(Debug, Serialize)]
struct Series {
    name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    tags: Option<HashMap<String, Option<String>>>,
    columns: Vec<String>,
    values: Vec<Row>,
}

/// A single row, or record in a time series
#[derive(Debug, Serialize)]
struct Row(Vec<Value>);

impl Deref for Row {
    type Target = Vec<Value>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for Row {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

/// A buffer for storing records from a stream of [`RecordBatch`]es
///
/// The optional `size` indicates whether this is operating in `chunked` mode (see
/// [`QueryResponseStream`]), and when specified, gives the size of chunks that will
/// be emitted.
#[derive(Debug)]
struct ChunkBuffer {
    size: Option<usize>,
    series: VecDeque<(String, BufferGroupByTagSet, Vec<Row>)>,
}

impl ChunkBuffer {
    fn new(size: Option<usize>) -> Self {
        Self {
            size,
            series: VecDeque::new(),
        }
    }

    /// Get the name of the current measurement [`Series`] being streamed
    fn current_measurement_name(&self) -> Option<&str> {
        self.series.front().map(|(n, _, _)| n.as_str())
    }

    /// For queries that produce multiple [`Series`], this will be called when
    /// the current series is completed streaming
    fn push_next_measurement<S: Into<String>>(&mut self, name: S) {
        self.series.push_front((name.into(), None, vec![]));
    }

    /// Push a new [`Row`] into the current measurement [`Series`]
    ///
    /// If the stream is producing tags that are part of a `GROUP BY` clause, then `group_by` should
    /// hold a map of those tag keys to tag values for the given row.
    fn push_row(
        &mut self,
        group_by: Option<HashMap<String, Option<String>>>,
        row: Row,
    ) -> Result<(), anyhow::Error> {
        let (_, tags, rows) = self
            .series
            .front()
            .context("tried to push row with no measurements buffered")?;

        // Usually series are split on the measurement name. This functin is not concerned with
        // that split, as the caller does that. However, if we are processing a query with a `GROUP BY`
        // clause, then we make the decision here. If the incoming `group_by` tag key/value pairs do
        // not match the those for the current row set, then we need to start a new entry in the
        // `series` on the chunk buffer.
        use BufferGroupByDecision::*;
        let group_by_decision = match (tags, &group_by) {
            (None, None) => NotAGroupBy,
            (Some(tags), Some(group_by)) => {
                if group_by.len() == tags.len() {
                    if group_by == tags {
                        NewRowInExistingSet
                    } else {
                        NewSet
                    }
                } else {
                    bail!(
                        "group by columns in query result and chunk buffer are not the same size"
                    );
                }
            }
            (None, Some(_)) => {
                if rows.is_empty() {
                    FirstRowInSeries
                } else {
                    bail!("received inconsistent group by tags in query result");
                }
            }
            (Some(_), None) => bail!(
                "chunk buffer expects group by tags but none were present in the query result"
            ),
        };

        match group_by_decision {
            NotAGroupBy | NewRowInExistingSet => self.series.front_mut().unwrap().2.push(row),
            FirstRowInSeries => {
                let (_, tags, rows) = self.series.front_mut().unwrap();
                *tags = group_by;
                rows.push(row);
            }
            NewSet => {
                let name = self.series.front().unwrap().0.clone();
                self.series.push_front((name, group_by, vec![row]));
            }
        }

        Ok(())
    }

    /// Flush a single chunk from the [`ChunkBuffer`], if possible
    fn flush_one(&mut self) -> Option<(String, BufferGroupByTagSet, Vec<Row>)> {
        if !self.can_flush() {
            return None;
        }
        // we can flush, so unwrap is safe:
        let size = self.size.unwrap();
        if self
            .series
            .back()
            .is_some_and(|(_, _, rows)| rows.len() <= size)
        {
            // the back series is smaller than the chunk size, so we just
            // pop and take the whole thing:
            self.series.pop_back()
        } else {
            // only drain a chunk's worth from the back series:
            self.series.back_mut().map(|(name, tags, rows)| {
                (name.to_owned(), tags.clone(), rows.drain(..size).collect())
            })
        }
    }

    /// The [`ChunkBuffer`] is operating in chunked mode, and can flush a chunk
    fn can_flush(&self) -> bool {
        if let (Some(size), Some(m)) = (self.size, self.series.back()) {
            m.2.len() >= size || self.series.len() > 1
        } else {
            false
        }
    }

    /// The [`ChunkBuffer`] is empty
    fn is_empty(&self) -> bool {
        self.series.is_empty()
    }
}

/// Convenience type for representing an optional map of tag name to optional tag values
type BufferGroupByTagSet = Option<HashMap<String, Option<String>>>;

/// Decide how to handle an incoming set of `GROUP BY` tag key value pairs when pushing a row into
/// the `ChunkBuffer`
enum BufferGroupByDecision {
    /// The query is not using a `GROUP BY` with tags
    NotAGroupBy,
    /// This is the first time a row has been pushed to the series with this `GROUP BY` tag
    /// key/value combination
    FirstRowInSeries,
    /// Still adding rows to the current set of `GROUP BY` tag key/value pairs
    NewRowInExistingSet,
    /// The incoming set of `GROUP BY` tag key/value pairs do not match, so we need to start a
    /// new row set in the series.
    NewSet,
}

/// The state of the [`QueryResponseStream`]
enum State {
    /// The initial state of the stream; no query results have been streamed
    Initialized,
    /// Rows have been buffered and/or flushed; query results are being streamed
    Buffering,
    /// The stream is done
    Done,
}

impl State {
    fn is_initialized(&self) -> bool {
        matches!(self, Self::Initialized)
    }
}

/// A wrapper around a [`SendableRecordBatchStream`] that buffers streamed
/// [`RecordBatch`]es and outputs [`QueryResponse`]s
///
/// Can operate in `chunked` mode, in which case, the `chunk_size` provided
/// will determine how often [`QueryResponse`]s are emitted. When not in
/// `chunked` mode, the entire input stream of [`RecordBatch`]es will be buffered
/// into memory before being emitted.
///
/// `format` will emit CSV, JSON or pretty formatted JSON.
///
/// Providing an `epoch` [`Precision`] will have the `time` column values emitted
/// as UNIX epoch times with the given precision.
///
/// The input stream is wrapped in [`Fuse`], because of the [`Stream`] implementation
/// below, it is possible that the input stream is polled after completion.
struct QueryResponseStream {
    buffer: ChunkBuffer,
    input: Fuse<SendableRecordBatchStream>,
    column_map: ColumnMap,
    statement_id: usize,
    format: QueryFormat,
    epoch: Option<Precision>,
    state: State,
}

impl QueryResponseStream {
    /// Create a new [`QueryResponseStream`]
    ///
    /// Specifying a `chunk_size` will have the stream operate in `chunked` mode.
    fn new(
        statement_id: usize,
        input: SendableRecordBatchStream,
        chunk_size: Option<usize>,
        format: QueryFormat,
        epoch: Option<Precision>,
        group_by_clause: Option<GroupByClause>,
    ) -> Result<Self, anyhow::Error> {
        let schema = input.schema();
        let buffer = ChunkBuffer::new(chunk_size);
        let column_map = ColumnMap::new(schema, group_by_clause)?;
        Ok(Self {
            buffer,
            column_map,
            input: input.fuse(),
            format,
            statement_id,
            epoch,
            state: State::Initialized,
        })
    }

    fn buffer_record_batch(&mut self, mut batch: RecordBatch) -> Result<(), anyhow::Error> {
        if self.epoch.is_some() {
            // If the `epoch` is specified, then we cast the `time` column into an Int64.
            // This will be in nanoseconds. The conversion to the given epoch precision
            // happens below, when processing the JSON rows
            batch = RecordBatch::try_from_iter(batch.schema().fields.iter().map(|f| {
                let name = f.name();
                let column = batch.column_by_name(name).unwrap();
                (
                    name,
                    if name == TIME_COLUMN_NAME {
                        // unwrap should be safe here because the time column cast to Int64
                        cast_with_options(column, &DataType::Int64, &CastOptions::default())
                            .unwrap()
                    } else {
                        Arc::clone(column)
                    },
                )
            }))
            .context("failed to cast batch time column with `epoch` parameter specified")?;
        }
        let column_map = &self.column_map;
        let columns = batch.columns();
        let schema = batch.schema();

        for row_index in 0..batch.num_rows() {
            let mut row = vec![Value::Null; column_map.row_size()];
            let mut tags = None;

            for (col_index, column) in columns.iter().enumerate() {
                let field = schema.field(col_index);
                let column_name = field.name();

                let mut cell_value = if !column.is_valid(row_index) {
                    continue;
                } else {
                    cast_column_value(column, row_index)?
                };

                // Handle the special case for the measurement column
                if column_name == INFLUXQL_MEASUREMENT_COLUMN_NAME {
                    if let Value::String(ref measurement_name) = cell_value {
                        if self.buffer.current_measurement_name().is_none()
                            || self
                                .buffer
                                .current_measurement_name()
                                .is_some_and(|n| n != measurement_name)
                        {
                            // we are on the "iox::measurement" column, which gives the name of the time series
                            // if we are on the first row, or if the measurement changes, we push into the
                            // buffer queue
                            self.buffer.push_next_measurement(measurement_name);
                        }
                    }
                    continue;
                }
                if column_name == TIME_COLUMN_NAME {
                    if let Some(precision) = self.epoch {
                        cell_value = convert_ns_epoch(cell_value, precision)?
                    }
                }
                if let Some(index) = column_map.as_row_index(column_name) {
                    row[index] = cell_value;
                } else if column_map.is_group_by_tag(column_name) {
                    let tag_val = match cell_value {
                        Value::Null => None,
                        Value::String(s) => Some(s),
                        other => bail!(
                            "tag column {column_name} expected as a string or null, got {other:?}"
                        ),
                    };
                    tags.get_or_insert_with(HashMap::new)
                        .insert(column_name.to_string(), tag_val);
                } else if column_map.is_orphan_group_by_tag(column_name) {
                    tags.get_or_insert_with(HashMap::new)
                        .insert(column_name.to_string(), Some(String::default()));
                } else {
                    bail!("failed to retrieve column position for column with name {column_name}");
                }
            }
            self.buffer.push_row(tags.take(), Row(row))?;
        }
        Ok(())
    }

    fn column_names(&self) -> Vec<String> {
        self.column_map.row_column_names()
    }

    /// Flush a single chunk, or time series, when operating in chunked mode
    fn flush_one(&mut self) -> QueryResponse {
        let columns = self.column_names();
        // this unwrap is okay because we only ever call flush_one
        // after calling can_flush on the buffer:
        let (name, tags, values) = self.buffer.flush_one().unwrap();
        let series = vec![Series {
            name,
            tags,
            columns,
            values,
        }];
        self.state = State::Buffering;
        QueryResponse {
            results: vec![StatementResponse {
                statement_id: self.statement_id,
                series,
            }],
            format: self.format,
        }
    }

    /// Flush the entire buffer
    fn flush_all(&mut self) -> QueryResponse {
        let columns = self.column_names();
        let series = self
            .buffer
            .series
            .drain(..)
            .map(|(name, tags, values)| Series {
                name,
                tags,
                columns: columns.clone(),
                values,
            })
            .collect();
        self.state = State::Buffering;
        QueryResponse {
            results: vec![StatementResponse {
                statement_id: self.statement_id,
                series,
            }],
            format: self.format,
        }
    }

    /// Flush an empty query result
    fn flush_empty(&mut self) -> QueryResponse {
        self.state = State::Done;
        QueryResponse {
            results: vec![StatementResponse {
                statement_id: self.statement_id,
                series: vec![],
            }],
            format: self.format,
        }
    }
}

/// Convert an epoch time in nanoseconds to the provided precision
fn convert_ns_epoch(value: Value, precision: Precision) -> Result<Value, anyhow::Error> {
    let epoch_ns = value
        .as_i64()
        .context("the provided nanosecond epoch time was not a valid i64")?;
    Ok(match precision {
        Precision::Nanoseconds => epoch_ns,
        Precision::Microseconds => epoch_ns / 1_000,
        Precision::Milliseconds => epoch_ns / 1_000_000,
        Precision::Seconds => epoch_ns / 1_000_000_000,
        Precision::Minutes => epoch_ns / (1_000_000_000 * 60),
        Precision::Hours => epoch_ns / (1_000_000_000 * 60 * 60),
    }
    .into())
}

/// Converts a value from an Arrow `ArrayRef` at a given row index into a `serde_json::Value`.
///
/// This function handles various Arrow data types, converting them into their corresponding
/// JSON representations. For unsupported data types, it returns an error using the `anyhow` crate.
fn cast_column_value(column: &ArrayRef, row_index: usize) -> Result<Value, anyhow::Error> {
    let value = match column.data_type() {
        DataType::Boolean => Value::Bool(column.as_boolean().value(row_index)),
        DataType::Null => Value::Null,
        DataType::Int8 => Value::Number(column.as_primitive::<Int8Type>().value(row_index).into()),
        DataType::Int16 => {
            Value::Number(column.as_primitive::<Int16Type>().value(row_index).into())
        }
        DataType::Int32 => {
            Value::Number(column.as_primitive::<Int32Type>().value(row_index).into())
        }
        DataType::Int64 => {
            Value::Number(column.as_primitive::<Int64Type>().value(row_index).into())
        }
        DataType::UInt8 => {
            Value::Number(column.as_primitive::<UInt8Type>().value(row_index).into())
        }
        DataType::UInt16 => {
            Value::Number(column.as_primitive::<UInt16Type>().value(row_index).into())
        }
        DataType::UInt32 => {
            Value::Number(column.as_primitive::<UInt32Type>().value(row_index).into())
        }
        DataType::UInt64 => {
            Value::Number(column.as_primitive::<UInt64Type>().value(row_index).into())
        }
        DataType::Float16 => Value::Number(
            serde_json::Number::from_f64(
                column
                    .as_primitive::<Float16Type>()
                    .value(row_index)
                    .to_f64(),
            )
            .context("failed to downcast Float16 column")?,
        ),
        DataType::Float32 => Value::Number(
            serde_json::Number::from_f64(
                column.as_primitive::<Float32Type>().value(row_index).into(),
            )
            .context("failed to downcast Float32 column")?,
        ),
        DataType::Float64 => Value::Number(
            serde_json::Number::from_f64(column.as_primitive::<Float64Type>().value(row_index))
                .context("failed to downcast Float64 column")?,
        ),
        DataType::Utf8 => Value::String(column.as_string::<i32>().value(row_index).to_string()),
        DataType::LargeUtf8 => {
            Value::String(column.as_string::<i64>().value(row_index).to_string())
        }
        DataType::Dictionary(key, value) => match (key.as_ref(), value.as_ref()) {
            (DataType::Int32, DataType::Utf8) => {
                let dict_array = column.as_dictionary::<Int32Type>();
                let keys = dict_array.keys();
                let values = as_string_array(dict_array.values());
                Value::String(values.value(keys.value(row_index) as usize).to_string())
            }
            _ => Value::Null,
        },
        DataType::Timestamp(TimeUnit::Nanosecond, None) => Value::String(
            DateTime::from_timestamp_nanos(
                column
                    .as_primitive::<TimestampNanosecondType>()
                    .value(row_index),
            )
            .to_rfc3339_opts(SecondsFormat::AutoSi, true),
        ),
        DataType::Timestamp(TimeUnit::Microsecond, None) => Value::String(
            DateTime::from_timestamp_micros(
                column
                    .as_primitive::<TimestampMicrosecondType>()
                    .value(row_index),
            )
            .context("failed to downcast TimestampMicrosecondType column")?
            .to_rfc3339_opts(SecondsFormat::AutoSi, true),
        ),
        DataType::Timestamp(TimeUnit::Millisecond, None) => Value::String(
            DateTime::from_timestamp_millis(
                column
                    .as_primitive::<TimestampMillisecondType>()
                    .value(row_index),
            )
            .context("failed to downcast TimestampNillisecondType column")?
            .to_rfc3339_opts(SecondsFormat::AutoSi, true),
        ),
        DataType::Timestamp(TimeUnit::Second, None) => Value::String(
            DateTime::from_timestamp(
                column
                    .as_primitive::<TimestampSecondType>()
                    .value(row_index),
                0,
            )
            .context("failed to downcast TimestampSecondType column")?
            .to_rfc3339_opts(SecondsFormat::AutoSi, true),
        ),
        t => bail!("Unsupported data type: {:?}", t),
    };
    Ok(value)
}

/// Map column names to their respective [`ColumnType`]
struct ColumnMap {
    /// The map of column names to column types
    map: HashMap<String, ColumnType>,
    /// How many columns are in the `values` set, i.e., that are not `GROUP BY` tags
    row_size: usize,
}

/// A column's type in the context of a v1 /query API response
enum ColumnType {
    /// A value to be included in the `series.[].values` array, at the given `index`
    Value { index: usize },
    /// A tag that is part of the `GROUP BY` clause, either explicitly, or by a regex/wildcard match
    /// and is included in the `series.[].tags` map
    GroupByTag,
    /// This is a case where a GROUP BY clause contains a field which doesn't exist in the table
    ///
    /// For example,
    /// ```text
    /// select * from foo group by t1, t2
    /// ```
    /// If `t1` is a tag in the table, but `t2` is not, nor is a field in the table, then the v1
    /// /query API response will include `t2` in the `series.[].tags` property in the results with
    /// an empty string for a value (`""`).
    OrphanGroupByTag,
}

impl ColumnMap {
    /// Create a new `ColumnMap`
    fn new(
        schema: SchemaRef,
        group_by_clause: Option<GroupByClause>,
    ) -> Result<Self, anyhow::Error> {
        let mut map = HashMap::new();
        let group_by = if let Some(clause) = group_by_clause {
            GroupByEval::from_clause(clause)?
        } else {
            None
        };
        let mut index = 0;
        for field in schema
            .fields()
            .into_iter()
            .filter(|f| f.name() != INFLUXQL_MEASUREMENT_COLUMN_NAME)
        {
            if group_by
                .as_ref()
                .is_some_and(|gb| is_tag(field) && gb.evaluate_tag(field.name()))
            {
                map.insert(field.name().to_string(), ColumnType::GroupByTag);
            } else if group_by.as_ref().is_some_and(|gb| {
                field.metadata().is_empty() && gb.contains_explicit_col_name(field.name())
            }) {
                map.insert(field.name().to_string(), ColumnType::OrphanGroupByTag);
            } else {
                map.insert(field.name().to_string(), ColumnType::Value { index });
                index += 1;
            }
        }
        Ok(Self {
            map,
            row_size: index,
        })
    }

    fn row_size(&self) -> usize {
        self.row_size
    }

    fn row_column_names(&self) -> Vec<String> {
        let mut result = vec![None; self.row_size];
        self.map.iter().for_each(|(name, c)| {
            if let ColumnType::Value { index } = c {
                result[*index].replace(name.to_owned());
            }
        });
        result.into_iter().flatten().collect()
    }

    /// If this column is part of the `values` row data, get its index, or `None` otherwise
    fn as_row_index(&self, column_name: &str) -> Option<usize> {
        self.map.get(column_name).and_then(|col| match col {
            ColumnType::Value { index } => Some(*index),
            ColumnType::GroupByTag | ColumnType::OrphanGroupByTag => None,
        })
    }

    /// This column is a `GROUP BY` tag
    fn is_group_by_tag(&self, column_name: &str) -> bool {
        self.map
            .get(column_name)
            .is_some_and(|col| matches!(col, ColumnType::GroupByTag))
    }

    /// This column is an orphan `GROUP BY` tag
    fn is_orphan_group_by_tag(&self, column_name: &str) -> bool {
        self.map
            .get(column_name)
            .is_some_and(|col| matches!(col, ColumnType::OrphanGroupByTag))
    }
}

// TODO: this is defined in schema crate, so needs to be made pub there:
const COLUMN_METADATA_KEY: &str = "iox::column::type";

/// Decide based on metadata if this [`Field`] is a tag column
fn is_tag(field: &Arc<Field>) -> bool {
    field
        .metadata()
        .get(COLUMN_METADATA_KEY)
        .map(|s| InfluxColumnType::try_from(s.as_str()))
        .transpose()
        .ok()
        .flatten()
        .is_some_and(|t| matches!(t, InfluxColumnType::Tag))
}

/// Derived from a [`GroupByClause`] and used to evaluate whether a given tag column is part of the
/// `GROUP BY` clause in an InfluxQL query
struct GroupByEval(Vec<GroupByEvalType>);

/// The kind of `GROUP BY` evaluator
enum GroupByEvalType {
    /// An explicit tag name in a `GROUP BY` clause, e.g., `GROUP BY t1, t2`
    Tag(String),
    /// A regex in a `GROUP BY` that could match 0-or-more tags, e.g., `GROUP BY /t[1,2]/`
    Regex(Regex),
    /// A wildcard that matches all tags, e.g., `GROUP BY *`
    Wildcard,
}

impl GroupByEval {
    /// Convert a [`GroupByClause`] to a [`GroupByEval`] if any of its members match on tag columns
    ///
    /// This will produce an error if an invalid regex is provided as one of the `GROUP BY` clauses.
    /// That will likely be caught upstream during query parsing, but handle it here anyway.
    fn from_clause(clause: GroupByClause) -> Result<Option<Self>, anyhow::Error> {
        let v = clause
            .iter()
            .filter_map(|dim| match dim {
                Dimension::Time(_) => None,
                Dimension::VarRef(tag) => Some(Ok(GroupByEvalType::Tag(tag.to_string()))),
                Dimension::Regex(regex) => Some(
                    Regex::new(regex.as_str())
                        .map(GroupByEvalType::Regex)
                        .context("invalid regex in group by clause"),
                ),
                Dimension::Wildcard => Some(Ok(GroupByEvalType::Wildcard)),
            })
            .collect::<Result<Vec<GroupByEvalType>, anyhow::Error>>()?;

        if v.is_empty() {
            Ok(None)
        } else {
            Ok(Some(Self(v)))
        }
    }

    /// Check if a tag is matched by this set of `GROUP BY` clauses
    fn evaluate_tag(&self, tag_name: &str) -> bool {
        self.0.iter().any(|eval| eval.test(tag_name))
    }

    /// Check if the tag name is included explicitly in the `GROUP BY` clause.
    ///
    /// This is for determining orphan `GROUP BY` tag columns.
    fn contains_explicit_col_name(&self, col_name: &str) -> bool {
        self.0.iter().any(|eval| match eval {
            GroupByEvalType::Tag(t) => t == col_name,
            _ => false,
        })
    }
}

impl GroupByEvalType {
    /// Test the given `tag_name` agains this evaluator
    fn test(&self, tag_name: &str) -> bool {
        match self {
            Self::Tag(t) => t == tag_name,
            Self::Regex(regex) => regex.is_match(tag_name),
            Self::Wildcard => true,
        }
    }
}

impl Stream for QueryResponseStream {
    type Item = Result<QueryResponse, anyhow::Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // check for data in the buffer that can be flushed, if we are operating in chunked mode,
        // this will drain the buffer as much as possible by repeatedly returning Ready here
        // until the buffer can no longer flush, and before the input stream is polled again:
        if self.buffer.can_flush() {
            return Poll::Ready(Some(Ok(self.flush_one())));
        }
        // poll the input record batch stream:
        match ready!(self.input.poll_next_unpin(cx)) {
            Some(Ok(batch)) => {
                // buffer the yielded batch:
                if let Err(e) = self.buffer_record_batch(batch) {
                    return Poll::Ready(Some(Err(e)));
                }
                if self.buffer.can_flush() {
                    // if we can flush the buffer, do so now, and return
                    Poll::Ready(Some(Ok(self.flush_one())))
                } else {
                    // otherwise, we want to poll again in order to pull more
                    // batches from the input record batch stream:
                    cx.waker().wake_by_ref();
                    Poll::Pending
                }
            }
            Some(Err(e)) => Poll::Ready(Some(Err(e.into()))),
            None => {
                if !self.buffer.is_empty() {
                    // we only get here if we are not operating in chunked mode and
                    // we need to flush the entire buffer at once, OR if we are in chunked
                    // mode, and there is less than a chunk's worth of records left
                    //
                    // this is why the input stream is fused, because we will end up
                    // polling the input stream again if we end up here.
                    Poll::Ready(Some(Ok(self.flush_all())))
                    // Poll::Ready(None)
                } else if self.state.is_initialized() {
                    // we are still in an initialized state, which means no records were buffered
                    // and therefore we need to emit an empty result set before ending the stream:
                    Poll::Ready(Some(Ok(self.flush_empty())))
                } else {
                    // this ends the stream:
                    Poll::Ready(None)
                }
            }
        }
    }
}
