use async_trait::async_trait;
use datafusion::arrow::error::ArrowError;
use datafusion::common::DataFusionError;
use datafusion::execution::SendableRecordBatchStream;
use influxdb_influxql_parser::statement::Statement;
use iox_query::query_log::QueryLogEntries;
use iox_query::{QueryDatabase, QueryNamespace};
use iox_query_params::StatementParams;
use miette::Diagnostic;
use std::fmt::Debug;
use std::sync::Arc;
use thiserror::Error;
use trace::ctx::SpanContext;
use trace::span::{Span, SpanExt};
use trace_http::ctx::RequestLogContext;
use tracker::InstrumentedAsyncOwnedSemaphorePermit;

#[derive(Debug, Diagnostic, Error)]
#[diagnostic(
    code(influxdb3_internal_api::query_executor),
    url("https://github.com/influxdata/influxdb/issues/new?template=bug_report.md")
)]
pub enum QueryExecutorError {
    #[error("database not found: {db_name}")]
    DatabaseNotFound { db_name: String },
    #[error("error while planning query")]
    QueryPlanning(#[source] DataFusionError),
    #[error("error while executing plan")]
    ExecuteStream(#[source] DataFusionError),
    #[error("unable to compose record batches from databases")]
    DatabasesToRecordBatch(#[source] ArrowError),
    #[error("unable to compose record batches from retention policies")]
    RetentionPoliciesToRecordBatch(#[source] ArrowError),
    #[error("invokded a method that is not implemented")]
    MethodNotImplemented(&'static str),
    #[error(transparent)]
    Anyhow(#[from] anyhow::Error),
}

#[async_trait]
pub trait QueryExecutor: QueryDatabase + Debug + Send + Sync + 'static {
    async fn get_db_namespace(
        &self,
        database_name: &str,
        span_ctx: &Option<SpanContext>,
    ) -> Result<Arc<dyn QueryNamespace>, QueryExecutorError> {
        self.namespace(
            database_name,
            span_ctx.child_span("get_db_namespace"),
            false,
        )
        .await
        .map_err(|_| QueryExecutorError::DatabaseNotFound {
            db_name: database_name.to_string(),
        })?
        .ok_or_else(|| QueryExecutorError::DatabaseNotFound {
            db_name: database_name.to_string(),
        })
    }

    async fn query_sql(
        &self,
        database: &str,
        q: &str,
        params: Option<StatementParams>,
        span_ctx: Option<SpanContext>,
        external_span_ctx: Option<RequestLogContext>,
    ) -> Result<SendableRecordBatchStream, QueryExecutorError>;

    async fn query_influxql(
        &self,
        database_name: &str,
        query_str: &str,
        influxql_statement: Statement,
        params: Option<StatementParams>,
        span_ctx: Option<SpanContext>,
        external_span_ctx: Option<RequestLogContext>,
    ) -> Result<SendableRecordBatchStream, QueryExecutorError>;

    fn show_databases(
        &self,
        include_deleted: bool,
    ) -> Result<SendableRecordBatchStream, QueryExecutorError>;

    async fn show_retention_policies(
        &self,
        database: Option<&str>,
        span_ctx: Option<SpanContext>,
    ) -> Result<SendableRecordBatchStream, QueryExecutorError>;

    fn upcast(&self) -> Arc<(dyn QueryDatabase + 'static)>;
}

#[derive(Debug, Copy, Clone)]
pub struct UnimplementedQueryExecutor;

#[async_trait]
impl QueryDatabase for UnimplementedQueryExecutor {
    async fn namespace(
        &self,
        _name: &str,
        _span: Option<Span>,
        _include_debug_info_tables: bool,
    ) -> Result<Option<Arc<dyn QueryNamespace>>, DataFusionError> {
        unimplemented!()
    }

    async fn acquire_semaphore(
        &self,
        _span: Option<Span>,
    ) -> InstrumentedAsyncOwnedSemaphorePermit {
        unimplemented!()
    }

    fn query_log(&self) -> QueryLogEntries {
        unimplemented!()
    }
}

#[async_trait]
impl QueryExecutor for UnimplementedQueryExecutor {
    async fn query_sql(
        &self,
        _database: &str,
        _q: &str,
        _params: Option<StatementParams>,
        _span_ctx: Option<SpanContext>,
        _external_span_ctx: Option<RequestLogContext>,
    ) -> Result<SendableRecordBatchStream, QueryExecutorError> {
        Err(QueryExecutorError::MethodNotImplemented("query_sql"))
    }

    async fn query_influxql(
        &self,
        _database_name: &str,
        _query_str: &str,
        _influxql_statement: Statement,
        _params: Option<StatementParams>,
        _span_ctx: Option<SpanContext>,
        _external_span_ctx: Option<RequestLogContext>,
    ) -> Result<SendableRecordBatchStream, QueryExecutorError> {
        Err(QueryExecutorError::MethodNotImplemented("query_influxql"))
    }

    fn show_databases(
        &self,
        _include_deleted: bool,
    ) -> Result<SendableRecordBatchStream, QueryExecutorError> {
        Err(QueryExecutorError::MethodNotImplemented("show_databases"))
    }

    async fn show_retention_policies(
        &self,
        _database: Option<&str>,
        _span_ctx: Option<SpanContext>,
    ) -> Result<SendableRecordBatchStream, QueryExecutorError> {
        Err(QueryExecutorError::MethodNotImplemented(
            "show_retention_policies",
        ))
    }

    fn upcast(&self) -> Arc<(dyn QueryDatabase + 'static)> {
        Arc::new(UnimplementedQueryExecutor) as _
    }
}
