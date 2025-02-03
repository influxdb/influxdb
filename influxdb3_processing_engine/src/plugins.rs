use crate::environment::PythonEnvironmentManager;
#[cfg(feature = "system-py")]
use crate::PluginCode;
#[cfg(feature = "system-py")]
use crate::{RequestEvent, ScheduleEvent, WalEvent};
use data_types::NamespaceName;
use hashbrown::HashMap;
use influxdb3_catalog::catalog::Catalog;
#[cfg(feature = "system-py")]
use influxdb3_client::plugin_development::{WalPluginTestRequest, WalPluginTestResponse};
#[cfg(feature = "system-py")]
use influxdb3_internal_api::query_executor::QueryExecutor;
use influxdb3_wal::Gen1Duration;
#[cfg(feature = "system-py")]
use influxdb3_wal::TriggerDefinition;
#[cfg(feature = "system-py")]
use influxdb3_wal::TriggerSpecificationDefinition;
use influxdb3_write::write_buffer;
use influxdb3_write::write_buffer::validator::WriteValidator;
use influxdb3_write::Precision;
#[cfg(feature = "system-py")]
use influxdb3_write::WriteBuffer;
#[cfg(feature = "system-py")]
use iox_time::TimeProvider;
use miette::Diagnostic;
use observability_deps::tracing::error;
use std::fmt::Debug;
use std::path::PathBuf;
#[cfg(feature = "system-py")]
use std::str::FromStr;
use std::sync::Arc;
use thiserror::Error;
#[cfg(feature = "system-py")]
use tokio::sync::mpsc;

#[derive(Debug, Diagnostic, Error)]
#[diagnostic(
    code(influxdb3_processing_engine::plugins),
    url("https://github.com/influxdata/influxdb/issues/new?template=bug_report.md")
)]
pub enum PluginError {
    #[error("invalid database {0}")]
    InvalidDatabase(String),

    #[error("couldn't find db")]
    MissingDb,

    #[cfg(feature = "system-py")]
    #[error(transparent)]
    PyError(#[from] pyo3::PyErr),

    #[error(transparent)]
    WriteBufferError(#[from] write_buffer::Error),

    #[error("failed to send shutdown message back")]
    FailedToShutdown,

    #[error(transparent)]
    AnyhowError(#[from] anyhow::Error),

    #[error("reading plugin file")]
    ReadPluginError(#[from] std::io::Error),

    #[error("error executing plugin")]
    PluginExecutionError(#[from] influxdb3_py_api::ExecutePluginError),

    #[error("invalid cron syntax")]
    InvalidCronSyntax(#[from] cron::error::Error),

    #[error("cron schedule never triggers: {0}")]
    CronScheduleNeverTriggers(String),

    #[error("tried to run a schedule plugin but the schedule iterator is over.")]
    ScheduledMissingTime,

    #[error("non-schedule plugin with schedule trigger: {0}")]
    NonSchedulePluginWithScheduleTrigger(String),

    #[error("Trigger schedule type {schedule_type} invalid for trigger type {trigger_type} and type mismatch")]
    TriggerScheduleTypeMismatch {
        schedule_type: String,
        trigger_type: String,
    },

    #[error("error reading file from Github: {0} {1}")]
    FetchingFromGithub(reqwest::StatusCode, String),
}

#[cfg(feature = "system-py")]
pub(crate) fn run_wal_contents_plugin(
    db_name: String,
    plugin_code: PluginCode,
    trigger_definition: TriggerDefinition,
    context: PluginContext,
    plugin_receiver: mpsc::Receiver<WalEvent>,
) {
    let trigger_plugin = TriggerPlugin {
        trigger_definition,
        db_name,
        plugin_code,
        write_buffer: context.write_buffer,
        query_executor: context.query_executor,
    };
    tokio::task::spawn(async move {
        trigger_plugin
            .run_wal_contents_plugin(plugin_receiver)
            .await
            .expect("trigger plugin failed");
    });
}

#[derive(Debug, Clone)]
pub struct ProcessingEngineEnvironmentManager {
    pub plugin_dir: Option<PathBuf>,
    pub virtual_env_location: Option<PathBuf>,
    pub package_manager: Arc<dyn PythonEnvironmentManager>,
}

#[cfg(feature = "system-py")]
pub(crate) fn run_schedule_plugin(
    db_name: String,
    plugin_code: PluginCode,
    trigger_definition: TriggerDefinition,
    time_provider: Arc<dyn TimeProvider>,
    context: PluginContext,
    plugin_receiver: mpsc::Receiver<ScheduleEvent>,
) -> Result<(), PluginError> {
    // Ensure that the plugin is a schedule plugin
    let plugin_type = trigger_definition.trigger.plugin_type();
    if !matches!(plugin_type, influxdb3_wal::PluginType::Schedule) {
        return Err(PluginError::NonSchedulePluginWithScheduleTrigger(format!(
            "{:?}",
            trigger_definition
        )));
    }

    let trigger_plugin = TriggerPlugin {
        trigger_definition,
        db_name,
        plugin_code,
        write_buffer: context.write_buffer,
        query_executor: context.query_executor,
    };

    let runner = python_plugin::ScheduleTriggerRunner::try_new(
        &trigger_plugin.trigger_definition.trigger,
        Arc::clone(&time_provider),
    )?;
    tokio::task::spawn(async move {
        trigger_plugin
            .run_schedule_plugin(plugin_receiver, runner, time_provider)
            .await
            .expect("cron trigger plugin failed");
    });

    Ok(())
}

#[cfg(feature = "system-py")]
pub(crate) fn run_request_plugin(
    db_name: String,
    plugin_code: PluginCode,
    trigger_definition: TriggerDefinition,
    context: PluginContext,
    plugin_receiver: mpsc::Receiver<RequestEvent>,
) {
    let trigger_plugin = TriggerPlugin {
        trigger_definition,
        db_name,
        plugin_code,
        write_buffer: context.write_buffer,
        query_executor: context.query_executor,
    };
    tokio::task::spawn(async move {
        trigger_plugin
            .run_request_plugin(plugin_receiver)
            .await
            .expect("trigger plugin failed");
    });
}

#[cfg(feature = "system-py")]
pub(crate) struct PluginContext {
    // handler to write data back to the DB.
    pub(crate) write_buffer: Arc<dyn WriteBuffer>,
    // query executor to hand off to the plugin
    pub(crate) query_executor: Arc<dyn QueryExecutor>,
}

#[cfg(feature = "system-py")]
#[derive(Debug)]
struct TriggerPlugin {
    trigger_definition: TriggerDefinition,
    plugin_code: PluginCode,
    db_name: String,
    write_buffer: Arc<dyn WriteBuffer>,
    query_executor: Arc<dyn QueryExecutor>,
}

#[cfg(feature = "system-py")]
mod python_plugin {
    use super::*;
    use anyhow::{anyhow, Context};
    use chrono::{DateTime, Duration, Utc};
    use cron::{OwnedScheduleIterator, Schedule as CronSchedule};
    use data_types::NamespaceName;
    use humantime::{format_duration, parse_duration};
    use hyper::http::HeaderValue;
    use hyper::{Body, Response, StatusCode};
    use influxdb3_catalog::catalog::DatabaseSchema;
    use influxdb3_py_api::system_py::{
        execute_python_with_batch, execute_request_trigger, execute_schedule_trigger,
        PluginReturnState,
    };
    use influxdb3_wal::{WalContents, WalOp};
    use influxdb3_write::Precision;
    use iox_time::Time;
    use observability_deps::tracing::{info, warn};
    use std::str::FromStr;
    use std::time::SystemTime;
    use tokio::sync::mpsc::Receiver;

    impl TriggerPlugin {
        pub(crate) async fn run_wal_contents_plugin(
            &self,
            mut receiver: Receiver<WalEvent>,
        ) -> Result<(), PluginError> {
            info!(?self.trigger_definition.trigger_name, ?self.trigger_definition.database_name, ?self.trigger_definition.plugin_filename, "starting wal contents plugin");

            loop {
                let event = match receiver.recv().await {
                    Some(event) => event,
                    None => {
                        warn!(?self.trigger_definition, "trigger plugin receiver closed");
                        break;
                    }
                };

                match event {
                    WalEvent::WriteWalContents(wal_contents) => {
                        if let Err(e) = self.process_wal_contents(wal_contents).await {
                            error!(?self.trigger_definition, "error processing wal contents: {}", e);
                        }
                    }
                    WalEvent::Shutdown(sender) => {
                        sender.send(()).map_err(|_| PluginError::FailedToShutdown)?;
                        break;
                    }
                }
            }

            Ok(())
        }

        pub(crate) async fn run_schedule_plugin(
            &self,
            mut receiver: Receiver<ScheduleEvent>,
            mut runner: ScheduleTriggerRunner,
            time_provider: Arc<dyn TimeProvider>,
        ) -> Result<(), PluginError> {
            loop {
                let Some(next_run_instant) = runner.next_run_time() else {
                    break;
                };

                tokio::select! {
                    _ = time_provider.sleep_until(next_run_instant) => {
                        let Some(schema) = self.write_buffer.catalog().db_schema(self.db_name.as_str()) else {
                            return Err(PluginError::MissingDb);
                        };
                        runner.run_at_time(self, schema).await?;
                    }
                    event = receiver.recv() => {
                        match event {
                            None => {
                                warn!(?self.trigger_definition, "trigger plugin receiver closed");
                                break;
                            }
                            Some(ScheduleEvent::Shutdown(sender)) => {
                                sender.send(()).map_err(|_| PluginError::FailedToShutdown)?;
                                break;
                            }
                        }
                    }
                }
            }

            Ok(())
        }

        pub(crate) async fn run_request_plugin(
            &self,
            mut receiver: Receiver<RequestEvent>,
        ) -> Result<(), PluginError> {
            info!(?self.trigger_definition.trigger_name, ?self.trigger_definition.database_name, ?self.trigger_definition.plugin_filename, "starting request plugin");

            loop {
                match receiver.recv().await {
                    None => {
                        warn!(?self.trigger_definition, "trigger plugin receiver closed");
                        break;
                    }
                    Some(RequestEvent::Request(request)) => {
                        let Some(schema) =
                            self.write_buffer.catalog().db_schema(self.db_name.as_str())
                        else {
                            error!(?self.trigger_definition, "missing db schema");
                            return Err(PluginError::MissingDb);
                        };
                        let result = execute_request_trigger(
                            self.plugin_code.code().as_ref(),
                            Arc::clone(&schema),
                            Arc::clone(&self.query_executor),
                            &self.trigger_definition.trigger_arguments,
                            request.query_params,
                            request.headers,
                            request.body,
                        );

                        // produce the HTTP response
                        let response = match result {
                            Ok((
                                response_code,
                                response_headers,
                                response_body,
                                plugin_return_state,
                            )) => {
                                let errors = self.handle_return_state(plugin_return_state).await;
                                // TODO: here is one spot we'll pick up errors to put into the plugin system table
                                for error in errors {
                                    error!(?self.trigger_definition, "error running request plugin: {}", error);
                                }

                                let response_status = StatusCode::from_u16(response_code)
                                    .unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
                                let mut response = Response::builder().status(response_status);

                                for (key, value) in response_headers {
                                    response = response.header(
                                        key.as_str(),
                                        HeaderValue::from_str(&value)
                                            .unwrap_or_else(|_| HeaderValue::from_static("")),
                                    );
                                }

                                response
                                    .body(Body::from(response_body))
                                    .context("building response")?
                            }
                            Err(e) => {
                                // build json string with the error with serde so that it is {"error": "error message"}
                                error!(?self.trigger_definition, "error running request plugin: {}", e);
                                let body = serde_json::json!({"error": e.to_string()}).to_string();
                                Response::builder()
                                    .status(StatusCode::INTERNAL_SERVER_ERROR)
                                    .body(Body::from(body))
                                    .context("building response")?
                            }
                        };

                        if request.response_tx.send(response).is_err() {
                            error!(?self.trigger_definition, "error sending response");
                        }
                    }
                    Some(RequestEvent::Shutdown(sender)) => {
                        sender.send(()).map_err(|_| PluginError::FailedToShutdown)?;
                        break;
                    }
                }
            }

            Ok(())
        }

        async fn process_wal_contents(
            &self,
            wal_contents: Arc<WalContents>,
        ) -> Result<(), PluginError> {
            let Some(schema) = self.write_buffer.catalog().db_schema(self.db_name.as_str()) else {
                return Err(PluginError::MissingDb);
            };

            for wal_op in &wal_contents.ops {
                match wal_op {
                    WalOp::Write(write_batch) => {
                        // determine if this write batch is for this database
                        if write_batch.database_name.as_ref()
                            != self.trigger_definition.database_name
                        {
                            continue;
                        }
                        let table_filter = match &self.trigger_definition.trigger {
                                    TriggerSpecificationDefinition::AllTablesWalWrite => {
                                        // no filter
                                        None
                                    }
                                    TriggerSpecificationDefinition::SingleTableWalWrite {
                                        table_name,
                                    } => {
                                        let table_id = schema
                                            .table_name_to_id(table_name.as_ref())
                                            .context("table not found")?;
                                        Some(table_id)
                                    }
                                    // This should not occur
                                    TriggerSpecificationDefinition::Schedule {
                                        schedule
                                    } => {
                                        return Err(anyhow!("unexpectedly found scheduled trigger specification cron:{} for WAL plugin {}", schedule, self.trigger_definition.trigger_name).into())
                                    }
                                    TriggerSpecificationDefinition::Every {
                                        duration,
                                    } => {
                                        return Err(anyhow!("unexpectedly found every trigger specification every:{} WAL plugin {}", format_duration(*duration), self.trigger_definition.trigger_name).into())
                                    }
                                    TriggerSpecificationDefinition::RequestPath { path } => {
                                        return Err(anyhow!("unexpectedly found request path trigger specification {} for WAL plugin {}", path, self.trigger_definition.trigger_name).into())
                                    }
                                };

                        let result = execute_python_with_batch(
                            self.plugin_code.code().as_ref(),
                            write_batch,
                            Arc::clone(&schema),
                            Arc::clone(&self.query_executor),
                            table_filter,
                            &self.trigger_definition.trigger_arguments,
                        )?;

                        let errors = self.handle_return_state(result).await;
                        // TODO: here is one spot we'll pick up errors to put into the plugin system table
                        for error in errors {
                            error!(?self.trigger_definition, "error running wal plugin: {}", error);
                        }
                    }
                    WalOp::Catalog(_) => {}
                    WalOp::Noop(_) => {}
                }
            }

            Ok(())
        }

        /// Handles the return state from the plugin, writing back lines and handling any errors.
        /// It returns a vec of error messages that can be used to log or report back to the user.
        async fn handle_return_state(&self, plugin_return_state: PluginReturnState) -> Vec<String> {
            let ingest_time = SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap();

            let mut errors = Vec::new();

            if !plugin_return_state.write_back_lines.is_empty() {
                if let Err(e) = self
                    .write_buffer
                    .write_lp(
                        NamespaceName::new(self.db_name.clone()).unwrap(),
                        plugin_return_state.write_back_lines.join("\n").as_str(),
                        Time::from_timestamp_nanos(ingest_time.as_nanos() as i64),
                        false,
                        Precision::Nanosecond,
                        false,
                    )
                    .await
                {
                    errors.push(format!("error writing back lines: {}", e));
                }
            }

            for (db_name, lines) in plugin_return_state.write_db_lines {
                let Ok(namespace_name) = NamespaceName::new(db_name.clone()) else {
                    errors.push(format!("invalid database name: {}", db_name));
                    continue;
                };

                if let Err(e) = self
                    .write_buffer
                    .write_lp(
                        namespace_name,
                        lines.join("\n").as_str(),
                        Time::from_timestamp_nanos(ingest_time.as_nanos() as i64),
                        false,
                        Precision::Nanosecond,
                        false,
                    )
                    .await
                {
                    errors.push(format!("error writing back lines to {}: {}", db_name, e));
                }
            }

            errors
        }
    }

    enum Schedule {
        Cron(OwnedScheduleIterator<Utc>),
        Every(Duration),
    }

    pub(crate) struct ScheduleTriggerRunner {
        schedule: Schedule,
        next_trigger_time: Option<DateTime<Utc>>,
    }

    impl ScheduleTriggerRunner {
        pub(crate) fn try_new(
            trigger_spec: &TriggerSpecificationDefinition,
            time_provider: Arc<dyn TimeProvider>,
        ) -> Result<Self, PluginError> {
            match trigger_spec {
                TriggerSpecificationDefinition::AllTablesWalWrite
                | TriggerSpecificationDefinition::SingleTableWalWrite { .. } => {
                    Err(anyhow!("shouldn't have table trigger for scheduled plugin").into())
                }
                TriggerSpecificationDefinition::RequestPath { .. } => {
                    Err(anyhow!("shouldn't have request path trigger for scheduled plugin").into())
                }
                TriggerSpecificationDefinition::Schedule { schedule } => {
                    let schedule = CronSchedule::from_str(schedule.as_str())
                        .context("cron schedule should be parsable")?;
                    Ok(Self::new_cron(schedule, time_provider))
                }
                TriggerSpecificationDefinition::Every { duration } => {
                    // check that duration isn't longer than a year, so we avoid overflows.
                    if *duration > parse_duration("1 year").unwrap() {
                        return Err(
                            anyhow!("schedule duration cannot be greater than 1 year").into()
                        );
                    }
                    Ok(Self::new_every(
                        Duration::from_std(*duration)
                            .context("should be able to convert durations. ")?,
                        time_provider,
                    ))
                }
            }
        }
        fn new_cron(cron_schedule: CronSchedule, time_provider: Arc<dyn TimeProvider>) -> Self {
            let mut schedule = cron_schedule.after_owned(time_provider.now().date_time());
            let next_trigger_time = schedule.next();
            Self {
                schedule: Schedule::Cron(schedule),
                next_trigger_time,
            }
        }

        fn new_every(duration: Duration, time_provider: Arc<dyn TimeProvider>) -> Self {
            let now = time_provider.now().date_time();
            let duration_millis = duration.num_milliseconds();
            let now_millis = now.timestamp_millis();
            let next_trigger_millis = ((now_millis / duration_millis) + 1) * duration_millis;
            let next_trigger_time = Some(
                DateTime::from_timestamp_millis(next_trigger_millis)
                    .expect("can't be out of range"),
            );
            Self {
                schedule: Schedule::Every(duration),
                next_trigger_time,
            }
        }

        async fn run_at_time(
            &mut self,
            plugin: &TriggerPlugin,
            db_schema: Arc<DatabaseSchema>,
        ) -> Result<(), PluginError> {
            let Some(trigger_time) = self.next_trigger_time else {
                return Err(anyhow!("running a cron trigger that is finished.").into());
            };

            let result = execute_schedule_trigger(
                plugin.plugin_code.code().as_ref(),
                trigger_time,
                Arc::clone(&db_schema),
                Arc::clone(&plugin.query_executor),
                &plugin.trigger_definition.trigger_arguments,
            )?;

            let errors = plugin.handle_return_state(result).await;
            // TODO: here is one spot we'll pick up errors to put into the plugin system table
            for error in errors {
                error!(?plugin.trigger_definition, "error running schedule plugin: {}", error);
            }

            self.advance_time();

            Ok(())
        }

        fn advance_time(&mut self) {
            self.next_trigger_time = match &mut self.schedule {
                Schedule::Cron(schedule) => schedule.next(),
                Schedule::Every(duration) => self.next_trigger_time.map(|time| time + *duration),
            };
        }

        /// A funky little method to get a tokio Instant that we can call `tokio::time::sleep_until()` on.
        fn next_run_time(&self) -> Option<Time> {
            let next_trigger_time = Time::from_datetime(*self.next_trigger_time.as_ref()?);
            Some(next_trigger_time)
        }
    }
}

#[cfg(feature = "system-py")]
pub(crate) fn run_test_wal_plugin(
    now_time: iox_time::Time,
    catalog: Arc<influxdb3_catalog::catalog::Catalog>,
    query_executor: Arc<dyn QueryExecutor>,
    code: String,
    request: WalPluginTestRequest,
) -> Result<WalPluginTestResponse, PluginError> {
    use data_types::NamespaceName;
    use influxdb3_wal::Gen1Duration;
    use influxdb3_write::write_buffer::validator::WriteValidator;
    use influxdb3_write::Precision;

    let database = request.database;
    let namespace = NamespaceName::new(database.clone())
        .map_err(|_e| PluginError::InvalidDatabase(database.clone()))?;
    // parse the lp into a write batch
    let validator = WriteValidator::initialize(
        namespace.clone(),
        Arc::clone(&catalog),
        now_time.timestamp_nanos(),
    )?;
    let data = validator.v1_parse_lines_and_update_schema(
        &request.input_lp,
        false,
        now_time,
        Precision::Nanosecond,
    )?;
    let data = data.convert_lines_to_buffer(Gen1Duration::new_1m());
    let db = catalog.db_schema(&database).ok_or(PluginError::MissingDb)?;

    let plugin_return_state = influxdb3_py_api::system_py::execute_python_with_batch(
        &code,
        &data.valid_data,
        db,
        query_executor,
        None,
        &request.input_arguments,
    )?;

    let log_lines = plugin_return_state.log();

    let mut database_writes = plugin_return_state.write_db_lines;
    database_writes.insert(database, plugin_return_state.write_back_lines);

    let test_write_handler = TestWriteHandler::new(Arc::clone(&catalog), now_time);
    let errors = test_write_handler.validate_all_writes(&database_writes);

    Ok(WalPluginTestResponse {
        log_lines,
        database_writes,
        errors,
    })
}

#[derive(Debug)]
pub struct TestWriteHandler {
    catalog: Arc<Catalog>,
    now_time: iox_time::Time,
}

impl TestWriteHandler {
    pub fn new(catalog: Arc<Catalog>, now_time: iox_time::Time) -> Self {
        Self { catalog, now_time }
    }

    /// Validates a vec of lines for a namespace, returning any errors that arise as strings
    fn validate_write_lines(
        &self,
        namespace: NamespaceName<'static>,
        lines: &[String],
    ) -> Vec<String> {
        let mut errors = Vec::new();

        let db_name = namespace.as_str();

        let validator = match WriteValidator::initialize(
            namespace.clone(),
            Arc::clone(&self.catalog),
            self.now_time.timestamp_nanos(),
        ) {
            Ok(v) => v,
            Err(e) => {
                errors.push(format!(
                    "Failed to initialize validator for db {}: {}",
                    db_name, e
                ));
                return errors;
            }
        };

        let lp = lines.join("\n");
        match validator.v1_parse_lines_and_update_schema(
            &lp,
            false,
            self.now_time,
            Precision::Nanosecond,
        ) {
            Ok(data) => {
                let data = data.convert_lines_to_buffer(Gen1Duration::new_1m());
                for err in data.errors {
                    errors.push(format!("{:?}", err));
                }
            }
            Err(write_buffer::Error::ParseError(e)) => {
                errors.push(format!(
                    "line protocol parse error on write to db {}: {:?}",
                    db_name, e
                ));
            }
            Err(e) => {
                errors.push(format!(
                    "Failed to validate output lines to db {}: {}",
                    db_name, e
                ));
            }
        }
        errors
    }
    pub fn validate_all_writes(&self, writes: &HashMap<String, Vec<String>>) -> Vec<String> {
        let mut all_errors = Vec::new();
        for (db_name, lines) in writes {
            let namespace = match NamespaceName::new(db_name.to_string()) {
                Ok(namespace) => namespace,
                Err(e) => {
                    all_errors.push(format!("database name {} is invalid: {}", db_name, e));
                    continue;
                }
            };

            let db_errors = self.validate_write_lines(namespace, lines);
            all_errors.extend(db_errors);
        }

        all_errors
    }
}

#[cfg(feature = "system-py")]
pub(crate) fn run_test_schedule_plugin(
    now_time: iox_time::Time,
    catalog: Arc<Catalog>,
    query_executor: Arc<dyn QueryExecutor>,
    code: String,
    request: influxdb3_client::plugin_development::SchedulePluginTestRequest,
) -> Result<influxdb3_client::plugin_development::SchedulePluginTestResponse, PluginError> {
    let database = request.database;
    let db = catalog.db_schema(&database).ok_or(PluginError::MissingDb)?;

    let cron_schedule = request.schedule.as_deref().unwrap_or("* * * * * *");

    let schedule = cron::Schedule::from_str(cron_schedule)?;
    let Some(schedule_time) = schedule.after(&now_time.date_time()).next() else {
        return Err(PluginError::CronScheduleNeverTriggers(
            cron_schedule.to_string(),
        ));
    };

    let plugin_return_state = influxdb3_py_api::system_py::execute_schedule_trigger(
        &code,
        schedule_time,
        db,
        query_executor,
        &request.input_arguments,
    )?;

    let log_lines = plugin_return_state.log();

    let mut database_writes = plugin_return_state.write_db_lines;
    if !plugin_return_state.write_back_lines.is_empty() {
        database_writes.insert(database, plugin_return_state.write_back_lines);
    }

    let test_write_handler = TestWriteHandler::new(Arc::clone(&catalog), now_time);
    let errors = test_write_handler.validate_all_writes(&database_writes);
    let trigger_time = schedule_time.to_rfc3339_opts(chrono::SecondsFormat::AutoSi, true);

    Ok(
        influxdb3_client::plugin_development::SchedulePluginTestResponse {
            trigger_time: Some(trigger_time),
            log_lines,
            database_writes,
            errors,
        },
    )
}

#[cfg(feature = "system-py")]
#[cfg(test)]
mod tests {
    use super::*;
    use hashbrown::HashMap;
    use influxdb3_catalog::catalog::Catalog;
    use influxdb3_internal_api::query_executor::UnimplementedQueryExecutor;
    use influxdb3_write::write_buffer::validator::WriteValidator;
    use influxdb3_write::Precision;
    use iox_time::Time;

    fn ensure_pyo3() {
        pyo3::prepare_freethreaded_python();
    }

    #[test]
    fn test_wal_plugin() {
        ensure_pyo3();
        let now = Time::from_timestamp_nanos(1);
        let catalog = Catalog::new("foo".into(), "bar".into());
        let code = r#"
def process_writes(influxdb3_local, table_batches, args=None):
    influxdb3_local.info("arg1: " + args["arg1"])

    for table_batch in table_batches:
        influxdb3_local.info("table: " + table_batch["table_name"])

        for row in table_batch["rows"]:
            influxdb3_local.info("row: " + str(row))

    line = LineBuilder("some_table")\
        .tag("tag1", "tag1_value")\
        .tag("tag2", "tag2_value")\
        .int64_field("field1", 1)\
        .float64_field("field2", 2.0)\
        .string_field("field3", "number three")
    influxdb3_local.write(line)

    other_line = LineBuilder("other_table")
    other_line.int64_field("other_field", 1)
    other_line.float64_field("other_field2", 3.14)
    other_line.time_ns(1302)

    influxdb3_local.write_to_db("mytestdb", other_line)

    influxdb3_local.info("done")"#;

        let lp = [
            "cpu,host=A,region=west usage=1i,system=23.2 100",
            "mem,host=B user=43.1 120",
        ]
        .join("\n");

        let request = WalPluginTestRequest {
            filename: "test".into(),
            database: "_testdb".into(),
            input_lp: lp,
            input_arguments: Some(HashMap::from([(
                String::from("arg1"),
                String::from("val1"),
            )])),
        };

        let executor: Arc<dyn QueryExecutor> = Arc::new(UnimplementedQueryExecutor);

        let response =
            run_test_wal_plugin(now, Arc::new(catalog), executor, code.to_string(), request)
                .unwrap();

        let expected_log_lines = vec![
            "INFO: arg1: val1",
            "INFO: table: cpu",
            "INFO: row: {'host': 'A', 'region': 'west', 'usage': 1, 'system': 23.2, 'time': 100}",
            "INFO: table: mem",
            "INFO: row: {'host': 'B', 'user': 43.1, 'time': 120}",
            "INFO: done",
        ]
        .into_iter()
        .map(|s| s.to_string())
        .collect::<Vec<_>>();
        assert_eq!(response.log_lines, expected_log_lines);

        let expected_testdb_lines = vec![
            "some_table,tag1=tag1_value,tag2=tag2_value field1=1i,field2=2.0,field3=\"number three\""
                .to_string(),
        ];
        assert_eq!(
            response.database_writes.get("_testdb").unwrap(),
            &expected_testdb_lines
        );
        let expected_mytestdb_lines =
            vec!["other_table other_field=1i,other_field2=3.14 1302".to_string()];
        assert_eq!(
            response.database_writes.get("mytestdb").unwrap(),
            &expected_mytestdb_lines
        );
    }

    #[test]
    fn test_wal_plugin_invalid_lines() {
        ensure_pyo3();
        // set up a catalog and write some data into it to create a schema
        let now = Time::from_timestamp_nanos(1);
        let catalog = Arc::new(Catalog::new("foo".into(), "bar".into()));
        let namespace = NamespaceName::new("foodb").unwrap();
        let validator = WriteValidator::initialize(
            namespace.clone(),
            Arc::clone(&catalog),
            now.timestamp_nanos(),
        )
        .unwrap();
        let _data = validator
            .v1_parse_lines_and_update_schema(
                "cpu,host=A f1=10i 100",
                false,
                now,
                Precision::Nanosecond,
            )
            .unwrap();

        let code = r#"
def process_writes(influxdb3_local, table_batches, args=None):
    line = LineBuilder("some_table")\
        .tag("tag1", "tag1_value")\
        .tag("tag2", "tag2_value")\
        .int64_field("field1", 1)\
        .float64_field("field2", 2.0)\
        .string_field("field3", "number three")
    influxdb3_local.write(line)

    cpu_valid = LineBuilder("cpu")\
        .tag("host", "A")\
        .int64_field("f1", 10)\
        .uint64_field("f2", 20)\
        .bool_field("f3", True)
    influxdb3_local.write_to_db("foodb", cpu_valid)

    cpu_invalid = LineBuilder("cpu")\
        .tag("host", "A")\
        .string_field("f1", "not_an_int")
    influxdb3_local.write_to_db("foodb", cpu_invalid)"#;

        let lp = ["mem,host=B user=43.1 120"].join("\n");

        let request = WalPluginTestRequest {
            filename: "test".into(),
            database: "_testdb".into(),
            input_lp: lp,
            input_arguments: None,
        };

        let executor: Arc<dyn QueryExecutor> = Arc::new(UnimplementedQueryExecutor);

        let reesponse = run_test_wal_plugin(
            now,
            Arc::clone(&catalog),
            executor,
            code.to_string(),
            request,
        )
        .unwrap();

        let expected_testdb_lines = vec![
            "some_table,tag1=tag1_value,tag2=tag2_value field1=1i,field2=2.0,field3=\"number three\""
                .to_string(),
        ];
        assert_eq!(
            reesponse.database_writes.get("_testdb").unwrap(),
            &expected_testdb_lines
        );

        // the lines should still come through in the output because that's what Python sent
        let expected_foodb_lines = vec![
            "cpu,host=A f1=10i,f2=20u,f3=t".to_string(),
            "cpu,host=A f1=\"not_an_int\"".to_string(),
        ];
        assert_eq!(
            reesponse.database_writes.get("foodb").unwrap(),
            &expected_foodb_lines
        );

        // there should be an error for the invalid line
        assert_eq!(reesponse.errors.len(), 1);
        let expected_error = "line protocol parse error on write to db foodb: WriteLineError { original_line: \"cpu,host=A f1=not_an_int\", line_number: 2, error_message: \"invalid field value in line protocol for field 'f1' on line 1: expected type iox::column_type::field::integer, but got iox::column_type::field::string\" }";
        assert_eq!(reesponse.errors[0], expected_error);
    }
}
