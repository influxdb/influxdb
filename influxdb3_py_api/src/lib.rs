use miette::Diagnostic;
use thiserror::Error;

#[derive(Debug, Diagnostic, Error)]
#[diagnostic(
    code(influxdb3_py_api::lib),
    url("https://github.com/influxdata/influxdb/issues/new?template=bug_report.md")
)]
pub enum ExecutePluginError {
    #[error("the process_writes function is not present in the plugin. Should be defined as: process_writes(influxdb3_local, table_batches, args=None)")]
    MissingProcessWritesFunction,

    #[error("the process_request function is not present in the plugin. Should be defined as: process_request(influxdb3_local, query_parameters, request_headers, request_body, args=None) -> Tuple[str, Optional[Dict[str, str]]]")]
    MissingProcessRequestFunction,

    #[error("the process_scheduled_call function is not present in the plugin. Should be defined as: process_scheduled_call(influxdb3_local, call_time, args=None)")]
    MissingProcessScheduledCallFunction,

    #[error("Error from plugin")]
    PluginError(#[from] anyhow::Error),
}

#[cfg(feature = "system-py")]
pub mod system_py;
