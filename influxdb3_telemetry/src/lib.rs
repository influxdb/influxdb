mod bucket;
mod metrics;
mod sampler;
mod sender;
mod stats;
pub mod store;

use miette::Diagnostic;
use thiserror::Error;

#[derive(Debug, Diagnostic, Error)]
#[diagnostic(
    code(influxdb3_telemetry::lib),
    url("https://github.com/influxdata/influxdb/issues/new?template=bug_report.md")
)]
pub enum TelemetryError {
    #[error("cannot serialize to JSON")]
    CannotSerializeJson(#[from] serde_json::Error),

    #[error("failed to get pid")]
    CannotGetPid(&'static str),

    #[error("cannot send telemetry")]
    CannotSendToTelemetryServer(#[from] reqwest::Error),
}

pub type Result<T, E = TelemetryError> = std::result::Result<T, E>;

pub trait ParquetMetrics: Send + Sync + std::fmt::Debug + 'static {
    fn get_metrics(&self) -> (u64, f64, u64);
}
