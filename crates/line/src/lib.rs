mod atomic_storage;
mod exporter;
mod line_protocol;
mod recorder;

use std::collections::BTreeMap;
use std::thread::JoinHandle;
use std::time::Duration;

use metrics::SetRecorderError;
use recorder::LineRecorder;
use thiserror::Error;
pub use tokio_util::sync::CancellationToken;

/// Installs the global metrics recorder.
///
/// # Returns
///
/// Returns a [`JoinHandle`] to the publisher thread.
///
/// # Errors
///
/// Errors on failure to install the recorder.
pub fn install(cxl: CancellationToken, config: Config) -> Result<JoinHandle<()>, InstallError> {
    LineRecorder::install(cxl, config)
}

/// Configuration for the `InfluxDB` line protocol exporter.
pub struct Config {
    /// Metrics that have changed will be collected this frequently.
    pub interval_min: Duration,
    /// Metrics that have not changed will be collected this frequently.
    pub interval_max: Duration,
    /// How often to flush the accumulated buffer via HTTP POST to `InfluxDB`.
    pub flush_interval: Duration,
    /// `InfluxDB` endpoint (e.g., `http://localhost:8086`).
    pub endpoint: String,
    /// `InfluxDB` v1 database name.
    pub database: String,
    /// Optional username for authentication.
    pub username: Option<String>,
    /// Optional password for authentication.
    pub password: Option<String>,
    /// Metric names will be prefixed with this (dot-separated).
    pub metric_prefix: Option<String>,
    /// Default tags applied to all metrics (`key_tags` from individual metrics
    /// override these).
    pub default_tags: BTreeMap<String, String>,
}

/// Possible failure while installing the line protocol exporter.
#[derive(Debug, Error)]
pub enum InstallError {
    /// Failed to set the global metrics recorder.
    #[error("Set recorder; err={0}")]
    SetRecorder(#[from] SetRecorderError<LineRecorder>),
    /// Failed to spawn the publisher worker thread.
    #[error("Spawn thread; err={0}")]
    SpawnThread(#[from] std::io::Error),
}
