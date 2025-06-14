mod atomic_storage;
mod exporter;
mod metric;
mod recorder;

use std::thread::JoinHandle;
use std::time::Duration;

pub use async_nats;
pub use metric::*;
use metrics::SetRecorderError;
use recorder::NatsRecorder;
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
pub fn install(
    cxl: CancellationToken,
    config: Config,
    client: &'static async_nats::Client,
) -> Result<JoinHandle<()>, InstallError> {
    NatsRecorder::install(cxl, config, client)
}

/// Configuration to control the NATS publishing.
pub struct Config {
    /// Metrics that have changed will be published this frequently.
    pub interval_min: Duration,
    /// Metrics that have not changed will be published this frequently.
    pub interval_max: Duration,
    /// Metric subjects will be prefixed with this.
    pub metric_prefix: Option<String>,
}

/// Possible failure while installing the NATS exporter.
#[derive(Debug, Error)]
pub enum InstallError {
    /// Failed to set the global metrics recorder.
    #[error("Set recorder; err={0}")]
    SetRecorder(#[from] SetRecorderError<NatsRecorder>),
    /// Failed to spawn the nats publisher worker thread.
    #[error("Spawn thread; err={0}")]
    SpawnThread(#[from] std::io::Error),
}
