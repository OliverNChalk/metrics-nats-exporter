mod atomic_storage;
mod recorder;

use std::thread::JoinHandle;
use std::time::Duration;

pub use async_nats::ServerAddr;
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
/// Returns an error an failure to install the recorder.
pub fn install(cxl: CancellationToken, config: Config) -> Result<JoinHandle<()>, InstallError> {
    NatsRecorder::install(cxl, config)
}

/// Configuration to control the NATS publishing.
pub struct Config {
    /// The NATS servers to connect to.
    pub nats_servers: Vec<ServerAddr>,
    /// Metrics that have changed will be published this frequently.
    pub interval_min: Duration,
    /// Metrics that have not changed will be published this frequently.
    pub interval_max: Duration,
}

#[derive(Debug, Error)]
pub enum InstallError {
    #[error("Set recorder; err={0}")]
    SetRecorder(#[from] SetRecorderError<NatsRecorder>),
    #[error("Spawn thread; err={0}")]
    SpawnThread(#[from] std::io::Error),
}
