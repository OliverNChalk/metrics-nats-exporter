mod atomic_storage;
mod recorder;

use std::thread::JoinHandle;
use std::time::Duration;

pub use async_nats::ServerAddr;
use metrics::SetRecorderError;
use recorder::NatsRecorder;
use thiserror::Error;
pub use tokio_util::sync::CancellationToken;

pub fn install(cxl: CancellationToken, config: Config) -> Result<JoinHandle<()>, InstallError> {
    NatsRecorder::install(cxl, config)
}

pub struct Config {
    pub nats_servers: Vec<ServerAddr>,
    pub interval_min: Duration,
    pub interval_max: Duration,
}

#[derive(Debug, Error)]
pub enum InstallError {
    #[error("Set recorder; err={0}")]
    SetRecorder(#[from] SetRecorderError<NatsRecorder>),
    #[error("Spawn thread; err={0}")]
    SpawnThread(#[from] std::io::Error),
}
