use std::sync::Arc;
use std::thread::JoinHandle;

use async_nats::Client;
use metrics::{Counter, Gauge, Histogram, Key, KeyName, Metadata, Recorder, SharedString, Unit};
use metrics_util::registry::Registry;
use tokio_util::sync::CancellationToken;

use crate::atomic_storage::AtomicStorage;
use crate::exporter::NatsExporter;
use crate::{Config, InstallError};

#[derive(Debug, Clone)]
pub struct NatsRecorder {
    pub(crate) registry: Arc<Registry<Key, AtomicStorage>>,
}

impl NatsRecorder {
    pub(crate) fn install(
        cxl: CancellationToken,
        config: Config,
        client: &'static Client,
    ) -> Result<JoinHandle<()>, InstallError> {
        let recorder = NatsRecorder { registry: Arc::new(Registry::new(AtomicStorage)) };
        let exporter = NatsExporter::spawn(cxl, config, client, recorder.clone())?;
        metrics::set_global_recorder(recorder)?;

        Ok(exporter)
    }
}

impl Recorder for NatsRecorder {
    fn describe_counter(&self, _: KeyName, _: Option<Unit>, _: SharedString) {}

    fn describe_gauge(
        &self,
        _: metrics::KeyName,
        _: Option<metrics::Unit>,
        _: metrics::SharedString,
    ) {
    }

    fn describe_histogram(&self, _: KeyName, _: Option<Unit>, _: SharedString) {}

    fn register_counter(&self, key: &Key, _: &Metadata<'_>) -> Counter {
        self.registry
            .get_or_create_counter(key, |counter| counter.clone().into())
    }

    fn register_gauge(&self, key: &Key, _: &Metadata<'_>) -> Gauge {
        self.registry
            .get_or_create_gauge(key, |gauge| gauge.clone().into())
    }

    fn register_histogram(&self, key: &Key, _: &Metadata<'_>) -> Histogram {
        self.registry
            .get_or_create_histogram(key, |histogram| histogram.clone().into())
    }
}
