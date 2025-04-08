use std::sync::atomic::{AtomicI64, AtomicU64};
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use futures::FutureExt;
use metrics::{
    Counter, CounterFn, Gauge, Histogram, HistogramFn, Key, KeyName, Metadata, Recorder,
    SetRecorderError, SharedString, Unit,
};
use metrics_util::registry::{AtomicStorage, Registry};
use thiserror::Error;
use tokio::time::MissedTickBehavior;

pub struct Config {
    pub interval_min: Duration,
    pub interval_max: Duration,
}

#[derive(Debug, Clone)]
pub struct NatsRecorder {
    registry: Arc<Registry<Key, AtomicStorage>>,
}

impl NatsRecorder {
    pub fn install(config: Config) -> Result<(), InstallError> {
        let recorder = NatsRecorder { registry: Arc::new(Registry::new(AtomicStorage)) };
        let exporter = NatsExporter::spawn(config, recorder.clone())?;
        metrics::set_global_recorder(recorder)?;

        Ok(())
    }
}

impl Recorder for NatsRecorder {
    fn describe_counter(&self, key: KeyName, unit: Option<Unit>, description: SharedString) {
        todo!()
    }

    fn describe_gauge(
        &self,
        key: metrics::KeyName,
        unit: Option<metrics::Unit>,
        description: metrics::SharedString,
    ) {
        todo!()
    }

    fn describe_histogram(&self, key: KeyName, unit: Option<Unit>, description: SharedString) {
        todo!()
    }

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

#[derive(Debug, Error)]
pub enum InstallError {
    #[error("Set recorder; err={0}")]
    SetRecorder(#[from] SetRecorderError<NatsRecorder>),
    #[error("Spawn thread; err={0}")]
    SpawnThread(#[from] std::io::Error),
}

struct NatsExporter {
    config: Config,
    recorder: NatsRecorder,
    interval: tokio::time::Interval,
    last_full_publish: Instant,
}

impl NatsExporter {
    fn spawn(config: Config, recorder: NatsRecorder) -> Result<JoinHandle<()>, InstallError> {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;

        std::thread::Builder::new()
            .name("MetricsNats".to_string())
            .spawn(move || {
                runtime
                    .block_on(NatsExporter::setup(config, recorder).then(|exporter| exporter.run()))
            })
            .map_err(Into::into)
    }

    async fn setup(config: Config, recorder: NatsRecorder) -> Self {
        let mut interval = tokio::time::interval(config.interval_min);
        interval.set_missed_tick_behavior(MissedTickBehavior::Delay);

        NatsExporter { config, recorder, interval, last_full_publish: Instant::now() }
    }

    async fn run(mut self) {
        // TODO:
        //
        // - Setup single thread tokio runtime.
        // - Setup interval with delay mode.
        // - Store last known value/counts for each metric::Key.
        // - Every interval_min, publish any metric that has changed.
        // - Every interval_max, publish all metrics and perform any required cleanup.
        loop {
            // Wait for next publish interval.
            self.interval.tick().await;

            // Determine type of publish.
            let now = Instant::now();
            match now - self.last_full_publish > self.config.interval_max {
                true => todo!("Full publish"),
                false => todo!("Publish only changed"),
            }
        }
    }
}
