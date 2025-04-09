use std::num::NonZeroU32;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::Duration;

use async_nats::Client;
use futures::future::BoxFuture;
use futures::stream::FuturesUnordered;
use futures::{FutureExt, StreamExt};
use hashbrown::HashMap;
use itertools::Itertools;
use metrics::{Counter, Gauge, Histogram, Key, KeyName, Metadata, Recorder, SharedString, Unit};
use metrics_exporter_prometheus::Distribution;
use metrics_util::registry::Registry;
use metrics_util::Quantile;
use tokio::time::MissedTickBehavior;
use tokio_util::sync::CancellationToken;
use tracing::warn;

use crate::atomic_storage::AtomicStorage;
use crate::{Config, InstallError};

const BUCKET_COUNT: NonZeroU32 = NonZeroU32::new(3).unwrap();
const BUCKET_DURATION: Duration = Duration::from_secs(20);
// TODO: Need to include labels in subjects else we'll get metric collisions.

#[derive(Debug, Clone)]
pub struct NatsRecorder {
    registry: Arc<Registry<Key, AtomicStorage>>,
}

impl NatsRecorder {
    pub(crate) fn install(
        cxl: CancellationToken,
        config: Config,
    ) -> Result<JoinHandle<()>, InstallError> {
        let recorder = NatsRecorder { registry: Arc::new(Registry::new(AtomicStorage)) };
        let exporter = NatsExporter::spawn(cxl, config, recorder.clone())?;
        metrics::set_global_recorder(recorder)?;

        Ok(exporter)
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

struct NatsExporter {
    cxl: CancellationToken,
    config: Config,

    recorder: NatsRecorder,
    metrics: HashMap<u64, (String, u64)>,
    histograms: HashMap<u64, HistogramState>,
    interval: tokio::time::Interval,
    last_publish_all: tokio::time::Instant,
    client: &'static Client,
    client_pending: FuturesUnordered<BoxFuture<'static, ()>>,
    consecutive_skipped: u64,
}

impl NatsExporter {
    fn spawn(
        cxl: CancellationToken,
        config: Config,
        recorder: NatsRecorder,
    ) -> Result<JoinHandle<()>, InstallError> {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;

        std::thread::Builder::new()
            .name("MetricsNats".to_string())
            .spawn(move || {
                runtime.block_on(
                    NatsExporter::setup(cxl, config, recorder).then(|exporter| exporter.run()),
                )
            })
            .map_err(Into::into)
    }

    async fn setup(cxl: CancellationToken, config: Config, recorder: NatsRecorder) -> Self {
        let mut interval = tokio::time::interval(config.interval_min);
        interval.set_missed_tick_behavior(MissedTickBehavior::Delay);

        let client = Box::leak(Box::new(async_nats::connect(&config.nats_servers).await.unwrap()));

        NatsExporter {
            config,
            cxl,

            recorder,
            metrics: HashMap::default(),
            histograms: HashMap::default(),
            interval,
            last_publish_all: tokio::time::Instant::now(),
            client,
            client_pending: FuturesUnordered::default(),
            consecutive_skipped: 0,
        }
    }

    async fn run(mut self) {
        self.publish_all();

        loop {
            tokio::select! {
                biased;

                _ = self.cxl.cancelled() => break,
                now = self.interval.tick() => self.tick(now),

                Some(()) = self.client_pending.next() => {},
            }
        }
    }

    fn tick(&mut self, interval_start: tokio::time::Instant) {
        // TODO: Check if histograms need resetting.

        // Backoff if previous publish has not been processed yet.
        if !self.client_pending.is_empty() {
            self.consecutive_skipped += 1;
            warn!(self.consecutive_skipped, "NatsExporter not keeping up, skipping publish");

            return;
        }

        // Reset the consecutive skip counter as we're able to publish.
        self.consecutive_skipped = 0;

        // Determine if we should perform a full publish.
        match interval_start - self.last_publish_all > self.config.interval_max {
            true => {
                self.publish_all();
                self.last_publish_all = interval_start;
            }
            false => self.publish_changed(),
        }
    }

    fn publish_all(&mut self) {
        self.recorder.registry.visit_counters(|key, counter| {
            // Record the latest value.
            let curr = counter.load(Ordering::Relaxed);
            let (subject, prev) = self
                .metrics
                .entry(key.get_hash())
                .or_insert_with(|| (Self::metric_subject(key), curr));
            *prev = curr;

            // Publish.
            Self::publish_metric(self.client, &self.client_pending, subject.to_string(), curr);
        });

        self.recorder.registry.visit_gauges(|key, gauge| {
            // Record the latest value.
            let curr = gauge.load(Ordering::Relaxed);
            let (subject, prev) = self
                .metrics
                .entry(key.get_hash())
                .or_insert_with(|| (Self::metric_subject(key), curr));
            *prev = curr;

            // Publish.
            Self::publish_metric(self.client, &self.client_pending, subject.to_string(), curr);
        });

        // TODO: Publish histograms.
        self.recorder.registry.visit_histograms(|key, histogram| {
            let HistogramState { distribution, quantile_subjects, count_subject, sum_subject } =
                self.histograms.entry(key.get_hash()).or_insert_with(|| {
                    let quantiles = Arc::new(vec![
                        Quantile::new(0.0),
                        Quantile::new(0.50),
                        Quantile::new(0.90),
                        Quantile::new(0.99),
                        Quantile::new(0.999),
                        Quantile::new(1.0),
                    ]);
                    let metrics_subject = Self::metric_subject(key);

                    HistogramState {
                        quantile_subjects: quantiles
                            .iter()
                            .map(|quantile| format!("{metrics_subject}.{}", quantile.label()))
                            .collect(),
                        distribution: Distribution::new_summary(
                            quantiles,
                            BUCKET_DURATION,
                            BUCKET_COUNT,
                        ),
                        count_subject: format!("{metrics_subject}.count"),
                        sum_subject: format!("{metrics_subject}.sum"),
                    }
                });

            // Drain the histogram into our distribution.
            histogram.clear_with(|samples| distribution.record_samples(samples));

            // Publish our distribution.
            let Distribution::Summary(summary, quantiles, sum) = distribution else {
                panic!();
            };
            let snapshot = summary.snapshot(quanta::Instant::now());

            // TODO: chain in sum and count into the publishing iterator.

            // Publish all quantiles.
            for (quantile, subject) in quantiles.iter().zip(quantile_subjects) {
                Self::publish_metric(
                    self.client,
                    &self.client_pending,
                    subject.to_string(),
                    snapshot.quantile(quantile.value()).unwrap_or(0.0),
                );
            }
        });
    }

    fn publish_changed(&mut self) {
        self.recorder.registry.visit_counters(|key, counter| {
            // Check if the counter has changed.
            let curr = counter.load(Ordering::Relaxed);
            let (subject, prev) = self.metrics.entry(key.get_hash()).or_default();
            if curr == *prev {
                return;
            }

            // Record the latest value.
            *prev = curr;

            // Publish.
            Self::publish_metric(self.client, &self.client_pending, subject.to_string(), curr);
        });

        self.recorder.registry.visit_gauges(|key, gauge| {
            // Check if the gauge has changed.
            let curr = gauge.load(Ordering::Relaxed);
            let (subject, prev) = self.metrics.entry(key.get_hash()).or_default();
            if curr == *prev {
                return;
            }

            // Record the latest value.
            *prev = curr;

            // Publish.
            Self::publish_metric(self.client, &self.client_pending, subject.to_string(), curr);
        });

        // TODO: Publish histograms.
    }

    fn publish_metric(
        client: &'static Client,
        client_pending: &FuturesUnordered<BoxFuture<'static, ()>>,
        subject: String,
        val: impl ToString,
    ) {
        let val = val.to_string();

        client_pending
            .push(async move { client.publish(subject, val.into()).await.unwrap() }.boxed());
    }

    fn metric_subject(key: &Key) -> String {
        format!(
            "metric.host.{}.{}",
            key.name(),
            key.labels()
                .map(|label| format!("{}={}", label.key(), label.value()))
                .join(".")
        )
    }
}

struct HistogramState {
    distribution: Distribution,
    quantile_subjects: Vec<String>,
    count_subject: String,
    sum_subject: String,
}
