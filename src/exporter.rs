use std::num::NonZeroU32;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::Duration;

use async_nats::Client;
use futures::future::BoxFuture;
use futures::stream::FuturesUnordered;
use futures::{FutureExt, StreamExt};
use hashbrown::hash_map::Entry;
use hashbrown::HashMap;
use itertools::{izip, Itertools};
use metrics::Key;
use metrics_exporter_prometheus::Distribution;
use metrics_util::Quantile;
use tokio::time::MissedTickBehavior;
use tokio_util::sync::CancellationToken;
use tracing::warn;

use crate::atomic_storage::AtomicBucketInstant;
use crate::recorder::NatsRecorder;
use crate::{Config, InstallError};

const BUCKET_COUNT: NonZeroU32 = NonZeroU32::new(3).unwrap();
const BUCKET_DURATION: Duration = Duration::from_secs(20);

pub(crate) struct NatsExporter {
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
    pub(crate) fn spawn(
        cxl: CancellationToken,
        config: Config,
        client: &'static Client,
        recorder: NatsRecorder,
    ) -> Result<JoinHandle<()>, InstallError> {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;

        std::thread::Builder::new()
            .name("MetricsNats".to_string())
            .spawn(move || {
                let exporter = {
                    let _guard = runtime.enter();
                    NatsExporter::setup(cxl, config, client, recorder)
                };
                runtime.block_on(exporter.run());
            })
            .map_err(Into::into)
    }

    fn setup(
        cxl: CancellationToken,
        config: Config,
        client: &'static Client,
        recorder: NatsRecorder,
    ) -> Self {
        let mut interval = tokio::time::interval(config.interval_min);
        interval.set_missed_tick_behavior(MissedTickBehavior::Delay);

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

                () = self.cxl.cancelled() => break,
                now = self.interval.tick() => self.tick(now),

                Some(()) = self.client_pending.next() => {},
            }
        }
    }

    fn tick(&mut self, interval_start: tokio::time::Instant) {
        // Backoff if previous publish has not been processed yet.
        #[allow(clippy::arithmetic_side_effects)]
        if !self.client_pending.is_empty() {
            self.consecutive_skipped += 1;
            warn!(self.consecutive_skipped, "NatsExporter not keeping up, skipping publish");

            return;
        }

        // Reset the consecutive skip counter as we're able to publish.
        self.consecutive_skipped = 0;

        // Determine if we should perform a full publish.
        #[allow(clippy::arithmetic_side_effects)]
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
            Self::handle_metric(
                &mut self.metrics,
                self.client,
                &self.client_pending,
                key,
                counter,
                true,
            );
        });

        self.recorder.registry.visit_gauges(|key, gauge| {
            Self::handle_metric(
                &mut self.metrics,
                self.client,
                &self.client_pending,
                key,
                gauge,
                true,
            );
        });

        self.recorder.registry.visit_histograms(|key, histogram| {
            Self::handle_histogram(
                &mut self.histograms,
                self.client,
                &self.client_pending,
                key,
                histogram,
                true,
            );
        });
    }

    fn publish_changed(&mut self) {
        self.recorder.registry.visit_counters(|key, counter| {
            Self::handle_metric(
                &mut self.metrics,
                self.client,
                &self.client_pending,
                key,
                counter,
                false,
            );
        });

        self.recorder.registry.visit_gauges(|key, gauge| {
            Self::handle_metric(
                &mut self.metrics,
                self.client,
                &self.client_pending,
                key,
                gauge,
                false,
            );
        });

        self.recorder.registry.visit_histograms(|key, histogram| {
            Self::handle_histogram(
                &mut self.histograms,
                self.client,
                &self.client_pending,
                key,
                histogram,
                false,
            );
        });
    }

    fn handle_metric(
        metrics: &mut HashMap<u64, (String, u64)>,
        client: &'static Client,
        client_pending: &FuturesUnordered<BoxFuture<'static, ()>>,
        key: &Key,
        metric: &Arc<AtomicU64>,
        publish_all: bool,
    ) {
        // Load current value.
        let curr = metric.load(Ordering::Relaxed);

        // Check if the metric has changed.
        let ((subject, prev), new) = match metrics.entry(key.get_hash()) {
            Entry::Occupied(entry) => (entry.into_mut(), false),
            Entry::Vacant(entry) => (entry.insert((Self::metric_subject(key), curr)), false),
        };
        let should_publish = curr != *prev || publish_all || new;

        // Record the latest value.
        *prev = curr;

        // Publish.
        if should_publish {
            Self::publish_metric(client, client_pending, subject.clone(), &curr);
        }
    }

    fn handle_histogram(
        histograms: &mut HashMap<u64, HistogramState>,
        client: &'static Client,
        client_pending: &FuturesUnordered<BoxFuture<'static, ()>>,
        key: &Key,
        histogram: &Arc<AtomicBucketInstant<f64>>,
        publish_all: bool,
    ) {
        let HistogramState {
            distribution,
            quantile_subjects,
            count_subject,
            sum_subject,
            previous_count,
        } = histograms.entry(key.get_hash()).or_insert_with(|| {
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
                distribution: Distribution::new_summary(quantiles, BUCKET_DURATION, BUCKET_COUNT),
                count_subject: format!("{metrics_subject}.count"),
                sum_subject: format!("{metrics_subject}.sum"),
                previous_count: 0,
            }
        });

        // Drain the histogram into our distribution.
        histogram.clear_with(|samples| distribution.record_samples(samples));

        // Publish our distribution.
        let Distribution::Summary(summary, quantiles, sum) = &distribution else {
            panic!();
        };
        let snapshot = summary.snapshot(quanta::Instant::now());

        // Check if we should publish.
        let count = snapshot.count();
        assert!(count >= *previous_count, "Count invariant broken (not monotonic)");
        let should_publish = publish_all || count != *previous_count;

        // Update previous count.
        *previous_count = count;

        if should_publish {
            // Convert overview and quantiles to publishable metrics.
            #[allow(clippy::cast_precision_loss)]
            let overview = [(&*count_subject, count as f64), (sum_subject, *sum)];
            let quantiles =
                izip!(quantile_subjects.iter(), quantiles.iter()).map(|(subject, quantile)| {
                    (subject, snapshot.quantile(quantile.value()).unwrap_or(0.0))
                });

            // Publish all metrics starting with the overview.
            for (subject, val) in overview.into_iter().chain(quantiles) {
                Self::publish_metric(client, client_pending, subject.to_string(), &val);
            }
        }
    }

    fn publish_metric(
        client: &'static Client,
        client_pending: &FuturesUnordered<BoxFuture<'static, ()>>,
        subject: String,
        val: &impl ToString,
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
    previous_count: usize,
}
