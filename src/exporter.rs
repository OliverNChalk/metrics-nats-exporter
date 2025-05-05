use std::collections::BTreeMap;
use std::num::NonZeroU32;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::{Duration, UNIX_EPOCH};

use async_nats::{Client, Subject};
use futures::future::BoxFuture;
use futures::stream::FuturesUnordered;
use futures::{FutureExt, StreamExt};
use hashbrown::hash_map::Entry;
use hashbrown::HashMap;
use metrics::Key;
use metrics_exporter_prometheus::Distribution;
use metrics_util::Quantile;
use tokio::time::MissedTickBehavior;
use tokio_util::sync::CancellationToken;
use tracing::warn;

use crate::atomic_storage::AtomicBucketInstant;
use crate::recorder::NatsRecorder;
use crate::{Config, InstallError, MetricBorrowed, MetricVariant};

const BUCKET_COUNT: NonZeroU32 = NonZeroU32::new(3).unwrap();
const BUCKET_DURATION: Duration = Duration::from_secs(20);

pub(crate) struct NatsExporter {
    cxl: CancellationToken,
    config: Config,

    recorder: NatsRecorder,
    state: NatsExporterState,
    interval: tokio::time::Interval,
    last_publish_all: tokio::time::Instant,
    consecutive_skipped: u64,
}

pub(crate) struct NatsExporterState {
    metrics: HashMap<u64, MetricState>,
    histograms: HashMap<u64, HistogramState>,
    client: &'static Client,
    client_pending: FuturesUnordered<BoxFuture<'static, ()>>,
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
            state: NatsExporterState {
                metrics: HashMap::default(),
                histograms: HashMap::default(),
                client,
                client_pending: FuturesUnordered::default(),
            },
            interval,
            last_publish_all: tokio::time::Instant::now(),
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

                Some(()) = self.state.client_pending.next() => {},
            }
        }
    }

    fn tick(&mut self, interval_start: tokio::time::Instant) {
        // Backoff if previous publish has not been processed yet.
        #[allow(clippy::arithmetic_side_effects)]
        if !self.state.client_pending.is_empty() {
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
        let now = UNIX_EPOCH.elapsed().unwrap().as_millis();

        self.recorder.registry.visit_counters(|key, counter| {
            Self::handle_metric(
                &mut self.state,
                self.config.metric_prefix.as_ref(),
                key,
                counter,
                MetricVariant::Counter,
                true,
                now,
            );
        });

        self.recorder.registry.visit_gauges(|key, gauge| {
            Self::handle_metric(
                &mut self.state,
                self.config.metric_prefix.as_ref(),
                key,
                gauge,
                |raw| MetricVariant::Gauge(f64::from_bits(raw)),
                true,
                now,
            );
        });

        self.recorder.registry.visit_histograms(|key, histogram| {
            Self::handle_histogram(
                &mut self.state,
                self.config.metric_prefix.as_ref(),
                key,
                histogram,
                true,
                now,
            );
        });
    }

    fn publish_changed(&mut self) {
        let now = UNIX_EPOCH.elapsed().unwrap().as_millis();

        self.recorder.registry.visit_counters(|key, counter| {
            Self::handle_metric(
                &mut self.state,
                self.config.metric_prefix.as_ref(),
                key,
                counter,
                MetricVariant::Counter,
                false,
                now,
            );
        });

        self.recorder.registry.visit_gauges(|key, gauge| {
            Self::handle_metric(
                &mut self.state,
                self.config.metric_prefix.as_ref(),
                key,
                gauge,
                |raw| MetricVariant::Gauge(f64::from_bits(raw)),
                false,
                now,
            );
        });

        self.recorder.registry.visit_histograms(|key, histogram| {
            Self::handle_histogram(
                &mut self.state,
                self.config.metric_prefix.as_ref(),
                key,
                histogram,
                false,
                now,
            );
        });
    }

    fn handle_metric<T>(
        NatsExporterState { metrics, client, client_pending, .. }: &mut NatsExporterState,
        metric_prefix: Option<&String>,
        key: &Key,
        metric: &Arc<AtomicU64>,
        convert: T,
        publish_all: bool,
        now: u128,
    ) where
        T: Fn(u64) -> MetricVariant,
    {
        // Load current value.
        let curr = metric.load(Ordering::Relaxed);

        // Check if the metric has changed.
        let (MetricState { subject, tags, previous }, fresh) = match metrics.entry(key.get_hash()) {
            Entry::Occupied(entry) => (entry.into_mut(), false),
            Entry::Vacant(entry) => (
                entry.insert(MetricState {
                    subject: Self::metric_subject(metric_prefix, key),
                    tags: key
                        .labels()
                        .map(|label| (label.key().to_string(), label.value().to_string()))
                        .collect(),
                    previous: curr,
                }),
                true,
            ),
        };
        let should_publish = curr != *previous || publish_all || fresh;

        // Record the latest value.
        *previous = curr;

        // Publish.
        if should_publish {
            Self::publish_metric(
                client,
                client_pending,
                subject.clone(),
                &MetricBorrowed { timestamp_ms: now, variant: convert(curr), tags },
            );
        }
    }

    fn handle_histogram(
        NatsExporterState { histograms, client, client_pending, .. }: &mut NatsExporterState,
        metric_prefix: Option<&String>,
        key: &Key,
        histogram: &Arc<AtomicBucketInstant<f64>>,
        publish_all: bool,
        now: u128,
    ) {
        let (HistogramState { subject, tags, distribution, previous_count }, fresh) =
            match histograms.entry(key.get_hash()) {
                Entry::Occupied(entry) => (entry.into_mut(), false),
                Entry::Vacant(entry) => {
                    let quantiles = Arc::new(vec![
                        Quantile::new(0.0),
                        Quantile::new(0.50),
                        Quantile::new(0.90),
                        Quantile::new(0.99),
                        Quantile::new(0.999),
                        Quantile::new(1.0),
                    ]);

                    (
                        entry.insert(HistogramState {
                            subject: Self::metric_subject(metric_prefix, key),
                            tags: key
                                .labels()
                                .map(|label| (label.key().to_string(), label.value().to_string()))
                                .collect(),
                            distribution: Distribution::new_summary(
                                quantiles,
                                BUCKET_DURATION,
                                BUCKET_COUNT,
                            ),
                            previous_count: 0,
                        }),
                        true,
                    )
                }
            };

        // Drain the histogram into our distribution.
        histogram.clear_with(|samples| distribution.record_samples(samples));

        // Publish our distribution.
        let Distribution::Summary(summary, _, sum) = &distribution else {
            panic!();
        };

        // Check if we should publish.
        let count = summary.count();
        assert!(
            count >= *previous_count,
            "Count invariant broken (not monotonic); prev={previous_count}; next={count}"
        );
        let should_publish = publish_all || fresh || count != *previous_count;

        // Update previous count.
        *previous_count = count;

        if should_publish {
            let snapshot = summary.snapshot(quanta::Instant::now());

            // Publish all metrics starting with the overview.
            Self::publish_metric(
                client,
                client_pending,
                subject.clone(),
                &MetricBorrowed {
                    timestamp_ms: now,
                    variant: MetricVariant::Histogram(crate::Histogram {
                        count: count as u64,
                        sum: *sum,
                        min: snapshot.quantile(0.0).unwrap_or(0.0),
                        p50: snapshot.quantile(0.50).unwrap_or(0.0),
                        p90: snapshot.quantile(0.90).unwrap_or(0.0),
                        p99: snapshot.quantile(0.99).unwrap_or(0.0),
                        p999: snapshot.quantile(0.999).unwrap_or(0.0),
                        max: snapshot.quantile(1.0).unwrap_or(0.0),
                    }),
                    tags,
                },
            );
        }
    }

    fn publish_metric(
        client: &'static Client,
        client_pending: &FuturesUnordered<BoxFuture<'static, ()>>,
        subject: Subject,
        val: &MetricBorrowed,
    ) {
        let val = serde_json::to_string(&val).unwrap();

        client_pending
            .push(async move { client.publish(subject, val.into()).await.unwrap() }.boxed());
    }

    fn metric_subject(metric_prefix: Option<&String>, key: &Key) -> Subject {
        format!("{}.{}", metric_prefix.map_or("metric", |s| s.as_str()), key.name()).into()
    }
}

struct MetricState {
    subject: Subject,
    tags: BTreeMap<String, String>,
    previous: u64,
}

struct HistogramState {
    subject: Subject,
    tags: BTreeMap<String, String>,
    distribution: Distribution,
    previous_count: usize,
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use expect_test::expect;
    use metrics::Label;

    use super::*;

    #[test]
    fn metric_subject_simple() {
        expect!["metric.my_metric"].assert_eq(&NatsExporter::metric_subject(
            None,
            &Key::from_parts("my_metric", vec![Label::new("key", "val")]),
        ));
    }

    #[test]
    fn metric_subject_metric_prefix() {
        expect!["metric.my-service.my_metric"].assert_eq(&NatsExporter::metric_subject(
            Some(&"metric.my-service".to_string()),
            &Key::from_parts("my_metric", vec![Label::new("key", "val")]),
        ));
    }

    #[test]
    fn serialize_counter() {
        let metric = MetricBorrowed {
            timestamp_ms: 123,
            variant: MetricVariant::Counter(100),
            tags: &BTreeMap::from_iter([
                ("a".to_string(), "0".to_string()),
                ("b".to_string(), "0".to_string()),
            ]),
        };

        let serialized = serde_json::to_string_pretty(&metric).unwrap();

        expect![[r#"
            {
              "timestamp_ms": 123,
              "counter": 100,
              "tags": {
                "a": "0",
                "b": "0"
              }
            }"#]]
            .assert_eq(&serialized);
    }

    #[test]
    fn serialize_gauge() {
        let metric = MetricBorrowed {
            timestamp_ms: 123,
            variant: MetricVariant::Gauge(100.0),
            tags: &BTreeMap::from_iter([
                ("a".to_string(), "0".to_string()),
                ("b".to_string(), "0".to_string()),
            ]),
        };

        let serialized = serde_json::to_string_pretty(&metric).unwrap();

        expect![[r#"
            {
              "timestamp_ms": 123,
              "gauge": 100.0,
              "tags": {
                "a": "0",
                "b": "0"
              }
            }"#]]
            .assert_eq(&serialized);
    }

    #[test]
    fn serialize_histogram() {
        let metric = MetricBorrowed {
            timestamp_ms: 123,
            variant: MetricVariant::Histogram(crate::Histogram {
                count: 20,
                sum: 100.5,
                min: 0.12,
                p50: 2.0,
                p90: 10.1,
                p99: 50.12,
                p999: 100.0,
                max: 1003.0,
            }),
            tags: &BTreeMap::from_iter([
                ("a".to_string(), "0".to_string()),
                ("b".to_string(), "0".to_string()),
            ]),
        };

        let serialized = serde_json::to_string_pretty(&metric).unwrap();

        expect![[r#"
            {
              "timestamp_ms": 123,
              "histogram": {
                "count": 20,
                "sum": 100.5,
                "min": 0.12,
                "p50": 2.0,
                "p90": 10.1,
                "p99": 50.12,
                "p999": 100.0,
                "max": 1003.0
              },
              "tags": {
                "a": "0",
                "b": "0"
              }
            }"#]]
            .assert_eq(&serialized);
    }
}
