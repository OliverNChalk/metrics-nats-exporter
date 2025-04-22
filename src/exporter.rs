use std::num::NonZeroU32;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::{Duration, UNIX_EPOCH};

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
use serde::{Deserialize, Serialize};
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
    state: NatsExporterState,
    interval: tokio::time::Interval,
    last_publish_all: tokio::time::Instant,
    consecutive_skipped: u64,
}

pub(crate) struct NatsExporterState {
    metrics: HashMap<u64, (String, u64)>,
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
                U64OrF64::U64,
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
                |raw| U64OrF64::F64(f64::from_bits(raw)),
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
                U64OrF64::U64,
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
                |raw| U64OrF64::F64(f64::from_bits(raw)),
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
        T: Fn(u64) -> U64OrF64,
    {
        // Load current value.
        let curr = metric.load(Ordering::Relaxed);

        // Check if the metric has changed.
        let ((subject, prev), fresh) = match metrics.entry(key.get_hash()) {
            Entry::Occupied(entry) => (entry.into_mut(), false),
            Entry::Vacant(entry) => {
                (entry.insert((Self::metric_subject(metric_prefix, key, None), curr)), true)
            }
        };
        let should_publish = curr != *prev || publish_all || fresh;

        // Record the latest value.
        *prev = curr;

        // Publish.
        if should_publish {
            Self::publish_metric(
                client,
                client_pending,
                subject.clone(),
                #[allow(clippy::cast_precision_loss)]
                &Metric { timestamp_ms: now, value: convert(curr) },
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
        let (
            HistogramState {
                distribution,
                quantile_subjects,
                count_subject,
                sum_subject,
                previous_count,
            },
            fresh,
        ) = match histograms.entry(key.get_hash()) {
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
                let metric_subject = Self::metric_subject(metric_prefix, key, None);

                (
                    entry.insert(HistogramState {
                        quantile_subjects: quantiles
                            .iter()
                            .map(|quantile| {
                                format!("{metric_subject}.quantile={}", quantile.label())
                            })
                            .collect(),
                        distribution: Distribution::new_summary(
                            quantiles,
                            BUCKET_DURATION,
                            BUCKET_COUNT,
                        ),
                        count_subject: Self::metric_subject(metric_prefix, key, Some("count")),
                        sum_subject: Self::metric_subject(metric_prefix, key, Some("sum")),
                        previous_count: 0,
                    }),
                    true,
                )
            }
        };

        // Drain the histogram into our distribution.
        histogram.clear_with(|samples| distribution.record_samples(samples));

        // Publish our distribution.
        let Distribution::Summary(summary, quantiles, sum) = &distribution else {
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

            // Convert overview and quantiles to publishable metrics.
            #[allow(clippy::cast_precision_loss)]
            let overview = [(&*count_subject, count as f64), (sum_subject, *sum)];
            let quantiles =
                izip!(quantile_subjects.iter(), quantiles.iter()).map(|(subject, quantile)| {
                    (subject, snapshot.quantile(quantile.value()).unwrap_or(0.0))
                });

            // Publish all metrics starting with the overview.
            for (subject, val) in overview.into_iter().chain(quantiles) {
                Self::publish_metric(
                    client,
                    client_pending,
                    subject.to_string(),
                    &Metric { timestamp_ms: now, value: U64OrF64::F64(val) },
                );
            }
        }
    }

    fn publish_metric(
        client: &'static Client,
        client_pending: &FuturesUnordered<BoxFuture<'static, ()>>,
        subject: String,
        val: &Metric,
    ) {
        let val = serde_json::to_string(&val).unwrap();

        client_pending
            .push(async move { client.publish(subject, val.into()).await.unwrap() }.boxed());
    }

    fn metric_subject(
        metric_prefix: Option<&String>,
        key: &Key,
        name_suffix: Option<&str>,
    ) -> String {
        format!(
            "{}.{}{}",
            metric_prefix.map_or("metric", |s| s.as_str()),
            name_suffix
                .map(|suffix| format!("{}_{suffix}", key.name()))
                .as_deref()
                .unwrap_or_else(|| key.name()),
            key.labels()
                .map(|label| format!(".{}={}", label.key(), label.value()))
                .join(""),
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

#[derive(Debug, Serialize, Deserialize)]
struct Metric {
    #[serde(rename = "t")]
    timestamp_ms: u128,
    #[serde(rename = "v")]
    value: U64OrF64,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(untagged)]
enum U64OrF64 {
    U64(u64),
    F64(f64),
}

#[cfg(test)]
mod tests {
    use expect_test::expect;
    use metrics::Label;

    use super::*;

    #[test]
    fn metric_subject_simple() {
        expect!["metric.my_metric.key=val"].assert_eq(&NatsExporter::metric_subject(
            None,
            &Key::from_parts("my_metric", vec![Label::new("key", "val")]),
            None,
        ));
    }

    #[test]
    fn metric_subject_metric_prefix() {
        expect!["metric.my-service.my_metric.key=val"].assert_eq(&NatsExporter::metric_subject(
            Some(&"metric.my-service".to_string()),
            &Key::from_parts("my_metric", vec![Label::new("key", "val")]),
            None,
        ));
    }

    #[test]
    fn metric_subject_name_suffix() {
        expect!["metric.my_metric_sum.key=val"].assert_eq(&NatsExporter::metric_subject(
            None,
            &Key::from_parts("my_metric", vec![Label::new("key", "val")]),
            Some("sum"),
        ));
    }

    #[test]
    fn serialize_metric_u64() {
        let metric = Metric { timestamp_ms: 123, value: U64OrF64::U64(100) };

        let serialized = serde_json::to_string(&metric).unwrap();

        expect![[r#"{"t":123,"v":100}"#]].assert_eq(&serialized);
    }

    #[test]
    fn serialize_metric_f64() {
        let metric = Metric { timestamp_ms: 123, value: U64OrF64::F64(100.0) };

        let serialized = serde_json::to_string(&metric).unwrap();

        expect![[r#"{"t":123,"v":100.0}"#]].assert_eq(&serialized);
    }
}
