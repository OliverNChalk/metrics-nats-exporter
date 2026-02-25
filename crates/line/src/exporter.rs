use std::collections::BTreeMap;
use std::num::NonZeroU32;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::{Duration, UNIX_EPOCH};

use hashbrown::hash_map::Entry;
use hashbrown::HashMap;
use metrics::Key;
use metrics_exporter_prometheus::Distribution;
use metrics_util::Quantile;
use reqwest::Client;
use tokio::time::MissedTickBehavior;
use tokio_util::sync::CancellationToken;
use tracing::warn;

use crate::atomic_storage::AtomicBucketInstant;
use crate::recorder::LineRecorder;
use crate::{Config, InstallError};

const BUCKET_COUNT: NonZeroU32 = NonZeroU32::new(3).unwrap();
const BUCKET_DURATION: Duration = Duration::from_secs(20);

pub(crate) struct LineExporter {
    cxl: CancellationToken,
    config: Config,

    recorder: LineRecorder,
    state: LineExporterState,
    tick_interval: tokio::time::Interval,
    flush_interval: tokio::time::Interval,
    last_publish_all: tokio::time::Instant,
    consecutive_skipped: u64,
    buffer: String,
}

pub(crate) struct LineExporterState {
    metrics: HashMap<u64, MetricState>,
    histograms: HashMap<u64, HistogramState>,
    http_client: Client,
    write_url: String,
    pending_request: Option<tokio::task::JoinHandle<()>>,
}

impl LineExporter {
    pub(crate) fn spawn(
        cxl: CancellationToken,
        config: Config,
        recorder: LineRecorder,
    ) -> Result<JoinHandle<()>, InstallError> {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;

        std::thread::Builder::new()
            .name("MetricsLine".to_string())
            .spawn(move || {
                let exporter = {
                    let _guard = runtime.enter();
                    LineExporter::setup(cxl, config, recorder)
                };
                runtime.block_on(exporter.run());
            })
            .map_err(Into::into)
    }

    fn setup(cxl: CancellationToken, config: Config, recorder: LineRecorder) -> Self {
        let mut tick_interval = tokio::time::interval(config.interval_min);
        tick_interval.set_missed_tick_behavior(MissedTickBehavior::Delay);

        let mut flush_interval = tokio::time::interval(config.flush_interval);
        flush_interval.set_missed_tick_behavior(MissedTickBehavior::Delay);

        let write_url = build_write_url(&config);
        let http_client = Client::new();

        LineExporter {
            config,
            cxl,

            recorder,
            state: LineExporterState {
                metrics: HashMap::default(),
                histograms: HashMap::default(),
                http_client,
                write_url,
                pending_request: None,
            },
            tick_interval,
            flush_interval,
            last_publish_all: tokio::time::Instant::now(),
            consecutive_skipped: 0,
            buffer: String::new(),
        }
    }

    async fn run(mut self) {
        self.collect_metrics(true);

        loop {
            tokio::select! {
                biased;

                () = self.cxl.cancelled() => break,
                now = self.tick_interval.tick() => self.tick(now),
                _ = self.flush_interval.tick() => self.flush(),
            }
        }
    }

    fn tick(&mut self, interval_start: tokio::time::Instant) {
        // Determine if we should perform a full collection.
        let publish_all = interval_start - self.last_publish_all > self.config.interval_max;
        if publish_all {
            self.last_publish_all = interval_start;
        }
        self.collect_metrics(publish_all);
    }

    fn flush(&mut self) {
        if self.buffer.is_empty() {
            return;
        }

        // Check if previous request is still in-flight.
        if self
            .state
            .pending_request
            .as_ref()
            .is_some_and(|h| !h.is_finished())
        {
            self.consecutive_skipped += 1;
            warn!(
                self.consecutive_skipped,
                "LineExporter flush skipped, previous request still in-flight"
            );

            return;
        }

        // Reset consecutive skip counter.
        self.consecutive_skipped = 0;

        // Take the buffer and send it.
        let body = std::mem::take(&mut self.buffer);
        let client = self.state.http_client.clone();
        let url = self.state.write_url.clone();

        self.state.pending_request = Some(tokio::spawn(async move {
            match client.post(&url).body(body).send().await {
                Ok(resp) if !resp.status().is_success() => {
                    let status = resp.status();
                    let body = resp.text().await.unwrap_or_default();
                    warn!(%status, %body, "InfluxDB write failed");
                }
                Err(err) => {
                    warn!(%err, "InfluxDB write request failed");
                }
                Ok(_) => {}
            }
        }));
    }

    fn collect_metrics(&mut self, publish_all: bool) {
        let now = UNIX_EPOCH.elapsed().unwrap().as_nanos();

        self.recorder.registry.visit_counters(|key, counter| {
            Self::handle_metric(
                &mut self.state,
                &mut self.buffer,
                &self.config,
                key,
                counter,
                MetricValue::Counter,
                publish_all,
                now,
            );
        });

        self.recorder.registry.visit_gauges(|key, gauge| {
            Self::handle_metric(
                &mut self.state,
                &mut self.buffer,
                &self.config,
                key,
                gauge,
                |raw| MetricValue::Gauge(f64::from_bits(raw)),
                publish_all,
                now,
            );
        });

        self.recorder.registry.visit_histograms(|key, histogram| {
            Self::handle_histogram(
                &mut self.state,
                &mut self.buffer,
                &self.config,
                key,
                histogram,
                publish_all,
                now,
            );
        });
    }

    #[allow(clippy::too_many_arguments)]
    fn handle_metric<T>(
        state: &mut LineExporterState,
        buffer: &mut String,
        config: &Config,
        key: &Key,
        metric: &Arc<AtomicU64>,
        convert: T,
        publish_all: bool,
        now: u128,
    ) where
        T: Fn(u64) -> MetricValue,
    {
        // Load current value.
        let curr = metric.load(Ordering::Relaxed);

        // Check if the metric has changed.
        let (MetricState { name, tags, previous }, fresh) =
            match state.metrics.entry(key.get_hash()) {
                Entry::Occupied(entry) => (entry.into_mut(), false),
                Entry::Vacant(entry) => (
                    entry.insert(MetricState {
                        name: metric_name(config.metric_prefix.as_deref(), key),
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

        // Write to buffer.
        if should_publish {
            match convert(curr) {
                MetricValue::Counter(v) => {
                    crate::line_protocol::write_counter(
                        buffer,
                        name,
                        tags,
                        &config.default_tags,
                        v,
                        now,
                    );
                }
                MetricValue::Gauge(v) => {
                    crate::line_protocol::write_gauge(
                        buffer,
                        name,
                        tags,
                        &config.default_tags,
                        v,
                        now,
                    );
                }
            }
        }
    }

    fn handle_histogram(
        state: &mut LineExporterState,
        buffer: &mut String,
        config: &Config,
        key: &Key,
        histogram: &Arc<AtomicBucketInstant<f64>>,
        publish_all: bool,
        now: u128,
    ) {
        let (HistogramState { name, tags, distribution, previous_count }, fresh) =
            match state.histograms.entry(key.get_hash()) {
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
                            name: metric_name(config.metric_prefix.as_deref(), key),
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
            unreachable!();
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

            crate::line_protocol::write_histogram(
                buffer,
                name,
                tags,
                &config.default_tags,
                &crate::line_protocol::HistogramSnapshot {
                    count: u64::try_from(count).unwrap(),
                    sum: *sum,
                    min: snapshot.quantile(0.0).unwrap_or(0.0),
                    p50: snapshot.quantile(0.50).unwrap_or(0.0),
                    p90: snapshot.quantile(0.90).unwrap_or(0.0),
                    p99: snapshot.quantile(0.99).unwrap_or(0.0),
                    p999: snapshot.quantile(0.999).unwrap_or(0.0),
                    max: snapshot.quantile(1.0).unwrap_or(0.0),
                },
                now,
            );
        }
    }
}

fn build_write_url(config: &Config) -> String {
    let mut url = format!("{}/write?db={}&precision=ns", config.endpoint, config.database);

    if let Some(username) = &config.username {
        url.push_str("&u=");
        url.push_str(username);
    }
    if let Some(password) = &config.password {
        url.push_str("&p=");
        url.push_str(password);
    }

    url
}

fn metric_name(prefix: Option<&str>, key: &Key) -> String {
    match prefix {
        Some(p) => format!("{p}.{}", key.name()),
        None => key.name().to_string(),
    }
}

enum MetricValue {
    Counter(u64),
    Gauge(f64),
}

struct MetricState {
    name: String,
    tags: BTreeMap<String, String>,
    previous: u64,
}

struct HistogramState {
    name: String,
    tags: BTreeMap<String, String>,
    distribution: Distribution,
    previous_count: usize,
}

#[cfg(test)]
mod tests {
    use expect_test::expect;
    use metrics::Label;

    use super::*;

    #[test]
    fn metric_name_no_prefix() {
        expect!["my_metric"].assert_eq(&metric_name(
            None,
            &Key::from_parts("my_metric", vec![Label::new("key", "val")]),
        ));
    }

    #[test]
    fn metric_name_with_prefix() {
        expect!["my-service.my_metric"].assert_eq(&metric_name(
            Some("my-service"),
            &Key::from_parts("my_metric", vec![Label::new("key", "val")]),
        ));
    }

    #[test]
    fn build_write_url_basic() {
        let config = Config {
            interval_min: Duration::from_secs(1),
            interval_max: Duration::from_secs(60),
            flush_interval: Duration::from_secs(5),
            endpoint: "http://localhost:8086".to_string(),
            database: "mydb".to_string(),
            username: None,
            password: None,
            metric_prefix: None,
            default_tags: BTreeMap::new(),
        };

        expect!["http://localhost:8086/write?db=mydb&precision=ns"]
            .assert_eq(&build_write_url(&config));
    }

    #[test]
    fn build_write_url_with_auth() {
        let config = Config {
            interval_min: Duration::from_secs(1),
            interval_max: Duration::from_secs(60),
            flush_interval: Duration::from_secs(5),
            endpoint: "http://localhost:8086".to_string(),
            database: "mydb".to_string(),
            username: Some("admin".to_string()),
            password: Some("secret".to_string()),
            metric_prefix: None,
            default_tags: BTreeMap::new(),
        };

        expect!["http://localhost:8086/write?db=mydb&precision=ns&u=admin&p=secret"]
            .assert_eq(&build_write_url(&config));
    }
}
