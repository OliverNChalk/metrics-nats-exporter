use std::sync::Arc;

use metrics::atomics::AtomicU64;
use metrics::HistogramFn;
use metrics_util::registry::GenerationalStorage;
use metrics_util::storage::AtomicBucket;
use quanta::Instant;

#[derive(Debug)]
pub(crate) struct AtomicStorage;

impl<K> metrics_util::registry::Storage<K> for AtomicStorage {
    type Counter = Arc<AtomicU64>;
    type Gauge = Arc<AtomicU64>;
    type Histogram = Arc<AtomicBucketInstant<f64>>;

    fn counter(&self, _: &K) -> Self::Counter {
        Arc::new(AtomicU64::new(0))
    }

    fn gauge(&self, _: &K) -> Self::Gauge {
        Arc::new(AtomicU64::new(0))
    }

    fn histogram(&self, _: &K) -> Self::Histogram {
        Arc::new(AtomicBucketInstant::new())
    }
}

#[derive(Debug)]
pub(crate) struct AtomicBucketInstant<T> {
    inner: AtomicBucket<(T, Instant)>,
}

impl<T> AtomicBucketInstant<T> {
    fn new() -> AtomicBucketInstant<T> {
        Self { inner: AtomicBucket::new() }
    }

    pub fn clear_with<F>(&self, f: F)
    where
        F: FnMut(&[(T, Instant)]),
    {
        self.inner.clear_with(f);
    }
}

impl HistogramFn for AtomicBucketInstant<f64> {
    fn record(&self, value: f64) {
        let now = Instant::now();
        self.inner.push((value, now));
    }
}
