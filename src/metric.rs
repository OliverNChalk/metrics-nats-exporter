use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

/// Metric representation.
///
/// Metrics are serialized using this type into the NATS message body.
#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Metric {
    pub timestamp_ms: u128,
    #[serde(flatten)]
    pub variant: MetricVariant,
    pub tags: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub(crate) struct MetricBorrowed<'a> {
    pub(crate) timestamp_ms: u128,
    #[serde(flatten)]
    pub(crate) variant: MetricVariant,
    #[serde(borrow)]
    pub(crate) tags: &'a BTreeMap<String, String>,
}

impl<'a> From<&'a Metric> for MetricBorrowed<'a> {
    fn from(value: &'a Metric) -> Self {
        MetricBorrowed {
            timestamp_ms: value.timestamp_ms,
            variant: value.variant,
            tags: &value.tags,
        }
    }
}

/// Enumeration of possible metric variants.
#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum MetricVariant {
    Counter(u64),
    Gauge(f64),
    Histogram(Histogram),
}

/// Histogram summary.
#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct Histogram {
    pub count: u64,
    pub sum: f64,
    pub min: f64,
    pub p50: f64,
    pub p90: f64,
    pub p99: f64,
    pub p999: f64,
    pub max: f64,
}

#[cfg(test)]
mod tests {
    use proptest::proptest;

    use super::*;

    #[test]
    fn metric_borrowed_equivalent_json() {
        proptest!(|(a: Metric)| {
            let borrowed = serde_json::to_string(&MetricBorrowed::from(&a)).unwrap();
            let owned = serde_json::to_string(&a).unwrap();

            assert_eq!(borrowed, owned);
        });
    }

    #[test]
    fn metric_borrowed_equivalent_json_pretty() {
        proptest!(|(a: Metric)| {
            let borrowed = serde_json::to_string(&MetricBorrowed::from(&a)).unwrap();
            let owned = serde_json::to_string(&a).unwrap();

            assert_eq!(borrowed, owned);
        });
    }
}
