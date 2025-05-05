use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Metric {
    pub timestamp_ms: u128,
    pub variant: MetricVariant,
    pub tags: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub(crate) struct MetricBorrowed<'a> {
    pub(crate) timestamp_ms: u128,
    pub(crate) variant: MetricVariant,
    #[serde(borrow)]
    pub(crate) tags: &'a BTreeMap<String, String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum MetricVariant {
    Counter(u64),
    Gauge(f64),
    Histogram(Histogram),
}
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
