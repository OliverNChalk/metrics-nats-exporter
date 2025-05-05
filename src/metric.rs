use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_with::serde_as;

/// Metric emitted over NATS by this crate.
///
/// This model complies with the [Vector.dev metric data model][0].
///
/// [0]: https://vector.dev/docs/about/under-the-hood/architecture/data-model/metric
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Metric {
    pub name: String,
    #[serde(flatten)]
    pub variant: MetricVariant,
    pub tags: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(from = "MetricVector", into = "MetricVector")]
pub(crate) struct MetricBorrowed<'a> {
    pub(crate) name: String,
    #[serde(flatten)]
    pub(crate) variant: MetricVariant,
    #[serde(borrow)]
    pub(crate) tags: BTreeMap<&'a str, &'a str>,
}

#[serde_as]
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum MetricVariant {
    Counter(#[serde_as(as = "serde_with::FromInto<ValueWrapper<u64>>")] u64),
    Gauge(#[serde_as(as = "serde_with::FromInto<ValueWrapper<f64>>")] f64),
    #[serde(rename = "summary")]
    Histogram(#[serde_as(as = "serde_with::FromInto<ValueWrapper<Histogram>>")] Histogram),
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct Histogram {
    pub min: f64,
    pub p50: f64,
    pub p90: f64,
    pub p99: f64,
    pub p999: f64,
    pub max: f64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct MetricVector<'a> {
    name: String,
    #[serde(flatten)]
    variant: MetricVariant,
    #[serde(skip_serializing_if = "Option::is_none")]
    kind: Option<CounterKind>,
    #[serde(borrow)]
    tags: BTreeMap<&'a str, &'a str>,
}

impl<'a> From<MetricBorrowed<'a>> for MetricVector<'a> {
    fn from(value: MetricBorrowed<'a>) -> Self {
        MetricVector {
            name: value.name,
            variant: value.variant,
            kind: match value.variant {
                MetricVariant::Counter(_) => Some(CounterKind::Absolute),
                _ => None,
            },
            tags: value.tags,
        }
    }
}

impl<'a> From<MetricVector<'a>> for MetricBorrowed<'a> {
    fn from(value: MetricVector<'a>) -> Self {
        MetricBorrowed { name: value.name, variant: value.variant, tags: value.tags }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
struct ValueWrapper<T> {
    value: T,
}

impl<T> From<T> for ValueWrapper<T> {
    fn from(value: T) -> Self {
        ValueWrapper { value }
    }
}

impl From<ValueWrapper<u64>> for u64 {
    fn from(value: ValueWrapper<u64>) -> Self {
        value.value
    }
}

impl From<ValueWrapper<f64>> for f64 {
    fn from(value: ValueWrapper<f64>) -> Self {
        value.value
    }
}

impl From<ValueWrapper<Histogram>> for Histogram {
    fn from(value: ValueWrapper<Histogram>) -> Self {
        value.value
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
enum CounterKind {
    Absolute,
}

#[cfg(test)]
mod tests {
    use expect_test::expect;

    use super::*;

    #[test]
    fn serialize_counter() {
        let val = Metric {
            name: "test_counter".to_string(),
            variant: MetricVariant::Counter(125),
            tags: BTreeMap::from_iter([
                ("a".to_string(), "0".to_string()),
                ("b".to_string(), "1".to_string()),
            ]),
        };
        let serialized = serde_json::to_string_pretty(&val).unwrap();

        // Assert.
        expect![[r#"
            {
              "name": "test_counter",
              "counter": {
                "value": 125
              },
              "kind": "absolute",
              "tags": {
                "a": "0",
                "b": "1"
              }
            }"#]]
        .assert_eq(&serialized);
    }

    #[test]
    fn serialize_gauge() {
        let val = Metric {
            name: "test_gauge".to_string(),
            variant: MetricVariant::Gauge(125.0),
            tags: BTreeMap::from_iter([
                ("a".to_string(), "0".to_string()),
                ("b".to_string(), "1".to_string()),
            ]),
        };
        let serialized = serde_json::to_string_pretty(&val).unwrap();

        // Assert.
        expect![[r#"
            {
              "name": "test_gauge",
              "gauge": {
                "value": 125.0
              },
              "tags": {
                "a": "0",
                "b": "1"
              }
            }"#]]
        .assert_eq(&serialized);
    }

    #[test]
    fn serialize_histogram() {
        let val = Metric {
            name: "test_histogram".to_string(),
            variant: MetricVariant::Histogram(Histogram {
                min: 1.0,
                p50: 5.25,
                p90: 10.50,
                p99: 20.0,
                p999: 118.3,
                max: 621.1,
            }),
            tags: BTreeMap::from_iter([
                ("a".to_string(), "0".to_string()),
                ("b".to_string(), "1".to_string()),
            ]),
        };
        let serialized = serde_json::to_string_pretty(&val).unwrap();

        // Assert.
        expect![[r#"
            {
              "name": "test_histogram",
              "summary": {
                "value": {
                  "min": 1.0,
                  "p50": 5.25,
                  "p90": 10.5,
                  "p99": 20.0,
                  "p999": 118.3,
                  "max": 621.1
                }
              },
              "tags": {
                "a": "0",
                "b": "1"
              }
            }"#]]
        .assert_eq(&serialized);
    }
}
