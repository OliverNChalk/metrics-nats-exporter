use std::collections::BTreeMap;
use std::fmt::Write;

pub(crate) struct HistogramSnapshot {
    pub count: u64,
    pub sum: f64,
    pub min: f64,
    pub p50: f64,
    pub p90: f64,
    pub p99: f64,
    pub p999: f64,
    pub max: f64,
}

pub(crate) fn write_counter(
    buf: &mut String,
    prefix: Option<&str>,
    name: &str,
    key_tags: &BTreeMap<String, String>,
    default_tags: &BTreeMap<String, String>,
    value: u64,
    timestamp_ns: u128,
) {
    write_measurement(buf, prefix, name);
    write_tags(buf, default_tags, key_tags);
    let _ = writeln!(buf, " value={value}u {timestamp_ns}");
}

pub(crate) fn write_gauge(
    buf: &mut String,
    prefix: Option<&str>,
    name: &str,
    key_tags: &BTreeMap<String, String>,
    default_tags: &BTreeMap<String, String>,
    value: f64,
    timestamp_ns: u128,
) {
    if !value.is_finite() {
        return;
    }

    write_measurement(buf, prefix, name);
    write_tags(buf, default_tags, key_tags);
    let _ = writeln!(buf, " value={value} {timestamp_ns}");
}

pub(crate) fn write_histogram(
    buf: &mut String,
    prefix: Option<&str>,
    name: &str,
    key_tags: &BTreeMap<String, String>,
    default_tags: &BTreeMap<String, String>,
    snapshot: &HistogramSnapshot,
    timestamp_ns: u128,
) {
    let HistogramSnapshot { count, sum, min, p50, p90, p99, p999, max } = snapshot;
    if ![*sum, *min, *p50, *p90, *p99, *p999, *max]
        .iter()
        .all(|v| v.is_finite())
    {
        return;
    }

    write_measurement(buf, prefix, name);
    write_tags(buf, default_tags, key_tags);
    let _ = writeln!(
        buf,
        " count={count}u,sum={sum},min={min},p50={p50},p90={p90},p99={p99},p999={p999},max={max} \
         {timestamp_ns}",
    );
}

fn write_measurement(buf: &mut String, prefix: Option<&str>, name: &str) {
    if let Some(p) = prefix {
        escape_measurement(buf, p);
        buf.push('.');
    }
    escape_measurement(buf, name);
}

fn write_tags(
    buf: &mut String,
    default_tags: &BTreeMap<String, String>,
    key_tags: &BTreeMap<String, String>,
) {
    let mut defaults = default_tags.iter().peekable();
    let mut keys = key_tags.iter().peekable();

    loop {
        match (defaults.peek(), keys.peek()) {
            (Some((dk, _)), Some((kk, _))) => match dk.as_str().cmp(kk.as_str()) {
                std::cmp::Ordering::Less => {
                    let (k, v) = defaults.next().unwrap();
                    write_single_tag(buf, k, v);
                }
                std::cmp::Ordering::Equal => {
                    // key_tags override defaults
                    defaults.next();
                    let (k, v) = keys.next().unwrap();
                    write_single_tag(buf, k, v);
                }
                std::cmp::Ordering::Greater => {
                    let (k, v) = keys.next().unwrap();
                    write_single_tag(buf, k, v);
                }
            },
            (Some(_), None) => {
                let (k, v) = defaults.next().unwrap();
                write_single_tag(buf, k, v);
            }
            (None, Some(_)) => {
                let (k, v) = keys.next().unwrap();
                write_single_tag(buf, k, v);
            }
            (None, None) => break,
        }
    }
}

fn write_single_tag(buf: &mut String, key: &str, value: &str) {
    buf.push(',');
    escape_tag_key_value(buf, key);
    buf.push('=');
    escape_tag_key_value(buf, value);
}

fn escape_measurement(buf: &mut String, s: &str) {
    for c in s.chars() {
        match c {
            ',' | ' ' | '\\' => {
                buf.push('\\');
                buf.push(c);
            }
            _ => buf.push(c),
        }
    }
}

fn escape_tag_key_value(buf: &mut String, s: &str) {
    for c in s.chars() {
        match c {
            ',' | '=' | ' ' | '\\' => {
                buf.push('\\');
                buf.push(c);
            }
            _ => buf.push(c),
        }
    }
}

#[cfg(test)]
mod tests {
    use expect_test::expect;

    use super::*;

    fn empty_tags() -> BTreeMap<String, String> {
        BTreeMap::new()
    }

    fn sample_tags() -> BTreeMap<String, String> {
        BTreeMap::from_iter([
            ("host".to_string(), "server01".to_string()),
            ("region".to_string(), "us-east".to_string()),
        ])
    }

    #[test]
    fn counter_basic() {
        let mut buf = String::new();
        write_counter(
            &mut buf,
            Some("myapp"),
            "requests",
            &sample_tags(),
            &empty_tags(),
            42,
            1_700_000_000_000_000_000,
        );
        expect!["myapp.requests,host=server01,region=us-east value=42u 1700000000000000000\n"]
            .assert_eq(&buf);
    }

    #[test]
    fn counter_no_prefix() {
        let mut buf = String::new();
        write_counter(&mut buf, None, "requests", &empty_tags(), &empty_tags(), 100, 1_000);
        expect!["requests value=100u 1000\n"].assert_eq(&buf);
    }

    #[test]
    fn gauge_basic() {
        let mut buf = String::new();
        write_gauge(
            &mut buf,
            Some("myapp"),
            "temperature",
            &sample_tags(),
            &empty_tags(),
            23.5,
            1_700_000_000_000_000_000,
        );
        expect!["myapp.temperature,host=server01,region=us-east value=23.5 1700000000000000000\n"]
            .assert_eq(&buf);
    }

    #[test]
    fn gauge_nan_skipped() {
        let mut buf = String::new();
        write_gauge(
            &mut buf,
            Some("myapp"),
            "temperature",
            &empty_tags(),
            &empty_tags(),
            f64::NAN,
            1_000,
        );
        expect![""].assert_eq(&buf);
    }

    #[test]
    fn gauge_inf_skipped() {
        let mut buf = String::new();
        write_gauge(
            &mut buf,
            Some("myapp"),
            "temperature",
            &empty_tags(),
            &empty_tags(),
            f64::INFINITY,
            1_000,
        );
        expect![""].assert_eq(&buf);
    }

    #[test]
    fn histogram_basic() {
        let mut buf = String::new();
        write_histogram(
            &mut buf,
            Some("myapp"),
            "latency",
            &sample_tags(),
            &empty_tags(),
            &HistogramSnapshot {
                count: 100,
                sum: 500.5,
                min: 0.1,
                p50: 2.0,
                p90: 10.5,
                p99: 50.0,
                p999: 99.9,
                max: 200.0,
            },
            1_700_000_000_000_000_000,
        );
        expect![
            "myapp.latency,host=server01,region=us-east \
             count=100u,sum=500.5,min=0.1,p50=2,p90=10.5,p99=50,p999=99.9,max=200 \
             1700000000000000000\n"
        ]
        .assert_eq(&buf);
    }

    #[test]
    fn histogram_nan_skipped() {
        let mut buf = String::new();
        write_histogram(
            &mut buf,
            Some("myapp"),
            "latency",
            &empty_tags(),
            &empty_tags(),
            &HistogramSnapshot {
                count: 100,
                sum: f64::NAN,
                min: 0.1,
                p50: 2.0,
                p90: 10.5,
                p99: 50.0,
                p999: 99.9,
                max: 200.0,
            },
            1_000,
        );
        expect![""].assert_eq(&buf);
    }

    #[test]
    fn escape_measurement_special_chars() {
        let mut buf = String::new();
        write_counter(
            &mut buf,
            Some("my app"),
            "req,count",
            &empty_tags(),
            &empty_tags(),
            1,
            1_000,
        );
        expect!["my\\ app.req\\,count value=1u 1000\n"].assert_eq(&buf);
    }

    #[test]
    fn escape_tag_special_chars() {
        let mut buf = String::new();
        let tags = BTreeMap::from_iter([("host name".to_string(), "server=01,a".to_string())]);
        write_counter(&mut buf, None, "m", &tags, &empty_tags(), 1, 1_000);
        expect!["m,host\\ name=server\\=01\\,a value=1u 1000\n"].assert_eq(&buf);
    }

    #[test]
    fn tag_merging_key_overrides_default() {
        let mut buf = String::new();
        let defaults = BTreeMap::from_iter([("host".to_string(), "default-host".to_string())]);
        let key_tags = BTreeMap::from_iter([("host".to_string(), "override-host".to_string())]);
        write_counter(&mut buf, None, "m", &key_tags, &defaults, 1, 1_000);
        expect!["m,host=override-host value=1u 1000\n"].assert_eq(&buf);
    }

    #[test]
    fn tag_merging_interleaved() {
        let mut buf = String::new();
        let defaults = BTreeMap::from_iter([
            ("a".to_string(), "1".to_string()),
            ("c".to_string(), "3".to_string()),
        ]);
        let key_tags = BTreeMap::from_iter([
            ("b".to_string(), "2".to_string()),
            ("d".to_string(), "4".to_string()),
        ]);
        write_counter(&mut buf, None, "m", &key_tags, &defaults, 1, 1_000);
        expect!["m,a=1,b=2,c=3,d=4 value=1u 1000\n"].assert_eq(&buf);
    }

    #[test]
    fn no_tags() {
        let mut buf = String::new();
        write_counter(&mut buf, None, "m", &empty_tags(), &empty_tags(), 1, 1_000);
        expect!["m value=1u 1000\n"].assert_eq(&buf);
    }

    #[test]
    fn backslash_in_measurement() {
        let mut buf = String::new();
        write_counter(&mut buf, None, "path\\metric", &empty_tags(), &empty_tags(), 1, 1_000);
        expect!["path\\\\metric value=1u 1000\n"].assert_eq(&buf);
    }

    #[test]
    fn backslash_in_tag() {
        let mut buf = String::new();
        let tags = BTreeMap::from_iter([("k\\ey".to_string(), "v\\al".to_string())]);
        write_counter(&mut buf, None, "m", &tags, &empty_tags(), 1, 1_000);
        expect!["m,k\\\\ey=v\\\\al value=1u 1000\n"].assert_eq(&buf);
    }

    #[test]
    fn multiple_writes_accumulate() {
        let mut buf = String::new();
        write_counter(&mut buf, None, "a", &empty_tags(), &empty_tags(), 1, 100);
        write_gauge(&mut buf, None, "b", &empty_tags(), &empty_tags(), 2.5, 200);
        expect![[r#"
            a value=1u 100
            b value=2.5 200
        "#]]
        .assert_eq(&buf);
    }
}
