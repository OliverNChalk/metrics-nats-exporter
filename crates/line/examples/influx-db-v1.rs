use std::collections::BTreeMap;
use std::time::Duration;

use metrics::{counter, gauge, histogram};
use metrics_line_exporter::{CancellationToken, Config};
use tracing::info;

fn main() {
    dotenvy::dotenv().ok();
    tracing_subscriber::fmt::init();

    let endpoint =
        std::env::var("INFLUXDB_ENDPOINT").unwrap_or("http://localhost:8086".to_string());
    let database = std::env::var("INFLUXDB_DATABASE").unwrap_or("test".to_string());
    let username = std::env::var("INFLUXDB_USERNAME").ok();
    let password = std::env::var("INFLUXDB_PASSWORD").ok();

    let cxl = CancellationToken::new();
    let handle = metrics_line_exporter::install(
        cxl.clone(),
        Config {
            interval_min: Duration::from_secs(1),
            interval_max: Duration::from_secs(10),
            flush_interval: Duration::from_secs(2),
            endpoint,
            database,
            username,
            password,
            metric_prefix: Some("smoke_test".to_string()),
            default_tags: BTreeMap::from_iter([("host".to_string(), "dev".to_string())]),
        },
    )
    .expect("failed to install recorder");

    info!("Exporter running. Emitting metrics for 15s...");

    for i in 0..15 {
        counter!("requests_total", "method" => "GET").increment(1);
        counter!("requests_total", "method" => "POST").increment(3);
        gauge!("temperature", "sensor" => "cpu").set(60.0 + f64::from(i) * 0.5);
        gauge!("temperature", "sensor" => "gpu").set(55.0 + f64::from(i) * 0.3);
        histogram!("request_duration_seconds", "endpoint" => "/api")
            .record(0.05 * f64::from(i) + 1.0);

        std::thread::sleep(Duration::from_secs(1));
    }

    info!("Done emitting. Waiting 3s for final flush...");
    std::thread::sleep(Duration::from_secs(3));

    cxl.cancel();
    handle.join().expect("exporter thread panicked");
}
