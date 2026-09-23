//! `dagr2otap <signal> < batch.dagr > batch.bar` — one Dagr signal buffer in, one serialized
//! OTAP `BatchArrowRecords` out. The gate (`otap/check.sh`) feeds its output to otel-arrow's
//! own consumer.

use std::io::{Read, Write};

fn main() {
    let signal = std::env::args().nth(1).unwrap_or_default();
    let mut input = Vec::new();
    std::io::stdin().read_to_end(&mut input).expect("read stdin");
    // `DAGR2OTAP_METRIC_METADATA=1`: also emit METRIC_ATTRS (Rust receivers only)
    let meta = std::env::var("DAGR2OTAP_METRIC_METADATA").is_ok_and(|v| v == "1");
    let mut p = otlp_dagr_otap::Producer::default().with_metric_metadata(meta);
    let out = match signal.as_str() {
        "traces" => p.traces(&input),
        "logs" => p.logs(&input),
        "metrics" => p.metrics(&input),
        other => {
            eprintln!("dagr2otap: unknown signal {other:?} (traces | logs | metrics)");
            std::process::exit(2);
        }
    };
    match out {
        Ok(bar) => std::io::stdout().write_all(&bar).expect("write stdout"),
        Err(e) => {
            eprintln!("dagr2otap: {e}");
            std::process::exit(1);
        }
    }
}
