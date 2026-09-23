//! `dagr2otap <signal> < batch.dagr > batch.bar` — one Dagr signal buffer in, one serialized
//! OTAP `BatchArrowRecords` out.
//!
//! `dagr2otap <signal> a.dagr b.dagr …` — a STREAM: the files, in order, through one
//! producer (schemas and dictionaries carried across), each written to `<file>.sbar`; a
//! receiver must read them in the same order on one connection.
//!
//! The gate (`otap/check.sh`) feeds both to otel-arrow's own consumers.

use std::io::{Read, Write};

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let signal = args.get(1).cloned().unwrap_or_default();
    // `DAGR2OTAP_METRIC_METADATA=1`: also emit METRIC_ATTRS (Rust receivers only)
    let meta = std::env::var("DAGR2OTAP_METRIC_METADATA").is_ok_and(|v| v == "1");
    // `DAGR2OTAP_ZSTD=1`: zstd-compress the IPC bodies
    let zstd = std::env::var("DAGR2OTAP_ZSTD").is_ok_and(|v| v == "1");
    let mut p = otlp_dagr_otap::Producer::default().with_metric_metadata(meta).with_zstd(zstd);
    let mut convert = |input: &[u8]| {
        let out = match signal.as_str() {
            "traces" => p.traces(input),
            "logs" => p.logs(input),
            "metrics" => p.metrics(input),
            other => {
                eprintln!("dagr2otap: unknown signal {other:?} (traces | logs | metrics)");
                std::process::exit(2);
            }
        };
        out.unwrap_or_else(|e| {
            eprintln!("dagr2otap: {e}");
            std::process::exit(1);
        })
    };
    if args.len() > 2 {
        for path in &args[2..] {
            let bar = convert(&std::fs::read(path).expect("read input"));
            let out = std::path::Path::new(path).with_extension("sbar");
            std::fs::write(out, bar).expect("write .sbar");
        }
        return;
    }
    let mut input = Vec::new();
    std::io::stdin().read_to_end(&mut input).expect("read stdin");
    std::io::stdout().write_all(&convert(&input)).expect("write stdout");
}
