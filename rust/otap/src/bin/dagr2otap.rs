//! `dagr2otap <signal> < batch.dagr > batch.bar` — one Dagr signal buffer in, one serialized
//! OTAP `BatchArrowRecords` out.
//!
//! `dagr2otap <signal> a.dagr b.dagr …` — a STREAM: the files, in order, through one
//! producer (schemas and dictionaries carried across), each written to `<file>.sbar`; a
//! receiver must read them in the same order on one connection.
//!
//! `dagr2otap spool <file>` — a SPOOL (otlp-dagr's exporter file, any signal — the header
//! says which): every record, in order, through one producer, each written to
//! `<file stem>-spool-NN.spbar`.
//!
//! The gate (`otap/check.sh`) feeds all of them to otel-arrow's own consumers.

use std::io::{Read, Write};

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let signal = args.get(1).cloned().unwrap_or_default();
    // `DAGR2OTAP_METRIC_METADATA=1`: also emit METRIC_ATTRS (Rust receivers only)
    let meta = std::env::var("DAGR2OTAP_METRIC_METADATA").is_ok_and(|v| v == "1");
    // `DAGR2OTAP_ZSTD=1`: zstd-compress the IPC bodies
    let zstd = std::env::var("DAGR2OTAP_ZSTD").is_ok_and(|v| v == "1");
    let mut p = otlp_dagr_otap::Producer::default().with_metric_metadata(meta).with_zstd(zstd);
    if signal == "spool" {
        let path = std::path::Path::new(args.get(2).expect("dagr2otap spool <file>"));
        let data = std::fs::read(path).expect("read spool");
        let stem = path.with_extension("");
        let mut i = 0;
        let summary = p
            .spool(&data, |bar| {
                std::fs::write(format!("{}-spool-{i:02}.spbar", stem.display()), bar).expect("write .spbar");
                i += 1;
            })
            .unwrap_or_else(|e| {
                eprintln!("dagr2otap: {e}");
                std::process::exit(1);
            });
        eprintln!("dagr2otap: {:?} spool, {} records", summary.signal, summary.records);
        return;
    }
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
