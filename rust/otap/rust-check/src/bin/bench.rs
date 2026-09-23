//! The otel-arrow half of `bench.sh`: `<signal>-<name> <µs> <bytes>` per batch — the
//! df-engine's path from OTLP protobuf bytes: its zero-copy protobuf views (`RawTraceData`,
//! no prost decode) → `encode_*_otap_batch` → `Producer::produce_bar` (transport-optimised:
//! sorted, dictionary- and delta-encoded) → prost-encoded. IPC compression OFF, as in the
//! Dagr producer.
#[path = "../timing.rs"]
mod timing;

use otel_arrow_dfe_pdata::encode::producer::{Producer, ProducerOptions};
use otel_arrow_dfe_pdata::encode::{encode_logs_otap_batch, encode_metrics_otap_batch, encode_spans_otap_batch};
use otel_arrow_dfe_pdata::views::otlp::bytes::logs::RawLogsData;
use otel_arrow_dfe_pdata::views::otlp::bytes::metrics::RawMetricsData;
use otel_arrow_dfe_pdata::views::otlp::bytes::traces::RawTraceData;
use prost::Message;
use timing::{BATCHES, STREAM, time};

fn main() {
    let dir = std::path::PathBuf::from(std::env::args().nth(1).expect("work dir"));
    for (sig, name) in BATCHES {
        let pb = std::fs::read(dir.join(format!("{sig}-{name}.pb"))).unwrap();
        let mut p = Producer::new_with_options(ProducerOptions { ipc_compression: None });
        let (us, n) = time(|| {
            let mut recs = match sig {
                "traces" => encode_spans_otap_batch(&RawTraceData::new(&pb)),
                "logs" => encode_logs_otap_batch(&RawLogsData::new(&pb)),
                _ => encode_metrics_otap_batch(&RawMetricsData::new(&pb)),
            }
            .unwrap();
            p.produce_bar(&mut recs).unwrap().encode_to_vec().len()
        });
        // cold: the first batch on a fresh stream (every schema and dictionary sent); then
        // with the producer's default zstd, cold and repeated
        let size = |zstd: bool, times: usize| {
            let opts = if zstd { ProducerOptions::default() } else { ProducerOptions { ipc_compression: None } };
            let mut fresh = Producer::new_with_options(opts);
            let mut n = 0;
            for _ in 0..times {
                let mut recs = match sig {
                    "traces" => encode_spans_otap_batch(&RawTraceData::new(&pb)),
                    "logs" => encode_logs_otap_batch(&RawLogsData::new(&pb)),
                    _ => encode_metrics_otap_batch(&RawMetricsData::new(&pb)),
                }
                .unwrap();
                n = fresh.produce_bar(&mut recs).unwrap().encode_to_vec().len();
            }
            n
        };
        println!("{sig}-{name} {us:.1} {} {n} {} {}", size(false, 1), size(true, 1), size(true, 2));
    }
    // the stream: 20 consecutive batches on ONE producer (its streams, schemas and
    // dictionaries carried from batch to batch)
    for sig in ["traces", "logs", "metrics"] {
        let batches: Vec<Vec<u8>> =
            (0..STREAM).map(|i| std::fs::read(dir.join(format!("{sig}-stream-{i:02}.pb"))).unwrap()).collect();
        let run = |zstd: bool| {
            let opts = if zstd { ProducerOptions::default() } else { ProducerOptions { ipc_compression: None } };
            let mut p = Producer::new_with_options(opts);
            let t = std::time::Instant::now();
            let mut total = 0;
            for pb in &batches {
                let mut recs = match sig {
                    "traces" => encode_spans_otap_batch(&RawTraceData::new(pb)),
                    "logs" => encode_logs_otap_batch(&RawLogsData::new(pb)),
                    _ => encode_metrics_otap_batch(&RawMetricsData::new(pb)),
                }
                .unwrap();
                total += p.produce_bar(&mut recs).unwrap().encode_to_vec().len();
            }
            (t.elapsed().as_secs_f64() * 1e6 / STREAM as f64, total)
        };
        let _ = run(false);
        let (us, plain) = (0..5).map(|_| run(false)).fold((f64::MAX, 0), |a, b| (a.0.min(b.0), b.1));
        let (_, z) = run(true);
        println!("{sig}-stream {us:.1} {plain} {z}");
    }
}
