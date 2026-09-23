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
use timing::{BATCHES, time};

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
        println!("{sig}-{name} {us:.1} {n}");
    }
}
