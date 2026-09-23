//! Where an OTAP batch's bytes go: per payload, the IPC record size and its schema
//! message, and the largest columns (Arrow buffer bytes). Ours (the .bar the gate wrote)
//! next to otel-arrow's producer on the same fixture.
//!
//!   sizes <work dir> <signal>-<name>
use std::io::Cursor;

use arrow::array::Array;
use arrow::ipc::reader::StreamReader;
use otel_arrow_dfe_pdata::encode::encode_spans_otap_batch;
use otel_arrow_dfe_pdata::encode::producer::{Producer, ProducerOptions};
use otel_arrow_dfe_pdata::proto::opentelemetry::arrow::v1::BatchArrowRecords;
use otel_arrow_dfe_pdata::views::otlp::bytes::traces::RawTraceData;
use otel_arrow_dfe_pdata::{arrow, encode::encode_logs_otap_batch, encode::encode_metrics_otap_batch};
use otel_arrow_dfe_pdata::views::otlp::bytes::logs::RawLogsData;
use otel_arrow_dfe_pdata::views::otlp::bytes::metrics::RawMetricsData;
use prost::Message;

fn report(label: &str, bar: &BatchArrowRecords) {
    println!("== {label}: {} B", bar.encode_to_vec().len());
    for p in &bar.arrow_payloads {
        // the first IPC message is the schema: 0xFFFFFFFF, i32 len, flatbuffer
        let r = &p.record;
        let schema_len = if r.len() > 8 { 8 + i32::from_le_bytes(r[4..8].try_into().unwrap()) as usize } else { 0 };
        let mut rd = StreamReader::try_new(Cursor::new(r.clone()), None).unwrap();
        let batch = rd.next().unwrap().unwrap();
        let mut cols: Vec<(String, usize)> = batch
            .schema()
            .fields()
            .iter()
            .zip(batch.columns())
            .map(|(f, c)| (format!("{}:{}", f.name(), short(f.data_type())), c.get_buffer_memory_size()))
            .collect();
        cols.sort_by(|a, b| b.1.cmp(&a.1));
        let top: Vec<String> = cols.iter().take(6).map(|(n, s)| format!("{n}={s}")).collect();
        println!(
            "  {:>3} {:<28} rows {:>6}  record {:>7} B (schema {:>5})  {}",
            p.r#type,
            format!("{:?}", otel_arrow_dfe_pdata::proto::opentelemetry::arrow::v1::ArrowPayloadType::try_from(p.r#type).unwrap()),
            batch.num_rows(),
            r.len(),
            schema_len,
            top.join(" ")
        );
    }
}

fn short(dt: &arrow::datatypes::DataType) -> String {
    let s = format!("{dt:?}");
    s.chars().take(18).collect()
}

fn main() {
    let a: Vec<String> = std::env::args().collect();
    let dir = std::path::PathBuf::from(&a[1]);
    let stem = &a[2];
    let ours = BatchArrowRecords::decode(&std::fs::read(dir.join(format!("{stem}.bar"))).unwrap()[..]).unwrap();
    report("dagr", &ours);
    let pb = std::fs::read(dir.join(format!("{stem}.pb"))).unwrap();
    let mut recs = if stem.starts_with("traces") {
        encode_spans_otap_batch(&RawTraceData::new(&pb))
    } else if stem.starts_with("logs") {
        encode_logs_otap_batch(&RawLogsData::new(&pb))
    } else {
        encode_metrics_otap_batch(&RawMetricsData::new(&pb))
    }
    .unwrap();
    // `sizes <dir> <stem> zstd`: otel-arrow with its default zstd (compare with a .bar
    // written under DAGR2OTAP_ZSTD=1)
    let zstd = a.get(3).is_some_and(|z| z == "zstd");
    let opts = if zstd { ProducerOptions::default() } else { ProducerOptions { ipc_compression: None } };
    let mut p = Producer::new_with_options(opts);
    report("otel-arrow", &p.produce_bar(&mut recs).unwrap());
}
