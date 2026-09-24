//! The second OTAP oracle: otel-arrow's Rust decoder (`otel-arrow-dfe-pdata`, the df-engine)
//! on every `<signal>-*.bar` in the work dir. It validates each payload against its schema
//! strictly — an unknown column, a null in a required column or a dictionary key width it
//! does not allow rejects the whole batch — which the Go consumer (the acceptance gate)
//! does not. The decoded request must equal the fixture's OTLP exactly (order included).
//!
//!   otap_rust_check <work dir> <signal>...

use otel_arrow_dfe_pdata::Consumer;
use otel_arrow_dfe_pdata::proto::opentelemetry::arrow::v1::BatchArrowRecords;
use otel_arrow_dfe_pdata::proto::opentelemetry::collector::logs::v1::ExportLogsServiceRequest;
use otel_arrow_dfe_pdata::proto::opentelemetry::collector::metrics::v1::ExportMetricsServiceRequest;
use otel_arrow_dfe_pdata::proto::opentelemetry::collector::trace::v1::ExportTraceServiceRequest;
use prost::Message;

/// What OTAP itself cannot tell apart, and one upstream decoder bug:
///
/// * a trace / span id that is empty in OTLP is all zeroes in OTAP — the id columns are
///   required FixedSizeBinary, so zeroes are the only way to write "unset" (pdata treats
///   the two as the same id);
/// * UPSTREAM BUG, otel-arrow-dfe-pdata 0.57.0 `otlp/traces/span_link.rs:102`: a link's
///   `dropped_attributes_count` is re-encoded under `SPAN_DROPPED_ATTRIBUTES_COUNT` (10, the
///   Span field) instead of `SPAN_LINK_DROPPED_ATTRIBUTES_COUNT` (5), so prost drops it and
///   every link's count reads back 0. The Go consumer keeps it; the acceptance gate checks it
///   there. Zeroed on the expected side only.
fn normalize_traces(r: &mut ExportTraceServiceRequest, expected: bool) {
    let unset = |b: &mut Vec<u8>| {
        if b.iter().all(|x| *x == 0) {
            b.clear();
        }
    };
    for rs in &mut r.resource_spans {
        for ss in &mut rs.scope_spans {
            for sp in &mut ss.spans {
                unset(&mut sp.trace_id);
                unset(&mut sp.span_id);
                unset(&mut sp.parent_span_id);
                for ln in &mut sp.links {
                    unset(&mut ln.trace_id);
                    unset(&mut ln.span_id);
                    if expected {
                        ln.dropped_attributes_count = 0;
                    }
                }
            }
        }
    }
}

/// An all-zero trace / span id on a log record is "unset", as in pdata; so is a body that
/// is present but holds no value (OTAP writes it as `body.type` 0, which the Rust decoder
/// reads back as no body — pdata cannot tell the two apart either).
fn normalize_logs(r: &mut ExportLogsServiceRequest) {
    for rl in &mut r.resource_logs {
        for sl in &mut rl.scope_logs {
            for lr in &mut sl.log_records {
                if lr.body.as_ref().is_some_and(|b| b.value.is_none()) {
                    lr.body = None;
                }
                for b in [&mut lr.trace_id, &mut lr.span_id] {
                    if b.iter().all(|x| *x == 0) {
                        b.clear();
                    }
                }
            }
        }
    }
}

/// UPSTREAM DIVERGENCE (otel-arrow 0.57.0): the two decoders read the `parent_id` of an
/// exemplar with NO value differently — Go (`metrics/otlp/exemplar.go`) always as a delta,
/// Rust (`materialize_parent_ids_by_columns`, nulls compare not-equal) as absolute — so no
/// encoding places it right in both. The producer encodes for Go, the collector's receiver;
/// the Rust decoder then attaches value-less exemplars to the wrong data point. They are
/// removed from both sides here; every exemplar WITH a value is still checked, placement
/// included.
fn drop_valueless_exemplars(r: &mut ExportMetricsServiceRequest) {
    use otel_arrow_dfe_pdata::proto::opentelemetry::metrics::v1::metric::Data;
    for rm in &mut r.resource_metrics {
        for sm in &mut rm.scope_metrics {
            for m in &mut sm.metrics {
                match &mut m.data {
                    Some(Data::Gauge(g)) => g.data_points.iter_mut().for_each(|p| p.exemplars.retain(|e| e.value.is_some())),
                    Some(Data::Sum(s)) => s.data_points.iter_mut().for_each(|p| p.exemplars.retain(|e| e.value.is_some())),
                    Some(Data::Histogram(h)) => h.data_points.iter_mut().for_each(|p| p.exemplars.retain(|e| e.value.is_some())),
                    Some(Data::ExponentialHistogram(h)) => {
                        h.data_points.iter_mut().for_each(|p| p.exemplars.retain(|e| e.value.is_some()))
                    }
                    _ => {}
                }
            }
        }
    }
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let dir = std::path::PathBuf::from(&args[1]);
    let mut failed = 0;
    for signal in &args[2..] {
        let mut names: Vec<_> = std::fs::read_dir(&dir)
            .unwrap()
            .filter_map(|e| e.ok()?.file_name().into_string().ok())
            .filter(|n| {
                n.starts_with(&format!("{signal}-")) && (n.ends_with(".bar") || n.ends_with(".sbar") || n.ends_with(".spbar"))
            })
            .collect();
        names.sort();
        // `.bar`: one batch, a fresh consumer. `.sbar`: one STREAM, written by one producer —
        // decoded in name order by ONE consumer (later batches reuse schemas and dictionaries).
        let mut stream = Consumer::default();
        let mut spool = Consumer::default(); // the `.spbar` batches: one spool, one stream
        for name in names {
            let is_stream = name.ends_with(".sbar");
            let is_spool = name.ends_with(".spbar");
            // a spool's batches must equal the stream fixtures the spool was written from
            let stem_owned = name.trim_end_matches(".spbar").trim_end_matches(".sbar").trim_end_matches(".bar").replacen("-spool-", "-stream-", 1);
            let stem = stem_owned.as_str();
            let bar = std::fs::read(dir.join(&name)).unwrap();
            let pb = std::fs::read(dir.join(format!("{stem}.pb"))).unwrap();
            let mut bar = BatchArrowRecords::decode(&bar[..]).expect("BatchArrowRecords");
            let mut fresh = Consumer::default();
            let c = if is_spool { &mut spool } else if is_stream { &mut stream } else { &mut fresh };
            let res = match signal.as_str() {
                "traces" => c.consume_traces_batches(&mut bar).map(|mut got| {
                    let mut want = ExportTraceServiceRequest::decode(&pb[..]).expect("fixture pb");
                    normalize_traces(&mut want, true);
                    normalize_traces(&mut got, false);
                    if got == want { Ok(()) } else { Err(format!("{:#?}\n!=\n{:#?}", got, want)) }
                }),
                "logs" => c.consume_logs_batches(&mut bar).map(|mut got| {
                    let mut want = ExportLogsServiceRequest::decode(&pb[..]).expect("fixture pb");
                    normalize_logs(&mut want);
                    normalize_logs(&mut got);
                    if got == want { Ok(()) } else { Err(format!("{:#?}\n!=\n{:#?}", got, want)) }
                }),
                "metrics" | "metricsmeta" => c.consume_metrics_batches(&mut bar).map(|mut got| {
                    let mut want = ExportMetricsServiceRequest::decode(&pb[..]).expect("fixture pb");
                    if signal == "metrics" {
                        // the default output drops metric metadata (the Go consumer rejects
                        // METRIC_ATTRS); "metricsmeta" is the opt-in output that keeps it
                        for rm in &mut want.resource_metrics {
                            for sm in &mut rm.scope_metrics {
                                for m in &mut sm.metrics {
                                    m.metadata.clear();
                                }
                            }
                        }
                    }
                    drop_valueless_exemplars(&mut want);
                    drop_valueless_exemplars(&mut got);
                    if got == want { Ok(()) } else { Err(format!("{:#?}\n!=\n{:#?}", got, want)) }
                }),
                other => panic!("unknown signal {other}"),
            };
            match res {
                Ok(Ok(())) => {
                    if !stem.contains("-stream-") || stem.ends_with("-00") {
                        println!("  rust decoder: {name} ok{}", if is_stream || is_spool { " (… and the rest of the stream)" } else { "" })
                    }
                }
                Ok(Err(diff)) => {
                    failed += 1;
                    let path = dir.join(format!("{name}.rust-diff.txt"));
                    std::fs::write(&path, diff).unwrap();
                    println!("  rust decoder: {name} DIFFERS (see {})", path.display());
                }
                Err(e) => {
                    failed += 1;
                    println!("  rust decoder: {name} REJECTED: {e}");
                }
            }
        }
    }
    if failed > 0 {
        std::process::exit(1);
    }
}
