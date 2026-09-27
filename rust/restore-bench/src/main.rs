//! Eager restore vs protobuf decode, on the shipped schema (spec/38 §0.1).
//!
//! Inputs are `otlpdagrctl fixtures` output (from go/: `go run ./cmd/otlpdagrctl fixtures
//! -out ../rust/otap/work`): `<signal>-batch.dagr` and `<signal>-batch.pb` hold the SAME
//! batch — the Go benchmark's — as Dagr bytes and as protobuf. Per signal, four timings:
//!
//!   pb decode        prost `decode` into its structs
//!   pb decode+walk   ... and a walk that touches one field per leaf item
//!   dagr restore     the generated arena's `from_bytes` (a full eager restore)
//!   dagr rest.+walk  ... and the same walk through the arena's accessors
//!
//! The walks must agree (checksum), so both sides decoded the same data. The walk is
//! reported separately because the arena's accessors are not free — they return owned
//! `Vec`s and `String`s — while prost's structs hand out references to what `decode`
//! already built.
//!
//!   cargo run --release -- [dir]

use std::time::Instant;

use prost::Message;

use otlp_dagr::{otlp_logs, otlp_metrics, otlp_profiles, otlp_traces, otlp_traces_lazy};

pub mod opentelemetry {
    pub mod proto {
        pub mod common {
            pub mod v1 {
                include!(concat!(env!("OUT_DIR"), "/opentelemetry.proto.common.v1.rs"));
            }
        }
        pub mod resource {
            pub mod v1 {
                include!(concat!(env!("OUT_DIR"), "/opentelemetry.proto.resource.v1.rs"));
            }
        }
        pub mod trace {
            pub mod v1 {
                include!(concat!(env!("OUT_DIR"), "/opentelemetry.proto.trace.v1.rs"));
            }
        }
        pub mod logs {
            pub mod v1 {
                include!(concat!(env!("OUT_DIR"), "/opentelemetry.proto.logs.v1.rs"));
            }
        }
        pub mod metrics {
            pub mod v1 {
                include!(concat!(env!("OUT_DIR"), "/opentelemetry.proto.metrics.v1.rs"));
            }
        }
        pub mod profiles {
            pub mod v1development {
                include!(concat!(env!("OUT_DIR"), "/opentelemetry.proto.profiles.v1development.rs"));
            }
        }
    }
}
use opentelemetry::proto::{logs::v1 as lpb, metrics::v1 as mpb, profiles::v1development as ppb, trace::v1 as tpb};

/// Best-of-7 mean µs per call over enough calls for ~50 ms, after a warm-up; and the
/// last output.
fn time<F: FnMut() -> u64>(mut f: F) -> (f64, u64) {
    let mut out = 0;
    let t = Instant::now();
    let mut warm = 0;
    while warm < 20 || t.elapsed().as_millis() < 100 {
        out = std::hint::black_box(f());
        warm += 1;
    }
    let n = (warm / 2).max(20);
    let mut best = f64::MAX;
    for _ in 0..7 {
        let t = Instant::now();
        for _ in 0..n {
            out = std::hint::black_box(f());
        }
        best = best.min(t.elapsed().as_secs_f64() * 1e6 / n as f64);
    }
    (best, out)
}

fn mix(acc: u64, v: u64) -> u64 {
    acc.wrapping_mul(31).wrapping_add(v)
}

// The walks: one field per leaf item, in the same order on both sides.

fn walk_traces_pb(d: &tpb::TracesData) -> u64 {
    let mut acc = 0;
    for rs in &d.resource_spans {
        for ss in &rs.scope_spans {
            for sp in &ss.spans {
                acc = mix(mix(acc, sp.start_time_unix_nano), sp.name.len() as u64);
            }
        }
    }
    acc
}

fn walk_traces_dagr(a: &otlp_traces::OtlpTracesArena<0>) -> u64 {
    let mut acc = 0;
    for rs in a.get_root().unwrap().resource_spans() {
        for ss in rs.scope_spans() {
            for sp in ss.spans() {
                acc = mix(mix(acc, sp.start_time_unix_nano().unwrap_or(0)), sp.name().map_or(0, |s| s.len()) as u64);
            }
        }
    }
    acc
}

// The lazy read of the same walk: no restore, accessors over the bytes (the 29x of
// spec/38 §0 — reported for reference, traces only).
fn walk_traces_lazy(b: &[u8]) -> u64 {
    let mut acc = 0;
    for rs in otlp_traces_lazy::read_root(b).unwrap().resource_spans().unwrap().iter() {
        for ss in rs.unwrap().scope_spans().unwrap().iter() {
            for sp in ss.unwrap().spans().unwrap().iter() {
                let sp = sp.unwrap();
                acc = mix(mix(acc, sp.start_time_unix_nano().unwrap_or(0)), sp.name().map_or(0, |s| s.len()) as u64);
            }
        }
    }
    acc
}

fn walk_logs_pb(d: &lpb::LogsData) -> u64 {
    let mut acc = 0;
    for rl in &d.resource_logs {
        for sl in &rl.scope_logs {
            for r in &sl.log_records {
                acc = mix(acc, r.time_unix_nano);
            }
        }
    }
    acc
}

fn walk_logs_dagr(a: &otlp_logs::OtlpLogsArena<0>) -> u64 {
    let mut acc = 0;
    for rl in a.get_root().unwrap().resource_logs() {
        for sl in rl.scope_logs() {
            for r in sl.log_records() {
                acc = mix(acc, r.time_unix_nano().unwrap_or(0));
            }
        }
    }
    acc
}

fn walk_metrics_pb(d: &mpb::MetricsData) -> u64 {
    let mut acc = 0;
    for rm in &d.resource_metrics {
        for sm in &rm.scope_metrics {
            for m in &sm.metrics {
                acc = mix(acc, m.name.len() as u64);
            }
        }
    }
    acc
}

fn walk_metrics_dagr(a: &otlp_metrics::OtlpMetricsArena<0>) -> u64 {
    let mut acc = 0;
    for rm in a.get_root().unwrap().resource_metrics() {
        for sm in rm.scope_metrics() {
            for m in sm.metrics() {
                acc = mix(acc, m.name().map_or(0, |s| s.len()) as u64);
            }
        }
    }
    acc
}

fn walk_profiles_pb(d: &ppb::ProfilesData) -> u64 {
    let mut acc = 0;
    for rp in &d.resource_profiles {
        for sp in &rp.scope_profiles {
            for p in &sp.profiles {
                acc = mix(mix(acc, p.time_unix_nano), p.samples.len() as u64);
            }
        }
    }
    acc
}

fn walk_profiles_dagr(a: &otlp_profiles::OtlpProfilesArena<0>) -> u64 {
    let mut acc = 0;
    for rp in a.get_root().unwrap().resource_profiles() {
        for sp in rp.scope_profiles() {
            for p in sp.profiles() {
                acc = mix(mix(acc, p.time_unix_nano().unwrap_or(0)), p.samples().len() as u64);
            }
        }
    }
    acc
}

macro_rules! bench {
    ($dir:expr, $sig:literal, $Pb:ty, $top:ident, $walk_pb:ident, $Arena:ty, $walk_dagr:ident) => {{
        let pb = std::fs::read(format!("{}/{}-batch.pb", $dir, $sig)).expect(concat!($sig, "-batch.pb"));
        let dg = std::fs::read(format!("{}/{}-batch.dagr", $dir, $sig)).expect(concat!($sig, "-batch.dagr"));
        let (pb_dec, _) = time(|| std::hint::black_box(<$Pb>::decode(&pb[..]).unwrap()).$top.len() as u64);
        let (pb_walk, ck_pb) = time(|| $walk_pb(&<$Pb>::decode(&pb[..]).unwrap()));
        let (dg_dec, _) = time(|| {
            // opaque: the optimizer must not drop restore work nothing reads
            let a = std::hint::black_box(<$Arena>::from_bytes(&dg).unwrap());
            a.get_root().is_some() as u64
        });
        let (dg_walk, ck_dg) = time(|| $walk_dagr(&<$Arena>::from_bytes(&dg).unwrap()));
        assert_eq!(ck_pb, ck_dg, concat!($sig, ": the two walks disagree"));
        println!(
            "{:<9} {:>7} {:>7} | {:>9.1} {:>9.1} | {:>9.1} {:>9.1} | {:>5.2}x {:>5.2}x",
            $sig, pb.len(), dg.len(), pb_dec, pb_walk, dg_dec, dg_walk, dg_dec / pb_dec, dg_walk / pb_walk
        );
    }};
}

fn main() {
    let dir = std::env::args().nth(1).unwrap_or_else(|| "../otap/work".into());
    println!(
        "{:<9} {:>7} {:>7} | {:>9} {:>9} | {:>9} {:>9} | {:>6} {:>6}",
        "signal", "pb B", "dagr B", "pb µs", "+walk", "restore", "+walk", "dec", "+walk"
    );
    bench!(dir, "traces", tpb::TracesData, resource_spans, walk_traces_pb, otlp_traces::OtlpTracesArena<0>, walk_traces_dagr);
    bench!(dir, "logs", lpb::LogsData, resource_logs, walk_logs_pb, otlp_logs::OtlpLogsArena<0>, walk_logs_dagr);
    bench!(dir, "metrics", mpb::MetricsData, resource_metrics, walk_metrics_pb, otlp_metrics::OtlpMetricsArena<0>, walk_metrics_dagr);
    bench!(dir, "profiles", ppb::ProfilesData, resource_profiles, walk_profiles_pb, otlp_profiles::OtlpProfilesArena<0>, walk_profiles_dagr);
    println!("(dec, +walk: Dagr time / protobuf time — above 1 is slower than protobuf)");
    let pb = std::fs::read(format!("{dir}/traces-batch.pb")).unwrap();
    let dg = std::fs::read(format!("{dir}/traces-batch.dagr")).unwrap();
    let (pb_walk, ck_pb) = time(|| walk_traces_pb(&tpb::TracesData::decode(&pb[..]).unwrap()));
    let (lz, ck_lz) = time(|| walk_traces_lazy(&dg));
    assert_eq!(ck_pb, ck_lz, "traces: the lazy walk disagrees");
    println!("traces lazy walk: {lz:.1} µs — {:.1}x faster than pb decode+walk", pb_walk / lz);
}
