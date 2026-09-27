// prost types for the vendored OTLP protos (../../proto/otlp — the same files schema.py
// was imported from). Needs `protoc` on PATH.
const PROTOS: &[&str] = &[
    "opentelemetry/proto/common/v1/common.proto",
    "opentelemetry/proto/resource/v1/resource.proto",
    "opentelemetry/proto/trace/v1/trace.proto",
    "opentelemetry/proto/logs/v1/logs.proto",
    "opentelemetry/proto/metrics/v1/metrics.proto",
    "opentelemetry/proto/profiles/v1development/profiles.proto",
];

fn main() {
    let root = "../../proto/otlp";
    let files: Vec<String> = PROTOS.iter().map(|p| format!("{root}/{p}")).collect();
    for f in &files {
        println!("cargo:rerun-if-changed={f}");
    }
    prost_build::compile_protos(&files, &[root]).expect("prost-build over the vendored OTLP protos");
}
