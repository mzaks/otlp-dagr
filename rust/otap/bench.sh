#!/usr/bin/env bash
# Dagr -> OTAP vs otel-arrow's OTLP -> OTAP on the gate's fixtures (run ./check.sh first).
# Two binaries, one timing loop (src/timing.rs): arrow 53 and otel-arrow's datafusion
# cannot share a build.
set -euo pipefail
cd "$(dirname "$0")"
WORK="${WORK:-$PWD/work}"
cargo build --release --quiet --bin bench_dagr
( cd rust-check && cargo build --release --quiet --bin bench )
join <(target/release/bench_dagr "$WORK" | sort) <(rust-check/target/release/bench "$WORK" | sort) |
  awk 'BEGIN { printf "%-16s %10s %14s %9s %10s %14s\n", "batch", "dagr µs", "otel-arrow µs", "speedup", "dagr B", "otel-arrow B" }
       { printf "%-16s %10.1f %14.1f %8.2fx %10d %14d\n", $1, $2, $4, $4/$2, $3, $5 }'
