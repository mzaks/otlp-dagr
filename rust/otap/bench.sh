#!/usr/bin/env bash
# Dagr -> OTAP vs otel-arrow's OTLP -> OTAP on the gate's fixtures (run ./check.sh first).
# Two binaries, one timing loop (src/timing.rs): arrow 53 and otel-arrow's datafusion
# cannot share a build. Sizes: "cold" is the first batch on a fresh stream (every schema
# and dictionary value sent); "repeat" is the same batch again on a warm stream — for a
# producer that reuses streams only new dictionary values are sent, and since the batch is
# IDENTICAL there are none, so "repeat" is a best case, not a typical next batch.
set -euo pipefail
cd "$(dirname "$0")"
WORK="${WORK:-$PWD/work}"
cargo build --release --quiet --bin bench_dagr
( cd rust-check && cargo build --release --quiet --bin bench )
target/release/bench_dagr "$WORK" | sort > "$WORK/bench.dagr"
rust-check/target/release/bench "$WORK" | sort > "$WORK/bench.oa"
echo "== single batches"
join <(grep -v -- -stream "$WORK/bench.dagr") <(grep -v -- -stream "$WORK/bench.oa") |
  awk 'BEGIN { printf "%-14s %8s %8s %7s | %-15s | %-15s | %-15s | %-15s\n", "batch", "dagr µs", "o-a µs", "speedup",
                     "cold  dagr/o-a", "repeat dagr/o-a", "zstd cold", "zstd repeat" }
       { printf "%-14s %8.1f %8.1f %6.2fx | %7d %7d | %7d %7d | %7d %7d | %7d %7d\n",
                $1, $2, $7, $7/$2, $3, $8, $4, $9, $5, $10, $6, $11 }'
echo
echo "== a stream: 20 consecutive batches (shared vocabulary, new ids/values), one producer each"
join <(grep -- -stream "$WORK/bench.dagr") <(grep -- -stream "$WORK/bench.oa") |
  awk 'BEGIN { printf "%-16s %12s %12s %8s | %-19s | %-19s\n", "stream", "dagr µs/b", "o-a µs/b", "speedup", "total  dagr/o-a", "zstd total" }
       { printf "%-16s %12.1f %12.1f %7.2fx | %9d %9d | %9d %9d\n", $1, $2, $5, $5/$2, $3, $6, $4, $7 }'
