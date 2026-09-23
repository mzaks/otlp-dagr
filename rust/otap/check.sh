#!/usr/bin/env bash
# The OTAP bridge gate: fixtures (Go) -> Dagr -> dagr2otap (Rust) -> otel-arrow's Go
# consumer (and otel-arrow's Rust decoder) -> must equal the OTLP the fixtures were built from.
#
#   rust/otap/check.sh           # every signal the bridge supports
set -euo pipefail
cd "$(dirname "$0")"
ROOT="$(cd ../.. && pwd)"
WORK="${WORK:-$PWD/work}"
SIGNALS="${SIGNALS:-traces logs metrics}"

rm -rf "$WORK"; mkdir -p "$WORK"
( cd "$ROOT/go" && go run ./cmd/otlpdagrctl fixtures -out "$WORK" )
cargo build --release --quiet --bin dagr2otap
BIN="$PWD/target/release/dagr2otap"
for sig in $SIGNALS; do
  for f in "$WORK/$sig"-*.dagr; do
    "$BIN" "$sig" < "$f" > "${f%.dagr}.bar"
    echo "  $(basename "${f%.dagr}"): $(wc -c < "$f" | tr -d ' ') B dagr -> $(wc -c < "${f%.dagr}.bar" | tr -d ' ') B OTAP"
  done
done
# 1. the acceptance gate: otel-arrow's Go consumer, the collector receiver's own code
# metric metadata (METRIC_ATTRS) is opt-in — the Go consumer rejects it — so the
# opt-in output goes to the Rust decoder only, as the "metricsmeta" signal
if [[ " $SIGNALS " == *" metrics "* ]]; then
  for f in "$WORK"/metrics-*.dagr; do
    b="$(basename "${f%.dagr}")"; m="$WORK/metricsmeta-${b#metrics-}"
    DAGR2OTAP_METRIC_METADATA=1 "$BIN" metrics < "$f" > "$m.bar"; cp "${f%.dagr}.pb" "$m.pb"
  done
fi
( cd check && OTAP_WORK="$WORK" go test -count=1 ./... )
# 2. the strict schema check: otel-arrow's Rust decoder (arrow 58 + datafusion; the first
#    build takes minutes). RUST_ORACLE=0 skips it.
if [ "${RUST_ORACLE:-1}" != "0" ]; then
  RS="$SIGNALS"; [[ " $SIGNALS " == *" metrics "* ]] && RS="$RS metricsmeta"
  ( cd rust-check && cargo run --release --quiet --bin otap_rust_check -- "$WORK" $RS )
fi
echo "otap: green (${SIGNALS})"
