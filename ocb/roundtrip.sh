#!/usr/bin/env bash
# Build a real collector with ocb and run every signal through it — traces, metrics, logs
# and profiles, each out to its own Dagr spool and back in — spec/38 §6.1.1 gate 2, plus
# gates 3 and 4 on the files the collector produced.
#
#   ./ocb/roundtrip.sh              # build if missing or stale, then round trip
#   ./ocb/roundtrip.sh build        # build the collector only
#   SPANS=5000 BATCHES=8 ./ocb/roundtrip.sh
#
# Needs the network on first run (ocb and the collector's module graph) and a generated
# go/gen — run ./regen.sh first.
set -euo pipefail
cd "$(dirname "$0")/.."
ROOT="$PWD"

OTELCOL_VERSION="${OTELCOL_VERSION:-0.161.0}"
CONTRIB_VERSION="${CONTRIB_VERSION:-0.161.0}"
SPANS="${SPANS:-500}"
BATCHES="${BATCHES:-4}"
WORK="${WORK:-$ROOT/ocb/work}"
BIN="$ROOT/ocb/build/otelcol-dagr"
COL="$ROOT/ocb/collector"

if [ ! -d "$ROOT/go/gen" ]; then
  echo "roundtrip: go/gen is missing — run ./regen.sh first" >&2
  exit 1
fi

render() {  # render <template> <out>
  ROOT="$ROOT" OTELCOL_VERSION="$OTELCOL_VERSION" CONTRIB_VERSION="$CONTRIB_VERSION" \
  SPOOL="$WORK/spans.dagr" DUMP="$WORK/out.json" WORK="$WORK" \
    python3 -c 'import os,sys,string;sys.stdout.write(string.Template(open(sys.argv[1]).read()).safe_substitute(os.environ))' \
    "$1" > "$2"
}

build() {
  mkdir -p "$WORK"
  render "$ROOT/ocb/builder-config.yaml" "$WORK/builder-config.yaml"
  if ! command -v ocb >/dev/null 2>&1; then
    echo "==> installing ocb v$OTELCOL_VERSION"
    GOBIN="$WORK/bin" go install "go.opentelemetry.io/collector/cmd/builder@v$OTELCOL_VERSION"
    export PATH="$WORK/bin:$PATH"
    ln -sf "$WORK/bin/builder" "$WORK/bin/ocb"
  fi
  echo "==> building otelcol-dagr"
  ocb --config "$WORK/builder-config.yaml" --skip-strict-versioning
}

run_phase() {  # run_phase <name> <config> <ready-log-fragment>
  local name="$1" cfg="$2" log="$WORK/$1.log"
  "$BIN" --config "$cfg" --feature-gates=service.profilesSupport >"$log" 2>&1 &
  local pid=$!
  for _ in $(seq 1 120); do
    grep -q "Everything is ready" "$log" 2>/dev/null && break
    kill -0 "$pid" 2>/dev/null || { echo "==> $name died:" >&2; cat "$log" >&2; exit 1; }
    sleep 0.25
  done
  echo "$pid"
}

[ "${1:-}" = "build" ] && { build; exit 0; }
# Rebuild when the binary is missing OR older than any Go source it is built from — a
# stale collector silently round-trips the OLD wire format.
if [ ! -x "$BIN" ] || [ -n "$(find "$ROOT/go" -name '*.go' -newer "$BIN" -print -quit)" ]; then
  build
fi

SIGNALS="metrics logs profiles"
rm -rf "$WORK/spans.dagr" "$WORK/out.json"
for s in $SIGNALS; do rm -f "$WORK/$s.dagr" "$WORK/out-$s.json"; done
mkdir -p "$WORK"
render "$ROOT/ocb/collector-export.yaml" "$WORK/collector-export.yaml"
render "$ROOT/ocb/collector-import.yaml" "$WORK/collector-import.yaml"

echo "==> phase A: OTLP -> dagrfile exporter -> $WORK/spans.dagr"
pid=$(run_phase export "$WORK/collector-export.yaml")
trap 'kill "$pid" 2>/dev/null || true' EXIT
(cd go && go run ./cmd/otlpdagrctl send -spans "$SPANS" -batches "$BATCHES")
for s in $SIGNALS; do
  (cd go && go run ./cmd/otlpdagrctl send -signal "$s" -batches "$BATCHES")
done
sleep 1
kill -INT "$pid"; wait "$pid" 2>/dev/null || true
trap - EXIT

echo "==> the spool a real collector wrote"
(cd go && go run ./cmd/otlpdagrctl spool -path "$WORK/spans.dagr" -batches "$BATCHES" -spans "$((SPANS * BATCHES))")
for s in $SIGNALS; do
  (cd go && go run ./cmd/otlpdagrctl spool -signal "$s" -path "$WORK/$s.dagr" -batches "$BATCHES")
done

echo "==> phase B: dagrfile receiver -> file exporter -> $WORK/out.json"
pid=$(run_phase import "$WORK/collector-import.yaml")
trap 'kill "$pid" 2>/dev/null || true' EXIT
for _ in $(seq 1 60); do
  ready=1
  for f in out.json out-metrics.json out-logs.json out-profiles.json; do [ -s "$WORK/$f" ] || ready=0; done
  [ $ready = 1 ] && break
  sleep 0.25
done
sleep 1
kill -INT "$pid"; wait "$pid" 2>/dev/null || true
trap - EXIT

echo "==> comparing what came out with what went in"
(cd go && go run ./cmd/otlpdagrctl verify -path "$WORK/out.json" -spans "$SPANS" -batches "$BATCHES")
for s in $SIGNALS; do
  (cd go && go run ./cmd/otlpdagrctl verify -signal "$s" -path "$WORK/out-$s.json" -batches "$BATCHES")
done

echo "==> a kill mid-write is detected, not silently accepted"
cp "$WORK/spans.dagr" "$WORK/torn.dagr"
python3 - "$WORK/torn.dagr" <<'PY'
import sys
p = sys.argv[1]
b = open(p, 'rb').read()
open(p, 'wb').write(b[:len(b) - max(1, len(b) // 7)])   # a write that never finished
PY
if (cd go && go run ./cmd/otlpdagrctl spool -path "$WORK/torn.dagr") ; then
  echo "FAIL: a torn spool was accepted as intact" >&2
  exit 1
fi
echo "==> round trip green"
