#!/usr/bin/env bash
# End to end through a REAL collector: the Dagr -> OTAP bridge's batches, sent over gRPC
# streams to the released contrib otelarrowreceiver in an ocb-built collector, must come
# out of its pipelines equal to the fixtures they were made from.
#
#   rust/otap/e2e.sh          (needs ./regen.sh; builds the collector on first run)
#
# Sent per signal, each on its own gRPC stream: the 20-batch stream (one producer: later
# batches reuse schemas and dictionaries), the same batches converted from the exporter's
# spool, and every single-batch fixture on a fresh stream. zstd on, as otel-arrow sends
# (E2E_ZSTD=0: uncompressed IPC bodies).
set -euo pipefail
cd "$(dirname "$0")"
ROOT="$(cd ../.. && pwd)"
WORK="$PWD/work"
PORT="${OTAP_PORT:-4327}"
BIN="$ROOT/ocb/build/otelcol-dagr"

# 1. the batches (and the unit gate over them)
DAGR2OTAP_ZSTD="${E2E_ZSTD:-1}" RUST_ORACLE=0 ./check.sh >/dev/null
echo "==> batches made and checked"

# 2. the collector (the ocb manifest has the otelarrow receiver)
if [ ! -x "$BIN" ] || ! strings "$BIN" | grep -q otelarrowreceiver; then
  "$ROOT/ocb/roundtrip.sh" build
fi

# 3. run it
WORK="$WORK" OTAP_PORT="$PORT" python3 -c 'import os,sys,string;sys.stdout.write(string.Template(open(sys.argv[1]).read()).safe_substitute(os.environ))' \
  "$ROOT/ocb/collector-otap.yaml" > "$WORK/collector-otap.yaml"
rm -f "$WORK"/out-otap-*.json "$WORK"/e2e-*.txt
"$BIN" --config "$WORK/collector-otap.yaml" > "$WORK/collector-otap.log" 2>&1 &
pid=$!
trap 'kill "$pid" 2>/dev/null || true' EXIT
for _ in $(seq 1 120); do
  grep -q "Everything is ready" "$WORK/collector-otap.log" 2>/dev/null && break
  kill -0 "$pid" 2>/dev/null || { cat "$WORK/collector-otap.log" >&2; exit 1; }
  sleep 0.25
done
echo "==> collector up on 127.0.0.1:$PORT"

# 4. send
SEND="$WORK/otapsend"
( cd check && go build -o "$SEND" ./cmd/otapsend )
for sig in traces logs metrics; do
  m="$WORK/e2e-$sig.txt"
  "$SEND" -endpoint "127.0.0.1:$PORT" -signal "$sig" "$WORK/$sig"-stream-*.sbar
  for f in "$WORK/$sig"-stream-*.sbar; do basename "${f%.sbar}"; done >> "$m"
  "$SEND" -endpoint "127.0.0.1:$PORT" -signal "$sig" "$WORK/$sig"-spool-*.spbar
  for f in "$WORK/$sig"-spool-*.spbar; do b="$(basename "${f%.spbar}")"; echo "${b/-spool-/-stream-}"; done >> "$m"
  for f in "$WORK/$sig"-*.bar; do
    case "$f" in *-stream-*|*-empty.bar) continue ;; esac
    "$SEND" -endpoint "127.0.0.1:$PORT" -signal "$sig" "$f"
    basename "${f%.bar}" >> "$m"
  done
  echo "  $sig: $(wc -l < "$m" | tr -d ' ') batches sent, every one acknowledged OK"
done

# 5. stop (the file exporter flushes on shutdown), then compare
kill -TERM "$pid"; wait "$pid" 2>/dev/null || true
trap - EXIT
( cd check && OTAP_E2E="$WORK" go test -count=1 -run TestCollector -v ./... | grep -E '^(---|ok|FAIL)|through the collector' )
echo "otap e2e: green (zstd=${E2E_ZSTD:-1})"
