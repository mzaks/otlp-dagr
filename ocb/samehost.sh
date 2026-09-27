#!/usr/bin/env bash
# The same-host metrics hop, measured (spec/38 §7.1): one app, one collector, three paths.
#
#   shm   the app's SDK exports into a shared-memory region; `dagrshm` reads it in place
#   otlp  the app's SDK exports OTLP/gRPC to localhost; the `otlp` receiver decodes it
#   prom  the app serves Prometheus /metrics; the `prometheus` receiver scrapes it
#
# Same load in every mode (go/cmd/shmbench: STREAMS streams, all recorded every
# INTERVAL); the collector discards what it receives (`nop`), so its CPU is ingest. For
# each round and mode: the app's own CPU (user+sys, from its rusage) and the collector's
# CPU over the app's run. Modes alternate within a round, so drift hits all three alike.
#
#   ./ocb/samehost.sh
#   ROUNDS=5 DURATION=30s STREAMS=500 INTERVAL=100ms ./ocb/samehost.sh
#
# Before timing, a check pass per mode swaps `nop` for a file exporter and requires the
# collector to have received every stream — a path that silently delivers nothing would
# otherwise measure cheapest.
set -euo pipefail
cd "$(dirname "$0")/.."
ROOT="$PWD"
WORK="${WORK:-$ROOT/ocb/work}"
BIN="$ROOT/ocb/build/otelcol-dagr"
INTERVAL="${INTERVAL:-100ms}"
DURATION="${DURATION:-20s}"
STREAMS="${STREAMS:-500}"
ROUNDS="${ROUNDS:-3}"
MODES="${MODES:-none direct shm otlp prom}"   # none: the app's recording cost alone, no collector

# rebuild a collector older than any Go source, the manifest or the PGO profile it is built
# from (the rule roundtrip.sh uses) — a stale binary would measure the OLD receiver
if [ ! -x "$BIN" ] || [ -n "$(find "$ROOT/go" -name '*.go' -newer "$BIN" -print -quit)" ] \
   || [ "$ROOT/ocb/builder-config.yaml" -nt "$BIN" ] || [ "$ROOT/ocb/default.pgo" -nt "$BIN" ]; then
  ./ocb/roundtrip.sh build
fi
mkdir -p "$WORK"
(cd go && go build -o "$WORK/shmbench" ./cmd/shmbench && go build -o "$WORK/otlpdagrctl" ./cmd/otlpdagrctl)

render() {  # render <template> <out> [nop|file]
  WORK="$WORK" INTERVAL="$INTERVAL" python3 - "$1" "$2" "${3:-nop}" <<'PY'
import os, sys, string
src = string.Template(open(sys.argv[1]).read()).safe_substitute(os.environ)
if sys.argv[3] == "file":
    src = src.replace("  nop:\n", "  file:\n    path: " + os.environ["WORK"] + "/samehost-out.json\n")
    src = src.replace("exporters: [nop]", "exporters: [file]")
open(sys.argv[2], "w").write(src)
PY
}

cpu() { ps -o time= -p "$1" | awk -F: '{ if (NF == 3) print $1*3600 + $2*60 + $3; else print $1*60 + $2 }'; }

# stop <pid> <mode>: SIGINT, then SIGKILL after 10 s — the prometheus receiver has been
# seen to hang in shutdown after its target went away, and a wait on it then never returns.
# The collector's CPU is sampled before this, so a forced stop changes no number.
stop() {
  kill -INT "$1" 2>/dev/null || true
  for _ in $(seq 1 100); do kill -0 "$1" 2>/dev/null || break; sleep 0.1; done
  if kill -0 "$1" 2>/dev/null; then
    echo "    ($2: the collector did not stop on SIGINT; killed)" >&2
    kill -9 "$1" 2>/dev/null || true
  fi
  wait "$1" 2>/dev/null || true
}

run() {  # run <mode> <nop|file> <duration> -> "app_cpu collector_cpu"
  local mode="$1" exp="$2" dur="$3"
  if [ "$mode" = none ]; then
    "$WORK/shmbench" -mode none -interval "$INTERVAL" -duration "$dur" -streams "$STREAMS" |
      sed -E 's/.*cpu=([0-9.]+)s.*/\1 0/'
    return
  fi
  rm -rf "$WORK/samehost" "$WORK/samehost-out.json"
  mkdir -p "$WORK/samehost"
  render "$ROOT/ocb/samehost-$mode.yaml" "$WORK/samehost-$mode.yaml" "$exp"
  # prom: Go's own resolver. Prometheus's scrape manager looks up this host's FQDN before
  # it scrapes anything (scrape.Manager.setOffsetSeed -> osutil.GetFQDN), and a reverse
  # DNS lookup the network does not answer blocks the system resolver for good (seen
  # 2026-09-27); Go's resolver gives up after its timeouts (~60 s here). Wait for the
  # scrape manager rather than guessing — the CPU window starts after it either way.
  local dns=""
  [ "$mode" = prom ] && dns=go
  GODEBUG="${dns:+netdns=$dns}" "$BIN" --config "$WORK/samehost-$mode.yaml" >"$WORK/samehost-$mode.log" 2>&1 &
  local pid=$!
  trap 'kill "$pid" 2>/dev/null || true' EXIT
  if [ "$mode" = prom ]; then
    for _ in $(seq 1 180); do grep -q "Starting scrape manager" "$WORK/samehost-$mode.log" && break; sleep 1; done
  fi
  # Prometheus' discovery hands targets to the scrape manager on a ~5s tick: every mode
  # waits past it, so no mode starts its window with its receiver still idle
  sleep 6
  kill -0 "$pid" 2>/dev/null || { cat "$WORK/samehost-$mode.log" >&2; exit 1; }
  local c0 c1 out
  c0=$(cpu "$pid")
  out=$("$WORK/shmbench" -mode "$mode" -dir "$WORK/samehost" -interval "$INTERVAL" \
          -duration "$dur" -streams "$STREAMS")
  c1=$(cpu "$pid")
  stop "$pid" "$mode"
  trap - EXIT
  echo "$(sed -E 's/.*cpu=([0-9.]+)s.*/\1/' <<<"$out") $(awk -v a="$c0" -v b="$c1" 'BEGIN { printf "%.3f", b - a }')"
}

echo "==> check: every mode delivers all $STREAMS streams"
for mode in $MODES; do
  [ "$mode" = none ] && continue
  run "$mode" file 5s >/dev/null
  read -r lines pts < <("$WORK/otlpdagrctl" points -path "$WORK/samehost-out.json")
  if [ "$pts" -lt "$STREAMS" ]; then
    echo "FAIL: $mode delivered $pts of $STREAMS streams ($lines batches)" >&2
    exit 1
  fi
  echo "    $mode: $lines batches, the last with $pts streams"
done

echo "==> timing: $ROUNDS rounds x ($MODES), $STREAMS streams every $INTERVAL for $DURATION"
printf "%-6s %-5s %10s %14s %10s\n" round mode app_cpu collector_cpu total
for r in $(seq 1 "$ROUNDS"); do
  for mode in $MODES; do
    read -r a c < <(run "$mode" nop "$DURATION")
    printf "%-6s %-5s %9.3fs %13.3fs %9.3fs\n" "$r" "$mode" "$a" "$c" "$(awk -v a="$a" -v c="$c" 'BEGIN { print a + c }')"
  done
done
