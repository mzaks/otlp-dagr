#!/usr/bin/env bash
# Regenerate the Go, Rust, C++ and Java targets from schema.py and refresh the receipt.
#
#   ./regen.sh            # regenerate
#   ./regen.sh check      # diff schema.py against the committed receipt, generate nothing
#
# The Dagr toolchain is not on PyPI; point DAGR_REPO at a checkout of it.
set -euo pipefail
cd "$(dirname "$0")"

DAGR_REPO="${DAGR_REPO:-$HOME/dev/RethinkingDagr}"
if [ ! -d "$DAGR_REPO/dagr" ]; then
  echo "regen: no Dagr checkout at $DAGR_REPO — set DAGR_REPO=/path/to/dagr" >&2
  exit 1
fi

cmd="${1:-build}"
PYTHONPATH="$DAGR_REPO" python3 -m dagr.cli "$cmd"

if [ "$cmd" = "build" ]; then
  ( cd go && gofmt -l . | grep -v '^gen/' || true )
  ( cd go && go build ./... && go vet ./... )
  echo "regen: go build + vet clean"
  ( cd rust/gen && cargo build --release --quiet )
  echo "regen: rust (lazy + arrow) build clean"
  cpp/check.sh
  JAVAC="${JAVA_HOME:+$JAVA_HOME/bin/}javac"
  [ -x /opt/homebrew/opt/openjdk/bin/javac ] && [ -z "${JAVA_HOME:-}" ] && JAVAC=/opt/homebrew/opt/openjdk/bin/javac
  rm -rf java/build/gen-check && "$JAVAC" --release 22 -nowarn -d java/build/gen-check $(find java/gen -name '*.java') 2>&1 | grep -v '^Note' || true
  [ -d java/build/gen-check/otlpgen ] && echo "regen: java build clean"
fi
