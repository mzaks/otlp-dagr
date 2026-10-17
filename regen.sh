#!/usr/bin/env bash
# Regenerate the Go, Rust and C++ targets from schema.py and refresh the receipt.
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
fi
