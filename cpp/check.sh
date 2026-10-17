#!/usr/bin/env bash
# The C++ producer gate (spec/38 §10.3.1): builds cpp/shmdirect against the generated
# headers (run ./regen.sh first), then has the Go `dagrshm` receiver read what a C++
# process recorded (go/shmreceiver/cpp_test.go). Pass `bench` to also time recordings.
#
#   cpp/check.sh [bench]
set -euo pipefail
cd "$(dirname "$0")"
[ -d gen/include/otlpgen ] || { echo "check: no cpp/gen — run ./regen.sh" >&2; exit 1; }
mkdir -p build
CXX="${CXX:-c++}"
FLAGS=(-std=c++17 -O2 -Wall -Wextra -Werror -pthread -Igen/include -Ishmdirect)
"$CXX" "${FLAGS[@]}" shmdirect/demo.cpp -o build/shmdirect_demo
# the same producer under ThreadSanitizer: the recordings race on purpose, atomically.
# Apple clang 17's TSan runtime crashes at start-up on macOS 26, so take the first
# compiler whose TSan binary runs ($TSAN_CXX, $CXX, Homebrew LLVM 22+) — or skip the leg.
demos=(shmdirect_demo)
printf '#include <thread>\nint x; int main() { std::thread t([] { __atomic_add_fetch(&x, 1, __ATOMIC_SEQ_CST); }); t.join(); }\n' > build/probe.cpp
for cand in "${TSAN_CXX:-}" "$CXX" /opt/homebrew/opt/llvm/bin/clang++ /usr/local/opt/llvm/bin/clang++; do
  [ -n "$cand" ] && command -v "$cand" >/dev/null || continue
  # a background job, so the shell does not report the probe's crash
  if "$cand" -std=c++17 -fsanitize=thread build/probe.cpp -o build/probe 2>/dev/null &&
     { ./build/probe >/dev/null 2>&1 & wait $!; } 2>/dev/null; then
    "$cand" "${FLAGS[@]}" -g -fsanitize=thread shmdirect/demo.cpp -o build/shmdirect_demo_tsan
    demos+=(shmdirect_demo_tsan)
    break
  fi
done
[ ${#demos[@]} -eq 2 ] || echo "check: no compiler with a working ThreadSanitizer — that leg is skipped"
for demo in "${demos[@]}"; do
  ( cd ../go && OTLPDAGR_CPP_DEMO="$PWD/../cpp/build/$demo" go test ./shmreceiver -run 'TestCpp' -count=1 -v ) \
    | grep -E '^(---|ok|FAIL|PASS)|cpp_test' || exit 1
done
if [ "${1:-}" = bench ]; then
  "$CXX" "${FLAGS[@]}" -O3 shmdirect/bench.cpp -o build/shmdirect_bench
  d=$(mktemp -d); ./build/shmdirect_bench "$d"; rm -rf "$d"
fi
echo "check: C++ producer → Go receiver green"
