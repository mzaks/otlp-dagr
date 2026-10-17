#!/usr/bin/env bash
# The Java exporter benchmark (spec/38 §10.3.2): OTel Java SDK spans encoded by
# opentelemetry-java's own OTLP marshalers and by the Dagr direct builder, same batch.
#
#   java/run.sh            build, then the gates (java/src/otlpdagr/Check.java and the Go
#                          writer re-encoding Java's Dagr bytes, go/pdatabridge/java_test.go)
#   java/run.sh bench      ... then the JMH benchmark (BENCH_FORKS, default 1)
#   java/run.sh bench -- <JMH args>
#
# Needs ./regen.sh (java/gen) and the fixtures: (cd go && go run ./cmd/otlpdagrctl fixtures
# -out ../rust/otap/work). Dependencies come from Maven Central into java/lib, checked
# against the SHA-256 sums below; opentelemetry-java is pinned one release behind the
# newest (1.67.0 was days old when this was written).
set -euo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
cd "$here"
if [ -n "${JAVA_HOME:-}" ]; then JBIN="$JAVA_HOME/bin"
elif [ -x /opt/homebrew/opt/openjdk/bin/javac ]; then JBIN=/opt/homebrew/opt/openjdk/bin
else JBIN="$(dirname "$(command -v javac)")"; fi
[ -d gen/src/main/java/otlpgen ] || { echo "run: no java/gen — run ./regen.sh" >&2; exit 1; }
FIX=../rust/otap/work/traces-batch.pb
[ -f "$FIX" ] || (cd ../go && go run ./cmd/otlpdagrctl fixtures -out ../rust/otap/work)

LIB=lib
mkdir -p "$LIB"
while read -r sum path; do
  jar="$LIB/$(basename "$path")"
  [ -f "$jar" ] || curl -sfLo "$jar" "https://repo1.maven.org/maven2/$path"
  echo "$sum  $jar" | shasum -a 256 -c --quiet - || { rm -f "$jar"; echo "checksum mismatch: $jar" >&2; exit 1; }
done <<'DEPS'
d582be8b5d7a5e31e01801373a9c48a1b28c5a3c880438982eba6daa823f128e io/opentelemetry/opentelemetry-common/1.66.0/opentelemetry-common-1.66.0.jar
d83bd8e93d5e1df017a88c6702ff77d0887a4b4862b50e0009d01e57295dc87e io/opentelemetry/opentelemetry-context/1.66.0/opentelemetry-context-1.66.0.jar
b93c5e4b5d504e77670e85fdd8c258ab08bad808473c403a91ae65f1a393f0b7 io/opentelemetry/opentelemetry-api/1.66.0/opentelemetry-api-1.66.0.jar
305f3a15118c39a45b0d5b9b61fc61b6cb2fad2bdf8f3da2e1419627fabe2079 io/opentelemetry/opentelemetry-sdk-common/1.66.0/opentelemetry-sdk-common-1.66.0.jar
579c96897d9112b626fbf325efc116c2d4f046aec1bd72e9909ec0f5431a2040 io/opentelemetry/opentelemetry-sdk-trace/1.66.0/opentelemetry-sdk-trace-1.66.0.jar
b48a8b8d29efd2a1be67b9a75165f6830c88aa27fbdba296961f52e2d8ec1bc9 io/opentelemetry/opentelemetry-exporter-common/1.66.0/opentelemetry-exporter-common-1.66.0.jar
adf00d6e5ef70c9055443d4bfec7cb58abf4f62a4969f420f97ab28dd89c00e3 io/opentelemetry/opentelemetry-exporter-otlp-common/1.66.0/opentelemetry-exporter-otlp-common-1.66.0.jar
400621a8f0aac52fab7ec88f492edd356f5a4355caa8f92028f217a8b5e4ac3b com/google/protobuf/protobuf-java/4.35.0/protobuf-java-4.35.0.jar
dc0eaf2bbf0036a70b60798c785d6e03a9daf06b68b8edb0f1ba9eb3421baeb3 org/openjdk/jmh/jmh-core/1.37/jmh-core-1.37.jar
6a5604b5b804e0daca1145df1077609321687734a8b49387e49f10557c186c77 org/openjdk/jmh/jmh-generator-annprocess/1.37/jmh-generator-annprocess-1.37.jar
df26cc58f235f477db07f753ba5a3ab243ebe5789d9f89ecf68dd62ea9a66c28 net/sf/jopt-simple/jopt-simple/5.0.4/jopt-simple-5.0.4.jar
1e56d7b058d28b65abd256b8458e3885b674c1d588fa43cd7d1cbb9c7ef2b308 org/apache/commons/commons-math3/3.6.1/commons-math3-3.6.1.jar
DEPS
case "$(protoc --version)" in
  "libprotoc 35."*) ;;
  *) echo "protoc $(protoc --version) — the pinned protobuf-java is 4.35.0 (protoc 35.x)" >&2; exit 1 ;;
esac

OUT=build/classes
rm -rf build/proto "$OUT"
mkdir -p build/proto "$OUT"
(cd ../proto/otlp && protoc -I . --java_out="$here/build/proto" opentelemetry/proto/trace/v1/trace.proto \
  opentelemetry/proto/common/v1/common.proto opentelemetry/proto/resource/v1/resource.proto)
"$JBIN/javac" --release 22 -nowarn -d "$OUT" -cp "$LIB/*" \
  -processorpath "$LIB/jmh-generator-annprocess-1.37.jar:$LIB/jmh-core-1.37.jar" \
  $(find gen/src build/proto src -name '*.java') 2>&1 | grep -v '^Note' || true
[ -f "$OUT/otlpdagr/Check.class" ] || { echo "run: compile failed" >&2; exit 1; }
JFLAGS=(--sun-misc-unsafe-memory-access=allow)
"$JBIN/java" "${JFLAGS[@]}" -cp "$OUT:$LIB/*" otlpdagr.Check "$FIX" build/traces-batch.java.dagr
(cd ../go && OTLPDAGR_JAVA_DAGR="$here/build/traces-batch.java.dagr" go test ./pdatabridge -run TestJavaWriter -count=1) | tail -1

if [ "${1:-}" = bench ]; then
  shift
  args=()
  if [ "${1:-}" = "--" ]; then shift; args=("$@"); fi
  "$JBIN/java" "${JFLAGS[@]}" -cp "$OUT:$LIB/*" -Dotlpdagr.fixture="$here/$FIX" org.openjdk.jmh.Main \
    -f "${BENCH_FORKS:-1}" -prof gc ${args[@]+"${args[@]}"} 'otlpdagr.EncodeBench'
fi
