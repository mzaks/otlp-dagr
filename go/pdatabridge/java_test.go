package pdatabridge_test

import (
	"bytes"
	"os"
	"testing"

	"otlpdagr/pdatabridge"
)

// The Java exporter's Dagr buffer (java/src/otlpdagr/DagrTraceEncoder.java, spec/38
// §10.3.2), read by Go and written again by every Go encoder: the bytes must not change.
// So Java's writer and Go's agree byte for byte on the same content — the benchmark batch
// as the OTel Java SDK holds it (attributes sorted, span flags set). `java/run.sh` writes
// the buffer and runs this; without OTLPDAGR_JAVA_DAGR it skips.
func TestJavaWriter(t *testing.T) {
	path := os.Getenv("OTLPDAGR_JAVA_DAGR")
	if path == "" {
		t.Skip("OTLPDAGR_JAVA_DAGR not set — run java/run.sh")
	}
	java, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	td, err := pdatabridge.ToTraces(java)
	if err != nil {
		t.Fatal(err)
	}
	if n := td.SpanCount(); n != 500 {
		t.Fatalf("%d spans, want 500", n)
	}
	for name, got := range map[string][]byte{
		"value tree": pdatabridge.ToBytes(td),
		"one pass":   pdatabridge.NewStreamBuilder().BuildAppend(nil, td),
	} {
		if !bytes.Equal(got, java) {
			t.Fatalf("Go's %s encoder: %d B, Java's %d B", name, len(got), len(java))
		}
	}
}
