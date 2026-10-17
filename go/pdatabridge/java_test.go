package pdatabridge_test

import (
	"bytes"
	"os"
	"strings"
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
	// the benchmark batch, and Java's wide batch (java/src/otlpdagr/Wide.java: links, arrays,
	// remote parents, trace state, dropped counts, every status, resources and scopes interleaved)
	for file, spans := range map[string]int{path: 500, strings.Replace(path, ".dagr", ".wide.dagr", 1): 24} {
		java, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		td, err := pdatabridge.ToTraces(java)
		if err != nil {
			t.Fatal(err)
		}
		if n := td.SpanCount(); n != spans {
			t.Fatalf("%s: %d spans, want %d", file, n, spans)
		}
		for name, got := range map[string][]byte{
			"value tree": pdatabridge.ToBytes(td),
			"one pass":   pdatabridge.NewStreamBuilder().BuildAppend(nil, td),
		} {
			if !bytes.Equal(got, java) {
				t.Fatalf("%s: Go's %s encoder: %d B, Java's %d B", file, name, len(got), len(java))
			}
		}
	}
}
