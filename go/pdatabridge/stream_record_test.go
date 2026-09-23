package pdatabridge_test

import (
	"bytes"
	"testing"

	"go.opentelemetry.io/collector/pdata/ptrace"

	"otlpdagr/internal/fixture"
	"otlpdagr/pdatabridge"
	"otlpgen/spanlog"
)

// AppendRecord hand-mirrors the generated SpanLog store around the one-pass traces
// encoder, so it is gated the way BuildAppend is (TestStreamMatches): the record it
// writes must be byte-for-byte the record SpanLogWriter.AppendBatch writes from the
// value tree — for a wide batch, an empty one, and a span count past the LEB width.
func TestStreamRecordMatches(t *testing.T) {
	cases := map[string]ptrace.Traces{
		"wide":  fixture.TracesWide(),
		"500":   fixture.TracesN(500),
		"empty": ptrace.NewTraces(),
	}
	enc := pdatabridge.NewStreamBuilder()
	for name, td := range cases {
		for _, ts := range []uint64{0, 1_758_000_000_000_000_000} {
			got := enc.AppendRecord(nil, ts, td)

			w := spanlog.NewSpanLogWriter()
			v := spanlog.Batch{ReceivedUnixNano: ts, SpanCount: uint32(td.SpanCount()),
				ResourceSpans: pdatabridge.ToValue(td).ResourceSpans.V}
			if _, err := w.AppendBatch(v); err != nil {
				t.Fatal(err)
			}
			rd, err := spanlog.OpenSpanLogReader(w.Bytes())
			if err != nil {
				t.Fatal(err)
			}
			rec, ok := rd.Next()
			if !ok {
				t.Fatalf("%s: no record", name)
			}
			if want := rec.Raw(); !bytes.Equal(got, want) {
				t.Fatalf("%s @%d: stream record %d bytes, generated %d", name, ts, len(got), len(want))
			}
		}
	}
}
