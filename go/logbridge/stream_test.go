package logbridge_test

import (
	"bytes"
	"testing"

	"go.opentelemetry.io/collector/pdata/plog"

	"otlpdagr/internal/fixture"
	"otlpdagr/logbridge"
	"otlpgen/logrecordlog"
)

func streamCases() map[string]plog.Logs {
	return map[string]plog.Logs{
		"wide":  fixture.LogsWide(),
		"batch": fixture.LogsBatch(200),
		"empty": plog.NewLogs(),
	}
}

// The one-pass encoder writes the containers by hand, so it is gated against the
// value-tree path byte for byte: the graph buffer (BuildAppend == ToBytes) and the spool
// record (AppendRecord == the generated LogRecordLogWriter.AppendBatch).
func TestStreamMatches(t *testing.T) {
	enc := logbridge.NewStreamBuilder()
	for name, md := range streamCases() {
		if got, want := enc.BuildAppend(nil, md), logbridge.ToBytes(md); !bytes.Equal(got, want) {
			t.Fatalf("%s: stream buffer %d bytes, value tree %d", name, len(got), len(want))
		}
		const ts = 1_758_000_000_000_000_000
		w := logrecordlog.NewLogRecordLogWriter()
		if _, err := w.AppendBatch(logrecordlog.Batch{ReceivedUnixNano: ts, LogRecordCount: uint32(md.LogRecordCount()),
			ResourceLogs: logbridge.ToValue(md).ResourceLogs.V}); err != nil {
			t.Fatal(err)
		}
		rd, _ := logrecordlog.OpenLogRecordLogReader(w.Bytes())
		rec, _ := rd.Next()
		if got := enc.AppendRecord(nil, ts, md); !bytes.Equal(got, rec.Raw()) {
			t.Fatalf("%s: stream record %d bytes, generated %d", name, len(got), len(rec.Raw()))
		}
	}
}

// Once its scratch has grown, the encoder allocates nothing.
func TestStreamAllocatesNothing(t *testing.T) {
	md := fixture.LogsBatch(200)
	enc := logbridge.NewStreamBuilder()
	dst := enc.AppendRecord(nil, 1, md)
	if n := testing.AllocsPerRun(20, func() { dst = enc.AppendRecord(dst[:0], 1, md) }); n != 0 {
		t.Fatalf("%.1f allocations per encode, want 0", n)
	}
}
