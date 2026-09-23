package metricbridge_test

import (
	"bytes"
	"testing"

	"go.opentelemetry.io/collector/pdata/pmetric"

	"otlpdagr/internal/fixture"
	"otlpdagr/metricbridge"
	"otlpgen/metriclog"
)

func streamCases() map[string]pmetric.Metrics {
	return map[string]pmetric.Metrics{
		"wide":  fixture.MetricsWide(),
		"batch": fixture.MetricsBatch(20, 30),
		"empty": pmetric.NewMetrics(),
	}
}

// The one-pass encoder writes the containers by hand, so it is gated against the
// value-tree path byte for byte: the graph buffer (BuildAppend == ToBytes) and the spool
// record (AppendRecord == the generated MetricLogWriter.AppendBatch).
func TestStreamMatches(t *testing.T) {
	enc := metricbridge.NewStreamBuilder()
	for name, md := range streamCases() {
		if got, want := enc.BuildAppend(nil, md), metricbridge.ToBytes(md); !bytes.Equal(got, want) {
			t.Fatalf("%s: stream buffer %d bytes, value tree %d", name, len(got), len(want))
		}
		const ts = 1_758_000_000_000_000_000
		w := metriclog.NewMetricLogWriter(metriclog.SpoolHeader{})
		if _, err := w.AppendBatch(metriclog.Batch{ReceivedUnixNano: ts, DataPointCount: uint32(md.DataPointCount()),
			ResourceMetrics: metricbridge.ToValue(md).ResourceMetrics.V}); err != nil {
			t.Fatal(err)
		}
		rd, _ := metriclog.OpenMetricLogReader(w.Bytes())
		rec, _ := rd.Next()
		if got := enc.AppendRecord(nil, ts, md); !bytes.Equal(got, rec.Raw()) {
			t.Fatalf("%s: stream record %d bytes, generated %d", name, len(got), len(rec.Raw()))
		}
	}
}

// Once its scratch has grown, the encoder allocates nothing.
func TestStreamAllocatesNothing(t *testing.T) {
	md := fixture.MetricsBatch(20, 30)
	enc := metricbridge.NewStreamBuilder()
	dst := enc.AppendRecord(nil, 1, md)
	if n := testing.AllocsPerRun(20, func() { dst = enc.AppendRecord(dst[:0], 1, md) }); n != 0 {
		t.Fatalf("%.1f allocations per encode, want 0", n)
	}
}
