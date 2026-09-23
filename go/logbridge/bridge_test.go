package logbridge_test

import (
	"bytes"
	"testing"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"

	"otlpdagr/internal/coverage"
	"otlpdagr/internal/fixture"
	"otlpdagr/logbridge"
	d "otlpgen/otlplogsdirect"
)

func marshal(t *testing.T, ld plog.Logs) []byte {
	t.Helper()
	b, err := (&plog.ProtoMarshaler{}).MarshalLogs(ld)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestWideCoverage(t *testing.T) {
	coverage.Check(t, logbridge.ToValue(fixture.LogsWide()), coverage.PdataInexpressible...)
}

func TestWideRoundTrip(t *testing.T) {
	in := fixture.LogsWide()
	out, err := logbridge.ToLogs(logbridge.ToBytes(in))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(marshal(t, in), marshal(t, out)) {
		t.Fatalf("logs did not survive the round trip")
	}
	lrs := out.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords()
	if lrs.At(2).Body().Type() != pcommon.ValueTypeEmpty {
		t.Fatal("an absent body became present")
	}
}

func TestWideBytesStable(t *testing.T) {
	in := fixture.LogsWide()
	want := logbridge.ToBytes(in)
	v, err := d.RestoreOtlpLogsValues(want)
	if err != nil {
		t.Fatal(err)
	}
	if got := d.BuildOtlpLogs(v); !bytes.Equal(got, want) {
		t.Fatalf("restore -> build is not byte-identical: %d B vs %d B", len(got), len(want))
	}
	w := logbridge.NewBuilder()
	for i := 0; i < 3; i++ {
		if got := w.BuildAppend(nil, in); !bytes.Equal(got, want) {
			t.Fatalf("builder pass %d diverged", i)
		}
	}
}
