package metricbridge_test

import (
	"bytes"
	"testing"

	"go.opentelemetry.io/collector/pdata/pmetric"

	"otlpdagr/internal/coverage"
	"otlpdagr/internal/fixture"
	"otlpdagr/metricbridge"
	d "otlpgen/otlpmetricsdirect"
)

func marshal(t *testing.T, md pmetric.Metrics) []byte {
	t.Helper()
	b, err := (&pmetric.ProtoMarshaler{}).MarshalMetrics(md)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// The wide batch reaches every field the schema has and pdata can express.
func TestWideCoverage(t *testing.T) {
	coverage.Check(t, metricbridge.ToValue(fixture.MetricsWide()), coverage.PdataInexpressible...)
}

// pdata -> Dagr -> pdata, compared as marshalled OTLP protobuf.
func TestWideRoundTrip(t *testing.T) {
	in := fixture.MetricsWide()
	out, err := metricbridge.ToMetrics(metricbridge.ToBytes(in))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(marshal(t, in), marshal(t, out)) {
		t.Fatalf("metrics did not survive the round trip")
	}
}

// Dagr -> values -> Dagr is byte-identical, and the reusable Builder agrees with the
// one-shot path across reuse (scratch that outlived a batch shows up on the second pass).
func TestWideBytesStable(t *testing.T) {
	in := fixture.MetricsWide()
	want := metricbridge.ToBytes(in)
	v, err := d.RestoreOtlpMetricsValues(want)
	if err != nil {
		t.Fatal(err)
	}
	if got := d.BuildOtlpMetrics(v); !bytes.Equal(got, want) {
		t.Fatalf("restore -> build is not byte-identical: %d B vs %d B", len(got), len(want))
	}
	w := metricbridge.NewBuilder()
	for i := 0; i < 3; i++ {
		if got := w.BuildAppend(nil, in); !bytes.Equal(got, want) {
			t.Fatalf("builder pass %d diverged", i)
		}
	}
}

// The distinctions a lossy bridge would erase, checked by value rather than through a
// byte comparison that would pass if both sides were wrong the same way.
func TestPresenceIsPreserved(t *testing.T) {
	out, err := metricbridge.ToMetrics(metricbridge.ToBytes(fixture.MetricsWide()))
	if err != nil {
		t.Fatal(err)
	}
	ms := out.ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics()
	gp := ms.At(0).Gauge().DataPoints().At(1)
	if gp.ValueType() != pmetric.NumberDataPointValueTypeInt || gp.IntValue() != 0 {
		t.Fatalf("an int 0 gauge value must stay present: %v", gp.ValueType())
	}
	h := ms.At(2).Histogram().DataPoints()
	if !h.At(0).HasSum() || h.At(0).Sum() != 0 {
		t.Fatal("a present 0.0 histogram sum became absent")
	}
	if h.At(1).HasSum() || h.At(1).HasMin() || h.At(1).HasMax() {
		t.Fatal("an absent histogram sum/min/max became present")
	}
	if ms.At(5).Type() != pmetric.MetricTypeEmpty {
		t.Fatalf("a metric with no data must stay Empty, got %v", ms.At(5).Type())
	}
}

func TestEmpty(t *testing.T) {
	for _, in := range []pmetric.Metrics{pmetric.NewMetrics(), func() pmetric.Metrics {
		md := pmetric.NewMetrics()
		md.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty()
		return md
	}()} {
		out, err := metricbridge.ToMetrics(metricbridge.ToBytes(in))
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(marshal(t, in), marshal(t, out)) {
			t.Fatal("empty shape did not survive")
		}
	}
}
