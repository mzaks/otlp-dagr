//go:build unix

package dagrshmreceiver

// The same-host hop, decomposed (spec/38 §7.1): what one export and one ingest of a
// 500-stream batch cost, piece by piece, next to the OTLP decode of the same batch.
//
//	go test ./shmreceiver -run '^$' -bench . -benchmem

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"

	"go.opentelemetry.io/collector/pdata/pmetric/pmetricotlp"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.opentelemetry.io/otel/sdk/resource"

	mr "otlpgen/metricsregion"

	"otlpdagr/shm"
	"otlpdagr/shmexporter"
)

// load500 is cmd/shmbench's load: 20 counters, 10 gauges, 20 histograms x 10 attribute
// sets, recorded once; returns a reader to collect it.
func load500(b *testing.B) *sdkmetric.ManualReader {
	rd := sdkmetric.NewManualReader(sdkmetric.WithTemporalitySelector(
		func(sdkmetric.InstrumentKind) metricdata.Temporality { return metricdata.CumulativeTemporality }))
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(rd),
		sdkmetric.WithResource(resource.NewSchemaless(attribute.String("service.name", "bench"))))
	m := mp.Meter("otlp-dagr/shmbench")
	ctx := context.Background()
	for s := 0; s < 10; s++ {
		o := metric.WithAttributes(attribute.String("route", fmt.Sprintf("/api/v1/r%d", s)), attribute.Int("code", 200+s))
		for i := 0; i < 20; i++ {
			c, _ := m.Int64Counter(fmt.Sprintf("bench.counter.%d", i), metric.WithUnit("{op}"))
			c.Add(ctx, int64(i+s), o)
			h, _ := m.Float64Histogram(fmt.Sprintf("bench.latency.%d", i), metric.WithUnit("ms"))
			h.Record(ctx, float64(i*37+s), o)
		}
		for i := 0; i < 10; i++ {
			g, _ := m.Float64Gauge(fmt.Sprintf("bench.gauge.%d", i), metric.WithUnit("1"))
			g.Record(ctx, float64(i*s), o)
		}
	}
	return rd
}

// The SDK's own collection pass — paid by EVERY push exporter, whatever it does next.
func BenchmarkAppCollect(b *testing.B) {
	rd := load500(b)
	var rm metricdata.ResourceMetrics
	b.ReportAllocs()
	for b.Loop() {
		rd.Collect(context.Background(), &rm)
	}
}

// shmexporter.Export alone: one seqlock write of the 500 points.
func BenchmarkAppExportShm(b *testing.B) {
	rd := load500(b)
	var rm metricdata.ResourceMetrics
	rd.Collect(context.Background(), &rm)
	exp, err := shmexporter.New(filepath.Join(b.TempDir(), "b.dagrshm"), "bench")
	if err != nil {
		b.Fatal(err)
	}
	defer exp.Shutdown(context.Background())
	b.ReportAllocs()
	for b.Loop() {
		exp.Export(context.Background(), &rm)
	}
}

func region500(b *testing.B) (*shm.Region, []byte) {
	rd := load500(b)
	var rm metricdata.ResourceMetrics
	rd.Collect(context.Background(), &rm)
	path := filepath.Join(b.TempDir(), "b.dagrshm")
	exp, err := shmexporter.New(path, "bench")
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { exp.Shutdown(context.Background()) })
	exp.Export(context.Background(), &rm)
	r, err := shm.Open(path)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { r.Close() })
	return r, make([]byte, mr.ByteSize)
}

// The collector's read: the seqlock snapshot (a copy of the whole region) ...
func BenchmarkIngestShmSnapshot(b *testing.B) {
	r, dst := region500(b)
	b.SetBytes(mr.ByteSize)
	b.ReportAllocs()
	for b.Loop() {
		if _, err := r.Snapshot(dst, 100); err != nil {
			b.Fatal(err)
		}
	}
}

// ... and the conversion into pdata, which any receiver pays to hand a batch on.
func BenchmarkIngestShmToPdata(b *testing.B) {
	r, dst := region500(b)
	s, err := r.Snapshot(dst, 100)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	for b.Loop() {
		ToMetrics(s)
	}
}

// The OTLP receiver's decode of the SAME batch (marshalled from the region's pdata).
func BenchmarkIngestOTLPUnmarshal(b *testing.B) {
	r, dst := region500(b)
	s, _ := r.Snapshot(dst, 100)
	raw, err := pmetricotlp.NewExportRequestFromMetrics(ToMetrics(s)).MarshalProto()
	if err != nil {
		b.Fatal(err)
	}
	b.SetBytes(int64(len(raw)))
	b.ReportAllocs()
	for b.Loop() {
		req := pmetricotlp.NewExportRequest()
		if err := req.UnmarshalProto(raw); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportMetric(float64(len(raw)), "otlp-bytes")
}
