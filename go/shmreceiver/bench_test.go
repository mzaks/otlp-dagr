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
	mv "otlpgen/metricsvalues"

	"otlpdagr/shm"
	"otlpdagr/shmdirect"
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

// A direct-instrument app's values merged into the descriptor snapshot, per poll — the
// part of a direct ingest that grows with the app's per-core copies (spec/38 §7.1.1).
// Same 500 streams as load500: 200 counters, 100 gauges, 200 histograms.
func BenchmarkIngestDirectOverlay(b *testing.B) {
	for _, n := range []int{1, 16, 64} {
		b.Run(fmt.Sprintf("copies=%d", n), func(b *testing.B) {
			path := filepath.Join(b.TempDir(), "d.dagrshm")
			p, err := shmdirect.NewProviderWith(path, "bench", shmdirect.Options{Shards: n})
			if err != nil {
				b.Fatal(err)
			}
			b.Cleanup(func() { p.Shutdown() })
			for s := 0; s < 10; s++ {
				at := []shmdirect.Attr{{Key: "route", Value: fmt.Sprintf("/api/v1/r%d", s)}, {Key: "code", Value: 200 + s}}
				for i := 0; i < 20; i++ {
					c, _ := p.Int64Counter("bench", fmt.Sprintf("counter_%d", i), "1", at...)
					c.Add(int64(i + s))
					h, _ := p.Float64Histogram("bench", fmt.Sprintf("hist_%d", i), "ms", nil, at...)
					h.Record(float64(i * s))
				}
				for i := 0; i < 10; i++ {
					g, _ := p.Float64Gauge("bench", fmt.Sprintf("gauge_%d", i), "1", at...)
					g.Record(float64(i))
				}
			}
			r, err := shm.Open(path)
			if err != nil {
				b.Fatal(err)
			}
			b.Cleanup(func() { r.Close() })
			s, err := r.Snapshot(make([]byte, mr.ByteSize), 100)
			if err != nil {
				b.Fatal(err)
			}
			vm, err := shm.OpenMapping(path+".values", mv.ByteSize)
			if err != nil {
				b.Fatal(err)
			}
			b.Cleanup(func() { vm.Close() })
			copies, err := shmdirect.Values(vm.Mem)
			if err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			for b.Loop() {
				OverlayValues(s, copies, 1)
			}
		})
	}
}
