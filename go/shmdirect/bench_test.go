//go:build unix

package shmdirect_test

// What one recording costs: a direct instrument (an atomic operation in shared memory)
// against the OTel SDK's instrument with the attributes precomputed as an attribute set
// (the fastest SDK form: it still finds the stream by the set on every call).
//
//	go test ./shmdirect -run '^$' -bench . -benchmem

import (
	"context"
	"path/filepath"
	"testing"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"

	"otlpdagr/shmdirect"
)

func provider(b *testing.B) *shmdirect.Provider {
	p, err := shmdirect.NewProvider(filepath.Join(b.TempDir(), "b.dagrshm"), "bench")
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { p.Shutdown() })
	return p
}

var attrs = []attribute.KeyValue{attribute.String("route", "/api/v1/r3"), attribute.Int("code", 203)}

func sdk(b *testing.B) metric.Meter {
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(sdkmetric.NewManualReader()))
	b.Cleanup(func() { mp.Shutdown(context.Background()) })
	return mp.Meter("bench")
}

func BenchmarkCounterAdd(b *testing.B) {
	b.Run("direct", func(b *testing.B) {
		c, _ := provider(b).Int64Counter("bench", "c", "1",
			shmdirect.Attr{Key: "route", Value: "/api/v1/r3"}, shmdirect.Attr{Key: "code", Value: 203})
		b.ReportAllocs()
		for b.Loop() {
			c.Add(1)
		}
	})
	b.Run("sdk", func(b *testing.B) {
		c, _ := sdk(b).Int64Counter("c")
		opt := metric.WithAttributeSet(attribute.NewSet(attrs...))
		ctx := context.Background()
		b.ReportAllocs()
		for b.Loop() {
			c.Add(ctx, 1, opt)
		}
	})
}

// All cores on ONE stream: the contended case (one cache line for every core).
func BenchmarkCounterAddParallel(b *testing.B) {
	b.Run("direct", func(b *testing.B) {
		c, _ := provider(b).Int64Counter("bench", "c", "1")
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				c.Add(1)
			}
		})
	})
	b.Run("sdk", func(b *testing.B) {
		c, _ := sdk(b).Int64Counter("c")
		opt := metric.WithAttributeSet(attribute.NewSet(attrs...))
		ctx := context.Background()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				c.Add(ctx, 1, opt)
			}
		})
	})
}

func BenchmarkHistogramRecord(b *testing.B) {
	b.Run("direct", func(b *testing.B) {
		h, _ := provider(b).Float64Histogram("bench", "h", "ms", nil)
		b.ReportAllocs()
		i := 0
		for b.Loop() {
			h.Record(float64(i % 2000))
			i++
		}
	})
	b.Run("sdk", func(b *testing.B) {
		h, _ := sdk(b).Float64Histogram("h")
		opt := metric.WithAttributeSet(attribute.NewSet(attrs...))
		ctx := context.Background()
		b.ReportAllocs()
		i := 0
		for b.Loop() {
			h.Record(ctx, float64(i%2000), opt)
			i++
		}
	})
}
