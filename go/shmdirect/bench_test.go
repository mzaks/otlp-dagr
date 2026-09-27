//go:build unix

package shmdirect_test

// What one recording costs: a direct instrument (an atomic operation in shared memory)
// against the OTel SDK's instrument with the attributes precomputed as an attribute set
// (the fastest SDK form: it still finds the stream by the set on every call).
//
//	go test ./shmdirect -run '^$' -bench . -benchmem

import (
	"context"
	"fmt"
	"path/filepath"
	"sync/atomic"
	"testing"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"

	"otlpdagr/shmdirect"
)

func providerWith(b *testing.B, o shmdirect.Options) *shmdirect.Provider {
	p, err := shmdirect.NewProviderWith(filepath.Join(b.TempDir(), "b.dagrshm"), "bench", o)
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
	each(b, func(b *testing.B, p *shmdirect.Provider) {
		c, _ := p.Int64Counter("bench", "c", "1",
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

// The copy choices a contended benchmark compares: the P's copy (the default) and one
// copy for everyone (no sharding).
var choices = []struct {
	name   string
	shards int
}{{"direct", 0}, {"direct-1copy", 1}}

func each(b *testing.B, run func(b *testing.B, p *shmdirect.Provider)) {
	for _, c := range choices {
		b.Run(c.name, func(b *testing.B) {
			run(b, providerWith(b, shmdirect.Options{Shards: c.shards}))
		})
	}
}

// All cores on ONE stream: the contended case (one cache line for every core).
func BenchmarkCounterAddParallel(b *testing.B) {
	each(b, func(b *testing.B, p *shmdirect.Provider) {
		c, _ := p.Int64Counter("bench", "c", "1")
		b.ReportAllocs()
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
		b.ReportAllocs()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				c.Add(ctx, 1, opt)
			}
		})
	})
}

// Every goroutine on a stream of its OWN: no contention to remove, so this is what the
// copies cost when they buy nothing (a counter's word is 176 bytes from its neighbour's,
// so adjacent streams do not share a line either way).
func BenchmarkCounterAddParallelStreams(b *testing.B) {
	each(b, func(b *testing.B, p *shmdirect.Provider) {
		var next atomic.Int32
		cs := make([]*shmdirect.Int64Counter, 64)
		for i := range cs {
			cs[i], _ = p.Int64Counter("bench", fmt.Sprintf("c%d", i), "1")
		}
		b.ReportAllocs()
		b.RunParallel(func(pb *testing.PB) {
			c := cs[int(next.Add(1)-1)%len(cs)]
			for pb.Next() {
				c.Add(1)
			}
		})
	})
}

func BenchmarkHistogramRecordParallel(b *testing.B) {
	each(b, func(b *testing.B, p *shmdirect.Provider) {
		h, _ := p.Float64Histogram("bench", "h", "ms", nil)
		b.ReportAllocs()
		b.RunParallel(func(pb *testing.PB) {
			i := 0
			for pb.Next() {
				h.Record(float64(i % 2000))
				i++
			}
		})
	})
}

func BenchmarkHistogramRecord(b *testing.B) {
	each(b, func(b *testing.B, p *shmdirect.Provider) {
		h, _ := p.Float64Histogram("bench", "h", "ms", nil)
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
