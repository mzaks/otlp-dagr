//go:build unix

package dagrshmreceiver

// Direct instruments (package shmdirect): streams registered once, updated with atomic
// operations in shared memory, read by the receiver through their descriptors.

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"go.opentelemetry.io/collector/pdata/pmetric"

	"otlpdagr/shmdirect"
)

func directProvider(t *testing.T, dir string) *shmdirect.Provider {
	t.Helper()
	p, err := shmdirect.NewProvider(filepath.Join(dir, "direct.dagrshm"), "direct",
		shmdirect.Attr{Key: "host.name", Value: "h1"})
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func lastForward(t *testing.T, r *shmReceiver, sink interface{ AllMetrics() []pmetric.Metrics }) pmetric.Metrics {
	t.Helper()
	r.Poll(context.Background())
	all := sink.AllMetrics()
	if len(all) == 0 {
		t.Fatal("nothing forwarded")
	}
	return all[len(all)-1]
}

func TestDirectRoundTrip(t *testing.T) {
	dir := t.TempDir()
	p := directProvider(t, dir)
	reqs, _ := p.Int64Counter("shop", "http.requests", "{request}",
		shmdirect.Attr{Key: "route", Value: "/pay"}, shmdirect.Attr{Key: "code", Value: 200})
	bytes, _ := p.Float64Counter("shop", "net.bytes", "By")
	depth, _ := p.Int64UpDownCounter("shop", "queue.depth", "1")
	temp, _ := p.Float64Gauge("shop", "cpu.temp", "Cel")
	lat, _ := p.Float64Histogram("shop", "http.duration", "ms", nil)
	reqs.Add(7)
	reqs.Add(5)
	bytes.Add(1.5)
	depth.Add(3)
	depth.Add(-1)
	temp.Record(61.5)
	for _, v := range []float64{3, 42, 42, 900, 12000} {
		lat.Record(v)
	}

	r, sink := newReceiver(dir)
	defer r.Shutdown(context.Background())
	md := lastForward(t, r, sink)
	if v, _ := md.ResourceMetrics().At(0).Resource().Attributes().Get("host.name"); v.Str() != "h1" {
		t.Fatal("resource")
	}
	dp := find(t, md, "http.requests").Sum().DataPoints().At(0)
	if dp.IntValue() != 12 || !find(t, md, "http.requests").Sum().IsMonotonic() {
		t.Fatalf("counter %d", dp.IntValue())
	}
	if code, _ := dp.Attributes().Get("code"); code.Int() != 200 {
		t.Fatal("typed attribute")
	}
	if find(t, md, "net.bytes").Sum().DataPoints().At(0).DoubleValue() != 1.5 {
		t.Fatal("float counter")
	}
	if d := find(t, md, "queue.depth").Sum(); d.IsMonotonic() || d.DataPoints().At(0).IntValue() != 2 {
		t.Fatal("up-down counter")
	}
	if find(t, md, "cpu.temp").Gauge().DataPoints().At(0).DoubleValue() != 61.5 {
		t.Fatal("gauge")
	}
	h := find(t, md, "http.duration").Histogram().DataPoints().At(0)
	if h.Count() != 5 || h.Sum() != 12987 || h.Min() != 3 || h.Max() != 12000 ||
		h.BucketCounts().Len() != 16 || h.BucketCounts().At(1) != 1 || h.BucketCounts().At(15) != 1 {
		t.Fatalf("histogram count %d sum %v buckets %v", h.Count(), h.Sum(), h.BucketCounts().AsRaw())
	}

	// a scrape: every poll forwards, carrying the running totals
	reqs.Add(1)
	if find(t, lastForward(t, r, sink), "http.requests").Sum().DataPoints().At(0).IntValue() != 13 {
		t.Fatal("second poll")
	}
	if len(sink.AllMetrics()) != 2 {
		t.Fatalf("%d forwards, want one per poll", len(sink.AllMetrics()))
	}

	if err := p.Shutdown(); err != nil {
		t.Fatal(err)
	}
	for _, f := range []string{"direct.dagrshm", "direct.dagrshm.values"} {
		if _, err := os.Stat(filepath.Join(dir, f)); !os.IsNotExist(err) {
			t.Fatalf("%s left behind", f)
		}
	}
}

// Many goroutines on one stream: nothing lost, histogram fields consistent at the end.
func TestDirectConcurrentRecording(t *testing.T) {
	dir := t.TempDir()
	p := directProvider(t, dir)
	defer p.Shutdown()
	c, _ := p.Int64Counter("s", "hits", "1")
	h, _ := p.Float64Histogram("s", "lat", "ms", []float64{10, 100})
	const workers, per = 8, 100000
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < per; i++ {
				c.Add(1)
				h.Record(float64(i % 200))
			}
		}()
	}
	wg.Wait()
	r, sink := newReceiver(dir)
	defer r.Shutdown(context.Background())
	md := lastForward(t, r, sink)
	if n := find(t, md, "hits").Sum().DataPoints().At(0).IntValue(); n != workers*per {
		t.Fatalf("counter %d, want %d", n, workers*per)
	}
	hd := find(t, md, "lat").Histogram().DataPoints().At(0)
	var total uint64
	for i := 0; i < hd.BucketCounts().Len(); i++ {
		total += hd.BucketCounts().At(i)
	}
	if hd.Count() != workers*per || total != workers*per || hd.Min() != 0 || hd.Max() != 199 {
		t.Fatalf("histogram count %d buckets %d min %v max %v", hd.Count(), total, hd.Min(), hd.Max())
	}
}
