//go:build unix

package dagrshmreceiver

// Direct instruments (package shmdirect): streams registered once, updated with atomic
// operations in shared memory, read by the receiver through their descriptors.

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"testing"

	"go.opentelemetry.io/collector/pdata/pmetric"

	mv "otlpgen/metricsvalues"

	"otlpdagr/shm"
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
	// the recordings went to their Ps' copies, not all to one (spec/38 §7.1.1)
	if runtime.GOMAXPROCS(0) > 1 {
		vm, err := shm.OpenMapping(filepath.Join(dir, "direct.dagrshm.values"), mv.ByteSize)
		if err != nil {
			t.Fatal(err)
		}
		copies, _ := shmdirect.Values(vm.Mem)
		used := 0
		for _, v := range copies {
			if v.Points(0).IntValue() > 0 {
				used++
			}
		}
		vm.Close()
		if used < 2 {
			t.Fatalf("%d goroutines recorded into %d of %d copies", workers, used, len(copies))
		}
		t.Logf("%d goroutines recorded into %d of %d copies", workers, used, len(copies))
	}
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

// Per-core copies (spec/38 §7.1.1): the values file holds one region per P, and the
// receiver merges them. Which copy a recording lands in depends on the scheduler, so this
// writes into the copies by hand: sums add up, min / max are the extremes, a gauge is copy
// 0's alone.
func TestDirectCopiesMerge(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "direct.dagrshm")
	p, err := shmdirect.NewProviderWith(path, "direct", shmdirect.Options{Shards: 4})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Shutdown()
	if p.Shards() != 4 {
		t.Fatalf("%d copies, want 4", p.Shards())
	}
	reqs, _ := p.Int64Counter("s", "hits", "1")
	bytes, _ := p.Float64Counter("s", "bytes", "By")
	temp, _ := p.Float64Gauge("s", "temp", "Cel")
	lat, _ := p.Float64Histogram("s", "lat", "ms", []float64{10, 100})
	reqs.Add(7)
	bytes.Add(1.5)
	temp.Record(20)
	lat.Record(50)

	vm, err := shm.OpenMapping(path+".values", mv.ByteSize)
	if err != nil {
		t.Fatal(err)
	}
	defer vm.Close()
	copies, err := shmdirect.Values(vm.Mem)
	if err != nil || len(copies) != 4 {
		t.Fatalf("copies %d: %v", len(copies), err)
	}
	for k := 1; k < 4; k++ {
		copies[k].Points(0).AddIntValue(int64(k))      // hits: 7 + 1 + 2 + 3
		copies[k].Points(1).AddDoubleValue(float64(k)) // bytes: 1.5 + 6
		copies[k].Points(2).SetDoubleValue(99)         // a gauge's other copies are ignored
	}
	h := copies[3].Points(3) // two more values, both beyond the extremes so far
	for _, x := range []float64{-4, 1000} {
		b := 0
		if x > 100 {
			b = 2
		}
		h.AddBuckets(b, 1)
		h.AddCount(1)
		h.AddDoubleValue(x)
		h.SetMin(min(h.Min(), x))
		h.SetMax(max(h.Max(), x))
	}

	r, sink := newReceiver(dir)
	defer r.Shutdown(context.Background())
	md := lastForward(t, r, sink)
	if n := find(t, md, "hits").Sum().DataPoints().At(0).IntValue(); n != 13 {
		t.Fatalf("counter %d, want 13", n)
	}
	if x := find(t, md, "bytes").Sum().DataPoints().At(0).DoubleValue(); x != 7.5 {
		t.Fatalf("float counter %v, want 7.5", x)
	}
	if x := find(t, md, "temp").Gauge().DataPoints().At(0).DoubleValue(); x != 20 {
		t.Fatalf("gauge %v, want copy 0's 20", x)
	}
	hd := find(t, md, "lat").Histogram().DataPoints().At(0)
	if hd.Count() != 3 || hd.Sum() != 1046 || hd.Min() != -4 || hd.Max() != 1000 ||
		hd.BucketCounts().At(0) != 1 || hd.BucketCounts().At(1) != 1 || hd.BucketCounts().At(2) != 1 {
		t.Fatalf("histogram count %d sum %v min %v max %v buckets %v",
			hd.Count(), hd.Sum(), hd.Min(), hd.Max(), hd.BucketCounts().AsRaw())
	}
}

// The default is one copy per P, each on its own 128-byte boundary; a reader maps the
// file without resizing it.
func TestDirectCopiesShape(t *testing.T) {
	dir := t.TempDir()
	p := directProvider(t, dir)
	defer p.Shutdown()
	want := min(runtime.GOMAXPROCS(0), shmdirect.MaxShards)
	if p.Shards() != want {
		t.Fatalf("%d copies, want GOMAXPROCS %d", p.Shards(), want)
	}
	if shmdirect.Stride%128 != 0 || shmdirect.Stride < mv.ByteSize {
		t.Fatalf("stride %d", shmdirect.Stride)
	}
	values := filepath.Join(dir, "direct.dagrshm.values")
	r, sink := newReceiver(dir)
	defer r.Shutdown(context.Background())
	lastForward(t, r, sink)
	if st, err := os.Stat(values); err != nil || st.Size() != int64(want*shmdirect.Stride) {
		t.Fatalf("values file %v bytes after the receiver attached, want %d", st.Size(), want*shmdirect.Stride)
	}
}

// A values file whose copies disagree about their count is not read as values.
func TestDirectCopiesForeignHeader(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "direct.dagrshm")
	p, err := shmdirect.NewProviderWith(path, "direct", shmdirect.Options{Shards: 3})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Shutdown()
	vm, err := shm.OpenMapping(path+".values", mv.ByteSize)
	if err != nil {
		t.Fatal(err)
	}
	defer vm.Close()
	mv.Wrap(vm.Mem[2*shmdirect.Stride:]).SetShards(2)
	if _, err := shmdirect.Values(vm.Mem); err == nil {
		t.Fatal("a copy with a different count was accepted")
	}
	r, _ := newReceiver(dir)
	defer r.Shutdown(context.Background())
	reg, err := r.attach(path)
	if err != nil {
		t.Fatal(err)
	}
	if reg.vals != nil {
		t.Fatal("the receiver took a foreign values file")
	}
}
