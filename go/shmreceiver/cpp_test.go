//go:build unix

package dagrshmreceiver

// A C++ app's direct instruments (cpp/shmdirect, spec/38 §10.3.1), read by this receiver
// unchanged: the C++ process creates the two region files from the generated C++ overlays,
// records into them, and stays alive while the receiver polls. `cpp/check.sh` builds the
// producer and runs this; without OTLPDAGR_CPP_DEMO it skips.

import (
	"bufio"
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	mv "otlpgen/metricsvalues"

	"otlpdagr/shm"
	"otlpdagr/shmdirect"
)

func TestCppProducer(t *testing.T) {
	demo := os.Getenv("OTLPDAGR_CPP_DEMO")
	if demo == "" {
		t.Skip("OTLPDAGR_CPP_DEMO not set — run cpp/check.sh")
	}
	dir := t.TempDir()
	cmd := exec.Command(demo, dir)
	cmd.Stderr = os.Stderr
	stdin, _ := cmd.StdinPipe()
	stdout, _ := cmd.StdoutPipe()
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	lines := bufio.NewScanner(stdout)
	ready := func() {
		t.Helper()
		if !lines.Scan() || lines.Text() != "ready" {
			cmd.Process.Kill()
			t.Fatalf("the C++ producer did not get ready: %q", lines.Text())
		}
	}
	ready()

	// the values file: 4 copies, the header Go's reader checks, and the 8 threads'
	// recordings spread over more than one copy
	vm, err := shm.OpenMapping(filepath.Join(dir, "cpp.dagrshm.values"), mv.ByteSize)
	if err != nil {
		t.Fatal(err)
	}
	copies, err := shmdirect.Values(vm.Mem)
	if err != nil || len(copies) != 4 {
		t.Fatalf("copies %d: %v", len(copies), err)
	}
	used := 0
	for _, v := range copies {
		if v.Points(5).IntValue() > 0 { // stream 5: stress/hits
			used++
		}
	}
	vm.Close()
	if used < 2 {
		t.Fatalf("8 threads recorded into %d of 4 copies", used)
	}

	r, sink := newReceiver(dir)
	defer r.Shutdown(context.Background())
	md := lastForward(t, r, sink)
	res := md.ResourceMetrics().At(0)
	if v, _ := res.Resource().Attributes().Get("service.name"); v.Str() != "cppapp" {
		t.Fatalf("service %q", v.Str())
	}
	if v, _ := res.Resource().Attributes().Get("host.name"); v.Str() != "h1" {
		t.Fatal("string resource attribute")
	}
	if v, _ := res.Resource().Attributes().Get("process.pid"); v.Int() != int64(cmd.Process.Pid) {
		t.Fatalf("int resource attribute %d, want %d", v.Int(), cmd.Process.Pid)
	}
	reqs := find(t, md, "http.requests")
	dp := reqs.Sum().DataPoints().At(0)
	if dp.IntValue() != 12 || !reqs.Sum().IsMonotonic() || reqs.Unit() != "{request}" {
		t.Fatalf("counter %d", dp.IntValue())
	}
	if code, _ := dp.Attributes().Get("code"); code.Int() != 200 {
		t.Fatal("int attribute")
	}
	if route, _ := dp.Attributes().Get("route"); route.Str() != "/pay" {
		t.Fatal("string attribute")
	}
	if find(t, md, "net.bytes").Sum().DataPoints().At(0).DoubleValue() != 1.5 {
		t.Fatal("float counter")
	}
	if d := find(t, md, "queue.depth").Sum(); d.IsMonotonic() || d.DataPoints().At(0).IntValue() != 2 {
		t.Fatal("up-down counter")
	}
	g := find(t, md, "cpu.temp").Gauge().DataPoints().At(0)
	if g.DoubleValue() != 61.5 {
		t.Fatal("gauge")
	}
	if s, _ := g.Attributes().Get("sensor"); s.Double() != 2.5 {
		t.Fatal("double attribute")
	}
	if ok, _ := g.Attributes().Get("ok"); !ok.Bool() {
		t.Fatal("bool attribute")
	}
	h := find(t, md, "http.duration").Histogram().DataPoints().At(0)
	if h.Count() != 5 || h.Sum() != 12987 || h.Min() != 3 || h.Max() != 12000 ||
		h.BucketCounts().Len() != 16 || h.BucketCounts().At(1) != 1 || h.BucketCounts().At(4) != 2 ||
		h.BucketCounts().At(15) != 1 || h.ExplicitBounds().Len() != 15 {
		t.Fatalf("histogram count %d sum %v buckets %v", h.Count(), h.Sum(), h.BucketCounts().AsRaw())
	}
	// 8 C++ threads on one stream, two per copy: nothing lost
	const total = 8 * 100000
	if n := find(t, md, "hits").Sum().DataPoints().At(0).IntValue(); n != total {
		t.Fatalf("counter %d, want %d", n, total)
	}
	hd := find(t, md, "lat").Histogram().DataPoints().At(0)
	var inBuckets uint64
	for i := 0; i < hd.BucketCounts().Len(); i++ {
		inBuckets += hd.BucketCounts().At(i)
	}
	if hd.Count() != total || inBuckets != total || hd.Min() != 0 || hd.Max() != 199 ||
		hd.BucketCounts().At(0) != 8*11*500 { // 0..10 of every 200
		t.Fatalf("histogram count %d buckets %v min %v max %v", hd.Count(), hd.BucketCounts().AsRaw(), hd.Min(), hd.Max())
	}

	// a scrape of a live C++ process: the next poll carries its new total
	stdin.Write([]byte("more\n"))
	ready()
	if n := find(t, lastForward(t, r, sink), "http.requests").Sum().DataPoints().At(0).IntValue(); n != 13 {
		t.Fatalf("second poll %d, want 13", n)
	}

	// the producer's shutdown removes both files
	stdin.Close()
	if err := cmd.Wait(); err != nil {
		t.Fatal(err)
	}
	for _, f := range []string{"cpp.dagrshm", "cpp.dagrshm.values"} {
		if _, err := os.Stat(filepath.Join(dir, f)); !os.IsNotExist(err) {
			t.Fatalf("%s left behind", f)
		}
	}
}
