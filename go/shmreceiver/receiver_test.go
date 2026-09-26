//go:build unix

package dagrshmreceiver

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	"go.uber.org/zap"

	mr "otlpgen/metricsregion"

	"otlpdagr/shm"
	"otlpdagr/shmexporter"
)

// app is an SDK MeterProvider exporting into a region under dir.
type app struct {
	mp   *sdkmetric.MeterProvider
	path string
}

func newApp(t *testing.T, dir, service string) *app {
	t.Helper()
	path := filepath.Join(dir, service+".dagrshm")
	exp, err := shmexporter.New(path, service)
	if err != nil {
		t.Fatal(err)
	}
	res := resource.NewSchemaless(attribute.String("service.name", service),
		attribute.String("host.name", "h1"), attribute.Int("replica", 3))
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithResource(res),
		sdkmetric.WithReader(sdkmetric.NewPeriodicReader(exp, sdkmetric.WithInterval(time.Hour))))
	return &app{mp: mp, path: path}
}

func (a *app) flush(t *testing.T) {
	t.Helper()
	if err := a.mp.ForceFlush(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func read(t *testing.T, path string) (mr.State, pmetric.Metrics) {
	t.Helper()
	r, err := shm.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { r.Close() })
	s, err := r.Snapshot(make([]byte, mr.ByteSize), 100)
	if err != nil {
		t.Fatal(err)
	}
	return s, ToMetrics(s)
}

func find(t *testing.T, md pmetric.Metrics, name string) pmetric.Metric {
	t.Helper()
	sms := md.ResourceMetrics().At(0).ScopeMetrics()
	for i := 0; i < sms.Len(); i++ {
		ms := sms.At(i).Metrics()
		for j := 0; j < ms.Len(); j++ {
			if ms.At(j).Name() == name {
				return ms.At(j)
			}
		}
	}
	t.Fatalf("no metric %q", name)
	return pmetric.Metric{}
}

// Every instrument kind the region carries, through the real SDK, back out as pdata.
func TestSDKRoundTrip(t *testing.T) {
	a := newApp(t, t.TempDir(), "checkout")
	ctx := context.Background()
	m := a.mp.Meter("shop/checkout")
	reqs, _ := m.Int64Counter("http.requests", metric.WithUnit("{request}"))
	inflight, _ := m.Float64UpDownCounter("queue.depth")
	temp, _ := m.Int64Gauge("cpu.temp", metric.WithUnit("Cel"))
	lat, _ := m.Float64Histogram("http.duration", metric.WithUnit("ms"))
	attrs := metric.WithAttributes(attribute.String("route", "/pay"), attribute.Int("code", 200),
		attribute.Bool("tls", true), attribute.Float64("weight", 0.5))
	reqs.Add(ctx, 7, attrs)
	reqs.Add(ctx, 5, attrs)
	inflight.Add(ctx, 2.5)
	inflight.Add(ctx, -1)
	temp.Record(ctx, 61)
	for _, v := range []float64{3, 42, 42, 900, 12000} {
		lat.Record(ctx, v)
	}
	a.flush(t)

	s, md := read(t, a.path)
	if s.Exports() != 1 || s.DroppedPoints() != 0 || s.Service() != "checkout" {
		t.Fatalf("header: exports %d dropped %d service %q", s.Exports(), s.DroppedPoints(), s.Service())
	}
	res := md.ResourceMetrics().At(0).Resource().Attributes()
	if v, _ := res.Get("host.name"); v.Str() != "h1" {
		t.Fatal("resource string attr")
	}
	if v, _ := res.Get("replica"); v.Int() != 3 {
		t.Fatal("resource int attr")
	}
	if sc := md.ResourceMetrics().At(0).ScopeMetrics().At(0).Scope().Name(); sc != "shop/checkout" {
		t.Fatalf("scope %q", sc)
	}

	sum := find(t, md, "http.requests")
	if sum.Unit() != "{request}" || !sum.Sum().IsMonotonic() ||
		sum.Sum().AggregationTemporality() != pmetric.AggregationTemporalityCumulative {
		t.Fatal("counter shape")
	}
	dp := sum.Sum().DataPoints().At(0)
	if dp.IntValue() != 12 || dp.StartTimestamp() == 0 || dp.Timestamp() < dp.StartTimestamp() {
		t.Fatalf("counter value %d", dp.IntValue())
	}
	want := map[string]any{"route": "/pay", "code": int64(200), "tls": true, "weight": 0.5}
	if got := dp.Attributes().AsRaw(); fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("typed attributes: %v", got)
	}

	ud := find(t, md, "queue.depth")
	if ud.Sum().IsMonotonic() || ud.Sum().DataPoints().At(0).DoubleValue() != 1.5 {
		t.Fatal("up-down counter")
	}
	if g := find(t, md, "cpu.temp"); g.Type() != pmetric.MetricTypeGauge ||
		g.Gauge().DataPoints().At(0).IntValue() != 61 {
		t.Fatal("gauge")
	}
	h := find(t, md, "http.duration").Histogram().DataPoints().At(0)
	if h.Count() != 5 || h.Sum() != 3+42+42+900+12000 || h.Min() != 3 || h.Max() != 12000 {
		t.Fatalf("histogram count %d sum %v", h.Count(), h.Sum())
	}
	if h.ExplicitBounds().Len() != 15 || h.BucketCounts().Len() != 16 {
		t.Fatalf("default buckets: %d bounds, %d counts", h.ExplicitBounds().Len(), h.BucketCounts().Len())
	}
	var total uint64
	for i := 0; i < h.BucketCounts().Len(); i++ {
		total += h.BucketCounts().At(i)
	}
	if total != 5 || h.BucketCounts().At(15) != 1 { // 12000 > the last bound, 10000
		t.Fatal("bucket counts")
	}

	// cumulative: a second export carries the running totals, not deltas
	reqs.Add(ctx, 1, attrs)
	a.flush(t)
	s, md = read(t, a.path)
	if s.Exports() != 2 || find(t, md, "http.requests").Sum().DataPoints().At(0).IntValue() != 13 {
		t.Fatal("second export")
	}

	// shutdown removes the region: a collector stops seeing the app
	if err := a.mp.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(a.path); !os.IsNotExist(err) {
		t.Fatal("region file left behind after shutdown")
	}
}

// More streams than the region holds: the first 512 are written, the rest counted.
func TestOverflowIsCounted(t *testing.T) {
	a := newApp(t, t.TempDir(), "wide")
	c, _ := a.mp.Meter("m").Int64Counter("hits")
	for i := 0; i < 600; i++ {
		c.Add(context.Background(), 1, metric.WithAttributes(attribute.Int("i", i)))
	}
	long, _ := a.mp.Meter("m").Int64Counter("x", metric.WithUnit("a-unit-longer-than-sixteen"))
	long.Add(context.Background(), 1)
	a.flush(t)
	s, md := read(t, a.path)
	if s.PointsLen() != 512 || s.DroppedPoints() != 600-512+1 {
		t.Fatalf("points %d dropped %d", s.PointsLen(), s.DroppedPoints())
	}
	if find(t, md, "hits").Sum().DataPoints().Len() != 512 {
		t.Fatal("points carried")
	}
}

func newReceiver(dir string) (*shmReceiver, *consumertest.MetricsSink) {
	sink := new(consumertest.MetricsSink)
	cfg := createDefaultConfig().(*Config)
	cfg.Directory = dir
	return &shmReceiver{cfg: cfg, log: zap.NewNop(), next: sink, open: map[string]*region{},
		snap: make([]byte, mr.ByteSize)}, sink
}

// The receiver forwards a region once per export, and lets go of a region whose file is
// gone.
func TestReceiverForwardsEachExportOnce(t *testing.T) {
	dir := t.TempDir()
	a := newApp(t, dir, "svc")
	c, _ := a.mp.Meter("m").Int64Counter("n")
	c.Add(context.Background(), 1)
	a.flush(t)
	r, sink := newReceiver(dir)
	defer r.Shutdown(context.Background())
	r.Poll(context.Background())
	r.Poll(context.Background())
	if len(sink.AllMetrics()) != 1 {
		t.Fatalf("forwarded %d times, want 1", len(sink.AllMetrics()))
	}
	c.Add(context.Background(), 1)
	a.flush(t)
	r.Poll(context.Background())
	if len(sink.AllMetrics()) != 2 {
		t.Fatal("new export not forwarded")
	}
	a.mp.Shutdown(context.Background())
	r.Poll(context.Background())
	if len(r.open) != 0 {
		t.Fatal("a removed region stays mapped")
	}
}

// A writer that died mid-write leaves its region busy for good: the receiver skips it —
// within its attempt bound — and still reads the healthy region next to it. A file that
// is not a region is skipped too.
func TestReceiverSkipsBusyAndForeignFiles(t *testing.T) {
	dir := t.TempDir()
	dead, err := shm.Create(filepath.Join(dir, "dead.dagrshm"), "dead")
	if err != nil {
		t.Fatal(err)
	}
	defer dead.Close()
	dead.BeginWrite() // never ended
	if err := os.WriteFile(filepath.Join(dir, "junk.dagrshm"), make([]byte, mr.ByteSize), 0o644); err != nil {
		t.Fatal(err)
	}
	a := newApp(t, dir, "alive")
	c, _ := a.mp.Meter("m").Int64Counter("n")
	c.Add(context.Background(), 1)
	a.flush(t)

	r, sink := newReceiver(dir)
	defer r.Shutdown(context.Background())
	done := make(chan struct{})
	go func() { r.Poll(context.Background()); close(done) }()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("a busy region stalled the receiver")
	}
	all := sink.AllMetrics()
	if len(all) != 1 {
		t.Fatalf("forwarded %d regions, want only the healthy one", len(all))
	}
	if v, _ := all[0].ResourceMetrics().At(0).Resource().Attributes().Get("service.name"); v.Str() != "alive" {
		t.Fatal("wrong region forwarded")
	}
}
