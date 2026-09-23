package fixture

// signals.go — realistic, deterministic batches for metrics, logs and profiles, the
// benchmark counterparts of the traces seed. The wide fixtures (wide.go) prove the
// bridges are complete; these measure. Same LCG as the seed, so every run builds the
// same bytes.
//
// The shapes follow what the Collector commonly carries: host/runtime metrics with a
// handful of attribute sets per metric, application logs with a string body and trace
// context on about half of them, and a CPU profile whose dictionary is far larger than
// any single sample.

import (
	"fmt"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/pprofile"
)

type lcg struct{ s uint64 }

func (r *lcg) next() uint64 {
	r.s = r.s*6364136223846793005 + 1442695040888963407
	return r.s
}

func (r *lcg) n(k int) int { return int((r.next() >> 33) % uint64(k)) }

func resourceAttrs(m pcommon.Map) {
	m.PutStr("service.name", "checkout")
	m.PutStr("service.version", "2.14.1")
	m.PutStr("service.instance.id", "8a1c6f0e-3b0d-4f53-9a0e-6a3f2d1b7c55")
	m.PutStr("host.name", "ip-10-0-3-17.eu-west-1.compute.internal")
	m.PutStr("k8s.namespace.name", "prod")
	m.PutStr("k8s.pod.name", "checkout-7d9c8b6f5-x2k4q")
}

var routes = [...]string{"/api/v1/cart", "/api/v1/cart/{id}", "/api/v1/checkout", "/api/v1/payment", "/healthz"}
var methods = [...]string{"GET", "POST", "PUT", "DELETE"}
var codes = [...]int64{200, 201, 204, 400, 404, 500}

// MetricsBatch builds nMetrics metrics cycling through gauge / monotonic sum / histogram,
// each with dpPerMetric data points that differ in their attribute set.
func MetricsBatch(nMetrics, dpPerMetric int) pmetric.Metrics {
	r := &lcg{s: 0x6d65747269637321}
	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	resourceAttrs(rm.Resource().Attributes())
	sm := rm.ScopeMetrics().AppendEmpty()
	sm.Scope().SetName("io.opentelemetry.runtime-telemetry")
	sm.Scope().SetVersion("1.32.0")
	start := t0 - 3_600_000_000_000
	bounds := []float64{0, 5, 10, 25, 50, 75, 100, 250, 500, 1000}
	ms := sm.Metrics()
	ms.EnsureCapacity(nMetrics)
	for i := 0; i < nMetrics; i++ {
		m := ms.AppendEmpty()
		attrs := func(p pcommon.Map, j int) {
			p.PutStr("http.route", routes[(i+j)%len(routes)])
			p.PutStr("http.request.method", methods[j%len(methods)])
			p.PutInt("http.response.status_code", codes[(i*3+j)%len(codes)])
		}
		now := pcommon.Timestamp(t0 + uint64(i)*1_000_000)
		switch i % 3 {
		case 0:
			m.SetName(fmt.Sprintf("process.runtime.memory.%d", i))
			m.SetUnit("By")
			dps := m.SetEmptyGauge().DataPoints()
			for j := 0; j < dpPerMetric; j++ {
				p := dps.AppendEmpty()
				attrs(p.Attributes(), j)
				p.SetTimestamp(now)
				p.SetDoubleValue(float64(r.next()%1_000_000) / 7)
			}
		case 1:
			m.SetName(fmt.Sprintf("http.server.request.count.%d", i))
			m.SetUnit("{request}")
			s := m.SetEmptySum()
			s.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
			s.SetIsMonotonic(true)
			for j := 0; j < dpPerMetric; j++ {
				p := s.DataPoints().AppendEmpty()
				attrs(p.Attributes(), j)
				p.SetStartTimestamp(pcommon.Timestamp(start))
				p.SetTimestamp(now)
				p.SetIntValue(int64(r.next() % 100_000))
			}
		case 2:
			m.SetName(fmt.Sprintf("http.server.request.duration.%d", i))
			m.SetDescription("Duration of HTTP server requests.")
			m.SetUnit("ms")
			h := m.SetEmptyHistogram()
			h.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
			for j := 0; j < dpPerMetric; j++ {
				p := h.DataPoints().AppendEmpty()
				attrs(p.Attributes(), j)
				p.SetStartTimestamp(pcommon.Timestamp(start))
				p.SetTimestamp(now)
				p.ExplicitBounds().FromRaw(bounds)
				var count uint64
				bc := make([]uint64, len(bounds)+1)
				for k := range bc {
					bc[k] = r.next() % 400
					count += bc[k]
				}
				p.BucketCounts().FromRaw(bc)
				p.SetCount(count)
				p.SetSum(float64(count) * 42.5)
				p.SetMin(0.3)
				p.SetMax(987.25)
			}
		}
	}
	return md
}

var levels = [...]struct {
	n    plog.SeverityNumber
	text string
}{{plog.SeverityNumberInfo, "INFO"}, {plog.SeverityNumberInfo, "INFO"}, {plog.SeverityNumberDebug, "DEBUG"},
	{plog.SeverityNumberWarn, "WARN"}, {plog.SeverityNumberInfo, "INFO"}, {plog.SeverityNumberError, "ERROR"}}

// LogsBatch builds n application log records: a string body, severity, a few attributes,
// and trace context on every other record.
func LogsBatch(n int) plog.Logs {
	r := &lcg{s: 0x6c6f677321212121}
	ld := plog.NewLogs()
	rl := ld.ResourceLogs().AppendEmpty()
	resourceAttrs(rl.Resource().Attributes())
	sl := rl.ScopeLogs().AppendEmpty()
	sl.Scope().SetName("io.opentelemetry.logback-appender-1.0")
	sl.Scope().SetVersion("2.8.0-alpha")
	lrs := sl.LogRecords()
	lrs.EnsureCapacity(n)
	for i := 0; i < n; i++ {
		lr := lrs.AppendEmpty()
		ts := pcommon.Timestamp(t0 + uint64(i)*250_000)
		lr.SetTimestamp(ts)
		lr.SetObservedTimestamp(ts + 12_000)
		lv := levels[i%len(levels)]
		lr.SetSeverityNumber(lv.n)
		lr.SetSeverityText(lv.text)
		lr.Body().SetStr(fmt.Sprintf("order %d: %s %s completed in %d ms for customer %d",
			100000+r.n(900000), methods[i%len(methods)], routes[i%len(routes)], r.n(900), r.n(50000)))
		at := lr.Attributes()
		at.PutStr("code.function", "com.example.checkout.CartService.submit")
		at.PutInt("thread.id", int64(20+r.n(16)))
		at.PutStr("thread.name", fmt.Sprintf("http-nio-8080-exec-%d", 1+r.n(10)))
		if i%2 == 0 {
			var tid pcommon.TraceID
			var sid pcommon.SpanID
			for k := range tid {
				tid[k] = byte(r.next() >> 56)
			}
			for k := range sid {
				sid[k] = byte(r.next() >> 56)
			}
			lr.SetTraceID(tid)
			lr.SetSpanID(sid)
			lr.SetFlags(plog.DefaultLogRecordFlags.WithIsSampled(true))
		}
	}
	return ld
}

// ProfilesBatch builds one CPU profile with nSamples samples over a dictionary of
// nFuncs functions, one location per function, and nStacks stacks of depth 8..23.
func ProfilesBatch(nSamples, nFuncs, nStacks int) pprofile.Profiles {
	r := &lcg{s: 0x70726f66696c6521}
	pd := pprofile.NewProfiles()
	dc := pd.Dictionary()
	st := dc.StringTable()
	st.Append("", "samples", "count", "cpu", "nanoseconds", "/usr/local/bin/checkout", "thread.name")
	firstFn := st.Len()
	for i := 0; i < nFuncs; i++ {
		st.Append(fmt.Sprintf("github.com/example/checkout/internal/cart.(*Service).step%03d", i))
	}
	firstFile := st.Len()
	for i := 0; i < nFuncs/8+1; i++ {
		st.Append(fmt.Sprintf("/src/checkout/internal/cart/file%02d.go", i))
	}

	dc.MappingTable().AppendEmpty()
	mp := dc.MappingTable().AppendEmpty()
	mp.SetMemoryStart(0x5555_5555_4000)
	mp.SetMemoryLimit(0x5555_5655_4000)
	mp.SetFilenameStrindex(5)

	dc.FunctionTable().AppendEmpty()
	dc.LocationTable().AppendEmpty()
	for i := 0; i < nFuncs; i++ {
		f := dc.FunctionTable().AppendEmpty()
		f.SetNameStrindex(int32(firstFn + i))
		f.SetSystemNameStrindex(int32(firstFn + i))
		f.SetFilenameStrindex(int32(firstFile + i/8))
		f.SetStartLine(int64(10 + i*7))
		l := dc.LocationTable().AppendEmpty()
		l.SetMappingIndex(1)
		l.SetAddress(0x5555_5555_4000 + uint64(i)*0x40 + uint64(r.n(0x40)))
		ln := l.Lines().AppendEmpty()
		ln.SetFunctionIndex(int32(1 + i))
		ln.SetLine(int64(12 + i*7 + r.n(5)))
	}
	dc.LinkTable().AppendEmpty()
	dc.AttributeTable().AppendEmpty()
	for w := 0; w < 4; w++ {
		a := dc.AttributeTable().AppendEmpty()
		a.SetKeyStrindex(6)
		a.Value().SetStr(fmt.Sprintf("worker-%d", w))
	}
	dc.StackTable().AppendEmpty()
	for s := 0; s < nStacks; s++ {
		depth := 8 + r.n(16)
		li := dc.StackTable().AppendEmpty().LocationIndices()
		li.EnsureCapacity(depth)
		for d := 0; d < depth; d++ {
			li.Append(int32(1 + r.n(nFuncs)))
		}
	}

	rp := pd.ResourceProfiles().AppendEmpty()
	resourceAttrs(rp.Resource().Attributes())
	sp := rp.ScopeProfiles().AppendEmpty()
	sp.Scope().SetName("go.opentelemetry.io/ebpf-profiler")
	p := sp.Profiles().AppendEmpty()
	p.SampleType().SetTypeStrindex(3)
	p.SampleType().SetUnitStrindex(4)
	p.PeriodType().SetTypeStrindex(3)
	p.PeriodType().SetUnitStrindex(4)
	p.SetPeriod(10_000_000)
	p.SetTime(pcommon.Timestamp(t0))
	p.SetDurationNano(10_000_000_000)
	ss := p.Samples()
	ss.EnsureCapacity(nSamples)
	for i := 0; i < nSamples; i++ {
		s := ss.AppendEmpty()
		s.SetStackIndex(int32(1 + r.n(nStacks)))
		s.AttributeIndices().Append(int32(1 + r.n(4)))
		s.Values().Append(int64(10_000_000 * (1 + r.n(3))))
		s.TimestampsUnixNano().Append(t0 + uint64(i)*10_000_000)
	}
	return pd
}
