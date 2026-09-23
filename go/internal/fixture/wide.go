package fixture

// wide.go — one "wide" batch per signal: every field pdata can express set to a
// non-default value, every union variant used, and the awkward shapes present (an empty
// element inside an array, a present-but-empty value, a proto3 `optional` that is present
// AND zero). A narrow corpus is how two bugs shipped through a byte-identical round trip:
// an absent field round-trips perfectly, so it gates nothing. The companion gate,
// internal/coverage, proves each of these batches reaches every field — it is not
// trusted on inspection.
//
// Kept out of `seed`, which stays a byte-for-byte port of the Rust benchmark's corpus.

import (
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/pprofile"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

// A wall-clock nanosecond timestamp (2026-09-23), well above `raw`'s 2^49 threshold.
const t0 = uint64(1_790_000_000_000_000_000)

var (
	tid = pcommon.TraceID{0x5b, 0x8e, 0xff, 0xf7, 0x98, 0x03, 0x81, 0x03, 0xd2, 0x69, 0xb6, 0x33, 0x81, 0x3f, 0xc6, 0x0c}
	sid = pcommon.SpanID{0xee, 0xe1, 0x9b, 0x7e, 0xc3, 0xc1, 0xb1, 0x74}
	pid = pcommon.SpanID{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08}
)

// allKinds puts one attribute of every AnyValue kind, nested ones included.
func allKinds(m pcommon.Map) {
	m.PutStr("str", "value")
	m.PutInt("int", -42)
	m.PutDouble("double", 3.25)
	m.PutBool("bool", true)
	m.PutEmptyBytes("bytes").FromRaw([]byte{0xde, 0xad, 0xbe, 0xef})
	s := m.PutEmptySlice("slice")
	s.AppendEmpty().SetStr("a")
	s.AppendEmpty() // an empty element keeps its position
	s.AppendEmpty().SetInt(7)
	s.AppendEmpty().SetEmptySlice().AppendEmpty().SetDouble(0.5)
	kv := m.PutEmptyMap("map")
	kv.PutStr("k", "v")
	kv.PutEmpty("absent")
	kv.PutEmptyMap("inner").PutBool("deep", false)
	m.PutEmpty("empty")
}

func wideResource(r pcommon.Resource) {
	allKinds(r.Attributes())
	r.SetDroppedAttributesCount(3)
}

func wideScope(s pcommon.InstrumentationScope) {
	s.SetName("io.opentelemetry.contrib.wide")
	s.SetVersion("1.2.3")
	s.Attributes().PutStr("scope.attr", "x")
	s.SetDroppedAttributesCount(1)
}

// TracesWide sets every field of the traces signal.
func TracesWide() ptrace.Traces {
	td := ptrace.NewTraces()
	rs := td.ResourceSpans().AppendEmpty()
	wideResource(rs.Resource())
	rs.SetSchemaUrl("https://opentelemetry.io/schemas/1.26.0")
	ss := rs.ScopeSpans().AppendEmpty()
	wideScope(ss.Scope())
	ss.SetSchemaUrl("https://opentelemetry.io/schemas/1.25.0")

	sp := ss.Spans().AppendEmpty()
	sp.SetTraceID(tid)
	sp.SetSpanID(sid)
	sp.TraceState().FromRaw("vendor=abc")
	sp.SetParentSpanID(pid)
	sp.SetFlags(0x101)
	sp.SetName("GET /wide")
	sp.SetKind(ptrace.SpanKindServer)
	sp.SetStartTimestamp(pcommon.Timestamp(t0))
	sp.SetEndTimestamp(pcommon.Timestamp(t0 + 1_500_000))
	allKinds(sp.Attributes())
	sp.SetDroppedAttributesCount(2)
	ev := sp.Events().AppendEmpty()
	ev.SetTimestamp(pcommon.Timestamp(t0 + 10))
	ev.SetName("exception")
	ev.Attributes().PutStr("exception.type", "Boom")
	ev.SetDroppedAttributesCount(4)
	sp.SetDroppedEventsCount(5)
	lk := sp.Links().AppendEmpty()
	lk.SetTraceID(tid)
	lk.SetSpanID(pid)
	lk.TraceState().FromRaw("other=1")
	lk.Attributes().PutInt("link.n", 1)
	lk.SetDroppedAttributesCount(6)
	lk.SetFlags(0x100)
	sp.SetDroppedLinksCount(7)
	sp.Status().SetCode(ptrace.StatusCodeError)
	sp.Status().SetMessage("it broke")
	return td
}

// MetricsWide sets every field of the metrics signal: one metric of each data kind, both
// NumberValue variants, both exemplar value kinds, and histogram sums that are PRESENT
// and 0.0 alongside ones that are absent.
func MetricsWide() pmetric.Metrics {
	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	wideResource(rm.Resource())
	rm.SetSchemaUrl("https://opentelemetry.io/schemas/1.26.0")
	sm := rm.ScopeMetrics().AppendEmpty()
	wideScope(sm.Scope())
	sm.SetSchemaUrl("https://opentelemetry.io/schemas/1.25.0")
	ms := sm.Metrics()

	exemplars := func(es pmetric.ExemplarSlice) {
		e := es.AppendEmpty()
		e.FilteredAttributes().PutStr("user", "u1")
		e.SetTimestamp(pcommon.Timestamp(t0 + 5))
		e.SetSpanID(sid)
		e.SetTraceID(tid)
		e.SetDoubleValue(12.5)
		es.AppendEmpty().SetIntValue(-3)
	}

	g := ms.AppendEmpty()
	g.SetName("system.cpu.utilization")
	g.SetDescription("CPU utilization")
	g.SetUnit("1")
	g.Metadata().PutStr("meta", "data")
	gp := g.SetEmptyGauge().DataPoints()
	p := gp.AppendEmpty()
	allKinds(p.Attributes())
	p.SetStartTimestamp(pcommon.Timestamp(t0 - 60_000_000_000))
	p.SetTimestamp(pcommon.Timestamp(t0))
	p.SetDoubleValue(0.75)
	p.SetFlags(pmetric.DefaultDataPointFlags.WithNoRecordedValue(true))
	exemplars(p.Exemplars())
	gp.AppendEmpty().SetIntValue(0) // an int 0 is PRESENT: the oneof's type is the data

	s := ms.AppendEmpty()
	s.SetName("http.server.requests")
	sum := s.SetEmptySum()
	sum.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
	sum.SetIsMonotonic(true)
	sp := sum.DataPoints().AppendEmpty()
	sp.SetTimestamp(pcommon.Timestamp(t0))
	sp.SetIntValue(1234)

	h := ms.AppendEmpty()
	h.SetName("http.server.duration")
	hist := h.SetEmptyHistogram()
	hist.SetAggregationTemporality(pmetric.AggregationTemporalityDelta)
	hp := hist.DataPoints().AppendEmpty()
	hp.Attributes().PutStr("route", "/wide")
	hp.SetStartTimestamp(pcommon.Timestamp(t0 - 10_000_000_000))
	hp.SetTimestamp(pcommon.Timestamp(t0))
	hp.SetCount(17)
	hp.SetSum(0) // present and zero — must not collapse into "absent"
	hp.BucketCounts().FromRaw([]uint64{1, 0, 6, 10})
	hp.ExplicitBounds().FromRaw([]float64{5, 10, 25})
	hp.SetMin(0.25)
	hp.SetMax(40)
	hp.SetFlags(pmetric.DefaultDataPointFlags.WithNoRecordedValue(true))
	exemplars(hp.Exemplars())
	hist.DataPoints().AppendEmpty().SetCount(1) // no sum/min/max at all

	e := ms.AppendEmpty()
	e.SetName("rpc.latency")
	eh := e.SetEmptyExponentialHistogram()
	eh.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
	ep := eh.DataPoints().AppendEmpty()
	ep.Attributes().PutBool("ok", true)
	ep.SetStartTimestamp(pcommon.Timestamp(t0 - 1))
	ep.SetTimestamp(pcommon.Timestamp(t0))
	ep.SetCount(30)
	ep.SetSum(-12.5)
	ep.SetScale(-3)
	ep.SetZeroCount(2)
	ep.Positive().SetOffset(-2)
	ep.Positive().BucketCounts().FromRaw([]uint64{4, 5, 6})
	ep.Negative().SetOffset(1)
	ep.Negative().BucketCounts().FromRaw([]uint64{7})
	ep.SetFlags(pmetric.DefaultDataPointFlags.WithNoRecordedValue(true))
	exemplars(ep.Exemplars())
	ep.SetMin(-9)
	ep.SetMax(11)
	ep.SetZeroThreshold(1e-9)

	u := ms.AppendEmpty()
	u.SetName("gc.pause")
	up := u.SetEmptySummary().DataPoints().AppendEmpty()
	up.Attributes().PutStr("gen", "young")
	up.SetStartTimestamp(pcommon.Timestamp(t0 - 2))
	up.SetTimestamp(pcommon.Timestamp(t0))
	up.SetCount(100)
	up.SetSum(250.5)
	q := up.QuantileValues().AppendEmpty()
	q.SetQuantile(0.99)
	q.SetValue(9.5)
	up.QuantileValues().AppendEmpty().SetValue(0.1) // quantile 0.0 — the minimum
	up.SetFlags(pmetric.DefaultDataPointFlags.WithNoRecordedValue(true))

	ms.AppendEmpty().SetName("no.data") // MetricTypeEmpty
	return md
}

// LogsWide sets every field of the logs signal, with a map body.
func LogsWide() plog.Logs {
	ld := plog.NewLogs()
	rl := ld.ResourceLogs().AppendEmpty()
	wideResource(rl.Resource())
	rl.SetSchemaUrl("https://opentelemetry.io/schemas/1.26.0")
	sl := rl.ScopeLogs().AppendEmpty()
	wideScope(sl.Scope())
	sl.SetSchemaUrl("https://opentelemetry.io/schemas/1.25.0")

	r := sl.LogRecords().AppendEmpty()
	r.SetTimestamp(pcommon.Timestamp(t0))
	r.SetObservedTimestamp(pcommon.Timestamp(t0 + 3))
	r.SetSeverityNumber(plog.SeverityNumberError2)
	r.SetSeverityText("ERROR")
	body := r.Body().SetEmptyMap()
	body.PutStr("msg", "disk full")
	body.PutInt("free", 0)
	allKinds(r.Attributes())
	r.SetDroppedAttributesCount(9)
	r.SetFlags(plog.DefaultLogRecordFlags.WithIsSampled(true))
	r.SetTraceID(tid)
	r.SetSpanID(sid)
	r.SetEventName("device.error")

	sl.LogRecords().AppendEmpty().Body().SetStr("plain line") // a string body
	sl.LogRecords().AppendEmpty()                             // no body at all
	return ld
}

// ProfilesWide sets every field of the profiles signal: every dictionary table populated
// with its mandatory zero-value entry 0, and one profile/sample referencing into them.
func ProfilesWide() pprofile.Profiles {
	pd := pprofile.NewProfiles()
	dc := pd.Dictionary()

	dc.StringTable().Append("", "cpu", "nanoseconds", "main", "main.go", "libc.so.6", "bytes", "thread.name")

	dc.MappingTable().AppendEmpty() // [0] zero value
	m := dc.MappingTable().AppendEmpty()
	m.SetMemoryStart(0x7f3a_1c00_0000)
	m.SetMemoryLimit(0x7f3a_1c20_0000)
	m.SetFileOffset(0x1000)
	m.SetFilenameStrindex(5)
	m.AttributeIndices().Append(1)

	dc.FunctionTable().AppendEmpty() // [0]
	f := dc.FunctionTable().AppendEmpty()
	f.SetNameStrindex(3)
	f.SetSystemNameStrindex(3)
	f.SetFilenameStrindex(4)
	f.SetStartLine(10)

	dc.LocationTable().AppendEmpty() // [0]
	l := dc.LocationTable().AppendEmpty()
	l.SetMappingIndex(1)
	l.SetAddress(0x7f3a_1c01_2345)
	ln := l.Lines().AppendEmpty()
	ln.SetFunctionIndex(1)
	ln.SetLine(42)
	ln.SetColumn(7)
	l.AttributeIndices().Append(1)

	dc.LinkTable().AppendEmpty() // [0]
	k := dc.LinkTable().AppendEmpty()
	k.SetTraceID(tid)
	k.SetSpanID(sid)

	dc.AttributeTable().AppendEmpty() // [0]
	a := dc.AttributeTable().AppendEmpty()
	a.SetKeyStrindex(7)
	a.Value().SetStr("worker-1")
	a.SetUnitStrindex(6)

	dc.StackTable().AppendEmpty() // [0]
	dc.StackTable().AppendEmpty().LocationIndices().Append(1, 1)

	rp := pd.ResourceProfiles().AppendEmpty()
	wideResource(rp.Resource())
	rp.SetSchemaUrl("https://opentelemetry.io/schemas/1.26.0")
	sp := rp.ScopeProfiles().AppendEmpty()
	wideScope(sp.Scope())
	sp.SetSchemaUrl("https://opentelemetry.io/schemas/1.25.0")

	p := sp.Profiles().AppendEmpty()
	p.SampleType().SetTypeStrindex(1)
	p.SampleType().SetUnitStrindex(2)
	s := p.Samples().AppendEmpty()
	s.SetStackIndex(1)
	s.AttributeIndices().Append(1)
	s.SetLinkIndex(1)
	s.Values().Append(10_000_000, -1)
	s.TimestampsUnixNano().Append(t0, t0+10_000_000)
	p.SetTime(pcommon.Timestamp(t0))
	p.SetDurationNano(10_000_000_000)
	p.PeriodType().SetTypeStrindex(1)
	p.PeriodType().SetUnitStrindex(2)
	p.SetPeriod(10_000_000)
	p.SetProfileID(pprofile.ProfileID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16})
	p.SetDroppedAttributesCount(1)
	p.SetOriginalPayloadFormat("pprofext")
	p.OriginalPayload().FromRaw([]byte{0x1f, 0x8b})
	p.AttributeIndices().Append(1)
	return pd
}
