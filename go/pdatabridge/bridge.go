// Package pdatabridge converts between the OpenTelemetry Collector's in-memory
// representation (`pdata`) and an OTLP-on-Dagr buffer.
//
// Encode (pdata -> bytes) goes through the DIRECT BUILDER (Dagr spec 33): it walks
// the borrowed ptrace accessors straight to bytes with no arena, which is the path
// that is ~1.9x faster than protobuf marshalling. This is the direction that matters.
//
// Decode (bytes -> pdata) exists for compatibility only — so a Dagr-speaking receiver
// can hand off to an unmodified pipeline — and is EXPECTED TO LOSE to a protobuf
// unmarshal, because it materialises everything (spec/38 §0.1, §4). The point of
// otlp-dagr is to read the buffer in place and never call this.
package pdatabridge

import (
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"otlptraces/dagr"
	d "otlptraces/otlptracesdirect"
)

// Every field of a generated value struct is a dagr.Opt[T]: an absent field is not
// written at all, which is how the Dagr side stays byte-comparable with protobuf's
// proto3-default elision. These three helpers are the only place that decision lives.

func some[T any](v T) dagr.Opt[T] { return dagr.Some(v) }

func nonEmpty(v string) dagr.Opt[string] {
	if v == "" {
		return dagr.Opt[string]{}
	}
	return dagr.Some(v)
}

func nonZero(v uint64) dagr.Opt[uint64] {
	if v == 0 {
		return dagr.Opt[uint64]{}
	}
	return dagr.Some(v)
}

func nonZeroU32(v uint32) dagr.Opt[uint32] {
	if v == 0 {
		return dagr.Opt[uint32]{}
	}
	return dagr.Some(v)
}

func optSlice[T any](s []T) dagr.Opt[[]T] {
	if len(s) == 0 {
		return dagr.Opt[[]T]{}
	}
	return dagr.Some(s)
}

// ── encode: pdata -> Dagr bytes (direct builder, no arena) ───────────────────

func attrValue(v pcommon.Value) (d.AnyValueValue, bool) {
	switch v.Type() {
	case pcommon.ValueTypeStr:
		return d.AnyValueStringValue(v.Str()), true
	case pcommon.ValueTypeInt:
		return d.AnyValueIntValue(v.Int()), true
	case pcommon.ValueTypeDouble:
		return d.AnyValueDoubleValue(v.Double()), true
	case pcommon.ValueTypeBool:
		return d.AnyValueBoolValue(v.Bool()), true
	case pcommon.ValueTypeBytes:
		return d.AnyValueBytesValue(v.Bytes().AsRaw()), true
	default:
		// Slice / Map would need ArrayValue / KeyValueList; the seeded workload has
		// neither, and silently dropping one would make a round-trip gate lie.
		return d.AnyValueValue{}, false
	}
}

func keyValues(m pcommon.Map) []d.KeyValue {
	if m.Len() == 0 {
		return nil
	}
	out := make([]d.KeyValue, 0, m.Len())
	m.Range(func(k string, v pcommon.Value) bool {
		if av, ok := attrValue(v); ok {
			out = append(out, d.KeyValue{Key: nonEmpty(k), Value: some(av)})
		}
		return true
	})
	return out
}

// ToBytes serializes td into an OTLP-on-Dagr buffer via the direct builder.
func ToBytes(td ptrace.Traces) []byte {
	return d.BuildOtlpTraces(ToValue(td))
}

// ToValue materialises the intermediate value tree the direct builder consumes, and
// BuildValue serializes one. They are split out so a benchmark can separate the two
// halves of ToBytes: the Dagr serializer proper (BuildValue) from this bridge's tree
// construction (ToValue), which is pure overhead that a pdata-walking direct builder
// would not pay. The Dagr repo's Rust benchmark times only the BuildValue half.
func ToValue(td ptrace.Traces) d.TracesData { return toValue(td) }

func BuildValue(v d.TracesData) []byte { return d.BuildOtlpTraces(v) }

// Builder is the reusable form: one scratch buffer, reset per Build.
type Builder struct{ b *d.OtlpTracesBuilder }

func NewBuilder() *Builder { return &Builder{b: d.NewOtlpTracesBuilder()} }

func (w *Builder) Build(td ptrace.Traces) []byte { return w.b.Build(toValue(td)) }

func toValue(td ptrace.Traces) d.TracesData {
	rss := td.ResourceSpans()
	outRS := make([]d.ResourceSpans, 0, rss.Len())
	for i := 0; i < rss.Len(); i++ {
		rs := rss.At(i)

		res := d.Resource{Attributes: optSlice(keyValues(rs.Resource().Attributes()))}
		sss := rs.ScopeSpans()
		outSS := make([]d.ScopeSpans, 0, sss.Len())
		for j := 0; j < sss.Len(); j++ {
			ss := sss.At(j)
			sc := d.InstrumentationScope{
				Name:    nonEmpty(ss.Scope().Name()),
				Version: nonEmpty(ss.Scope().Version()),
			}
			sps := ss.Spans()
			outSp := make([]d.Span, 0, sps.Len())
			for k := 0; k < sps.Len(); k++ {
				outSp = append(outSp, toSpan(sps.At(k)))
			}
			outSS = append(outSS, d.ScopeSpans{
				Scope:     some(sc),
				Spans:     optSlice(outSp),
				SchemaURL: nonEmpty(ss.SchemaUrl()),
			})
		}
		outRS = append(outRS, d.ResourceSpans{
			Resource:   some(res),
			ScopeSpans: optSlice(outSS),
			SchemaURL:  nonEmpty(rs.SchemaUrl()),
		})
	}
	return d.TracesData{ResourceSpans: optSlice(outRS)}
}

func toSpan(s ptrace.Span) d.Span {
	out := d.Span{
		Name:              nonEmpty(s.Name()),
		Kind:              some(d.SpanKind(s.Kind())),
		StartTimeUnixNano: nonZero(uint64(s.StartTimestamp())),
		EndTimeUnixNano:   nonZero(uint64(s.EndTimestamp())),
		Attributes:        optSlice(keyValues(s.Attributes())),
		TraceState:        nonEmpty(s.TraceState().AsRaw()),
	}
	tid := s.TraceID()
	if !tid.IsEmpty() {
		out.TraceID = some(append([]byte(nil), tid[:]...))
	}
	sid := s.SpanID()
	if !sid.IsEmpty() {
		out.SpanID = some(append([]byte(nil), sid[:]...))
	}
	psid := s.ParentSpanID()
	if !psid.IsEmpty() {
		out.ParentSpanID = some(append([]byte(nil), psid[:]...))
	}
	if f := s.Flags(); f != 0 {
		out.Flags = some(f)
	}
	if n := s.DroppedAttributesCount(); n != 0 {
		out.DroppedAttributesCount = some(n)
	}
	if n := s.DroppedEventsCount(); n != 0 {
		out.DroppedEventsCount = some(n)
	}
	if n := s.DroppedLinksCount(); n != 0 {
		out.DroppedLinksCount = some(n)
	}

	evs := s.Events()
	if evs.Len() > 0 {
		out.Events = some(make([]d.Event, 0, evs.Len()))
		lst := out.Events.V
		for i := 0; i < evs.Len(); i++ {
			e := evs.At(i)
			lst = append(lst, d.Event{
				TimeUnixNano:           nonZero(uint64(e.Timestamp())),
				Name:                   nonEmpty(e.Name()),
				Attributes:             optSlice(keyValues(e.Attributes())),
				DroppedAttributesCount: nonZeroU32(e.DroppedAttributesCount()),
			})
		}
		out.Events = some(lst)
	}

	lks := s.Links()
	if lks.Len() > 0 {
		lst := make([]d.Link, 0, lks.Len())
		for i := 0; i < lks.Len(); i++ {
			l := lks.At(i)
			lk := d.Link{
				TraceState:             nonEmpty(l.TraceState().AsRaw()),
				Attributes:             optSlice(keyValues(l.Attributes())),
				DroppedAttributesCount: nonZeroU32(l.DroppedAttributesCount()),
			}
			ltid := l.TraceID()
			if !ltid.IsEmpty() {
				lk.TraceID = some(append([]byte(nil), ltid[:]...))
			}
			lsid := l.SpanID()
			if !lsid.IsEmpty() {
				lk.SpanID = some(append([]byte(nil), lsid[:]...))
			}
			if f := l.Flags(); f != 0 {
				lk.Flags = some(f)
			}
			lst = append(lst, lk)
		}
		out.Links = optSlice(lst)
	}

	st := s.Status()
	if st.Code() != ptrace.StatusCodeUnset || st.Message() != "" {
		out.Status = some(d.Status{
			Message: nonEmpty(st.Message()),
			Code:    some(d.StatusCode(st.Code())),
		})
	}
	return out
}
