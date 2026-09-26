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
	"go.opentelemetry.io/collector/pdata/ptrace"

	d "otlpgen/otlptracesdirect"

	cb "otlpdagr/commonbridge"
)

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

// BuildAppend appends the encoded batch to dst and returns the extended slice — the Go
// analogue of Rust's write_into. An exporter that keeps one Builder and one dst reuses
// both, so the serialize step allocates nothing at steady state.
func (w *Builder) BuildAppend(dst []byte, td ptrace.Traces) []byte {
	return w.b.BuildAppend(dst, toValue(td))
}

// BuildValueAppend is BuildAppend against an already-built value tree, so a benchmark can
// separate the serializer from this bridge's tree construction.
func (w *Builder) BuildValueAppend(dst []byte, v d.TracesData) []byte {
	return w.b.BuildAppend(dst, v)
}

func toValue(td ptrace.Traces) d.TracesData {
	rss := td.ResourceSpans()
	outRS := make([]d.ResourceSpans, 0, rss.Len())
	for i := 0; i < rss.Len(); i++ {
		rs := rss.At(i)

		res := cb.Resource(rs.Resource())
		sss := rs.ScopeSpans()
		outSS := make([]d.ScopeSpans, 0, sss.Len())
		for j := 0; j < sss.Len(); j++ {
			ss := sss.At(j)
			sc := cb.Scope(ss.Scope())
			sps := ss.Spans()
			outSp := make([]d.Span, 0, sps.Len())
			for k := 0; k < sps.Len(); k++ {
				outSp = append(outSp, toSpan(sps.At(k)))
			}
			outSS = append(outSS, d.ScopeSpans{
				Scope:     cb.Some(sc),
				Spans:     cb.OptSlice(outSp),
				SchemaURL: cb.NonEmpty(ss.SchemaUrl()),
			})
		}
		outRS = append(outRS, d.ResourceSpans{
			Resource:   cb.Some(res),
			ScopeSpans: cb.OptSlice(outSS),
			SchemaURL:  cb.NonEmpty(rs.SchemaUrl()),
		})
	}
	return d.TracesData{ResourceSpans: cb.OptSlice(outRS)}
}

func toSpan(s ptrace.Span) d.Span {
	out := d.Span{
		Name:              cb.NonEmpty(s.Name()),
		Kind:              cb.Some(d.SpanKind(s.Kind())),
		StartTimeUnixNano: cb.NonZero(uint64(s.StartTimestamp())),
		EndTimeUnixNano:   cb.NonZero(uint64(s.EndTimestamp())),
		Attributes:        cb.OptSlice(cb.KeyValues(s.Attributes())),
		TraceState:        cb.NonEmpty(s.TraceState().AsRaw()),
	}
	tid := s.TraceID()
	if !tid.IsEmpty() {
		out.TraceID = cb.Some(append([]byte(nil), tid[:]...))
	}
	sid := s.SpanID()
	if !sid.IsEmpty() {
		out.SpanID = cb.Some(append([]byte(nil), sid[:]...))
	}
	psid := s.ParentSpanID()
	if !psid.IsEmpty() {
		out.ParentSpanID = cb.Some(append([]byte(nil), psid[:]...))
	}
	if f := s.Flags(); f != 0 {
		out.Flags = cb.Some(f)
	}
	if n := s.DroppedAttributesCount(); n != 0 {
		out.DroppedAttributesCount = cb.Some(n)
	}
	if n := s.DroppedEventsCount(); n != 0 {
		out.DroppedEventsCount = cb.Some(n)
	}
	if n := s.DroppedLinksCount(); n != 0 {
		out.DroppedLinksCount = cb.Some(n)
	}

	evs := s.Events()
	if evs.Len() > 0 {
		out.Events = cb.Some(make([]d.Event, 0, evs.Len()))
		lst := out.Events.V
		for i := 0; i < evs.Len(); i++ {
			e := evs.At(i)
			lst = append(lst, d.Event{
				TimeUnixNano:           cb.NonZero(uint64(e.Timestamp())),
				Name:                   cb.NonEmpty(e.Name()),
				Attributes:             cb.OptSlice(cb.KeyValues(e.Attributes())),
				DroppedAttributesCount: cb.NonZeroU32(e.DroppedAttributesCount()),
			})
		}
		out.Events = cb.Some(lst)
	}

	lks := s.Links()
	if lks.Len() > 0 {
		lst := make([]d.Link, 0, lks.Len())
		for i := 0; i < lks.Len(); i++ {
			l := lks.At(i)
			lk := d.Link{
				TraceState:             cb.NonEmpty(l.TraceState().AsRaw()),
				Attributes:             cb.OptSlice(cb.KeyValues(l.Attributes())),
				DroppedAttributesCount: cb.NonZeroU32(l.DroppedAttributesCount()),
			}
			ltid := l.TraceID()
			if !ltid.IsEmpty() {
				lk.TraceID = cb.Some(append([]byte(nil), ltid[:]...))
			}
			lsid := l.SpanID()
			if !lsid.IsEmpty() {
				lk.SpanID = cb.Some(append([]byte(nil), lsid[:]...))
			}
			if f := l.Flags(); f != 0 {
				lk.Flags = cb.Some(f)
			}
			lst = append(lst, lk)
		}
		out.Links = cb.OptSlice(lst)
	}

	st := s.Status()
	if st.Code() != ptrace.StatusCodeUnset || st.Message() != "" {
		out.Status = cb.Some(d.Status{
			Message: cb.NonEmpty(st.Message()),
			Code:    cb.Some(d.StatusCode(st.Code())),
		})
	}
	return out
}
