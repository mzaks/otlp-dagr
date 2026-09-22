package pdatabridge

import (
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"otlptraces/dagr"
	d "otlptraces/otlptracesdirect"
)

// Scratch builds the direct builder's value tree into storage it OWNS and REUSES, rather
// than allocating a fresh tree per batch.
//
// toValue() allocates ~11 times per span — one []KeyValue for the attributes, three
// []byte for the trace/span/parent ids, an []Event where there is one — which came to
// 5,628 allocations and 527 KB on a 500-span batch, 99.9% of everything the encode path
// allocated once Dagr's serializer stopped allocating at all.
//
// The trick that makes reuse safe is sizing FIRST. Every node's children are handed to
// the builder as a sub-slice of a shared backing array, so a mid-fill `append` that grows
// the backing would invalidate every sub-slice already handed out. A cheap counting pass
// over pdata (all Len() calls) gives the exact totals, the backings are grown once, and
// the fill then only ever writes into indices that already exist.
type Scratch struct {
	rs    []d.ResourceSpans
	ss    []d.ScopeSpans
	spans []d.Span
	evs   []d.Event
	kv    []d.KeyValue
	ids   []byte // trace / span / parent ids, 8 or 16 bytes each, sub-sliced

	// cursors into the backings during a fill
	nRS, nSS, nSpan, nEv, nKV, nID int
}

type sizes struct{ rs, ss, spans, evs, kv, idBytes int }

func measure(td ptrace.Traces) sizes {
	var z sizes
	rss := td.ResourceSpans()
	z.rs = rss.Len()
	for i := 0; i < rss.Len(); i++ {
		rs := rss.At(i)
		z.kv += rs.Resource().Attributes().Len()
		sss := rs.ScopeSpans()
		z.ss += sss.Len()
		for j := 0; j < sss.Len(); j++ {
			ss := sss.At(j)
			z.kv += ss.Scope().Attributes().Len()
			sps := ss.Spans()
			z.spans += sps.Len()
			for k := 0; k < sps.Len(); k++ {
				sp := sps.At(k)
				z.kv += sp.Attributes().Len()
				z.idBytes += 32 // trace(16) + span(8) + parent(8)
				evs := sp.Events()
				z.evs += evs.Len()
				for e := 0; e < evs.Len(); e++ {
					z.kv += evs.At(e).Attributes().Len()
				}
				lks := sp.Links()
				for l := 0; l < lks.Len(); l++ {
					z.kv += lks.At(l).Attributes().Len()
					z.idBytes += 24
				}
			}
		}
	}
	return z
}

func grow[T any](s []T, n int) []T {
	if cap(s) >= n {
		return s[:n]
	}
	return make([]T, n, n+n/4+8)
}

func (s *Scratch) reset(z sizes) {
	s.rs = grow(s.rs, z.rs)
	s.ss = grow(s.ss, z.ss)
	s.spans = grow(s.spans, z.spans)
	s.evs = grow(s.evs, z.evs)
	s.kv = grow(s.kv, z.kv)
	s.ids = grow(s.ids, z.idBytes)
	s.nRS, s.nSS, s.nSpan, s.nEv, s.nKV, s.nID = 0, 0, 0, 0, 0, 0
}

// putID copies an id into the shared backing and returns a stable sub-slice.
func (s *Scratch) putID(b []byte) []byte {
	lo := s.nID
	s.nID += copy(s.ids[lo:], b)
	return s.ids[lo:s.nID]
}

// putAttrs copies a pcommon.Map into the shared KeyValue backing.
func (s *Scratch) putAttrs(m pcommon.Map) dagr.Opt[[]d.KeyValue] {
	if m.Len() == 0 {
		return dagr.Opt[[]d.KeyValue]{}
	}
	lo := s.nKV
	m.Range(func(k string, v pcommon.Value) bool {
		av, ok := attrValue(v)
		if !ok {
			return true
		}
		s.kv[s.nKV] = d.KeyValue{Key: nonEmpty(k), Value: some(av)}
		s.nKV++
		return true
	})
	if s.nKV == lo {
		return dagr.Opt[[]d.KeyValue]{}
	}
	return dagr.Some(s.kv[lo:s.nKV])
}

// ToValueReuse fills the scratch from td and returns the root value. The returned tree
// borrows the scratch, so it is only valid until the next call on this Scratch.
func (s *Scratch) ToValueReuse(td ptrace.Traces) d.TracesData {
	s.reset(measure(td))
	rss := td.ResourceSpans()
	rsLo := s.nRS
	for i := 0; i < rss.Len(); i++ {
		src := rss.At(i)
		ssLo := s.nSS
		sss := src.ScopeSpans()
		for j := 0; j < sss.Len(); j++ {
			ssSrc := sss.At(j)
			spLo := s.nSpan
			sps := ssSrc.Spans()
			for k := 0; k < sps.Len(); k++ {
				s.spans[s.nSpan] = s.toSpan(sps.At(k))
				s.nSpan++
			}
			s.ss[s.nSS] = d.ScopeSpans{
				Scope: some(d.InstrumentationScope{
					Name:       nonEmpty(ssSrc.Scope().Name()),
					Version:    nonEmpty(ssSrc.Scope().Version()),
					Attributes: s.putAttrs(ssSrc.Scope().Attributes()),
				}),
				Spans:     optSlice(s.spans[spLo:s.nSpan]),
				SchemaURL: nonEmpty(ssSrc.SchemaUrl()),
			}
			s.nSS++
		}
		s.rs[s.nRS] = d.ResourceSpans{
			Resource:   some(d.Resource{Attributes: s.putAttrs(src.Resource().Attributes())}),
			ScopeSpans: optSlice(s.ss[ssLo:s.nSS]),
			SchemaURL:  nonEmpty(src.SchemaUrl()),
		}
		s.nRS++
	}
	return d.TracesData{ResourceSpans: optSlice(s.rs[rsLo:s.nRS])}
}

func (s *Scratch) toSpan(src ptrace.Span) d.Span {
	out := d.Span{
		Name:              nonEmpty(src.Name()),
		Kind:              some(d.SpanKind(src.Kind())),
		StartTimeUnixNano: nonZero(uint64(src.StartTimestamp())),
		EndTimeUnixNano:   nonZero(uint64(src.EndTimestamp())),
		TraceState:        nonEmpty(src.TraceState().AsRaw()),
		Attributes:        s.putAttrs(src.Attributes()),
	}
	if tid := src.TraceID(); !tid.IsEmpty() {
		out.TraceID = some(s.putID(tid[:]))
	}
	if sid := src.SpanID(); !sid.IsEmpty() {
		out.SpanID = some(s.putID(sid[:]))
	}
	if psid := src.ParentSpanID(); !psid.IsEmpty() {
		out.ParentSpanID = some(s.putID(psid[:]))
	}
	if f := src.Flags(); f != 0 {
		out.Flags = some(f)
	}
	if n := src.DroppedAttributesCount(); n != 0 {
		out.DroppedAttributesCount = some(n)
	}
	if n := src.DroppedEventsCount(); n != 0 {
		out.DroppedEventsCount = some(n)
	}
	if n := src.DroppedLinksCount(); n != 0 {
		out.DroppedLinksCount = some(n)
	}

	if evs := src.Events(); evs.Len() > 0 {
		lo := s.nEv
		for i := 0; i < evs.Len(); i++ {
			e := evs.At(i)
			s.evs[s.nEv] = d.Event{
				TimeUnixNano:           nonZero(uint64(e.Timestamp())),
				Name:                   nonEmpty(e.Name()),
				Attributes:             s.putAttrs(e.Attributes()),
				DroppedAttributesCount: nonZeroU32(e.DroppedAttributesCount()),
			}
			s.nEv++
		}
		out.Events = optSlice(s.evs[lo:s.nEv])
	}

	if st := src.Status(); st.Code() != ptrace.StatusCodeUnset || st.Message() != "" {
		out.Status = some(d.Status{
			Message: nonEmpty(st.Message()),
			Code:    some(d.StatusCode(st.Code())),
		})
	}
	return out
}

// ReuseBuilder is a Builder that keeps its value-tree scratch between batches.
type ReuseBuilder struct {
	b *d.OtlpTracesBuilder
	s Scratch
}

func NewReuseBuilder() *ReuseBuilder {
	return &ReuseBuilder{b: d.NewOtlpTracesBuilder()}
}

// BuildAppend encodes td into dst. With dst retained by the caller, steady-state encoding
// allocates nothing at all.
func (w *ReuseBuilder) BuildAppend(dst []byte, td ptrace.Traces) []byte {
	return w.b.BuildAppend(dst, w.s.ToValueReuse(td))
}

// TreeOnly exists so a benchmark can time the fill without the serializer.
func (w *ReuseBuilder) TreeOnly(td ptrace.Traces) int {
	return len(w.s.ToValueReuse(td).ResourceSpans.V)
}
