package pdatabridge

import (
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"

	cb "otlpdagr/commonbridge"
	"otlpgen/dagr"
	cd "otlpgen/otlpcommondirect"
	d "otlpgen/otlptracesdirect"
)

// StreamBuilder walks pdata straight into the buffer: nothing batch-sized is ever
// materialised.
//
// The scratch version (scratch.go) still built a value tree for the whole batch — sized
// by a counting pass, then filled, then serialized: pdata walked twice and the tree once.
// Here pdata is walked ONCE. Dagr's generated `Put{N}(n, b, buf, i)` entry points take a
// value struct and a write position, so a caller can hand them ONE node at a time: each
// span's value struct is built on the stack, immediately written, and the scratch behind
// its attribute and event slices is reused by the next span.
//
// The three container levels (TracesData / ResourceSpans / ScopeSpans) are written here
// rather than through their generated `Put{N}`, because their span array is what is being
// streamed. They mirror the generated field order and tags exactly, and
// TestStreamMatches gates the bytes against the generated path so the mirroring cannot
// drift silently.
type StreamBuilder struct {
	b *dagr.Builder

	spanKV  []cd.KeyValue // one span's attributes
	evKV    []cd.KeyValue // all of one span's event attributes
	events  []d.Event     // one span's events
	lkKV    []cd.KeyValue // all of one span's link attributes
	links   []d.Link      // one span's links
	linkIDs []byte        // one span's link trace + span ids, 24 bytes each
	otherKV []cd.KeyValue // resource / scope attributes (once per batch)
	ids     [32]byte      // one span's trace + span + parent ids
}

func NewStreamBuilder() *StreamBuilder { return &StreamBuilder{b: dagr.NewBuilder(0)} }

func fit[T any](s []T, n int) []T {
	if cap(s) >= n {
		return s[:n]
	}
	return make([]T, n, n+n/2+8)
}

// BuildAppend encodes td into dst. With dst retained by the caller nothing allocates.
func (w *StreamBuilder) BuildAppend(dst []byte, td ptrace.Traces) []byte {
	b := w.b
	b.Reset()
	buf, i := b.Take()
	buf, i = w.putTraces(td, b, buf, i)
	b.Put(buf, i)
	// The root node ends exactly at the framing word, mirroring the generated build().
	off := b.Cursor()
	b.StoreLEB(uint64(b.Cursor()-off) << 2)
	return b.AppendTo(dst)
}

// AppendRecord encodes td as the content of one SpanLog `Batch` record — what
// spool.Writer.AppendRecord takes — into dst, in the same single pdata pass as
// BuildAppend. The record holds the ResourceSpans array inline, so only the envelope
// around it differs from a TracesData buffer; TestStreamRecordMatches gates the bytes
// against the generated SpanLogWriter.AppendBatch.
func (w *StreamBuilder) AppendRecord(dst []byte, receivedUnixNano uint64, td ptrace.Traces) []byte {
	b := w.b
	b.Reset()
	buf, i := b.Take()
	before := len(buf) - i
	// resource_spans: field 2, a required packed node array (tag 2<<1|1, sized)
	rss := td.ResourceSpans()
	n := rss.Len()
	a := len(buf) - i
	for k := n - 1; k >= 0; k-- {
		buf, i = w.putResourceSpans(rss.At(k), b, buf, i)
	}
	buf, i = dagr.PutLEBAt(buf, i, uint64(n))
	buf, i = dagr.PutLEBAt(buf, i, uint64((len(buf)-i)-a))
	buf, i = dagr.PutLEBAt(buf, i, 5)
	// span_count: field 1, u32 — LEB when it is shorter than the fixed width
	if c := uint64(uint32(td.SpanCount())); dagr.LebLength(c) < 4 {
		buf, i = dagr.PutLEBTaggedAt(buf, i, c, 2)
	} else {
		buf, i = dagr.PutFixedAt(buf, i, c, 4)
		buf, i = dagr.PutLEBAt(buf, i, 3)
	}
	// received_unix_nano: field 0, `raw` u64 — always fixed width
	buf, i = dagr.PutFixedAt(buf, i, receivedUnixNano, 8)
	buf, i = dagr.PutLEBAt(buf, i, 1)
	buf, i = dagr.PutLEBAt(buf, i, uint64((len(buf)-i)-before))
	b.Put(buf, i)
	return b.AppendTo(dst)
}

func (w *StreamBuilder) putTraces(td ptrace.Traces, b *dagr.Builder, buf []byte, i int) ([]byte, int) {
	before := len(buf) - i
	rss := td.ResourceSpans()
	if n := rss.Len(); n > 0 {
		a := len(buf) - i
		for k := n - 1; k >= 0; k-- {
			buf, i = w.putResourceSpans(rss.At(k), b, buf, i)
		}
		buf, i = dagr.PutLEBAt(buf, i, uint64(n))
		buf, i = dagr.PutLEBAt(buf, i, uint64((len(buf)-i)-a))
		buf, i = dagr.PutLEBAt(buf, i, 1)
	}
	return dagr.PutLEBAt(buf, i, uint64((len(buf)-i)-before))
}

func (w *StreamBuilder) putResourceSpans(rs ptrace.ResourceSpans, b *dagr.Builder, buf []byte, i int) ([]byte, int) {
	before := len(buf) - i
	if v := rs.SchemaUrl(); v != "" {
		buf, i = dagr.PutUtf8TaggedAt(buf, i, v, 5)
	}
	sss := rs.ScopeSpans()
	if n := sss.Len(); n > 0 {
		a := len(buf) - i
		for k := n - 1; k >= 0; k-- {
			buf, i = w.putScopeSpans(sss.At(k), b, buf, i)
		}
		buf, i = dagr.PutLEBAt(buf, i, uint64(n))
		buf, i = dagr.PutLEBAt(buf, i, uint64((len(buf)-i)-a))
		buf, i = dagr.PutLEBAt(buf, i, 3)
	}
	res := cd.Resource{
		Attributes:             w.otherAttrs(rs.Resource().Attributes()),
		DroppedAttributesCount: cb.NonZeroU32(rs.Resource().DroppedAttributesCount()),
	}
	buf, i = cd.PutResource(res, b, buf, i)
	buf, i = dagr.PutLEBAt(buf, i, 1) // field tag, as PutResourceSpans emits it
	return dagr.PutLEBAt(buf, i, uint64((len(buf)-i)-before))
}

func (w *StreamBuilder) putScopeSpans(ss ptrace.ScopeSpans, b *dagr.Builder, buf []byte, i int) ([]byte, int) {
	before := len(buf) - i
	if v := ss.SchemaUrl(); v != "" {
		buf, i = dagr.PutUtf8TaggedAt(buf, i, v, 5)
	}
	sps := ss.Spans()
	if n := sps.Len(); n > 0 {
		a := len(buf) - i
		for k := n - 1; k >= 0; k-- {
			buf, i = d.PutSpan(w.spanValue(sps.At(k)), b, buf, i)
		}
		buf, i = dagr.PutLEBAt(buf, i, uint64(n))
		buf, i = dagr.PutLEBAt(buf, i, uint64((len(buf)-i)-a))
		buf, i = dagr.PutLEBAt(buf, i, 3)
	}
	sc := cd.InstrumentationScope{
		Name:                   cb.NonEmpty(ss.Scope().Name()),
		Version:                cb.NonEmpty(ss.Scope().Version()),
		Attributes:             w.otherAttrs(ss.Scope().Attributes()),
		DroppedAttributesCount: cb.NonZeroU32(ss.Scope().DroppedAttributesCount()),
	}
	buf, i = cd.PutInstrumentationScope(sc, b, buf, i)
	buf, i = dagr.PutLEBAt(buf, i, 1) // field tag, as PutScopeSpans emits it
	return dagr.PutLEBAt(buf, i, uint64((len(buf)-i)-before))
}

// ── one node at a time, into scratch the next node reuses ────────────────────

// fillAttrs writes one attribute map into caller-owned storage. It mirrors keyValues:
// an EMPTY pdata value keeps its key and leaves the Dagr field absent, rather than
// dropping the attribute.
//
// Nested Slice / Map values allocate inside attrValue — they must, since the builder
// keeps a pointer into the slice each level hands it, so that storage cannot be scratch
// the next attribute reuses. The flat path this function exists for stays allocation-free.
func fillAttrs(dst []cd.KeyValue, m pcommon.Map) []cd.KeyValue {
	n := 0
	m.Range(func(k string, v pcommon.Value) bool {
		kv := cd.KeyValue{Key: cb.NonEmpty(k)}
		if av, ok := cb.AttrValue(v); ok {
			kv.Value = cb.Some(av)
		}
		dst[n] = kv
		n++
		return true
	})
	return dst[:n]
}

func (w *StreamBuilder) otherAttrs(m pcommon.Map) dagr.Opt[[]cd.KeyValue] {
	if m.Len() == 0 {
		return dagr.Opt[[]cd.KeyValue]{}
	}
	w.otherKV = fit(w.otherKV, m.Len())
	return cb.OptSlice(fillAttrs(w.otherKV, m))
}

func (w *StreamBuilder) spanValue(src ptrace.Span) d.Span {
	out := d.Span{
		Name:              cb.NonEmpty(src.Name()),
		Kind:              cb.Some(d.SpanKind(src.Kind())),
		StartTimeUnixNano: cb.NonZero(uint64(src.StartTimestamp())),
		EndTimeUnixNano:   cb.NonZero(uint64(src.EndTimestamp())),
		TraceState:        cb.NonEmpty(src.TraceState().AsRaw()),
	}
	if a := src.Attributes(); a.Len() > 0 {
		w.spanKV = fit(w.spanKV, a.Len())
		out.Attributes = cb.OptSlice(fillAttrs(w.spanKV, a))
	}

	// ids live in one 32-byte scratch: the span is written before the next one starts
	p := 0
	if tid := src.TraceID(); !tid.IsEmpty() {
		p += copy(w.ids[p:], tid[:])
		out.TraceID = cb.Some(w.ids[p-len(tid) : p])
	}
	if sid := src.SpanID(); !sid.IsEmpty() {
		p += copy(w.ids[p:], sid[:])
		out.SpanID = cb.Some(w.ids[p-len(sid) : p])
	}
	if psid := src.ParentSpanID(); !psid.IsEmpty() {
		p += copy(w.ids[p:], psid[:])
		out.ParentSpanID = cb.Some(w.ids[p-len(psid) : p])
	}

	if f := src.Flags(); f != 0 {
		out.Flags = cb.Some(f)
	}
	if n := src.DroppedAttributesCount(); n != 0 {
		out.DroppedAttributesCount = cb.Some(n)
	}
	if n := src.DroppedEventsCount(); n != 0 {
		out.DroppedEventsCount = cb.Some(n)
	}
	if n := src.DroppedLinksCount(); n != 0 {
		out.DroppedLinksCount = cb.Some(n)
	}

	if evs := src.Events(); evs.Len() > 0 {
		total := 0
		for k := 0; k < evs.Len(); k++ {
			total += evs.At(k).Attributes().Len()
		}
		w.events = fit(w.events, evs.Len())
		w.evKV = fit(w.evKV, total)
		used := 0
		for k := 0; k < evs.Len(); k++ {
			e := evs.At(k)
			ev := d.Event{
				TimeUnixNano:           cb.NonZero(uint64(e.Timestamp())),
				Name:                   cb.NonEmpty(e.Name()),
				DroppedAttributesCount: cb.NonZeroU32(e.DroppedAttributesCount()),
			}
			if a := e.Attributes(); a.Len() > 0 {
				got := fillAttrs(w.evKV[used:used+a.Len()], a)
				used += len(got)
				ev.Attributes = cb.OptSlice(got)
			}
			w.events[k] = ev
		}
		out.Events = cb.OptSlice(w.events)
	}

	if lks := src.Links(); lks.Len() > 0 {
		total := 0
		for k := 0; k < lks.Len(); k++ {
			total += lks.At(k).Attributes().Len()
		}
		w.links = fit(w.links, lks.Len())
		w.lkKV = fit(w.lkKV, total)
		w.linkIDs = fit(w.linkIDs, lks.Len()*24)
		used, idp := 0, 0
		for k := 0; k < lks.Len(); k++ {
			l := lks.At(k)
			lk := d.Link{
				TraceState:             cb.NonEmpty(l.TraceState().AsRaw()),
				DroppedAttributesCount: cb.NonZeroU32(l.DroppedAttributesCount()),
			}
			if tid := l.TraceID(); !tid.IsEmpty() {
				idp += copy(w.linkIDs[idp:], tid[:])
				lk.TraceID = cb.Some(w.linkIDs[idp-len(tid) : idp])
			}
			if sid := l.SpanID(); !sid.IsEmpty() {
				idp += copy(w.linkIDs[idp:], sid[:])
				lk.SpanID = cb.Some(w.linkIDs[idp-len(sid) : idp])
			}
			if f := l.Flags(); f != 0 {
				lk.Flags = cb.Some(f)
			}
			if a := l.Attributes(); a.Len() > 0 {
				got := fillAttrs(w.lkKV[used:used+a.Len()], a)
				used += len(got)
				lk.Attributes = cb.OptSlice(got)
			}
			w.links[k] = lk
		}
		out.Links = cb.OptSlice(w.links)
	}

	if st := src.Status(); st.Code() != ptrace.StatusCodeUnset || st.Message() != "" {
		out.Status = cb.Some(d.Status{
			Message: cb.NonEmpty(st.Message()),
			Code:    cb.Some(d.StatusCode(st.Code())),
		})
	}
	return out
}
