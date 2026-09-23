package pdatabridge

import (
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"

	g "otlptraces"
)

// ToTraces materialises an OTLP-on-Dagr buffer into pdata, for handing off to an
// unmodified collector pipeline.
//
// This is the SLOW path by construction and it is here for compatibility, not for
// speed: it allocates the whole pdata tree, which is exactly the cost the lazy
// accessors exist to avoid (spec/38 §0.1 — eager materialisation is ~1.6x slower
// than a protobuf unmarshal). Do not benchmark otlp-dagr through this function and
// conclude anything about otlp-dagr.
func ToTraces(buf []byte) (ptrace.Traces, error) {
	root, err := g.OpenOtlpTracesUnchecked(buf)
	if err != nil {
		return ptrace.Traces{}, err
	}
	td := ptrace.NewTraces()
	n, _ := root.ResourceSpansLen()
	for i := 0; i < n; i++ {
		src := root.ResourceSpansAt(i)
		rs := td.ResourceSpans().AppendEmpty()
		if r, ok := src.Resource(); ok {
			putAttrs(rs.Resource().Attributes(), r.AttributesLen, r.AttributesAt)
		}
		if v, ok := src.SchemaURL(); ok {
			rs.SetSchemaUrl(v)
		}
		m, _ := src.ScopeSpansLen()
		for j := 0; j < m; j++ {
			ssSrc := src.ScopeSpansAt(j)
			ss := rs.ScopeSpans().AppendEmpty()
			if sc, ok := ssSrc.Scope(); ok {
				if v, ok := sc.Name(); ok {
					ss.Scope().SetName(v)
				}
				if v, ok := sc.Version(); ok {
					ss.Scope().SetVersion(v)
				}
				putAttrs(ss.Scope().Attributes(), sc.AttributesLen, sc.AttributesAt)
			}
			if v, ok := ssSrc.SchemaURL(); ok {
				ss.SetSchemaUrl(v)
			}
			k, _ := ssSrc.SpansLen()
			for q := 0; q < k; q++ {
				toPSpan(ssSrc.SpansAt(q), ss.Spans().AppendEmpty())
			}
		}
	}
	return td, nil
}

func toPSpan(src g.SpanAccessor, sp ptrace.Span) {
	if v, ok := src.TraceID(); ok {
		var id pcommon.TraceID
		copy(id[:], v)
		sp.SetTraceID(id)
	}
	if v, ok := src.SpanID(); ok {
		var id pcommon.SpanID
		copy(id[:], v)
		sp.SetSpanID(id)
	}
	if v, ok := src.ParentSpanID(); ok {
		var id pcommon.SpanID
		copy(id[:], v)
		sp.SetParentSpanID(id)
	}
	if v, ok := src.TraceState(); ok {
		sp.TraceState().FromRaw(v)
	}
	if v, ok := src.Flags(); ok {
		sp.SetFlags(v)
	}
	if v, ok := src.Name(); ok {
		sp.SetName(v)
	}
	if v, ok := src.Kind(); ok {
		sp.SetKind(ptrace.SpanKind(v))
	}
	if v, ok := src.StartTimeUnixNano(); ok {
		sp.SetStartTimestamp(pcommon.Timestamp(v))
	}
	if v, ok := src.EndTimeUnixNano(); ok {
		sp.SetEndTimestamp(pcommon.Timestamp(v))
	}
	putAttrs(sp.Attributes(), src.AttributesLen, src.AttributesAt)
	if v, ok := src.DroppedAttributesCount(); ok {
		sp.SetDroppedAttributesCount(v)
	}

	if n, ok := src.EventsLen(); ok {
		for i := 0; i < n; i++ {
			e := src.EventsAt(i)
			ev := sp.Events().AppendEmpty()
			if v, ok := e.TimeUnixNano(); ok {
				ev.SetTimestamp(pcommon.Timestamp(v))
			}
			if v, ok := e.Name(); ok {
				ev.SetName(v)
			}
			putAttrs(ev.Attributes(), e.AttributesLen, e.AttributesAt)
			if v, ok := e.DroppedAttributesCount(); ok {
				ev.SetDroppedAttributesCount(v)
			}
		}
	}
	if v, ok := src.DroppedEventsCount(); ok {
		sp.SetDroppedEventsCount(v)
	}

	if n, ok := src.LinksLen(); ok {
		for i := 0; i < n; i++ {
			l := src.LinksAt(i)
			lk := sp.Links().AppendEmpty()
			if v, ok := l.TraceID(); ok {
				var id pcommon.TraceID
				copy(id[:], v)
				lk.SetTraceID(id)
			}
			if v, ok := l.SpanID(); ok {
				var id pcommon.SpanID
				copy(id[:], v)
				lk.SetSpanID(id)
			}
			if v, ok := l.TraceState(); ok {
				lk.TraceState().FromRaw(v)
			}
			putAttrs(lk.Attributes(), l.AttributesLen, l.AttributesAt)
			if v, ok := l.DroppedAttributesCount(); ok {
				lk.SetDroppedAttributesCount(v)
			}
			if v, ok := l.Flags(); ok {
				lk.SetFlags(v)
			}
		}
	}
	if v, ok := src.DroppedLinksCount(); ok {
		sp.SetDroppedLinksCount(v)
	}

	if st, ok := src.Status(); ok {
		if v, ok := st.Message(); ok {
			sp.Status().SetMessage(v)
		}
		if v, ok := st.Code(); ok {
			sp.Status().SetCode(ptrace.StatusCode(v))
		}
	}
}

// MaxValueDepth bounds how far ToTraces will follow nested attribute values.
//
// A Dagr buffer can nest `array_value` / `kvlist_value` arbitrarily, and this decoder is
// recursive, so an adversarial or corrupt buffer could otherwise exhaust the goroutine
// stack — a crash that the reader's own bounds checking does not prevent, because every
// individual read is perfectly valid. Beyond the limit the value is left EMPTY rather
// than silently truncated to something that looks like data. Protobuf's own decoder
// takes the same precaution (a recursion limit, 100 by default); 32 is far past any
// real OTLP attribute, which is rarely nested at all.
const MaxValueDepth = 32

// putAttrs copies a Dagr []KeyValue into a pcommon.Map. The accessor pair is passed
// as funcs because five different node types expose the same two-method shape.
func putAttrs(dst pcommon.Map, length func() (int, bool), at func(int) g.KeyValueAccessor) {
	n, ok := length()
	if !ok || n == 0 {
		return
	}
	dst.EnsureCapacity(n)
	for i := 0; i < n; i++ {
		kv := at(i)
		key, ok := kv.Key()
		if !ok {
			continue
		}
		// An absent value is an attribute that is present and empty — pdata spells that
		// ValueTypeEmpty — so the key is kept either way.
		slot := dst.PutEmpty(key)
		if v, ok := kv.Value(); ok {
			putValue(slot, v, 0)
		}
	}
}

// putValue materialises one Dagr union value into a pdata Value, recursing through
// array_value and kvlist_value. An unknown tag leaves the value empty, which is how a
// reader built against an older schema survives a newer writer.
func putValue(dst pcommon.Value, v g.AnyValuePackedView, depth int) {
	if depth >= MaxValueDepth {
		return
	}
	switch v.Tag() {
	case g.AnyValueTagStringValue:
		dst.SetStr(v.StringValue())
	case g.AnyValueTagIntValue:
		dst.SetInt(v.IntValue())
	case g.AnyValueTagDoubleValue:
		dst.SetDouble(v.DoubleValue())
	case g.AnyValueTagBoolValue:
		dst.SetBool(v.BoolValue())
	case g.AnyValueTagBytesValue:
		dst.SetEmptyBytes().FromRaw(v.BytesValue())
	case g.AnyValueTagArrayValue:
		putSlice(dst.SetEmptySlice(), v, depth+1)
	case g.AnyValueTagKvlistValue:
		putKvlist(dst.SetEmptyMap(), v, depth+1)
	}
}

// putSlice copies an array_value. Elements are optional on the wire, and an absent one
// keeps its position as an empty value — dropping it would renumber the slice.
func putSlice(dst pcommon.Slice, v g.AnyValuePackedView, depth int) {
	av := v.ArrayValue()
	dst.EnsureCapacity(av.Len())
	for elem, present := range av.All() {
		slot := dst.AppendEmpty()
		if present {
			putValue(slot, elem, depth)
		}
	}
}

// putKvlist copies a kvlist_value. Its elements are KeyValue nodes, so they come through
// the node accessors rather than the union array view.
func putKvlist(dst pcommon.Map, v g.AnyValuePackedView, depth int) {
	n := v.KvlistValueLen()
	dst.EnsureCapacity(n)
	for i := 0; i < n; i++ {
		kv := v.KvlistValueAt(i)
		key, ok := kv.Key()
		if !ok {
			continue
		}
		slot := dst.PutEmpty(key)
		if inner, ok := kv.Value(); ok {
			putValue(slot, inner, depth)
		}
	}
}
