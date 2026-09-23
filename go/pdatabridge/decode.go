package pdatabridge

import (
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"

	g "otlpgen/otlptraces"
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
	it := root.ResourceSpansIter()
	for src, ok := it.Next(); ok; src, ok = it.Next() {
		rs := td.ResourceSpans().AppendEmpty()
		if r, ok := src.Resource(); ok {
			putResource(rs.Resource(), r)
		}
		if v, ok := src.SchemaURL(); ok {
			rs.SetSchemaUrl(v)
		}
		it := src.ScopeSpansIter()
		for ssSrc, ok := it.Next(); ok; ssSrc, ok = it.Next() {
			ss := rs.ScopeSpans().AppendEmpty()
			if sc, ok := ssSrc.Scope(); ok {
				putScope(ss.Scope(), sc)
			}
			if v, ok := ssSrc.SchemaURL(); ok {
				ss.SetSchemaUrl(v)
			}
			if k, ok := ssSrc.SpansLen(); ok {
				ss.Spans().EnsureCapacity(k)
			}
			it := ssSrc.SpansIter()
			for sp, ok := it.Next(); ok; sp, ok = it.Next() {
				toPSpan(sp, ss.Spans().AppendEmpty())
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
	putAttrs(sp.Attributes(), src.AttributesIter())
	if v, ok := src.DroppedAttributesCount(); ok {
		sp.SetDroppedAttributesCount(v)
	}

	if _, ok := src.EventsLen(); ok {
		it := src.EventsIter()
		for e, ok := it.Next(); ok; e, ok = it.Next() {
			ev := sp.Events().AppendEmpty()
			if v, ok := e.TimeUnixNano(); ok {
				ev.SetTimestamp(pcommon.Timestamp(v))
			}
			if v, ok := e.Name(); ok {
				ev.SetName(v)
			}
			putAttrs(ev.Attributes(), e.AttributesIter())
			if v, ok := e.DroppedAttributesCount(); ok {
				ev.SetDroppedAttributesCount(v)
			}
		}
	}
	if v, ok := src.DroppedEventsCount(); ok {
		sp.SetDroppedEventsCount(v)
	}

	if _, ok := src.LinksLen(); ok {
		it := src.LinksIter()
		for l, ok := it.Next(); ok; l, ok = it.Next() {
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
			putAttrs(lk.Attributes(), l.AttributesIter())
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
