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
		v, ok := kv.Value()
		if !ok {
			continue
		}
		switch v.Tag() {
		case g.AnyValueTagStringValue:
			dst.PutStr(key, v.StringValue())
		case g.AnyValueTagIntValue:
			dst.PutInt(key, v.IntValue())
		case g.AnyValueTagDoubleValue:
			dst.PutDouble(key, v.DoubleValue())
		case g.AnyValueTagBoolValue:
			dst.PutBool(key, v.BoolValue())
		case g.AnyValueTagBytesValue:
			dst.PutEmptyBytes(key).FromRaw(v.BytesValue())
		}
	}
}
