// Package fixture materialises the deterministic seed as a pdata.Traces — the shape
// the OpenTelemetry Collector actually carries — so both sides of every benchmark
// start from identical information.
package fixture

import (
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"otlpdagr/internal/seed"
)

// Traces builds the 500-span batch as pdata.
func Traces() ptrace.Traces { return TracesN(seed.NSpans) }

// TracesN builds the same batch with n spans.
func TracesN(n int) ptrace.Traces {
	sd := seed.NewN(n)
	td := ptrace.NewTraces()
	rs := td.ResourceSpans().AppendEmpty()

	ra := rs.Resource().Attributes()
	ra.EnsureCapacity(len(sd.ResourceAttrs))
	for _, kv := range sd.ResourceAttrs {
		ra.PutStr(kv[0], kv[1])
	}

	ss := rs.ScopeSpans().AppendEmpty()
	ss.Scope().SetName(sd.ScopeName)
	ss.Scope().SetVersion(sd.ScopeVersion)

	spans := ss.Spans()
	spans.EnsureCapacity(len(sd.Spans))
	for i := range sd.Spans {
		s := &sd.Spans[i]
		sp := spans.AppendEmpty()

		var tid pcommon.TraceID
		copy(tid[:], s.TraceID)
		sp.SetTraceID(tid)
		var sid, psid pcommon.SpanID
		copy(sid[:], s.SpanID)
		sp.SetSpanID(sid)
		copy(psid[:], s.ParentSpanID)
		sp.SetParentSpanID(psid)

		sp.SetName(s.Name)
		sp.SetKind(ptrace.SpanKindServer)
		sp.SetStartTimestamp(pcommon.Timestamp(s.Start))
		sp.SetEndTimestamp(pcommon.Timestamp(s.End))

		at := sp.Attributes()
		at.EnsureCapacity(len(seed.AttrKeys))
		for j, k := range seed.AttrKeys {
			switch v := s.Attrs[j]; v.Kind {
			case seed.AttrString:
				at.PutStr(k, v.S)
			case seed.AttrInt:
				at.PutInt(k, v.I)
			case seed.AttrBool:
				at.PutBool(k, v.B)
			}
		}

		if s.HasEvent {
			ev := sp.Events().AppendEmpty()
			ev.SetTimestamp(pcommon.Timestamp(s.EventTime))
			ev.SetName(s.EventName)
			ev.Attributes().PutStr("exception.type", seed.ExceptionType)
		}

		if s.OK {
			sp.Status().SetCode(ptrace.StatusCodeOk)
		} else {
			sp.Status().SetCode(ptrace.StatusCodeError)
		}
	}
	return td
}
