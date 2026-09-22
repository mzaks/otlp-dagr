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

// TracesWithLinks is the same batch with a span link on every third span, and a
// second link (with its own attributes) on every ninth.
//
// It is deliberately NOT part of the seed: `seed` is a byte-for-byte port of the Rust
// benchmark's `otlp_seed()`, and adding a field to it would silently make the Go and
// Rust numbers incomparable. Links only need to exist for the encode path to be gated
// on them, which is what this is for.
func TracesWithLinks(n int) ptrace.Traces {
	td := TracesN(n)
	i := 0
	for r := 0; r < td.ResourceSpans().Len(); r++ {
		rs := td.ResourceSpans().At(r)
		for s := 0; s < rs.ScopeSpans().Len(); s++ {
			spans := rs.ScopeSpans().At(s).Spans()
			for k := 0; k < spans.Len(); k++ {
				sp := spans.At(k)
				if i%3 != 0 {
					i++
					continue
				}
				lk := sp.Links().AppendEmpty()
				var tid pcommon.TraceID
				var sid pcommon.SpanID
				for b := range tid {
					tid[b] = byte(i*7 + b)
				}
				for b := range sid {
					sid[b] = byte(i*13 + b)
				}
				lk.SetTraceID(tid)
				lk.SetSpanID(sid)
				lk.TraceState().FromRaw("sampled=1")
				lk.SetFlags(uint32(i%256) | 0x100)
				lk.Attributes().PutStr("link.kind", "follows_from")
				if i%9 == 0 {
					l2 := sp.Links().AppendEmpty()
					l2.SetTraceID(tid)
					l2.Attributes().PutInt("link.index", int64(i))
					l2.Attributes().PutBool("link.remote", i%2 == 0)
					l2.SetDroppedAttributesCount(uint32(i % 5))
				}
				i++
			}
		}
	}
	return td
}
