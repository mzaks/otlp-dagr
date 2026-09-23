// Package fixture materialises the deterministic seed as a pdata.Traces — the shape
// the OpenTelemetry Collector actually carries — so both sides of every benchmark
// start from identical information.
package fixture

import (
	"encoding/binary"

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

// ── a corpus for tail sampling ───────────────────────────────────────────────

// Sampling is a synthetic workload shaped like what a tail sampler actually faces:
// traces whose spans arrive in DIFFERENT batches, so no single batch holds a whole
// trace and a decision cannot be made from one record.
//
// It is generated rather than seeded because `seed` gives every span its own random
// trace id — perfect for measuring encode and decode, useless for grouping.
type Sampling struct {
	Batches   []ptrace.Traces
	TraceIDs  [][16]byte
	Errors    map[[16]byte]bool // traces carrying at least one ERROR span
	Slow      map[[16]byte]bool // traces whose span set exceeds SlowNanos
	SpansPer  map[[16]byte]int
	LastNano  map[[16]byte]uint64
	TotalSpan int
}

// SlowNanos is the duration above which Sampling marks a trace slow.
const SlowNanos uint64 = 500_000_000

// NewSampling builds nTraces traces of 2..4 spans each, round-robined across nBatches
// batches so a trace's spans are split across them. Deterministic.
//
// Roughly one trace in thirteen errors and one in seventeen is slow, so a policy that
// keeps errors and slow traces keeps ~13% of them. The ratio is chosen to be realistic
// rather than convenient: tail sampling is worth building because the kept fraction is
// small, and a corpus that sampled half of everything would flatter it.
func NewSampling(nTraces, nBatches int) *Sampling {
	const base uint64 = 1_758_000_000_000_000_000
	s := &Sampling{
		Errors:   map[[16]byte]bool{},
		Slow:     map[[16]byte]bool{},
		SpansPer: map[[16]byte]int{},
		LastNano: map[[16]byte]uint64{},
	}

	type pending struct {
		tid      [16]byte
		name     string
		start    uint64
		dur      uint64
		isError  bool
		batchIdx int
	}
	var all []pending

	for i := 0; i < nTraces; i++ {
		// Injective in i, and it has to be: an id built byte-wise from `i` wraps at 256
		// and silently merges trace 300 into trace 44. The benchmark found that by
		// reporting 256 traces for a 400-trace corpus; the test below now gates it.
		var tid [16]byte
		binary.BigEndian.PutUint64(tid[0:8], uint64(i)*0x9E3779B97F4A7C15)
		binary.BigEndian.PutUint64(tid[8:16], uint64(i)+1)
		s.TraceIDs = append(s.TraceIDs, tid)
		nSpans := 2 + i%3
		isError := i%13 == 0
		isSlow := i%17 == 0
		start := base + uint64(i)*1_000_000
		for k := 0; k < nSpans; k++ {
			dur := uint64(1_000_000) * uint64(1+k)
			if isSlow && k == nSpans-1 {
				dur = SlowNanos + 1_000_000
			}
			all = append(all, pending{
				tid:      tid,
				name:     "span",
				start:    start + uint64(k)*100_000,
				dur:      dur,
				isError:  isError && k == nSpans-1,
				batchIdx: (i + k) % nBatches,
			})
			s.SpansPer[tid]++
			if end := start + uint64(k)*100_000 + dur; end > s.LastNano[tid] {
				s.LastNano[tid] = end
			}
		}
		if isError {
			s.Errors[tid] = true
		}
		if isSlow {
			s.Slow[tid] = true
		}
	}
	s.TotalSpan = len(all)

	s.Batches = make([]ptrace.Traces, nBatches)
	for b := range s.Batches {
		td := ptrace.NewTraces()
		rs := td.ResourceSpans().AppendEmpty()
		rs.Resource().Attributes().PutStr("service.name", "checkout-api")
		ss := rs.ScopeSpans().AppendEmpty()
		ss.Scope().SetName("tail-sampling-corpus")
		for _, p := range all {
			if p.batchIdx != b {
				continue
			}
			sp := ss.Spans().AppendEmpty()
			sp.SetTraceID(pcommon.TraceID(p.tid))
			var sid pcommon.SpanID
			binary.BigEndian.PutUint64(sid[:], p.start)
			sp.SetSpanID(sid)
			sp.SetName(p.name)
			sp.SetKind(ptrace.SpanKindServer)
			sp.SetStartTimestamp(pcommon.Timestamp(p.start))
			sp.SetEndTimestamp(pcommon.Timestamp(p.start + p.dur))
			if p.isError {
				sp.Status().SetCode(ptrace.StatusCodeError)
			} else {
				sp.Status().SetCode(ptrace.StatusCodeOk)
			}
			sp.Attributes().PutStr("http.request.method", "GET")
		}
		s.Batches[b] = td
	}
	return s
}

// ── nested attribute values ──────────────────────────────────────────────────

// TracesWithNestedAttrs is the batch with Slice and Map attribute values added to every
// fourth span, nested three deep, including the awkward cases: an empty slice, an empty
// map, an empty element in the middle of a slice, and a map value that is itself absent.
//
// Like TracesWithLinks it is kept out of `seed`, which stays a byte-for-byte port of the
// Rust benchmark's otlp_seed(). And like links, these values exist because the encode
// path treats them separately: an attribute that is a scalar never reaches the recursive
// branch, so a corpus of scalars gates nothing about it.
func TracesWithNestedAttrs(n int) ptrace.Traces {
	td := TracesN(n)
	AddNestedAttrs(td)
	return td
}

// AddNestedAttrs decorates an existing batch in place, so a gate can combine it with
// another shape — links and nested values in one batch, which is what a real collector
// sees and what the exporter/receiver round trip runs on.
func AddNestedAttrs(td ptrace.Traces) {
	i := 0
	for r := 0; r < td.ResourceSpans().Len(); r++ {
		rs := td.ResourceSpans().At(r)
		for s := 0; s < rs.ScopeSpans().Len(); s++ {
			spans := rs.ScopeSpans().At(s).Spans()
			for k := 0; k < spans.Len(); k++ {
				sp := spans.At(k)
				if i%4 != 0 {
					i++
					continue
				}
				at := sp.Attributes()

				// a flat slice of scalars — the common real case (header values)
				hdr := at.PutEmptySlice("http.request.header.accept")
				hdr.AppendEmpty().SetStr("text/html")
				hdr.AppendEmpty().SetStr("application/xhtml+xml")

				// mixed types, an empty element in the middle, and a nested slice
				mixed := at.PutEmptySlice("rpc.args")
				mixed.AppendEmpty().SetInt(int64(i))
				mixed.AppendEmpty() // stays ValueTypeEmpty — must keep its position
				mixed.AppendEmpty().SetBool(i%8 == 0)
				inner := mixed.AppendEmpty().SetEmptySlice()
				inner.AppendEmpty().SetDouble(float64(i) / 4)
				inner.AppendEmpty().SetEmptyBytes().FromRaw([]byte{byte(i), 0xff})

				// a map, holding a map, holding a slice — three levels down
				m := at.PutEmptyMap("db.query.parameters")
				m.PutStr("table", "orders")
				m.PutInt("limit", int64(100+i))
				sub := m.PutEmptyMap("filter")
				sub.PutBool("active", true)
				sub.PutEmpty("unset") // a present key with no value
				tags := sub.PutEmptySlice("tags")
				tags.AppendEmpty().SetStr("eu")
				tags.AppendEmpty().SetStr("prod")

				// the degenerate shapes
				at.PutEmptySlice("empty.slice")
				at.PutEmptyMap("empty.map")
				at.PutEmpty("empty.value")

				i++
			}
		}
	}
}
