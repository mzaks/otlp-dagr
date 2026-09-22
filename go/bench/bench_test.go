// Package bench answers the one question that decides whether otlp-dagr is worth
// building: is reading an OTLP batch through Dagr's lazy accessors decisively
// cheaper than `ptrace.Unmarshal` + pdata accessors — in nanoseconds AND, more
// importantly, in ALLOCATIONS?
//
// Allocations are the point. The OpenTelemetry Collector's chronic complaint about
// pdata is GC pressure, not raw speed: every receiver unmarshals into a pointer-rich
// tree that the processors then walk and the exporter throws away. A Dagr lazy read
// allocates nothing at all — it indexes a borrowed []byte.
//
// Run:  go test ./bench -bench . -benchmem -benchtime 200x
//
// The SWEEP workload mirrors the Rust benchmark in the Dagr repo
// (benchmarks/proto_vs_dagr, "OTLP traces"): walk every span, accumulate
// start_time_unix_nano and len(name). The ATTR workload is the realistic collector
// processor: find one attribute by key on every span, which is what routing,
// filtering and sampling actually do (spec/38 §5.1).
package bench

import (
	"bytes"
	"testing"

	"go.opentelemetry.io/collector/pdata/ptrace"

	g "otlptraces"

	"otlpdagr/internal/fixture"
	"otlpdagr/internal/seed"
	"otlpdagr/pdatabridge"
)

var (
	traces     = fixture.Traces()
	protoBytes = mustProto(traces)
	dagrBytes  = pdatabridge.ToBytes(traces)

	protoUnmarshaler = &ptrace.ProtoUnmarshaler{}
	protoMarshaler   = &ptrace.ProtoMarshaler{}

	// sink keeps the compiler from eliminating the work under measurement.
	sink uint64
)

func mustProto(td ptrace.Traces) []byte {
	b, err := (&ptrace.ProtoMarshaler{}).MarshalTraces(td)
	if err != nil {
		panic(err)
	}
	return b
}

// ── correctness gates — a faster read that reads the wrong thing is not a result ──

func TestSizes(t *testing.T) {
	t.Logf("protobuf %d B   dagr %d B   (%.2fx)", len(protoBytes), len(dagrBytes),
		float64(len(dagrBytes))/float64(len(protoBytes)))
	if len(dagrBytes) == 0 {
		t.Fatal("empty dagr buffer")
	}
}

func TestSweepsAgree(t *testing.T) {
	if a, b := sweepProto(protoBytes), sweepDagr(dagrBytes); a != b {
		t.Fatalf("sweep checksum differs: proto %d, dagr %d", a, b)
	}
	if a, b := attrProto(protoBytes), attrDagr(dagrBytes); a != b {
		t.Fatalf("attr checksum differs: proto %d, dagr %d", a, b)
	}
}

// TestRoundTrip is the §13.4 gate: pdata -> Dagr -> pdata preserves the batch, and
// Dagr -> pdata -> Dagr is byte-identical.
func TestRoundTrip(t *testing.T) {
	back, err := pdatabridge.ToTraces(dagrBytes)
	if err != nil {
		t.Fatalf("ToTraces: %v", err)
	}
	if got, want := mustProto(back), protoBytes; !bytes.Equal(got, want) {
		t.Fatalf("pdata -> dagr -> pdata lost information: %d B vs %d B", len(got), len(want))
	}
	if got := pdatabridge.ToBytes(back); !bytes.Equal(got, dagrBytes) {
		t.Fatalf("dagr -> pdata -> dagr not byte-identical: %d B vs %d B", len(got), len(dagrBytes))
	}
}

// ── the sweep: every span, two fields ────────────────────────────────────────

func sweepProto(buf []byte) uint64 {
	td, err := protoUnmarshaler.UnmarshalTraces(buf)
	if err != nil {
		panic(err)
	}
	var acc uint64
	rss := td.ResourceSpans()
	for i := 0; i < rss.Len(); i++ {
		sss := rss.At(i).ScopeSpans()
		for j := 0; j < sss.Len(); j++ {
			sps := sss.At(j).Spans()
			for k := 0; k < sps.Len(); k++ {
				sp := sps.At(k)
				acc += uint64(sp.StartTimestamp()) + uint64(len(sp.Name()))
			}
		}
	}
	return acc
}

func sweepDagr(buf []byte) uint64 {
	root, err := g.OpenOtlpTracesUnchecked(buf)
	if err != nil {
		panic(err)
	}
	var acc uint64
	n, _ := root.ResourceSpansLen()
	for i := 0; i < n; i++ {
		rs := root.ResourceSpansAt(i)
		m, _ := rs.ScopeSpansLen()
		for j := 0; j < m; j++ {
			ss := rs.ScopeSpansAt(j)
			k, _ := ss.SpansLen()
			for q := 0; q < k; q++ {
				sp := ss.SpansAt(q)
				start, _ := sp.StartTimeUnixNano()
				nm, _ := sp.NameBytes()
				acc += start + uint64(len(nm))
			}
		}
	}
	return acc
}

// ── the processor workload: one attribute, by key, on every span ─────────────

const routeKey = "url.path"

func attrProto(buf []byte) uint64 {
	td, err := protoUnmarshaler.UnmarshalTraces(buf)
	if err != nil {
		panic(err)
	}
	var acc uint64
	rss := td.ResourceSpans()
	for i := 0; i < rss.Len(); i++ {
		sss := rss.At(i).ScopeSpans()
		for j := 0; j < sss.Len(); j++ {
			sps := sss.At(j).Spans()
			for k := 0; k < sps.Len(); k++ {
				if v, ok := sps.At(k).Attributes().Get(routeKey); ok {
					acc += uint64(len(v.Str()))
				}
			}
		}
	}
	return acc
}

var routeKeyBytes = []byte(routeKey)

func attrDagr(buf []byte) uint64 {
	root, err := g.OpenOtlpTracesUnchecked(buf)
	if err != nil {
		panic(err)
	}
	var acc uint64
	n, _ := root.ResourceSpansLen()
	for i := 0; i < n; i++ {
		rs := root.ResourceSpansAt(i)
		m, _ := rs.ScopeSpansLen()
		for j := 0; j < m; j++ {
			ss := rs.ScopeSpansAt(j)
			k, _ := ss.SpansLen()
			for q := 0; q < k; q++ {
				sp := ss.SpansAt(q)
				na, _ := sp.AttributesLen()
				for x := 0; x < na; x++ {
					kv := sp.AttributesAt(x)
					// KeyBytes borrows the buffer; Key() would allocate a string.
					if kb, ok := kv.KeyBytes(); ok && bytes.Equal(kb, routeKeyBytes) {
						if v, ok := kv.Value(); ok && v.Tag() == g.AnyValueTagStringValue {
							acc += uint64(len(v.StringValueBytes()))
						}
						break
					}
				}
			}
		}
	}
	return acc
}

// ── benchmarks ───────────────────────────────────────────────────────────────

func BenchmarkSweep_Proto(b *testing.B) {
	b.SetBytes(int64(len(protoBytes)))
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		sink += sweepProto(protoBytes)
	}
}

func BenchmarkSweep_DagrLazy(b *testing.B) {
	b.SetBytes(int64(len(dagrBytes)))
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		sink += sweepDagr(dagrBytes)
	}
}

func BenchmarkAttr_Proto(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		sink += attrProto(protoBytes)
	}
}

func BenchmarkAttr_DagrLazy(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		sink += attrDagr(dagrBytes)
	}
}

// Pinpoint: read four specific spans. protobuf pays the full decode either way;
// Dagr indexes straight to them.
var pinpoint = [4]int{0, 137, 298, seed.NSpans - 1}

func BenchmarkPinpoint_Proto(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		td, err := protoUnmarshaler.UnmarshalTraces(protoBytes)
		if err != nil {
			b.Fatal(err)
		}
		sps := td.ResourceSpans().At(0).ScopeSpans().At(0).Spans()
		var acc uint64
		for _, idx := range pinpoint {
			acc += uint64(sps.At(idx).StartTimestamp())
		}
		sink += acc
	}
}

func BenchmarkPinpoint_DagrLazy(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		root, err := g.OpenOtlpTracesUnchecked(dagrBytes)
		if err != nil {
			b.Fatal(err)
		}
		ss := root.ResourceSpansAt(0).ScopeSpansAt(0)
		var acc uint64
		for _, idx := range pinpoint {
			v, _ := ss.SpansAt(idx).StartTimeUnixNano()
			acc += v
		}
		sink += acc
	}
}

// Open alone — the fixed cost before the first field read.
func BenchmarkOpen_Proto(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		td, err := protoUnmarshaler.UnmarshalTraces(protoBytes)
		if err != nil {
			b.Fatal(err)
		}
		sink += uint64(td.SpanCount())
	}
}

func BenchmarkOpen_DagrUnchecked(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		root, err := g.OpenOtlpTracesUnchecked(dagrBytes)
		if err != nil {
			b.Fatal(err)
		}
		n, _ := root.ResourceSpansLen()
		sink += uint64(n)
	}
}

// OpenOtlpTraces walks the whole reachable graph once so later getters cannot panic
// on hostile bytes (spec/37). This is what a receiver on an untrusted hop pays.
func BenchmarkOpen_DagrValidated(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		root, err := g.OpenOtlpTraces(dagrBytes)
		if err != nil {
			b.Fatal(err)
		}
		n, _ := root.ResourceSpansLen()
		sink += uint64(n)
	}
}

// The §0.1 catch, in Go: full materialisation into pdata is the slow path, and is
// here to be measured rather than assumed.
func BenchmarkMaterialise_DagrToPdata(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		td, err := pdatabridge.ToTraces(dagrBytes)
		if err != nil {
			b.Fatal(err)
		}
		sink += uint64(td.SpanCount())
	}
}

// ── encode ───────────────────────────────────────────────────────────────────

func BenchmarkEncode_ProtoMarshal(b *testing.B) {
	b.SetBytes(int64(len(protoBytes)))
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		out, err := protoMarshaler.MarshalTraces(traces)
		if err != nil {
			b.Fatal(err)
		}
		sink += uint64(len(out))
	}
}

func BenchmarkEncode_DagrDirect(b *testing.B) {
	b.SetBytes(int64(len(dagrBytes)))
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		sink += uint64(len(pdatabridge.ToBytes(traces)))
	}
}

// Reusable builder: one scratch buffer, reset per Build (spec 33 §4.3) — the shape
// an exporter would actually use.
func BenchmarkEncode_DagrDirectReused(b *testing.B) {
	w := pdatabridge.NewBuilder()
	b.SetBytes(int64(len(dagrBytes)))
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		sink += uint64(len(w.Build(traces)))
	}
}

// ── scaling: is the lazy sweep linear in span count? ─────────────────────────
//
// It is not. `SpansAt(i)` on a PACKED node array has no offset table — elements are
// inlined and self-sizing, so reaching element i means skipping i blobs from the
// array start. Sweeping every span with At(i) is therefore O(n^2), and the Go lazy
// API exposes no iterator to walk the array once. These sub-benchmarks measure it:
// protobuf scales ~linearly with n, the Dagr indexed sweep ~quadratically.
//
// This is an API omission, not a wire-format limitation — the reader already walks
// the array sequentially inside At(i); it just throws the cursor away each call.

var scales = []int{50, 100, 250, 500}

type fx struct {
	n     int
	proto []byte
	dagr  []byte
}

var scaleFixtures = func() []fx {
	out := make([]fx, 0, len(scales))
	for _, n := range scales {
		td := fixture.TracesN(n)
		out = append(out, fx{n: n, proto: mustProto(td), dagr: pdatabridge.ToBytes(td)})
	}
	return out
}()

func BenchmarkScaling_Proto(b *testing.B) {
	for _, f := range scaleFixtures {
		b.Run(spanLabel(f.n), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				sink += sweepProto(f.proto)
			}
		})
	}
}

func BenchmarkScaling_DagrLazy(b *testing.B) {
	for _, f := range scaleFixtures {
		b.Run(spanLabel(f.n), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				sink += sweepDagr(f.dagr)
			}
		})
	}
}

func spanLabel(n int) string { return "spans=" + itoa(n) }

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var b [8]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	return string(b[i:])
}

// ── encode, split ────────────────────────────────────────────────────────────
//
// BenchmarkEncode_DagrDirect above times pdata -> value tree -> bytes. The Rust
// benchmark this project's numbers come from times only the second half, against a
// pre-built tree. Both are reported so the bridge's cost is visible separately.

var prebuiltTree = pdatabridge.ToValue(traces)

func BenchmarkEncode_DagrSerializeOnly(b *testing.B) {
	b.SetBytes(int64(len(dagrBytes)))
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		sink += uint64(len(pdatabridge.BuildValue(prebuiltTree)))
	}
}

func BenchmarkEncode_DagrTreeOnly(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		v := pdatabridge.ToValue(traces)
		sink += uint64(len(v.ResourceSpans.V))
	}
}

// ── iterators (Dagr spec/38 §10.2 / D7) ──────────────────────────────────────
//
// `{F}Iter()` keeps the cursor between elements instead of replaying the skip loop
// from the array start, so a sweep is O(n) rather than O(n^2). `{F}All()` is the
// range-over-func form built on it. A faster walk that walks something else is not a
// result, so the agreement test runs first.

func sweepDagrIter(buf []byte) uint64 {
	root, err := g.OpenOtlpTracesUnchecked(buf)
	if err != nil {
		panic(err)
	}
	var acc uint64
	for rs := range root.ResourceSpansAll() {
		for ss := range rs.ScopeSpansAll() {
			for sp := range ss.SpansAll() {
				start, _ := sp.StartTimeUnixNano()
				nm, _ := sp.NameBytes()
				acc += start + uint64(len(nm))
			}
		}
	}
	return acc
}

// sweepDagrCursor is the same walk through the cursor primitive rather than
// range-over-func, so the two forms can be compared directly.
func sweepDagrCursor(buf []byte) uint64 {
	root, err := g.OpenOtlpTracesUnchecked(buf)
	if err != nil {
		panic(err)
	}
	var acc uint64
	rsIt := root.ResourceSpansIter()
	for rs, ok := rsIt.Next(); ok; rs, ok = rsIt.Next() {
		ssIt := rs.ScopeSpansIter()
		for ss, ok := ssIt.Next(); ok; ss, ok = ssIt.Next() {
			spIt := ss.SpansIter()
			for sp, ok := spIt.Next(); ok; sp, ok = spIt.Next() {
				start, _ := sp.StartTimeUnixNano()
				nm, _ := sp.NameBytes()
				acc += start + uint64(len(nm))
			}
		}
	}
	return acc
}

func attrDagrIter(buf []byte) uint64 {
	root, err := g.OpenOtlpTracesUnchecked(buf)
	if err != nil {
		panic(err)
	}
	var acc uint64
	for rs := range root.ResourceSpansAll() {
		for ss := range rs.ScopeSpansAll() {
			for sp := range ss.SpansAll() {
				for kv := range sp.AttributesAll() {
					if kb, ok := kv.KeyBytes(); ok && bytes.Equal(kb, routeKeyBytes) {
						if v, ok := kv.Value(); ok && v.Tag() == g.AnyValueTagStringValue {
							acc += uint64(len(v.StringValueBytes()))
						}
						break
					}
				}
			}
		}
	}
	return acc
}

// TestIterMatchesAt walks every array in the batch both ways and compares element by
// element — node arrays at three nesting levels, and the attribute arrays under them.
func TestIterMatchesAt(t *testing.T) {
	root, err := g.OpenOtlpTracesUnchecked(dagrBytes)
	if err != nil {
		t.Fatal(err)
	}
	rsN, _ := root.ResourceSpansLen()
	seen := 0
	it := root.ResourceSpansIter()
	for i := 0; i < rsN; i++ {
		rs, ok := it.Next()
		if !ok {
			t.Fatalf("ResourceSpans iterator ended early at %d/%d", i, rsN)
		}
		want := root.ResourceSpansAt(i)
		ssN, _ := rs.ScopeSpansLen()
		if w, _ := want.ScopeSpansLen(); w != ssN {
			t.Fatalf("ResourceSpans[%d]: ScopeSpansLen %d vs %d", i, ssN, w)
		}
		ssIt := rs.ScopeSpansIter()
		for j := 0; j < ssN; j++ {
			ss, ok := ssIt.Next()
			if !ok {
				t.Fatalf("ScopeSpans iterator ended early at %d", j)
			}
			spN, _ := ss.SpansLen()
			spIt := ss.SpansIter()
			for k := 0; k < spN; k++ {
				sp, ok := spIt.Next()
				if !ok {
					t.Fatalf("Spans iterator ended early at %d/%d", k, spN)
				}
				exp := rs.ScopeSpansAt(j).SpansAt(k)
				gotS, _ := sp.StartTimeUnixNano()
				wantS, _ := exp.StartTimeUnixNano()
				gotN, _ := sp.Name()
				wantN, _ := exp.Name()
				if gotS != wantS || gotN != wantN {
					t.Fatalf("span %d: iter (%d,%q) vs At (%d,%q)", k, gotS, gotN, wantS, wantN)
				}
				// attributes: a second, inner array on every element
				aN, _ := sp.AttributesLen()
				aIt := sp.AttributesIter()
				for x := 0; x < aN; x++ {
					kv, ok := aIt.Next()
					if !ok {
						t.Fatalf("Attributes iterator ended early at %d/%d", x, aN)
					}
					gk, _ := kv.Key()
					wk, _ := exp.AttributesAt(x).Key()
					if gk != wk {
						t.Fatalf("span %d attr %d: iter %q vs At %q", k, x, gk, wk)
					}
					seen++
				}
				if _, ok := aIt.Next(); ok {
					t.Fatalf("Attributes iterator overran at span %d", k)
				}
			}
			if _, ok := spIt.Next(); ok {
				t.Fatal("Spans iterator overran")
			}
		}
	}
	if _, ok := it.Next(); ok {
		t.Fatal("ResourceSpans iterator overran")
	}
	if seen != seed.NSpans*len(seed.AttrKeys) {
		t.Fatalf("visited %d attributes, want %d", seen, seed.NSpans*len(seed.AttrKeys))
	}
}

func TestIterSweepsAgree(t *testing.T) {
	want := sweepProto(protoBytes)
	for name, got := range map[string]uint64{
		"At":     sweepDagr(dagrBytes),
		"All":    sweepDagrIter(dagrBytes),
		"cursor": sweepDagrCursor(dagrBytes),
	} {
		if got != want {
			t.Errorf("%s sweep = %d, want %d", name, got, want)
		}
	}
	if got, want := attrDagrIter(dagrBytes), attrProto(protoBytes); got != want {
		t.Errorf("All attr sweep = %d, want %d", got, want)
	}
}

func BenchmarkSweep_DagrAll(b *testing.B) {
	b.SetBytes(int64(len(dagrBytes)))
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		sink += sweepDagrIter(dagrBytes)
	}
}

func BenchmarkSweep_DagrCursor(b *testing.B) {
	b.SetBytes(int64(len(dagrBytes)))
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		sink += sweepDagrCursor(dagrBytes)
	}
}

func BenchmarkAttr_DagrAll(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		sink += attrDagrIter(dagrBytes)
	}
}

func BenchmarkScaling_DagrAll(b *testing.B) {
	for _, f := range scaleFixtures {
		b.Run(spanLabel(f.n), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				sink += sweepDagrIter(f.dagr)
			}
		})
	}
}

// ── encode without the copy ──────────────────────────────────────────────────
//
// Build copies the finished record out of the builder (MakeData). BuildAppend appends
// it to a caller-owned slice instead, so an exporter that keeps one builder and one
// destination buffer reuses both and the serialize step allocates nothing.

func BenchmarkEncode_DagrSerializeAppend(b *testing.B) {
	w := pdatabridge.NewBuilder()
	dst := make([]byte, 0, len(dagrBytes)+64)
	if got := w.BuildValueAppend(dst[:0], prebuiltTree); !bytes.Equal(got, dagrBytes) {
		b.Fatalf("BuildAppend diverged: %d B vs %d B", len(got), len(dagrBytes))
	}
	b.SetBytes(int64(len(dagrBytes)))
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		dst = w.BuildValueAppend(dst[:0], prebuiltTree)
		sink += uint64(len(dst))
	}
}

func BenchmarkEncode_DagrDirectAppend(b *testing.B) {
	w := pdatabridge.NewBuilder()
	dst := make([]byte, 0, len(dagrBytes)+64)
	if got := w.BuildAppend(dst[:0], traces); !bytes.Equal(got, dagrBytes) {
		b.Fatalf("BuildAppend diverged: %d B vs %d B", len(got), len(dagrBytes))
	}
	b.SetBytes(int64(len(dagrBytes)))
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		dst = w.BuildAppend(dst[:0], traces)
		sink += uint64(len(dst))
	}
}

// ── the value tree, built into reused scratch ────────────────────────────────
//
// Once Dagr's serializer stopped allocating, 99.9% of what the encode path allocated
// was this bridge building a fresh value tree per batch — ~11 allocations per span.
// Scratch sizes the backings once from a counting pass over pdata and then fills them
// in place, so the tree borrows storage the builder owns and reuses.

func TestReuseBuilderMatches(t *testing.T) {
	w := pdatabridge.NewReuseBuilder()
	// twice, because the second call is the one that reuses the scratch
	for i := 0; i < 2; i++ {
		if got := w.BuildAppend(nil, traces); !bytes.Equal(got, dagrBytes) {
			t.Fatalf("pass %d: reuse builder diverged: %d B vs %d B", i, len(got), len(dagrBytes))
		}
	}
	// and interleaved with a different-sized batch, to catch stale sub-slices
	small := fixture.TracesN(7)
	smallWant := pdatabridge.ToBytes(small)
	for i := 0; i < 3; i++ {
		if got := w.BuildAppend(nil, small); !bytes.Equal(got, smallWant) {
			t.Fatalf("small pass %d diverged", i)
		}
		if got := w.BuildAppend(nil, traces); !bytes.Equal(got, dagrBytes) {
			t.Fatalf("large pass %d diverged", i)
		}
	}
}

func BenchmarkEncode_DagrTreeOnlyReuse(b *testing.B) {
	w := pdatabridge.NewReuseBuilder()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		sink += uint64(w.TreeOnly(traces))
	}
}

func BenchmarkEncode_DagrReuseAppend(b *testing.B) {
	w := pdatabridge.NewReuseBuilder()
	dst := make([]byte, 0, len(dagrBytes)+64)
	if got := w.BuildAppend(dst[:0], traces); !bytes.Equal(got, dagrBytes) {
		b.Fatal("diverged")
	}
	b.SetBytes(int64(len(dagrBytes)))
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		dst = w.BuildAppend(dst[:0], traces)
		sink += uint64(len(dst))
	}
}
