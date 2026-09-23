package pdatabridge_test

import (
	"bytes"
	"testing"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/pdata/ptrace/ptraceotlp"

	"otlpdagr/internal/fixture"
	"otlpdagr/pdatabridge"
)

// The gate that matters: a batch carrying Slice and Map attribute values must survive
// pdata -> Dagr -> pdata unchanged. Compared as marshalled OTLP protobuf, so the
// comparison is over OTLP's own notion of equality rather than this bridge's.
func TestNestedAttributesRoundTrip(t *testing.T) {
	for _, build := range []struct {
		name string
		fn   func(ptrace.Traces) []byte
	}{
		{"value_tree", pdatabridge.ToBytes},
		{"stream", func(td ptrace.Traces) []byte {
			return pdatabridge.NewStreamBuilder().BuildAppend(nil, td)
		}},
	} {
		t.Run(build.name, func(t *testing.T) {
			in := fixture.TracesWithNestedAttrs(48)
			out, err := pdatabridge.ToTraces(build.fn(in))
			if err != nil {
				t.Fatalf("to traces: %v", err)
			}
			if !bytes.Equal(marshal(t, in), marshal(t, out)) {
				t.Fatalf("nested attributes did not survive the round trip:\n in: %v\nout: %v",
					firstNested(t, in), firstNested(t, out))
			}
		})
	}
}

// The streaming and value-tree encoders must agree byte for byte on nested values too —
// the streaming path builds each span on the stack into reused scratch, and nested
// values are exactly where that model has to give way to allocation.
func TestNestedStreamMatchesValueTree(t *testing.T) {
	in := fixture.TracesWithNestedAttrs(64)
	want := pdatabridge.ToBytes(in)
	w := pdatabridge.NewStreamBuilder()
	// twice, plus an interleaved flat batch: scratch that outlived its span shows up here
	flat := fixture.TracesN(9)
	flatWant := pdatabridge.ToBytes(flat)
	for i := 0; i < 3; i++ {
		if got := w.BuildAppend(nil, in); !bytes.Equal(got, want) {
			t.Fatalf("nested pass %d diverged: %d B vs %d B", i, len(got), len(want))
		}
		if got := w.BuildAppend(nil, flat); !bytes.Equal(got, flatWant) {
			t.Fatalf("flat pass %d diverged", i)
		}
	}
}

// The awkward shapes, checked individually rather than through a byte comparison that
// would pass if BOTH sides were wrong in the same way.
func TestNestedEdgeShapes(t *testing.T) {
	in := ptrace.NewTraces()
	sp := in.ResourceSpans().AppendEmpty().ScopeSpans().AppendEmpty().Spans().AppendEmpty()
	sp.SetName("edges")
	at := sp.Attributes()
	at.PutEmptySlice("empty.slice")
	at.PutEmptyMap("empty.map")
	at.PutEmpty("empty.value")
	holes := at.PutEmptySlice("holes")
	holes.AppendEmpty().SetStr("a")
	holes.AppendEmpty()
	holes.AppendEmpty().SetStr("c")
	m := at.PutEmptyMap("m")
	m.PutEmpty("absent")

	out, err := pdatabridge.ToTraces(pdatabridge.ToBytes(in))
	if err != nil {
		t.Fatalf("to traces: %v", err)
	}
	got := out.ResourceSpans().At(0).ScopeSpans().At(0).Spans().At(0).Attributes()

	if v, ok := got.Get("empty.slice"); !ok || v.Type() != pcommon.ValueTypeSlice || v.Slice().Len() != 0 {
		t.Fatalf("empty slice came back as %v", v)
	}
	if v, ok := got.Get("empty.map"); !ok || v.Type() != pcommon.ValueTypeMap || v.Map().Len() != 0 {
		t.Fatalf("empty map came back as %v", v)
	}
	if v, ok := got.Get("empty.value"); !ok || v.Type() != pcommon.ValueTypeEmpty {
		t.Fatalf("empty value came back as %v — an attribute was dropped or invented", v)
	}
	h, ok := got.Get("holes")
	if !ok || h.Slice().Len() != 3 {
		t.Fatalf("slice with a hole came back with %d elements, want 3", h.Slice().Len())
	}
	// A plain array cannot say "absent": it coerces the element to the union's zero
	// variant, so the hole came back as an empty STRING and the type was silently
	// rewritten. array_with_optionals is what makes this assertion hold.
	if h.Slice().At(0).Str() != "a" || h.Slice().At(2).Str() != "c" {
		t.Fatal("the slice's populated elements moved")
	}
	if got := h.Slice().At(1).Type(); got != pcommon.ValueTypeEmpty {
		t.Fatalf("the hole came back as %v, not an empty value", got)
	}
	mv, ok := got.Get("m")
	if !ok || mv.Map().Len() != 1 {
		t.Fatalf("map with an absent value came back with %d entries", mv.Map().Len())
	}
	if v, ok := mv.Map().Get("absent"); !ok || v.Type() != pcommon.ValueTypeEmpty {
		t.Fatal("a present key with no value lost its key")
	}
}

// Deep nesting must not take the stack down. The decoder is recursive over a structure
// an attacker could choose, so the depth bound is load-bearing, not cosmetic.
func TestDeepNestingIsBounded(t *testing.T) {
	in := ptrace.NewTraces()
	sp := in.ResourceSpans().AppendEmpty().ScopeSpans().AppendEmpty().Spans().AppendEmpty()
	cur := sp.Attributes().PutEmptySlice("deep")
	for i := 0; i < pdatabridge.MaxValueDepth*3; i++ {
		cur = cur.AppendEmpty().SetEmptySlice()
	}
	cur.AppendEmpty().SetStr("bottom")

	out, err := pdatabridge.ToTraces(pdatabridge.ToBytes(in))
	if err != nil {
		t.Fatalf("to traces: %v", err)
	}
	v, ok := out.ResourceSpans().At(0).ScopeSpans().At(0).Spans().At(0).Attributes().Get("deep")
	if !ok {
		t.Fatal("the deep attribute vanished entirely")
	}
	depth := 0
	for v.Type() == pcommon.ValueTypeSlice && v.Slice().Len() > 0 {
		v = v.Slice().At(0)
		depth++
	}
	if depth > pdatabridge.MaxValueDepth {
		t.Fatalf("decoded %d levels deep, past the %d bound", depth, pdatabridge.MaxValueDepth)
	}
	if depth < 2 {
		t.Fatalf("only %d levels survived — the bound is cutting real data", depth)
	}
}

// Scalar-only batches must stay allocation-free on encode: the recursive path is only
// for values that need it, and adding it must not have cost the common case.
func TestFlatEncodeStillAllocatesNothing(t *testing.T) {
	td := fixture.TracesN(500)
	w := pdatabridge.NewStreamBuilder()
	var dst []byte
	dst = w.BuildAppend(dst[:0], td)
	if n := testing.AllocsPerRun(5, func() { dst = w.BuildAppend(dst[:0], td) }); n != 0 {
		t.Fatalf("a flat batch allocated %.1f times per encode, want 0", n)
	}
}

func marshal(t *testing.T, td ptrace.Traces) []byte {
	t.Helper()
	b, err := ptraceotlp.NewExportRequestFromTraces(td).MarshalProto()
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func firstNested(t *testing.T, td ptrace.Traces) any {
	t.Helper()
	at := td.ResourceSpans().At(0).ScopeSpans().At(0).Spans().At(0).Attributes()
	v, _ := at.Get("db.query.parameters")
	return v.AsRaw()
}
