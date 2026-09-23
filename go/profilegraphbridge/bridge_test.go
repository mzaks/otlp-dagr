package profilegraphbridge_test

import (
	"fmt"
	"strings"
	"testing"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pprofile"

	"otlpdagr/internal/fixture"
	"otlpdagr/profilegraphbridge"
)

// resolve flattens a batch into what it MEANS: every index replaced by the entry it names,
// recursively, one line per profile and per sample. Two batches that differ only in
// dictionary order (or orphaned entries) resolve identically — which is exactly the
// equivalence the proto grants a processor, and exactly what the graph round trip must
// preserve.
func resolve(pd pprofile.Profiles) []string {
	dc := pd.Dictionary()
	str := func(i int32) string {
		if i < 0 || int(i) >= dc.StringTable().Len() {
			return "<bad>"
		}
		return dc.StringTable().At(int(i))
	}
	attr := func(i int32) string {
		a := dc.AttributeTable().At(int(i))
		return fmt.Sprintf("%s=%v[%s]", str(a.KeyStrindex()), a.Value().AsRaw(), str(a.UnitStrindex()))
	}
	attrs := func(s pcommon.Int32Slice) string {
		var out []string
		for i := 0; i < s.Len(); i++ {
			out = append(out, attr(s.At(i)))
		}
		return "{" + strings.Join(out, ",") + "}"
	}
	fn := func(i int32) string {
		f := dc.FunctionTable().At(int(i))
		return fmt.Sprintf("%s/%s@%s:%d", str(f.NameStrindex()), str(f.SystemNameStrindex()), str(f.FilenameStrindex()), f.StartLine())
	}
	loc := func(i int32) string {
		l := dc.LocationTable().At(int(i))
		m := dc.MappingTable().At(int(l.MappingIndex()))
		var lines []string
		for j := 0; j < l.Lines().Len(); j++ {
			ln := l.Lines().At(j)
			lines = append(lines, fmt.Sprintf("%s:%d:%d", fn(ln.FunctionIndex()), ln.Line(), ln.Column()))
		}
		return fmt.Sprintf("[map %x-%x+%x %s %s] %x %v %s", m.MemoryStart(), m.MemoryLimit(), m.FileOffset(),
			str(m.FilenameStrindex()), attrs(m.AttributeIndices()), l.Address(), lines, attrs(l.AttributeIndices()))
	}
	vt := func(v pprofile.ValueType) string { return str(v.TypeStrindex()) + "/" + str(v.UnitStrindex()) }
	var out []string
	for r := 0; r < pd.ResourceProfiles().Len(); r++ {
		rp := pd.ResourceProfiles().At(r)
		out = append(out, fmt.Sprintf("R %v %d %s", rp.Resource().Attributes().AsRaw(), rp.Resource().DroppedAttributesCount(), rp.SchemaUrl()))
		for s := 0; s < rp.ScopeProfiles().Len(); s++ {
			sp := rp.ScopeProfiles().At(s)
			sc := sp.Scope()
			out = append(out, fmt.Sprintf(" S %s %s %v %d %s", sc.Name(), sc.Version(), sc.Attributes().AsRaw(), sc.DroppedAttributesCount(), sp.SchemaUrl()))
			for p := 0; p < sp.Profiles().Len(); p++ {
				pr := sp.Profiles().At(p)
				out = append(out, fmt.Sprintf("  P %s %s %d %d %d %x %d %s %x %s", vt(pr.SampleType()), vt(pr.PeriodType()),
					pr.Time(), pr.DurationNano(), pr.Period(), pr.ProfileID(), pr.DroppedAttributesCount(),
					pr.OriginalPayloadFormat(), pr.OriginalPayload().AsRaw(), attrs(pr.AttributeIndices())))
				for q := 0; q < pr.Samples().Len(); q++ {
					sm := pr.Samples().At(q)
					var stack []string
					li := dc.StackTable().At(int(sm.StackIndex())).LocationIndices()
					for k := 0; k < li.Len(); k++ {
						stack = append(stack, loc(li.At(k)))
					}
					lk := dc.LinkTable().At(int(sm.LinkIndex()))
					out = append(out, fmt.Sprintf("   s %v %v %s link=%x/%x stack=%v", sm.Values().AsRaw(),
						sm.TimestampsUnixNano().AsRaw(), attrs(sm.AttributeIndices()), lk.TraceID(), lk.SpanID(), stack))
				}
			}
		}
	}
	return out
}

func roundTrip(t *testing.T, name string, in pprofile.Profiles) {
	t.Helper()
	want := resolve(in)
	out, err := profilegraphbridge.ToProfiles(profilegraphbridge.ToBytes(in))
	if err != nil {
		t.Fatal(err)
	}
	got := resolve(out)
	if len(got) != len(want) {
		t.Fatalf("%s: %d resolved lines out, %d in", name, len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("%s: line %d differs after the graph round trip:\n  in:  %s\n  out: %s", name, i, want[i], got[i])
		}
	}
	// entry 0 of every rebuilt table is the zero value
	dc := out.Dictionary()
	if dc.StringTable().At(0) != "" || dc.StackTable().At(0).LocationIndices().Len() != 0 ||
		dc.FunctionTable().At(0).NameStrindex() != 0 {
		t.Fatalf("%s: a rebuilt table has a non-zero entry 0", name)
	}
}

func TestWideRoundTrip(t *testing.T) { roundTrip(t, "wide", fixture.ProfilesWide()) }

func TestBatchRoundTrip(t *testing.T) { roundTrip(t, "batch", fixture.ProfilesBatch(2000, 400, 200)) }

// The point of the graph: a Function referenced from many Lines is stored ONCE. The
// rebuilt function table therefore has exactly as many entries as the input has
// distinct REFERENCED functions (+ the zero entry) — not one per reference.
func TestSharedNodesAreStoredOnce(t *testing.T) {
	in := fixture.ProfilesBatch(2000, 400, 200)
	out, err := profilegraphbridge.ToProfiles(profilegraphbridge.ToBytes(in))
	if err != nil {
		t.Fatal(err)
	}
	used := map[int32]bool{}
	dc := in.Dictionary()
	for i := 1; i < dc.LocationTable().Len(); i++ {
		for j := 0; j < dc.LocationTable().At(i).Lines().Len(); j++ {
			used[dc.LocationTable().At(i).Lines().At(j).FunctionIndex()] = true
		}
	}
	if got, want := out.Dictionary().FunctionTable().Len(), len(used)+1; got > want {
		t.Fatalf("function table rebuilt with %d entries, at most %d distinct functions are referenced", got, want)
	}
}
