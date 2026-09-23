package bench

// profiles_graph_test.go — spec/38 §2.6: does a GRAPH layout of OTLP profiles win, and
// where does it lose? Three encodings of the same ProfilesBatch:
//
//	proto     pprofile.ProtoMarshaler (indices into a dictionary)
//	faithful  OtlpProfiles — packed, the proto's shape 1:1 (indices into a dictionary)
//	graph     OtlpProfilesGraph — regular nodes, references instead of indices,
//	          every shared entry stored once, no string table
//
// Two reads: the SHALLOW sweep (sum every sample value — touches no dictionary) and the
// DEEP one (for every sample, walk stack -> locations -> lines -> function and add up
// the function names' lengths — the read a flame graph does). The faithful layout's
// dictionary tables are packed arrays with no offset table, so a random lookup is O(i);
// its deep read first builds a position index per table in one cursor pass, and that
// cost is inside the measurement.
//
//	go test ./bench -run TestProfilesGraphSizes -v
//	go test ./bench -run '^$' -bench 'ProfilesGraph' -benchmem

import (
	"fmt"
	"testing"

	"go.opentelemetry.io/collector/pdata/pprofile"

	"otlpdagr/internal/fixture"
	"otlpdagr/profilegraphbridge"
	gp "otlpgen/otlpprofiles"
	gg "otlpgen/otlpprofilesgraph"
)

var profilesGraph = profilegraphbridge.ToBytes(fixture.ProfilesBatch(nSamples, nFuncs, nStacks))

func TestProfilesGraphSizes(t *testing.T) {
	t.Logf("%-9s %10s %7s | %10s %7s", "layout", "raw", "vs pb", "gzipped", "vs pb")
	pz := gz(profilesPB)
	for _, r := range []struct {
		name string
		b    []byte
	}{{"proto", profilesPB}, {"faithful", profilesDagr}, {"graph", profilesGraph}} {
		z := gz(r.b)
		t.Logf("%-9s %10d %6.2fx | %10d %6.2fx", r.name, len(r.b),
			float64(len(r.b))/float64(len(profilesPB)), z, float64(z)/float64(pz))
	}
	if a, b, c := deepProto(), deepFaithful(), deepGraph(); a != b || a != c {
		t.Fatalf("deep sweeps disagree: proto %d, faithful %d, graph %d", a, b, c)
	}
	if a, b := sweepProfilesProto(), sweepProfilesGraph(); a != b {
		t.Fatalf("shallow sweeps disagree: proto %d, graph %d", a, b)
	}
}

// ── deep sweep: sum of function-name lengths over every sample's whole stack ──

func deepProto() int {
	pd, err := (&pprofile.ProtoUnmarshaler{}).UnmarshalProfiles(profilesPB)
	if err != nil {
		panic(err)
	}
	dc := pd.Dictionary()
	st, lt, ft, str := dc.StackTable(), dc.LocationTable(), dc.FunctionTable(), dc.StringTable()
	n := 0
	rps := pd.ResourceProfiles()
	for i := 0; i < rps.Len(); i++ {
		sps := rps.At(i).ScopeProfiles()
		for j := 0; j < sps.Len(); j++ {
			ps := sps.At(j).Profiles()
			for k := 0; k < ps.Len(); k++ {
				ss := ps.At(k).Samples()
				for q := 0; q < ss.Len(); q++ {
					li := st.At(int(ss.At(q).StackIndex())).LocationIndices()
					for x := 0; x < li.Len(); x++ {
						lines := lt.At(int(li.At(x))).Lines()
						for y := 0; y < lines.Len(); y++ {
							n += len(str.At(int(ft.At(int(lines.At(y).FunctionIndex())).NameStrindex())))
						}
					}
				}
			}
		}
	}
	return n
}

func deepFaithful() int {
	root, err := gp.OpenOtlpProfilesUnchecked(profilesDagr)
	if err != nil {
		panic(err)
	}
	dc, _ := root.Dictionary()
	// one cursor pass per table: a position index, since At(i) on a packed array is O(i)
	var stacks []gp.StackAccessor
	for it := dc.StackTableIter(); ; {
		s, ok := it.Next()
		if !ok {
			break
		}
		stacks = append(stacks, s)
	}
	var locs []gp.LocationAccessor
	for it := dc.LocationTableIter(); ; {
		l, ok := it.Next()
		if !ok {
			break
		}
		locs = append(locs, l)
	}
	var fnName []int32
	for it := dc.FunctionTableIter(); ; {
		f, ok := it.Next()
		if !ok {
			break
		}
		v, _ := f.NameStrindex()
		fnName = append(fnName, v)
	}
	var strLen []int
	for it := dc.StringTableIter(); ; {
		s, ok := it.Next()
		if !ok {
			break
		}
		strLen = append(strLen, len(s))
	}
	n := 0
	ri := root.ResourceProfilesIter()
	for rp, ok := ri.Next(); ok; rp, ok = ri.Next() {
		si := rp.ScopeProfilesIter()
		for sp, ok := si.Next(); ok; sp, ok = si.Next() {
			pi := sp.ProfilesIter()
			for p, ok := pi.Next(); ok; p, ok = pi.Next() {
				smi := p.SamplesIter()
				for s, ok := smi.Next(); ok; s, ok = smi.Next() {
					si, _ := s.StackIndex()
					li := stacks[si].LocationIndicesIter()
					for x, ok := li.Next(); ok; x, ok = li.Next() {
						lni := locs[x].LinesIter()
						for ln, ok := lni.Next(); ok; ln, ok = lni.Next() {
							fi, _ := ln.FunctionIndex()
							n += strLen[fnName[fi]]
						}
					}
				}
			}
		}
	}
	return n
}

func deepGraph() int {
	root, err := gg.OpenOtlpProfilesGraphUnchecked(profilesGraph)
	if err != nil {
		panic(err)
	}
	n := 0
	ri := root.ResourceProfilesIter()
	for rp, ok := ri.Next(); ok; rp, ok = ri.Next() {
		si := rp.ScopeProfilesIter()
		for sp, ok := si.Next(); ok; sp, ok = si.Next() {
			pi := sp.ProfilesIter()
			for p, ok := pi.Next(); ok; p, ok = pi.Next() {
				smi := p.SamplesIter()
				for s, ok := smi.Next(); ok; s, ok = smi.Next() {
					st, ok := s.Stack()
					if !ok {
						continue
					}
					li := st.LocationsIter()
					for l, ok := li.Next(); ok; l, ok = li.Next() {
						lni := l.LinesIter()
						for ln, ok := lni.Next(); ok; ln, ok = lni.Next() {
							if f, ok := ln.Function(); ok {
								b, _ := f.NameBytes()
								n += len(b)
							}
						}
					}
				}
			}
		}
	}
	return n
}

func sweepProfilesGraph() int64 {
	root, err := gg.OpenOtlpProfilesGraphUnchecked(profilesGraph)
	if err != nil {
		panic(err)
	}
	var sum int64
	ri := root.ResourceProfilesIter()
	for rp, ok := ri.Next(); ok; rp, ok = ri.Next() {
		si := rp.ScopeProfilesIter()
		for sp, ok := si.Next(); ok; sp, ok = si.Next() {
			pi := sp.ProfilesIter()
			for p, ok := pi.Next(); ok; p, ok = pi.Next() {
				smi := p.SamplesIter()
				for s, ok := smi.Next(); ok; s, ok = smi.Next() {
					vi := s.ValuesIter()
					for v, ok := vi.Next(); ok; v, ok = vi.Next() {
						sum += v
					}
				}
			}
		}
	}
	return sum
}

func BenchmarkProfilesGraph(b *testing.B) {
	pb := profilegraphbridge.ToBytes
	for _, c := range []struct {
		name string
		fn   func()
	}{
		{"encode/graph", func() { sinkAny = pb(profilesIn) }},
		{"to_pdata/graph", func() { sinkAny, _ = profilegraphbridge.ToProfiles(profilesGraph) }},
		{"sweep/graph", func() { sinkAny = sweepProfilesGraph() }},
		{"deep/proto", func() { sinkAny = deepProto() }},
		{"deep/faithful", func() { sinkAny = deepFaithful() }},
		{"deep/graph", func() { sinkAny = deepGraph() }},
	} {
		b.Run(fmt.Sprint(c.name), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				c.fn()
			}
		})
	}
}
