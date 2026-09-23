// Package profilegraphbridge converts between pdata profiles (`pprofile`) and the GRAPH
// form of OTLP profiles, `OtlpProfilesGraph` — the experiment beside the faithful
// OtlpProfiles (schema.py, spec/38 §2.6).
//
// Encode resolves every dictionary index into a node reference. Each dictionary entry
// becomes at most ONE arena node, created the first time something refers to it, so a
// Function referenced by a thousand Lines is stored once; the strings move into the nodes
// that own them and the string table disappears. The graph shares nodes, so it encodes
// through the arena (the direct builder is for trees).
//
// Decode rebuilds a ProfilesDictionary, keyed on each node's BufferPos (its identity in
// the buffer): every table in first-reference order, entry 0 the mandatory zero value.
// The indices therefore differ from the input's, and orphaned entries are dropped — both
// of which the proto permits ("MUST NOT have any observable effects") — so the round
// trip is checked on the RESOLVED data, not on the dictionary's bytes.
//
// It has no common_gen.go: the shared conversions there build direct-builder values, and
// this graph has no direct builder, so the attribute conversion is its own (arena) form.
package profilegraphbridge

import (
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pprofile"

	"otlpgen/dagr"
	g "otlpgen/otlpprofilesgraph"
)

// ToBytes serializes pd into an OtlpProfilesGraph buffer via the arena.
func ToBytes(pd pprofile.Profiles) []byte {
	a := g.NewArena()
	return g.ToBytesOtlpProfilesGraph(ToArena(a, pd), 0)
}

// encoder memoises one arena node per dictionary entry.
type encoder struct {
	a   *g.Arena
	dc  pprofile.ProfilesDictionary
	str pcommon.StringSlice

	fns    map[int32]g.Function
	maps   map[int32]g.Mapping
	locs   map[int32]g.Location
	stacks map[int32]g.Stack
	links  map[int32]g.Link
	attrs  map[int32]g.KeyValueAndUnit
}

// ToArena builds the graph in a and returns its root.
func ToArena(a *g.Arena, pd pprofile.Profiles) g.ProfilesData {
	dc := pd.Dictionary()
	e := &encoder{a: a, dc: dc, str: dc.StringTable(),
		fns: map[int32]g.Function{}, maps: map[int32]g.Mapping{}, locs: map[int32]g.Location{},
		stacks: map[int32]g.Stack{}, links: map[int32]g.Link{}, attrs: map[int32]g.KeyValueAndUnit{}}

	rps := pd.ResourceProfiles()
	outRP := make([]g.ResourceProfiles, 0, rps.Len())
	for i := 0; i < rps.Len(); i++ {
		rp := rps.At(i)
		n := a.NewResourceProfiles()
		n.SetResource(e.resource(rp.Resource()))
		if v := rp.SchemaUrl(); v != "" {
			n.SetSchemaURL(v)
		}
		sps := rp.ScopeProfiles()
		outSP := make([]g.ScopeProfiles, 0, sps.Len())
		for j := 0; j < sps.Len(); j++ {
			sp := sps.At(j)
			sn := a.NewScopeProfiles()
			sn.SetScope(e.scope(sp.Scope()))
			if v := sp.SchemaUrl(); v != "" {
				sn.SetSchemaURL(v)
			}
			ps := sp.Profiles()
			outP := make([]g.Profile, 0, ps.Len())
			for k := 0; k < ps.Len(); k++ {
				outP = append(outP, e.profile(ps.At(k)))
			}
			if len(outP) > 0 {
				sn.SetProfiles(outP)
			}
			outSP = append(outSP, sn)
		}
		if len(outSP) > 0 {
			n.SetScopeProfiles(outSP)
		}
		outRP = append(outRP, n)
	}
	root := a.NewProfilesData()
	if len(outRP) > 0 {
		root.SetResourceProfiles(outRP)
	}
	a.SetRoot(root)
	return root
}

func (e *encoder) s(i int32) string {
	if i <= 0 || int(i) >= e.str.Len() {
		return ""
	}
	return e.str.At(int(i))
}

func (e *encoder) valueType(v pprofile.ValueType) (g.ValueType, bool) {
	if v.TypeStrindex() == 0 && v.UnitStrindex() == 0 {
		return g.ValueType{}, false
	}
	n := e.a.NewValueType()
	if s := e.s(v.TypeStrindex()); s != "" {
		n.SetType(s)
	}
	if s := e.s(v.UnitStrindex()); s != "" {
		n.SetUnit(s)
	}
	return n, true
}

func (e *encoder) profile(p pprofile.Profile) g.Profile {
	n := e.a.NewProfile()
	if vt, ok := e.valueType(p.SampleType()); ok {
		n.SetSampleType(vt)
	}
	if vt, ok := e.valueType(p.PeriodType()); ok {
		n.SetPeriodType(vt)
	}
	if v := uint64(p.Time()); v != 0 {
		n.SetTimeUnixNano(v)
	}
	if v := p.DurationNano(); v != 0 {
		n.SetDurationNano(v)
	}
	if v := p.Period(); v != 0 {
		n.SetPeriod(v)
	}
	if id := p.ProfileID(); !id.IsEmpty() {
		n.SetProfileID(append([]byte(nil), id[:]...))
	}
	if v := p.DroppedAttributesCount(); v != 0 {
		n.SetDroppedAttributesCount(v)
	}
	if v := p.OriginalPayloadFormat(); v != "" {
		n.SetOriginalPayloadFormat(v)
	}
	if v := p.OriginalPayload(); v.Len() > 0 {
		n.SetOriginalPayload(v.AsRaw())
	}
	if as := e.attrList(p.AttributeIndices()); len(as) > 0 {
		n.SetAttributes(as)
	}
	ss := p.Samples()
	if ss.Len() > 0 {
		out := make([]g.Sample, 0, ss.Len())
		for i := 0; i < ss.Len(); i++ {
			s := ss.At(i)
			sn := e.a.NewSample()
			if st, ok := e.stack(s.StackIndex()); ok {
				sn.SetStack(st)
			}
			if as := e.attrList(s.AttributeIndices()); len(as) > 0 {
				sn.SetAttributes(as)
			}
			if lk, ok := e.link(s.LinkIndex()); ok {
				sn.SetLink(lk)
			}
			if v := s.Values(); v.Len() > 0 {
				sn.SetValues(v.AsRaw())
			}
			if v := s.TimestampsUnixNano(); v.Len() > 0 {
				sn.SetTimestampsUnixNano(v.AsRaw())
			}
			out = append(out, sn)
		}
		n.SetSamples(out)
	}
	return n
}

// Index 0 of every table is the zero value, which the graph spells as an ABSENT
// reference. An out-of-range index is treated the same way rather than panicking.

func (e *encoder) stack(i int32) (g.Stack, bool) {
	if i <= 0 || int(i) >= e.dc.StackTable().Len() {
		return g.Stack{}, false
	}
	if n, ok := e.stacks[i]; ok {
		return n, true
	}
	n := e.a.NewStack()
	li := e.dc.StackTable().At(int(i)).LocationIndices()
	if li.Len() > 0 {
		locs := make([]g.Location, 0, li.Len())
		for j := 0; j < li.Len(); j++ {
			// a stack may name index 0 (the zero Location); keep it, as an empty node, so
			// the stack's depth survives
			locs = append(locs, e.location(li.At(j)))
		}
		n.SetLocations(locs)
	}
	e.stacks[i] = n
	return n, true
}

func (e *encoder) location(i int32) g.Location {
	if n, ok := e.locs[i]; ok {
		return n
	}
	n := e.a.NewLocation()
	e.locs[i] = n
	if i <= 0 || int(i) >= e.dc.LocationTable().Len() {
		return n
	}
	l := e.dc.LocationTable().At(int(i))
	if m, ok := e.mapping(l.MappingIndex()); ok {
		n.SetMapping(m)
	}
	if v := l.Address(); v != 0 {
		n.SetAddress(v)
	}
	if ls := l.Lines(); ls.Len() > 0 {
		out := make([]g.Line, 0, ls.Len())
		for j := 0; j < ls.Len(); j++ {
			ln := ls.At(j)
			x := e.a.NewLine()
			if f, ok := e.function(ln.FunctionIndex()); ok {
				x.SetFunction(f)
			}
			if v := ln.Line(); v != 0 {
				x.SetLine(v)
			}
			if v := ln.Column(); v != 0 {
				x.SetColumn(v)
			}
			out = append(out, x)
		}
		n.SetLines(out)
	}
	if as := e.attrList(l.AttributeIndices()); len(as) > 0 {
		n.SetAttributes(as)
	}
	return n
}

func (e *encoder) mapping(i int32) (g.Mapping, bool) {
	if i <= 0 || int(i) >= e.dc.MappingTable().Len() {
		return g.Mapping{}, false
	}
	if n, ok := e.maps[i]; ok {
		return n, true
	}
	m := e.dc.MappingTable().At(int(i))
	n := e.a.NewMapping()
	if v := m.MemoryStart(); v != 0 {
		n.SetMemoryStart(v)
	}
	if v := m.MemoryLimit(); v != 0 {
		n.SetMemoryLimit(v)
	}
	if v := m.FileOffset(); v != 0 {
		n.SetFileOffset(v)
	}
	if s := e.s(m.FilenameStrindex()); s != "" {
		n.SetFilename(s)
	}
	if as := e.attrList(m.AttributeIndices()); len(as) > 0 {
		n.SetAttributes(as)
	}
	e.maps[i] = n
	return n, true
}

func (e *encoder) function(i int32) (g.Function, bool) {
	if i <= 0 || int(i) >= e.dc.FunctionTable().Len() {
		return g.Function{}, false
	}
	if n, ok := e.fns[i]; ok {
		return n, true
	}
	f := e.dc.FunctionTable().At(int(i))
	n := e.a.NewFunction()
	if s := e.s(f.NameStrindex()); s != "" {
		n.SetName(s)
	}
	if s := e.s(f.SystemNameStrindex()); s != "" {
		n.SetSystemName(s)
	}
	if s := e.s(f.FilenameStrindex()); s != "" {
		n.SetFilename(s)
	}
	if v := f.StartLine(); v != 0 {
		n.SetStartLine(v)
	}
	e.fns[i] = n
	return n, true
}

func (e *encoder) link(i int32) (g.Link, bool) {
	if i <= 0 || int(i) >= e.dc.LinkTable().Len() {
		return g.Link{}, false
	}
	if n, ok := e.links[i]; ok {
		return n, true
	}
	l := e.dc.LinkTable().At(int(i))
	n := e.a.NewLink()
	if id := l.TraceID(); !id.IsEmpty() {
		n.SetTraceID(append([]byte(nil), id[:]...))
	}
	if id := l.SpanID(); !id.IsEmpty() {
		n.SetSpanID(append([]byte(nil), id[:]...))
	}
	e.links[i] = n
	return n, true
}

func (e *encoder) attrList(idx pcommon.Int32Slice) []g.KeyValueAndUnit {
	if idx.Len() == 0 {
		return nil
	}
	out := make([]g.KeyValueAndUnit, 0, idx.Len())
	for j := 0; j < idx.Len(); j++ {
		out = append(out, e.attr(idx.At(j)))
	}
	return out
}

func (e *encoder) attr(i int32) g.KeyValueAndUnit {
	if n, ok := e.attrs[i]; ok {
		return n
	}
	n := e.a.NewKeyValueAndUnit()
	e.attrs[i] = n
	if i <= 0 || int(i) >= e.dc.AttributeTable().Len() {
		return n
	}
	kv := e.dc.AttributeTable().At(int(i))
	if s := e.s(kv.KeyStrindex()); s != "" {
		n.SetKey(s)
	}
	if v, ok := e.value(kv.Value()); ok {
		n.SetValue(v)
	}
	if s := e.s(kv.UnitStrindex()); s != "" {
		n.SetUnit(s)
	}
	return n
}

func (e *encoder) resource(r pcommon.Resource) g.Resource {
	n := e.a.NewResource()
	if kvs := e.keyValues(r.Attributes()); len(kvs) > 0 {
		n.SetAttributes(kvs)
	}
	if v := r.DroppedAttributesCount(); v != 0 {
		n.SetDroppedAttributesCount(v)
	}
	return n
}

func (e *encoder) scope(s pcommon.InstrumentationScope) g.InstrumentationScope {
	n := e.a.NewInstrumentationScope()
	if v := s.Name(); v != "" {
		n.SetName(v)
	}
	if v := s.Version(); v != "" {
		n.SetVersion(v)
	}
	if kvs := e.keyValues(s.Attributes()); len(kvs) > 0 {
		n.SetAttributes(kvs)
	}
	if v := s.DroppedAttributesCount(); v != 0 {
		n.SetDroppedAttributesCount(v)
	}
	return n
}

func (e *encoder) keyValues(m pcommon.Map) []g.KeyValue {
	if m.Len() == 0 {
		return nil
	}
	out := make([]g.KeyValue, 0, m.Len())
	for k, v := range m.All() {
		kv := e.a.NewKeyValue()
		if k != "" {
			kv.SetKey(k)
		}
		if av, ok := e.value(v); ok {
			kv.SetValue(av)
		}
		out = append(out, kv)
	}
	return out
}

// value is the arena form of pdatabridge's attrValue: false means ValueTypeEmpty.
func (e *encoder) value(v pcommon.Value) (g.AnyValueValue, bool) {
	switch v.Type() {
	case pcommon.ValueTypeStr:
		return g.AnyValueStringValue(v.Str()), true
	case pcommon.ValueTypeInt:
		return g.AnyValueIntValue(v.Int()), true
	case pcommon.ValueTypeDouble:
		return g.AnyValueDoubleValue(v.Double()), true
	case pcommon.ValueTypeBool:
		return g.AnyValueBoolValue(v.Bool()), true
	case pcommon.ValueTypeBytes:
		return g.AnyValueBytesValue(v.Bytes().AsRaw()), true
	case pcommon.ValueTypeSlice:
		s := v.Slice()
		out := make(g.AnyValueList, 0, s.Len())
		for i := 0; i < s.Len(); i++ {
			if av, ok := e.value(s.At(i)); ok {
				out = append(out, dagrSome(av))
			} else {
				out = append(out, dagrNone())
			}
		}
		return g.AnyValueArrayValue(out), true
	case pcommon.ValueTypeMap:
		return g.AnyValueKvlistValue(e.keyValues(v.Map())), true
	}
	return g.AnyValueValue{}, false
}

func dagrSome(v g.AnyValueValue) dagr.Opt[g.AnyValueValue] { return dagr.Some(v) }
func dagrNone() dagr.Opt[g.AnyValueValue]                  { return dagr.Opt[g.AnyValueValue]{} }
