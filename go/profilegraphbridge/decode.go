package profilegraphbridge

import (
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pprofile"

	g "otlpgen/otlpprofilesgraph"
)

// MaxValueDepth bounds nested attribute values, as commonbridge.MaxValueDepth does.
const MaxValueDepth = 32

// ToProfiles materialises an OtlpProfilesGraph buffer into pdata, rebuilding the
// ProfilesDictionary: one table entry per distinct NODE (keyed on BufferPos), in
// first-reference order, entry 0 the mandatory zero value, strings interned.
func ToProfiles(buf []byte) (pprofile.Profiles, error) {
	root, err := g.OpenOtlpProfilesGraphUnchecked(buf)
	if err != nil {
		return pprofile.Profiles{}, err
	}
	pd := pprofile.NewProfiles()
	d := newDecoder(pd.Dictionary())
	rit := root.ResourceProfilesIter()
	for src, ok := rit.Next(); ok; src, ok = rit.Next() {
		rp := pd.ResourceProfiles().AppendEmpty()
		if r, ok := src.Resource(); ok {
			putKVs(rp.Resource().Attributes(), ptr(r.AttributesIter()))
			if v, ok := r.DroppedAttributesCount(); ok {
				rp.Resource().SetDroppedAttributesCount(v)
			}
		}
		if v, ok := src.SchemaURL(); ok {
			rp.SetSchemaUrl(v)
		}
		sit := src.ScopeProfilesIter()
		for spSrc, ok := sit.Next(); ok; spSrc, ok = sit.Next() {
			sp := rp.ScopeProfiles().AppendEmpty()
			if sc, ok := spSrc.Scope(); ok {
				s := sp.Scope()
				if v, ok := sc.Name(); ok {
					s.SetName(v)
				}
				if v, ok := sc.Version(); ok {
					s.SetVersion(v)
				}
				putKVs(s.Attributes(), ptr(sc.AttributesIter()))
				if v, ok := sc.DroppedAttributesCount(); ok {
					s.SetDroppedAttributesCount(v)
				}
			}
			if v, ok := spSrc.SchemaURL(); ok {
				sp.SetSchemaUrl(v)
			}
			pit := spSrc.ProfilesIter()
			for p, ok := pit.Next(); ok; p, ok = pit.Next() {
				d.profile(sp.Profiles().AppendEmpty(), p)
			}
		}
	}
	return pd, nil
}

type decoder struct {
	dc     pprofile.ProfilesDictionary
	strs   map[string]int32
	fns    map[int]int32
	maps   map[int]int32
	locs   map[int]int32
	stacks map[int]int32
	links  map[int]int32
	attrs  map[int]int32
}

func newDecoder(dc pprofile.ProfilesDictionary) *decoder {
	// entry 0 of every table: the zero value, present
	dc.StringTable().Append("")
	dc.MappingTable().AppendEmpty()
	dc.LocationTable().AppendEmpty()
	dc.FunctionTable().AppendEmpty()
	dc.LinkTable().AppendEmpty()
	dc.AttributeTable().AppendEmpty()
	dc.StackTable().AppendEmpty()
	return &decoder{dc: dc, strs: map[string]int32{"": 0},
		fns: map[int]int32{}, maps: map[int]int32{}, locs: map[int]int32{},
		stacks: map[int]int32{}, links: map[int]int32{}, attrs: map[int]int32{}}
}

func (d *decoder) str(s string, ok bool) int32 {
	if !ok || s == "" {
		return 0
	}
	if i, ok := d.strs[s]; ok {
		return i
	}
	i := int32(d.dc.StringTable().Len())
	d.dc.StringTable().Append(s)
	d.strs[s] = i
	return i
}

func (d *decoder) valueType(dst pprofile.ValueType, v g.ValueTypeAccessor) {
	dst.SetTypeStrindex(d.str(v.Type()))
	dst.SetUnitStrindex(d.str(v.Unit()))
}

func (d *decoder) profile(p pprofile.Profile, src g.ProfileAccessor) {
	if v, ok := src.SampleType(); ok {
		d.valueType(p.SampleType(), v)
	}
	if v, ok := src.PeriodType(); ok {
		d.valueType(p.PeriodType(), v)
	}
	if v, ok := src.TimeUnixNano(); ok {
		p.SetTime(pcommon.Timestamp(v))
	}
	if v, ok := src.DurationNano(); ok {
		p.SetDurationNano(v)
	}
	if v, ok := src.Period(); ok {
		p.SetPeriod(v)
	}
	if v, ok := src.ProfileID(); ok {
		var id pprofile.ProfileID
		copy(id[:], v)
		p.SetProfileID(id)
	}
	if v, ok := src.DroppedAttributesCount(); ok {
		p.SetDroppedAttributesCount(v)
	}
	if v, ok := src.OriginalPayloadFormat(); ok {
		p.SetOriginalPayloadFormat(v)
	}
	if v, ok := src.OriginalPayload(); ok {
		p.OriginalPayload().FromRaw(v)
	}
	ait := src.AttributesIter()
	for a, ok := ait.Next(); ok; a, ok = ait.Next() {
		p.AttributeIndices().Append(d.attr(a))
	}
	sit := src.SamplesIter()
	p.Samples().EnsureCapacity(sit.Len())
	for s, ok := sit.Next(); ok; s, ok = sit.Next() {
		out := p.Samples().AppendEmpty()
		if st, ok := s.Stack(); ok {
			out.SetStackIndex(d.stack(st))
		}
		ait := s.AttributesIter()
		for a, ok := ait.Next(); ok; a, ok = ait.Next() {
			out.AttributeIndices().Append(d.attr(a))
		}
		if lk, ok := s.Link(); ok {
			out.SetLinkIndex(d.link(lk))
		}
		if v, ok := s.Values(); ok {
			out.Values().FromRaw(v)
		}
		if v, ok := s.TimestampsUnixNano(); ok {
			out.TimestampsUnixNano().FromRaw(v)
		}
	}
}

func (d *decoder) stack(src g.StackAccessor) int32 {
	if i, ok := d.stacks[src.BufferPos()]; ok {
		return i
	}
	// Reserve the index before recursing, so the table order is first-reference order.
	i := int32(d.dc.StackTable().Len())
	st := d.dc.StackTable().AppendEmpty()
	d.stacks[src.BufferPos()] = i
	it := src.LocationsIter()
	for l, ok := it.Next(); ok; l, ok = it.Next() {
		st.LocationIndices().Append(d.location(l))
	}
	return i
}

func (d *decoder) location(src g.LocationAccessor) int32 {
	if i, ok := d.locs[src.BufferPos()]; ok {
		return i
	}
	i := int32(d.dc.LocationTable().Len())
	loc := d.dc.LocationTable().AppendEmpty()
	d.locs[src.BufferPos()] = i
	if m, ok := src.Mapping(); ok {
		loc.SetMappingIndex(d.mapping(m))
	}
	if v, ok := src.Address(); ok {
		loc.SetAddress(v)
	}
	lit := src.LinesIter()
	for ln, ok := lit.Next(); ok; ln, ok = lit.Next() {
		out := loc.Lines().AppendEmpty()
		if f, ok := ln.Function(); ok {
			out.SetFunctionIndex(d.function(f))
		}
		if v, ok := ln.Line(); ok {
			out.SetLine(v)
		}
		if v, ok := ln.Column(); ok {
			out.SetColumn(v)
		}
	}
	ait := src.AttributesIter()
	for a, ok := ait.Next(); ok; a, ok = ait.Next() {
		loc.AttributeIndices().Append(d.attr(a))
	}
	return i
}

func (d *decoder) mapping(src g.MappingAccessor) int32 {
	if i, ok := d.maps[src.BufferPos()]; ok {
		return i
	}
	i := int32(d.dc.MappingTable().Len())
	m := d.dc.MappingTable().AppendEmpty()
	d.maps[src.BufferPos()] = i
	if v, ok := src.MemoryStart(); ok {
		m.SetMemoryStart(v)
	}
	if v, ok := src.MemoryLimit(); ok {
		m.SetMemoryLimit(v)
	}
	if v, ok := src.FileOffset(); ok {
		m.SetFileOffset(v)
	}
	m.SetFilenameStrindex(d.str(src.Filename()))
	ait := src.AttributesIter()
	for a, ok := ait.Next(); ok; a, ok = ait.Next() {
		m.AttributeIndices().Append(d.attr(a))
	}
	return i
}

func (d *decoder) function(src g.FunctionAccessor) int32 {
	if i, ok := d.fns[src.BufferPos()]; ok {
		return i
	}
	i := int32(d.dc.FunctionTable().Len())
	f := d.dc.FunctionTable().AppendEmpty()
	d.fns[src.BufferPos()] = i
	f.SetNameStrindex(d.str(src.Name()))
	f.SetSystemNameStrindex(d.str(src.SystemName()))
	f.SetFilenameStrindex(d.str(src.Filename()))
	if v, ok := src.StartLine(); ok {
		f.SetStartLine(v)
	}
	return i
}

func (d *decoder) link(src g.LinkAccessor) int32 {
	if i, ok := d.links[src.BufferPos()]; ok {
		return i
	}
	i := int32(d.dc.LinkTable().Len())
	l := d.dc.LinkTable().AppendEmpty()
	d.links[src.BufferPos()] = i
	if v, ok := src.TraceID(); ok {
		var id pcommon.TraceID
		copy(id[:], v)
		l.SetTraceID(id)
	}
	if v, ok := src.SpanID(); ok {
		var id pcommon.SpanID
		copy(id[:], v)
		l.SetSpanID(id)
	}
	return i
}

func (d *decoder) attr(src g.KeyValueAndUnitAccessor) int32 {
	if i, ok := d.attrs[src.BufferPos()]; ok {
		return i
	}
	i := int32(d.dc.AttributeTable().Len())
	a := d.dc.AttributeTable().AppendEmpty()
	d.attrs[src.BufferPos()] = i
	a.SetKeyStrindex(d.str(src.Key()))
	a.SetUnitStrindex(d.str(src.Unit()))
	if v, ok := src.Value(); ok {
		putValue(a.Value(), v, 0)
	}
	return i
}

// ── attribute values ─────────────────────────────────────────────────────────
//
// Two union views reach here: AnyValueView (and its array element) under a REGULAR
// parent — the dictionary's KeyValueAndUnit — and AnyValuePackedView under the packed
// OtlpCommon types (resource and scope attributes).

type regularValue interface {
	Tag() g.AnyValueTag
	StringValue() string
	BoolValue() bool
	IntValue() int64
	DoubleValue() float64
	BytesValue() []byte
	ArrayValue() g.AnyValueArrayView
	KvlistValueLen() int
	KvlistValueAt(i int) g.KeyValueAccessor
}

func putValue(dst pcommon.Value, v regularValue, depth int) {
	if depth >= MaxValueDepth {
		return
	}
	switch v.Tag() {
	case g.AnyValueTagStringValue:
		dst.SetStr(v.StringValue())
	case g.AnyValueTagIntValue:
		dst.SetInt(v.IntValue())
	case g.AnyValueTagDoubleValue:
		dst.SetDouble(v.DoubleValue())
	case g.AnyValueTagBoolValue:
		dst.SetBool(v.BoolValue())
	case g.AnyValueTagBytesValue:
		dst.SetEmptyBytes().FromRaw(v.BytesValue())
	case g.AnyValueTagArrayValue:
		s := dst.SetEmptySlice()
		it := v.ArrayValue().Iter()
		for e, present, ok := it.Next(); ok; e, present, ok = it.Next() {
			slot := s.AppendEmpty()
			if present {
				putValue(slot, e, depth+1)
			}
		}
	case g.AnyValueTagKvlistValue:
		m := dst.SetEmptyMap()
		for i, n := 0, v.KvlistValueLen(); i < n; i++ {
			kv := v.KvlistValueAt(i)
			if key, ok := kv.Key(); ok {
				slot := m.PutEmpty(key)
				if inner, ok := kv.Value(); ok {
					putValue(slot, inner, depth+1)
				}
			}
		}
	}
}

type kvCursor interface {
	Next() (g.KeyValueAccessor, bool)
	Len() int
}

func putKVs(dst pcommon.Map, it kvCursor) {
	if it.Len() == 0 {
		return
	}
	dst.EnsureCapacity(it.Len())
	for kv, ok := it.Next(); ok; kv, ok = it.Next() {
		key, ok := kv.Key()
		if !ok {
			continue
		}
		slot := dst.PutEmpty(key)
		if v, ok := kv.Value(); ok {
			putValue(slot, v, 0)
		}
	}
}

func ptr[T any](v T) *T { return &v }
