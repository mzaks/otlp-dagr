package profilebridge

import (
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pprofile"

	g "otlpgen/otlpprofiles"
)

// ToProfiles materialises an OtlpProfiles buffer into pdata, for handing off to an
// unmodified collector pipeline. The slow path by construction — see pdatabridge.ToTraces.
func ToProfiles(buf []byte) (pprofile.Profiles, error) {
	root, err := g.OpenOtlpProfilesUnchecked(buf)
	if err != nil {
		return pprofile.Profiles{}, err
	}
	pd := pprofile.NewProfiles()
	if dc, ok := root.Dictionary(); ok {
		putDictionary(pd.Dictionary(), dc)
	}
	it := root.ResourceProfilesIter()
	for src, ok := it.Next(); ok; src, ok = it.Next() {
		rp := pd.ResourceProfiles().AppendEmpty()
		if r, ok := src.Resource(); ok {
			putResource(rp.Resource(), r)
		}
		if v, ok := src.SchemaURL(); ok {
			rp.SetSchemaUrl(v)
		}
		it := src.ScopeProfilesIter()
		for spSrc, ok := it.Next(); ok; spSrc, ok = it.Next() {
			sp := rp.ScopeProfiles().AppendEmpty()
			if sc, ok := spSrc.Scope(); ok {
				putScope(sp.Scope(), sc)
			}
			if v, ok := spSrc.SchemaURL(); ok {
				sp.SetSchemaUrl(v)
			}
			it := spSrc.ProfilesIter()
			for p, ok := it.Next(); ok; p, ok = it.Next() {
				putProfile(sp.Profiles().AppendEmpty(), p)
			}
		}
	}
	return pd, nil
}

// int32s / int64s / uint64s copy a scalar array through its cursor.
func int32s[I any, P cursor[I, int32]](dst pcommon.Int32Slice, it I) {
	c := P(&it)
	dst.EnsureCapacity(c.Len())
	for v, ok := c.Next(); ok; v, ok = c.Next() {
		dst.Append(v)
	}
}

func int64s[I any, P cursor[I, int64]](dst pcommon.Int64Slice, it I) {
	c := P(&it)
	dst.EnsureCapacity(c.Len())
	for v, ok := c.Next(); ok; v, ok = c.Next() {
		dst.Append(v)
	}
}

func uint64s[I any, P cursor[I, uint64]](dst pcommon.UInt64Slice, it I) {
	c := P(&it)
	dst.EnsureCapacity(c.Len())
	for v, ok := c.Next(); ok; v, ok = c.Next() {
		dst.Append(v)
	}
}

func putDictionary(dst pprofile.ProfilesDictionary, src g.ProfilesDictionaryAccessor) {
	{
		it := src.MappingTableIter()
		for m, ok := it.Next(); ok; m, ok = it.Next() {
			out := dst.MappingTable().AppendEmpty()
			if v, ok := m.MemoryStart(); ok {
				out.SetMemoryStart(v)
			}
			if v, ok := m.MemoryLimit(); ok {
				out.SetMemoryLimit(v)
			}
			if v, ok := m.FileOffset(); ok {
				out.SetFileOffset(v)
			}
			if v, ok := m.FilenameStrindex(); ok {
				out.SetFilenameStrindex(v)
			}
			int32s(out.AttributeIndices(), m.AttributeIndicesIter())
		}
	}
	{
		it := src.LocationTableIter()
		for l, ok := it.Next(); ok; l, ok = it.Next() {
			out := dst.LocationTable().AppendEmpty()
			if v, ok := l.MappingIndex(); ok {
				out.SetMappingIndex(v)
			}
			if v, ok := l.Address(); ok {
				out.SetAddress(v)
			}
			lit := l.LinesIter()
			for ln, ok := lit.Next(); ok; ln, ok = lit.Next() {
				o := out.Lines().AppendEmpty()
				if v, ok := ln.FunctionIndex(); ok {
					o.SetFunctionIndex(v)
				}
				if v, ok := ln.Line(); ok {
					o.SetLine(v)
				}
				if v, ok := ln.Column(); ok {
					o.SetColumn(v)
				}
			}
			int32s(out.AttributeIndices(), l.AttributeIndicesIter())
		}
	}
	{
		it := src.FunctionTableIter()
		for f, ok := it.Next(); ok; f, ok = it.Next() {
			out := dst.FunctionTable().AppendEmpty()
			if v, ok := f.NameStrindex(); ok {
				out.SetNameStrindex(v)
			}
			if v, ok := f.SystemNameStrindex(); ok {
				out.SetSystemNameStrindex(v)
			}
			if v, ok := f.FilenameStrindex(); ok {
				out.SetFilenameStrindex(v)
			}
			if v, ok := f.StartLine(); ok {
				out.SetStartLine(v)
			}
		}
	}
	{
		it := src.LinkTableIter()
		for k, ok := it.Next(); ok; k, ok = it.Next() {
			out := dst.LinkTable().AppendEmpty()
			if v, ok := k.TraceID(); ok {
				out.SetTraceID(toTraceID(v))
			}
			if v, ok := k.SpanID(); ok {
				out.SetSpanID(toSpanID(v))
			}
		}
	}
	{
		st := dst.StringTable()
		it := src.StringTableIter()
		st.EnsureCapacity(it.Len())
		for s, ok := it.Next(); ok; s, ok = it.Next() {
			st.Append(s)
		}
	}
	{
		it := src.AttributeTableIter()
		for a, ok := it.Next(); ok; a, ok = it.Next() {
			out := dst.AttributeTable().AppendEmpty()
			if v, ok := a.KeyStrindex(); ok {
				out.SetKeyStrindex(v)
			}
			if v, ok := a.Value(); ok {
				putValue(out.Value(), v, 0)
			}
			if v, ok := a.UnitStrindex(); ok {
				out.SetUnitStrindex(v)
			}
		}
	}
	{
		it := src.StackTableIter()
		for s, ok := it.Next(); ok; s, ok = it.Next() {
			int32s(dst.StackTable().AppendEmpty().LocationIndices(), s.LocationIndicesIter())
		}
	}
}

func putValueType(dst pprofile.ValueType, src g.ValueTypeAccessor) {
	if v, ok := src.TypeStrindex(); ok {
		dst.SetTypeStrindex(v)
	}
	if v, ok := src.UnitStrindex(); ok {
		dst.SetUnitStrindex(v)
	}
}

func putProfile(p pprofile.Profile, src g.ProfileAccessor) {
	if v, ok := src.SampleType(); ok {
		putValueType(p.SampleType(), v)
	}
	it := src.SamplesIter()
	p.Samples().EnsureCapacity(it.Len())
	for s, ok := it.Next(); ok; s, ok = it.Next() {
		out := p.Samples().AppendEmpty()
		if v, ok := s.StackIndex(); ok {
			out.SetStackIndex(v)
		}
		int32s(out.AttributeIndices(), s.AttributeIndicesIter())
		if v, ok := s.LinkIndex(); ok {
			out.SetLinkIndex(v)
		}
		int64s(out.Values(), s.ValuesIter())
		uint64s(out.TimestampsUnixNano(), s.TimestampsUnixNanoIter())
	}
	if v, ok := src.TimeUnixNano(); ok {
		p.SetTime(pcommon.Timestamp(v))
	}
	if v, ok := src.DurationNano(); ok {
		p.SetDurationNano(v)
	}
	if v, ok := src.PeriodType(); ok {
		putValueType(p.PeriodType(), v)
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
	int32s(p.AttributeIndices(), src.AttributeIndicesIter())
}
