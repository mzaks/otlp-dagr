// Package profilebridge converts between pdata profiles (`pprofile`, v0.x — profiles is
// `v1development` upstream) and an OTLP-on-Dagr `OtlpProfiles` buffer. Encode goes
// through the direct builder; decode (ToProfiles) is the compatibility path.
//
// The Dagr graph keeps the proto's shape exactly — every cross-reference an integer index
// into a ProfilesDictionary table — so this bridge is a field-for-field copy with no index
// translation (schema.py, "Profiles"). The shared messages convert through package
// commonbridge.
package profilebridge

import (
	"go.opentelemetry.io/collector/pdata/pprofile"

	cb "otlpdagr/commonbridge"
	"otlpgen/dagr"
	d "otlpgen/otlpprofilesdirect"
)

// ToBytes serializes pd into an OtlpProfiles buffer via the direct builder.
func ToBytes(pd pprofile.Profiles) []byte { return d.BuildOtlpProfiles(ToValue(pd)) }

// ToValue materialises the value tree the direct builder consumes.
func ToValue(pd pprofile.Profiles) d.ProfilesData {
	rps := pd.ResourceProfiles()
	outRP := make([]d.ResourceProfiles, 0, rps.Len())
	for i := 0; i < rps.Len(); i++ {
		rp := rps.At(i)
		sps := rp.ScopeProfiles()
		outSP := make([]d.ScopeProfiles, 0, sps.Len())
		for j := 0; j < sps.Len(); j++ {
			sp := sps.At(j)
			ps := sp.Profiles()
			outP := make([]d.Profile, 0, ps.Len())
			for k := 0; k < ps.Len(); k++ {
				outP = append(outP, toProfile(ps.At(k)))
			}
			outSP = append(outSP, d.ScopeProfiles{
				Scope:     cb.Some(cb.Scope(sp.Scope())),
				Profiles:  cb.OptSlice(outP),
				SchemaURL: cb.NonEmpty(sp.SchemaUrl()),
			})
		}
		outRP = append(outRP, d.ResourceProfiles{
			Resource:      cb.Some(cb.Resource(rp.Resource())),
			ScopeProfiles: cb.OptSlice(outSP),
			SchemaURL:     cb.NonEmpty(rp.SchemaUrl()),
		})
	}
	return d.ProfilesData{
		ResourceProfiles: cb.OptSlice(outRP),
		Dictionary:       dictionary(pd.Dictionary()),
	}
}

// Builder is the reusable form: one scratch buffer, reset per Build.
type Builder struct{ b *d.OtlpProfilesBuilder }

func NewBuilder() *Builder { return &Builder{b: d.NewOtlpProfilesBuilder()} }

func (w *Builder) Build(pd pprofile.Profiles) []byte { return w.b.Build(ToValue(pd)) }

// BuildAppend appends the encoded batch to dst and returns the extended slice.
func (w *Builder) BuildAppend(dst []byte, pd pprofile.Profiles) []byte {
	return w.b.BuildAppend(dst, ToValue(pd))
}

// dictionary copies every table. Entry 0 of each is the mandatory zero value, and it is
// an ordinary element here: an all-absent packed node, or "" in the string table — the
// index space must not shift by one, so nothing is special-cased or dropped.
func dictionary(dc pprofile.ProfilesDictionary) dagr.Opt[d.ProfilesDictionary] {
	out := d.ProfilesDictionary{StringTable: cb.OptSlice(dc.StringTable().AsRaw())}

	if mt := dc.MappingTable(); mt.Len() > 0 {
		lst := make([]d.Mapping, 0, mt.Len())
		for i := 0; i < mt.Len(); i++ {
			m := mt.At(i)
			lst = append(lst, d.Mapping{
				MemoryStart:      cb.NonZero(m.MemoryStart()),
				MemoryLimit:      cb.NonZero(m.MemoryLimit()),
				FileOffset:       cb.NonZero(m.FileOffset()),
				FilenameStrindex: cb.NZ(m.FilenameStrindex()),
				AttributeIndices: cb.OptSlice(m.AttributeIndices().AsRaw()),
			})
		}
		out.MappingTable = cb.Some(lst)
	}
	if lt := dc.LocationTable(); lt.Len() > 0 {
		lst := make([]d.Location, 0, lt.Len())
		for i := 0; i < lt.Len(); i++ {
			l := lt.At(i)
			var lines []d.Line
			if ls := l.Lines(); ls.Len() > 0 {
				lines = make([]d.Line, 0, ls.Len())
				for j := 0; j < ls.Len(); j++ {
					ln := ls.At(j)
					lines = append(lines, d.Line{
						FunctionIndex: cb.NZ(ln.FunctionIndex()),
						Line:          cb.NZ(ln.Line()),
						Column:        cb.NZ(ln.Column()),
					})
				}
			}
			lst = append(lst, d.Location{
				MappingIndex:     cb.NZ(l.MappingIndex()),
				Address:          cb.NonZero(l.Address()),
				Lines:            cb.OptSlice(lines),
				AttributeIndices: cb.OptSlice(l.AttributeIndices().AsRaw()),
			})
		}
		out.LocationTable = cb.Some(lst)
	}
	if ft := dc.FunctionTable(); ft.Len() > 0 {
		lst := make([]d.Function, 0, ft.Len())
		for i := 0; i < ft.Len(); i++ {
			f := ft.At(i)
			lst = append(lst, d.Function{
				NameStrindex:       cb.NZ(f.NameStrindex()),
				SystemNameStrindex: cb.NZ(f.SystemNameStrindex()),
				FilenameStrindex:   cb.NZ(f.FilenameStrindex()),
				StartLine:          cb.NZ(f.StartLine()),
			})
		}
		out.FunctionTable = cb.Some(lst)
	}
	if kt := dc.LinkTable(); kt.Len() > 0 {
		lst := make([]d.Link, 0, kt.Len())
		for i := 0; i < kt.Len(); i++ {
			k := kt.At(i)
			lst = append(lst, d.Link{TraceID: cb.TraceID(k.TraceID()), SpanID: cb.SpanID(k.SpanID())})
		}
		out.LinkTable = cb.Some(lst)
	}
	if at := dc.AttributeTable(); at.Len() > 0 {
		lst := make([]d.KeyValueAndUnit, 0, at.Len())
		for i := 0; i < at.Len(); i++ {
			a := at.At(i)
			kv := d.KeyValueAndUnit{
				KeyStrindex:  cb.NZ(a.KeyStrindex()),
				UnitStrindex: cb.NZ(a.UnitStrindex()),
			}
			if v, ok := cb.AttrValue(a.Value()); ok {
				kv.Value = cb.Some(v)
			}
			lst = append(lst, kv)
		}
		out.AttributeTable = cb.Some(lst)
	}
	if st := dc.StackTable(); st.Len() > 0 {
		lst := make([]d.Stack, 0, st.Len())
		for i := 0; i < st.Len(); i++ {
			lst = append(lst, d.Stack{LocationIndices: cb.OptSlice(st.At(i).LocationIndices().AsRaw())})
		}
		out.StackTable = cb.Some(lst)
	}
	return cb.Some(out)
}

func valueType(v pprofile.ValueType) dagr.Opt[d.ValueType] {
	if v.TypeStrindex() == 0 && v.UnitStrindex() == 0 {
		return dagr.Opt[d.ValueType]{}
	}
	return cb.Some(d.ValueType{TypeStrindex: cb.NZ(v.TypeStrindex()), UnitStrindex: cb.NZ(v.UnitStrindex())})
}

func toProfile(p pprofile.Profile) d.Profile {
	out := d.Profile{
		SampleType:             valueType(p.SampleType()),
		TimeUnixNano:           cb.NonZero(uint64(p.Time())),
		DurationNano:           cb.NonZero(p.DurationNano()),
		PeriodType:             valueType(p.PeriodType()),
		Period:                 cb.NZ(p.Period()),
		DroppedAttributesCount: cb.NonZeroU32(p.DroppedAttributesCount()),
		OriginalPayloadFormat:  cb.NonEmpty(p.OriginalPayloadFormat()),
		OriginalPayload:        cb.OptSlice(p.OriginalPayload().AsRaw()),
		AttributeIndices:       cb.OptSlice(p.AttributeIndices().AsRaw()),
	}
	if id := p.ProfileID(); !id.IsEmpty() {
		out.ProfileID = cb.Some(append([]byte(nil), id[:]...))
	}
	if ss := p.Samples(); ss.Len() > 0 {
		lst := make([]d.Sample, 0, ss.Len())
		for i := 0; i < ss.Len(); i++ {
			s := ss.At(i)
			lst = append(lst, d.Sample{
				StackIndex:         cb.NZ(s.StackIndex()),
				AttributeIndices:   cb.OptSlice(s.AttributeIndices().AsRaw()),
				LinkIndex:          cb.NZ(s.LinkIndex()),
				Values:             cb.OptSlice(s.Values().AsRaw()),
				TimestampsUnixNano: cb.OptSlice(s.TimestampsUnixNano().AsRaw()),
			})
		}
		out.Samples = cb.Some(lst)
	}
	return out
}
