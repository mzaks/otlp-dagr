// Package profilebridge converts between pdata profiles (`pprofile`, v0.x — profiles is
// `v1development` upstream) and an OTLP-on-Dagr `OtlpProfiles` buffer. Encode goes
// through the direct builder; decode (ToProfiles) is the compatibility path.
//
// The Dagr graph keeps the proto's shape exactly — every cross-reference an integer index
// into a ProfilesDictionary table — so this bridge is a field-for-field copy with no index
// translation (schema.py, "Profiles"). The shared messages come from common_gen.go, which
// regen.sh copies from pdatabridge/common.go.
package profilebridge

import (
	"go.opentelemetry.io/collector/pdata/pprofile"

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
				Scope:     some(scope(sp.Scope())),
				Profiles:  optSlice(outP),
				SchemaURL: nonEmpty(sp.SchemaUrl()),
			})
		}
		outRP = append(outRP, d.ResourceProfiles{
			Resource:      some(resource(rp.Resource())),
			ScopeProfiles: optSlice(outSP),
			SchemaURL:     nonEmpty(rp.SchemaUrl()),
		})
	}
	return d.ProfilesData{
		ResourceProfiles: optSlice(outRP),
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
	out := d.ProfilesDictionary{StringTable: optSlice(dc.StringTable().AsRaw())}

	if mt := dc.MappingTable(); mt.Len() > 0 {
		lst := make([]d.Mapping, 0, mt.Len())
		for i := 0; i < mt.Len(); i++ {
			m := mt.At(i)
			lst = append(lst, d.Mapping{
				MemoryStart:      nonZero(m.MemoryStart()),
				MemoryLimit:      nonZero(m.MemoryLimit()),
				FileOffset:       nonZero(m.FileOffset()),
				FilenameStrindex: nz(m.FilenameStrindex()),
				AttributeIndices: optSlice(m.AttributeIndices().AsRaw()),
			})
		}
		out.MappingTable = some(lst)
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
						FunctionIndex: nz(ln.FunctionIndex()),
						Line:          nz(ln.Line()),
						Column:        nz(ln.Column()),
					})
				}
			}
			lst = append(lst, d.Location{
				MappingIndex:     nz(l.MappingIndex()),
				Address:          nonZero(l.Address()),
				Lines:            optSlice(lines),
				AttributeIndices: optSlice(l.AttributeIndices().AsRaw()),
			})
		}
		out.LocationTable = some(lst)
	}
	if ft := dc.FunctionTable(); ft.Len() > 0 {
		lst := make([]d.Function, 0, ft.Len())
		for i := 0; i < ft.Len(); i++ {
			f := ft.At(i)
			lst = append(lst, d.Function{
				NameStrindex:       nz(f.NameStrindex()),
				SystemNameStrindex: nz(f.SystemNameStrindex()),
				FilenameStrindex:   nz(f.FilenameStrindex()),
				StartLine:          nz(f.StartLine()),
			})
		}
		out.FunctionTable = some(lst)
	}
	if kt := dc.LinkTable(); kt.Len() > 0 {
		lst := make([]d.Link, 0, kt.Len())
		for i := 0; i < kt.Len(); i++ {
			k := kt.At(i)
			lst = append(lst, d.Link{TraceID: traceID(k.TraceID()), SpanID: spanID(k.SpanID())})
		}
		out.LinkTable = some(lst)
	}
	if at := dc.AttributeTable(); at.Len() > 0 {
		lst := make([]d.KeyValueAndUnit, 0, at.Len())
		for i := 0; i < at.Len(); i++ {
			a := at.At(i)
			kv := d.KeyValueAndUnit{
				KeyStrindex:  nz(a.KeyStrindex()),
				UnitStrindex: nz(a.UnitStrindex()),
			}
			if v, ok := attrValue(a.Value()); ok {
				kv.Value = some(v)
			}
			lst = append(lst, kv)
		}
		out.AttributeTable = some(lst)
	}
	if st := dc.StackTable(); st.Len() > 0 {
		lst := make([]d.Stack, 0, st.Len())
		for i := 0; i < st.Len(); i++ {
			lst = append(lst, d.Stack{LocationIndices: optSlice(st.At(i).LocationIndices().AsRaw())})
		}
		out.StackTable = some(lst)
	}
	return some(out)
}

func valueType(v pprofile.ValueType) dagr.Opt[d.ValueType] {
	if v.TypeStrindex() == 0 && v.UnitStrindex() == 0 {
		return dagr.Opt[d.ValueType]{}
	}
	return some(d.ValueType{TypeStrindex: nz(v.TypeStrindex()), UnitStrindex: nz(v.UnitStrindex())})
}

func toProfile(p pprofile.Profile) d.Profile {
	out := d.Profile{
		SampleType:             valueType(p.SampleType()),
		TimeUnixNano:           nonZero(uint64(p.Time())),
		DurationNano:           nonZero(p.DurationNano()),
		PeriodType:             valueType(p.PeriodType()),
		Period:                 nz(p.Period()),
		DroppedAttributesCount: nonZeroU32(p.DroppedAttributesCount()),
		OriginalPayloadFormat:  nonEmpty(p.OriginalPayloadFormat()),
		OriginalPayload:        optSlice(p.OriginalPayload().AsRaw()),
		AttributeIndices:       optSlice(p.AttributeIndices().AsRaw()),
	}
	if id := p.ProfileID(); !id.IsEmpty() {
		out.ProfileID = some(append([]byte(nil), id[:]...))
	}
	if ss := p.Samples(); ss.Len() > 0 {
		lst := make([]d.Sample, 0, ss.Len())
		for i := 0; i < ss.Len(); i++ {
			s := ss.At(i)
			lst = append(lst, d.Sample{
				StackIndex:         nz(s.StackIndex()),
				AttributeIndices:   optSlice(s.AttributeIndices().AsRaw()),
				LinkIndex:          nz(s.LinkIndex()),
				Values:             optSlice(s.Values().AsRaw()),
				TimestampsUnixNano: optSlice(s.TimestampsUnixNano().AsRaw()),
			})
		}
		out.Samples = some(lst)
	}
	return out
}
