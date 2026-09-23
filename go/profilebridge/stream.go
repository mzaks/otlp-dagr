package profilebridge

import (
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pprofile"

	"otlpgen/dagr"
	d "otlpgen/otlpprofilesdirect"
)

// StreamBuilder is the one-pass profiles encoder — pdatabridge.StreamBuilder's twin. pdata
// is walked once: the container levels (ProfilesData / ResourceProfiles / ScopeProfiles)
// are written by hand, and the two kinds of leaf — the ProfilesDictionary, once per batch,
// and each Profile with its samples — are built as value structs in reused scratch and
// written with their generated Put{N}. Every index and value array is copied into a
// pool instead of through pdata's allocating AsRaw. Once the pools have grown nothing
// allocates. TestStreamMatches gates the bytes against the value-tree path.
type StreamBuilder struct {
	b  *dagr.Builder
	lv levels

	kv    pool[d.KeyValue]
	i32   pool[int32]
	i64   pool[int64]
	u64   pool[uint64]
	bytes pool[byte]
	strs  pool[string]
	smp   pool[d.Sample]
	maps  pool[d.Mapping]
	locs  pool[d.Location]
	lines pool[d.Line]
	funcs pool[d.Function]
	links pool[d.Link]
	attrs pool[d.KeyValueAndUnit]
	stks  pool[d.Stack]
}

func NewStreamBuilder() *StreamBuilder { return &StreamBuilder{b: dagr.NewBuilder(0)} }

// BuildAppend encodes pd as an OtlpProfiles buffer into dst — the same bytes as ToBytes.
func (w *StreamBuilder) BuildAppend(dst []byte, pd pprofile.Profiles) []byte {
	b := w.b
	b.Reset()
	buf, i := b.Take()
	before := len(buf) - i
	buf, i = w.putDictionary(pd.Dictionary(), b, buf, i)
	buf, i = dagr.PutLEBAt(buf, i, 3) // ProfilesData.dictionary, field 1
	if pd.ResourceProfiles().Len() > 0 {
		buf, i = w.putResourceProfiles(pd, b, buf, i)
		buf, i = dagr.PutLEBAt(buf, i, 1) // ProfilesData.resource_profiles, field 0
	}
	buf, i = dagr.PutLEBAt(buf, i, uint64((len(buf)-i)-before))
	b.Put(buf, i)
	return finishGraph(b, dst)
}

// AppendRecord encodes pd as the content of one ProfileLog `Batch` record — what
// spool.Writer.AppendRecord takes — in the same single pass.
func (w *StreamBuilder) AppendRecord(dst []byte, receivedUnixNano uint64, pd pprofile.Profiles) []byte {
	b := w.b
	b.Reset()
	buf, i := b.Take()
	before := len(buf) - i
	buf, i = w.putDictionary(pd.Dictionary(), b, buf, i)
	buf, i = dagr.PutLEBAt(buf, i, 7) // Batch.dictionary, field 3
	buf, i = w.putResourceProfiles(pd, b, buf, i)
	buf, i = dagr.PutLEBAt(buf, i, 5) // Batch.resource_profiles, field 2 (required)
	buf, i = putRecordEnvelope(buf, i, before, receivedUnixNano, uint32(pd.SampleCount()))
	b.Put(buf, i)
	return b.AppendTo(dst)
}

// putResourceProfiles writes the resource_profiles array content (no field tag).
func (w *StreamBuilder) putResourceProfiles(pd pprofile.Profiles, b *dagr.Builder, buf []byte, i int) ([]byte, int) {
	rps := pd.ResourceProfiles()
	return putArray(buf, i, rps.Len(), func(k int, buf []byte, i int) ([]byte, int) {
		rp := rps.At(k)
		sps := rp.ScopeProfiles()
		return putLevel(buf, i, rp.SchemaUrl(), sps.Len(),
			func(k int, buf []byte, i int) ([]byte, int) {
				sp := sps.At(k)
				ps := sp.Profiles()
				return putLevel(buf, i, sp.SchemaUrl(), ps.Len(),
					func(k int, buf []byte, i int) ([]byte, int) {
						return d.PutProfile(w.profileValue(ps.At(k)), b, buf, i)
					},
					func(buf []byte, i int) ([]byte, int) { return w.lv.putScope(sp.Scope(), b, buf, i) })
			},
			func(buf []byte, i int) ([]byte, int) { return w.lv.putResource(rp.Resource(), b, buf, i) })
	})
}

func (w *StreamBuilder) reset() {
	w.kv.reset()
	w.i32.reset()
	w.i64.reset()
	w.u64.reset()
	w.bytes.reset()
	w.strs.reset()
	w.smp.reset()
	w.maps.reset()
	w.locs.reset()
	w.lines.reset()
	w.funcs.reset()
	w.links.reset()
	w.attrs.reset()
	w.stks.reset()
}

func (w *StreamBuilder) i32s(s pcommon.Int32Slice) dagr.Opt[[]int32] {
	if s.Len() == 0 {
		return dagr.Opt[[]int32]{}
	}
	dst := w.i32.take(s.Len())
	for k := range dst {
		dst[k] = s.At(k)
	}
	return some(dst)
}

func (w *StreamBuilder) i64s(s pcommon.Int64Slice) dagr.Opt[[]int64] {
	if s.Len() == 0 {
		return dagr.Opt[[]int64]{}
	}
	dst := w.i64.take(s.Len())
	for k := range dst {
		dst[k] = s.At(k)
	}
	return some(dst)
}

func (w *StreamBuilder) u64s(s pcommon.UInt64Slice) dagr.Opt[[]uint64] {
	if s.Len() == 0 {
		return dagr.Opt[[]uint64]{}
	}
	dst := w.u64.take(s.Len())
	for k := range dst {
		dst[k] = s.At(k)
	}
	return some(dst)
}

// putDictionary writes the ProfilesDictionary node — dictionary() over reused scratch.
// Like the value-tree bridge it is always written, and every table keeps entry 0.
func (w *StreamBuilder) putDictionary(dc pprofile.ProfilesDictionary, b *dagr.Builder, buf []byte, i int) ([]byte, int) {
	w.reset()
	out := d.ProfilesDictionary{}
	if st := dc.StringTable(); st.Len() > 0 {
		l := w.strs.take(st.Len())
		for k := range l {
			l[k] = st.At(k)
		}
		out.StringTable = some(l)
	}
	if mt := dc.MappingTable(); mt.Len() > 0 {
		l := w.maps.take(mt.Len())
		for k := range l {
			m := mt.At(k)
			l[k] = d.Mapping{
				MemoryStart:      nonZero(m.MemoryStart()),
				MemoryLimit:      nonZero(m.MemoryLimit()),
				FileOffset:       nonZero(m.FileOffset()),
				FilenameStrindex: nz(m.FilenameStrindex()),
				AttributeIndices: w.i32s(m.AttributeIndices()),
			}
		}
		out.MappingTable = some(l)
	}
	if lt := dc.LocationTable(); lt.Len() > 0 {
		l := w.locs.take(lt.Len())
		for k := range l {
			loc := lt.At(k)
			var lines dagr.Opt[[]d.Line]
			if ls := loc.Lines(); ls.Len() > 0 {
				ln := w.lines.take(ls.Len())
				for j := range ln {
					x := ls.At(j)
					ln[j] = d.Line{FunctionIndex: nz(x.FunctionIndex()), Line: nz(x.Line()), Column: nz(x.Column())}
				}
				lines = some(ln)
			}
			l[k] = d.Location{
				MappingIndex:     nz(loc.MappingIndex()),
				Address:          nonZero(loc.Address()),
				Lines:            lines,
				AttributeIndices: w.i32s(loc.AttributeIndices()),
			}
		}
		out.LocationTable = some(l)
	}
	if ft := dc.FunctionTable(); ft.Len() > 0 {
		l := w.funcs.take(ft.Len())
		for k := range l {
			f := ft.At(k)
			l[k] = d.Function{
				NameStrindex:       nz(f.NameStrindex()),
				SystemNameStrindex: nz(f.SystemNameStrindex()),
				FilenameStrindex:   nz(f.FilenameStrindex()),
				StartLine:          nz(f.StartLine()),
			}
		}
		out.FunctionTable = some(l)
	}
	if kt := dc.LinkTable(); kt.Len() > 0 {
		l := w.links.take(kt.Len())
		for k := range l {
			x := kt.At(k)
			tid, sid := x.TraceID(), x.SpanID()
			l[k] = d.Link{TraceID: idInto(&w.bytes, tid[:]), SpanID: idInto(&w.bytes, sid[:])}
		}
		out.LinkTable = some(l)
	}
	if at := dc.AttributeTable(); at.Len() > 0 {
		l := w.attrs.take(at.Len())
		for k := range l {
			a := at.At(k)
			kv := d.KeyValueAndUnit{KeyStrindex: nz(a.KeyStrindex()), UnitStrindex: nz(a.UnitStrindex())}
			if v, ok := attrValue(a.Value()); ok {
				kv.Value = some(v)
			}
			l[k] = kv
		}
		out.AttributeTable = some(l)
	}
	if st := dc.StackTable(); st.Len() > 0 {
		l := w.stks.take(st.Len())
		for k := range l {
			l[k] = d.Stack{LocationIndices: w.i32s(st.At(k).LocationIndices())}
		}
		out.StackTable = some(l)
	}
	return d.PutProfilesDictionary(out, b, buf, i)
}

// profileValue is toProfile over reused scratch: the same value, so the same bytes.
func (w *StreamBuilder) profileValue(p pprofile.Profile) d.Profile {
	w.reset()
	out := d.Profile{
		SampleType:             valueType(p.SampleType()),
		TimeUnixNano:           nonZero(uint64(p.Time())),
		DurationNano:           nonZero(p.DurationNano()),
		PeriodType:             valueType(p.PeriodType()),
		Period:                 nz(p.Period()),
		DroppedAttributesCount: nonZeroU32(p.DroppedAttributesCount()),
		OriginalPayloadFormat:  nonEmpty(p.OriginalPayloadFormat()),
		AttributeIndices:       w.i32s(p.AttributeIndices()),
	}
	if op := p.OriginalPayload(); op.Len() > 0 {
		l := w.bytes.take(op.Len())
		for k := range l {
			l[k] = op.At(k)
		}
		out.OriginalPayload = some(l)
	}
	if id := p.ProfileID(); !id.IsEmpty() {
		out.ProfileID = idInto(&w.bytes, id[:])
	}
	if ss := p.Samples(); ss.Len() > 0 {
		l := w.smp.take(ss.Len())
		for k := range l {
			s := ss.At(k)
			l[k] = d.Sample{
				StackIndex:         nz(s.StackIndex()),
				AttributeIndices:   w.i32s(s.AttributeIndices()),
				LinkIndex:          nz(s.LinkIndex()),
				Values:             w.i64s(s.Values()),
				TimestampsUnixNano: w.u64s(s.TimestampsUnixNano()),
			}
		}
		out.Samples = some(l)
	}
	return out
}
