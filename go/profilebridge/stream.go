package profilebridge

import (
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pprofile"

	cb "otlpdagr/commonbridge"
	"otlpgen/dagr"
	cd "otlpgen/otlpcommondirect"
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
	lv cb.Levels

	kv    cb.Pool[cd.KeyValue]
	i32   cb.Pool[int32]
	i64   cb.Pool[int64]
	u64   cb.Pool[uint64]
	bytes cb.Pool[byte]
	strs  cb.Pool[string]
	smp   cb.Pool[d.Sample]
	maps  cb.Pool[d.Mapping]
	locs  cb.Pool[d.Location]
	lines cb.Pool[d.Line]
	funcs cb.Pool[d.Function]
	links cb.Pool[d.Link]
	attrs cb.Pool[d.KeyValueAndUnit]
	stks  cb.Pool[d.Stack]
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
	return cb.FinishGraph(b, dst)
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
	buf, i = cb.PutRecordEnvelope(buf, i, before, receivedUnixNano, uint32(pd.SampleCount()))
	b.Put(buf, i)
	return b.AppendTo(dst)
}

// putResourceProfiles writes the resource_profiles array content (no field tag).
func (w *StreamBuilder) putResourceProfiles(pd pprofile.Profiles, b *dagr.Builder, buf []byte, i int) ([]byte, int) {
	rps := pd.ResourceProfiles()
	return cb.PutArray(buf, i, rps.Len(), func(k int, buf []byte, i int) ([]byte, int) {
		rp := rps.At(k)
		sps := rp.ScopeProfiles()
		return cb.PutLevel(buf, i, rp.SchemaUrl(), sps.Len(),
			func(k int, buf []byte, i int) ([]byte, int) {
				sp := sps.At(k)
				ps := sp.Profiles()
				return cb.PutLevel(buf, i, sp.SchemaUrl(), ps.Len(),
					func(k int, buf []byte, i int) ([]byte, int) {
						return d.PutProfile(w.profileValue(ps.At(k)), b, buf, i)
					},
					func(buf []byte, i int) ([]byte, int) { return w.lv.PutScope(sp.Scope(), b, buf, i) })
			},
			func(buf []byte, i int) ([]byte, int) { return w.lv.PutResource(rp.Resource(), b, buf, i) })
	})
}

func (w *StreamBuilder) reset() {
	w.kv.Reset()
	w.i32.Reset()
	w.i64.Reset()
	w.u64.Reset()
	w.bytes.Reset()
	w.strs.Reset()
	w.smp.Reset()
	w.maps.Reset()
	w.locs.Reset()
	w.lines.Reset()
	w.funcs.Reset()
	w.links.Reset()
	w.attrs.Reset()
	w.stks.Reset()
}

func (w *StreamBuilder) i32s(s pcommon.Int32Slice) dagr.Opt[[]int32] {
	if s.Len() == 0 {
		return dagr.Opt[[]int32]{}
	}
	dst := w.i32.Take(s.Len())
	for k := range dst {
		dst[k] = s.At(k)
	}
	return cb.Some(dst)
}

func (w *StreamBuilder) i64s(s pcommon.Int64Slice) dagr.Opt[[]int64] {
	if s.Len() == 0 {
		return dagr.Opt[[]int64]{}
	}
	dst := w.i64.Take(s.Len())
	for k := range dst {
		dst[k] = s.At(k)
	}
	return cb.Some(dst)
}

func (w *StreamBuilder) u64s(s pcommon.UInt64Slice) dagr.Opt[[]uint64] {
	if s.Len() == 0 {
		return dagr.Opt[[]uint64]{}
	}
	dst := w.u64.Take(s.Len())
	for k := range dst {
		dst[k] = s.At(k)
	}
	return cb.Some(dst)
}

// putDictionary writes the ProfilesDictionary node — dictionary() over reused scratch.
// Like the value-tree bridge it is always written, and every table keeps entry 0.
func (w *StreamBuilder) putDictionary(dc pprofile.ProfilesDictionary, b *dagr.Builder, buf []byte, i int) ([]byte, int) {
	w.reset()
	out := d.ProfilesDictionary{}
	if st := dc.StringTable(); st.Len() > 0 {
		l := w.strs.Take(st.Len())
		for k := range l {
			l[k] = st.At(k)
		}
		out.StringTable = cb.Some(l)
	}
	if mt := dc.MappingTable(); mt.Len() > 0 {
		l := w.maps.Take(mt.Len())
		for k := range l {
			m := mt.At(k)
			l[k] = d.Mapping{
				MemoryStart:      cb.NonZero(m.MemoryStart()),
				MemoryLimit:      cb.NonZero(m.MemoryLimit()),
				FileOffset:       cb.NonZero(m.FileOffset()),
				FilenameStrindex: cb.NZ(m.FilenameStrindex()),
				AttributeIndices: w.i32s(m.AttributeIndices()),
			}
		}
		out.MappingTable = cb.Some(l)
	}
	if lt := dc.LocationTable(); lt.Len() > 0 {
		l := w.locs.Take(lt.Len())
		for k := range l {
			loc := lt.At(k)
			var lines dagr.Opt[[]d.Line]
			if ls := loc.Lines(); ls.Len() > 0 {
				ln := w.lines.Take(ls.Len())
				for j := range ln {
					x := ls.At(j)
					ln[j] = d.Line{FunctionIndex: cb.NZ(x.FunctionIndex()), Line: cb.NZ(x.Line()), Column: cb.NZ(x.Column())}
				}
				lines = cb.Some(ln)
			}
			l[k] = d.Location{
				MappingIndex:     cb.NZ(loc.MappingIndex()),
				Address:          cb.NonZero(loc.Address()),
				Lines:            lines,
				AttributeIndices: w.i32s(loc.AttributeIndices()),
			}
		}
		out.LocationTable = cb.Some(l)
	}
	if ft := dc.FunctionTable(); ft.Len() > 0 {
		l := w.funcs.Take(ft.Len())
		for k := range l {
			f := ft.At(k)
			l[k] = d.Function{
				NameStrindex:       cb.NZ(f.NameStrindex()),
				SystemNameStrindex: cb.NZ(f.SystemNameStrindex()),
				FilenameStrindex:   cb.NZ(f.FilenameStrindex()),
				StartLine:          cb.NZ(f.StartLine()),
			}
		}
		out.FunctionTable = cb.Some(l)
	}
	if kt := dc.LinkTable(); kt.Len() > 0 {
		l := w.links.Take(kt.Len())
		for k := range l {
			x := kt.At(k)
			tid, sid := x.TraceID(), x.SpanID()
			l[k] = d.Link{TraceID: cb.IDInto(&w.bytes, tid[:]), SpanID: cb.IDInto(&w.bytes, sid[:])}
		}
		out.LinkTable = cb.Some(l)
	}
	if at := dc.AttributeTable(); at.Len() > 0 {
		l := w.attrs.Take(at.Len())
		for k := range l {
			a := at.At(k)
			kv := d.KeyValueAndUnit{KeyStrindex: cb.NZ(a.KeyStrindex()), UnitStrindex: cb.NZ(a.UnitStrindex())}
			if v, ok := cb.AttrValue(a.Value()); ok {
				kv.Value = cb.Some(v)
			}
			l[k] = kv
		}
		out.AttributeTable = cb.Some(l)
	}
	if st := dc.StackTable(); st.Len() > 0 {
		l := w.stks.Take(st.Len())
		for k := range l {
			l[k] = d.Stack{LocationIndices: w.i32s(st.At(k).LocationIndices())}
		}
		out.StackTable = cb.Some(l)
	}
	return d.PutProfilesDictionary(out, b, buf, i)
}

// profileValue is toProfile over reused scratch: the same value, so the same bytes.
func (w *StreamBuilder) profileValue(p pprofile.Profile) d.Profile {
	w.reset()
	out := d.Profile{
		SampleType:             valueType(p.SampleType()),
		TimeUnixNano:           cb.NonZero(uint64(p.Time())),
		DurationNano:           cb.NonZero(p.DurationNano()),
		PeriodType:             valueType(p.PeriodType()),
		Period:                 cb.NZ(p.Period()),
		DroppedAttributesCount: cb.NonZeroU32(p.DroppedAttributesCount()),
		OriginalPayloadFormat:  cb.NonEmpty(p.OriginalPayloadFormat()),
		AttributeIndices:       w.i32s(p.AttributeIndices()),
	}
	if op := p.OriginalPayload(); op.Len() > 0 {
		l := w.bytes.Take(op.Len())
		for k := range l {
			l[k] = op.At(k)
		}
		out.OriginalPayload = cb.Some(l)
	}
	if id := p.ProfileID(); !id.IsEmpty() {
		out.ProfileID = cb.IDInto(&w.bytes, id[:])
	}
	if ss := p.Samples(); ss.Len() > 0 {
		l := w.smp.Take(ss.Len())
		for k := range l {
			s := ss.At(k)
			l[k] = d.Sample{
				StackIndex:         cb.NZ(s.StackIndex()),
				AttributeIndices:   w.i32s(s.AttributeIndices()),
				LinkIndex:          cb.NZ(s.LinkIndex()),
				Values:             w.i64s(s.Values()),
				TimestampsUnixNano: w.u64s(s.TimestampsUnixNano()),
			}
		}
		out.Samples = cb.Some(l)
	}
	return out
}
