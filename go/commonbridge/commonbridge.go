// Package commonbridge holds the conversions for the messages every signal shares
// (common.proto + resource.proto: AnyValue, KeyValue, InstrumentationScope, Resource),
// and the encode / decode helpers the four signal bridges have in common.
//
// It is one package because `OtlpCommon` is one: under `layout="package-per-graph"` Dagr
// emits an imported graph ONCE, as `otlpgen/otlpcommon` (+ `otlpcommondirect`), and each
// signal package refers to it instead of carrying its own copy (Dagr spec/37 §18). These
// used to be copied into every bridge by regen.sh, because each signal's KeyValue was a
// distinct Go type. OtlpProfilesGraph still holds OtlpCommon as vtable nodes — a
// different wire shape — so it keeps its own copy, and profilegraphbridge its own helpers.
package commonbridge

import (
	"go.opentelemetry.io/collector/pdata/pcommon"

	"otlpgen/dagr"
	g "otlpgen/otlpcommon"
	d "otlpgen/otlpcommondirect"
)

// Every field of a generated value struct is a dagr.Opt[T]: an absent field is not
// written at all, which is how the Dagr side stays byte-comparable with protobuf's
// proto3-default elision. These three helpers are the only place that decision lives.

func Some[T any](v T) dagr.Opt[T] { return dagr.Some(v) }

func NonEmpty(v string) dagr.Opt[string] {
	if v == "" {
		return dagr.Opt[string]{}
	}
	return dagr.Some(v)
}

func NonZero(v uint64) dagr.Opt[uint64] {
	if v == 0 {
		return dagr.Opt[uint64]{}
	}
	return dagr.Some(v)
}

func NonZeroU32(v uint32) dagr.Opt[uint32] {
	if v == 0 {
		return dagr.Opt[uint32]{}
	}
	return dagr.Some(v)
}

// nz is the general form of the three above: absent at the zero value, which is how
// protobuf elides a proto3 scalar. NOT for proto3 `optional` fields (histogram
// sum/min/max) — those carry presence of their own and are written from Has*().
func NZ[T comparable](v T) dagr.Opt[T] {
	var zero T
	if v == zero {
		return dagr.Opt[T]{}
	}
	return dagr.Some(v)
}

// traceID / spanID copy a pdata id out as an owned []byte, absent when all-zero (the
// proto's "invalid id", which protobuf elides as an empty bytes field).
func TraceID(id pcommon.TraceID) dagr.Opt[[]byte] {
	if id.IsEmpty() {
		return dagr.Opt[[]byte]{}
	}
	return dagr.Some(append([]byte(nil), id[:]...))
}

func SpanID(id pcommon.SpanID) dagr.Opt[[]byte] {
	if id.IsEmpty() {
		return dagr.Opt[[]byte]{}
	}
	return dagr.Some(append([]byte(nil), id[:]...))
}

// toTraceID / toSpanID are the decode side. A wrong-length id (a hostile or foreign
// buffer) copies what fits rather than panicking.
func ToTraceID(b []byte) pcommon.TraceID {
	var id pcommon.TraceID
	copy(id[:], b)
	return id
}

func ToSpanID(b []byte) pcommon.SpanID {
	var id pcommon.SpanID
	copy(id[:], b)
	return id
}

func OptSlice[T any](s []T) dagr.Opt[[]T] {
	if len(s) == 0 {
		return dagr.Opt[[]T]{}
	}
	return dagr.Some(s)
}

// ── encode: pdata -> Dagr bytes (direct builder, no arena) ───────────────────

// attrValue converts one pdata attribute value, recursing through Slice and Map.
//
// The recursion is why `ArrayValue` and `KeyValueList` are collapsed out of the schema:
// those two messages exist in the proto only because "oneof in AnyValue does not allow
// repeated fields", and a Dagr union takes an array variant directly. So a nested value
// maps straight onto `array_value` / `kvlist_value` with no wrapper node in between.
//
// The `false` return now means ONE thing — pcommon.ValueTypeEmpty, an attribute that is
// present but holds no value. Callers turn that into an ABSENT Dagr field rather than
// dropping the attribute, so an empty value survives the round trip as an empty value.
func AttrValue(v pcommon.Value) (d.AnyValueValue, bool) {
	switch v.Type() {
	case pcommon.ValueTypeStr:
		return d.AnyValueStringValue(v.Str()), true
	case pcommon.ValueTypeInt:
		return d.AnyValueIntValue(v.Int()), true
	case pcommon.ValueTypeDouble:
		return d.AnyValueDoubleValue(v.Double()), true
	case pcommon.ValueTypeBool:
		return d.AnyValueBoolValue(v.Bool()), true
	case pcommon.ValueTypeBytes:
		return d.AnyValueBytesValue(v.Bytes().AsRaw()), true
	case pcommon.ValueTypeSlice:
		return d.AnyValueArrayValue(AnyValueList(v.Slice())), true
	case pcommon.ValueTypeMap:
		return d.AnyValueKvlistValue(KeyValues(v.Map())), true
	default: // pcommon.ValueTypeEmpty
		return d.AnyValueValue{}, false
	}
}

// anyValueList converts a pdata Slice. The element type is an OPTIONAL union
// (`dagr.Opt[AnyValueValue]`), so an empty element keeps its position instead of
// collapsing the slice — which is the difference between `["a", , "c"]` and `["a", "c"]`.
//
// Nested values allocate, unlike the flat attribute path, and they have to: each level
// hands the builder a slice it keeps a pointer into, so the storage cannot be scratch
// that the next attribute overwrites. Scalar-only batches — which is nearly all OTLP
// traffic, and all of the benchmark's — never reach here and stay allocation-free.
func AnyValueList(s pcommon.Slice) d.AnyValueList {
	if s.Len() == 0 {
		return nil
	}
	out := make(d.AnyValueList, 0, s.Len())
	for i := 0; i < s.Len(); i++ {
		if av, ok := AttrValue(s.At(i)); ok {
			out = append(out, dagr.Some(av))
		} else {
			out = append(out, dagr.Opt[d.AnyValueValue]{})
		}
	}
	return out
}

func KeyValues(m pcommon.Map) []d.KeyValue {
	if m.Len() == 0 {
		return nil
	}
	out := make([]d.KeyValue, 0, m.Len())
	m.Range(func(k string, v pcommon.Value) bool {
		kv := d.KeyValue{Key: NonEmpty(k)}
		if av, ok := AttrValue(v); ok {
			kv.Value = Some(av)
		}
		out = append(out, kv)
		return true
	})
	return out
}

// resource converts a pcommon.Resource. pdata v1.67 exposes attributes and the dropped
// count only — `entity_refs` (Status: [Development]) has no pdata accessor yet, so the
// field stays absent, as it would in pdata's own protobuf marshal.
func Resource(r pcommon.Resource) d.Resource {
	return d.Resource{
		Attributes:             OptSlice(KeyValues(r.Attributes())),
		DroppedAttributesCount: NonZeroU32(r.DroppedAttributesCount()),
	}
}

// scope converts a pcommon.InstrumentationScope, all four fields.
func Scope(s pcommon.InstrumentationScope) d.InstrumentationScope {
	return d.InstrumentationScope{
		Name:                   NonEmpty(s.Name()),
		Version:                NonEmpty(s.Version()),
		Attributes:             OptSlice(KeyValues(s.Attributes())),
		DroppedAttributesCount: NonZeroU32(s.DroppedAttributesCount()),
	}
}

// ── one-pass encode: what every signal's StreamBuilder shares ──────────────────
//
// A one-pass encoder walks pdata once and writes straight into the buffer: the container
// levels by hand, and each leaf (a span, a metric, a log record, a profile) as a value
// struct built in reused scratch and written with its generated Put{N}. These are the
// pieces every signal needs for that.

// pool hands out slices of one reused backing array; reset it before each leaf. Growing
// allocates a fresh array, but the slices already handed out keep the old one, so they
// stay valid until the leaf is written — and once the pool has grown to the largest leaf
// it never allocates again.
type Pool[T any] struct {
	buf []T
	n   int
}

func (p *Pool[T]) Reset() { p.n = 0 }

func (p *Pool[T]) Take(k int) []T {
	if p.n+k > len(p.buf) {
		p.buf = make([]T, max(2*len(p.buf), k, 16))
		p.n = 0
	}
	s := p.buf[p.n : p.n+k : p.n+k]
	p.n += k
	return s
}

// attrsInto fills attributes from pool; absent when the map is empty.
func AttrsInto(p *Pool[d.KeyValue], m pcommon.Map) dagr.Opt[[]d.KeyValue] {
	if m.Len() == 0 {
		return dagr.Opt[[]d.KeyValue]{}
	}
	dst := p.Take(m.Len())
	n := 0
	m.Range(func(k string, v pcommon.Value) bool {
		kv := d.KeyValue{Key: NonEmpty(k)}
		if av, ok := AttrValue(v); ok {
			kv.Value = Some(av)
		}
		dst[n] = kv
		n++
		return true
	})
	return OptSlice(dst[:n])
}

// idInto copies a trace / span id into pool; absent when it is all zero.
func IDInto(p *Pool[byte], id []byte) dagr.Opt[[]byte] {
	for _, c := range id {
		if c != 0 {
			s := p.Take(len(id))
			copy(s, id)
			return Some(s)
		}
	}
	return dagr.Opt[[]byte]{}
}

// levels writes the two container levels every signal shares, Resource{X} and
// Scope{X}: both are {node 0, [items] 1, schema_url 2}, so field tags 1, 3 and 5.
type Levels struct{ KV Pool[d.KeyValue] }

// putLevel writes one container node: schema_url, the n items (item writes item k), then
// the head node (the Resource or the InstrumentationScope, always present — as the
// value-tree bridge writes it) and the node's size.
func PutLevel(buf []byte, i int, schemaURL string, n int,
	item func(k int, buf []byte, i int) ([]byte, int),
	head func(buf []byte, i int) ([]byte, int)) ([]byte, int) {
	before := len(buf) - i
	if schemaURL != "" {
		buf, i = dagr.PutUtf8TaggedAt(buf, i, schemaURL, 5)
	}
	if n > 0 {
		buf, i = PutArray(buf, i, n, item)
		buf, i = dagr.PutLEBAt(buf, i, 3)
	}
	buf, i = head(buf, i)
	buf, i = dagr.PutLEBAt(buf, i, 1)
	return dagr.PutLEBAt(buf, i, uint64((len(buf)-i)-before))
}

// putArray writes a packed node array's content — items last to first, count, block
// size — without the field tag.
func PutArray(buf []byte, i int, n int, item func(k int, buf []byte, i int) ([]byte, int)) ([]byte, int) {
	a := len(buf) - i
	for k := n - 1; k >= 0; k-- {
		buf, i = item(k, buf, i)
	}
	buf, i = dagr.PutLEBAt(buf, i, uint64(n))
	return dagr.PutLEBAt(buf, i, uint64((len(buf)-i)-a))
}

func (w *Levels) PutResource(r pcommon.Resource, b *dagr.Builder, buf []byte, i int) ([]byte, int) {
	w.KV.Reset()
	return d.PutResource(d.Resource{
		Attributes:             AttrsInto(&w.KV, r.Attributes()),
		DroppedAttributesCount: NonZeroU32(r.DroppedAttributesCount()),
	}, b, buf, i)
}

func (w *Levels) PutScope(s pcommon.InstrumentationScope, b *dagr.Builder, buf []byte, i int) ([]byte, int) {
	w.KV.Reset()
	return d.PutInstrumentationScope(d.InstrumentationScope{
		Name:                   NonEmpty(s.Name()),
		Version:                NonEmpty(s.Version()),
		Attributes:             AttrsInto(&w.KV, s.Attributes()),
		DroppedAttributesCount: NonZeroU32(s.DroppedAttributesCount()),
	}, b, buf, i)
}

// putRecordEnvelope wraps an already-written record body tail — every field from index
// 2 on — with the two envelope scalars every spool's Batch starts with
// (received_unix_nano: raw u64, field 0; the item count: u32, field 1) and the record's
// size. before is the builder position the body started at.
func PutRecordEnvelope(buf []byte, i, before int, receivedUnixNano uint64, count uint32) ([]byte, int) {
	if c := uint64(count); dagr.LebLength(c) < 4 {
		buf, i = dagr.PutLEBTaggedAt(buf, i, c, 2)
	} else {
		buf, i = dagr.PutFixedAt(buf, i, c, 4)
		buf, i = dagr.PutLEBAt(buf, i, 3)
	}
	buf, i = dagr.PutFixedAt(buf, i, receivedUnixNano, 8)
	buf, i = dagr.PutLEBAt(buf, i, 1)
	return dagr.PutLEBAt(buf, i, uint64((len(buf)-i)-before))
}

// finishGraph frames a graph buffer whose root node ends at the builder's cursor, as the
// generated build() does (no alignment: no OTLP graph has an aligned field).
func FinishGraph(b *dagr.Builder, dst []byte) []byte {
	off := b.Cursor()
	b.StoreLEB(uint64(b.Cursor()-off) << 2)
	return b.AppendTo(dst)
}

// ── decode: the shared messages, Dagr -> pdata ───────────────────────────────

// putResource copies a Dagr Resource into pdata.
func DecodeResource(dst pcommon.Resource, r g.ResourceAccessor) {
	DecodeAttrs(dst.Attributes(), r.AttributesIter())
	if v, ok := r.DroppedAttributesCount(); ok {
		dst.SetDroppedAttributesCount(v)
	}
}

// putScope copies a Dagr InstrumentationScope into pdata.
func DecodeScope(dst pcommon.InstrumentationScope, sc g.InstrumentationScopeAccessor) {
	if v, ok := sc.Name(); ok {
		dst.SetName(v)
	}
	if v, ok := sc.Version(); ok {
		dst.SetVersion(v)
	}
	DecodeAttrs(dst.Attributes(), sc.AttributesIter())
	if v, ok := sc.DroppedAttributesCount(); ok {
		dst.SetDroppedAttributesCount(v)
	}
}

// MaxValueDepth bounds how far ToTraces will follow nested attribute values.
//
// A Dagr buffer can nest `array_value` / `kvlist_value` arbitrarily, and this decoder is
// recursive, so an adversarial or corrupt buffer could otherwise exhaust the goroutine
// stack — a crash that the reader's own bounds checking does not prevent, because every
// individual read is perfectly valid. Beyond the limit the value is left EMPTY rather
// than silently truncated to something that looks like data. Protobuf's own decoder
// takes the same precaution (a recursion limit, 100 by default); 32 is far past any
// real OTLP attribute, which is rarely nested at all.
const MaxValueDepth = 32

// Cursor / kvCursor: any generated `{Node}{Field}Iter`, e.g. `SpanAttributesIter` (or any other cursor over a
// []KeyValue): a value type whose pointer has Next and Len. Taking the cursor by value
// through this constraint keeps the walk allocation-free — an iter.Seq passed through a
// func parameter heap-allocates its closure, which cost the decoders ~2,500 allocations
// per 500-span batch when they first moved off At(i).
type Cursor[I, T any] interface {
	*I
	Next() (T, bool)
	Len() int
}

type kvCursor[I any] interface{ Cursor[I, g.KeyValueAccessor] }

// putAttrs copies a Dagr []KeyValue into a pcommon.Map.
//
// It walks the array with the CURSOR, never `{F}At(i)`: a packed array has no offset
// table, so At(i) skips i elements from the start and a loop over it is O(n²) — the
// decoders did exactly that until the metrics port found it (spec/38 §10.2).
func DecodeAttrs[I any, P kvCursor[I]](dst pcommon.Map, it I) {
	c := P(&it)
	n := c.Len()
	if n == 0 {
		return
	}
	dst.EnsureCapacity(n)
	for kv, ok := c.Next(); ok; kv, ok = c.Next() {
		key, ok := kv.Key()
		if !ok {
			continue
		}
		// An absent value is an attribute that is present and empty — pdata spells that
		// ValueTypeEmpty — so the key is kept either way.
		slot := dst.PutEmpty(key)
		if v, ok := kv.Value(); ok {
			DecodeValue(slot, v, 0)
		}
	}
}

// putValue materialises one Dagr union value into a pdata Value, recursing through
// array_value and kvlist_value. An unknown tag leaves the value empty, which is how a
// reader built against an older schema survives a newer writer.
func DecodeValue(dst pcommon.Value, v g.AnyValuePackedView, depth int) {
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
		decodeSlice(dst.SetEmptySlice(), v, depth+1)
	case g.AnyValueTagKvlistValue:
		decodeKvlist(dst.SetEmptyMap(), v, depth+1)
	}
}

// putSlice copies an array_value. Elements are optional on the wire, and an absent one
// keeps its position as an empty value — dropping it would renumber the slice.
func decodeSlice(dst pcommon.Slice, v g.AnyValuePackedView, depth int) {
	av := v.ArrayValue()
	dst.EnsureCapacity(av.Len())
	for elem, present := range av.All() {
		slot := dst.AppendEmpty()
		if present {
			DecodeValue(slot, elem, depth)
		}
	}
}

// putKvlist copies a kvlist_value. Its elements are KeyValue nodes, so they come through
// the node accessors rather than the union array view.
func decodeKvlist(dst pcommon.Map, v g.AnyValuePackedView, depth int) {
	it := v.KvlistValueIter()
	dst.EnsureCapacity(it.Len())
	for kv, ok := it.Next(); ok; kv, ok = it.Next() {
		key, ok := kv.Key()
		if !ok {
			continue
		}
		slot := dst.PutEmpty(key)
		if inner, ok := kv.Value(); ok {
			DecodeValue(slot, inner, depth)
		}
	}
}
