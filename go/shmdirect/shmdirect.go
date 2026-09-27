//go:build unix

// Package shmdirect is direct metric instruments over shared memory (Dagr spec/38 §7.1,
// spec/40 §2): an app registers each stream once and then updates it with atomic
// operations in place — `Add` on a counter is ONE atomic add into a region the collector
// maps — with no SDK aggregation and no collect or export pass.
//
// Two regions per app, side by side in the shared directory:
//
//	<name>.dagrshm         the descriptors (MetricsRegion): scope, name, unit, kind,
//	                       attributes and histogram bounds of every stream, written when
//	                       a stream is registered
//	<name>.dagrshm.values  the values (MetricsValues, concurrency=atomic): slot i is
//	                       stream i's value, count, sum, min, max and bucket counts —
//	                       in Shards copies of the region, one per core (see below)
//
// The `dagrshm` receiver reads both; since values change with no export, it forwards a
// direct region on every poll, as a scrape would.
//
// Instruments are BOUND: the attribute set is fixed at registration, so a recording is the
// atomic operation and nothing else. (Recording with per-call attributes would need a
// lookup from attribute set to slot — the cost the OTel SDK pays on every Add.) This is
// not the OTel metric API; it is the measurement of what the region path costs.
//
// Per-core copies (spec/38 §7.1.1): an atomic add is cheap only while one core owns the
// cache line; every core adding to one stream makes the line bounce between them. So the
// values file holds one copy of the region per scheduler slot (Go's P, GOMAXPROCS of them
// by default), each on its own cache lines, and a recording goes to the copy of the P it
// runs on. The receiver adds the copies up. The count is chosen at startup, not in the
// schema: a SharedBuffer's size is fixed at codegen. GOMAXPROCS may change later (a
// container's CPU limit can); a P id past the count wraps around, and two Ps then share
// a copy — still correct, since every update stays atomic.
package shmdirect

import (
	"errors"
	"fmt"
	"math"
	"runtime"
	"sort"
	"sync"
	"time"
	_ "unsafe" // go:linkname

	mr "otlpgen/metricsregion"
	mv "otlpgen/metricsvalues"

	"otlpdagr/shm"
)

// Magic and Version are `MetricsValuesMagic` / `MetricsValuesVersion` in schema.py.
const (
	ValuesMagic   = 0x564D4744 // "DGMV" little-endian
	ValuesVersion = 2
)

// Stride is the distance between two copies of the values region in the file: a copy
// starts on a 128-byte boundary (Apple silicon's cache line; twice x86's), so no line
// holds words of two copies.
const Stride = (mv.ByteSize + 127) &^ 127

// MaxShards caps the copies: 64 of them are 5.8 MB, which a memory-backed volume charges
// to the pod.
const MaxShards = 64

// Options tune a Provider.
type Options struct {
	// Shards is the number of per-core copies of the values region: 0 is
	// runtime.GOMAXPROCS(0) (container-aware since Go 1.25), capped at MaxShards; 1 turns
	// sharding off.
	Shards int
}

// Values returns the copies in a values file: every copy's header must agree on the
// magic, version and count.
func Values(mem []byte) ([]mv.Values, error) {
	if len(mem) < mv.ByteSize {
		return nil, errors.New("shmdirect: values file shorter than one region")
	}
	n := int(mv.Wrap(mem).Shards())
	if n < 1 || n > MaxShards || len(mem) < (n-1)*Stride+mv.ByteSize {
		return nil, fmt.Errorf("shmdirect: values file of %d bytes cannot hold %d copies", len(mem), n)
	}
	vs := make([]mv.Values, n)
	for k := range vs {
		v := mv.Wrap(mem[k*Stride:])
		if v.Magic() != ValuesMagic || v.Version() != ValuesVersion || int(v.Shards()) != n {
			return nil, fmt.Errorf("shmdirect: values copy %d has a foreign header", k)
		}
		vs[k] = v
	}
	return vs, nil
}

//go:linkname procPin runtime.procPin
func procPin() int

//go:linkname procUnpin runtime.procUnpin
func procUnpin()

// slot is the copy of a stream a recording on this goroutine goes to: its P's. The P can
// change right after the unpin; that only means another P's copy takes the update,
// atomically. The pin is two runtime calls (~1.8 ns, and never inlined), so a single
// copy skips it. (A random copy was measured instead: 9.1 ns contended against 0.29 —
// spec/38 §7.1.1.)
func slot(vs []mv.Value) mv.Value {
	if len(vs) == 1 {
		return vs[0]
	}
	return vs[pinned(len(vs))]
}

func pinned(n int) int {
	id := procPin()
	procUnpin()
	if id < n { // always, unless GOMAXPROCS grew past the count: skip the divide
		return id
	}
	return id % n
}

// ErrFull: the regions hold 512 streams.
var ErrFull = errors.New("shmdirect: no free stream slot (a region holds 512)")

// DefaultBounds are the OTel SDK's default explicit histogram buckets.
var DefaultBounds = []float64{0, 5, 10, 25, 50, 75, 100, 250, 500, 750, 1000, 2500, 5000, 7500, 10000}

// Attr is one attribute of a stream: Value is a string, an int / int64, a float64 or a bool.
type Attr struct {
	Key   string
	Value any
}

// Provider owns an app's two regions.
type Provider struct {
	mu     sync.Mutex // registration: the descriptor region has one writer
	desc   *shm.Region
	vals   *shm.Mapping
	values []mv.Values // the per-core copies
	n      int
}

// NewProvider creates the regions at path (e.g. /run/otlp-dagr/checkout-1.dagrshm) for
// service, with resource attributes, and one values copy per P.
func NewProvider(path, service string, resource ...Attr) (*Provider, error) {
	return NewProviderWith(path, service, Options{}, resource...)
}

// NewProviderWith is NewProvider with Options.
func NewProviderWith(path, service string, o Options, resource ...Attr) (*Provider, error) {
	n := o.Shards
	if n <= 0 {
		n = runtime.GOMAXPROCS(0)
	}
	n = min(n, MaxShards)
	// the values first: a receiver that sees the descriptors must find them
	vals, err := shm.CreateMapping(path+".values", n*Stride, func(p []byte) {
		for k := 0; k < n; k++ {
			v := mv.Wrap(p[k*Stride:])
			v.SetMagic(ValuesMagic)
			v.SetVersion(ValuesVersion)
			v.SetShards(uint32(n))
		}
	})
	if err != nil {
		return nil, err
	}
	values, err := Values(vals.Mem)
	if err != nil {
		vals.Remove()
		return nil, err
	}
	desc, err := shm.Create(path, service)
	if err != nil {
		vals.Remove()
		return nil, err
	}
	w := desc.BeginWrite()
	for i, a := range resource {
		if i >= 16 || !putAttr(w.Resource(i), a) {
			break
		}
		w.SetResourceLen(uint8(i + 1))
	}
	w.SetStartUnixNano(uint64(time.Now().UnixNano()))
	desc.EndWrite()
	return &Provider{desc: desc, vals: vals, values: values}, nil
}

// Shards is the number of per-core copies of the values region.
func (p *Provider) Shards() int { return len(p.values) }

// Shutdown removes both regions: a collector stops seeing the app.
func (p *Provider) Shutdown() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	err := p.vals.Remove()
	if derr := p.desc.Remove(); err == nil {
		err = derr
	}
	return err
}

// register writes stream n's descriptor and hands out its value slot, one per copy.
func (p *Provider) register(scope, name, unit string, kind mr.PointKind, monotonic bool,
	bounds []float64, attrs []Attr) ([]mv.Value, error) {
	if len(scope) > 32 || len(name) > 64 || len(unit) > 16 || len(attrs) > 8 || len(bounds) > 15 {
		return nil, errors.New("shmdirect: a name, unit, scope, attribute set or bound list exceeds the region's capacity")
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.n == 512 {
		return nil, ErrFull
	}
	i := p.n
	vs := make([]mv.Value, len(p.values))
	for k, c := range p.values {
		vs[k] = c.Points(i)
		vs[k].SetMin(math.Inf(1))
		vs[k].SetMax(math.Inf(-1))
	}
	w := p.desc.BeginWrite()
	pt := w.Points(i)
	pt.SetScope(scope)
	pt.SetName(name)
	pt.SetUnit(unit)
	pt.SetKind(kind)
	pt.SetMonotonic(monotonic)
	for k, a := range attrs {
		if !putAttr(pt.Attrs(k), a) {
			p.desc.EndWrite()
			return nil, errors.New("shmdirect: an attribute key or value exceeds the region's capacity")
		}
	}
	pt.SetAttrsLen(uint8(len(attrs)))
	for k, b := range bounds {
		pt.SetBounds(k, b)
	}
	pt.SetBoundsLen(uint8(len(bounds)))
	if kind == mr.PointKindHistogram {
		pt.SetBucketCountsLen(uint8(len(bounds) + 1))
	}
	w.SetPointsLen(uint16(i + 1))
	w.SetExports(w.Exports() + 1)
	w.SetWriteUnixNano(uint64(time.Now().UnixNano()))
	p.desc.EndWrite()
	for _, c := range p.values {
		c.SetPointsLen(i + 1) // a reader takes the least length over the copies
	}
	p.n++
	return vs, nil
}

func putAttr(a mr.Attr, at Attr) bool {
	if len(at.Key) > 32 {
		return false
	}
	switch v := at.Value.(type) {
	case string:
		if len(v) > 64 {
			return false
		}
		a.SetKind(mr.AttrKindString)
		a.SetStringValue(v)
	case int:
		a.SetKind(mr.AttrKindInt)
		a.SetIntValue(int64(v))
	case int64:
		a.SetKind(mr.AttrKindInt)
		a.SetIntValue(v)
	case float64:
		a.SetKind(mr.AttrKindDouble)
		a.SetDoubleValue(v)
	case bool:
		a.SetKind(mr.AttrKindBool)
		if v {
			a.SetIntValue(1)
		} else {
			a.SetIntValue(0)
		}
	default:
		return false
	}
	a.SetKey(at.Key)
	return true
}

// Int64Counter is a monotonic sum.
type Int64Counter struct{ v []mv.Value }

func (p *Provider) Int64Counter(scope, name, unit string, attrs ...Attr) (*Int64Counter, error) {
	v, err := p.register(scope, name, unit, mr.PointKindSumInt, true, nil, attrs)
	return &Int64Counter{v}, err
}

// Add is one atomic add into shared memory.
func (c *Int64Counter) Add(n int64) { slot(c.v).AddIntValue(n) }

// Float64Counter is a monotonic sum of doubles.
type Float64Counter struct{ v []mv.Value }

func (p *Provider) Float64Counter(scope, name, unit string, attrs ...Attr) (*Float64Counter, error) {
	v, err := p.register(scope, name, unit, mr.PointKindSumDouble, true, nil, attrs)
	return &Float64Counter{v}, err
}

func (c *Float64Counter) Add(x float64) { slot(c.v).AddDoubleValue(x) }

// Int64UpDownCounter is a non-monotonic sum.
type Int64UpDownCounter struct{ v []mv.Value }

func (p *Provider) Int64UpDownCounter(scope, name, unit string, attrs ...Attr) (*Int64UpDownCounter, error) {
	v, err := p.register(scope, name, unit, mr.PointKindSumInt, false, nil, attrs)
	return &Int64UpDownCounter{v}, err
}

func (c *Int64UpDownCounter) Add(n int64) { slot(c.v).AddIntValue(n) }

// Float64Gauge holds the last recorded value — in copy 0 only: "last" cannot be merged
// over copies without a timestamp.
type Float64Gauge struct{ v []mv.Value }

func (p *Provider) Float64Gauge(scope, name, unit string, attrs ...Attr) (*Float64Gauge, error) {
	v, err := p.register(scope, name, unit, mr.PointKindGaugeDouble, false, nil, attrs)
	return &Float64Gauge{v}, err
}

func (g *Float64Gauge) Record(x float64) { g.v[0].SetDoubleValue(x) }

// Float64Histogram counts values into explicit buckets (nil bounds: DefaultBounds).
type Float64Histogram struct {
	v      []mv.Value
	bounds []float64
}

func (p *Provider) Float64Histogram(scope, name, unit string, bounds []float64, attrs ...Attr) (*Float64Histogram, error) {
	if bounds == nil {
		bounds = DefaultBounds
	}
	v, err := p.register(scope, name, unit, mr.PointKindHistogram, false, bounds, attrs)
	return &Float64Histogram{v: v, bounds: bounds}, err
}

// Record is a few atomic operations: the bucket, the count, the sum, and min / max when
// the value extends them.
func (h *Float64Histogram) Record(x float64) {
	v := slot(h.v)
	v.AddBuckets(sort.SearchFloat64s(h.bounds, x), 1) // x in (bounds[k-1], bounds[k]]
	v.AddCount(1)
	v.AddDoubleValue(x)
	for m := v.Min(); x < m && !v.CompareAndSwapMin(m, x); m = v.Min() {
	}
	for m := v.Max(); x > m && !v.CompareAndSwapMax(m, x); m = v.Max() {
	}
}
