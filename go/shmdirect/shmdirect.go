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
//	                       stream i's value, count, sum, min, max and bucket counts
//
// The `dagrshm` receiver reads both; since values change with no export, it forwards a
// direct region on every poll, as a scrape would.
//
// Instruments are BOUND: the attribute set is fixed at registration, so a recording is the
// atomic operation and nothing else. (Recording with per-call attributes would need a
// lookup from attribute set to slot — the cost the OTel SDK pays on every Add.) This is
// not the OTel metric API; it is the measurement of what the region path costs.
package shmdirect

import (
	"errors"
	"math"
	"sort"
	"sync"
	"time"

	mr "otlpgen/metricsregion"
	mv "otlpgen/metricsvalues"

	"otlpdagr/shm"
)

// Magic and Version are `MetricsValuesMagic` / `MetricsValuesVersion` in schema.py.
const (
	ValuesMagic   = 0x564D4744 // "DGMV" little-endian
	ValuesVersion = 1
)

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
	values mv.Values
	n      int
}

// NewProvider creates the regions at path (e.g. /run/otlp-dagr/checkout-1.dagrshm) for
// service, with resource attributes.
func NewProvider(path, service string, resource ...Attr) (*Provider, error) {
	// the values first: a receiver that sees the descriptors must find them
	vals, err := shm.CreateMapping(path+".values", mv.ByteSize, func(p []byte) {
		v := mv.Wrap(p)
		v.SetMagic(ValuesMagic)
		v.SetVersion(ValuesVersion)
	})
	if err != nil {
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
	return &Provider{desc: desc, vals: vals, values: mv.Wrap(vals.Mem)}, nil
}

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

// register writes stream n's descriptor and hands out its value slot.
func (p *Provider) register(scope, name, unit string, kind mr.PointKind, monotonic bool,
	bounds []float64, attrs []Attr) (mv.Value, error) {
	if len(scope) > 32 || len(name) > 64 || len(unit) > 16 || len(attrs) > 8 || len(bounds) > 15 {
		return mv.Value{}, errors.New("shmdirect: a name, unit, scope, attribute set or bound list exceeds the region's capacity")
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.n == 512 {
		return mv.Value{}, ErrFull
	}
	i := p.n
	v := p.values.Points(i)
	v.SetMin(math.Inf(1))
	v.SetMax(math.Inf(-1))
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
			return mv.Value{}, errors.New("shmdirect: an attribute key or value exceeds the region's capacity")
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
	p.values.SetPointsLen(i + 1)
	p.n++
	return v, nil
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
type Int64Counter struct{ v mv.Value }

func (p *Provider) Int64Counter(scope, name, unit string, attrs ...Attr) (*Int64Counter, error) {
	v, err := p.register(scope, name, unit, mr.PointKindSumInt, true, nil, attrs)
	return &Int64Counter{v}, err
}

// Add is one atomic add into shared memory.
func (c *Int64Counter) Add(n int64) { c.v.AddIntValue(n) }

// Float64Counter is a monotonic sum of doubles.
type Float64Counter struct{ v mv.Value }

func (p *Provider) Float64Counter(scope, name, unit string, attrs ...Attr) (*Float64Counter, error) {
	v, err := p.register(scope, name, unit, mr.PointKindSumDouble, true, nil, attrs)
	return &Float64Counter{v}, err
}

func (c *Float64Counter) Add(x float64) { c.v.AddDoubleValue(x) }

// Int64UpDownCounter is a non-monotonic sum.
type Int64UpDownCounter struct{ v mv.Value }

func (p *Provider) Int64UpDownCounter(scope, name, unit string, attrs ...Attr) (*Int64UpDownCounter, error) {
	v, err := p.register(scope, name, unit, mr.PointKindSumInt, false, nil, attrs)
	return &Int64UpDownCounter{v}, err
}

func (c *Int64UpDownCounter) Add(n int64) { c.v.AddIntValue(n) }

// Float64Gauge holds the last recorded value.
type Float64Gauge struct{ v mv.Value }

func (p *Provider) Float64Gauge(scope, name, unit string, attrs ...Attr) (*Float64Gauge, error) {
	v, err := p.register(scope, name, unit, mr.PointKindGaugeDouble, false, nil, attrs)
	return &Float64Gauge{v}, err
}

func (g *Float64Gauge) Record(x float64) { g.v.SetDoubleValue(x) }

// Float64Histogram counts values into explicit buckets (nil bounds: DefaultBounds).
type Float64Histogram struct {
	v      mv.Value
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
	h.v.AddBuckets(sort.SearchFloat64s(h.bounds, x), 1) // x in (bounds[k-1], bounds[k]]
	h.v.AddCount(1)
	h.v.AddDoubleValue(x)
	for m := h.v.Min(); x < m && !h.v.CompareAndSwapMin(m, x); m = h.v.Min() {
	}
	for m := h.v.Max(); x > m && !h.v.CompareAndSwapMax(m, x); m = h.v.Max() {
	}
}
