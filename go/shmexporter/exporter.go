//go:build unix

// Package shmexporter is an OpenTelemetry Go SDK metric exporter that publishes into a
// shared-memory metrics region (package shm, Dagr spec/38 §7.1) instead of sending.
//
// Drive it with a PeriodicReader, as any push exporter:
//
//	exp, _ := shmexporter.New("/run/otlp-dagr/checkout-1.dagrshm", "checkout")
//	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(sdkmetric.NewPeriodicReader(exp)))
//
// The SDK still aggregates; each export is ONE seqlock write of the current, cumulative
// state — a store into shared memory, no serialisation to a wire format, no syscall, no
// queue — and the app never waits for the collector, which reads the region at its own
// cadence. A point that does not fit the region (more points than it holds, more
// attributes or buckets, a longer name or value) is dropped and counted in the region's
// `dropped_points`, never written in part.
package shmexporter

import (
	"context"
	"sync"

	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	mr "otlpgen/metricsregion"

	"otlpdagr/shm"
)

// Caps of the region layout (schema.py, MetricsRegion). Checked before any byte of a
// point is written, so an overflowing point is skipped whole.
const (
	maxPoints   = 512
	maxAttrs    = 8
	maxResource = 16
	maxScope    = 32
	maxName     = 64
	maxUnit     = 16
	maxKey      = 32
	maxStrValue = 64
	maxBounds   = 15
	maxBuckets  = 16
	maxService  = 64
)

// Exporter writes each export into one region.
type Exporter struct {
	mu      sync.Mutex // the seqlock takes ONE writer
	region  *shm.Region
	exports uint64
	start   int64 // the cumulative start time, from the first export
}

var _ sdkmetric.Exporter = (*Exporter)(nil)

// New creates the region file at path (replacing a stale one) for service.
func New(path, service string) (*Exporter, error) {
	if len(service) > maxService {
		service = service[:maxService]
	}
	r, err := shm.Create(path, service)
	if err != nil {
		return nil, err
	}
	return &Exporter{region: r}, nil
}

// Temporality is cumulative for every instrument: the region is the current state.
func (e *Exporter) Temporality(sdkmetric.InstrumentKind) metricdata.Temporality {
	return metricdata.CumulativeTemporality
}

func (e *Exporter) Aggregation(k sdkmetric.InstrumentKind) sdkmetric.Aggregation {
	return sdkmetric.DefaultAggregationSelector(k)
}

// Export publishes rm as the region's new state, in one seqlock write.
func (e *Exporter) Export(_ context.Context, rm *metricdata.ResourceMetrics) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.region == nil {
		return nil
	}
	e.exports++
	w := e.region.BeginWrite()
	defer e.region.EndWrite()

	nres := 0
	if rm.Resource != nil {
		for it := rm.Resource.Iter(); it.Next(); {
			kv := it.Attribute()
			if kv.Key == "service.name" {
				if s := kv.Value.AsString(); len(s) <= maxService {
					w.SetService(s)
				}
			}
			if nres < maxResource && putAttr(w.Resource(nres), kv) {
				nres++
			}
		}
	}
	w.SetResourceLen(uint8(nres))

	n, dropped := 0, uint32(0)
	var last int64
	for i := range rm.ScopeMetrics {
		sm := &rm.ScopeMetrics[i]
		scope := sm.Scope.Name
		if len(scope) > maxScope {
			scope = scope[:maxScope] // a scope name is context, not identity: truncate
		}
		for j := range sm.Metrics {
			m := &sm.Metrics[j]
			if len(m.Name) > maxName || len(m.Unit) > maxUnit {
				dropped += uint32(countPoints(m.Data))
				continue
			}
			put := func(set *attribute.Set, t int64, fill func(p mr.Point) bool) {
				if n == maxPoints || set.Len() > maxAttrs {
					dropped++
					return
				}
				p := w.Points(n)
				na := 0
				for it := set.Iter(); it.Next(); {
					if !putAttr(p.Attrs(na), it.Attribute()) {
						dropped++
						return
					}
					na++
				}
				if !fill(p) {
					dropped++
					return
				}
				p.SetAttrsLen(uint8(na))
				p.SetScope(scope)
				p.SetName(m.Name)
				p.SetUnit(m.Unit)
				p.SetTimeUnixNano(uint64(t))
				if t > last {
					last = t
				}
				n++
			}
			switch d := m.Data.(type) {
			case metricdata.Gauge[int64]:
				for k := range d.DataPoints {
					dp := &d.DataPoints[k]
					e.noteStart(dp.StartTime.UnixNano())
					put(&dp.Attributes, dp.Time.UnixNano(), func(p mr.Point) bool {
						p.SetKind(mr.PointKindGaugeInt)
						p.SetMonotonic(false)
						p.SetIntValue(dp.Value)
						return true
					})
				}
			case metricdata.Gauge[float64]:
				for k := range d.DataPoints {
					dp := &d.DataPoints[k]
					e.noteStart(dp.StartTime.UnixNano())
					put(&dp.Attributes, dp.Time.UnixNano(), func(p mr.Point) bool {
						p.SetKind(mr.PointKindGaugeDouble)
						p.SetMonotonic(false)
						p.SetDoubleValue(dp.Value)
						return true
					})
				}
			case metricdata.Sum[int64]:
				for k := range d.DataPoints {
					dp := &d.DataPoints[k]
					e.noteStart(dp.StartTime.UnixNano())
					put(&dp.Attributes, dp.Time.UnixNano(), func(p mr.Point) bool {
						p.SetKind(mr.PointKindSumInt)
						p.SetMonotonic(d.IsMonotonic)
						p.SetIntValue(dp.Value)
						return true
					})
				}
			case metricdata.Sum[float64]:
				for k := range d.DataPoints {
					dp := &d.DataPoints[k]
					e.noteStart(dp.StartTime.UnixNano())
					put(&dp.Attributes, dp.Time.UnixNano(), func(p mr.Point) bool {
						p.SetKind(mr.PointKindSumDouble)
						p.SetMonotonic(d.IsMonotonic)
						p.SetDoubleValue(dp.Value)
						return true
					})
				}
			case metricdata.Histogram[int64]:
				for k := range d.DataPoints {
					dp := &d.DataPoints[k]
					e.noteStart(dp.StartTime.UnixNano())
					put(&dp.Attributes, dp.Time.UnixNano(), func(p mr.Point) bool {
						mn, okMin := dp.Min.Value()
						mx, okMax := dp.Max.Value()
						return putHistogram(p, dp.Count, float64(dp.Sum), dp.Bounds, dp.BucketCounts,
							float64(mn), okMin, float64(mx), okMax)
					})
				}
			case metricdata.Histogram[float64]:
				for k := range d.DataPoints {
					dp := &d.DataPoints[k]
					e.noteStart(dp.StartTime.UnixNano())
					put(&dp.Attributes, dp.Time.UnixNano(), func(p mr.Point) bool {
						mn, okMin := dp.Min.Value()
						mx, okMax := dp.Max.Value()
						return putHistogram(p, dp.Count, dp.Sum, dp.Bounds, dp.BucketCounts,
							mn, okMin, mx, okMax)
					})
				}
			default: // exponential histograms, summaries: not carried
				dropped += uint32(countPoints(m.Data))
			}
		}
	}
	w.SetPointsLen(uint16(n))
	w.SetDroppedPoints(dropped)
	w.SetStartUnixNano(uint64(e.start))
	w.SetWriteUnixNano(uint64(last))
	w.SetExports(e.exports)
	return nil
}

func (e *Exporter) noteStart(t int64) {
	if e.start == 0 || (t > 0 && t < e.start) {
		e.start = t
	}
}

func putHistogram(p mr.Point, count uint64, sum float64, bounds []float64, counts []uint64,
	mn float64, okMin bool, mx float64, okMax bool) bool {
	if len(bounds) > maxBounds || len(counts) > maxBuckets {
		return false
	}
	p.SetKind(mr.PointKindHistogram)
	p.SetMonotonic(false)
	p.SetCount(count)
	p.SetDoubleValue(sum)
	for i, b := range bounds {
		p.SetBounds(i, b)
	}
	p.SetBoundsLen(uint8(len(bounds)))
	for i, c := range counts {
		p.SetBucketCounts(i, c)
	}
	p.SetBucketCountsLen(uint8(len(counts)))
	if okMin {
		p.SetMin(mn)
	} else {
		p.ClearMin()
	}
	if okMax {
		p.SetMax(mx)
	} else {
		p.ClearMax()
	}
	return true
}

// putAttr writes one attribute; false when it does not fit (the caller drops the point).
func putAttr(a mr.Attr, kv attribute.KeyValue) bool {
	if len(kv.Key) > maxKey {
		return false
	}
	switch kv.Value.Type() {
	case attribute.BOOL:
		a.SetKind(mr.AttrKindBool)
		if kv.Value.AsBool() {
			a.SetIntValue(1)
		} else {
			a.SetIntValue(0)
		}
	case attribute.INT64:
		a.SetKind(mr.AttrKindInt)
		a.SetIntValue(kv.Value.AsInt64())
	case attribute.FLOAT64:
		a.SetKind(mr.AttrKindDouble)
		a.SetDoubleValue(kv.Value.AsFloat64())
	default: // STRING, and slices as their string form
		s := kv.Value.AsString()
		if kv.Value.Type() != attribute.STRING {
			s = kv.Value.Emit()
		}
		if len(s) > maxStrValue {
			return false
		}
		a.SetKind(mr.AttrKindString)
		a.SetStringValue(s)
	}
	a.SetKey(string(kv.Key))
	return true
}

func countPoints(a metricdata.Aggregation) int {
	switch d := a.(type) {
	case metricdata.Gauge[int64]:
		return len(d.DataPoints)
	case metricdata.Gauge[float64]:
		return len(d.DataPoints)
	case metricdata.Sum[int64]:
		return len(d.DataPoints)
	case metricdata.Sum[float64]:
		return len(d.DataPoints)
	case metricdata.Histogram[int64]:
		return len(d.DataPoints)
	case metricdata.Histogram[float64]:
		return len(d.DataPoints)
	case metricdata.ExponentialHistogram[int64]:
		return len(d.DataPoints)
	case metricdata.ExponentialHistogram[float64]:
		return len(d.DataPoints)
	case metricdata.Summary:
		return len(d.DataPoints)
	}
	return 0
}

// ForceFlush has nothing to flush: every export is already in the region.
func (e *Exporter) ForceFlush(context.Context) error { return nil }

// Shutdown unmaps the region and removes its file: a collector stops seeing the app.
func (e *Exporter) Shutdown(context.Context) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.region == nil {
		return nil
	}
	err := e.region.Remove()
	e.region = nil
	return err
}
