// Package dagrshmreceiver is an OpenTelemetry Collector metrics receiver that reads the
// shared-memory metrics regions applications publish with package shmexporter (Dagr
// spec/38 §7.1). It polls a directory, snapshots each region under its seqlock, and
// forwards the region's state as cumulative pmetric data — the same-host hop with no
// serialisation, no socket and no queue in between.
package dagrshmreceiver

import (
	"math"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"

	mr "otlpgen/metricsregion"
	mv "otlpgen/metricsvalues"
)

// ToMetrics converts one region snapshot to pdata: one ResourceMetrics (the region's
// resource), one ScopeMetrics per scope, one Metric per (name, unit, kind, monotonic) in
// the order the points first name it, and the points under it. Sums and histograms are
// cumulative from the region's start time.
func ToMetrics(r mr.State) pmetric.Metrics {
	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	res := rm.Resource().Attributes()
	for i := 0; i < int(r.ResourceLen()); i++ {
		putAttr(res, r.Resource(i))
	}
	if _, ok := res.Get("service.name"); !ok {
		if s := r.Service(); s != "" {
			res.PutStr("service.name", s)
		}
	}
	start := pcommon.Timestamp(r.StartUnixNano())

	type key struct {
		scope, name, unit string
		kind              mr.PointKind
		monotonic         bool
	}
	scopes := map[string]pmetric.ScopeMetrics{}
	metrics := map[key]pmetric.Metric{}
	var (
		k    key
		m    pmetric.Metric
		prev mr.Point
	)
	n := int(r.PointsLen())
	for i := 0; i < n; i++ {
		p := r.Points(i)
		kind := p.Kind()
		if kind > mr.PointKindHistogram {
			continue // a kind this reader does not know: skip the point
		}
		// The exporter writes a metric's points next to each other, so most points
		// belong to the previous point's metric: compare the bytes in place, and build
		// the (allocating) string key only when the metric changes.
		if i > 0 && kind == k.kind && p.Monotonic() == k.monotonic &&
			string(p.NameBytes()) == string(prev.NameBytes()) &&
			string(p.UnitBytes()) == string(prev.UnitBytes()) &&
			string(p.ScopeBytes()) == string(prev.ScopeBytes()) {
			putPoint(m, k.kind, p, start)
			prev = p
			continue
		}
		prev = p
		k = key{p.Scope(), p.Name(), p.Unit(), kind, p.Monotonic()}
		var ok bool
		m, ok = metrics[k]
		if !ok {
			sm, ok := scopes[k.scope]
			if !ok {
				sm = rm.ScopeMetrics().AppendEmpty()
				sm.Scope().SetName(k.scope)
				scopes[k.scope] = sm
			}
			m = sm.Metrics().AppendEmpty()
			m.SetName(k.name)
			m.SetUnit(k.unit)
			switch k.kind {
			case mr.PointKindGaugeInt, mr.PointKindGaugeDouble:
				m.SetEmptyGauge()
			case mr.PointKindSumInt, mr.PointKindSumDouble:
				s := m.SetEmptySum()
				s.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
				s.SetIsMonotonic(k.monotonic)
			case mr.PointKindHistogram:
				m.SetEmptyHistogram().SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
			}
			metrics[k] = m
		}
		putPoint(m, k.kind, p, start)
	}
	return md
}

// putPoint appends p's data point to m.
func putPoint(m pmetric.Metric, kind mr.PointKind, p mr.Point, start pcommon.Timestamp) {
	t := pcommon.Timestamp(p.TimeUnixNano())
	switch kind {
	case mr.PointKindGaugeInt, mr.PointKindGaugeDouble, mr.PointKindSumInt, mr.PointKindSumDouble:
		var dp pmetric.NumberDataPoint
		if kind == mr.PointKindGaugeInt || kind == mr.PointKindGaugeDouble {
			dp = m.Gauge().DataPoints().AppendEmpty()
		} else {
			dp = m.Sum().DataPoints().AppendEmpty()
			dp.SetStartTimestamp(start)
		}
		dp.SetTimestamp(t)
		if kind == mr.PointKindGaugeInt || kind == mr.PointKindSumInt {
			dp.SetIntValue(p.IntValue())
		} else {
			dp.SetDoubleValue(p.DoubleValue())
		}
		putAttrs(dp.Attributes(), p)
	case mr.PointKindHistogram:
		dp := m.Histogram().DataPoints().AppendEmpty()
		dp.SetStartTimestamp(start)
		dp.SetTimestamp(t)
		dp.SetCount(p.Count())
		dp.SetSum(p.DoubleValue())
		nb := int(p.BoundsLen())
		b := dp.ExplicitBounds()
		b.EnsureCapacity(nb)
		for j := 0; j < nb; j++ {
			b.Append(p.Bounds(j))
		}
		nc := int(p.BucketCountsLen())
		c := dp.BucketCounts()
		c.EnsureCapacity(nc)
		for j := 0; j < nc; j++ {
			c.Append(p.BucketCounts(j))
		}
		if v, ok := p.Min(); ok {
			dp.SetMin(v)
		}
		if v, ok := p.Max(); ok {
			dp.SetMax(v)
		}
		putAttrs(dp.Attributes(), p)
	}
}

func putAttrs(dst pcommon.Map, p mr.Point) {
	n := int(p.AttrsLen())
	dst.EnsureCapacity(n)
	for i := 0; i < n; i++ {
		putAttr(dst, p.Attrs(i))
	}
}

func putAttr(dst pcommon.Map, a mr.Attr) {
	switch a.Kind() {
	case mr.AttrKindInt:
		dst.PutInt(a.Key(), a.IntValue())
	case mr.AttrKindDouble:
		dst.PutDouble(a.Key(), a.DoubleValue())
	case mr.AttrKindBool:
		dst.PutBool(a.Key(), a.IntValue() != 0)
	default:
		dst.PutStr(a.Key(), a.StringValue())
	}
}

// OverlayValues writes a direct-instrument app's live values (package shmdirect) into a
// descriptor snapshot, stamped now: stream i's value slot is values point i. The snapshot
// is the receiver's private copy, so ToMetrics then converts it as it converts an
// exporter's region. Values are read word by word — each atomically, not all at one
// instant (spec/40 §2).
func OverlayValues(s mr.State, v mv.Values, now uint64) {
	n := int(s.PointsLen())
	if m := v.PointsLen(); m < n {
		n = m
	}
	for i := 0; i < n; i++ {
		p, val := s.Points(i), v.Points(i)
		switch p.Kind() {
		case mr.PointKindGaugeInt, mr.PointKindSumInt:
			p.SetIntValue(val.IntValue())
		case mr.PointKindGaugeDouble, mr.PointKindSumDouble:
			p.SetDoubleValue(val.DoubleValue())
		case mr.PointKindHistogram:
			c := val.Count()
			p.SetCount(c)
			p.SetDoubleValue(val.DoubleValue())
			nb := int(p.BucketCountsLen())
			for k := 0; k < nb; k++ {
				p.SetBucketCounts(k, val.Buckets(k))
			}
			if mn, mx := val.Min(), val.Max(); c > 0 && !math.IsInf(mn, 0) && !math.IsInf(mx, 0) {
				p.SetMin(mn)
				p.SetMax(mx)
			} else {
				p.ClearMin()
				p.ClearMax()
			}
		}
		p.SetTimeUnixNano(now)
	}
}
