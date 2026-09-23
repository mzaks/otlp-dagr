package metricbridge

import (
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"

	"otlpgen/dagr"
	d "otlpgen/otlpmetricsdirect"
)

// StreamBuilder is the one-pass metrics encoder — pdatabridge.StreamBuilder's twin. pdata
// is walked once: the container levels (MetricsData / ResourceMetrics / ScopeMetrics) are
// written by hand, and each Metric is built as a value struct in reused scratch and
// written with the generated PutMetric. Nothing batch-sized is materialised, and once the
// scratch pools have grown to the largest metric nothing allocates (nested Slice / Map
// attribute values excepted, as in traces). TestStreamMatches gates the bytes against the
// value-tree path.
type StreamBuilder struct {
	b  *dagr.Builder
	lv levels

	kv   pool[d.KeyValue]
	u64  pool[uint64]
	f64  pool[float64]
	ids  pool[byte]
	num  pool[d.NumberDataPoint]
	hist pool[d.HistogramDataPoint]
	exp  pool[d.ExponentialHistogramDataPoint]
	sum  pool[d.SummaryDataPoint]
	ex   pool[d.Exemplar]
	q    pool[d.ValueAtQuantile]

	// The MetricData variant of the metric being written. The union points at it
	// (`…Ref`) rather than copying it to the heap; one metric is in flight at a time.
	gaugeNode d.Gauge
	sumNode   d.Sum
	histoNode d.Histogram
	expoNode  d.ExponentialHistogram
}

func NewStreamBuilder() *StreamBuilder { return &StreamBuilder{b: dagr.NewBuilder(0)} }

// BuildAppend encodes md as an OtlpMetrics buffer into dst — the same bytes as ToBytes.
func (w *StreamBuilder) BuildAppend(dst []byte, md pmetric.Metrics) []byte {
	b := w.b
	b.Reset()
	buf, i := b.Take()
	before := len(buf) - i
	if n := md.ResourceMetrics().Len(); n > 0 {
		buf, i = w.putResourceMetrics(md, b, buf, i)
		buf, i = dagr.PutLEBAt(buf, i, 1) // MetricsData.resource_metrics, field 0
	}
	buf, i = dagr.PutLEBAt(buf, i, uint64((len(buf)-i)-before))
	b.Put(buf, i)
	return finishGraph(b, dst)
}

// AppendRecord encodes md as the content of one MetricLog `Batch` record — what
// spool.Writer.AppendRecord takes — in the same single pass.
func (w *StreamBuilder) AppendRecord(dst []byte, receivedUnixNano uint64, md pmetric.Metrics) []byte {
	b := w.b
	b.Reset()
	buf, i := b.Take()
	before := len(buf) - i
	buf, i = w.putResourceMetrics(md, b, buf, i)
	buf, i = dagr.PutLEBAt(buf, i, 5) // Batch.resource_metrics, field 2 (required)
	buf, i = putRecordEnvelope(buf, i, before, receivedUnixNano, uint32(md.DataPointCount()))
	b.Put(buf, i)
	return b.AppendTo(dst)
}

// putResourceMetrics writes the resource_metrics array content (no field tag).
func (w *StreamBuilder) putResourceMetrics(md pmetric.Metrics, b *dagr.Builder, buf []byte, i int) ([]byte, int) {
	rms := md.ResourceMetrics()
	return putArray(buf, i, rms.Len(), func(k int, buf []byte, i int) ([]byte, int) {
		rm := rms.At(k)
		sms := rm.ScopeMetrics()
		return putLevel(buf, i, rm.SchemaUrl(), sms.Len(),
			func(k int, buf []byte, i int) ([]byte, int) {
				sm := sms.At(k)
				ms := sm.Metrics()
				return putLevel(buf, i, sm.SchemaUrl(), ms.Len(),
					func(k int, buf []byte, i int) ([]byte, int) {
						return d.PutMetric(w.metricValue(ms.At(k)), b, buf, i)
					},
					func(buf []byte, i int) ([]byte, int) { return w.lv.putScope(sm.Scope(), b, buf, i) })
			},
			func(buf []byte, i int) ([]byte, int) { return w.lv.putResource(rm.Resource(), b, buf, i) })
	})
}

func (w *StreamBuilder) reset() {
	w.kv.reset()
	w.u64.reset()
	w.f64.reset()
	w.ids.reset()
	w.num.reset()
	w.hist.reset()
	w.exp.reset()
	w.sum.reset()
	w.ex.reset()
	w.q.reset()
}

// metricValue is toMetric over reused scratch: the same value, so the same bytes.
func (w *StreamBuilder) metricValue(m pmetric.Metric) d.Metric {
	w.reset()
	out := d.Metric{
		Name:            nonEmpty(m.Name()),
		DescriptionText: nonEmpty(m.Description()),
		Unit:            nonEmpty(m.Unit()),
		Metadata:        attrsInto(&w.kv, m.Metadata()),
	}
	switch m.Type() {
	case pmetric.MetricTypeGauge:
		w.gaugeNode = d.Gauge{DataPoints: w.numberPoints(m.Gauge().DataPoints())}
		out.Data = some(d.MetricDataGaugeRef(&w.gaugeNode))
	case pmetric.MetricTypeSum:
		s := m.Sum()
		w.sumNode = d.Sum{
			DataPoints:             w.numberPoints(s.DataPoints()),
			AggregationTemporality: nz(d.AggregationTemporality(s.AggregationTemporality())),
			IsMonotonic:            nz(s.IsMonotonic()),
		}
		out.Data = some(d.MetricDataSumRef(&w.sumNode))
	case pmetric.MetricTypeHistogram:
		h := m.Histogram()
		w.histoNode = d.Histogram{
			DataPoints:             w.histogramPoints(h.DataPoints()),
			AggregationTemporality: nz(d.AggregationTemporality(h.AggregationTemporality())),
		}
		out.Data = some(d.MetricDataHistogramRef(&w.histoNode))
	case pmetric.MetricTypeExponentialHistogram:
		h := m.ExponentialHistogram()
		w.expoNode = d.ExponentialHistogram{
			DataPoints:             w.expHistogramPoints(h.DataPoints()),
			AggregationTemporality: nz(d.AggregationTemporality(h.AggregationTemporality())),
		}
		out.Data = some(d.MetricDataExponentialHistogramRef(&w.expoNode))
	case pmetric.MetricTypeSummary:
		out.Data = some(d.MetricDataSummary(w.summaryPoints(m.Summary().DataPoints()).V))
	}
	return out
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

func (w *StreamBuilder) f64s(s pcommon.Float64Slice) dagr.Opt[[]float64] {
	if s.Len() == 0 {
		return dagr.Opt[[]float64]{}
	}
	dst := w.f64.take(s.Len())
	for k := range dst {
		dst[k] = s.At(k)
	}
	return some(dst)
}

func (w *StreamBuilder) numberPoints(ps pmetric.NumberDataPointSlice) dagr.Opt[[]d.NumberDataPoint] {
	if ps.Len() == 0 {
		return dagr.Opt[[]d.NumberDataPoint]{}
	}
	out := w.num.take(ps.Len())
	for k := range out {
		p := ps.At(k)
		dp := d.NumberDataPoint{
			Attributes:        attrsInto(&w.kv, p.Attributes()),
			StartTimeUnixNano: nonZero(uint64(p.StartTimestamp())),
			TimeUnixNano:      nonZero(uint64(p.Timestamp())),
			Exemplars:         w.exemplars(p.Exemplars()),
			Flags:             nonZeroU32(uint32(p.Flags())),
		}
		switch p.ValueType() {
		case pmetric.NumberDataPointValueTypeDouble:
			dp.Value = some(d.NumberValueAsDouble(p.DoubleValue()))
		case pmetric.NumberDataPointValueTypeInt:
			dp.Value = some(d.NumberValueAsInt(p.IntValue()))
		}
		out[k] = dp
	}
	return some(out)
}

func (w *StreamBuilder) histogramPoints(ps pmetric.HistogramDataPointSlice) dagr.Opt[[]d.HistogramDataPoint] {
	if ps.Len() == 0 {
		return dagr.Opt[[]d.HistogramDataPoint]{}
	}
	out := w.hist.take(ps.Len())
	for k := range out {
		p := ps.At(k)
		dp := d.HistogramDataPoint{
			Attributes:        attrsInto(&w.kv, p.Attributes()),
			StartTimeUnixNano: nonZero(uint64(p.StartTimestamp())),
			TimeUnixNano:      nonZero(uint64(p.Timestamp())),
			Count:             nonZero(p.Count()),
			BucketCounts:      w.u64s(p.BucketCounts()),
			ExplicitBounds:    w.f64s(p.ExplicitBounds()),
			Exemplars:         w.exemplars(p.Exemplars()),
			Flags:             nonZeroU32(uint32(p.Flags())),
		}
		if p.HasSum() {
			dp.Sum = some(p.Sum())
		}
		if p.HasMin() {
			dp.Min = some(p.Min())
		}
		if p.HasMax() {
			dp.Max = some(p.Max())
		}
		out[k] = dp
	}
	return some(out)
}

func (w *StreamBuilder) expHistogramPoints(ps pmetric.ExponentialHistogramDataPointSlice) dagr.Opt[[]d.ExponentialHistogramDataPoint] {
	if ps.Len() == 0 {
		return dagr.Opt[[]d.ExponentialHistogramDataPoint]{}
	}
	out := w.exp.take(ps.Len())
	for k := range out {
		p := ps.At(k)
		dp := d.ExponentialHistogramDataPoint{
			Attributes:        attrsInto(&w.kv, p.Attributes()),
			StartTimeUnixNano: nonZero(uint64(p.StartTimestamp())),
			TimeUnixNano:      nonZero(uint64(p.Timestamp())),
			Count:             nonZero(p.Count()),
			Scale:             nz(p.Scale()),
			ZeroCount:         nonZero(p.ZeroCount()),
			Positive:          w.buckets(p.Positive()),
			Negative:          w.buckets(p.Negative()),
			Flags:             nonZeroU32(uint32(p.Flags())),
			Exemplars:         w.exemplars(p.Exemplars()),
			ZeroThreshold:     nz(p.ZeroThreshold()),
		}
		if p.HasSum() {
			dp.Sum = some(p.Sum())
		}
		if p.HasMin() {
			dp.Min = some(p.Min())
		}
		if p.HasMax() {
			dp.Max = some(p.Max())
		}
		out[k] = dp
	}
	return some(out)
}

// buckets: absent when both fields are at their zero value, as in the value-tree bridge.
func (w *StreamBuilder) buckets(bk pmetric.ExponentialHistogramDataPointBuckets) dagr.Opt[d.Buckets] {
	if bk.Offset() == 0 && bk.BucketCounts().Len() == 0 {
		return dagr.Opt[d.Buckets]{}
	}
	return some(d.Buckets{Offset: nz(bk.Offset()), BucketCounts: w.u64s(bk.BucketCounts())})
}

func (w *StreamBuilder) summaryPoints(ps pmetric.SummaryDataPointSlice) dagr.Opt[[]d.SummaryDataPoint] {
	if ps.Len() == 0 {
		return dagr.Opt[[]d.SummaryDataPoint]{}
	}
	out := w.sum.take(ps.Len())
	for k := range out {
		p := ps.At(k)
		qs := p.QuantileValues()
		var qv dagr.Opt[[]d.ValueAtQuantile]
		if qs.Len() > 0 {
			l := w.q.take(qs.Len())
			for j := range l {
				q := qs.At(j)
				l[j] = d.ValueAtQuantile{Quantile: nz(q.Quantile()), Value: nz(q.Value())}
			}
			qv = some(l)
		}
		out[k] = d.SummaryDataPoint{
			Attributes:        attrsInto(&w.kv, p.Attributes()),
			StartTimeUnixNano: nonZero(uint64(p.StartTimestamp())),
			TimeUnixNano:      nonZero(uint64(p.Timestamp())),
			Count:             nonZero(p.Count()),
			Sum:               nz(p.Sum()),
			QuantileValues:    qv,
			Flags:             nonZeroU32(uint32(p.Flags())),
		}
	}
	return some(out)
}

func (w *StreamBuilder) exemplars(es pmetric.ExemplarSlice) dagr.Opt[[]d.Exemplar] {
	if es.Len() == 0 {
		return dagr.Opt[[]d.Exemplar]{}
	}
	out := w.ex.take(es.Len())
	for k := range out {
		e := es.At(k)
		sid, tid := e.SpanID(), e.TraceID()
		ex := d.Exemplar{
			FilteredAttributes: attrsInto(&w.kv, e.FilteredAttributes()),
			TimeUnixNano:       nonZero(uint64(e.Timestamp())),
			SpanID:             idInto(&w.ids, sid[:]),
			TraceID:            idInto(&w.ids, tid[:]),
		}
		switch e.ValueType() {
		case pmetric.ExemplarValueTypeDouble:
			ex.Value = some(d.NumberValueAsDouble(e.DoubleValue()))
		case pmetric.ExemplarValueTypeInt:
			ex.Value = some(d.NumberValueAsInt(e.IntValue()))
		}
		out[k] = ex
	}
	return some(out)
}
