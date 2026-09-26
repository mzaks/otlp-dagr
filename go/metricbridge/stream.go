package metricbridge

import (
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"

	cb "otlpdagr/commonbridge"
	"otlpgen/dagr"
	cd "otlpgen/otlpcommondirect"
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
	lv cb.Levels

	kv   cb.Pool[cd.KeyValue]
	u64  cb.Pool[uint64]
	f64  cb.Pool[float64]
	ids  cb.Pool[byte]
	num  cb.Pool[d.NumberDataPoint]
	hist cb.Pool[d.HistogramDataPoint]
	exp  cb.Pool[d.ExponentialHistogramDataPoint]
	sum  cb.Pool[d.SummaryDataPoint]
	ex   cb.Pool[d.Exemplar]
	q    cb.Pool[d.ValueAtQuantile]

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
	return cb.FinishGraph(b, dst)
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
	buf, i = cb.PutRecordEnvelope(buf, i, before, receivedUnixNano, uint32(md.DataPointCount()))
	b.Put(buf, i)
	return b.AppendTo(dst)
}

// putResourceMetrics writes the resource_metrics array content (no field tag).
func (w *StreamBuilder) putResourceMetrics(md pmetric.Metrics, b *dagr.Builder, buf []byte, i int) ([]byte, int) {
	rms := md.ResourceMetrics()
	return cb.PutArray(buf, i, rms.Len(), func(k int, buf []byte, i int) ([]byte, int) {
		rm := rms.At(k)
		sms := rm.ScopeMetrics()
		return cb.PutLevel(buf, i, rm.SchemaUrl(), sms.Len(),
			func(k int, buf []byte, i int) ([]byte, int) {
				sm := sms.At(k)
				ms := sm.Metrics()
				return cb.PutLevel(buf, i, sm.SchemaUrl(), ms.Len(),
					func(k int, buf []byte, i int) ([]byte, int) {
						return d.PutMetric(w.metricValue(ms.At(k)), b, buf, i)
					},
					func(buf []byte, i int) ([]byte, int) { return w.lv.PutScope(sm.Scope(), b, buf, i) })
			},
			func(buf []byte, i int) ([]byte, int) { return w.lv.PutResource(rm.Resource(), b, buf, i) })
	})
}

func (w *StreamBuilder) reset() {
	w.kv.Reset()
	w.u64.Reset()
	w.f64.Reset()
	w.ids.Reset()
	w.num.Reset()
	w.hist.Reset()
	w.exp.Reset()
	w.sum.Reset()
	w.ex.Reset()
	w.q.Reset()
}

// metricValue is toMetric over reused scratch: the same value, so the same bytes.
func (w *StreamBuilder) metricValue(m pmetric.Metric) d.Metric {
	w.reset()
	out := d.Metric{
		Name:            cb.NonEmpty(m.Name()),
		DescriptionText: cb.NonEmpty(m.Description()),
		Unit:            cb.NonEmpty(m.Unit()),
		Metadata:        cb.AttrsInto(&w.kv, m.Metadata()),
	}
	switch m.Type() {
	case pmetric.MetricTypeGauge:
		w.gaugeNode = d.Gauge{DataPoints: w.numberPoints(m.Gauge().DataPoints())}
		out.Data = cb.Some(d.MetricDataGaugeRef(&w.gaugeNode))
	case pmetric.MetricTypeSum:
		s := m.Sum()
		w.sumNode = d.Sum{
			DataPoints:             w.numberPoints(s.DataPoints()),
			AggregationTemporality: cb.NZ(d.AggregationTemporality(s.AggregationTemporality())),
			IsMonotonic:            cb.NZ(s.IsMonotonic()),
		}
		out.Data = cb.Some(d.MetricDataSumRef(&w.sumNode))
	case pmetric.MetricTypeHistogram:
		h := m.Histogram()
		w.histoNode = d.Histogram{
			DataPoints:             w.histogramPoints(h.DataPoints()),
			AggregationTemporality: cb.NZ(d.AggregationTemporality(h.AggregationTemporality())),
		}
		out.Data = cb.Some(d.MetricDataHistogramRef(&w.histoNode))
	case pmetric.MetricTypeExponentialHistogram:
		h := m.ExponentialHistogram()
		w.expoNode = d.ExponentialHistogram{
			DataPoints:             w.expHistogramPoints(h.DataPoints()),
			AggregationTemporality: cb.NZ(d.AggregationTemporality(h.AggregationTemporality())),
		}
		out.Data = cb.Some(d.MetricDataExponentialHistogramRef(&w.expoNode))
	case pmetric.MetricTypeSummary:
		out.Data = cb.Some(d.MetricDataSummary(w.summaryPoints(m.Summary().DataPoints()).V))
	}
	return out
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

func (w *StreamBuilder) f64s(s pcommon.Float64Slice) dagr.Opt[[]float64] {
	if s.Len() == 0 {
		return dagr.Opt[[]float64]{}
	}
	dst := w.f64.Take(s.Len())
	for k := range dst {
		dst[k] = s.At(k)
	}
	return cb.Some(dst)
}

func (w *StreamBuilder) numberPoints(ps pmetric.NumberDataPointSlice) dagr.Opt[[]d.NumberDataPoint] {
	if ps.Len() == 0 {
		return dagr.Opt[[]d.NumberDataPoint]{}
	}
	out := w.num.Take(ps.Len())
	for k := range out {
		p := ps.At(k)
		dp := d.NumberDataPoint{
			Attributes:        cb.AttrsInto(&w.kv, p.Attributes()),
			StartTimeUnixNano: cb.NonZero(uint64(p.StartTimestamp())),
			TimeUnixNano:      cb.NonZero(uint64(p.Timestamp())),
			Exemplars:         w.exemplars(p.Exemplars()),
			Flags:             cb.NonZeroU32(uint32(p.Flags())),
		}
		switch p.ValueType() {
		case pmetric.NumberDataPointValueTypeDouble:
			dp.Value = cb.Some(d.NumberValueAsDouble(p.DoubleValue()))
		case pmetric.NumberDataPointValueTypeInt:
			dp.Value = cb.Some(d.NumberValueAsInt(p.IntValue()))
		}
		out[k] = dp
	}
	return cb.Some(out)
}

func (w *StreamBuilder) histogramPoints(ps pmetric.HistogramDataPointSlice) dagr.Opt[[]d.HistogramDataPoint] {
	if ps.Len() == 0 {
		return dagr.Opt[[]d.HistogramDataPoint]{}
	}
	out := w.hist.Take(ps.Len())
	for k := range out {
		p := ps.At(k)
		dp := d.HistogramDataPoint{
			Attributes:        cb.AttrsInto(&w.kv, p.Attributes()),
			StartTimeUnixNano: cb.NonZero(uint64(p.StartTimestamp())),
			TimeUnixNano:      cb.NonZero(uint64(p.Timestamp())),
			Count:             cb.NonZero(p.Count()),
			BucketCounts:      w.u64s(p.BucketCounts()),
			ExplicitBounds:    w.f64s(p.ExplicitBounds()),
			Exemplars:         w.exemplars(p.Exemplars()),
			Flags:             cb.NonZeroU32(uint32(p.Flags())),
		}
		if p.HasSum() {
			dp.Sum = cb.Some(p.Sum())
		}
		if p.HasMin() {
			dp.Min = cb.Some(p.Min())
		}
		if p.HasMax() {
			dp.Max = cb.Some(p.Max())
		}
		out[k] = dp
	}
	return cb.Some(out)
}

func (w *StreamBuilder) expHistogramPoints(ps pmetric.ExponentialHistogramDataPointSlice) dagr.Opt[[]d.ExponentialHistogramDataPoint] {
	if ps.Len() == 0 {
		return dagr.Opt[[]d.ExponentialHistogramDataPoint]{}
	}
	out := w.exp.Take(ps.Len())
	for k := range out {
		p := ps.At(k)
		dp := d.ExponentialHistogramDataPoint{
			Attributes:        cb.AttrsInto(&w.kv, p.Attributes()),
			StartTimeUnixNano: cb.NonZero(uint64(p.StartTimestamp())),
			TimeUnixNano:      cb.NonZero(uint64(p.Timestamp())),
			Count:             cb.NonZero(p.Count()),
			Scale:             cb.NZ(p.Scale()),
			ZeroCount:         cb.NonZero(p.ZeroCount()),
			Positive:          w.buckets(p.Positive()),
			Negative:          w.buckets(p.Negative()),
			Flags:             cb.NonZeroU32(uint32(p.Flags())),
			Exemplars:         w.exemplars(p.Exemplars()),
			ZeroThreshold:     cb.NZ(p.ZeroThreshold()),
		}
		if p.HasSum() {
			dp.Sum = cb.Some(p.Sum())
		}
		if p.HasMin() {
			dp.Min = cb.Some(p.Min())
		}
		if p.HasMax() {
			dp.Max = cb.Some(p.Max())
		}
		out[k] = dp
	}
	return cb.Some(out)
}

// buckets: absent when both fields are at their zero value, as in the value-tree bridge.
func (w *StreamBuilder) buckets(bk pmetric.ExponentialHistogramDataPointBuckets) dagr.Opt[d.Buckets] {
	if bk.Offset() == 0 && bk.BucketCounts().Len() == 0 {
		return dagr.Opt[d.Buckets]{}
	}
	return cb.Some(d.Buckets{Offset: cb.NZ(bk.Offset()), BucketCounts: w.u64s(bk.BucketCounts())})
}

func (w *StreamBuilder) summaryPoints(ps pmetric.SummaryDataPointSlice) dagr.Opt[[]d.SummaryDataPoint] {
	if ps.Len() == 0 {
		return dagr.Opt[[]d.SummaryDataPoint]{}
	}
	out := w.sum.Take(ps.Len())
	for k := range out {
		p := ps.At(k)
		qs := p.QuantileValues()
		var qv dagr.Opt[[]d.ValueAtQuantile]
		if qs.Len() > 0 {
			l := w.q.Take(qs.Len())
			for j := range l {
				q := qs.At(j)
				l[j] = d.ValueAtQuantile{Quantile: cb.NZ(q.Quantile()), Value: cb.NZ(q.Value())}
			}
			qv = cb.Some(l)
		}
		out[k] = d.SummaryDataPoint{
			Attributes:        cb.AttrsInto(&w.kv, p.Attributes()),
			StartTimeUnixNano: cb.NonZero(uint64(p.StartTimestamp())),
			TimeUnixNano:      cb.NonZero(uint64(p.Timestamp())),
			Count:             cb.NonZero(p.Count()),
			Sum:               cb.NZ(p.Sum()),
			QuantileValues:    qv,
			Flags:             cb.NonZeroU32(uint32(p.Flags())),
		}
	}
	return cb.Some(out)
}

func (w *StreamBuilder) exemplars(es pmetric.ExemplarSlice) dagr.Opt[[]d.Exemplar] {
	if es.Len() == 0 {
		return dagr.Opt[[]d.Exemplar]{}
	}
	out := w.ex.Take(es.Len())
	for k := range out {
		e := es.At(k)
		sid, tid := e.SpanID(), e.TraceID()
		ex := d.Exemplar{
			FilteredAttributes: cb.AttrsInto(&w.kv, e.FilteredAttributes()),
			TimeUnixNano:       cb.NonZero(uint64(e.Timestamp())),
			SpanID:             cb.IDInto(&w.ids, sid[:]),
			TraceID:            cb.IDInto(&w.ids, tid[:]),
		}
		switch e.ValueType() {
		case pmetric.ExemplarValueTypeDouble:
			ex.Value = cb.Some(d.NumberValueAsDouble(e.DoubleValue()))
		case pmetric.ExemplarValueTypeInt:
			ex.Value = cb.Some(d.NumberValueAsInt(e.IntValue()))
		}
		out[k] = ex
	}
	return cb.Some(out)
}
