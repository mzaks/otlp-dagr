// Package metricbridge converts between pdata metrics (`pmetric`) and an OTLP-on-Dagr
// `OtlpMetrics` buffer — the metrics twin of package pdatabridge (traces).
//
// Encode goes through the direct builder (Dagr spec 33): pdata is walked into the
// builder's value tree and serialized with no arena. Decode (ToMetrics) materialises
// pdata for handing a batch to an unmodified pipeline; like pdatabridge.ToTraces it is
// the compatibility path, not the one otlp-dagr exists for.
//
// The shared messages (attributes, Resource, InstrumentationScope) convert through
// package commonbridge.
package metricbridge

import (
	"go.opentelemetry.io/collector/pdata/pmetric"

	cb "otlpdagr/commonbridge"
	"otlpgen/dagr"
	d "otlpgen/otlpmetricsdirect"
)

// ToBytes serializes md into an OtlpMetrics buffer via the direct builder.
func ToBytes(md pmetric.Metrics) []byte { return d.BuildOtlpMetrics(ToValue(md)) }

// ToValue materialises the value tree the direct builder consumes.
func ToValue(md pmetric.Metrics) d.MetricsData {
	rms := md.ResourceMetrics()
	outRM := make([]d.ResourceMetrics, 0, rms.Len())
	for i := 0; i < rms.Len(); i++ {
		rm := rms.At(i)
		sms := rm.ScopeMetrics()
		outSM := make([]d.ScopeMetrics, 0, sms.Len())
		for j := 0; j < sms.Len(); j++ {
			sm := sms.At(j)
			ms := sm.Metrics()
			outM := make([]d.Metric, 0, ms.Len())
			for k := 0; k < ms.Len(); k++ {
				outM = append(outM, toMetric(ms.At(k)))
			}
			outSM = append(outSM, d.ScopeMetrics{
				Scope:     cb.Some(cb.Scope(sm.Scope())),
				Metrics:   cb.OptSlice(outM),
				SchemaURL: cb.NonEmpty(sm.SchemaUrl()),
			})
		}
		outRM = append(outRM, d.ResourceMetrics{
			Resource:     cb.Some(cb.Resource(rm.Resource())),
			ScopeMetrics: cb.OptSlice(outSM),
			SchemaURL:    cb.NonEmpty(rm.SchemaUrl()),
		})
	}
	return d.MetricsData{ResourceMetrics: cb.OptSlice(outRM)}
}

// Builder is the reusable form: one scratch buffer, reset per Build.
type Builder struct{ b *d.OtlpMetricsBuilder }

func NewBuilder() *Builder { return &Builder{b: d.NewOtlpMetricsBuilder()} }

func (w *Builder) Build(md pmetric.Metrics) []byte { return w.b.Build(ToValue(md)) }

// BuildAppend appends the encoded batch to dst and returns the extended slice.
func (w *Builder) BuildAppend(dst []byte, md pmetric.Metrics) []byte {
	return w.b.BuildAppend(dst, ToValue(md))
}

func toMetric(m pmetric.Metric) d.Metric {
	out := d.Metric{
		Name:            cb.NonEmpty(m.Name()),
		DescriptionText: cb.NonEmpty(m.Description()),
		Unit:            cb.NonEmpty(m.Unit()),
		Metadata:        cb.OptSlice(cb.KeyValues(m.Metadata())),
	}
	// The `data` oneof. MetricTypeEmpty — a metric with no data set — leaves the union
	// absent, the same thing protobuf writes for an unset oneof.
	switch m.Type() {
	case pmetric.MetricTypeGauge:
		out.Data = cb.Some(d.MetricDataGauge(d.Gauge{
			DataPoints: cb.OptSlice(numberPoints(m.Gauge().DataPoints())),
		}))
	case pmetric.MetricTypeSum:
		s := m.Sum()
		out.Data = cb.Some(d.MetricDataSum(d.Sum{
			DataPoints:             cb.OptSlice(numberPoints(s.DataPoints())),
			AggregationTemporality: cb.NZ(d.AggregationTemporality(s.AggregationTemporality())),
			IsMonotonic:            cb.NZ(s.IsMonotonic()),
		}))
	case pmetric.MetricTypeHistogram:
		h := m.Histogram()
		out.Data = cb.Some(d.MetricDataHistogram(d.Histogram{
			DataPoints:             cb.OptSlice(histogramPoints(h.DataPoints())),
			AggregationTemporality: cb.NZ(d.AggregationTemporality(h.AggregationTemporality())),
		}))
	case pmetric.MetricTypeExponentialHistogram:
		h := m.ExponentialHistogram()
		out.Data = cb.Some(d.MetricDataExponentialHistogram(d.ExponentialHistogram{
			DataPoints:             cb.OptSlice(expHistogramPoints(h.DataPoints())),
			AggregationTemporality: cb.NZ(d.AggregationTemporality(h.AggregationTemporality())),
		}))
	case pmetric.MetricTypeSummary:
		// Summary has no node of its own in the schema: the variant IS the array.
		out.Data = cb.Some(d.MetricDataSummary(summaryPoints(m.Summary().DataPoints())))
	}
	return out
}

// numberValue is the `value` oneof of a NumberDataPoint / Exemplar. Presence is the
// value TYPE, never the value: an int 0 is present, exactly as protobuf's oneof writes it.
func numberPoints(ps pmetric.NumberDataPointSlice) []d.NumberDataPoint {
	if ps.Len() == 0 {
		return nil
	}
	out := make([]d.NumberDataPoint, 0, ps.Len())
	for i := 0; i < ps.Len(); i++ {
		p := ps.At(i)
		dp := d.NumberDataPoint{
			Attributes:        cb.OptSlice(cb.KeyValues(p.Attributes())),
			StartTimeUnixNano: cb.NonZero(uint64(p.StartTimestamp())),
			TimeUnixNano:      cb.NonZero(uint64(p.Timestamp())),
			Exemplars:         cb.OptSlice(exemplars(p.Exemplars())),
			Flags:             cb.NonZeroU32(uint32(p.Flags())),
		}
		switch p.ValueType() {
		case pmetric.NumberDataPointValueTypeDouble:
			dp.Value = cb.Some(d.NumberValueAsDouble(p.DoubleValue()))
		case pmetric.NumberDataPointValueTypeInt:
			dp.Value = cb.Some(d.NumberValueAsInt(p.IntValue()))
		}
		out = append(out, dp)
	}
	return out
}

// sum / min / max are proto3 `optional`: written exactly when pdata says Has*, so a
// present 0.0 survives and an absent one stays absent.
func histogramPoints(ps pmetric.HistogramDataPointSlice) []d.HistogramDataPoint {
	if ps.Len() == 0 {
		return nil
	}
	out := make([]d.HistogramDataPoint, 0, ps.Len())
	for i := 0; i < ps.Len(); i++ {
		p := ps.At(i)
		dp := d.HistogramDataPoint{
			Attributes:        cb.OptSlice(cb.KeyValues(p.Attributes())),
			StartTimeUnixNano: cb.NonZero(uint64(p.StartTimestamp())),
			TimeUnixNano:      cb.NonZero(uint64(p.Timestamp())),
			Count:             cb.NonZero(p.Count()),
			BucketCounts:      cb.OptSlice(p.BucketCounts().AsRaw()),
			ExplicitBounds:    cb.OptSlice(p.ExplicitBounds().AsRaw()),
			Exemplars:         cb.OptSlice(exemplars(p.Exemplars())),
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
		out = append(out, dp)
	}
	return out
}

func expHistogramPoints(ps pmetric.ExponentialHistogramDataPointSlice) []d.ExponentialHistogramDataPoint {
	if ps.Len() == 0 {
		return nil
	}
	out := make([]d.ExponentialHistogramDataPoint, 0, ps.Len())
	for i := 0; i < ps.Len(); i++ {
		p := ps.At(i)
		dp := d.ExponentialHistogramDataPoint{
			Attributes:        cb.OptSlice(cb.KeyValues(p.Attributes())),
			StartTimeUnixNano: cb.NonZero(uint64(p.StartTimestamp())),
			TimeUnixNano:      cb.NonZero(uint64(p.Timestamp())),
			Count:             cb.NonZero(p.Count()),
			Scale:             cb.NZ(p.Scale()),
			ZeroCount:         cb.NonZero(p.ZeroCount()),
			Positive:          buckets(p.Positive()),
			Negative:          buckets(p.Negative()),
			Flags:             cb.NonZeroU32(uint32(p.Flags())),
			Exemplars:         cb.OptSlice(exemplars(p.Exemplars())),
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
		out = append(out, dp)
	}
	return out
}

// buckets is absent when both fields are at their zero value. pdata has no presence for
// the Buckets message (it always hands one back), so an empty one and a missing one are
// the same pdata — and the same decoded pdata either way.
func buckets(b pmetric.ExponentialHistogramDataPointBuckets) dagr.Opt[d.Buckets] {
	if b.Offset() == 0 && b.BucketCounts().Len() == 0 {
		return dagr.Opt[d.Buckets]{}
	}
	return cb.Some(d.Buckets{
		Offset:       cb.NZ(b.Offset()),
		BucketCounts: cb.OptSlice(b.BucketCounts().AsRaw()),
	})
}

func summaryPoints(ps pmetric.SummaryDataPointSlice) []d.SummaryDataPoint {
	if ps.Len() == 0 {
		return nil
	}
	out := make([]d.SummaryDataPoint, 0, ps.Len())
	for i := 0; i < ps.Len(); i++ {
		p := ps.At(i)
		qs := p.QuantileValues()
		var qv []d.ValueAtQuantile
		if qs.Len() > 0 {
			qv = make([]d.ValueAtQuantile, 0, qs.Len())
			for j := 0; j < qs.Len(); j++ {
				q := qs.At(j)
				qv = append(qv, d.ValueAtQuantile{Quantile: cb.NZ(q.Quantile()), Value: cb.NZ(q.Value())})
			}
		}
		out = append(out, d.SummaryDataPoint{
			Attributes:        cb.OptSlice(cb.KeyValues(p.Attributes())),
			StartTimeUnixNano: cb.NonZero(uint64(p.StartTimestamp())),
			TimeUnixNano:      cb.NonZero(uint64(p.Timestamp())),
			Count:             cb.NonZero(p.Count()),
			Sum:               cb.NZ(p.Sum()), // plain `double` here, not `optional`
			QuantileValues:    cb.OptSlice(qv),
			Flags:             cb.NonZeroU32(uint32(p.Flags())),
		})
	}
	return out
}

func exemplars(es pmetric.ExemplarSlice) []d.Exemplar {
	if es.Len() == 0 {
		return nil
	}
	out := make([]d.Exemplar, 0, es.Len())
	for i := 0; i < es.Len(); i++ {
		e := es.At(i)
		ex := d.Exemplar{
			FilteredAttributes: cb.OptSlice(cb.KeyValues(e.FilteredAttributes())),
			TimeUnixNano:       cb.NonZero(uint64(e.Timestamp())),
			SpanID:             cb.SpanID(e.SpanID()),
			TraceID:            cb.TraceID(e.TraceID()),
		}
		switch e.ValueType() {
		case pmetric.ExemplarValueTypeDouble:
			ex.Value = cb.Some(d.NumberValueAsDouble(e.DoubleValue()))
		case pmetric.ExemplarValueTypeInt:
			ex.Value = cb.Some(d.NumberValueAsInt(e.IntValue()))
		}
		out = append(out, ex)
	}
	return out
}
