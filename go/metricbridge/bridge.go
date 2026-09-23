// Package metricbridge converts between pdata metrics (`pmetric`) and an OTLP-on-Dagr
// `OtlpMetrics` buffer — the metrics twin of package pdatabridge (traces).
//
// Encode goes through the direct builder (Dagr spec 33): pdata is walked into the
// builder's value tree and serialized with no arena. Decode (ToMetrics) materialises
// pdata for handing a batch to an unmodified pipeline; like pdatabridge.ToTraces it is
// the compatibility path, not the one otlp-dagr exists for.
//
// The shared messages (attributes, Resource, InstrumentationScope) come from
// common_gen.go, which regen.sh copies from pdatabridge/common.go.
package metricbridge

import (
	"go.opentelemetry.io/collector/pdata/pmetric"

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
				Scope:     some(scope(sm.Scope())),
				Metrics:   optSlice(outM),
				SchemaURL: nonEmpty(sm.SchemaUrl()),
			})
		}
		outRM = append(outRM, d.ResourceMetrics{
			Resource:     some(resource(rm.Resource())),
			ScopeMetrics: optSlice(outSM),
			SchemaURL:    nonEmpty(rm.SchemaUrl()),
		})
	}
	return d.MetricsData{ResourceMetrics: optSlice(outRM)}
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
		Name:            nonEmpty(m.Name()),
		DescriptionText: nonEmpty(m.Description()),
		Unit:            nonEmpty(m.Unit()),
		Metadata:        optSlice(keyValues(m.Metadata())),
	}
	// The `data` oneof. MetricTypeEmpty — a metric with no data set — leaves the union
	// absent, the same thing protobuf writes for an unset oneof.
	switch m.Type() {
	case pmetric.MetricTypeGauge:
		out.Data = some(d.MetricDataGauge(d.Gauge{
			DataPoints: optSlice(numberPoints(m.Gauge().DataPoints())),
		}))
	case pmetric.MetricTypeSum:
		s := m.Sum()
		out.Data = some(d.MetricDataSum(d.Sum{
			DataPoints:             optSlice(numberPoints(s.DataPoints())),
			AggregationTemporality: nz(d.AggregationTemporality(s.AggregationTemporality())),
			IsMonotonic:            nz(s.IsMonotonic()),
		}))
	case pmetric.MetricTypeHistogram:
		h := m.Histogram()
		out.Data = some(d.MetricDataHistogram(d.Histogram{
			DataPoints:             optSlice(histogramPoints(h.DataPoints())),
			AggregationTemporality: nz(d.AggregationTemporality(h.AggregationTemporality())),
		}))
	case pmetric.MetricTypeExponentialHistogram:
		h := m.ExponentialHistogram()
		out.Data = some(d.MetricDataExponentialHistogram(d.ExponentialHistogram{
			DataPoints:             optSlice(expHistogramPoints(h.DataPoints())),
			AggregationTemporality: nz(d.AggregationTemporality(h.AggregationTemporality())),
		}))
	case pmetric.MetricTypeSummary:
		// Summary has no node of its own in the schema: the variant IS the array.
		out.Data = some(d.MetricDataSummary(summaryPoints(m.Summary().DataPoints())))
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
			Attributes:        optSlice(keyValues(p.Attributes())),
			StartTimeUnixNano: nonZero(uint64(p.StartTimestamp())),
			TimeUnixNano:      nonZero(uint64(p.Timestamp())),
			Exemplars:         optSlice(exemplars(p.Exemplars())),
			Flags:             nonZeroU32(uint32(p.Flags())),
		}
		switch p.ValueType() {
		case pmetric.NumberDataPointValueTypeDouble:
			dp.Value = some(d.NumberValueAsDouble(p.DoubleValue()))
		case pmetric.NumberDataPointValueTypeInt:
			dp.Value = some(d.NumberValueAsInt(p.IntValue()))
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
			Attributes:        optSlice(keyValues(p.Attributes())),
			StartTimeUnixNano: nonZero(uint64(p.StartTimestamp())),
			TimeUnixNano:      nonZero(uint64(p.Timestamp())),
			Count:             nonZero(p.Count()),
			BucketCounts:      optSlice(p.BucketCounts().AsRaw()),
			ExplicitBounds:    optSlice(p.ExplicitBounds().AsRaw()),
			Exemplars:         optSlice(exemplars(p.Exemplars())),
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
			Attributes:        optSlice(keyValues(p.Attributes())),
			StartTimeUnixNano: nonZero(uint64(p.StartTimestamp())),
			TimeUnixNano:      nonZero(uint64(p.Timestamp())),
			Count:             nonZero(p.Count()),
			Scale:             nz(p.Scale()),
			ZeroCount:         nonZero(p.ZeroCount()),
			Positive:          buckets(p.Positive()),
			Negative:          buckets(p.Negative()),
			Flags:             nonZeroU32(uint32(p.Flags())),
			Exemplars:         optSlice(exemplars(p.Exemplars())),
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
	return some(d.Buckets{
		Offset:       nz(b.Offset()),
		BucketCounts: optSlice(b.BucketCounts().AsRaw()),
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
				qv = append(qv, d.ValueAtQuantile{Quantile: nz(q.Quantile()), Value: nz(q.Value())})
			}
		}
		out = append(out, d.SummaryDataPoint{
			Attributes:        optSlice(keyValues(p.Attributes())),
			StartTimeUnixNano: nonZero(uint64(p.StartTimestamp())),
			TimeUnixNano:      nonZero(uint64(p.Timestamp())),
			Count:             nonZero(p.Count()),
			Sum:               nz(p.Sum()), // plain `double` here, not `optional`
			QuantileValues:    optSlice(qv),
			Flags:             nonZeroU32(uint32(p.Flags())),
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
			FilteredAttributes: optSlice(keyValues(e.FilteredAttributes())),
			TimeUnixNano:       nonZero(uint64(e.Timestamp())),
			SpanID:             spanID(e.SpanID()),
			TraceID:            traceID(e.TraceID()),
		}
		switch e.ValueType() {
		case pmetric.ExemplarValueTypeDouble:
			ex.Value = some(d.NumberValueAsDouble(e.DoubleValue()))
		case pmetric.ExemplarValueTypeInt:
			ex.Value = some(d.NumberValueAsInt(e.IntValue()))
		}
		out = append(out, ex)
	}
	return out
}
