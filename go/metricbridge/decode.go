package metricbridge

import (
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"

	"otlpgen/metriclog"
	g "otlpgen/otlpmetrics"

	cb "otlpdagr/commonbridge"
)

// ToMetrics materialises an OtlpMetrics buffer into pdata, for handing off to an
// unmodified collector pipeline. The slow path by construction — see pdatabridge.ToTraces.
func ToMetrics(buf []byte) (pmetric.Metrics, error) {
	root, err := g.OpenOtlpMetricsUnchecked(buf)
	if err != nil {
		return pmetric.Metrics{}, err
	}
	return putMetrics(root.ResourceMetricsIter()), nil
}

// FromRecord materialises one MetricLog spool record (spool.Batch.Record) into pdata. The
// record holds the batch's ResourceMetrics inline, read in place with the same accessors
// ToMetrics uses — metriclog aliases them — so the two share one body.
func FromRecord(record []byte) pmetric.Metrics {
	return putMetrics(metriclog.NewBatchAccessor(record, 0).ResourceMetricsIter())
}

func putMetrics[I any, P cb.Cursor[I, g.ResourceMetricsAccessor]](it I) pmetric.Metrics {
	c := P(&it)
	md := pmetric.NewMetrics()
	for src, ok := c.Next(); ok; src, ok = c.Next() {
		rm := md.ResourceMetrics().AppendEmpty()
		if r, ok := src.Resource(); ok {
			cb.DecodeResource(rm.Resource(), r)
		}
		if v, ok := src.SchemaURL(); ok {
			rm.SetSchemaUrl(v)
		}
		it := src.ScopeMetricsIter()
		for smSrc, ok := it.Next(); ok; smSrc, ok = it.Next() {
			sm := rm.ScopeMetrics().AppendEmpty()
			if sc, ok := smSrc.Scope(); ok {
				cb.DecodeScope(sm.Scope(), sc)
			}
			if v, ok := smSrc.SchemaURL(); ok {
				sm.SetSchemaUrl(v)
			}
			it := smSrc.MetricsIter()
			for dp, ok := it.Next(); ok; dp, ok = it.Next() {
				putMetric(sm.Metrics().AppendEmpty(), dp)
			}
		}
	}
	return md
}

func putMetric(dst pmetric.Metric, src g.MetricAccessor) {
	if v, ok := src.Name(); ok {
		dst.SetName(v)
	}
	if v, ok := src.DescriptionText(); ok {
		dst.SetDescription(v)
	}
	if v, ok := src.Unit(); ok {
		dst.SetUnit(v)
	}
	cb.DecodeAttrs(dst.Metadata(), src.MetadataIter())
	data, ok := src.Data()
	if !ok {
		return // MetricTypeEmpty
	}
	// An unknown tag (a newer writer's metric kind) leaves the metric Empty rather than
	// guessing — the same forward-compatibility rule as putValue's unknown AnyValue.
	switch data.Tag() {
	case g.MetricDataTagGauge:
		gs := data.Gauge()
		putNumberPoints(dst.SetEmptyGauge().DataPoints(), gs.DataPointsIter())
	case g.MetricDataTagSum:
		s := data.Sum()
		out := dst.SetEmptySum()
		if v, ok := s.AggregationTemporality(); ok {
			out.SetAggregationTemporality(pmetric.AggregationTemporality(v))
		}
		if v, ok := s.IsMonotonic(); ok {
			out.SetIsMonotonic(v)
		}
		putNumberPoints(out.DataPoints(), s.DataPointsIter())
	case g.MetricDataTagHistogram:
		h := data.Histogram()
		out := dst.SetEmptyHistogram()
		if v, ok := h.AggregationTemporality(); ok {
			out.SetAggregationTemporality(pmetric.AggregationTemporality(v))
		}
		it := h.DataPointsIter()
		for dp, ok := it.Next(); ok; dp, ok = it.Next() {
			putHistogramPoint(out.DataPoints().AppendEmpty(), dp)
		}
	case g.MetricDataTagExponentialHistogram:
		h := data.ExponentialHistogram()
		out := dst.SetEmptyExponentialHistogram()
		if v, ok := h.AggregationTemporality(); ok {
			out.SetAggregationTemporality(pmetric.AggregationTemporality(v))
		}
		it := h.DataPointsIter()
		for dp, ok := it.Next(); ok; dp, ok = it.Next() {
			putExpHistogramPoint(out.DataPoints().AppendEmpty(), dp)
		}
	case g.MetricDataTagSummary:
		out := dst.SetEmptySummary()
		it := data.SummaryIter()
		for dp, ok := it.Next(); ok; dp, ok = it.Next() {
			putSummaryPoint(out.DataPoints().AppendEmpty(), dp)
		}
	}
}

func putNumberPoints[I any, P cb.Cursor[I, g.NumberDataPointAccessor]](dst pmetric.NumberDataPointSlice, it I) {
	c := P(&it)
	dst.EnsureCapacity(c.Len())
	for src, ok := c.Next(); ok; src, ok = c.Next() {
		p := dst.AppendEmpty()
		cb.DecodeAttrs(p.Attributes(), src.AttributesIter())
		if v, ok := src.StartTimeUnixNano(); ok {
			p.SetStartTimestamp(pcommon.Timestamp(v))
		}
		if v, ok := src.TimeUnixNano(); ok {
			p.SetTimestamp(pcommon.Timestamp(v))
		}
		putExemplars(p.Exemplars(), src.ExemplarsIter())
		if v, ok := src.Flags(); ok {
			p.SetFlags(pmetric.DataPointFlags(v))
		}
		if v, ok := src.Value(); ok {
			switch v.Tag() {
			case g.NumberValueTagAsDouble:
				p.SetDoubleValue(v.AsDouble())
			case g.NumberValueTagAsInt:
				p.SetIntValue(v.AsInt())
			}
		}
	}
}

func putHistogramPoint(p pmetric.HistogramDataPoint, src g.HistogramDataPointAccessor) {
	cb.DecodeAttrs(p.Attributes(), src.AttributesIter())
	if v, ok := src.StartTimeUnixNano(); ok {
		p.SetStartTimestamp(pcommon.Timestamp(v))
	}
	if v, ok := src.TimeUnixNano(); ok {
		p.SetTimestamp(pcommon.Timestamp(v))
	}
	if v, ok := src.Count(); ok {
		p.SetCount(v)
	}
	if v, ok := src.Sum(); ok {
		p.SetSum(v)
	}
	if n, ok := src.BucketCountsLen(); ok {
		bc := p.BucketCounts()
		bc.EnsureCapacity(n)
		it := src.BucketCountsIter()
		for v, ok := it.Next(); ok; v, ok = it.Next() {
			bc.Append(v)
		}
	}
	if n, ok := src.ExplicitBoundsLen(); ok {
		eb := p.ExplicitBounds()
		eb.EnsureCapacity(n)
		it := src.ExplicitBoundsIter()
		for v, ok := it.Next(); ok; v, ok = it.Next() {
			eb.Append(v)
		}
	}
	putExemplars(p.Exemplars(), src.ExemplarsIter())
	if v, ok := src.Flags(); ok {
		p.SetFlags(pmetric.DataPointFlags(v))
	}
	if v, ok := src.Min(); ok {
		p.SetMin(v)
	}
	if v, ok := src.Max(); ok {
		p.SetMax(v)
	}
}

func putExpHistogramPoint(p pmetric.ExponentialHistogramDataPoint, src g.ExponentialHistogramDataPointAccessor) {
	cb.DecodeAttrs(p.Attributes(), src.AttributesIter())
	if v, ok := src.StartTimeUnixNano(); ok {
		p.SetStartTimestamp(pcommon.Timestamp(v))
	}
	if v, ok := src.TimeUnixNano(); ok {
		p.SetTimestamp(pcommon.Timestamp(v))
	}
	if v, ok := src.Count(); ok {
		p.SetCount(v)
	}
	if v, ok := src.Sum(); ok {
		p.SetSum(v)
	}
	if v, ok := src.Scale(); ok {
		p.SetScale(v)
	}
	if v, ok := src.ZeroCount(); ok {
		p.SetZeroCount(v)
	}
	if b, ok := src.Positive(); ok {
		putBuckets(p.Positive(), b)
	}
	if b, ok := src.Negative(); ok {
		putBuckets(p.Negative(), b)
	}
	if v, ok := src.Flags(); ok {
		p.SetFlags(pmetric.DataPointFlags(v))
	}
	putExemplars(p.Exemplars(), src.ExemplarsIter())
	if v, ok := src.Min(); ok {
		p.SetMin(v)
	}
	if v, ok := src.Max(); ok {
		p.SetMax(v)
	}
	if v, ok := src.ZeroThreshold(); ok {
		p.SetZeroThreshold(v)
	}
}

func putBuckets(dst pmetric.ExponentialHistogramDataPointBuckets, src g.BucketsAccessor) {
	if v, ok := src.Offset(); ok {
		dst.SetOffset(v)
	}
	if n, ok := src.BucketCountsLen(); ok {
		bc := dst.BucketCounts()
		bc.EnsureCapacity(n)
		it := src.BucketCountsIter()
		for v, ok := it.Next(); ok; v, ok = it.Next() {
			bc.Append(v)
		}
	}
}

func putSummaryPoint(p pmetric.SummaryDataPoint, src g.SummaryDataPointAccessor) {
	cb.DecodeAttrs(p.Attributes(), src.AttributesIter())
	if v, ok := src.StartTimeUnixNano(); ok {
		p.SetStartTimestamp(pcommon.Timestamp(v))
	}
	if v, ok := src.TimeUnixNano(); ok {
		p.SetTimestamp(pcommon.Timestamp(v))
	}
	if v, ok := src.Count(); ok {
		p.SetCount(v)
	}
	if v, ok := src.Sum(); ok {
		p.SetSum(v)
	}
	if n, ok := src.QuantileValuesLen(); ok {
		qs := p.QuantileValues()
		qs.EnsureCapacity(n)
		it := src.QuantileValuesIter()
		for q, ok := it.Next(); ok; q, ok = it.Next() {
			out := qs.AppendEmpty()
			if v, ok := q.Quantile(); ok {
				out.SetQuantile(v)
			}
			if v, ok := q.Value(); ok {
				out.SetValue(v)
			}
		}
	}
	if v, ok := src.Flags(); ok {
		p.SetFlags(pmetric.DataPointFlags(v))
	}
}

func putExemplars[I any, P cb.Cursor[I, g.ExemplarAccessor]](dst pmetric.ExemplarSlice, it I) {
	c := P(&it)
	if c.Len() == 0 {
		return
	}
	dst.EnsureCapacity(c.Len())
	for src, ok := c.Next(); ok; src, ok = c.Next() {
		e := dst.AppendEmpty()
		cb.DecodeAttrs(e.FilteredAttributes(), src.FilteredAttributesIter())
		if v, ok := src.TimeUnixNano(); ok {
			e.SetTimestamp(pcommon.Timestamp(v))
		}
		if v, ok := src.SpanID(); ok {
			e.SetSpanID(cb.ToSpanID(v))
		}
		if v, ok := src.TraceID(); ok {
			e.SetTraceID(cb.ToTraceID(v))
		}
		if v, ok := src.Value(); ok {
			switch v.Tag() {
			case g.NumberValueTagAsDouble:
				e.SetDoubleValue(v.AsDouble())
			case g.NumberValueTagAsInt:
				e.SetIntValue(v.AsInt())
			}
		}
	}
}
