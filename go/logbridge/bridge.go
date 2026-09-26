// Package logbridge converts between pdata logs (`plog`) and an OTLP-on-Dagr `OtlpLogs`
// buffer — the logs twin of package pdatabridge (traces). Encode goes through the direct
// builder; decode (ToLogs) is the compatibility path for an unmodified pipeline.
//
// The shared messages (attributes, the AnyValue body, Resource, InstrumentationScope)
// convert through package commonbridge.
package logbridge

import (
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"

	d "otlpgen/otlplogsdirect"

	cb "otlpdagr/commonbridge"
)

// ToBytes serializes ld into an OtlpLogs buffer via the direct builder.
func ToBytes(ld plog.Logs) []byte { return d.BuildOtlpLogs(ToValue(ld)) }

// ToValue materialises the value tree the direct builder consumes.
func ToValue(ld plog.Logs) d.LogsData {
	rls := ld.ResourceLogs()
	outRL := make([]d.ResourceLogs, 0, rls.Len())
	for i := 0; i < rls.Len(); i++ {
		rl := rls.At(i)
		sls := rl.ScopeLogs()
		outSL := make([]d.ScopeLogs, 0, sls.Len())
		for j := 0; j < sls.Len(); j++ {
			sl := sls.At(j)
			lrs := sl.LogRecords()
			outLR := make([]d.LogRecord, 0, lrs.Len())
			for k := 0; k < lrs.Len(); k++ {
				outLR = append(outLR, toLogRecord(lrs.At(k)))
			}
			outSL = append(outSL, d.ScopeLogs{
				Scope:      cb.Some(cb.Scope(sl.Scope())),
				LogRecords: cb.OptSlice(outLR),
				SchemaURL:  cb.NonEmpty(sl.SchemaUrl()),
			})
		}
		outRL = append(outRL, d.ResourceLogs{
			Resource:  cb.Some(cb.Resource(rl.Resource())),
			ScopeLogs: cb.OptSlice(outSL),
			SchemaURL: cb.NonEmpty(rl.SchemaUrl()),
		})
	}
	return d.LogsData{ResourceLogs: cb.OptSlice(outRL)}
}

// Builder is the reusable form: one scratch buffer, reset per Build.
type Builder struct{ b *d.OtlpLogsBuilder }

func NewBuilder() *Builder { return &Builder{b: d.NewOtlpLogsBuilder()} }

func (w *Builder) Build(ld plog.Logs) []byte { return w.b.Build(ToValue(ld)) }

// BuildAppend appends the encoded batch to dst and returns the extended slice.
func (w *Builder) BuildAppend(dst []byte, ld plog.Logs) []byte {
	return w.b.BuildAppend(dst, ToValue(ld))
}

func toLogRecord(r plog.LogRecord) d.LogRecord {
	out := d.LogRecord{
		TimeUnixNano:           cb.NonZero(uint64(r.Timestamp())),
		ObservedTimeUnixNano:   cb.NonZero(uint64(r.ObservedTimestamp())),
		SeverityNumber:         cb.NZ(d.SeverityNumber(r.SeverityNumber())),
		SeverityText:           cb.NonEmpty(r.SeverityText()),
		Attributes:             cb.OptSlice(cb.KeyValues(r.Attributes())),
		DroppedAttributesCount: cb.NonZeroU32(r.DroppedAttributesCount()),
		Flags:                  cb.NonZeroU32(uint32(r.Flags())),
		TraceID:                cb.TraceID(r.TraceID()),
		SpanID:                 cb.SpanID(r.SpanID()),
		EventName:              cb.NonEmpty(r.EventName()),
	}
	// The body is an AnyValue like any attribute value; an Empty body is an absent one.
	if r.Body().Type() != pcommon.ValueTypeEmpty {
		if v, ok := cb.AttrValue(r.Body()); ok {
			out.Body = cb.Some(v)
		}
	}
	return out
}
