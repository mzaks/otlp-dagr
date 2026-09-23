// Package logbridge converts between pdata logs (`plog`) and an OTLP-on-Dagr `OtlpLogs`
// buffer — the logs twin of package pdatabridge (traces). Encode goes through the direct
// builder; decode (ToLogs) is the compatibility path for an unmodified pipeline.
//
// The shared messages (attributes, the AnyValue body, Resource, InstrumentationScope)
// come from common_gen.go, which regen.sh copies from pdatabridge/common.go.
package logbridge

import (
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"

	d "otlpgen/otlplogsdirect"
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
				Scope:      some(scope(sl.Scope())),
				LogRecords: optSlice(outLR),
				SchemaURL:  nonEmpty(sl.SchemaUrl()),
			})
		}
		outRL = append(outRL, d.ResourceLogs{
			Resource:  some(resource(rl.Resource())),
			ScopeLogs: optSlice(outSL),
			SchemaURL: nonEmpty(rl.SchemaUrl()),
		})
	}
	return d.LogsData{ResourceLogs: optSlice(outRL)}
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
		TimeUnixNano:           nonZero(uint64(r.Timestamp())),
		ObservedTimeUnixNano:   nonZero(uint64(r.ObservedTimestamp())),
		SeverityNumber:         nz(d.SeverityNumber(r.SeverityNumber())),
		SeverityText:           nonEmpty(r.SeverityText()),
		Attributes:             optSlice(keyValues(r.Attributes())),
		DroppedAttributesCount: nonZeroU32(r.DroppedAttributesCount()),
		Flags:                  nonZeroU32(uint32(r.Flags())),
		TraceID:                traceID(r.TraceID()),
		SpanID:                 spanID(r.SpanID()),
		EventName:              nonEmpty(r.EventName()),
	}
	// The body is an AnyValue like any attribute value; an Empty body is an absent one.
	if r.Body().Type() != pcommon.ValueTypeEmpty {
		if v, ok := attrValue(r.Body()); ok {
			out.Body = some(v)
		}
	}
	return out
}
