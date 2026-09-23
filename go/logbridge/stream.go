package logbridge

import (
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"

	"otlpgen/dagr"
	d "otlpgen/otlplogsdirect"
)

// StreamBuilder is the one-pass logs encoder — pdatabridge.StreamBuilder's twin. pdata is
// walked once: the container levels (LogsData / ResourceLogs / ScopeLogs) are written by
// hand, and each LogRecord is built as a value struct in reused scratch and written with
// the generated PutLogRecord. Once the scratch has grown nothing allocates (nested Slice /
// Map values excepted). TestStreamMatches gates the bytes against the value-tree path.
type StreamBuilder struct {
	b   *dagr.Builder
	lv  levels
	kv  pool[d.KeyValue]
	ids pool[byte]
}

func NewStreamBuilder() *StreamBuilder { return &StreamBuilder{b: dagr.NewBuilder(0)} }

// BuildAppend encodes ld as an OtlpLogs buffer into dst — the same bytes as ToBytes.
func (w *StreamBuilder) BuildAppend(dst []byte, ld plog.Logs) []byte {
	b := w.b
	b.Reset()
	buf, i := b.Take()
	before := len(buf) - i
	if ld.ResourceLogs().Len() > 0 {
		buf, i = w.putResourceLogs(ld, b, buf, i)
		buf, i = dagr.PutLEBAt(buf, i, 1) // LogsData.resource_logs, field 0
	}
	buf, i = dagr.PutLEBAt(buf, i, uint64((len(buf)-i)-before))
	b.Put(buf, i)
	return finishGraph(b, dst)
}

// AppendRecord encodes ld as the content of one LogRecordLog `Batch` record — what
// spool.Writer.AppendRecord takes — in the same single pass.
func (w *StreamBuilder) AppendRecord(dst []byte, receivedUnixNano uint64, ld plog.Logs) []byte {
	b := w.b
	b.Reset()
	buf, i := b.Take()
	before := len(buf) - i
	buf, i = w.putResourceLogs(ld, b, buf, i)
	buf, i = dagr.PutLEBAt(buf, i, 5) // Batch.resource_logs, field 2 (required)
	buf, i = putRecordEnvelope(buf, i, before, receivedUnixNano, uint32(ld.LogRecordCount()))
	b.Put(buf, i)
	return b.AppendTo(dst)
}

// putResourceLogs writes the resource_logs array content (no field tag).
func (w *StreamBuilder) putResourceLogs(ld plog.Logs, b *dagr.Builder, buf []byte, i int) ([]byte, int) {
	rls := ld.ResourceLogs()
	return putArray(buf, i, rls.Len(), func(k int, buf []byte, i int) ([]byte, int) {
		rl := rls.At(k)
		sls := rl.ScopeLogs()
		return putLevel(buf, i, rl.SchemaUrl(), sls.Len(),
			func(k int, buf []byte, i int) ([]byte, int) {
				sl := sls.At(k)
				lrs := sl.LogRecords()
				return putLevel(buf, i, sl.SchemaUrl(), lrs.Len(),
					func(k int, buf []byte, i int) ([]byte, int) {
						return d.PutLogRecord(w.logRecordValue(lrs.At(k)), b, buf, i)
					},
					func(buf []byte, i int) ([]byte, int) { return w.lv.putScope(sl.Scope(), b, buf, i) })
			},
			func(buf []byte, i int) ([]byte, int) { return w.lv.putResource(rl.Resource(), b, buf, i) })
	})
}

// logRecordValue is toLogRecord over reused scratch: the same value, so the same bytes.
func (w *StreamBuilder) logRecordValue(r plog.LogRecord) d.LogRecord {
	w.kv.reset()
	w.ids.reset()
	tid, sid := r.TraceID(), r.SpanID()
	out := d.LogRecord{
		TimeUnixNano:           nonZero(uint64(r.Timestamp())),
		ObservedTimeUnixNano:   nonZero(uint64(r.ObservedTimestamp())),
		SeverityNumber:         nz(d.SeverityNumber(r.SeverityNumber())),
		SeverityText:           nonEmpty(r.SeverityText()),
		Attributes:             attrsInto(&w.kv, r.Attributes()),
		DroppedAttributesCount: nonZeroU32(r.DroppedAttributesCount()),
		Flags:                  nonZeroU32(uint32(r.Flags())),
		TraceID:                idInto(&w.ids, tid[:]),
		SpanID:                 idInto(&w.ids, sid[:]),
		EventName:              nonEmpty(r.EventName()),
	}
	if r.Body().Type() != pcommon.ValueTypeEmpty {
		if v, ok := attrValue(r.Body()); ok {
			out.Body = some(v)
		}
	}
	return out
}
