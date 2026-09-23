package logbridge

import (
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"

	"otlpgen/logrecordlog"
	g "otlpgen/otlplogs"
)

// ToLogs materialises an OtlpLogs buffer into pdata, for handing off to an unmodified
// collector pipeline. The slow path by construction — see pdatabridge.ToTraces.
func ToLogs(buf []byte) (plog.Logs, error) {
	root, err := g.OpenOtlpLogsUnchecked(buf)
	if err != nil {
		return plog.Logs{}, err
	}
	return putLogs(root.ResourceLogsIter()), nil
}

// FromRecord materialises one LogRecordLog spool record (spool.Batch.Record) into pdata. The
// record holds the batch's ResourceLogs inline, read in place with the same accessors
// ToLogs uses — logrecordlog aliases them — so the two share one body.
func FromRecord(record []byte) plog.Logs {
	return putLogs(logrecordlog.NewBatchAccessor(record, 0).ResourceLogsIter())
}

func putLogs[I any, P cursor[I, g.ResourceLogsAccessor]](it I) plog.Logs {
	c := P(&it)
	ld := plog.NewLogs()
	for src, ok := c.Next(); ok; src, ok = c.Next() {
		rl := ld.ResourceLogs().AppendEmpty()
		if r, ok := src.Resource(); ok {
			putResource(rl.Resource(), r)
		}
		if v, ok := src.SchemaURL(); ok {
			rl.SetSchemaUrl(v)
		}
		it := src.ScopeLogsIter()
		for slSrc, ok := it.Next(); ok; slSrc, ok = it.Next() {
			sl := rl.ScopeLogs().AppendEmpty()
			if sc, ok := slSrc.Scope(); ok {
				putScope(sl.Scope(), sc)
			}
			if v, ok := slSrc.SchemaURL(); ok {
				sl.SetSchemaUrl(v)
			}
			if k, ok := slSrc.LogRecordsLen(); ok {
				sl.LogRecords().EnsureCapacity(k)
			}
			it := slSrc.LogRecordsIter()
			for lr, ok := it.Next(); ok; lr, ok = it.Next() {
				putLogRecord(sl.LogRecords().AppendEmpty(), lr)
			}
		}
	}
	return ld
}

func putLogRecord(r plog.LogRecord, src g.LogRecordAccessor) {
	if v, ok := src.TimeUnixNano(); ok {
		r.SetTimestamp(pcommon.Timestamp(v))
	}
	if v, ok := src.ObservedTimeUnixNano(); ok {
		r.SetObservedTimestamp(pcommon.Timestamp(v))
	}
	if v, ok := src.SeverityNumber(); ok {
		r.SetSeverityNumber(plog.SeverityNumber(v))
	}
	if v, ok := src.SeverityText(); ok {
		r.SetSeverityText(v)
	}
	if v, ok := src.Body(); ok {
		putValue(r.Body(), v, 0)
	}
	putAttrs(r.Attributes(), src.AttributesIter())
	if v, ok := src.DroppedAttributesCount(); ok {
		r.SetDroppedAttributesCount(v)
	}
	if v, ok := src.Flags(); ok {
		r.SetFlags(plog.LogRecordFlags(v))
	}
	if v, ok := src.TraceID(); ok {
		r.SetTraceID(toTraceID(v))
	}
	if v, ok := src.SpanID(); ok {
		r.SetSpanID(toSpanID(v))
	}
	if v, ok := src.EventName(); ok {
		r.SetEventName(v)
	}
}
