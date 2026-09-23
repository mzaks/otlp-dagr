package spool

// signal.go — the four spools. Every signal's DataSink (SpanLog, MetricLog, LogRecordLog,
// ProfileLog in schema.py) has the same shape: one doubly-linked record type, `Batch`,
// holding the receive time, an item count and the batch as a raw embedded graph. The
// framing is identical too, so everything in spool.go that does not touch a record body
// — tail integrity, recovery, the reverse walk — is shared, and only these adapters
// know which generated sink package is behind a Signal.

import (
	"fmt"
	"io"

	"otlpgen/logrecordlog"
	"otlpgen/metriclog"
	"otlpgen/profilelog"
	"otlpgen/spanlog"
)

// Signal selects the spool (and the OTLP graph its records embed).
type Signal uint8

const (
	Traces   Signal = iota // SpanLog: Count = spans, Payload = an OtlpTraces buffer
	Metrics                // MetricLog: Count = data points, Payload = an OtlpMetrics buffer
	Logs                   // LogRecordLog: Count = log records, Payload = an OtlpLogs buffer
	Profiles               // ProfileLog: Count = samples, Payload = an OtlpProfiles buffer
)

func (s Signal) String() string {
	switch s {
	case Traces:
		return "traces"
	case Metrics:
		return "metrics"
	case Logs:
		return "logs"
	case Profiles:
		return "profiles"
	}
	return fmt.Sprintf("signal(%d)", uint8(s))
}

// Range is where a record sits in the spool: [Start, End) including its trailing RLEB.
type Range struct{ TypeID, Start, End int }

// framedWriter is the generated stream writer, reduced to what Writer needs.
type framedWriter interface {
	EnsureFraming() error
	append(receivedUnixNano uint64, count uint32, payload []byte) error
}

type ops struct {
	newWriter    func(io.Writer) framedWriter
	recordsStart func(buf []byte) (int, error)
}

// forward / reverse dispatch with a switch of DIRECT calls, not through func-valued
// fields: a callback passed through a func value escapes (the compiler cannot see what
// the callee does with it), so every caller's closure would move to the heap — the
// store's allocation gate caught exactly that (6 allocations per scan instead of <= 2).
func forward(sig Signal, buf []byte, fn func(Batch) bool) error {
	switch sig {
	case Traces:
		return spanForward(buf, fn)
	case Metrics:
		return metricForward(buf, fn)
	case Logs:
		return logForward(buf, fn)
	case Profiles:
		return profileForward(buf, fn)
	}
	return fmt.Errorf("spool: unknown signal %d", uint8(sig))
}

func reverse(sig Signal, buf []byte, fn func(Batch) bool) error {
	switch sig {
	case Traces:
		return spanReverse(buf, fn)
	case Metrics:
		return metricReverse(buf, fn)
	case Logs:
		return logReverse(buf, fn)
	case Profiles:
		return profileReverse(buf, fn)
	}
	return fmt.Errorf("spool: unknown signal %d", uint8(sig))
}

func (s Signal) ops() ops {
	switch s {
	case Traces:
		return spanOps
	case Metrics:
		return metricOps
	case Logs:
		return logOps
	case Profiles:
		return profileOps
	}
	panic(fmt.Sprintf("spool: unknown signal %d", uint8(s)))
}

// ── traces ──

type spanW struct{ *spanlog.SpanLogWriter }

func (w spanW) append(t uint64, n uint32, p []byte) error {
	_, err := w.AppendBatch(spanlog.Batch{ReceivedUnixNano: t, SpanCount: n, Traces: p})
	return err
}

func spanBatch(rec spanlog.SpanLogRecord) (Batch, bool) {
	v, ok := rec.Batch()
	r := rec.Range()
	return Batch{v.ReceivedUnixNano, v.SpanCount, v.Traces, Range{r.TypeID, r.Start, r.End}}, ok
}

var spanOps = ops{
	newWriter: func(w io.Writer) framedWriter { return spanW{spanlog.NewSpanLogStreamWriter(w)} },
	recordsStart: func(buf []byte) (int, error) {
		rd, err := spanlog.OpenSpanLogReader(buf)
		if err != nil {
			return 0, err
		}
		return rd.RecordsStart(), nil
	},
}

func spanForward(buf []byte, fn func(Batch) bool) error {
	rd, err := spanlog.OpenSpanLogReader(buf)
	if err != nil {
		return err
	}
	for rec, ok := rd.Next(); ok; rec, ok = rd.Next() {
		if b, ok := spanBatch(rec); ok && !fn(b) {
			return nil
		}
	}
	return nil
}

func spanReverse(buf []byte, fn func(Batch) bool) error {
	rd, err := spanlog.OpenSpanLogReverseReader(buf)
	if err != nil {
		return err
	}
	for rec, ok := rd.Prev(); ok; rec, ok = rd.Prev() {
		if b, ok := spanBatch(rec); ok && !fn(b) {
			return nil
		}
	}
	return nil
}

// ── metrics ──

type metricW struct{ *metriclog.MetricLogWriter }

func (w metricW) append(t uint64, n uint32, p []byte) error {
	_, err := w.AppendBatch(metriclog.Batch{ReceivedUnixNano: t, DataPointCount: n, Metrics: p})
	return err
}

func metricBatch(rec metriclog.MetricLogRecord) (Batch, bool) {
	v, ok := rec.Batch()
	r := rec.Range()
	return Batch{v.ReceivedUnixNano, v.DataPointCount, v.Metrics, Range{r.TypeID, r.Start, r.End}}, ok
}

var metricOps = ops{
	newWriter: func(w io.Writer) framedWriter { return metricW{metriclog.NewMetricLogStreamWriter(w)} },
	recordsStart: func(buf []byte) (int, error) {
		rd, err := metriclog.OpenMetricLogReader(buf)
		if err != nil {
			return 0, err
		}
		return rd.RecordsStart(), nil
	},
}

func metricForward(buf []byte, fn func(Batch) bool) error {
	rd, err := metriclog.OpenMetricLogReader(buf)
	if err != nil {
		return err
	}
	for rec, ok := rd.Next(); ok; rec, ok = rd.Next() {
		if b, ok := metricBatch(rec); ok && !fn(b) {
			return nil
		}
	}
	return nil
}

func metricReverse(buf []byte, fn func(Batch) bool) error {
	rd, err := metriclog.OpenMetricLogReverseReader(buf)
	if err != nil {
		return err
	}
	for rec, ok := rd.Prev(); ok; rec, ok = rd.Prev() {
		if b, ok := metricBatch(rec); ok && !fn(b) {
			return nil
		}
	}
	return nil
}

// ── logs ──

type logW struct {
	*logrecordlog.LogRecordLogWriter
}

func (w logW) append(t uint64, n uint32, p []byte) error {
	_, err := w.AppendBatch(logrecordlog.Batch{ReceivedUnixNano: t, LogRecordCount: n, Logs: p})
	return err
}

func logBatch(rec logrecordlog.LogRecordLogRecord) (Batch, bool) {
	v, ok := rec.Batch()
	r := rec.Range()
	return Batch{v.ReceivedUnixNano, v.LogRecordCount, v.Logs, Range{r.TypeID, r.Start, r.End}}, ok
}

var logOps = ops{
	newWriter: func(w io.Writer) framedWriter { return logW{logrecordlog.NewLogRecordLogStreamWriter(w)} },
	recordsStart: func(buf []byte) (int, error) {
		rd, err := logrecordlog.OpenLogRecordLogReader(buf)
		if err != nil {
			return 0, err
		}
		return rd.RecordsStart(), nil
	},
}

func logForward(buf []byte, fn func(Batch) bool) error {
	rd, err := logrecordlog.OpenLogRecordLogReader(buf)
	if err != nil {
		return err
	}
	for rec, ok := rd.Next(); ok; rec, ok = rd.Next() {
		if b, ok := logBatch(rec); ok && !fn(b) {
			return nil
		}
	}
	return nil
}

func logReverse(buf []byte, fn func(Batch) bool) error {
	rd, err := logrecordlog.OpenLogRecordLogReverseReader(buf)
	if err != nil {
		return err
	}
	for rec, ok := rd.Prev(); ok; rec, ok = rd.Prev() {
		if b, ok := logBatch(rec); ok && !fn(b) {
			return nil
		}
	}
	return nil
}

// ── profiles ──

type profileW struct{ *profilelog.ProfileLogWriter }

func (w profileW) append(t uint64, n uint32, p []byte) error {
	_, err := w.AppendBatch(profilelog.Batch{ReceivedUnixNano: t, SampleCount: n, Profiles: p})
	return err
}

func profileBatch(rec profilelog.ProfileLogRecord) (Batch, bool) {
	v, ok := rec.Batch()
	r := rec.Range()
	return Batch{v.ReceivedUnixNano, v.SampleCount, v.Profiles, Range{r.TypeID, r.Start, r.End}}, ok
}

var profileOps = ops{
	newWriter: func(w io.Writer) framedWriter { return profileW{profilelog.NewProfileLogStreamWriter(w)} },
	recordsStart: func(buf []byte) (int, error) {
		rd, err := profilelog.OpenProfileLogReader(buf)
		if err != nil {
			return 0, err
		}
		return rd.RecordsStart(), nil
	},
}

func profileForward(buf []byte, fn func(Batch) bool) error {
	rd, err := profilelog.OpenProfileLogReader(buf)
	if err != nil {
		return err
	}
	for rec, ok := rd.Next(); ok; rec, ok = rd.Next() {
		if b, ok := profileBatch(rec); ok && !fn(b) {
			return nil
		}
	}
	return nil
}

func profileReverse(buf []byte, fn func(Batch) bool) error {
	rd, err := profilelog.OpenProfileLogReverseReader(buf)
	if err != nil {
		return err
	}
	for rec, ok := rd.Prev(); ok; rec, ok = rd.Prev() {
		if b, ok := profileBatch(rec); ok && !fn(b) {
			return nil
		}
	}
	return nil
}
