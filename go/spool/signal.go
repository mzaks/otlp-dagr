package spool

// signal.go — the four spools. Every signal's DataSink (SpanLog, MetricLog, LogRecordLog,
// ProfileLog in schema.py) has the same shape: one doubly-linked record type, `Batch`,
// holding the receive time, an item count and the signal's top-level array inline
// (`resource_spans`, ...; profiles adds its `dictionary`). The framing is identical too,
// so everything in spool.go that does not touch a record body — tail integrity,
// recovery, the reverse walk — is shared, and only these adapters know which generated
// sink package is behind a Signal.

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
	Traces   Signal = iota // SpanLog: Count = spans, Record = a spanlog Batch
	Metrics                // MetricLog: Count = data points, Record = a metriclog Batch
	Logs                   // LogRecordLog: Count = log records, Record = a logrecordlog Batch
	Profiles               // ProfileLog: Count = samples, Record = a profilelog Batch
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

// batchTypeID is the typeId of `Batch`, the one record type of every spool.
const batchTypeID = 0

// framedWriter is the generated stream writer, reduced to what Writer needs.
type framedWriter interface {
	EnsureFraming() error
	appendRaw(record []byte) error
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

func (w spanW) appendRaw(record []byte) error {
	_, err := w.AppendRaw(batchTypeID, record)
	return err
}

// AppendTraces writes one SpanLog batch from values: the span graph's direct-builder
// values, which spanlog aliases, stored with that graph's own code.
func (w *Writer) AppendTraces(v spanlog.Batch) error {
	sw, ok := w.w.(spanW)
	if !ok {
		return w.wrongSignal(Traces)
	}
	_, err := sw.AppendBatch(v)
	return w.appended(err)
}

func spanBatch(rec spanlog.SpanLogRecord) (Batch, bool) {
	a, ok := rec.BatchAccessor()
	if !ok {
		return Batch{}, false
	}
	r := rec.Range()
	return Batch{a.ReceivedUnixNano(), a.SpanCount(), rec.Raw(), Range{r.TypeID, r.Start, r.End}}, true
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

func (w metricW) appendRaw(record []byte) error {
	_, err := w.AppendRaw(batchTypeID, record)
	return err
}

// AppendMetrics writes one MetricLog batch from values: the metric graph's direct-builder
// values, which metriclog aliases, stored with that graph's own code.
func (w *Writer) AppendMetrics(v metriclog.Batch) error {
	sw, ok := w.w.(metricW)
	if !ok {
		return w.wrongSignal(Metrics)
	}
	_, err := sw.AppendBatch(v)
	return w.appended(err)
}

func metricBatch(rec metriclog.MetricLogRecord) (Batch, bool) {
	a, ok := rec.BatchAccessor()
	if !ok {
		return Batch{}, false
	}
	r := rec.Range()
	return Batch{a.ReceivedUnixNano(), a.DataPointCount(), rec.Raw(), Range{r.TypeID, r.Start, r.End}}, true
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

func (w logW) appendRaw(record []byte) error {
	_, err := w.AppendRaw(batchTypeID, record)
	return err
}

// AppendLogs writes one LogRecordLog batch from values: the log graph's direct-builder
// values, which logrecordlog aliases, stored with that graph's own code.
func (w *Writer) AppendLogs(v logrecordlog.Batch) error {
	sw, ok := w.w.(logW)
	if !ok {
		return w.wrongSignal(Logs)
	}
	_, err := sw.AppendBatch(v)
	return w.appended(err)
}

func logBatch(rec logrecordlog.LogRecordLogRecord) (Batch, bool) {
	a, ok := rec.BatchAccessor()
	if !ok {
		return Batch{}, false
	}
	r := rec.Range()
	return Batch{a.ReceivedUnixNano(), a.LogRecordCount(), rec.Raw(), Range{r.TypeID, r.Start, r.End}}, true
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

func (w profileW) appendRaw(record []byte) error {
	_, err := w.AppendRaw(batchTypeID, record)
	return err
}

// AppendProfiles writes one ProfileLog batch from values: the profile graph's direct-builder
// values, which profilelog aliases, stored with that graph's own code.
func (w *Writer) AppendProfiles(v profilelog.Batch) error {
	sw, ok := w.w.(profileW)
	if !ok {
		return w.wrongSignal(Profiles)
	}
	_, err := sw.AppendBatch(v)
	return w.appended(err)
}

func profileBatch(rec profilelog.ProfileLogRecord) (Batch, bool) {
	a, ok := rec.BatchAccessor()
	if !ok {
		return Batch{}, false
	}
	r := rec.Range()
	return Batch{a.ReceivedUnixNano(), a.SampleCount(), rec.Raw(), Range{r.TypeID, r.Start, r.End}}, true
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
