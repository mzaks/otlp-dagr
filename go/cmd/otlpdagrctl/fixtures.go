package main

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/pprofile"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"otlpdagr/internal/fixture"
	"otlpdagr/logbridge"
	"otlpdagr/metricbridge"
	"otlpdagr/pdatabridge"
	"otlpdagr/profilebridge"
	"otlpdagr/spool"
	"otlpgen/logrecordlog"
	"otlpgen/metriclog"
	"otlpgen/spanlog"
)

// fixtures writes every fixture the OTAP gate (rust/otap/check.sh) converts, twice: as the
// signal's Dagr graph buffer (<signal>-<name>.dagr, the bridge's input) and as OTLP
// protobuf (<signal>-<name>.pb, what otel-arrow's decoder must give back).
//
//	otlpdagrctl fixtures -out DIR
func fixtures(args []string) error {
	fs := flag.NewFlagSet("fixtures", flag.ExitOnError)
	out := fs.String("out", "", "directory to write into")
	_ = fs.Parse(args)
	if *out == "" {
		return errors.New("fixtures: -out is required")
	}
	if err := os.MkdirAll(*out, 0o755); err != nil {
		return err
	}
	write := func(name string, dagr []byte, pb []byte, err error) error {
		if err != nil {
			return fmt.Errorf("%s: %w", name, err)
		}
		if err := os.WriteFile(filepath.Join(*out, name+".dagr"), dagr, 0o644); err != nil {
			return err
		}
		return os.WriteFile(filepath.Join(*out, name+".pb"), pb, 0o644)
	}
	traces := map[string]ptrace.Traces{
		"wide":   fixture.TracesWide(),
		"batch":  fixture.TracesN(500),
		"links":  fixture.TracesWithLinks(50),
		"nested": fixture.TracesWithNestedAttrs(50),
		"empty":  ptrace.NewTraces(),
	}
	// a STREAM: one large seeded batch cut into consecutive batches, which share their
	// vocabulary (keys, names, scopes, resources) but not their ids and values — what an
	// exporter sends, and what cross-batch stream state is measured on (bench.sh)
	streamTraces := splitTraces(fixture.TracesN(streamBatches*500), 500)
	for i, td := range streamTraces {
		traces[fmt.Sprintf("stream-%02d", i)] = td
	}
	for name, td := range traces {
		pb, err := (&ptrace.ProtoMarshaler{}).MarshalTraces(td)
		if err := write("traces-"+name, pdatabridge.ToBytes(td), pb, err); err != nil {
			return err
		}
	}
	logs := map[string]plog.Logs{"wide": fixture.LogsWide(), "batch": fixture.LogsBatch(500)}
	streamLogs := splitLogs(fixture.LogsBatch(streamBatches*500), 500)
	for i, ld := range streamLogs {
		logs[fmt.Sprintf("stream-%02d", i)] = ld
	}
	for name, ld := range logs {
		pb, err := (&plog.ProtoMarshaler{}).MarshalLogs(ld)
		if err := write("logs-"+name, logbridge.ToBytes(ld), pb, err); err != nil {
			return err
		}
	}
	metrics := map[string]pmetric.Metrics{
		"wide":      fixture.MetricsWide(),
		"batch":     fixture.MetricsBatch(100, 6),
		"exemplars": exemplarRuns(),
	}
	streamMetrics := splitMetrics(fixture.MetricsBatch(streamBatches*100, 6), 100)
	for i, md := range streamMetrics {
		metrics[fmt.Sprintf("stream-%02d", i)] = md
	}
	for name, md := range metrics {
		pb, err := (&pmetric.ProtoMarshaler{}).MarshalMetrics(md)
		if err := write("metrics-"+name, metricbridge.ToBytes(md), pb, err); err != nil {
			return err
		}
	}
	// profiles: the benchmark's batch only (no OTAP mapping exists, so check.sh skips it;
	// rust/restore-bench reads it). ProtoMarshaler MUTATES its input: build it twice.
	pb, err := (&pprofile.ProtoMarshaler{}).MarshalProfiles(fixture.ProfilesBatch(2000, 400, 200))
	if err := write("profiles-batch", profilebridge.ToBytes(fixture.ProfilesBatch(2000, 400, 200)), pb, err); err != nil {
		return err
	}
	// the same stream batches as a real SPOOL (the exporter's file, go/spool): one record
	// each, in order — the bridge's spool input must give back the stream-NN fixtures
	return writeSpools(*out, streamTraces, streamLogs, streamMetrics)
}

func writeSpools(dir string, td []ptrace.Traces, ld []plog.Logs, md []pmetric.Metrics) error {
	open := func(name string, sig spool.Signal) (*spool.Writer, error) {
		return spool.CreateWith(filepath.Join(dir, name+".spool"), spool.NewHeader(sig, "otlpdagrctl fixtures"))
	}
	w, err := open("traces", spool.Traces)
	if err != nil {
		return err
	}
	for i, t := range td {
		if err := w.AppendTraces(spanlog.Batch{ReceivedUnixNano: uint64(1_758_000_000_000_000_000 + i),
			SpanCount: uint32(t.SpanCount()), ResourceSpans: pdatabridge.ToValue(t).ResourceSpans.V}); err != nil {
			return err
		}
	}
	if err := w.Close(); err != nil {
		return err
	}
	if w, err = open("logs", spool.Logs); err != nil {
		return err
	}
	for i, l := range ld {
		if err := w.AppendLogs(logrecordlog.Batch{ReceivedUnixNano: uint64(1_758_000_000_000_000_000 + i),
			LogRecordCount: uint32(l.LogRecordCount()), ResourceLogs: logbridge.ToValue(l).ResourceLogs.V}); err != nil {
			return err
		}
	}
	if err := w.Close(); err != nil {
		return err
	}
	if w, err = open("metrics", spool.Metrics); err != nil {
		return err
	}
	for i, m := range md {
		if err := w.AppendMetrics(metriclog.Batch{ReceivedUnixNano: uint64(1_758_000_000_000_000_000 + i),
			DataPointCount: uint32(m.DataPointCount()), ResourceMetrics: metricbridge.ToValue(m).ResourceMetrics.V}); err != nil {
			return err
		}
	}
	return w.Close()
}

// exemplarRuns exercises exemplar parent_id QUASI-DELTA, which neither the wide nor the
// batch fixture reaches: runs of equal int and equal double values across data points
// (delta rows), value-less exemplars (always delta in Go), and repeated filtered
// attributes. It avoids the one pattern the Go and Rust decoders read differently — a
// value-less exemplar BETWEEN two equal ones (Go compares with the last row that had a
// value, Rust with the row before).
func exemplarRuns() pmetric.Metrics {
	md := pmetric.NewMetrics()
	sm := md.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty()
	sum := sm.Metrics().AppendEmpty()
	sum.SetName("requests")
	s := sum.SetEmptySum()
	s.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
	s.SetIsMonotonic(true)
	add := func(i int, set func(pmetric.Exemplar)) {
		dp := s.DataPoints().At(i)
		ex := dp.Exemplars().AppendEmpty()
		ex.SetTimestamp(pcommon.Timestamp(1_700_000_000_000_000_000 + int64(i)))
		ex.FilteredAttributes().PutStr("route", "/api")
		set(ex)
	}
	for i := 0; i < 11; i++ {
		dp := s.DataPoints().AppendEmpty()
		dp.SetTimestamp(pcommon.Timestamp(1_700_000_000_000_000_000 + int64(i)))
		dp.SetIntValue(int64(i))
	}
	for i := 0; i < 6; i++ {
		add(i, func(e pmetric.Exemplar) { e.SetIntValue(7) })
	}
	for i := 6; i < 9; i++ {
		add(i, func(e pmetric.Exemplar) { e.SetDoubleValue(1.5) })
		add(i, func(e pmetric.Exemplar) { e.SetDoubleValue(1.5) })
	}
	add(9, func(pmetric.Exemplar) {})
	add(9, func(pmetric.Exemplar) {})
	add(10, func(e pmetric.Exemplar) { e.SetIntValue(8) })
	return md
}

// streamBatches is how many consecutive batches the `stream-NN` fixtures hold.
const streamBatches = 20

// splitTraces cuts the first ResourceSpans/ScopeSpans of td into batches of n spans, each
// under a copy of the same resource and scope.
func splitTraces(td ptrace.Traces, n int) []ptrace.Traces {
	rs := td.ResourceSpans().At(0)
	ss := rs.ScopeSpans().At(0)
	var out []ptrace.Traces
	for lo := 0; lo < ss.Spans().Len(); lo += n {
		b := ptrace.NewTraces()
		brs := b.ResourceSpans().AppendEmpty()
		rs.Resource().CopyTo(brs.Resource())
		brs.SetSchemaUrl(rs.SchemaUrl())
		bss := brs.ScopeSpans().AppendEmpty()
		ss.Scope().CopyTo(bss.Scope())
		bss.SetSchemaUrl(ss.SchemaUrl())
		for i := lo; i < lo+n && i < ss.Spans().Len(); i++ {
			ss.Spans().At(i).CopyTo(bss.Spans().AppendEmpty())
		}
		out = append(out, b)
	}
	return out
}

func splitLogs(ld plog.Logs, n int) []plog.Logs {
	rl := ld.ResourceLogs().At(0)
	sl := rl.ScopeLogs().At(0)
	var out []plog.Logs
	for lo := 0; lo < sl.LogRecords().Len(); lo += n {
		b := plog.NewLogs()
		brl := b.ResourceLogs().AppendEmpty()
		rl.Resource().CopyTo(brl.Resource())
		brl.SetSchemaUrl(rl.SchemaUrl())
		bsl := brl.ScopeLogs().AppendEmpty()
		sl.Scope().CopyTo(bsl.Scope())
		bsl.SetSchemaUrl(sl.SchemaUrl())
		for i := lo; i < lo+n && i < sl.LogRecords().Len(); i++ {
			sl.LogRecords().At(i).CopyTo(bsl.LogRecords().AppendEmpty())
		}
		out = append(out, b)
	}
	return out
}

func splitMetrics(md pmetric.Metrics, n int) []pmetric.Metrics {
	rm := md.ResourceMetrics().At(0)
	sm := rm.ScopeMetrics().At(0)
	var out []pmetric.Metrics
	for lo := 0; lo < sm.Metrics().Len(); lo += n {
		b := pmetric.NewMetrics()
		brm := b.ResourceMetrics().AppendEmpty()
		rm.Resource().CopyTo(brm.Resource())
		brm.SetSchemaUrl(rm.SchemaUrl())
		bsm := brm.ScopeMetrics().AppendEmpty()
		sm.Scope().CopyTo(bsm.Scope())
		bsm.SetSchemaUrl(sm.SchemaUrl())
		for i := lo; i < lo+n && i < sm.Metrics().Len(); i++ {
			sm.Metrics().At(i).CopyTo(bsm.Metrics().AppendEmpty())
		}
		out = append(out, b)
	}
	return out
}
