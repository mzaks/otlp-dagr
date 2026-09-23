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
	"go.opentelemetry.io/collector/pdata/ptrace"

	"otlpdagr/internal/fixture"
	"otlpdagr/logbridge"
	"otlpdagr/metricbridge"
	"otlpdagr/pdatabridge"
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
	for name, td := range traces {
		pb, err := (&ptrace.ProtoMarshaler{}).MarshalTraces(td)
		if err := write("traces-"+name, pdatabridge.ToBytes(td), pb, err); err != nil {
			return err
		}
	}
	logs := map[string]plog.Logs{"wide": fixture.LogsWide(), "batch": fixture.LogsBatch(500)}
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
	for name, md := range metrics {
		pb, err := (&pmetric.ProtoMarshaler{}).MarshalMetrics(md)
		if err := write("metrics-"+name, metricbridge.ToBytes(md), pb, err); err != nil {
			return err
		}
	}
	return nil
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
