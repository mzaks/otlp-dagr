// Package otapcheck is the OTAP bridge's acceptance gate: every batch the Rust producer
// (rust/otap, `dagr2otap`) wrote from a Dagr fixture is decoded by otel-arrow's OWN Go
// consumer — the code the collector's otelarrowreceiver runs — and must give back the
// OTLP the fixture was built from. Run through ../check.sh, which writes the inputs.
//
// Two comparisons per fixture:
//   - Equiv: otel-arrow's canonical, order-insensitive comparison (its own test oracle);
//   - Exact: the decoded batch marshals to the SAME protobuf bytes as the input, i.e.
//     order survived too. The producer keeps OTLP order everywhere it can; the one place
//     it cannot is a kvlist nested inside a value, which travels as CBOR and comes out of
//     Go's decoder as a Go map. Fixtures with nested kvlists are exempt from Exact.
package otapcheck

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	arrowpb "github.com/open-telemetry/otel-arrow/go/api/experimental/arrow/v1"
	"github.com/open-telemetry/otel-arrow/go/pkg/otel/arrow_record"
	"github.com/open-telemetry/otel-arrow/go/pkg/otel/assert"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/plog/plogotlp"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/pmetric/pmetricotlp"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/pdata/ptrace/ptraceotlp"
	"google.golang.org/protobuf/proto"
)

// unordered lists fixtures whose values hold nested kvlists: their order goes through CBOR
// into a Go map, so it is random run to run (metrics-wide failed Exact 8 times in 20).
// OTAP_DUMP=dir writes both sides as JSON when Exact fails.
var unordered = map[string]bool{"traces-nested": true, "traces-wide": true, "logs-wide": true, "metrics-wide": true}

func work(t *testing.T) string {
	dir := os.Getenv("OTAP_WORK")
	if dir == "" {
		t.Skip("OTAP_WORK not set — run ../check.sh")
	}
	return dir
}

// signal is what differs per signal: decode the fixture's OTLP and the consumer's output,
// count items, and marshal for the two comparisons.
type signal[T any] struct {
	unmarshal func([]byte) (T, error)
	// expected adjusts the fixture to what OTAP can carry to this consumer (nil: as is)
	expected func(T) T
	consume  func(*arrow_record.Consumer, *arrowpb.BatchArrowRecords) ([]T, error)
	count    func(T) int
	request  func(T) json.Marshaler
	marshal  func(T) ([]byte, error)
}

func run[T any](t *testing.T, prefix string, s signal[T]) {
	dir := work(t)
	bars, _ := filepath.Glob(filepath.Join(dir, prefix+"-*.bar"))
	if len(bars) == 0 {
		t.Skipf("no %s-*.bar in %s (signal not generated)", prefix, dir)
	}
	for _, barPath := range bars {
		name := strings.TrimSuffix(filepath.Base(barPath), ".bar")
		t.Run(name, func(t *testing.T) {
			raw, err := os.ReadFile(barPath)
			if err != nil {
				t.Fatal(err)
			}
			var bar arrowpb.BatchArrowRecords
			if err := proto.Unmarshal(raw, &bar); err != nil {
				t.Fatalf("BatchArrowRecords: %v", err)
			}
			pb, err := os.ReadFile(filepath.Join(dir, name+".pb"))
			if err != nil {
				t.Fatal(err)
			}
			want, err := s.unmarshal(pb)
			if err != nil {
				t.Fatal(err)
			}
			if s.expected != nil {
				want = s.expected(want)
				if pb, err = s.marshal(want); err != nil {
					t.Fatal(err)
				}
			}
			c := arrow_record.NewConsumer()
			defer c.Close()
			got, err := s.consume(c, &bar)
			if err != nil {
				t.Fatalf("otel-arrow consumer rejected the batch: %v", err)
			}
			if s.count(want) == 0 {
				if len(got) != 0 && s.count(got[0]) != 0 {
					t.Fatalf("empty input decoded to %d items", s.count(got[0]))
				}
				return
			}
			if len(got) != 1 {
				t.Fatalf("decoded %d batches, want 1", len(got))
			}
			if n, m := s.count(got[0]), s.count(want); n != m {
				t.Fatalf("decoded %d items, want %d", n, m)
			}
			assert.Equiv(assert.NewStdUnitTest(t), []json.Marshaler{s.request(want)}, []json.Marshaler{s.request(got[0])})
			if unordered[name] {
				return
			}
			gotPB, err := s.marshal(got[0])
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(gotPB, pb) {
				if d := os.Getenv("OTAP_DUMP"); d != "" {
					g, _ := json.MarshalIndent(s.request(got[0]), "", " ")
					w, _ := json.MarshalIndent(s.request(want), "", " ")
					_ = os.WriteFile(filepath.Join(d, name+".got.json"), g, 0o644)
					_ = os.WriteFile(filepath.Join(d, name+".want.json"), w, 0o644)
				}
				t.Errorf("Equiv holds but order did not survive: %d proto bytes decoded vs %d in", len(gotPB), len(pb))
			}
		})
	}
}

// runStream decodes the `<prefix>-stream-NN.sbar` batches — written by ONE producer, so
// later ones reuse earlier schemas and dictionaries — with ONE consumer, in order, and
// checks each against its fixture.
func runStream[T any](t *testing.T, prefix string, s signal[T]) {
	runSeq(t, prefix+"-stream-*.sbar", func(p string) string { return strings.TrimSuffix(filepath.Base(p), ".sbar") }, s)
}

// runSpool checks the batches the bridge made from a SPOOL (`<prefix>-spool-NN.spbar`,
// one per record) against the same stream fixtures the spool was written from.
func runSpool[T any](t *testing.T, prefix string, s signal[T]) {
	runSeq(t, prefix+"-spool-*.spbar", func(p string) string {
		return strings.Replace(strings.TrimSuffix(filepath.Base(p), ".spbar"), "-spool-", "-stream-", 1)
	}, s)
}

// runSeq decodes the files matching glob, in name order, with ONE consumer; fixture maps
// a file to the name of the fixture it must equal.
func runSeq[T any](t *testing.T, glob string, fixture func(string) string, s signal[T]) {
	dir := work(t)
	sbars, _ := filepath.Glob(filepath.Join(dir, glob))
	if len(sbars) == 0 {
		t.Skipf("no %s in %s", glob, dir)
	}
	sort.Strings(sbars)
	c := arrow_record.NewConsumer()
	defer c.Close()
	for _, p := range sbars {
		name := fixture(p)
		raw, err := os.ReadFile(p)
		if err != nil {
			t.Fatal(err)
		}
		var bar arrowpb.BatchArrowRecords
		if err := proto.Unmarshal(raw, &bar); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		pb, err := os.ReadFile(filepath.Join(dir, name+".pb"))
		if err != nil {
			t.Fatal(err)
		}
		want, err := s.unmarshal(pb)
		if err != nil {
			t.Fatal(err)
		}
		if s.expected != nil {
			want = s.expected(want)
			if pb, err = s.marshal(want); err != nil {
				t.Fatal(err)
			}
		}
		got, err := s.consume(c, &bar)
		if err != nil {
			t.Fatalf("%s: the consumer rejected the stream's batch: %v", name, err)
		}
		if len(got) != 1 {
			t.Fatalf("%s: decoded %d batches, want 1", name, len(got))
		}
		gotPB, err := s.marshal(got[0])
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(gotPB, pb) {
			t.Errorf("%s: decoded batch differs from its fixture (%d proto bytes vs %d)", name, len(gotPB), len(pb))
			assert.Equiv(assert.NewStdUnitTest(t), []json.Marshaler{s.request(want)}, []json.Marshaler{s.request(got[0])})
			return
		}
	}
}

func TestTracesStream(t *testing.T)  { runStream(t, "traces", tracesSignal) }
func TestLogsStream(t *testing.T)    { runStream(t, "logs", logsSignal) }
func TestMetricsStream(t *testing.T) { runStream(t, "metrics", metricsSignal) }
func TestTracesSpool(t *testing.T)   { runSpool(t, "traces", tracesSignal) }
func TestLogsSpool(t *testing.T)     { runSpool(t, "logs", logsSignal) }
func TestMetricsSpool(t *testing.T)  { runSpool(t, "metrics", metricsSignal) }

var tracesSignal = signal[ptrace.Traces]{
	unmarshal: (&ptrace.ProtoUnmarshaler{}).UnmarshalTraces,
	consume:   (*arrow_record.Consumer).TracesFrom,
	count:     ptrace.Traces.SpanCount,
	request:   func(td ptrace.Traces) json.Marshaler { return ptraceotlp.NewExportRequestFromTraces(td) },
	marshal:   (&ptrace.ProtoMarshaler{}).MarshalTraces,
}

func TestTraces(t *testing.T) { run(t, "traces", tracesSignal) }

var logsSignal = signal[plog.Logs]{
	unmarshal: (&plog.ProtoUnmarshaler{}).UnmarshalLogs,
	consume:   (*arrow_record.Consumer).LogsFrom,
	count:     plog.Logs.LogRecordCount,
	request:   func(ld plog.Logs) json.Marshaler { return plogotlp.NewExportRequestFromLogs(ld) },
	marshal:   (&plog.ProtoMarshaler{}).MarshalLogs,
}

func TestLogs(t *testing.T) { run(t, "logs", logsSignal) }

var metricsSignal = signal[pmetric.Metrics]{
	unmarshal: (&pmetric.ProtoUnmarshaler{}).UnmarshalMetrics,
	// metric metadata is not sent to a Go receiver (it has no METRIC_ATTRS case)
	expected: func(md pmetric.Metrics) pmetric.Metrics {
		rms := md.ResourceMetrics()
		for i := 0; i < rms.Len(); i++ {
			sms := rms.At(i).ScopeMetrics()
			for j := 0; j < sms.Len(); j++ {
				ms := sms.At(j).Metrics()
				for k := 0; k < ms.Len(); k++ {
					ms.At(k).Metadata().Clear()
				}
			}
		}
		return md
	},
	consume: (*arrow_record.Consumer).MetricsFrom,
	count:   pmetric.Metrics.DataPointCount,
	request: func(md pmetric.Metrics) json.Marshaler { return pmetricotlp.NewExportRequestFromMetrics(md) },
	marshal: (&pmetric.ProtoMarshaler{}).MarshalMetrics,
}

func TestMetrics(t *testing.T) { run(t, "metrics", metricsSignal) }
