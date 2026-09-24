package otapcheck

// TestCollector is the end-to-end half of the gate (../e2e.sh): the OTAP batches the Dagr
// bridge produced went over gRPC into the RELEASED contrib otelarrowreceiver, inside an
// ocb-built collector, and each signal's pipeline wrote what it decoded as OTLP JSON, one
// line per batch. e2e.sh wrote, per signal, the fixture each line must equal
// (`e2e-<signal>.txt`, in send order). Skipped unless OTAP_E2E names that directory.

import (
	"bufio"
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/open-telemetry/otel-arrow/go/pkg/otel/assert"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

func collected[T any](t *testing.T, sig string, s signal[T], fromJSON func([]byte) (T, error)) {
	dir := os.Getenv("OTAP_E2E")
	if dir == "" {
		t.Skip("OTAP_E2E not set — run ../e2e.sh")
	}
	manifest, err := os.ReadFile(filepath.Join(dir, "e2e-"+sig+".txt"))
	if err != nil {
		t.Fatal(err)
	}
	names := strings.Fields(string(manifest))
	out, err := os.Open(filepath.Join(dir, "out-otap-"+sig+".json"))
	if err != nil {
		t.Fatal(err)
	}
	defer out.Close()
	sc := bufio.NewScanner(out)
	sc.Buffer(make([]byte, 64<<20), 64<<20)
	i := 0
	for ; sc.Scan(); i++ {
		if i >= len(names) {
			t.Fatalf("the collector wrote more than the %d batches sent", len(names))
		}
		name := names[i]
		got, err := fromJSON(sc.Bytes())
		if err != nil {
			t.Fatalf("line %d (%s): %v", i, name, err)
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
		if s.count(got) != s.count(want) {
			t.Fatalf("line %d (%s): %d items, want %d", i, name, s.count(got), s.count(want))
		}
		gotPB, err := s.marshal(got)
		if err != nil {
			t.Fatal(err)
		}
		if bytes.Equal(gotPB, pb) {
			continue
		}
		base := strings.Split(name, "-")[0] + "-" + strings.Split(name, "-")[1]
		if !unordered[base] {
			t.Errorf("line %d (%s): the collector's batch differs from the fixture", i, name)
		}
		assert.Equiv(assert.NewStdUnitTest(t), []json.Marshaler{s.request(want)}, []json.Marshaler{s.request(got)})
	}
	if err := sc.Err(); err != nil {
		t.Fatal(err)
	}
	if i != len(names) {
		t.Fatalf("the collector wrote %d batches, %d were sent", i, len(names))
	}
	t.Logf("%d %s batches through the collector, each equal to its fixture", i, sig)
}

func TestCollectorTraces(t *testing.T) {
	collected(t, "traces", tracesSignal, (&ptrace.JSONUnmarshaler{}).UnmarshalTraces)
}

func TestCollectorLogs(t *testing.T) {
	collected(t, "logs", logsSignal, (&plog.JSONUnmarshaler{}).UnmarshalLogs)
}

func TestCollectorMetrics(t *testing.T) {
	collected(t, "metrics", metricsSignal, (&pmetric.JSONUnmarshaler{}).UnmarshalMetrics)
}
