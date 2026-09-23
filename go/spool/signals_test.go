package spool_test

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"otlpdagr/internal/fixture"
	"otlpdagr/logbridge"
	"otlpdagr/metricbridge"
	"otlpdagr/pdatabridge"
	"otlpdagr/profilebridge"
	"otlpdagr/spool"
)

// The WAL guarantees hold for every signal, not just the one they were written against.
// The four sinks share their framing, so these gates exercise the shared tail logic
// through each generated sink package, on each signal's WIDE batch (every field set).

type signalCase struct {
	sig     spool.Signal
	payload []byte
	count   uint32
	decode  func([]byte) error // the bridge back to pdata: the payload must still be a graph
}

func signalCases(t *testing.T) []signalCase {
	t.Helper()
	td, md, ld, pd := fixture.TracesWide(), fixture.MetricsWide(), fixture.LogsWide(), fixture.ProfilesWide()
	return []signalCase{
		{spool.Traces, pdatabridge.ToBytes(td), uint32(td.SpanCount()),
			func(b []byte) error { _, err := pdatabridge.ToTraces(b); return err }},
		{spool.Metrics, metricbridge.ToBytes(md), uint32(md.DataPointCount()),
			func(b []byte) error { _, err := metricbridge.ToMetrics(b); return err }},
		{spool.Logs, logbridge.ToBytes(ld), uint32(ld.LogRecordCount()),
			func(b []byte) error { _, err := logbridge.ToLogs(b); return err }},
		{spool.Profiles, profilebridge.ToBytes(pd), uint32(pd.SampleCount()),
			func(b []byte) error { _, err := profilebridge.ToProfiles(b); return err }},
	}
}

func writeN(t *testing.T, path string, c signalCase, n int) {
	t.Helper()
	w, err := spool.Create(path, c.sig)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < n; i++ {
		if err := w.Append(uint64(100+i), c.count+uint32(i), c.payload); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestEverySignalSpool(t *testing.T) {
	for _, c := range signalCases(t) {
		t.Run(c.sig.String(), func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, "spool.dagr")
			writeN(t, path, c, 3)

			r, err := spool.Open(path, c.sig)
			if err != nil {
				t.Fatal(err)
			}
			// verbatim, oldest-first, and still a graph the bridge reads
			var fwd []uint32
			if err := r.Forward(func(b spool.Batch) bool {
				if !bytes.Equal(b.Payload, c.payload) {
					t.Fatalf("record payload is not the encoder output")
				}
				if err := c.decode(b.Payload); err != nil {
					t.Fatalf("embedded payload does not decode: %v", err)
				}
				fwd = append(fwd, b.Count)
				return true
			}); err != nil {
				t.Fatal(err)
			}
			last, err := r.Last(3)
			if err != nil || len(last) != 3 || last[0].Count != c.count+2 || last[2].Count != c.count {
				t.Fatalf("reverse order wrong: %v (%v)", last, err)
			}
			if len(fwd) != 3 || fwd[0] != c.count {
				t.Fatalf("forward order wrong: %v", fwd)
			}

			// every cut into the last record is torn, and recovers to its start
			whole := r.Bytes()
			lastStart := int64(len(whole)) - int64(last[0].Range.End-last[0].Range.Start)
			for cut := int64(len(whole)) - 1; cut > lastStart; cut-- {
				got, err := spool.CheckTail(whole[:cut], c.sig)
				if err != nil {
					t.Fatal(err)
				}
				if got.Torn == 0 || got.ValidEnd != lastStart {
					t.Fatalf("cut at %d: %s, want torn back to %d", cut, got, lastStart)
				}
			}

			// crash mid-record, resume, append: identical to a spool written in one go
			if err := os.WriteFile(path, whole[:lastStart+int64(len(c.payload)/2)], 0o644); err != nil {
				t.Fatal(err)
			}
			w, rec, err := spool.Resume(path, c.sig)
			if err != nil || rec.Torn == 0 {
				t.Fatalf("resume: %s (%v)", rec, err)
			}
			if err := w.Append(102, c.count+2, c.payload); err != nil {
				t.Fatal(err)
			}
			w.Close()
			resumed, _ := os.ReadFile(path)
			if !bytes.Equal(resumed, whole) {
				t.Fatalf("resumed spool (%d B) differs from the one written in one run (%d B)",
					len(resumed), len(whole))
			}
		})
	}
}
