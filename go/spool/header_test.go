package spool_test

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"otlpdagr/spool"
	"otlpgen/dagr"
	"otlpgen/logrecordlog"
	"otlpgen/metriclog"
	"otlpgen/profilelog"
	"otlpgen/spanlog"
)

// The header is what makes a spool say which signal it holds (spec/38 D2). These gates
// hold it to that on every signal: it round-trips, it is identical across the four
// generated sinks, a spool refuses to be read or resumed as another signal, and damage
// or absence is an error, never a panic.

func TestHeaderRoundTripsOnEverySignal(t *testing.T) {
	for _, c := range signalCases(t) {
		t.Run(c.sig.String(), func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "s.dagr")
			want := spool.NewHeader(c.sig, "collector-7/dagrfile")
			w, err := spool.CreateWith(path, want)
			if err != nil {
				t.Fatal(err)
			}
			if err := c.append(w, 1, c.count); err != nil {
				t.Fatal(err)
			}
			w.Close()

			got, err := spool.ReadHeaderFile(path)
			if err != nil || got != want {
				t.Fatalf("ReadHeaderFile = %+v, %v; want %+v", got, err, want)
			}
			if got.SignalName != c.sig.GraphName() || got.FormatVersion != spool.FormatVersion ||
				got.OTLPProtoVersion != spool.OTLPProtoVersion {
				t.Fatalf("header fields: %+v", got)
			}
			r, err := spool.OpenAny(path)
			if err != nil {
				t.Fatal(err)
			}
			if r.Signal() != c.sig || r.Header() != want {
				t.Fatalf("OpenAny: signal %s header %+v", r.Signal(), r.Header())
			}
			n := 0
			if err := r.Forward(func(spool.Batch) bool { n++; return true }); err != nil || n != 1 {
				t.Fatalf("forward after the header: %d batches, %v", n, err)
			}
		})
	}
}

// The header's bytes do not depend on which generated sink wrote them — that is what
// lets ReadHeader decode any spool with one package before it knows the signal.
func TestHeaderBytesAreTheSameInEverySink(t *testing.T) {
	h := spanlog.SpoolHeader{Signal: 2, SignalName: "OtlpLogs", CreatedUnixNano: 1_758_000_000_123_456_789,
		FormatVersion: 1, OtlpProtoVersion: dagr.Some("v"), Producer: dagr.Some("p")}
	want := spanlog.NewSpanLogWriter(h).Bytes()
	for name, got := range map[string][]byte{
		"metrics":  metriclog.NewMetricLogWriter(metriclog.SpoolHeader(h)).Bytes(),
		"logs":     logrecordlog.NewLogRecordLogWriter(logrecordlog.SpoolHeader(h)).Bytes(),
		"profiles": profilelog.NewProfileLogWriter(profilelog.SpoolHeader(h)).Bytes(),
	} {
		if string(got) != string(want) {
			t.Fatalf("%s sink writes % x, traces sink % x", name, got, want)
		}
	}
	got, _, err := spool.ReadHeader(want)
	if err != nil || got.Signal != spool.Logs || got.Producer != "p" || got.CreatedUnixNano != h.CreatedUnixNano {
		t.Fatalf("ReadHeader = %+v, %v", got, err)
	}
}

func TestSpoolRefusesAnotherSignal(t *testing.T) {
	path := filepath.Join(t.TempDir(), "m.dagr")
	cs := signalCases(t)
	writeN(t, path, cs[1], 2) // metrics
	before, _ := os.ReadFile(path)

	if _, err := spool.Open(path, spool.Traces); err == nil || !strings.Contains(err.Error(), "holds metrics") {
		t.Fatalf("Open as traces: %v", err)
	}
	if _, _, err := spool.Resume(path, spool.Logs); err == nil || !strings.Contains(err.Error(), "holds metrics") {
		t.Fatalf("Resume as logs: %v", err)
	}
	after, _ := os.ReadFile(path)
	if string(before) != string(after) {
		t.Fatal("a refused Resume touched the spool")
	}
	if _, err := spool.Open(path, spool.Metrics); err != nil {
		t.Fatalf("Open as metrics: %v", err)
	}
}

func TestHeaderFromTheFutureIsRefused(t *testing.T) {
	h := spool.NewHeader(spool.Traces, "")
	h.FormatVersion = spool.FormatVersion + 1
	if _, err := spool.CreateWith(filepath.Join(t.TempDir(), "x"), h); err == nil {
		t.Fatal("CreateWith accepted a header this build cannot read")
	}
	h = spool.NewHeader(spool.Traces, "")
	h.SignalName = "OtlpMetrics"
	if err := h.Check(spool.Traces); err == nil {
		t.Fatal("a header whose code and name disagree passed")
	}
}

// A spool written before the header existed fails loudly, with a named error.
func TestHeaderlessSpoolIsNamed(t *testing.T) {
	path := filepath.Join(t.TempDir(), "old.dagr")
	os.WriteFile(path, []byte{2}, 0o644) // the old framing word: a DataSink, no header
	if _, err := spool.Open(path, spool.Traces); !errors.Is(err, spool.ErrNoHeader) {
		t.Fatalf("Open: %v", err)
	}
	if _, _, err := spool.Resume(path, spool.Traces); !errors.Is(err, spool.ErrNoHeader) {
		t.Fatalf("Resume: %v", err)
	}
}

// A Create killed while writing the header leaves a prefix of it. Every prefix is an
// error — from the in-memory reader and the file reader alike — and none panics.
func TestTruncatedHeaderIsAnError(t *testing.T) {
	path := filepath.Join(t.TempDir(), "s.dagr")
	w, err := spool.CreateWith(path, spool.NewHeader(spool.Profiles, "a producer name"))
	if err != nil {
		t.Fatal(err)
	}
	w.Close()
	whole, _ := os.ReadFile(path)
	cut := filepath.Join(t.TempDir(), "cut.dagr")
	for n := 1; n < len(whole); n++ {
		if _, _, err := spool.ReadHeader(whole[:n]); err == nil {
			t.Fatalf("ReadHeader accepted %d of %d header bytes", n, len(whole))
		}
		os.WriteFile(cut, whole[:n], 0o644)
		if _, err := spool.ReadHeaderFile(cut); err == nil {
			t.Fatalf("ReadHeaderFile accepted %d of %d header bytes", n, len(whole))
		}
	}
}

// ...and Resume starts such a spool over instead of refusing it forever.
func TestResumeRecreatesATornHeader(t *testing.T) {
	path := filepath.Join(t.TempDir(), "s.dagr")
	w, _ := spool.CreateWith(path, spool.NewHeader(spool.Logs, "p"))
	w.Close()
	whole, _ := os.ReadFile(path)
	os.WriteFile(path, whole[:len(whole)/2], 0o644)

	w, rec, err := spool.Resume(path, spool.Logs)
	if err != nil {
		t.Fatal(err)
	}
	w.Close()
	if !rec.Fresh || rec.Torn != int64(len(whole)/2) {
		t.Fatalf("recovery %+v", rec)
	}
	if _, err := spool.Open(path, spool.Logs); err != nil {
		t.Fatalf("recreated spool: %v", err)
	}
}
