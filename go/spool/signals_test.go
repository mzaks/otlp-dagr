package spool_test

import (
	"bytes"
	"iter"
	"os"
	"path/filepath"
	"testing"

	"otlpdagr/internal/fixture"
	"otlpdagr/logbridge"
	"otlpdagr/metricbridge"
	"otlpdagr/pdatabridge"
	"otlpdagr/profilebridge"
	"otlpdagr/spool"
	"otlpgen/dagr"
	"otlpgen/logrecordlog"
	"otlpgen/metriclog"
	"otlpgen/otlplogs"
	"otlpgen/otlpmetrics"
	"otlpgen/otlpprofiles"
	"otlpgen/otlptraces"
	"otlpgen/profilelog"
	"otlpgen/spanlog"
)

// The WAL guarantees hold for every signal, not just the one they were written against.
// The four sinks share their framing, so these gates exercise the shared tail logic
// through each generated sink package, on each signal's WIDE batch (every field set).

type signalCase struct {
	sig   spool.Signal
	count uint32
	// append writes the wide batch as one record, stamped ts, with the given count
	append func(w *spool.Writer, ts uint64, count uint32) error
	// items decodes a record back to pdata and counts its items
	items func(record []byte) int
	// graph is the same batch as a standalone graph buffer (the direct builder's output)
	// and inline lists, per record, the node spans the record and that graph share
	graph  []byte
	inline func(record, graph []byte) (rec, gr [][]byte)
}

func signalCases(t *testing.T) []signalCase {
	t.Helper()
	td, md, ld, pd := fixture.TracesWide(), fixture.MetricsWide(), fixture.LogsWide(), fixture.ProfilesWide()
	pv := profilebridge.ToValue(pd)
	return []signalCase{
		{spool.Traces, uint32(td.SpanCount()),
			func(w *spool.Writer, ts uint64, n uint32) error {
				return w.AppendTraces(spanlog.Batch{ReceivedUnixNano: ts, SpanCount: n,
					ResourceSpans: pdatabridge.ToValue(td).ResourceSpans.V})
			},
			func(r []byte) int { return pdatabridge.FromRecord(r).SpanCount() },
			pdatabridge.ToBytes(td),
			func(r, g []byte) ([][]byte, [][]byte) {
				root, _ := otlptraces.OpenOtlpTraces(g)
				return nodes(spanlog.NewBatchAccessor(r, 0).ResourceSpansAll()), nodes(root.ResourceSpansAll())
			}},
		{spool.Metrics, uint32(md.DataPointCount()),
			func(w *spool.Writer, ts uint64, n uint32) error {
				return w.AppendMetrics(metriclog.Batch{ReceivedUnixNano: ts, DataPointCount: n,
					ResourceMetrics: metricbridge.ToValue(md).ResourceMetrics.V})
			},
			func(r []byte) int { return metricbridge.FromRecord(r).DataPointCount() },
			metricbridge.ToBytes(md),
			func(r, g []byte) ([][]byte, [][]byte) {
				root, _ := otlpmetrics.OpenOtlpMetrics(g)
				return nodes(metriclog.NewBatchAccessor(r, 0).ResourceMetricsAll()), nodes(root.ResourceMetricsAll())
			}},
		{spool.Logs, uint32(ld.LogRecordCount()),
			func(w *spool.Writer, ts uint64, n uint32) error {
				return w.AppendLogs(logrecordlog.Batch{ReceivedUnixNano: ts, LogRecordCount: n,
					ResourceLogs: logbridge.ToValue(ld).ResourceLogs.V})
			},
			func(r []byte) int { return logbridge.FromRecord(r).LogRecordCount() },
			logbridge.ToBytes(ld),
			func(r, g []byte) ([][]byte, [][]byte) {
				root, _ := otlplogs.OpenOtlpLogs(g)
				return nodes(logrecordlog.NewBatchAccessor(r, 0).ResourceLogsAll()), nodes(root.ResourceLogsAll())
			}},
		{spool.Profiles, uint32(pd.SampleCount()),
			func(w *spool.Writer, ts uint64, n uint32) error {
				return w.AppendProfiles(profilelog.Batch{ReceivedUnixNano: ts, SampleCount: n,
					ResourceProfiles: pv.ResourceProfiles.V, Dictionary: pv.Dictionary})
			},
			func(r []byte) int { return profilebridge.FromRecord(r).SampleCount() },
			profilebridge.ToBytes(pd),
			func(r, g []byte) ([][]byte, [][]byte) {
				root, _ := otlpprofiles.OpenOtlpProfiles(g)
				a := profilelog.NewBatchAccessor(r, 0)
				rec, gr := nodes(a.ResourceProfilesAll()), nodes(root.ResourceProfilesAll())
				rd, _ := a.Dictionary()
				gd, _ := root.Dictionary()
				return append(rec, nodeBytes(rd)), append(gr, nodeBytes(gd))
			}},
	}
}

// nodeBytes is one packed node's bytes: its size LEB and the body it sizes.
func nodeBytes(a interface {
	BufferBytes() []byte
	BufferPos() int
}) []byte {
	buf, pos := a.BufferBytes(), a.BufferPos()
	n, nb := dagr.ReadLEB(buf, pos)
	return buf[pos : pos+nb+int(n)]
}

func nodes[A interface {
	BufferBytes() []byte
	BufferPos() int
}](all iter.Seq[A]) [][]byte {
	var out [][]byte
	for a := range all {
		out = append(out, nodeBytes(a))
	}
	return out
}

func writeN(t *testing.T, path string, c signalCase, n int) {
	t.Helper()
	w, err := spool.Create(path, c.sig)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < n; i++ {
		if err := c.append(w, uint64(100+i), c.count+uint32(i)); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
}

// The inline batch is byte-for-byte what the signal's graph writes (Dagr spec/11 §12):
// every node a record holds — each top-level resource entry, and profiles' dictionary —
// equals the same node in the direct builder's graph buffer. Metrics carries the
// frozen+packed Buckets, so this also gates that a sink keeps an imported node's layout.
func TestRecordNodesAreTheGraphsBytes(t *testing.T) {
	for _, c := range signalCases(t) {
		t.Run(c.sig.String(), func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "spool.dagr")
			writeN(t, path, c, 1)
			r, err := spool.Open(path, c.sig)
			if err != nil {
				t.Fatal(err)
			}
			last, err := r.Last(1)
			if err != nil || len(last) != 1 {
				t.Fatalf("last: %v", err)
			}
			rec, gr := c.inline(last[0].Record, c.graph)
			if len(rec) == 0 || len(rec) != len(gr) {
				t.Fatalf("record holds %d nodes, the graph %d", len(rec), len(gr))
			}
			for i := range rec {
				if !bytes.Equal(rec[i], gr[i]) {
					t.Fatalf("node %d: %d bytes in the record, %d in the graph — not the graph's bytes",
						i, len(rec[i]), len(gr[i]))
				}
			}
		})
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
			// oldest-first, and every record decodes back to the whole batch
			var fwd []uint32
			if err := r.Forward(func(b spool.Batch) bool {
				if got := c.items(b.Record); got != int(c.count) {
					t.Fatalf("record decodes to %d items, the batch has %d", got, c.count)
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
			if err := os.WriteFile(path, whole[:lastStart+int64(len(last[0].Record)/2)], 0o644); err != nil {
				t.Fatal(err)
			}
			w, rec, err := spool.Resume(path, c.sig)
			if err != nil || rec.Torn == 0 {
				t.Fatalf("resume: %s (%v)", rec, err)
			}
			if err := c.append(w, 102, c.count+2); err != nil {
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
