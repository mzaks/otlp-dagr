package spool_test

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"otlpdagr/internal/fixture"
	"otlpdagr/pdatabridge"
	"otlpdagr/spool"
)

// encode is the exporter's encode step: one StreamBuilder and one dst, reused.
func encode(t *testing.T, n int) ([]byte, uint32) {
	t.Helper()
	td := fixture.TracesN(n)
	b := pdatabridge.NewStreamBuilder()
	return b.BuildAppend(nil, td), uint32(td.SpanCount())
}

func writeSpool(t *testing.T, path string, sizes ...int) [][]byte {
	t.Helper()
	w, err := spool.Create(path)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	var want [][]byte
	for i, n := range sizes {
		buf, count := encode(t, n)
		want = append(want, buf)
		if err := w.Append(uint64(1000+i), count, buf); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	return want
}

// Gate 1 (spec/38 §6.1.1): what the spool stores is what the direct builder produced,
// byte for byte. The record is a `raw` embedded graph, so the exporter frames and
// appends — it never re-encodes — and the receiver gets the original buffer back.
func TestRecordIsTheEncoderOutputVerbatim(t *testing.T) {
	path := filepath.Join(t.TempDir(), "spans.dagr")
	want := writeSpool(t, path, 50, 7, 120)

	r, err := spool.Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	var got [][]byte
	if err := r.Forward(func(b spool.Batch) bool {
		got = append(got, b.Traces)
		return true
	}); err != nil {
		t.Fatalf("forward: %v", err)
	}
	if len(got) != len(want) {
		t.Fatalf("read %d records, wrote %d", len(got), len(want))
	}
	for i := range want {
		if !bytes.Equal(got[i], want[i]) {
			t.Fatalf("record %d: %d bytes stored, %d encoded — not verbatim", i, len(got[i]), len(want[i]))
		}
	}
	// And the blob is a view INTO the spool, not a copy of it.
	if len(got) > 0 {
		blob := got[0]
		spoolBytes := r.Bytes()
		if cap(blob) == 0 || &blob[0] != &spoolBytes[recordOffset(t, spoolBytes, blob)] {
			t.Fatal("embedded batch was copied out of the spool")
		}
	}
}

// recordOffset locates blob inside buf by identity of its backing array.
func recordOffset(t *testing.T, buf, blob []byte) int {
	t.Helper()
	for i := 0; i+len(blob) <= len(buf); i++ {
		if &buf[i] == &blob[0] {
			return i
		}
	}
	t.Fatal("blob does not alias the spool")
	return 0
}

// The embedded leaf is a standalone Dagr buffer: it reads through OtlpTraces' own
// readers, which is what makes a Dagr spool self-describing rather than an opaque frame.
func TestEmbeddedBatchReadsAsAGraph(t *testing.T) {
	path := filepath.Join(t.TempDir(), "spans.dagr")
	writeSpool(t, path, 40)

	r, err := spool.Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	last, err := r.Last(1)
	if err != nil || len(last) != 1 {
		t.Fatalf("last: %v (%d records)", err, len(last))
	}
	td, err := pdatabridge.ToTraces(last[0].Traces)
	if err != nil {
		t.Fatalf("to traces: %v", err)
	}
	if got := td.SpanCount(); got != 40 {
		t.Fatalf("span count %d, want 40", got)
	}
	if got := last[0].SpanCount; got != 40 {
		t.Fatalf("envelope span count %d, want 40", got)
	}
}

// Gate 3: the reverse reader yields the newest N batches with no forward pass, and the
// counts match. The envelope's span_count is read straight off the record — no embedded
// batch is opened — which is the whole reason the envelope exists.
func TestReverseReaderYieldsNewestFirst(t *testing.T) {
	path := filepath.Join(t.TempDir(), "spans.dagr")
	sizes := []int{5, 11, 3, 29, 7, 13}
	writeSpool(t, path, sizes...)

	r, err := spool.Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	last3, err := r.Last(3)
	if err != nil {
		t.Fatalf("last: %v", err)
	}
	wantNewest := []uint32{13, 7, 29}
	if len(last3) != 3 {
		t.Fatalf("got %d batches, want 3", len(last3))
	}
	for i, w := range wantNewest {
		if last3[i].SpanCount != w {
			t.Fatalf("newest[%d] = %d spans, want %d", i, last3[i].SpanCount, w)
		}
		if last3[i].ReceivedUnixNano != uint64(1000+len(sizes)-1-i) {
			t.Fatalf("newest[%d] timestamp %d out of order", i, last3[i].ReceivedUnixNano)
		}
	}

	// The reverse walk visits exactly the records the forward walk does.
	n, err := spool.CountBackward(r.Bytes())
	if err != nil {
		t.Fatalf("count backward: %v", err)
	}
	if n != len(sizes) {
		t.Fatalf("reverse count %d, want %d", n, len(sizes))
	}

	// "The last 40 spans before the error" — §6.3's crash ring, without an index.
	got, total, err := r.LastSpans(40)
	if err != nil {
		t.Fatalf("last spans: %v", err)
	}
	if total < 40 {
		t.Fatalf("collected %d spans, want >= 40", total)
	}
	if want := 13 + 7 + 29; total != want || len(got) != 3 {
		t.Fatalf("collected %d spans over %d batches, want %d over 3", total, len(got), want)
	}
}

// Gate 4: a tail torn by a kill mid-write is DETECTED, not silently accepted.
//
// The forward reader alone would quietly stop at the damage and report a shorter stream,
// which for a WAL is the dangerous answer. CheckTail cross-checks the trailing RLEB
// against the record's own forward framing, so the damage is named and measured.
func TestTornTailIsDetected(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "spans.dagr")
	sizes := []int{9, 4, 17}
	writeSpool(t, path, sizes...)

	whole, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	rec, err := spool.CheckTail(whole)
	if err != nil {
		t.Fatalf("check intact: %v", err)
	}
	if rec.Torn != 0 || rec.ValidEnd != int64(len(whole)) {
		t.Fatalf("intact spool reported as torn: %s", rec)
	}
	if !rec.Backward {
		t.Fatal("an intact tail must be settled by the reverse walk alone")
	}

	// Every prefix that cuts into the last record must be reported torn, and must
	// recover to exactly the same point: the end of the second record.
	full, err := spool.CountBackward(whole)
	if err != nil || full != len(sizes) {
		t.Fatalf("count %d (%v), want %d", full, err, len(sizes))
	}
	r, err := spool.OpenBytes(whole)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	last, err := r.Last(1)
	if err != nil || len(last) != 1 {
		t.Fatalf("last: %v", err)
	}
	lastStart := int64(len(whole)) - int64(last[0].Range.End-last[0].Range.Start)

	for cut := int64(len(whole)) - 1; cut > lastStart; cut-- {
		torn := whole[:cut]
		got, err := spool.CheckTail(torn)
		if err != nil {
			t.Fatalf("check tail at cut %d: %v", cut, err)
		}
		if got.Torn == 0 {
			t.Fatalf("cut at %d (%d bytes into the last record) accepted as intact",
				cut, cut-lastStart)
		}
		if got.ValidEnd != lastStart {
			t.Fatalf("cut at %d recovered to %d, want %d", cut, got.ValidEnd, lastStart)
		}
	}
}

// Recovery truncates the damage away and Resume appends to a single well-formed stream —
// the restart path a WAL is for.
func TestResumeAfterCrashKeepsOneStream(t *testing.T) {
	path := filepath.Join(t.TempDir(), "spans.dagr")
	writeSpool(t, path, 6, 8)

	whole, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	// Simulate a kill in the middle of writing a third record.
	partial, count := encode(t, 12)
	torn := append(append([]byte(nil), whole...), partial[:len(partial)/2]...)
	if err := os.WriteFile(path, torn, 0o644); err != nil {
		t.Fatalf("write torn: %v", err)
	}

	w, rec, err := spool.Resume(path)
	if err != nil {
		t.Fatalf("resume: %v", err)
	}
	if rec.Torn == 0 {
		t.Fatalf("resume accepted a torn tail: %s", rec)
	}
	if rec.ValidEnd != int64(len(whole)) {
		t.Fatalf("recovered to %d, want %d", rec.ValidEnd, len(whole))
	}
	if err := w.Append(1002, count, partial); err != nil {
		t.Fatalf("append after resume: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	r, err := spool.Open(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	var counts []uint32
	if err := r.Forward(func(b spool.Batch) bool {
		counts = append(counts, b.SpanCount)
		return true
	}); err != nil {
		t.Fatalf("forward: %v", err)
	}
	if want := []uint32{6, 8, 12}; len(counts) != 3 || counts[0] != want[0] || counts[1] != want[1] || counts[2] != want[2] {
		t.Fatalf("after resume the spool holds %v, want %v", counts, want)
	}
	n, err := spool.CountBackward(r.Bytes())
	if err != nil || n != 3 {
		t.Fatalf("reverse count %d (%v), want 3", n, err)
	}
	// A record appended after a resume must be byte-identical to the same record written
	// by a writer that framed the spool itself — otherwise "resume" quietly forks the
	// format at every restart.
	fresh := filepath.Join(t.TempDir(), "fresh.dagr")
	writeSpool(t, fresh, 6, 8, 12)
	freshBytes, err := os.ReadFile(fresh)
	if err != nil {
		t.Fatalf("read fresh: %v", err)
	}
	if !bytes.Equal(freshBytes, r.Bytes()) {
		t.Fatalf("resumed spool is %d bytes, a freshly written one %d — the formats differ",
			len(r.Bytes()), len(freshBytes))
	}
}

// An empty spool is a valid stream, not an empty file: a reader attached before the
// first batch sees a framed sink.
func TestEmptySpoolIsValid(t *testing.T) {
	path := filepath.Join(t.TempDir(), "empty.dagr")
	w, err := spool.Create(path)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	r, err := spool.Open(path)
	if err != nil {
		t.Fatalf("open empty: %v", err)
	}
	n := 0
	if err := r.Forward(func(spool.Batch) bool { n++; return true }); err != nil {
		t.Fatalf("forward: %v", err)
	}
	if n != 0 {
		t.Fatalf("empty spool yielded %d records", n)
	}
	rec, err := spool.CheckTail(r.Bytes())
	if err != nil || rec.Torn != 0 {
		t.Fatalf("empty spool tail: %s (%v)", rec, err)
	}
}
