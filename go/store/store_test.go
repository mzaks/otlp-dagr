package store_test

import (
	"path/filepath"
	"testing"

	"otlpdagr/internal/fixture"
	"otlpdagr/pdatabridge"
	"otlpdagr/spool"
	"otlpdagr/store"
)

// writeSpool appends one batch per entry in sizes and returns the path.
func writeSpool(t testing.TB, sizes ...int) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "spans.dagr")
	w, err := spool.Create(path, spool.Traces)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	enc := pdatabridge.NewStreamBuilder()
	var dst []byte
	for i, n := range sizes {
		td := fixture.TracesN(n)
		dst = enc.AppendRecord(dst[:0], uint64(1000+i), td)
		if err := w.AppendRecord(dst); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	return path
}

// The crash ring's whole query (spec/38 §6.3): the newest spans, newest first, across
// the batch boundary, with the batch and index saying where each came from.
func TestLastSpansAreNewestFirst(t *testing.T) {
	q, err := store.Open(writeSpool(t, 40, 25, 10))
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	var got []store.Span
	if err := q.LastSpans(14, func(s store.Span) bool {
		got = append(got, s)
		return true
	}); err != nil {
		t.Fatalf("last spans: %v", err)
	}
	if len(got) != 14 {
		t.Fatalf("got %d spans, want 14", len(got))
	}

	// The newest batch holds 10 spans, so the first ten come from it in reverse index
	// order, then the walk crosses into the 25-span batch and continues from ITS end.
	want := []struct{ batch, index int }{
		{0, 9}, {0, 8}, {0, 7}, {0, 6}, {0, 5}, {0, 4}, {0, 3}, {0, 2}, {0, 1}, {0, 0},
		{1, 24}, {1, 23}, {1, 22}, {1, 21},
	}
	for i, w := range want {
		if got[i].Batch != w.batch || got[i].Index != w.index {
			t.Fatalf("span %d came from batch %d index %d, want batch %d index %d",
				i, got[i].Batch, got[i].Index, w.batch, w.index)
		}
	}

	// Within a batch the seed's start timestamps ascend with the index, so reading one
	// field off each accessor confirms the order is the data's, not just the bookkeeping.
	for i := 1; i < 10; i++ {
		prev, _ := got[i-1].Acc.StartTimeUnixNano()
		cur, _ := got[i].Acc.StartTimeUnixNano()
		if cur >= prev {
			t.Fatalf("span %d starts at %d, not before %d — not newest-first", i, cur, prev)
		}
	}
	if got[0].Received != 1002 || got[10].Received != 1001 {
		t.Fatalf("envelope stamps out of order: %d then %d", got[0].Received, got[10].Received)
	}
}

// "No forward pass" has to be measurable, not asserted. A query for a handful of spans
// over a spool of big batches must open ONE batch — the envelope's span_count answers
// how far back to go without parsing anything.
func TestLastSpansOpensOnlyWhatItNeeds(t *testing.T) {
	sizes := make([]int, 20)
	for i := range sizes {
		sizes[i] = 500
	}
	q, err := store.Open(writeSpool(t, sizes...))
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	n := 0
	if err := q.LastSpans(10, func(store.Span) bool { n++; return true }); err != nil {
		t.Fatalf("last spans: %v", err)
	}
	st := q.Stats()
	if n != 10 {
		t.Fatalf("got %d spans, want 10", n)
	}
	if st.BatchesOpened != 1 {
		t.Fatalf("opened %d batches for 10 spans, want 1 (%s)", st.BatchesOpened, st)
	}
	if st.RecordsWalked != 1 {
		t.Fatalf("walked %d records, want 1 (%s)", st.RecordsWalked, st)
	}
	// 10,000 spans in the spool; the query touched 500 and delivered 10.
	if st.SpansBuffered != 500 {
		t.Fatalf("buffered %d spans, want 500 (%s)", st.SpansBuffered, st)
	}

	// Crossing into a second batch opens exactly one more.
	if err := q.LastSpans(501, func(store.Span) bool { return true }); err != nil {
		t.Fatalf("last spans: %v", err)
	}
	if st := q.Stats(); st.BatchesOpened != 2 {
		t.Fatalf("opened %d batches for 501 spans, want 2 (%s)", st.BatchesOpened, st)
	}
}

// Counting spans never opens a batch: the envelope carries the count.
func TestSpanCountReadsEnvelopesOnly(t *testing.T) {
	q, err := store.Open(writeSpool(t, 7, 13, 21))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	spans, batches, err := q.SpanCount()
	if err != nil {
		t.Fatalf("span count: %v", err)
	}
	if spans != 41 || batches != 3 {
		t.Fatalf("got %d spans over %d batches, want 41 over 3", spans, batches)
	}
	if st := q.Stats(); st.BatchesOpened != 0 {
		t.Fatalf("counting opened %d batches, want 0", st.BatchesOpened)
	}
}

// A scan that stops at the first match pays only for the batches it reached.
func TestScanStopsEarly(t *testing.T) {
	q, err := store.Open(writeSpool(t, 100, 100, 100, 100))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	seen := 0
	if err := q.Scan(func(s store.Span) bool {
		seen++
		return seen < 3
	}); err != nil {
		t.Fatalf("scan: %v", err)
	}
	st := q.Stats()
	if seen != 3 {
		t.Fatalf("saw %d spans, want 3", seen)
	}
	if st.BatchesOpened != 1 || st.RecordsWalked != 1 {
		t.Fatalf("stopping after 3 spans cost %s", st)
	}
}

// The point of the whole package: reading a field costs nothing per span, because the
// accessor reads out of the spool's bytes instead of building something.
//
// A scan is not literally zero-allocation — opening the reverse reader costs two, once
// — so the claim is stated the way it is true: allocation is constant in the number of
// spans read. Quadrupling the spool must not move the number.
func TestScanAllocationIsConstantInSpans(t *testing.T) {
	var sink uint64
	measure := func(sizes ...int) float64 {
		q, err := store.Open(writeSpool(t, sizes...))
		if err != nil {
			t.Fatalf("open: %v", err)
		}
		read := func() {
			_ = q.Scan(func(s store.Span) bool {
				if v, ok := s.Acc.EndTimeUnixNano(); ok {
					sink += v
				}
				if st, ok := s.Acc.Status(); ok {
					if c, ok := st.Code(); ok {
						sink += uint64(c)
					}
				}
				return true
			})
		}
		read() // the first walk sizes the scratch; every later one reuses it
		return testing.AllocsPerRun(5, read)
	}

	small := measure(500, 500)
	large := measure(500, 500, 500, 500, 500, 500, 500, 500)
	if small != large {
		t.Fatalf("scanning 1,000 spans allocated %.1f times, 4,000 spans %.1f — "+
			"the per-span cost is not zero", small, large)
	}
	if small > 2 {
		t.Fatalf("a scan allocated %.1f times; only opening the reverse reader should", small)
	}
	if sink == 0 {
		t.Fatal("the scan read nothing")
	}
}

// Retention keeps the newest records and the result is a well-formed spool in both
// directions — a ring that corrupts its own stream while trimming it is worse than one
// that grows.
func TestRingKeepsNewestBatches(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ring.dagr")
	r, rec, err := store.OpenRing(path, store.RingOptions{MaxBatches: 4})
	if err != nil {
		t.Fatalf("open ring: %v", err)
	}
	if !rec.Fresh {
		t.Fatalf("a new ring reported %s", rec)
	}

	enc := pdatabridge.NewStreamBuilder()
	var dst []byte
	for i := 0; i < 12; i++ {
		td := fixture.TracesN(3 + i)
		dst = enc.AppendRecord(dst[:0], uint64(2000+i), td)
		if err := r.Append(dst); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}
	if r.Compactions() == 0 {
		t.Fatal("12 batches into a 4-batch ring compacted nothing")
	}
	// Compaction must not read the prefix it is dropping: the kept set plus the one
	// record that broke the budget, and nothing before it.
	if w := r.LastWalked(); w != 5 {
		t.Fatalf("the last compaction walked %d records, want 5 (4 kept + 1 over)", w)
	}
	if err := r.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	rd, err := spool.Open(path, spool.Traces)
	if err != nil {
		t.Fatalf("open spool: %v", err)
	}
	var stamps []uint64
	if err := rd.Forward(func(b spool.Batch) bool {
		stamps = append(stamps, b.ReceivedUnixNano)
		return true
	}); err != nil {
		t.Fatalf("forward: %v", err)
	}
	want := []uint64{2008, 2009, 2010, 2011}
	if len(stamps) != len(want) {
		t.Fatalf("ring holds %d batches (%v), want %d", len(stamps), stamps, len(want))
	}
	for i := range want {
		if stamps[i] != want[i] {
			t.Fatalf("ring holds %v, want %v", stamps, want)
		}
	}
	if n, err := spool.CountBackward(rd.Bytes(), spool.Traces); err != nil || n != 4 {
		t.Fatalf("reverse chain has %d records (%v), want 4", n, err)
	}
	if tail, err := spool.CheckTail(rd.Bytes(), spool.Traces); err != nil || tail.Torn != 0 {
		t.Fatalf("compaction left a torn spool: %s (%v)", tail, err)
	}

	// And the query surface still works over the compacted file.
	q := store.NewQuery(rd)
	spans, batches, err := q.SpanCount()
	if err != nil {
		t.Fatalf("span count: %v", err)
	}
	if batches != 4 || spans != (3+8)+(3+9)+(3+10)+(3+11) {
		t.Fatalf("compacted ring holds %d spans over %d batches", spans, batches)
	}
}

// The byte budget keeps whole records: the result is the largest set of newest records
// that fits, never a truncated one.
func TestRingKeepsWholeRecordsUnderAByteBudget(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ring.dagr")
	enc := pdatabridge.NewStreamBuilder()
	td50 := fixture.TracesN(50)
	one := enc.AppendRecord(nil, 3000, td50)
	budget := int64(len(one)) * 3

	r, _, err := store.OpenRing(path, store.RingOptions{MaxBytes: budget})
	if err != nil {
		t.Fatalf("open ring: %v", err)
	}
	for i := 0; i < 10; i++ {
		if err := r.Append(enc.AppendRecord(nil, uint64(3000+i), td50)); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}
	if err := r.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	rd, err := spool.Open(path, spool.Traces)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if int64(len(rd.Bytes())) > budget {
		t.Fatalf("ring is %d bytes, over its %d budget", len(rd.Bytes()), budget)
	}
	var newest uint64
	n := 0
	if err := rd.Reverse(func(b spool.Batch) bool {
		if n == 0 {
			newest = b.ReceivedUnixNano
		}
		n++
		return true
	}); err != nil {
		t.Fatalf("reverse: %v", err)
	}
	if newest != 3009 {
		t.Fatalf("newest record is %d, want 3009 — the ring dropped the wrong end", newest)
	}
	if n == 0 {
		t.Fatal("the ring kept nothing")
	}
	if tail, err := spool.CheckTail(rd.Bytes(), spool.Traces); err != nil || tail.Torn != 0 {
		t.Fatalf("byte-budget compaction left a torn spool: %s (%v)", tail, err)
	}
}

// A ring survives a restart: it resumes the existing spool rather than truncating it.
func TestRingResumesAcrossRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ring.dagr")
	enc := pdatabridge.NewStreamBuilder()
	td5 := fixture.TracesN(5)

	for run := 0; run < 3; run++ {
		r, rec, err := store.OpenRing(path, store.RingOptions{MaxBatches: 10})
		if err != nil {
			t.Fatalf("run %d: %v", run, err)
		}
		if run > 0 && rec.Fresh {
			t.Fatalf("run %d treated an existing ring as fresh", run)
		}
		if err := r.Append(enc.AppendRecord(nil, uint64(4000+run), td5)); err != nil {
			t.Fatalf("run %d append: %v", run, err)
		}
		if err := r.Close(); err != nil {
			t.Fatalf("run %d close: %v", run, err)
		}
	}

	q, err := store.Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	spans, batches, err := q.SpanCount()
	if err != nil {
		t.Fatalf("count: %v", err)
	}
	if batches != 3 || spans != 15 {
		t.Fatalf("after three runs the ring holds %d spans over %d batches, want 15 over 3", spans, batches)
	}
}

// Compaction rewrites the file but not the spool's identity: the header it was created
// with — signal, producer, creation time — survives every rewrite.
func TestRingCompactionKeepsTheHeader(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ring.dagr")
	r, _, err := store.OpenRing(path, store.RingOptions{MaxBatches: 2})
	if err != nil {
		t.Fatal(err)
	}
	created, err := spool.ReadHeaderFile(path)
	if err != nil {
		t.Fatal(err)
	}
	enc := pdatabridge.NewStreamBuilder()
	for i := 0; i < 6; i++ {
		if err := r.Append(enc.AppendRecord(nil, uint64(i), fixture.TracesN(2))); err != nil {
			t.Fatal(err)
		}
	}
	r.Close()
	if r.Compactions() == 0 {
		t.Fatal("nothing compacted")
	}
	if got, err := spool.ReadHeaderFile(path); err != nil || got != created {
		t.Fatalf("after compaction the header is %+v (%v), was %+v", got, err, created)
	}
}
