package store_test

import (
	"path/filepath"
	"testing"

	"otlpdagr/internal/fixture"
	"otlpdagr/pdatabridge"
	"otlpdagr/spool"
	"otlpdagr/store"
)

// writeSampling spools a tail-sampling corpus, one record per batch.
func writeSampling(t testing.TB, nTraces, nBatches int) (string, *fixture.Sampling) {
	t.Helper()
	c := fixture.NewSampling(nTraces, nBatches)
	path := filepath.Join(t.TempDir(), "sampling.dagr")
	w, err := spool.Create(path, spool.Traces)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	enc := pdatabridge.NewStreamBuilder()
	var dst []byte
	for i, td := range c.Batches {
		dst = enc.AppendRecord(dst[:0], uint64(5000+i), td)
		if err := w.AppendRecord(dst); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	return path, c
}

// The corpus must hold as many distinct traces as it claims. An earlier version derived
// the trace id byte-wise from the trace index, which wraps at 256: a 400-trace corpus
// quietly became a 256-trace one, with the surplus traces merged into existing ids and
// every per-trace expectation silently wrong. Nothing failed — the benchmark just
// reported a number that did not match its input.
func TestSamplingCorpusHasDistinctTraceIDs(t *testing.T) {
	for _, n := range []int{60, 256, 400, 1000} {
		c := fixture.NewSampling(n, 7)
		seen := map[[16]byte]bool{}
		for _, id := range c.TraceIDs {
			if seen[id] {
				t.Fatalf("%d traces: id %x is used twice", n, id)
			}
			seen[id] = true
		}
		if len(seen) != n {
			t.Fatalf("asked for %d traces, corpus has %d distinct ids", n, len(seen))
		}
		if len(c.SpansPer) != n || len(c.LastNano) != n {
			t.Fatalf("%d traces: per-trace maps hold %d/%d entries", n, len(c.SpansPer), len(c.LastNano))
		}
	}
}

// The corpus must actually split traces across records, or the sampler would be gated
// on a problem it does not have.
func TestSamplingCorpusSplitsTracesAcrossBatches(t *testing.T) {
	path, c := writeSampling(t, 60, 5)
	q, err := store.Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	s := &store.Sampler{Policy: func(store.Summary) bool { return true }}
	d, err := s.Decide(q, 0)
	if err != nil {
		t.Fatalf("decide: %v", err)
	}
	if d.Stats.Traces != len(c.TraceIDs) {
		t.Fatalf("saw %d traces, corpus has %d", d.Stats.Traces, len(c.TraceIDs))
	}
	split := 0
	for _, sum := range d.Sampled {
		if sum.Batches > 1 {
			split++
		}
		if want := c.SpansPer[sum.ID]; sum.Spans != want {
			t.Fatalf("trace %x has %d spans in the window, corpus wrote %d", sum.ID, sum.Spans, want)
		}
	}
	if split == 0 {
		t.Fatal("no trace spans more than one record — the corpus is not exercising the sampler")
	}
}

// The decision itself (spec/38 §6.2): the policy picks exactly the traces the corpus
// marked, and it does so from summaries alone.
func TestDecideFindsErrorAndSlowTraces(t *testing.T) {
	path, c := writeSampling(t, 90, 6)
	q, err := store.Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	s := &store.Sampler{
		Policy: store.SampleAny(store.SampleErrors, store.SampleSlowerThan(fixture.SlowNanos)),
	}
	d, err := s.Decide(q, 0)
	if err != nil {
		t.Fatalf("decide: %v", err)
	}

	want := map[[16]byte]bool{}
	for id := range c.Errors {
		want[id] = true
	}
	for id := range c.Slow {
		want[id] = true
	}
	if len(d.Sampled) != len(want) {
		t.Fatalf("sampled %d traces, want %d", len(d.Sampled), len(want))
	}
	for _, sum := range d.Sampled {
		if !want[sum.ID] {
			t.Fatalf("sampled trace %x, which is neither errored nor slow", sum.ID)
		}
		if c.Errors[sum.ID] && !sum.HasError() {
			t.Fatalf("trace %x errored in the corpus but not in its summary", sum.ID)
		}
	}
	// A sanity floor: the policy must not be keeping everything.
	if len(d.Sampled) >= d.Stats.Traces {
		t.Fatalf("policy kept %d of %d traces — it is not discriminating",
			len(d.Sampled), d.Stats.Traces)
	}
}

// Wait is what makes it a TAIL sampler: a trace still receiving spans is not decided.
func TestWaitDefersStillArrivingTraces(t *testing.T) {
	path, c := writeSampling(t, 40, 4)
	q, err := store.Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	var newest uint64
	for _, last := range c.LastNano {
		if last > newest {
			newest = last
		}
	}

	// A clock at the newest span, with a wait longer than the corpus's whole span of
	// time: nothing has gone quiet, so nothing is decidable.
	s := &store.Sampler{Wait: 1 << 62, Policy: func(store.Summary) bool { return true }}
	d, err := s.Decide(q, newest)
	if err != nil {
		t.Fatalf("decide: %v", err)
	}
	if d.Stats.Decidable != 0 || len(d.Sampled) != 0 {
		t.Fatalf("with an impossible wait, %d traces were decidable and %d sampled",
			d.Stats.Decidable, len(d.Sampled))
	}
	if d.Stats.Traces == 0 {
		t.Fatal("the window held no traces at all")
	}

	// Move the clock past the wait and everything decides.
	s.Wait = 1_000_000
	d, err = s.Decide(q, newest+2_000_000)
	if err != nil {
		t.Fatalf("decide: %v", err)
	}
	if d.Stats.Decidable != d.Stats.Traces {
		t.Fatalf("%d of %d traces decidable after the wait elapsed",
			d.Stats.Decidable, d.Stats.Traces)
	}
}

// The claim that matters: what the sampler holds in memory is O(traces) of flat
// summaries, not O(spans) of spans. The corpus's spans outweigh its traces, and the
// held bytes must track the traces.
func TestDecisionHoldsSummariesNotSpans(t *testing.T) {
	path, c := writeSampling(t, 200, 8)
	q, err := store.Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	s := &store.Sampler{Policy: store.SampleErrors}
	d, err := s.Decide(q, 0)
	if err != nil {
		t.Fatalf("decide: %v", err)
	}

	if d.Stats.SpansExamined != c.TotalSpan {
		t.Fatalf("examined %d spans, corpus has %d", d.Stats.SpansExamined, c.TotalSpan)
	}
	perTrace := d.Stats.HeldBytes / d.Stats.Traces
	if perTrace > 96 {
		t.Fatalf("the decision holds %d bytes per trace; a Summary should be flat and small", perTrace)
	}
	// Whatever a span costs, a trace here has more than one of them, so holding
	// summaries must be strictly cheaper than holding spans would be.
	if d.Stats.HeldBytes >= c.TotalSpan*perTrace {
		t.Fatalf("held %d bytes for %d traces over %d spans — not summary-sized",
			d.Stats.HeldBytes, d.Stats.Traces, c.TotalSpan)
	}
}

// The second walk touches only the sampled traces' spans, and hands them over as lazy
// handles — the point being that the un-sampled majority is never looked at twice and
// never materialised at all.
func TestEmitYieldsOnlySampledTraces(t *testing.T) {
	path, c := writeSampling(t, 120, 6)
	q, err := store.Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	s := &store.Sampler{Policy: store.SampleErrors}
	d, err := s.Decide(q, 0)
	if err != nil {
		t.Fatalf("decide: %v", err)
	}

	emitted := map[[16]byte]int{}
	total := 0
	if err := d.Emit(q, s.Window, func(sum store.Summary, sp store.Span) bool {
		id, ok := sp.Acc.TraceID()
		if !ok {
			t.Fatal("emitted a span with no trace id")
		}
		var key [16]byte
		copy(key[:], id)
		if key != sum.ID {
			t.Fatalf("span of trace %x emitted under summary %x", key, sum.ID)
		}
		if !c.Errors[key] {
			t.Fatalf("emitted trace %x, which the corpus did not mark errored", key)
		}
		emitted[key]++
		total++
		return true
	}); err != nil {
		t.Fatalf("emit: %v", err)
	}

	if len(emitted) != len(c.Errors) {
		t.Fatalf("emitted %d traces, %d errored in the corpus", len(emitted), len(c.Errors))
	}
	for id, n := range emitted {
		if n != c.SpansPer[id] {
			t.Fatalf("trace %x: emitted %d spans, corpus wrote %d", id, n, c.SpansPer[id])
		}
	}
	if total >= c.TotalSpan {
		t.Fatalf("emit touched %d of %d spans — it is not sampling", total, c.TotalSpan)
	}
	// And the sampled fraction is small, which is the economic argument: the spans that
	// get materialised downstream are the ones a policy asked for, not the buffer.
	if frac := float64(total) / float64(c.TotalSpan); frac > 0.2 {
		t.Fatalf("emitted %.0f%% of all spans — the corpus is not sampling-shaped", frac*100)
	}
}

// A window bounds the decision pass: it looks at the newest records and stops.
func TestWindowBoundsTheDecisionPass(t *testing.T) {
	path, _ := writeSampling(t, 150, 10)
	q, err := store.Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	full := &store.Sampler{Policy: store.SampleErrors}
	all, err := full.Decide(q, 0)
	if err != nil {
		t.Fatalf("decide: %v", err)
	}

	windowed := &store.Sampler{Window: store.Window{MaxBatches: 3}, Policy: store.SampleErrors}
	part, err := windowed.Decide(q, 0)
	if err != nil {
		t.Fatalf("decide: %v", err)
	}

	if part.Stats.Scan.BatchesOpened != 3 {
		t.Fatalf("a 3-batch window opened %d batches", part.Stats.Scan.BatchesOpened)
	}
	if all.Stats.Scan.BatchesOpened != 10 {
		t.Fatalf("the unbounded pass opened %d batches, want 10", all.Stats.Scan.BatchesOpened)
	}
	if part.Stats.SpansExamined >= all.Stats.SpansExamined {
		t.Fatalf("the window examined %d spans, the whole spool %d",
			part.Stats.SpansExamined, all.Stats.SpansExamined)
	}
	// A trace whose spans are split across records may be only partly visible in a
	// short window — that is a real property of tail sampling, not a bug, and the
	// summary's Spans count is what tells a caller so.
	if part.Stats.Traces == 0 {
		t.Fatal("the window found no traces")
	}
}

// A sampler that decides on a timer runs the same pass forever, so its working state has
// to be reused rather than rebuilt. What is allowed to allocate is the RESULT — the
// sampled summaries and the keep set — and that scales with the traces kept, not with
// the traces or spans examined.
//
// The gate is a ratio, not a constant: quadrupling the corpus must not quadruple the
// allocations.
func TestDecideAllocationDoesNotTrackCorpusSize(t *testing.T) {
	measure := func(nTraces, nBatches int) float64 {
		path, _ := writeSampling(t, nTraces, nBatches)
		q, err := store.Open(path)
		if err != nil {
			t.Fatalf("open: %v", err)
		}
		// A policy that keeps nothing isolates the decision pass from the result.
		s := &store.Sampler{Policy: func(store.Summary) bool { return false }}
		run := func() {
			if _, err := s.Decide(q, 0); err != nil {
				t.Fatalf("decide: %v", err)
			}
		}
		run() // size the scratch and the index
		return testing.AllocsPerRun(5, run)
	}

	small := measure(100, 4)
	large := measure(400, 8)
	if large > small+2 {
		t.Fatalf("deciding 100 traces allocated %.0f times, 400 traces %.0f — "+
			"the working state is being rebuilt per trace", small, large)
	}
	if small > 8 {
		t.Fatalf("a decision pass allocated %.0f times; only the result should", small)
	}
}
