// Package store is the query surface over an OTLP-on-Dagr spool: "the last N spans
// before the error", "every span of this trace", and the retention that keeps a spool
// from growing without bound.
//
// It is the half of spec/38 that a collector pipeline cannot host. The type flowing
// between receiver, processor and exporter is pdata, so a component that wants a field
// must materialise the whole batch to get it (§5.3). Nothing here does: a spool record
// carries its batch's ResourceSpans inline, and the graph's generated accessors read a
// field in place, out of the mapped file, allocating nothing. That is where the 29x
// lives, and it lives in a service that reads the spool directly rather than in a
// processor.
//
// Two orderings matter and they are not the same. Batches come newest-first for free —
// that is what the sink's doubly-linked span is for. Spans WITHIN a batch are stored
// oldest-first and a packed array's indexed access is O(i), so a batch's spans are read
// forward, once, through the generated iterator, into a reusable buffer, and emitted
// from the end. Cost is proportional to the spans examined, never to the spool.
package store

import (
	"fmt"

	"otlpgen/dagr"
	g "otlpgen/otlptraces"
	"otlpgen/spanlog"

	"otlpdagr/spool"
)

// Span is one span, unread. The accessor borrows the spool's bytes, so reading a field
// costs the decode of that field and nothing else — and so a Span is only valid while
// the Query it came from is alive.
type Span struct {
	Acc g.SpanAccessor

	// Batch counts back from the newest record (0 is the newest), Index is the span's
	// position within its batch, and Received is the batch's envelope stamp. Together
	// they order two spans without reading either one.
	Batch    int
	Index    int
	Received uint64
}

// Stats counts the work a walk actually did. It exists because the claims in this
// package are about work avoided, and a claim like that has to be measurable rather
// than argued: a "last 500 spans" query over a 40-batch spool should open one or two
// batches, not forty.
type Stats struct {
	RecordsWalked int // envelope records stepped over, newest-first
	BatchesOpened int // batches whose spans were actually read
	SpansExamined int // spans handed to the caller's function
	SpansBuffered int // spans iterated to reach them (>= SpansExamined)
}

func (s Stats) String() string {
	return fmt.Sprintf("%d records, %d batches opened, %d/%d spans",
		s.RecordsWalked, s.BatchesOpened, s.SpansExamined, s.SpansBuffered)
}

// Query reads a spool. It is not safe for concurrent use: it owns a scratch buffer that
// each walk reuses, which is what keeps a steady-state scan allocation-free.
type Query struct {
	r *spool.Reader

	// Validate walks each batch's ResourceSpans through the graph's checking entry point.
	// Off by default: a spool this process wrote is not untrusted input. Turn it on for
	// a spool that arrived from somewhere else — it walks the reachable graph once, so
	// it is not free (spec/38 §10.1).
	Validate bool

	scratch []g.SpanAccessor
	stats   Stats
}

// Open reads the spool at path.
func Open(path string) (*Query, error) {
	r, err := spool.Open(path, spool.Traces)
	if err != nil {
		return nil, err
	}
	return &Query{r: r}, nil
}

// NewQuery wraps a spool already in hand.
func NewQuery(r *spool.Reader) *Query { return &Query{r: r} }

// Reader is the underlying spool.
func (q *Query) Reader() *spool.Reader { return q.r }

// Stats reports the work the last walk did.
func (q *Query) Stats() Stats { return q.stats }

// LastSpans calls fn for the newest n spans, newest first, and stops. Passing n <= 0
// walks the whole spool.
//
// Batches are opened only as far back as n requires: the envelope's span_count says how
// many spans a record holds without opening it, so a query for 10 spans over a spool of
// 500-span batches parses exactly one batch.
func (q *Query) LastSpans(n int, fn func(Span) bool) error {
	return q.walkBack(Window{MaxSpans: n}, fn)
}

// Window bounds how far back a walk reaches. A zero Window is the whole spool; when both
// limits are set, whichever binds first ends the walk.
//
// This is the "buffer" a tail sampler works over, and the point is that it is a window
// onto a FILE. The collector's in-memory tail sampler caps itself at num_traces because
// the traces are in RAM; here they are not, so the cap is a policy choice about how far
// back a decision may look rather than a limit imposed by memory.
type Window struct {
	MaxBatches int
	MaxSpans   int
}

// ScanWindow is Scan bounded by w.
func (q *Query) ScanWindow(w Window, fn func(Span) bool) error { return q.walkBack(w, fn) }

// Scan calls fn for every span in the spool, newest first, until fn returns false.
//
// Unlike LastSpans it cannot know in advance how far back to go, so it opens each batch
// as it reaches it. A predicate that stops early still only pays for the batches it
// touched.
func (q *Query) Scan(fn func(Span) bool) error { return q.walkBack(Window{}, fn) }

// SpanCount totals the spool's spans from the envelopes alone — no batch is opened.
func (q *Query) SpanCount() (spans int, batches int, err error) {
	err = q.r.Reverse(func(b spool.Batch) bool {
		spans += int(b.Count)
		batches++
		return true
	})
	return spans, batches, err
}

// walkBack steps records newest-first and emits each one's spans from the end, bounded
// by w.
//
// The walk stops the moment a limit is met, which is what keeps the batch count honest:
// a query for ten spans never steps onto the record before the one it found them in, so
// it neither reads nor opens it.
func (q *Query) walkBack(w Window, fn func(Span) bool) error {
	q.stats = Stats{}
	delivered := 0
	batchIdx := 0
	var walkErr error

	err := q.r.Reverse(func(b spool.Batch) bool {
		if w.MaxBatches > 0 && batchIdx >= w.MaxBatches {
			return false
		}
		q.stats.RecordsWalked++
		spans, err := q.fill(b.Record)
		if err != nil {
			walkErr = err
			return false
		}
		q.stats.BatchesOpened++
		q.stats.SpansBuffered += len(spans)

		// Emit from the end: within a batch the newest span is the last one.
		for i := len(spans) - 1; i >= 0; i-- {
			q.stats.SpansExamined++
			if !fn(Span{Acc: spans[i], Batch: batchIdx, Index: i, Received: b.ReceivedUnixNano}) {
				return false
			}
			delivered++
			if w.MaxSpans > 0 && delivered >= w.MaxSpans {
				return false
			}
		}
		batchIdx++
		return true
	})
	if walkErr != nil {
		return walkErr
	}
	return err
}

// fill collects one record's span accessors into the reusable scratch, in stored
// (oldest-first) order. The record holds the batch's ResourceSpans inline; with Validate
// each is walked once first (otlptraces.ValidateResourceSpans) — what Open did for the
// embedded buffer this record used to carry — so a malformed spool is an error, never a
// panic.
//
// It iterates with SpansAll rather than SpansAt: a packed array's indexed access walks
// the array to reach element i, so a loop of SpansAt is quadratic by construction. The
// generated iterator is the O(1)-per-step cursor (spec/38 §10.2).
func (q *Query) fill(record []byte) (out []g.SpanAccessor, err error) {
	if q.Validate {
		defer func() {
			if r := recover(); r != nil {
				out, err = nil, dagr.ErrMalformed
			}
		}()
	}
	a := spanlog.NewBatchAccessor(record, 0)
	if q.Validate {
		for rs := range a.ResourceSpansAll() {
			if err := g.ValidateResourceSpans(rs); err != nil {
				return nil, err
			}
		}
	}
	out = q.scratch[:0]
	for rs := range a.ResourceSpansAll() {
		for ss := range rs.ScopeSpansAll() {
			for sp := range ss.SpansAll() {
				out = append(out, sp)
			}
		}
	}
	q.scratch = out
	return out, nil
}
