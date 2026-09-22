package store

import (
	"fmt"
	"unsafe"

	g "otlptraces"
)

// Sampler is a tail sampler whose buffer is the spool (spec/38 §6.2).
//
// The collector's tail_sampling processor holds every span of every undecided trace in
// a map, and caps itself at `num_traces` — a cap that exists because the spans are in
// RAM. Here they are not. The spans are already on disk, in a file that can be read
// backwards, so the only thing that has to be in memory is what the POLICY needs, and
// that is a fixed handful of bytes per trace rather than the trace.
//
// So a pass is two walks over the same window, both of them cheap:
//
//	Decide   walk newest-first, read trace_id, start, end and status.code off each span
//	         in place, and fold them into one small Summary per trace. Nothing is kept
//	         but the summaries — no spans, no accessors, no batch.
//	Emit     for the traces the policy accepted, walk the window again and hand out
//	         their spans. Only the sampled fraction is ever touched twice, and only the
//	         sampled fraction is ever materialised — which in tail sampling is the whole
//	         economic argument, because the sampled fraction is a few percent.
//
// Walking twice is the thing the in-memory design cannot afford and this one can: the
// second walk re-reads a file that is already in the page cache, against holding every
// span of every open trace in the heap for the length of the decision wait.
type Sampler struct {
	// Window is how far back a decision may look. Zero is the whole spool.
	Window Window

	// Wait is how long a trace must go quiet before it is decidable, in nanoseconds
	// against the `now` passed to Decide. Zero decides every trace in the window.
	Wait uint64

	// Policy returns true for a trace worth keeping. It sees only the summary — by
	// construction, since the spans were never held.
	Policy Policy

	// Working state, reused across passes: the summaries live in one flat slice and the
	// map holds indexes into it, so a sampler deciding on a timer forever does not
	// allocate a Summary per trace per pass. Not safe for concurrent use.
	sums  []Summary
	index map[[16]byte]int
}

// Policy decides one trace from its summary.
type Policy func(Summary) bool

// Summary is everything the decision pass keeps about one trace. It is deliberately
// small and flat: this is the structure whose size sets the sampler's memory ceiling,
// and it is what replaces "every span of every undecided trace".
type Summary struct {
	ID         [16]byte
	Spans      int
	FirstNano  uint64 // earliest start seen
	LastNano   uint64 // latest end seen
	ErrorSpans int
	Batches    int // distinct records the trace's spans were found in
	lastBatch  int
}

// DurationNanos is the trace's wall span as observed in the window.
func (s Summary) DurationNanos() uint64 {
	if s.LastNano < s.FirstNano {
		return 0
	}
	return s.LastNano - s.FirstNano
}

// HasError reports whether any span in the window carried STATUS_CODE_ERROR.
func (s Summary) HasError() bool { return s.ErrorSpans > 0 }

// DecideStats is what a decision pass cost.
type DecideStats struct {
	Scan          Stats
	Traces        int // distinct traces seen in the window
	Decidable     int // traces quiet for at least Wait
	Sampled       int
	HeldBytes     int // memory the summaries occupy — the sampler's actual ceiling
	SpansExamined int
}

func (d DecideStats) String() string {
	return fmt.Sprintf("%d traces (%d decidable, %d sampled) from %d spans, %d B held",
		d.Traces, d.Decidable, d.Sampled, d.SpansExamined, d.HeldBytes)
}

// Decisions is the result of a decision pass.
type Decisions struct {
	Sampled []Summary
	Stats   DecideStats

	keep map[[16]byte]struct{}
}

// Decide walks the window, summarises every trace in it, and applies the policy to the
// ones that have gone quiet. `now` is the clock in nanoseconds; pass the newest end
// timestamp in the spool to decide against the data's own clock rather than the wall's.
func (s *Sampler) Decide(q *Query, now uint64) (*Decisions, error) {
	s.sums = s.sums[:0]
	if s.index == nil {
		s.index = map[[16]byte]int{}
	} else {
		clear(s.index)
	}

	err := q.ScanWindow(s.Window, func(sp Span) bool {
		id, ok := sp.Acc.TraceID()
		if !ok || len(id) != 16 {
			return true
		}
		key := [16]byte(id)

		at, seen := s.index[key]
		if !seen {
			at = len(s.sums)
			s.sums = append(s.sums, Summary{ID: key, FirstNano: ^uint64(0), lastBatch: -1})
			s.index[key] = at
		}
		sum := &s.sums[at]

		sum.Spans++
		if sp.Batch != sum.lastBatch {
			sum.Batches++
			sum.lastBatch = sp.Batch
		}
		if v, ok := sp.Acc.StartTimeUnixNano(); ok && v < sum.FirstNano {
			sum.FirstNano = v
		}
		if v, ok := sp.Acc.EndTimeUnixNano(); ok && v > sum.LastNano {
			sum.LastNano = v
		}
		if st, ok := sp.Acc.Status(); ok {
			if c, ok := st.Code(); ok && c == g.StatusCodeSTATUSCODEERROR {
				sum.ErrorSpans++
			}
		}
		return true
	})
	if err != nil {
		return nil, err
	}

	d := &Decisions{keep: map[[16]byte]struct{}{}}
	d.Stats.Scan = q.Stats()
	d.Stats.SpansExamined = d.Stats.Scan.SpansExamined
	d.Stats.Traces = len(s.sums)
	d.Stats.HeldBytes = len(s.sums) * int(unsafe.Sizeof(Summary{}))

	for i := range s.sums {
		sum := &s.sums[i]
		if sum.FirstNano == ^uint64(0) {
			sum.FirstNano = sum.LastNano
		}
		if s.Wait > 0 && now < sum.LastNano+s.Wait {
			continue // still arriving; decide it on a later pass
		}
		d.Stats.Decidable++
		if s.Policy != nil && !s.Policy(*sum) {
			continue
		}
		d.Sampled = append(d.Sampled, *sum)
		d.keep[sum.ID] = struct{}{}
	}
	d.Stats.Sampled = len(d.Sampled)
	return d, nil
}

// Sampled reports whether a trace id was kept.
func (d *Decisions) Contains(id []byte) bool {
	if len(id) != 16 {
		return false
	}
	_, ok := d.keep[[16]byte(id)]
	return ok
}

// Emit walks the window again and calls fn for every span of every sampled trace,
// newest-first. This is the only point at which a span is looked at twice, and it
// happens for the sampled fraction alone.
//
// The spans arrive as lazy handles, so a caller that only needs to forward them pays no
// decode at all; one that must hand them to a pdata pipeline materialises here, and
// materialises the few percent it decided to keep rather than everything it buffered.
func (d *Decisions) Emit(q *Query, w Window, fn func(Summary, Span) bool) error {
	if len(d.keep) == 0 {
		return nil
	}
	byID := map[[16]byte]Summary{}
	for _, s := range d.Sampled {
		byID[s.ID] = s
	}
	return q.ScanWindow(w, func(sp Span) bool {
		id, ok := sp.Acc.TraceID()
		if !ok || len(id) != 16 {
			return true
		}
		sum, ok := byID[[16]byte(id)]
		if !ok {
			return true
		}
		return fn(sum, sp)
	})
}

// ── policies ─────────────────────────────────────────────────────────────────

// SampleErrors keeps any trace with an errored span — the policy every tail sampler
// starts with.
func SampleErrors(s Summary) bool { return s.HasError() }

// SampleSlowerThan keeps traces whose observed duration exceeds d nanoseconds.
func SampleSlowerThan(d uint64) Policy {
	return func(s Summary) bool { return s.DurationNanos() > d }
}

// SampleAny keeps a trace matching any of the given policies.
func SampleAny(ps ...Policy) Policy {
	return func(s Summary) bool {
		for _, p := range ps {
			if p(s) {
				return true
			}
		}
		return false
	}
}
