package store_test

import (
	"testing"

	"go.opentelemetry.io/collector/pdata/ptrace"

	"otlpdagr/internal/fixture"
	"otlpdagr/pdatabridge"
	"otlpdagr/spool"
	"otlpdagr/store"
)

// BenchmarkDecide is the §6.2 claim, measured: the same tail-sampling decision over the
// same spool, once reading fields in place and once materialising each batch into pdata
// first.
//
// Both walk the same records, fold the same four fields into the same summaries, and
// reach the same verdict. The only difference is whether the span exists as an object
// before its status code is read.
func BenchmarkDecide(b *testing.B) {
	path, c := writeSampling(b, 400, 12)
	q, err := store.Open(path)
	if err != nil {
		b.Fatal(err)
	}
	s := &store.Sampler{Policy: store.SampleAny(store.SampleErrors, store.SampleSlowerThan(fixture.SlowNanos))}

	// Agree on the answer before timing either one.
	lazy, err := s.Decide(q, 0)
	if err != nil {
		b.Fatal(err)
	}
	eager, err := decideViaPdata(q.Reader(), s.Policy)
	if err != nil {
		b.Fatal(err)
	}
	if lazy.Stats.Sampled != eager {
		b.Fatalf("the two paths disagree: %d sampled lazily, %d through pdata",
			lazy.Stats.Sampled, eager)
	}
	b.Logf("%d traces over %d spans, %d sampled (%.1f%%)",
		lazy.Stats.Traces, c.TotalSpan, lazy.Stats.Sampled,
		100*float64(lazy.Stats.Sampled)/float64(lazy.Stats.Traces))

	b.Run("lazy", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			if _, err := s.Decide(q, 0); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("via_pdata", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			if _, err := decideViaPdata(q.Reader(), s.Policy); err != nil {
				b.Fatal(err)
			}
		}
	})
}

// decideViaPdata is the same decision taken the way a collector component would have to
// take it: materialise the batch, then look at it.
func decideViaPdata(r *spool.Reader, policy store.Policy) (int, error) {
	byTrace := map[[16]byte]*store.Summary{}
	err := r.Reverse(func(bt spool.Batch) bool {
		td := pdatabridge.FromRecord(bt.Record)
		for i := 0; i < td.ResourceSpans().Len(); i++ {
			rs := td.ResourceSpans().At(i)
			for j := 0; j < rs.ScopeSpans().Len(); j++ {
				sp := rs.ScopeSpans().At(j).Spans()
				for k := 0; k < sp.Len(); k++ {
					fold(byTrace, sp.At(k))
				}
			}
		}
		return true
	})
	if err != nil {
		return 0, err
	}
	n := 0
	for _, sum := range byTrace {
		if policy(*sum) {
			n++
		}
	}
	return n, nil
}

func fold(byTrace map[[16]byte]*store.Summary, sp ptrace.Span) {
	key := [16]byte(sp.TraceID())
	sum := byTrace[key]
	if sum == nil {
		sum = &store.Summary{ID: key, FirstNano: ^uint64(0)}
		byTrace[key] = sum
	}
	sum.Spans++
	if v := uint64(sp.StartTimestamp()); v < sum.FirstNano {
		sum.FirstNano = v
	}
	if v := uint64(sp.EndTimestamp()); v > sum.LastNano {
		sum.LastNano = v
	}
	if sp.Status().Code() == ptrace.StatusCodeError {
		sum.ErrorSpans++
	}
}

// BenchmarkLastSpans is the crash-ring query (§6.3): the newest 500 spans out of a spool
// that holds twenty times that, with nothing but the envelope consulted to know where to
// stop.
func BenchmarkLastSpans(b *testing.B) {
	sizes := make([]int, 20)
	for i := range sizes {
		sizes[i] = 500
	}
	q, err := store.Open(writeSpool(b, sizes...))
	if err != nil {
		b.Fatal(err)
	}
	var sink uint64
	b.ReportAllocs()
	for b.Loop() {
		if err := q.LastSpans(500, func(s store.Span) bool {
			if v, ok := s.Acc.EndTimeUnixNano(); ok {
				sink += v
			}
			return true
		}); err != nil {
			b.Fatal(err)
		}
	}
	if sink == 0 {
		b.Fatal("read nothing")
	}
	b.ReportMetric(float64(q.Stats().BatchesOpened), "batches/op")
}
