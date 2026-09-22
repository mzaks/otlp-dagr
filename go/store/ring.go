package store

import (
	"fmt"
	"os"

	"otlpdagr/spool"
)

// Ring is a crash/debug ring: a spool that keeps the newest records and drops the rest,
// so "the last N spans before the error" is always available and the file never grows
// without bound (spec/38 §6.3).
//
// Retention is done by COMPACTION, not by rotation into segments, and the doubly-linked
// span is what makes that affordable. Deciding what to keep is a walk backwards from EOF
// that stops as soon as the budget is met: the records to drop are never read, never
// parsed, and never even stepped over. A spool with a 4 MB budget sitting at 400 MB
// costs 4 MB of reading to compact, not 400.
//
// The alternative — segment files with a directory of them — buys nothing here. It
// exists in log systems because their readers cannot start from the end.
type Ring struct {
	path string
	opt  RingOptions
	w    *spool.Writer

	compactions int
	dropped     int64
	lastWalked  int // records the last compaction had to look at
}

// RingOptions bounds what the ring keeps. Both limits apply; whichever binds first wins.
// A zero limit is no limit, and a Ring with neither set is just an append-only spool.
type RingOptions struct {
	// MaxBytes is the spool size that triggers compaction, and roughly the size it is
	// compacted back to — "roughly" because compaction keeps whole records, so the
	// result is the largest set of newest records that fits.
	MaxBytes int64

	// MaxBatches caps the number of records kept.
	MaxBatches int

	// Sync fsyncs after every append.
	Sync bool
}

// OpenRing opens or creates the ring at path, recovering a torn tail if the last process
// was killed mid-write.
func OpenRing(path string, opt RingOptions) (*Ring, spool.Recovery, error) {
	w, rec, err := spool.Resume(path)
	if err != nil {
		return nil, rec, err
	}
	return &Ring{path: path, opt: opt, w: w}, rec, nil
}

// Append adds a batch and compacts if the ring is over budget.
func (r *Ring) Append(receivedUnixNano uint64, spanCount uint32, traces []byte) error {
	if err := r.w.Append(receivedUnixNano, spanCount, traces); err != nil {
		return err
	}
	if r.opt.Sync {
		if err := r.w.Sync(); err != nil {
			return err
		}
	}
	return r.compactIfNeeded()
}

func (r *Ring) Close() error { return r.w.Close() }

// Compactions and Dropped report what retention has actually done.
func (r *Ring) Compactions() int { return r.compactions }
func (r *Ring) Dropped() int64   { return r.dropped }

// LastWalked is how many records the last compaction had to look at. It should be the
// kept set plus the one record that broke the budget — never the whole spool. This is
// the number that says the doubly-linked span is earning its keep.
func (r *Ring) LastWalked() int { return r.lastWalked }

func (r *Ring) overBudget(size int64, records int) bool {
	if r.opt.MaxBytes > 0 && size > r.opt.MaxBytes {
		return true
	}
	return r.opt.MaxBatches > 0 && records > r.opt.MaxBatches
}

func (r *Ring) compactIfNeeded() error {
	st, err := os.Stat(r.path)
	if err != nil {
		return err
	}
	if r.opt.MaxBytes > 0 && st.Size() > r.opt.MaxBytes {
		return r.Compact()
	}
	if r.opt.MaxBatches <= 0 {
		return nil
	}
	// Counting records needs the reverse walk, but only as far as the cap plus one —
	// the moment one more record than the budget has been seen, the answer is known.
	rd, err := spool.Open(r.path)
	if err != nil {
		return err
	}
	seen := 0
	if err := rd.Reverse(func(spool.Batch) bool {
		seen++
		return seen <= r.opt.MaxBatches
	}); err != nil {
		return err
	}
	if seen > r.opt.MaxBatches {
		return r.Compact()
	}
	return nil
}

// Compact rewrites the ring keeping only the newest records that fit the budget.
//
// The kept set is chosen by walking backwards until a limit binds, so the dropped
// prefix is never touched. Writing them back out has to go forward — a stream is built
// oldest-first — so the kept records are collected, reversed, and appended to a fresh
// spool which then replaces the old one by rename.
func (r *Ring) Compact() error {
	if err := r.w.Close(); err != nil {
		return err
	}
	rd, err := spool.Open(r.path)
	if err != nil {
		return err
	}
	before := int64(len(rd.Bytes()))

	type kept struct {
		received uint64
		count    uint32
		traces   []byte
	}
	var keep []kept
	var bytes int64
	walked := 0
	if err := rd.Reverse(func(b spool.Batch) bool {
		walked++
		size := int64(b.Range.End - b.Range.Start)
		if r.opt.MaxBytes > 0 && bytes+size > r.opt.MaxBytes && len(keep) > 0 {
			return false
		}
		if r.opt.MaxBatches > 0 && len(keep) >= r.opt.MaxBatches {
			return false
		}
		keep = append(keep, kept{b.ReceivedUnixNano, b.SpanCount, b.Traces})
		bytes += size
		return true
	}); err != nil {
		return err
	}
	r.lastWalked = walked

	tmp := r.path + ".compact"
	w, err := spool.Create(tmp)
	if err != nil {
		return err
	}
	for i := len(keep) - 1; i >= 0; i-- {
		if err := w.Append(keep[i].received, keep[i].count, keep[i].traces); err != nil {
			w.Close()
			os.Remove(tmp)
			return err
		}
	}
	if err := w.Sync(); err != nil {
		w.Close()
		os.Remove(tmp)
		return err
	}
	if err := w.Close(); err != nil {
		os.Remove(tmp)
		return err
	}
	if err := os.Rename(tmp, r.path); err != nil {
		os.Remove(tmp)
		return err
	}

	st, err := os.Stat(r.path)
	if err != nil {
		return err
	}
	r.compactions++
	r.dropped += before - st.Size()

	// Reopen for appending. The spool on disk is already framed, so this is the same
	// resume path a restart takes.
	nw, _, err := spool.Resume(r.path)
	if err != nil {
		return err
	}
	r.w = nw
	return nil
}

func (r *Ring) String() string {
	return fmt.Sprintf("ring %s (max %d B / %d batches, %d compactions, %d B dropped)",
		r.path, r.opt.MaxBytes, r.opt.MaxBatches, r.compactions, r.dropped)
}
