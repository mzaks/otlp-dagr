// Package spool is the file side of the SpanLog DataSink: create or resume a spool,
// append batches, read them back forward or newest-first, and — the part a write-ahead
// log lives or dies on — decide whether the tail is intact.
//
// The sink's wire shape (Dagr spec 11) is
//
//	[framing LEB] ( [typeId LEB] [content_size LEB] [packed body] [span RLEB] )*
//
// and the trailing reverse-delimited LEB is there because `doubly_linked=True`: it holds
// the record's own byte length written backwards, so a reader that starts at EOF can step
// to the record's first byte without having seen anything before it (spec 11 §11.4).
// That is what makes restart recovery O(1) instead of O(spool): the only question after a
// crash is whether the LAST record is whole, and the RLEB answers it directly.
//
// Nothing here parses a record body. Framing is walked with the generated reader and the
// Dagr runtime's LEB primitives; bodies are decoded by the generated accessors.
package spool

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"

	"otlptraces/dagr"
	"otlptraces/spanlog"
)

// ── Writing ──────────────────────────────────────────────────────────────────

// Writer appends batches to a spool file. One record reaches the file in one Write, so
// a reader never sees a partial record except after a crash mid-syscall — which is
// exactly what CheckTail detects.
type Writer struct {
	f   *os.File
	w   *spanlog.SpanLogWriter
	n   int   // records appended by THIS writer
	off int64 // file size when this writer opened it (0 for a fresh spool)
}

// Create starts a new spool at path, truncating anything already there.
func Create(path string) (*Writer, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return nil, err
	}
	w := &Writer{f: f, w: spanlog.NewSpanLogStreamWriter(f)}
	// Write the framing word up front: a spool opened and not yet written to is still a
	// valid, empty stream, so a reader started before the first batch sees a stream
	// rather than an empty file.
	if err := w.w.EnsureFraming(); err != nil {
		f.Close()
		return nil, err
	}
	return w, nil
}

// Resume reopens an existing spool for appending, after recovering its tail.
//
// This is the restart path, and it is the reason the sink is doubly linked. Recover
// walks backwards from EOF; in the ordinary case (the process was shut down cleanly, or
// killed between records) it validates one record and returns. Only a genuinely torn
// tail costs a forward pass, and only once. Whatever it drops is reported in Recovery,
// never silently kept.
//
// A missing or empty file is not an error — Resume creates the spool.
func Resume(path string) (*Writer, Recovery, error) {
	st, err := os.Stat(path)
	if errors.Is(err, os.ErrNotExist) || (err == nil && st.Size() == 0) {
		w, cErr := Create(path)
		return w, Recovery{Fresh: true}, cErr
	}
	if err != nil {
		return nil, Recovery{}, err
	}
	rec, err := Recover(path)
	if err != nil {
		return nil, rec, err
	}
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, rec, err
	}
	// The stream is already framed on disk. Hand the generated writer a destination that
	// swallows its framing write and passes every record through, so an appended record
	// is byte-identical to one written by a writer that had framed the file itself —
	// TestResumeAfterCrashKeepsOneStream asserts exactly that, against a spool written
	// in one run.
	//
	// The one thing this trick cannot give back is the writer's idea of where it is: it
	// counts from the framing word it thinks it just wrote, not from the end of the file
	// it is appending to. Two consequences, both fine for this schema and both worth
	// knowing before reusing the trick:
	//
	//   * the RecordRange that AppendBatch returns is relative to the wrong origin, so
	//     Append discards it (read ranges back through the Reader instead);
	//   * a record embedding an ALIGNED raw leaf would compute its pad from that wrong
	//     origin and misalign. SpanLog's leaf has no aligned field (its max alignment is
	//     1, so the generator emits no padding pass at all), but a schema that did would
	//     need the writer to be resumable for real — an Open{S}Writer over an existing
	//     stream, in the generator, rather than this wrapper.
	sw := spanlog.NewSpanLogStreamWriter(&skipFraming{to: f})
	if err := sw.EnsureFraming(); err != nil {
		f.Close()
		return nil, rec, err
	}
	return &Writer{f: f, w: sw, off: rec.ValidEnd}, rec, nil
}

// skipFraming drops the first Write (the framing word the stream is already carrying)
// and forwards every later one. The generated writer emits the framing as a single Write
// before any record, so "the first Write" is exactly the framing word and nothing else.
type skipFraming struct {
	to    io.Writer
	begun bool
}

func (s *skipFraming) Write(p []byte) (int, error) {
	if !s.begun {
		s.begun = true
		return len(p), nil
	}
	return s.to.Write(p)
}

// Append writes one batch: the encoded OtlpTraces buffer verbatim as the record's raw
// embedded leaf (spec 18 §11), plus the two envelope fields a reverse reader can answer
// questions from without opening the leaf at all.
func (w *Writer) Append(receivedUnixNano uint64, spanCount uint32, traces []byte) error {
	_, err := w.w.AppendBatch(spanlog.Batch{
		ReceivedUnixNano: receivedUnixNano,
		SpanCount:        spanCount,
		Traces:           traces,
	})
	if err != nil {
		return err
	}
	w.n++
	return nil
}

// Sync flushes the spool to stable storage. An exporter that promises durability calls
// this before acknowledging; one that only promises "survives a process crash" does not.
func (w *Writer) Sync() error { return w.f.Sync() }

func (w *Writer) Close() error { return w.f.Close() }

// Appended is how many records THIS writer added (not the spool's total).
func (w *Writer) Appended() int { return w.n }

// ── Reading ──────────────────────────────────────────────────────────────────

// Batch is one spool record: the envelope fields, and the embedded OTLP buffer as a
// sub-slice of the spool bytes — no copy, no decode.
type Batch struct {
	ReceivedUnixNano uint64
	SpanCount        uint32
	Traces           []byte
	Range            spanlog.SpanLogRecordRange
}

// Reader is a spool held in memory (the whole file; spools are rotated, not unbounded).
type Reader struct{ buf []byte }

// Open reads path and validates the stream framing.
func Open(path string) (*Reader, error) {
	buf, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return OpenBytes(buf)
}

// OpenBytes is Open over bytes already in hand.
func OpenBytes(buf []byte) (*Reader, error) {
	if _, err := spanlog.OpenSpanLogReader(buf); err != nil {
		return nil, err
	}
	return &Reader{buf: buf}, nil
}

// Bytes is the spool's raw content.
func (r *Reader) Bytes() []byte { return r.buf }

// Forward calls fn for every batch oldest-first, stopping early if fn returns false.
func (r *Reader) Forward(fn func(Batch) bool) error {
	rd, err := spanlog.OpenSpanLogReader(r.buf)
	if err != nil {
		return err
	}
	for {
		rec, ok := rd.Next()
		if !ok {
			return nil
		}
		b, ok := toBatch(rec)
		if !ok {
			continue // a typeId this build does not know: skipped for free (spec 11 §2)
		}
		if !fn(b) {
			return nil
		}
	}
}

// Reverse calls fn for every batch NEWEST-first, stopping early if fn returns false.
//
// This is the whole point of the doubly-linked sink: answering "the last N batches"
// touches N records, not the whole spool, with no index and no forward pass.
func (r *Reader) Reverse(fn func(Batch) bool) error {
	rd, err := spanlog.OpenSpanLogReverseReader(r.buf)
	if err != nil {
		return err
	}
	for {
		rec, ok := rd.Prev()
		if !ok {
			return nil
		}
		b, ok := toBatch(rec)
		if !ok {
			continue
		}
		if !fn(b) {
			return nil
		}
	}
}

// Last returns the newest n batches, newest first (fewer if the spool is shorter).
func (r *Reader) Last(n int) ([]Batch, error) {
	if n <= 0 {
		return nil, nil
	}
	out := make([]Batch, 0, n)
	err := r.Reverse(func(b Batch) bool {
		out = append(out, b)
		return len(out) < n
	})
	return out, err
}

// LastSpans walks backwards until it has accounted for at least n spans, and returns the
// batches that hold them, newest first — the "last N spans before the crash" query.
// Only the envelope's span_count is read; no embedded batch is opened.
func (r *Reader) LastSpans(n int) ([]Batch, int, error) {
	var out []Batch
	total := 0
	err := r.Reverse(func(b Batch) bool {
		out = append(out, b)
		total += int(b.SpanCount)
		return total < n
	})
	return out, total, err
}

func toBatch(rec spanlog.SpanLogRecord) (Batch, bool) {
	v, ok := rec.Batch()
	if !ok {
		return Batch{}, false
	}
	return Batch{
		ReceivedUnixNano: v.ReceivedUnixNano,
		SpanCount:        v.SpanCount,
		Traces:           v.Traces,
		Range:            rec.Range(),
	}, true
}

// ── Tail integrity ───────────────────────────────────────────────────────────

// Recovery is what a restart found at the spool's tail.
type Recovery struct {
	Fresh    bool  // the spool did not exist (or was empty) and was created
	ValidEnd int64 // one past the last intact record; the truncation point
	Torn     int64 // bytes dropped after ValidEnd
	Backward bool  // true when the answer came from the reverse walk alone (the fast path)
}

func (r Recovery) String() string {
	switch {
	case r.Fresh:
		return "fresh spool"
	case r.Torn == 0:
		return fmt.Sprintf("intact tail at %d bytes (%s)", r.ValidEnd, r.how())
	default:
		return fmt.Sprintf("torn tail: dropped %d bytes after %d (%s)", r.Torn, r.ValidEnd, r.how())
	}
}

func (r Recovery) how() string {
	if r.Backward {
		return "reverse walk"
	}
	return "forward rescan"
}

var errTorn = errors.New("spool: torn record")

// ValidateDepth is how many records back from EOF CheckTail validates before it accepts
// the tail.
//
// One record is not enough, and the reason is worth stating because it is easy to get
// wrong. Validating a single record asks: does the trailing RLEB name a span whose
// forward framing lands back on it? A torn write usually fails that — but not always.
// The RLEB's terminating byte only needs its high bit clear, and a short span only needs
// two LEB reads to agree, so arbitrary body bytes satisfy the cross-check by accident
// often enough to measure: over the ~8,000 distinct truncations of one 500-span batch,
// a depth-1 check accepted several of them as intact.
//
// Chaining the check fixes it. Each further step must land on a record whose own RLEB
// and forward framing agree with the step before, and a coincidence cannot survive that
// repeatedly. The walk still stops after a fixed number of records, so recovery stays
// O(1) in the spool's size — it never becomes the forward scan the doubly-linked span
// exists to avoid.
const ValidateDepth = 32

// CheckTail decides whether the spool's last record is whole, WITHOUT trusting it.
//
// The fast path walks backwards from EOF. Each step reads the trailing RLEB, steps back
// over the record it claims, and re-parses that record FORWARD: the span the RLEB
// asserts must equal the length the record's own typeId and content_size imply. It takes
// up to ValidateDepth such steps (fewer if the chain reaches the first record), and
// accepts the tail only if every one of them holds.
//
// Only when the chain breaks does the spool get scanned forward, to find the last offset
// where it did hold. That is a one-off cost on a crash, not the steady state.
func CheckTail(buf []byte) (Recovery, error) {
	rd, err := spanlog.OpenSpanLogReader(buf)
	if err != nil {
		return Recovery{}, err
	}
	start := rd.RecordsStart()
	if len(buf) == start {
		return Recovery{ValidEnd: int64(start), Backward: true}, nil
	}
	intact := true
	pos := len(buf)
	for depth := 0; depth < ValidateDepth && pos > start; depth++ {
		begin, err := stepBack(buf, pos, start)
		if err != nil {
			intact = false
			break
		}
		pos = begin
	}
	if intact {
		return Recovery{ValidEnd: int64(len(buf)), Backward: true}, nil
	}
	end, err := forwardEnd(buf, start)
	if err != nil {
		return Recovery{}, err
	}
	return Recovery{ValidEnd: int64(end), Torn: int64(len(buf) - end)}, nil
}

// Recover applies CheckTail to the file at path and truncates a torn tail away, so the
// spool that Resume then appends to is a single well-formed stream.
func Recover(path string) (Recovery, error) {
	buf, err := os.ReadFile(path)
	if err != nil {
		return Recovery{}, err
	}
	rec, err := CheckTail(buf)
	if err != nil {
		return rec, err
	}
	if rec.Torn > 0 {
		if err := os.Truncate(path, rec.ValidEnd); err != nil {
			return rec, err
		}
	}
	return rec, nil
}

// CountBackward walks the whole spool newest-first, validating every step, and returns
// the record count. Unlike Reverse it never trusts an RLEB it has not cross-checked, so
// it also answers "is this spool internally consistent end to end".
func CountBackward(buf []byte) (int, error) {
	rd, err := spanlog.OpenSpanLogReader(buf)
	if err != nil {
		return 0, err
	}
	start := rd.RecordsStart()
	n := 0
	for pos := len(buf); pos > start; n++ {
		begin, err := stepBack(buf, pos, start)
		if err != nil {
			return n, err
		}
		pos = begin
	}
	return n, nil
}

// stepBack validates the record ending at pos and returns its first byte.
func stepBack(buf []byte, pos, start int) (int, error) {
	span, n, ok := readRLEBBackward(buf, pos, start)
	if !ok {
		return 0, errTorn
	}
	bodyEnd := pos - n
	begin := bodyEnd - int(span)
	if begin < start || span == 0 {
		return 0, errTorn
	}
	end, err := forwardStep(buf, begin)
	if err != nil || end != bodyEnd {
		return 0, errTorn
	}
	return begin, nil
}

// forwardStep parses one record's framing at begin and returns the offset one past its
// body (i.e. before the trailing RLEB).
//
// It reads varints with encoding/binary rather than the Dagr runtime's ReadLEB, which
// panics on a truncated one — by construction this function is pointed at bytes that may
// be garbage, so "malformed" has to be a return value here, not a crash.
func forwardStep(buf []byte, begin int) (int, error) {
	_, tl, ok := uvarintAt(buf, begin)
	if !ok {
		return 0, errTorn
	}
	body := begin + tl
	sz, szl, ok := uvarintAt(buf, body)
	if !ok {
		return 0, errTorn
	}
	end := body + szl + int(sz)
	if end > len(buf) || end < body {
		return 0, errTorn
	}
	return end, nil
}

// uvarintAt is a bounds-safe LEB128 read: (value, byteLen, ok).
func uvarintAt(buf []byte, at int) (uint64, int, bool) {
	if at < 0 || at >= len(buf) {
		return 0, 0, false
	}
	v, n := binary.Uvarint(buf[at:])
	if n <= 0 {
		return 0, 0, false
	}
	return v, n, true
}

// forwardEnd scans from start and returns one past the last record that framed cleanly
// AND carried a matching RLEB — the truncation point for a torn spool.
func forwardEnd(buf []byte, start int) (int, error) {
	pos := start
	for pos < len(buf) {
		bodyEnd, err := forwardStep(buf, pos)
		if err != nil {
			break
		}
		span := uint64(bodyEnd - pos)
		rl := dagr.LebLength(span)
		if bodyEnd+rl > len(buf) {
			break
		}
		got, n, ok := readRLEBBackward(buf, bodyEnd+rl, pos)
		if !ok || n != rl || got != span {
			break
		}
		pos = bodyEnd + rl
	}
	return pos, nil
}

// readRLEBBackward decodes the reverse-delimited LEB whose LAST byte is at end-1
// (spec 11 §11.4: the standard LEB bytes in reversed order, so a right-to-left read
// meets the terminator first). It refuses to read below floor.
func readRLEBBackward(buf []byte, end, floor int) (uint64, int, bool) {
	var value uint64
	shift := 0
	n := 0
	for pos := end; pos > floor; {
		pos--
		b := buf[pos]
		value |= uint64(b&0x7f) << shift
		n++
		if b&0x80 == 0 {
			return value, n, true
		}
		if shift += 7; shift > 63 {
			return 0, n, false
		}
	}
	return 0, n, false
}
