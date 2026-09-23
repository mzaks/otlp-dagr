// Package spool is the file side of the four OTLP DataSinks (SpanLog, MetricLog,
// LogRecordLog, ProfileLog — one per Signal, see signal.go): create or resume a spool,
// append batches, read them back forward or newest-first, and — the part a write-ahead
// log lives or dies on — decide whether the tail is intact.
//
// The sink's wire shape (Dagr spec 11) is
//
//	[framing LEB] [SpoolHeader] ( [typeId LEB] [content_size LEB] [packed body] [span RLEB] )*
//
// (the header, header.go, says which signal the spool holds) and the trailing
// reverse-delimited LEB is there because `doubly_linked=True`: it holds
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

	"otlpgen/dagr"
)

// ── Writing ──────────────────────────────────────────────────────────────────

// Writer appends batches to a spool file. One record reaches the file in one Write, so
// a reader never sees a partial record except after a crash mid-syscall — which is
// exactly what CheckTail detects.
type Writer struct {
	f   *os.File
	w   framedWriter
	sig Signal
	hdr Header
	n   int   // records appended by THIS writer
	off int64 // file size when this writer opened it (0 for a fresh spool)
}

// Create starts a new spool for sig at path, truncating anything already there, under
// NewHeader(sig, "") — no producer named.
func Create(path string, sig Signal) (*Writer, error) {
	return CreateWith(path, NewHeader(sig, ""))
}

// CreateWith starts a new spool at path under header h (its Signal picks the sink).
func CreateWith(path string, h Header) (*Writer, error) {
	if err := h.Check(h.Signal); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return nil, err
	}
	w := &Writer{f: f, w: h.Signal.ops().newWriter(f, h), sig: h.Signal, hdr: h}
	// Write the framing word and header up front: a spool opened and not yet written to is still a
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
// A missing or empty file is not an error — Resume creates the spool, under
// NewHeader(sig, "").
func Resume(path string, sig Signal) (*Writer, Recovery, error) {
	return ResumeWith(path, NewHeader(sig, ""))
}

// ResumeWith is Resume that creates a missing spool under h. An existing spool keeps
// the header it was created with — only its signal has to match h's; a spool of another
// signal, or one without a header, is refused before anything is truncated.
func ResumeWith(path string, h Header) (*Writer, Recovery, error) {
	sig := h.Signal
	st, err := os.Stat(path)
	if errors.Is(err, os.ErrNotExist) || (err == nil && st.Size() == 0) {
		w, cErr := CreateWith(path, h)
		return w, Recovery{Fresh: true}, cErr
	}
	if err != nil {
		return nil, Recovery{}, err
	}
	onDisk, err := ReadHeaderFile(path)
	if errors.Is(err, ErrHeaderTruncated) {
		// Killed while writing the header: nothing after it can exist, so start over.
		w, cErr := CreateWith(path, h)
		return w, Recovery{Fresh: true, Torn: st.Size()}, cErr
	}
	if err != nil {
		return nil, Recovery{}, fmt.Errorf("%s: %w", path, err)
	}
	if err := onDisk.Check(sig); err != nil {
		return nil, Recovery{}, fmt.Errorf("%s: %w", path, err)
	}
	rec, err := Recover(path, sig)
	if err != nil {
		return nil, rec, err
	}
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, rec, err
	}
	// The stream is already framed on disk, header included. Hand the generated writer a
	// destination that swallows its framing write (the framing word and the header go out
	// as ONE Write) and passes every record through, so an appended record
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
	//     the appends discard it (read ranges back through the Reader instead);
	//   * a record embedding an ALIGNED raw leaf would compute its pad from that wrong
	//     origin and misalign. No OTLP leaf has an aligned field (its max alignment is
	//     1, so the generator emits no padding pass at all), but a schema that did would
	//     need the writer to be resumable for real — an Open{S}Writer over an existing
	//     stream, in the generator, rather than this wrapper.
	sw := sig.ops().newWriter(&skipFraming{to: f}, onDisk)
	if err := sw.EnsureFraming(); err != nil {
		f.Close()
		return nil, rec, err
	}
	return &Writer{f: f, w: sw, sig: sig, hdr: onDisk, off: rec.ValidEnd}, rec, nil
}

// skipFraming drops the first Write (the framing word and header the stream is already
// carrying) and forwards every later one. The generated writer emits both as a single
// Write before any record, so "the first Write" is exactly that and nothing else.
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

// AppendRecord writes one batch whose record content is already encoded — a
// Batch.Record read from a spool of the same signal (compaction copies records this way,
// with no decode), or a record a streaming encoder built directly
// (pdatabridge.StreamBuilder.AppendRecord). The typed Append{Signal} methods in
// signal.go write one from values.
func (w *Writer) AppendRecord(record []byte) error {
	return w.appended(w.w.appendRaw(record))
}

func (w *Writer) appended(err error) error {
	if err != nil {
		return err
	}
	w.n++
	return nil
}

func (w *Writer) wrongSignal(want Signal) error {
	return fmt.Errorf("spool: %s batch appended to a %s spool", want, w.sig)
}

// Sync flushes the spool to stable storage. An exporter that promises durability calls
// this before acknowledging; one that only promises "survives a process crash" does not.
func (w *Writer) Sync() error { return w.f.Sync() }

func (w *Writer) Close() error { return w.f.Close() }

// Appended is how many records THIS writer added (not the spool's total).
func (w *Writer) Appended() int { return w.n }

// Signal is the spool's signal.
func (w *Writer) Signal() Signal { return w.sig }

// Header is the spool's header — for a resumed spool, the one it was created with.
func (w *Writer) Header() Header { return w.hdr }

// ── Reading ──────────────────────────────────────────────────────────────────

// Batch is one spool record: the envelope fields, and the record itself as a sub-slice
// of the spool bytes — no copy, no decode. Record is the record's content (content_size
// LEB + packed body, what the generated Record.Raw returns): the signal's sink opens it
// in place (`spanlog.NewBatchAccessor(b.Record, 0)`), and so do the bridges
// (`pdatabridge.FromRecord`); AppendRecord writes it to another spool unchanged.
type Batch struct {
	ReceivedUnixNano uint64
	Count            uint32 // spans / data points / log records / samples
	Record           []byte
	Range            Range
}

// Reader is a spool held in memory (the whole file; spools are rotated, not unbounded).
type Reader struct {
	buf []byte
	sig Signal
	hdr Header
}

// Open reads path and validates the stream framing.
func Open(path string, sig Signal) (*Reader, error) {
	buf, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return OpenBytes(buf, sig)
}

// OpenBytes is Open over bytes already in hand. The header must name sig.
func OpenBytes(buf []byte, sig Signal) (*Reader, error) {
	h, _, err := ReadHeader(buf)
	if err != nil {
		return nil, err
	}
	if err := h.Check(sig); err != nil {
		return nil, err
	}
	return &Reader{buf: buf, sig: sig, hdr: h}, nil
}

// OpenAny opens a spool of whichever signal its header names.
func OpenAny(path string) (*Reader, error) {
	buf, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	h, _, err := ReadHeader(buf)
	if err != nil {
		return nil, err
	}
	return OpenBytes(buf, h.Signal)
}

// Signal is the spool's signal.
func (r *Reader) Signal() Signal { return r.sig }

// Header is the spool's header.
func (r *Reader) Header() Header { return r.hdr }

// Bytes is the spool's raw content.
func (r *Reader) Bytes() []byte { return r.buf }

// Forward calls fn for every batch oldest-first, stopping early if fn returns false.
// A record of a typeId this build does not know is skipped for free (spec 11 §2).
func (r *Reader) Forward(fn func(Batch) bool) error { return forward(r.sig, r.buf, fn) }

// Reverse calls fn for every batch NEWEST-first, stopping early if fn returns false.
//
// This is the whole point of the doubly-linked sink: answering "the last N batches"
// touches N records, not the whole spool, with no index and no forward pass.
func (r *Reader) Reverse(fn func(Batch) bool) error { return reverse(r.sig, r.buf, fn) }

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

// LastItems walks backwards until it has accounted for at least n items (spans, data
// points, log records or samples), and returns the batches that hold them, newest first
// — the "last N spans before the crash" query. Only the envelope's count is read; the
// batch itself is never touched.
func (r *Reader) LastItems(n int) ([]Batch, int, error) {
	var out []Batch
	total := 0
	err := r.Reverse(func(b Batch) bool {
		out = append(out, b)
		total += int(b.Count)
		return total < n
	})
	return out, total, err
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
	case r.Fresh && r.Torn > 0:
		return fmt.Sprintf("torn header: dropped %d bytes, spool recreated", r.Torn)
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
func CheckTail(buf []byte, sig Signal) (Recovery, error) {
	start, err := sig.ops().recordsStart(buf)
	if err != nil {
		return Recovery{}, err
	}
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
func Recover(path string, sig Signal) (Recovery, error) {
	buf, err := os.ReadFile(path)
	if err != nil {
		return Recovery{}, err
	}
	rec, err := CheckTail(buf, sig)
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
func CountBackward(buf []byte, sig Signal) (int, error) {
	start, err := sig.ops().recordsStart(buf)
	if err != nil {
		return 0, err
	}
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
