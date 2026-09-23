package spool

// header.go — the spool header (schema.py `SpoolHeader`, Dagr spec 15, spec/38 D2).
//
// All four sinks declare the SAME header node, so its bytes do not depend on the signal:
// the framing word (0b11 — a DataSink with a header) is followed by one packed
// SpoolHeader, and only then by records. That is what lets ReadHeader decode any spool
// with one generated package before anything knows which signal it holds, and what makes
// a Header convertible to each package's own SpoolHeader type by a plain struct
// conversion.
//
// The header is written once, at stream open (spec 15 §9: metadata only). Resume keeps
// the one on disk and compaction copies it, so `CreatedUnixNano` is when the spool was
// FIRST created, not when its current file was written.

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	"otlpgen/dagr"
	"otlpgen/spanlog"
)

// FormatVersion is the receipt's `wire_format_version` (dagr.lock.json) this build writes
// and reads. A spool claiming a newer one is refused rather than misread.
const FormatVersion = 1

// OTLPProtoVersion names the vendored opentelemetry-proto the schema was imported from
// (proto/otlp/PROVENANCE.txt). Upstream was vendored from `main`, not a tag, so this is
// the branch plus the fetch date of the newest file; change it when re-vendoring.
const OTLPProtoVersion = "main@2026-09-23"

// Header is a spool's SpoolHeader in signal-neutral form. An absent optional string is "".
type Header struct {
	Signal           Signal
	SignalName       string // the embedded graph's name: "OtlpTraces", "OtlpMetrics", ...
	CreatedUnixNano  uint64
	FormatVersion    uint32
	OTLPProtoVersion string
	Producer         string
}

// NewHeader is the header a writer of this build puts on a new spool for sig.
func NewHeader(sig Signal, producer string) Header {
	return Header{
		Signal:           sig,
		SignalName:       sig.GraphName(),
		CreatedUnixNano:  uint64(time.Now().UnixNano()),
		FormatVersion:    FormatVersion,
		OTLPProtoVersion: OTLPProtoVersion,
		Producer:         producer,
	}
}

// GraphName is the Dagr graph a signal's spool records embed.
func (s Signal) GraphName() string {
	switch s {
	case Traces:
		return "OtlpTraces"
	case Metrics:
		return "OtlpMetrics"
	case Logs:
		return "OtlpLogs"
	case Profiles:
		return "OtlpProfiles"
	}
	return ""
}

func (h Header) String() string {
	s := fmt.Sprintf("%s spool (%s), format %d, created %s", h.Signal, h.SignalName, h.FormatVersion,
		time.Unix(0, int64(h.CreatedUnixNano)).UTC().Format(time.RFC3339Nano))
	if h.OTLPProtoVersion != "" {
		s += ", opentelemetry-proto " + h.OTLPProtoVersion
	}
	if h.Producer != "" {
		s += ", producer " + h.Producer
	}
	return s
}

// Check reports why a spool with this header cannot be read as sig by this build.
func (h Header) Check(sig Signal) error {
	if h.FormatVersion > FormatVersion {
		return fmt.Errorf("spool: format version %d, this build reads up to %d", h.FormatVersion, FormatVersion)
	}
	if h.Signal.GraphName() == "" || h.SignalName != h.Signal.GraphName() {
		return fmt.Errorf("spool: header names signal %d as %q — not a signal this build knows", uint8(h.Signal), h.SignalName)
	}
	if h.Signal != sig {
		return fmt.Errorf("spool: holds %s, opened as %s", h.Signal, sig)
	}
	return nil
}

// ErrNoHeader is a spool written before the header existed (or not a spool at all).
var ErrNoHeader = errors.New("spool: no SpoolHeader — written before spec/38 D2, or not a spool")

// ErrHeaderTruncated is a spool that ends inside its header: a Create killed before its
// first Write finished. No record can follow a header that is not whole, so Resume
// recreates such a spool (and reports the bytes as Torn).
var ErrHeaderTruncated = errors.New("spool: header truncated")

// ReadHeader decodes the header at the front of a spool's bytes, and returns where the
// records start. It never panics on bytes that end inside the header — a Create killed
// mid-write — but reports them.
func ReadHeader(buf []byte) (Header, int, error) {
	framing, fl, ok := uvarintAt(buf, 0)
	if !ok {
		return Header{}, 0, ErrHeaderTruncated
	}
	if (framing>>1)&1 != 1 {
		return Header{}, 0, errors.New("spool: not a DataSink stream")
	}
	if framing&1 != 1 {
		return Header{}, 0, ErrNoHeader
	}
	size, sl, ok := uvarintAt(buf, fl)
	if !ok || fl+sl+int(size) > len(buf) || fl+sl+int(size) < fl {
		return Header{}, 0, ErrHeaderTruncated
	}
	h, err := decodeHeader(buf[:fl+sl+int(size)], fl)
	return h, fl + sl + int(size), err
}

// decodeHeader reads the SpoolHeader at pos. buf ends where the header does, so a
// malformed field runs off the end of it; the generated accessor reports that by
// panicking, which is turned into an error here.
func decodeHeader(buf []byte, pos int) (h Header, err error) {
	defer func() {
		if recover() != nil {
			err = errors.New("spool: malformed header")
		}
	}()
	a := spanlog.NewSpoolHeaderAccessor(buf, pos)
	h = Header{
		Signal:          Signal(a.Signal()),
		SignalName:      a.SignalName(),
		CreatedUnixNano: a.CreatedUnixNano(),
		FormatVersion:   a.FormatVersion(),
	}
	h.OTLPProtoVersion, _ = a.OtlpProtoVersion()
	h.Producer, _ = a.Producer()
	return h, nil
}

// ReadHeaderFile reads only the header of the spool at path — a few dozen bytes, however
// large the spool is.
func ReadHeaderFile(path string) (Header, error) {
	f, err := os.Open(path)
	if err != nil {
		return Header{}, err
	}
	defer f.Close()
	br := bufio.NewReader(f)
	var prefix []byte
	var size uint64
	for i := range 2 { // the framing word, then the header's size
		v, err := binary.ReadUvarint(br)
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return Header{}, ErrHeaderTruncated
		}
		if err != nil {
			return Header{}, fmt.Errorf("spool: reading header: %w", err)
		}
		prefix = binary.AppendUvarint(prefix, v)
		if i == 0 && v&3 != 3 { // not a DataSink with a header: ReadHeader names which
			_, _, err := ReadHeader(prefix)
			return Header{}, err
		}
		size = v
	}
	if size > 1<<20 {
		return Header{}, errors.New("spool: header size implausible")
	}
	buf := append(prefix, make([]byte, size)...)
	if _, err := io.ReadFull(br, buf[len(prefix):]); err != nil {
		return Header{}, ErrHeaderTruncated
	}
	h, _, err := ReadHeader(buf)
	return h, err
}

// dagrHeader is the header as the generated packages spell it. Every sink's SpoolHeader
// has these fields in this order, so a struct conversion turns it into any of them.
func (h Header) dagrHeader() spanlog.SpoolHeader {
	v := spanlog.SpoolHeader{
		Signal:          uint8(h.Signal),
		SignalName:      h.SignalName,
		CreatedUnixNano: h.CreatedUnixNano,
		FormatVersion:   h.FormatVersion,
	}
	if h.OTLPProtoVersion != "" {
		v.OtlpProtoVersion = dagr.Some(h.OTLPProtoVersion)
	}
	if h.Producer != "" {
		v.Producer = dagr.Some(h.Producer)
	}
	return v
}
