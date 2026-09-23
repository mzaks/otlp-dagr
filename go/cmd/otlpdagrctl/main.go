// Command otlpdagrctl drives and checks the round trip through a real, ocb-built
// collector (spec/38 §6.1.1, gate 2). It is a test harness, not part of the components.
//
//	otlpdagrctl send   -endpoint URL -spans N      send one batch over OTLP/HTTP
//	otlpdagrctl spool  -path FILE [-spans N]       report and CHECK a spool
//	otlpdagrctl verify -path FILE -spans N         compare a file-exporter dump to the sent batch
//	otlpdagrctl fixtures -out DIR                  write the OTAP gate's inputs (.dagr + .pb)
//
// `-signal metrics|logs|profiles` switches send / spool / verify to that signal; send
// then posts the signal's WIDE fixture (every field set) to its OTLP/HTTP path, and
// verify compares every dumped request with it as marshalled protobuf. Traces keeps the
// benchmark batch and the per-span digest.
package main

import (
	"bufio"
	"bytes"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"sort"
	"time"

	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/plog/plogotlp"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/pmetric/pmetricotlp"
	"go.opentelemetry.io/collector/pdata/pprofile"
	"go.opentelemetry.io/collector/pdata/pprofile/pprofileotlp"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/pdata/ptrace/ptraceotlp"

	"otlpdagr/internal/fixture"
	"otlpdagr/spool"
)

func main() {
	if len(os.Args) < 2 {
		fail(errors.New("usage: otlpdagrctl send|spool|verify|fixtures [flags]"))
	}
	var err error
	switch os.Args[1] {
	case "send":
		err = send(os.Args[2:])
	case "spool":
		err = checkSpool(os.Args[2:])
	case "verify":
		err = verify(os.Args[2:])
	case "fixtures":
		err = fixtures(os.Args[2:])
	default:
		err = fmt.Errorf("unknown command %q", os.Args[1])
	}
	if err != nil {
		fail(err)
	}
}

func fail(err error) {
	fmt.Fprintln(os.Stderr, "otlpdagrctl:", err)
	os.Exit(1)
}

// send posts the deterministic fixture batch to a collector's OTLP/HTTP endpoint. The
// same fixture backs the benchmarks, so what crosses the wire here is the batch every
// number in the README was measured on.
func send(args []string) error {
	fs := flag.NewFlagSet("send", flag.ExitOnError)
	sigName := fs.String("signal", "traces", "traces | metrics | logs | profiles")
	base := fs.String("base", "http://127.0.0.1:4318", "OTLP/HTTP base URL")
	endpoint := fs.String("endpoint", "", "full endpoint (default: base + the signal's path)")
	spans := fs.Int("spans", 500, "spans per batch (traces)")
	batches := fs.Int("batches", 1, "how many batches to send")
	retry := fs.Duration("retry", 20*time.Second, "keep retrying until the collector is up")
	_ = fs.Parse(args)
	sig, err := parseSignal(*sigName)
	if err != nil {
		return err
	}
	if sig != spool.Traces {
		body, path, err := wideRequest(sig)
		if err != nil {
			return err
		}
		if *endpoint == "" {
			*endpoint = *base + path
		}
		for i := 0; i < *batches; i++ {
			if err := post(*endpoint, body, *retry); err != nil {
				return err
			}
		}
		fmt.Printf("sent %d wide %s batches to %s\n", *batches, sig, *endpoint)
		return nil
	}
	if *endpoint == "" {
		*endpoint = *base + "/v1/traces"
	}

	body, err := ptraceotlp.NewExportRequestFromTraces(fixture.TracesN(*spans)).MarshalProto()
	if err != nil {
		return err
	}
	for i := 0; i < *batches; i++ {
		if err := post(*endpoint, body, *retry); err != nil {
			return err
		}
	}
	fmt.Printf("sent %d batches of %d spans to %s\n", *batches, *spans, *endpoint)
	return nil
}

// post retries until the collector is listening — ocb starts the binary and the OTLP
// port a moment later, and the alternative is a sleep that is either flaky or slow.
func post(endpoint string, body []byte, retry time.Duration) error {
	deadline := time.Now().Add(retry)
	for {
		resp, err := http.Post(endpoint, "application/x-protobuf", bytes.NewReader(body))
		if err == nil {
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			if resp.StatusCode/100 == 2 {
				return nil
			}
			err = fmt.Errorf("%s", resp.Status)
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("sending to %s: %w", endpoint, err)
		}
		time.Sleep(250 * time.Millisecond)
	}
}

// checkSpool reports what a spool produced by a real collector holds, and gates the two
// properties that the doubly-linked sink exists for: the reverse walk agrees with the
// forward one, and the tail is intact.
func checkSpool(args []string) error {
	fs := flag.NewFlagSet("spool", flag.ExitOnError)
	sigName := fs.String("signal", "", "traces | metrics | logs | profiles (default: the spool header's)")
	path := fs.String("path", "", "spool file")
	spans := fs.Int("spans", 0, "expected total spans (0 = do not check)")
	batches := fs.Int("batches", 0, "expected batch count (0 = do not check)")
	_ = fs.Parse(args)
	if *path == "" {
		return errors.New("spool: -path is required")
	}
	var r *spool.Reader
	var err error
	if *sigName == "" {
		r, err = spool.OpenAny(*path)
	} else {
		var sig spool.Signal
		if sig, err = parseSignal(*sigName); err != nil {
			return err
		}
		r, err = spool.Open(*path, sig)
	}
	if err != nil {
		return err
	}
	sig := r.Signal()

	// The tail first: a spool left by a killed exporter can end mid-record, and every
	// count taken before that is known would be a count of an unknown prefix.
	tail, err := spool.CheckTail(r.Bytes(), sig)
	if err != nil {
		return err
	}
	if tail.Torn != 0 {
		return fmt.Errorf("spool tail is torn: %s", tail)
	}

	fwdBatches, fwdSpans := 0, 0
	if err := r.Forward(func(b spool.Batch) bool {
		fwdBatches++
		fwdSpans += int(b.Count)
		return true
	}); err != nil {
		return err
	}

	// The reverse pass never opens a batch: it reads the trailing span, steps back, and
	// reads two packed scalars. "The newest N" costs N records, not the file.
	revBatches, revSpans := 0, 0
	if err := r.Reverse(func(b spool.Batch) bool {
		revBatches++
		revSpans += int(b.Count)
		return true
	}); err != nil {
		return err
	}
	if revBatches != fwdBatches || revSpans != fwdSpans {
		return fmt.Errorf("reverse walk saw %d batches / %d spans, forward saw %d / %d",
			revBatches, revSpans, fwdBatches, fwdSpans)
	}
	chained, err := spool.CountBackward(r.Bytes(), sig)
	if err != nil {
		return fmt.Errorf("reverse chain broken after %d records: %w", chained, err)
	}
	if chained != fwdBatches {
		return fmt.Errorf("validated reverse chain has %d records, forward has %d", chained, fwdBatches)
	}

	newest, newestSpans, err := r.LastItems(1)
	if err != nil {
		return err
	}
	fmt.Printf("%s spool %s: %d bytes, %d batches, %d items, %s\n",
		sig, *path, len(r.Bytes()), fwdBatches, fwdSpans, tail)
	fmt.Printf("  header: %s\n", r.Header())
	if len(newest) > 0 {
		fmt.Printf("  newest batch: %d items, received %s, %d record bytes\n",
			newestSpans, time.Unix(0, int64(newest[0].ReceivedUnixNano)).Format(time.RFC3339Nano),
			len(newest[0].Record))
	}
	if *batches != 0 && fwdBatches != *batches {
		return fmt.Errorf("spool holds %d batches, expected %d", fwdBatches, *batches)
	}
	if *spans != 0 && fwdSpans != *spans {
		return fmt.Errorf("spool holds %d spans, expected %d", fwdSpans, *spans)
	}
	return nil
}

// verify compares a file exporter's JSON dump — what came out of the FAR end of the
// pipeline, after the dagrfile receiver materialised it back into pdata — against the
// batch that was sent in.
func verify(args []string) error {
	fs := flag.NewFlagSet("verify", flag.ExitOnError)
	sigName := fs.String("signal", "traces", "traces | metrics | logs | profiles")
	path := fs.String("path", "", "file exporter output (OTLP JSON, one request per line)")
	spans := fs.Int("spans", 500, "spans per batch that was sent")
	batches := fs.Int("batches", 1, "how many batches were sent")
	_ = fs.Parse(args)
	if *path == "" {
		return errors.New("verify: -path is required")
	}
	sig, err := parseSignal(*sigName)
	if err != nil {
		return err
	}
	if sig != spool.Traces {
		return verifyWide(sig, *path, *batches)
	}

	f, err := os.Open(*path)
	if err != nil {
		return err
	}
	defer f.Close()

	got := ptrace.NewTraces()
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 1<<20), 1<<28)
	for sc.Scan() {
		line := bytes.TrimSpace(sc.Bytes())
		if len(line) == 0 {
			continue
		}
		req := ptraceotlp.NewExportRequest()
		if err := req.UnmarshalJSON(line); err != nil {
			return fmt.Errorf("parsing %s: %w", *path, err)
		}
		req.Traces().ResourceSpans().MoveAndAppendTo(got.ResourceSpans())
	}
	if err := sc.Err(); err != nil {
		return err
	}

	// Every batch carries the same fixture, so the expected digest is the fixture's,
	// repeated — and because both sides are sorted, duplicate spans line up exactly.
	one := digest(fixture.TracesN(*spans))
	ws := make([]string, 0, len(one)**batches)
	for i := 0; i < *batches; i++ {
		ws = append(ws, one...)
	}
	sort.Strings(ws)
	gs := digest(got)
	if len(gs) != len(ws) {
		return fmt.Errorf("%d spans came out, %d went in", len(gs), len(ws))
	}
	for i := range ws {
		if ws[i] != gs[i] {
			return fmt.Errorf("span %d differs after the round trip:\n  in:  %s\n  out: %s", i, ws[i], gs[i])
		}
	}
	fmt.Printf("verified %d spans through the collector round trip\n", got.SpanCount())
	return nil
}

// digest reduces a batch to one sorted comparable line per span: identity, timing, kind,
// status, every attribute, and every event.
func digest(td ptrace.Traces) []string {
	var out []string
	for i := 0; i < td.ResourceSpans().Len(); i++ {
		rs := td.ResourceSpans().At(i)
		for j := 0; j < rs.ScopeSpans().Len(); j++ {
			ss := rs.ScopeSpans().At(j)
			for k := 0; k < ss.Spans().Len(); k++ {
				s := ss.Spans().At(k)
				var attrs []string
				for key, v := range s.Attributes().All() {
					attrs = append(attrs, fmt.Sprintf("%s=%v", key, v.AsRaw()))
				}
				sort.Strings(attrs)
				var events []string
				for e := 0; e < s.Events().Len(); e++ {
					ev := s.Events().At(e)
					events = append(events, fmt.Sprintf("%s@%d", ev.Name(), ev.Timestamp()))
				}
				out = append(out, fmt.Sprintf("%x/%x parent=%x %q kind=%d %d-%d status=%d attrs=%v events=%v",
					s.TraceID(), s.SpanID(), s.ParentSpanID(), s.Name(), s.Kind(),
					s.StartTimestamp(), s.EndTimestamp(), s.Status().Code(), attrs, events))
			}
		}
	}
	sort.Strings(out)
	return out
}

func parseSignal(name string) (spool.Signal, error) {
	for _, s := range []spool.Signal{spool.Traces, spool.Metrics, spool.Logs, spool.Profiles} {
		if s.String() == name {
			return s, nil
		}
	}
	return 0, fmt.Errorf("unknown signal %q", name)
}

// wideRequest is the OTLP/HTTP body for a signal's wide fixture, and its URL path.
func wideRequest(sig spool.Signal) ([]byte, string, error) {
	switch sig {
	case spool.Metrics:
		b, err := pmetricotlp.NewExportRequestFromMetrics(fixture.MetricsWide()).MarshalProto()
		return b, "/v1/metrics", err
	case spool.Logs:
		b, err := plogotlp.NewExportRequestFromLogs(fixture.LogsWide()).MarshalProto()
		return b, "/v1/logs", err
	case spool.Profiles:
		b, err := pprofileotlp.NewExportRequestFromProfiles(fixture.ProfilesWide()).MarshalProto()
		return b, "/v1development/profiles", err
	}
	return nil, "", fmt.Errorf("no wide request for %s", sig)
}

// wideProto is the fixture as marshalled protobuf — the comparison form. Built fresh on
// every call: pprofile's marshaler mutates the batch it is handed.
func wideProto(sig spool.Signal) ([]byte, error) {
	switch sig {
	case spool.Metrics:
		return (&pmetric.ProtoMarshaler{}).MarshalMetrics(fixture.MetricsWide())
	case spool.Logs:
		return (&plog.ProtoMarshaler{}).MarshalLogs(fixture.LogsWide())
	case spool.Profiles:
		return (&pprofile.ProtoMarshaler{}).MarshalProfiles(fixture.ProfilesWide())
	}
	return nil, fmt.Errorf("no wide fixture for %s", sig)
}

// jsonLineToProto parses one file-exporter line (an OTLP/JSON export request) and
// re-marshals its payload as protobuf.
func jsonLineToProto(sig spool.Signal, line []byte) ([]byte, error) {
	switch sig {
	case spool.Metrics:
		req := pmetricotlp.NewExportRequest()
		if err := req.UnmarshalJSON(line); err != nil {
			return nil, err
		}
		return (&pmetric.ProtoMarshaler{}).MarshalMetrics(req.Metrics())
	case spool.Logs:
		req := plogotlp.NewExportRequest()
		if err := req.UnmarshalJSON(line); err != nil {
			return nil, err
		}
		return (&plog.ProtoMarshaler{}).MarshalLogs(req.Logs())
	case spool.Profiles:
		req := pprofileotlp.NewExportRequest()
		if err := req.UnmarshalJSON(line); err != nil {
			return nil, err
		}
		return (&pprofile.ProtoMarshaler{}).MarshalProfiles(req.Profiles())
	}
	return nil, fmt.Errorf("no JSON form for %s", sig)
}

// verifyWide checks that every request the far end of the pipeline wrote is the wide
// fixture that went in — OTLP in, Dagr spool, dagrfile receiver, file exporter out.
func verifyWide(sig spool.Signal, path string, batches int) error {
	want, err := wideProto(sig)
	if err != nil {
		return err
	}
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 1<<20), 1<<28)
	n := 0
	for sc.Scan() {
		line := bytes.TrimSpace(sc.Bytes())
		if len(line) == 0 {
			continue
		}
		got, err := jsonLineToProto(sig, line)
		if err != nil {
			return fmt.Errorf("parsing %s: %w", path, err)
		}
		if !bytes.Equal(got, want) {
			return fmt.Errorf("%s request %d differs from the batch that was sent", sig, n)
		}
		n++
	}
	if err := sc.Err(); err != nil {
		return err
	}
	if n != batches {
		return fmt.Errorf("%d %s requests came out, %d went in", n, sig, batches)
	}
	fmt.Printf("verified %d wide %s batches through the collector round trip\n", n, sig)
	return nil
}
