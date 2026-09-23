package dagrfileexporter_test

import (
	"bytes"
	"context"
	"path/filepath"
	"reflect"
	"testing"

	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/exporter/exportertest"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/receiver"
	"go.opentelemetry.io/collector/receiver/receivertest"

	dagrfileexporter "otlpdagr/exporter"
	"otlpdagr/internal/fixture"
	"otlpdagr/pdatabridge"
	dagrfilereceiver "otlpdagr/receiver"
	"otlpdagr/spool"
)

func newExporter(t testing.TB, cfg *dagrfileexporter.Config) exporter.Traces {
	t.Helper()
	f := dagrfileexporter.NewFactory()
	e, err := f.CreateTraces(context.Background(), exportertest.NewNopSettings(dagrfileexporter.Type), cfg)
	if err != nil {
		t.Fatalf("create exporter: %v", err)
	}
	return e
}

// Gate 1 (spec/38 §6.1.1): what the exporter component writes is byte for byte what
// pdatabridge.StreamBuilder produces for the same batch. The record carries the batch as
// a `raw` embedded graph, so "frame and append" has to mean exactly that — if the
// exporter ever re-encoded, this is where it would show.
func TestExporterWritesTheBuilderOutputVerbatim(t *testing.T) {
	path := filepath.Join(t.TempDir(), "spans.dagr")
	e := newExporter(t, &dagrfileexporter.Config{Path: path})
	if err := e.Start(context.Background(), componenttest.NewNopHost()); err != nil {
		t.Fatalf("start: %v", err)
	}

	batches := []ptrace.Traces{fixture.TracesN(500), fixture.TracesN(3), fixture.TracesN(64)}
	want := make([][]byte, len(batches))
	ref := pdatabridge.NewStreamBuilder()
	for i, td := range batches {
		want[i] = ref.BuildAppend(nil, td)
		if err := e.ConsumeTraces(context.Background(), td); err != nil {
			t.Fatalf("consume %d: %v", i, err)
		}
	}
	if err := e.Shutdown(context.Background()); err != nil {
		t.Fatalf("shutdown: %v", err)
	}

	r, err := spool.Open(path, spool.Traces)
	if err != nil {
		t.Fatalf("open spool: %v", err)
	}
	i := 0
	if err := r.Forward(func(b spool.Batch) bool {
		if i >= len(want) {
			t.Fatalf("spool holds more than %d records", len(want))
		}
		if !bytes.Equal(b.Payload, want[i]) {
			t.Fatalf("record %d: %d bytes on disk, %d from the builder", i, len(b.Payload), len(want[i]))
		}
		if got, expect := b.Count, uint32(batches[i].SpanCount()); got != expect {
			t.Fatalf("record %d: envelope says %d spans, batch has %d", i, got, expect)
		}
		i++
		return true
	}); err != nil {
		t.Fatalf("forward: %v", err)
	}
	if i != len(want) {
		t.Fatalf("spool holds %d records, exported %d", i, len(want))
	}
	if rec, err := spool.CheckTail(r.Bytes(), spool.Traces); err != nil || rec.Torn != 0 {
		t.Fatalf("exporter left a torn spool: %s (%v)", rec, err)
	}
}

// Gate 2, at component level: exporter -> file -> receiver -> pipeline, and the spans
// that come out are the spans that went in. (The same path through a real ocb-built
// collector binary is gated by ocb/roundtrip.sh.)
func TestExporterReceiverRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "spans.dagr")
	// A batch with span links AND nested (Slice / Map) attribute values. The seeded
	// corpus has neither, and the encode path treats both separately from the flat
	// scalar case, so a round trip over the plain fixture would not notice an exporter
	// that dropped either one — which is exactly how the links gap survived this long.
	in := fixture.TracesWithLinks(250)
	fixture.AddNestedAttrs(in)

	e := newExporter(t, &dagrfileexporter.Config{Path: path, Sync: true})
	if err := e.Start(context.Background(), componenttest.NewNopHost()); err != nil {
		t.Fatalf("start exporter: %v", err)
	}
	if err := e.ConsumeTraces(context.Background(), in); err != nil {
		t.Fatalf("consume: %v", err)
	}
	if err := e.Shutdown(context.Background()); err != nil {
		t.Fatalf("shutdown exporter: %v", err)
	}

	sink := new(consumertest.TracesSink)
	rf := dagrfilereceiver.NewFactory()
	var rcv receiver.Traces
	rcv, err := rf.CreateTraces(context.Background(),
		receivertest.NewNopSettings(dagrfilereceiver.Type),
		&dagrfilereceiver.Config{Path: path}, sink)
	if err != nil {
		t.Fatalf("create receiver: %v", err)
	}
	if err := rcv.Start(context.Background(), componenttest.NewNopHost()); err != nil {
		t.Fatalf("start receiver: %v", err)
	}
	if err := rcv.Shutdown(context.Background()); err != nil {
		t.Fatalf("shutdown receiver: %v", err)
	}

	if got := len(sink.AllTraces()); got != 1 {
		t.Fatalf("receiver forwarded %d batches, want 1", got)
	}
	out := sink.AllTraces()[0]
	if out.SpanCount() != in.SpanCount() {
		t.Fatalf("round trip: %d spans out, %d in", out.SpanCount(), in.SpanCount())
	}
	assertSpansEqual(t, in, out)
	if n := countLinks(in); n == 0 {
		t.Fatal("the fixture carried no links — the link comparison would pass vacuously")
	}
}

// Resuming an existing spool across an exporter restart keeps one stream, and the second
// run's records are readable alongside the first's — restart recovery, end to end.
func TestExporterResumeAcrossRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "spans.dagr")
	cfg := &dagrfileexporter.Config{Path: path, Resume: true}

	for run := 0; run < 3; run++ {
		e := newExporter(t, cfg)
		if err := e.Start(context.Background(), componenttest.NewNopHost()); err != nil {
			t.Fatalf("start run %d: %v", run, err)
		}
		if err := e.ConsumeTraces(context.Background(), fixture.TracesN(10+run)); err != nil {
			t.Fatalf("consume run %d: %v", run, err)
		}
		if err := e.Shutdown(context.Background()); err != nil {
			t.Fatalf("shutdown run %d: %v", run, err)
		}
	}

	r, err := spool.Open(path, spool.Traces)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	var counts []uint32
	if err := r.Forward(func(b spool.Batch) bool {
		counts = append(counts, b.Count)
		return true
	}); err != nil {
		t.Fatalf("forward: %v", err)
	}
	if want := []uint32{10, 11, 12}; len(counts) != 3 || counts[0] != want[0] || counts[2] != want[2] {
		t.Fatalf("after three runs the spool holds %v, want %v", counts, want)
	}
	if n, err := spool.CountBackward(r.Bytes(), spool.Traces); err != nil || n != 3 {
		t.Fatalf("reverse count %d (%v), want 3", n, err)
	}
}

// Gate 3 at component level: a receiver configured `newest: true` drains the spool
// newest-first, and with a limit it touches only that many records — the LIFO drain
// policy spec/38 §6.1 argues for under backpressure.
func TestReceiverNewestFirstWithLimit(t *testing.T) {
	path := filepath.Join(t.TempDir(), "spans.dagr")
	e := newExporter(t, &dagrfileexporter.Config{Path: path})
	if err := e.Start(context.Background(), componenttest.NewNopHost()); err != nil {
		t.Fatalf("start: %v", err)
	}
	sizes := []int{4, 9, 16, 25}
	for _, n := range sizes {
		if err := e.ConsumeTraces(context.Background(), fixture.TracesN(n)); err != nil {
			t.Fatalf("consume: %v", err)
		}
	}
	if err := e.Shutdown(context.Background()); err != nil {
		t.Fatalf("shutdown: %v", err)
	}

	sink := new(consumertest.TracesSink)
	rf := dagrfilereceiver.NewFactory()
	rcv, err := rf.CreateTraces(context.Background(),
		receivertest.NewNopSettings(dagrfilereceiver.Type),
		&dagrfilereceiver.Config{Path: path, Newest: true, Limit: 2}, sink)
	if err != nil {
		t.Fatalf("create receiver: %v", err)
	}
	if err := rcv.Start(context.Background(), componenttest.NewNopHost()); err != nil {
		t.Fatalf("start receiver: %v", err)
	}
	if err := rcv.Shutdown(context.Background()); err != nil {
		t.Fatalf("shutdown receiver: %v", err)
	}

	got := sink.AllTraces()
	if len(got) != 2 {
		t.Fatalf("forwarded %d batches, want 2", len(got))
	}
	if got[0].SpanCount() != 25 || got[1].SpanCount() != 16 {
		t.Fatalf("newest-first gave %d then %d spans, want 25 then 16",
			got[0].SpanCount(), got[1].SpanCount())
	}
}

func assertSpansEqual(t *testing.T, want, got ptrace.Traces) {
	t.Helper()
	ws := flatten(want)
	gs := flatten(got)
	if len(ws) != len(gs) {
		t.Fatalf("%d spans out, %d in", len(gs), len(ws))
	}
	for i := range ws {
		w, g := ws[i], gs[i]
		if w.Name() != g.Name() || w.TraceID() != g.TraceID() || w.SpanID() != g.SpanID() ||
			w.ParentSpanID() != g.ParentSpanID() || w.Kind() != g.Kind() ||
			w.StartTimestamp() != g.StartTimestamp() || w.EndTimestamp() != g.EndTimestamp() ||
			w.Status().Code() != g.Status().Code() {
			t.Fatalf("span %d differs after the round trip: %q vs %q", i, w.Name(), g.Name())
		}
		if w.Attributes().Len() != g.Attributes().Len() {
			t.Fatalf("span %d: %d attributes out, %d in", i, g.Attributes().Len(), w.Attributes().Len())
		}
		for k, wv := range w.Attributes().All() {
			gv, ok := g.Attributes().Get(k)
			// DeepEqual, not !=: a Slice or Map attribute's AsRaw() is a []any / map,
			// and == on those panics at runtime rather than failing to compile.
			if !ok || !reflect.DeepEqual(gv.AsRaw(), wv.AsRaw()) {
				t.Fatalf("span %d attribute %q: %v vs %v", i, k, gv.AsRaw(), wv.AsRaw())
			}
		}
		if w.Links().Len() != g.Links().Len() {
			t.Fatalf("span %d: %d links out, %d in", i, g.Links().Len(), w.Links().Len())
		}
		for l := 0; l < w.Links().Len(); l++ {
			wl, gl := w.Links().At(l), g.Links().At(l)
			if wl.TraceID() != gl.TraceID() || wl.SpanID() != gl.SpanID() ||
				wl.Flags() != gl.Flags() || wl.TraceState().AsRaw() != gl.TraceState().AsRaw() ||
				wl.Attributes().Len() != gl.Attributes().Len() ||
				wl.DroppedAttributesCount() != gl.DroppedAttributesCount() {
				t.Fatalf("span %d link %d differs after the round trip", i, l)
			}
		}
		if w.Events().Len() != g.Events().Len() {
			t.Fatalf("span %d: %d events out, %d in", i, g.Events().Len(), w.Events().Len())
		}
		for e := 0; e < w.Events().Len(); e++ {
			if w.Events().At(e).Name() != g.Events().At(e).Name() {
				t.Fatalf("span %d event %d: %q vs %q", i, e,
					g.Events().At(e).Name(), w.Events().At(e).Name())
			}
		}
	}
}

func flatten(td ptrace.Traces) []ptrace.Span {
	var out []ptrace.Span
	for i := 0; i < td.ResourceSpans().Len(); i++ {
		rs := td.ResourceSpans().At(i)
		for j := 0; j < rs.ScopeSpans().Len(); j++ {
			ss := rs.ScopeSpans().At(j)
			for k := 0; k < ss.Spans().Len(); k++ {
				out = append(out, ss.Spans().At(k))
			}
		}
	}
	return out
}

func countLinks(td ptrace.Traces) int {
	n := 0
	for _, s := range flatten(td) {
		n += s.Links().Len()
	}
	return n
}
