package dagrfileexporter_test

import (
	"context"
	"os"
	"testing"

	"go.opentelemetry.io/collector/component/componenttest"

	dagrfileexporter "otlpdagr/exporter"
	"otlpdagr/internal/fixture"
	"otlpdagr/pdatabridge"
)

// BenchmarkConsumeTraces is the exporter's whole steady-state cost: encode the batch and
// append one record. It is measured against the encode step alone so the framing's share
// is visible rather than assumed — spec/38 §6.1.1 claims a steady-state export allocates
// nothing, and a claim like that is worth exactly what its benchmark says.
//
// The spool is /dev/null, so what is timed is the component and the sink framing, not
// the filesystem.
func BenchmarkConsumeTraces(b *testing.B) {
	td := fixture.TracesN(500)

	b.Run("encode", func(b *testing.B) {
		enc := pdatabridge.NewStreamBuilder()
		var dst []byte
		b.ReportAllocs()
		for b.Loop() {
			dst = enc.BuildAppend(dst[:0], td)
		}
		b.SetBytes(int64(len(dst)))
	})

	b.Run("encode+append", func(b *testing.B) {
		e := newExporter(b, &dagrfileexporter.Config{Path: os.DevNull})
		if err := e.Start(context.Background(), componenttest.NewNopHost()); err != nil {
			b.Fatal(err)
		}
		b.Cleanup(func() { _ = e.Shutdown(context.Background()) })
		ctx := context.Background()
		b.ReportAllocs()
		for b.Loop() {
			if err := e.ConsumeTraces(ctx, td); err != nil {
				b.Fatal(err)
			}
		}
	})
}
