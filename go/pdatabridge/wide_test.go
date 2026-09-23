package pdatabridge_test

import (
	"bytes"
	"testing"

	"go.opentelemetry.io/collector/pdata/ptrace"

	"otlpdagr/internal/coverage"
	"otlpdagr/internal/fixture"
	"otlpdagr/pdatabridge"
)

// The traces wide batch: every field, through all three encoders (value tree, reused
// scratch, one-pass stream) — which must agree byte for byte — and back.
//
// This is the gate that did not exist when every encoder dropped the scope's attributes
// and the resource's dropped count: the seed sets neither, so a round trip of the seed
// could not see it.
func TestWideCoverage(t *testing.T) {
	coverage.Check(t, pdatabridge.ToValue(fixture.TracesWide()), coverage.PdataInexpressible...)
}

func TestWideAllEncodersAgree(t *testing.T) {
	in := fixture.TracesWide()
	want := pdatabridge.ToBytes(in)
	for name, got := range map[string][]byte{
		"builder": pdatabridge.NewBuilder().BuildAppend(nil, in),
		"stream":  pdatabridge.NewStreamBuilder().BuildAppend(nil, in),
	} {
		if !bytes.Equal(got, want) {
			t.Errorf("%s encoder diverged from the value tree: %d B vs %d B", name, len(got), len(want))
		}
	}
	out, err := pdatabridge.ToTraces(want)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(marshalW(t, in), marshalW(t, out)) {
		t.Fatal("wide traces did not survive the round trip")
	}
}

func marshalW(t *testing.T, td ptrace.Traces) []byte {
	t.Helper()
	b, err := (&ptrace.ProtoMarshaler{}).MarshalTraces(td)
	if err != nil {
		t.Fatal(err)
	}
	return b
}
