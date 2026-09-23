package profilebridge_test

import (
	"bytes"
	"testing"

	"go.opentelemetry.io/collector/pdata/pprofile"

	"otlpdagr/internal/fixture"
	"otlpdagr/profilebridge"
	"otlpgen/profilelog"
)

func streamCases() map[string]pprofile.Profiles {
	return map[string]pprofile.Profiles{
		"wide":  fixture.ProfilesWide(),
		"batch": fixture.ProfilesBatch(500, 100, 50),
		"empty": pprofile.NewProfiles(),
	}
}

// The one-pass encoder writes the containers by hand, so it is gated against the
// value-tree path byte for byte: the graph buffer (BuildAppend == ToBytes) and the spool
// record (AppendRecord == the generated ProfileLogWriter.AppendBatch).
func TestStreamMatches(t *testing.T) {
	enc := profilebridge.NewStreamBuilder()
	for name, pd := range streamCases() {
		if got, want := enc.BuildAppend(nil, pd), profilebridge.ToBytes(pd); !bytes.Equal(got, want) {
			t.Fatalf("%s: stream buffer %d bytes, value tree %d", name, len(got), len(want))
		}
		const ts = 1_758_000_000_000_000_000
		v := profilebridge.ToValue(pd)
		w := profilelog.NewProfileLogWriter(profilelog.SpoolHeader{})
		if _, err := w.AppendBatch(profilelog.Batch{ReceivedUnixNano: ts, SampleCount: uint32(pd.SampleCount()),
			ResourceProfiles: v.ResourceProfiles.V, Dictionary: v.Dictionary}); err != nil {
			t.Fatal(err)
		}
		rd, _ := profilelog.OpenProfileLogReader(w.Bytes())
		rec, _ := rd.Next()
		if got := enc.AppendRecord(nil, ts, pd); !bytes.Equal(got, rec.Raw()) {
			t.Fatalf("%s: stream record %d bytes, generated %d", name, len(got), len(rec.Raw()))
		}
	}
}

// Once its scratch has grown, the encoder allocates nothing.
func TestStreamAllocatesNothing(t *testing.T) {
	pd := fixture.ProfilesBatch(500, 100, 50)
	enc := profilebridge.NewStreamBuilder()
	dst := enc.AppendRecord(nil, 1, pd)
	if n := testing.AllocsPerRun(20, func() { dst = enc.AppendRecord(dst[:0], 1, pd) }); n != 0 {
		t.Fatalf("%.1f allocations per encode, want 0", n)
	}
}
