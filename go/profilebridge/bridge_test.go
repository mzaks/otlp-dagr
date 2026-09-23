package profilebridge_test

import (
	"bytes"
	"testing"

	"go.opentelemetry.io/collector/pdata/pprofile"

	"otlpdagr/internal/coverage"
	"otlpdagr/internal/fixture"
	"otlpdagr/profilebridge"
	d "otlpgen/otlpprofilesdirect"
)

func marshal(t *testing.T, pd pprofile.Profiles) []byte {
	t.Helper()
	b, err := (&pprofile.ProtoMarshaler{}).MarshalProfiles(pd)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestWideCoverage(t *testing.T) {
	coverage.Check(t, profilebridge.ToValue(fixture.ProfilesWide()), coverage.PdataInexpressible...)
}

func TestWideRoundTrip(t *testing.T) {
	in := fixture.ProfilesWide()
	out, err := profilebridge.ToProfiles(profilebridge.ToBytes(in))
	if err != nil {
		t.Fatal(err)
	}
	// entry 0 of every table is the mandatory zero value — the index space must not shift.
	// Checked BEFORE marshalling: pprofile's ProtoMarshaler mutates the batch it is
	// given (it moves attribute keys into the string table as key_strindex — 8 entries
	// become 24 here), so after marshal() neither side has the shape it was built with.
	dc := out.Dictionary()
	if dc.StringTable().At(0) != "" || dc.StringTable().Len() != 8 || dc.MappingTable().Len() != 2 ||
		dc.StackTable().Len() != 2 || dc.AttributeTable().Len() != 2 {
		t.Fatal("a dictionary table lost or shifted its zero entry")
	}
	if !bytes.Equal(marshal(t, in), marshal(t, out)) {
		t.Fatalf("profiles did not survive the round trip")
	}
}

func TestWideBytesStable(t *testing.T) {
	in := fixture.ProfilesWide()
	want := profilebridge.ToBytes(in)
	v, err := d.RestoreOtlpProfilesValues(want)
	if err != nil {
		t.Fatal(err)
	}
	if got := d.BuildOtlpProfiles(v); !bytes.Equal(got, want) {
		t.Fatalf("restore -> build is not byte-identical: %d B vs %d B", len(got), len(want))
	}
	w := profilebridge.NewBuilder()
	for i := 0; i < 3; i++ {
		if got := w.BuildAppend(nil, in); !bytes.Equal(got, want) {
			t.Fatalf("builder pass %d diverged", i)
		}
	}
}
