package dagrfileexporter_test

import (
	"bytes"
	"context"
	"path/filepath"
	"testing"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/exporter/exportertest"
	"go.opentelemetry.io/collector/exporter/xexporter"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/pprofile"
	"go.opentelemetry.io/collector/receiver/receivertest"
	"go.opentelemetry.io/collector/receiver/xreceiver"

	dagrfileexporter "otlpdagr/exporter"
	"otlpdagr/internal/fixture"
	dagrfilereceiver "otlpdagr/receiver"
)

// The component round trip for metrics, logs and profiles: the `dagrfile` exporter
// writes each signal's WIDE batch through the collector's own factory API, and the
// `dagrfile` receiver hands it back to a consumertest sink. Compared as marshalled OTLP
// protobuf, so equality is OTLP's, not this repo's.

func lifecycle(t *testing.T, c component.Component, run func()) {
	t.Helper()
	if err := c.Start(context.Background(), componenttest.NewNopHost()); err != nil {
		t.Fatalf("start: %v", err)
	}
	run()
	if err := c.Shutdown(context.Background()); err != nil {
		t.Fatalf("shutdown: %v", err)
	}
}

func TestMetricsComponentRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "metrics.dagr")
	in := fixture.MetricsWide()
	e, err := dagrfileexporter.NewFactory().CreateMetrics(context.Background(),
		exportertest.NewNopSettings(dagrfileexporter.Type), &dagrfileexporter.Config{Path: path})
	if err != nil {
		t.Fatal(err)
	}
	lifecycle(t, e, func() {
		if err := e.ConsumeMetrics(context.Background(), in); err != nil {
			t.Fatal(err)
		}
	})
	sink := new(consumertest.MetricsSink)
	r, err := dagrfilereceiver.NewFactory().CreateMetrics(context.Background(),
		receivertest.NewNopSettings(dagrfilereceiver.Type), &dagrfilereceiver.Config{Path: path}, sink)
	if err != nil {
		t.Fatal(err)
	}
	lifecycle(t, r, func() {})
	if len(sink.AllMetrics()) != 1 {
		t.Fatalf("receiver forwarded %d batches, want 1", len(sink.AllMetrics()))
	}
	m := &pmetric.ProtoMarshaler{}
	a, _ := m.MarshalMetrics(in)
	b, _ := m.MarshalMetrics(sink.AllMetrics()[0])
	if !bytes.Equal(a, b) {
		t.Fatal("metrics changed through exporter -> spool -> receiver")
	}
}

func TestLogsComponentRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "logs.dagr")
	in := fixture.LogsWide()
	e, err := dagrfileexporter.NewFactory().CreateLogs(context.Background(),
		exportertest.NewNopSettings(dagrfileexporter.Type), &dagrfileexporter.Config{Path: path})
	if err != nil {
		t.Fatal(err)
	}
	lifecycle(t, e, func() {
		if err := e.ConsumeLogs(context.Background(), in); err != nil {
			t.Fatal(err)
		}
	})
	sink := new(consumertest.LogsSink)
	r, err := dagrfilereceiver.NewFactory().CreateLogs(context.Background(),
		receivertest.NewNopSettings(dagrfilereceiver.Type), &dagrfilereceiver.Config{Path: path}, sink)
	if err != nil {
		t.Fatal(err)
	}
	lifecycle(t, r, func() {})
	if len(sink.AllLogs()) != 1 {
		t.Fatalf("receiver forwarded %d batches, want 1", len(sink.AllLogs()))
	}
	m := &plog.ProtoMarshaler{}
	a, _ := m.MarshalLogs(in)
	b, _ := m.MarshalLogs(sink.AllLogs()[0])
	if !bytes.Equal(a, b) {
		t.Fatal("logs changed through exporter -> spool -> receiver")
	}
}

func TestProfilesComponentRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "profiles.dagr")
	in := fixture.ProfilesWide()
	ef := dagrfileexporter.NewFactory().(xexporter.Factory)
	e, err := ef.CreateProfiles(context.Background(),
		exportertest.NewNopSettings(dagrfileexporter.Type), &dagrfileexporter.Config{Path: path})
	if err != nil {
		t.Fatal(err)
	}
	lifecycle(t, e, func() {
		if err := e.ConsumeProfiles(context.Background(), in); err != nil {
			t.Fatal(err)
		}
	})
	sink := new(consumertest.ProfilesSink)
	rf := dagrfilereceiver.NewFactory().(xreceiver.Factory)
	r, err := rf.CreateProfiles(context.Background(),
		receivertest.NewNopSettings(dagrfilereceiver.Type), &dagrfilereceiver.Config{Path: path}, sink)
	if err != nil {
		t.Fatal(err)
	}
	lifecycle(t, r, func() {})
	if len(sink.AllProfiles()) != 1 {
		t.Fatalf("receiver forwarded %d batches, want 1", len(sink.AllProfiles()))
	}
	// pprofile's marshaler mutates its input (attribute keys move into the string table),
	// so each side is marshalled from a fresh copy.
	m := &pprofile.ProtoMarshaler{}
	a, _ := m.MarshalProfiles(fixture.ProfilesWide())
	b, _ := m.MarshalProfiles(sink.AllProfiles()[0])
	if !bytes.Equal(a, b) {
		t.Fatal("profiles changed through exporter -> spool -> receiver")
	}
}
