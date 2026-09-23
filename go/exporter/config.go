// Package dagrfileexporter is an OpenTelemetry Collector exporter (traces, metrics, logs,
// profiles) that writes
// batches to an OTLP-on-Dagr spool: an append-only, doubly-linked DataSink file.
//
// The whole exporter is one step: encode the batch into a spool record and append it.
// The record holds the batch's top-level array (ResourceSpans, ...) inline, written with
// the signal graph's own generated code, so those bytes are exactly what the graph's
// direct builder (spec 33) writes. Traces go through a one-pass StreamBuilder that walks
// the borrowed pdata accessors straight into the record with no arena and no
// intermediate tree — the path that measured 1.50x protobuf's marshal while allocating
// nothing per batch; the other signals build a value tree first.
//
// It deliberately does not wrap itself in exporterhelper's queue and retry machinery.
// The spool IS the queue — that is the point of spec/38 §6.1 — and putting a second,
// in-memory queue in front of it would only obscure what is being demonstrated.
package dagrfileexporter

import (
	"errors"

	"go.opentelemetry.io/collector/component"
)

// Type is the component type as it appears in a collector config: `dagrfile`.
var Type = component.MustNewType("dagrfile")

// Config is the exporter's YAML surface.
type Config struct {
	// Path is the spool file. Required.
	Path string `mapstructure:"path"`

	// Resume appends to an existing spool instead of truncating it, after recovering
	// its tail (spec/38 §6.1: restart recovery walks backwards from EOF). This is the
	// setting that makes the spool a write-ahead log rather than a per-run dump.
	Resume bool `mapstructure:"resume"`

	// Sync fsyncs after every batch. Off by default: without it a batch survives a
	// process crash but not a machine one, which is the same promise the collector's
	// own file storage makes by default.
	Sync bool `mapstructure:"sync"`
}

func (c *Config) Validate() error {
	if c.Path == "" {
		return errors.New("dagrfile: `path` is required")
	}
	return nil
}

func createDefaultConfig() component.Config { return &Config{} }
