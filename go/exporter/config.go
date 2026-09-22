// Package dagrfileexporter is an OpenTelemetry Collector traces exporter that writes
// batches to an OTLP-on-Dagr spool: an append-only, doubly-linked DataSink file.
//
// The whole exporter is three steps — encode the batch with the direct builder, stamp an
// envelope, append one record — and the middle step is the interesting one. Dagr's
// direct builder (spec 33) walks the borrowed pdata accessors straight to bytes with no
// arena and no intermediate tree, which measured 1.50x protobuf's marshal in this repo's
// benchmark while allocating nothing per batch. The record stores that buffer VERBATIM
// as a `raw` embedded graph (spec 18 §11), so the exporter never re-encodes what it just
// encoded, and the bytes on disk are a genuine standalone Dagr buffer.
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
