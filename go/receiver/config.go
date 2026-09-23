// Package dagrfilereceiver is an OpenTelemetry Collector receiver (traces, metrics, logs,
// profiles) that reads an
// OTLP-on-Dagr spool and feeds it into an ordinary pipeline.
//
// It exists so an UNMODIFIED collector can consume a spool, and that makes it the slow
// path on purpose. Every batch it forwards is materialised into pdata, which spec/38
// §0.1 measures at ~1.6x slower than a protobuf unmarshal — eager restore is the one
// dimension where Dagr loses, and this receiver is built entirely out of it.
//
// Do not benchmark otlp-dagr through this component and conclude anything about Dagr.
// The fast path is the opposite shape: keep the buffer and read the fields you need in
// place, which is 29x a protobuf decode and allocates nothing (spec/38 §4, §5.1). A
// pipeline that wants that cannot use pdata as its transfer type, so it cannot be a
// receiver — it is a standalone service reading the spool directly, and `spool.Reader`
// is the whole API it needs.
package dagrfilereceiver

import (
	"errors"
	"time"

	"go.opentelemetry.io/collector/component"
)

// Type is the component type as it appears in a collector config: `dagrfile`.
var Type = component.MustNewType("dagrfile")

// Config is the receiver's YAML surface.
type Config struct {
	// Path is the spool file to read. Required.
	Path string `mapstructure:"path"`

	// Newest reads the spool NEWEST-FIRST, which on a doubly-linked sink costs one step
	// per record from EOF and no forward pass at all (spec/11 §11.7). Combined with
	// Limit it is the "drain the freshest telemetry first" policy a saturated queue
	// wants and a FIFO KV store cannot cheaply offer (spec/38 §6.1).
	Newest bool `mapstructure:"newest"`

	// Limit caps how many batches are forwarded (0 = all of them).
	Limit int `mapstructure:"limit"`

	// Poll re-reads the spool on this interval, forwarding whatever has been appended
	// since. Zero reads the spool once, at startup, and stops.
	Poll time.Duration `mapstructure:"poll"`
}

func (c *Config) Validate() error {
	if c.Path == "" {
		return errors.New("dagrfile: `path` is required")
	}
	if c.Limit < 0 {
		return errors.New("dagrfile: `limit` cannot be negative")
	}
	if c.Newest && c.Poll != 0 {
		return errors.New("dagrfile: `newest` and `poll` are incompatible — a reverse " +
			"read starts from the end, so it cannot also follow the end")
	}
	return nil
}

func createDefaultConfig() component.Config { return &Config{} }
