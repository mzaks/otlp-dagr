package dagrfileexporter

import (
	"context"
	"errors"
	"sync"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.uber.org/zap"

	"otlpdagr/pdatabridge"
	"otlpdagr/spool"
)

var errShutDown = errors.New("dagrfile: exporter is not running")

// NewFactory returns the `dagrfile` traces exporter factory, for an ocb manifest.
func NewFactory() exporter.Factory {
	return exporter.NewFactory(Type, createDefaultConfig,
		exporter.WithTraces(createTraces, component.StabilityLevelDevelopment))
}

func createTraces(_ context.Context, set exporter.Settings, cfg component.Config) (exporter.Traces, error) {
	return &dagrFileExporter{cfg: cfg.(*Config), log: set.Logger, now: time.Now}, nil
}

// dagrFileExporter implements exporter.Traces directly (component.Component plus
// consumer.Traces) — there is nothing between ConsumeTraces and the spool to hide in.
type dagrFileExporter struct {
	cfg *Config
	log *zap.Logger
	now func() time.Time

	mu sync.Mutex
	w  *spool.Writer
	// The builder and its destination slice are held here and reused, so a steady-state
	// export allocates nothing to encode a batch — the property spec/38 §10.2.1 chased
	// the Go direct builder from 15,124 allocs/op down to 0 to get.
	enc *pdatabridge.StreamBuilder
	dst []byte
}

func (e *dagrFileExporter) Capabilities() consumer.Capabilities {
	// The exporter reads pdata and writes bytes; it never touches the batch.
	return consumer.Capabilities{MutatesData: false}
}

func (e *dagrFileExporter) Start(_ context.Context, _ component.Host) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.enc = pdatabridge.NewStreamBuilder()

	if !e.cfg.Resume {
		w, err := spool.Create(e.cfg.Path)
		if err != nil {
			return err
		}
		e.w = w
		e.log.Info("dagrfile: spool created", zap.String("path", e.cfg.Path))
		return nil
	}

	w, rec, err := spool.Resume(e.cfg.Path)
	if err != nil {
		return err
	}
	e.w = w
	// A dropped tail is a fact about data loss, so it is logged at Warn and named in
	// bytes — never swallowed (spec/38 §6.1.1, gate 4).
	if rec.Torn > 0 {
		e.log.Warn("dagrfile: torn tail dropped on resume",
			zap.String("path", e.cfg.Path), zap.Int64("bytes", rec.Torn),
			zap.Int64("valid_end", rec.ValidEnd))
	} else {
		e.log.Info("dagrfile: spool resumed", zap.String("path", e.cfg.Path),
			zap.String("tail", rec.String()))
	}
	return nil
}

func (e *dagrFileExporter) Shutdown(context.Context) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.w == nil {
		return nil
	}
	w := e.w
	e.w = nil
	if err := w.Sync(); err != nil {
		w.Close()
		return err
	}
	return w.Close()
}

func (e *dagrFileExporter) ConsumeTraces(_ context.Context, td ptrace.Traces) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.w == nil {
		return errShutDown
	}

	// Encode into the retained destination, then hand the spool exactly those bytes.
	e.dst = e.enc.BuildAppend(e.dst[:0], td)
	if err := e.w.Append(uint64(e.now().UnixNano()), uint32(td.SpanCount()), e.dst); err != nil {
		return err
	}
	if e.cfg.Sync {
		return e.w.Sync()
	}
	return nil
}
