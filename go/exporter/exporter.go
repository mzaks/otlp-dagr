package dagrfileexporter

import (
	"context"
	"errors"
	"sync"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/exporter/xexporter"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/pprofile"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.uber.org/zap"

	"otlpdagr/logbridge"
	"otlpdagr/metricbridge"
	"otlpdagr/pdatabridge"
	"otlpdagr/profilebridge"
	"otlpdagr/spool"
	"otlpgen/logrecordlog"
	"otlpgen/metriclog"
	"otlpgen/profilelog"
)

var errShutDown = errors.New("dagrfile: exporter is not running")

// NewFactory returns the `dagrfile` exporter factory, for an ocb manifest: traces,
// metrics and logs, plus profiles through the collector's experimental xexporter API
// (profiles is `v1development` upstream and needs the `service.profilesSupport` gate).
//
// One component instance serves one pipeline, so each signal gets its own spool — point
// each at its own path. A spool records no signal identity on the wire yet (spec/38 D2),
// so two signals sharing one file is not detected; Validate cannot see the signal.
func NewFactory() exporter.Factory {
	return xexporter.NewFactory(Type, createDefaultConfig,
		xexporter.WithTraces(createTraces, component.StabilityLevelDevelopment),
		xexporter.WithMetrics(createMetrics, component.StabilityLevelDevelopment),
		xexporter.WithLogs(createLogs, component.StabilityLevelDevelopment),
		xexporter.WithProfiles(createProfiles, component.StabilityLevelDevelopment))
}

func newBase(set exporter.Settings, cfg component.Config, sig spool.Signal) base {
	return base{cfg: cfg.(*Config), log: set.Logger, now: time.Now, sig: sig}
}

func createTraces(_ context.Context, set exporter.Settings, cfg component.Config) (exporter.Traces, error) {
	return &tracesExporter{base: newBase(set, cfg, spool.Traces)}, nil
}

func createMetrics(_ context.Context, set exporter.Settings, cfg component.Config) (exporter.Metrics, error) {
	return &metricsExporter{base: newBase(set, cfg, spool.Metrics)}, nil
}

func createLogs(_ context.Context, set exporter.Settings, cfg component.Config) (exporter.Logs, error) {
	return &logsExporter{base: newBase(set, cfg, spool.Logs)}, nil
}

func createProfiles(_ context.Context, set exporter.Settings, cfg component.Config) (xexporter.Profiles, error) {
	return &profilesExporter{base: newBase(set, cfg, spool.Profiles)}, nil
}

// base is everything that does not depend on the signal: the spool's lifecycle and the
// append. Each exporter implements its signal's consumer interface directly (component
// plus consumer) — there is nothing between Consume* and the spool to hide in.
type base struct {
	cfg *Config
	log *zap.Logger
	now func() time.Time
	sig spool.Signal

	mu sync.Mutex
	w  *spool.Writer
	// The record slice is held and reused, so the traces encode allocates nothing at
	// steady state (spec/38 §10.2.1): a one-pass StreamBuilder writes the record. The
	// other signals build a value tree first and append it through the spool's typed
	// append, which stores it with the graph's generated code.
	dst []byte
}

func (e *base) Capabilities() consumer.Capabilities {
	// The exporter reads pdata and writes bytes; it never touches the batch.
	return consumer.Capabilities{MutatesData: false}
}

func (e *base) Start(_ context.Context, _ component.Host) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if !e.cfg.Resume {
		w, err := spool.Create(e.cfg.Path, e.sig)
		if err != nil {
			return err
		}
		e.w = w
		e.log.Info("dagrfile: spool created", zap.String("path", e.cfg.Path),
			zap.Stringer("signal", e.sig))
		return nil
	}

	w, rec, err := spool.Resume(e.cfg.Path, e.sig)
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
			zap.Stringer("signal", e.sig), zap.String("tail", rec.String()))
	}
	return nil
}

func (e *base) Shutdown(context.Context) error {
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

// append hands the spool one batch through add, which receives the receive time.
// Called with e.mu held.
func (e *base) append(add func(receivedUnixNano uint64) error) error {
	if e.w == nil {
		return errShutDown
	}
	if err := add(uint64(e.now().UnixNano())); err != nil {
		return err
	}
	if e.cfg.Sync {
		return e.w.Sync()
	}
	return nil
}

type tracesExporter struct {
	base
	enc *pdatabridge.StreamBuilder
}

func (e *tracesExporter) ConsumeTraces(_ context.Context, td ptrace.Traces) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.enc == nil {
		e.enc = pdatabridge.NewStreamBuilder()
	}
	return e.append(func(t uint64) error {
		e.dst = e.enc.AppendRecord(e.dst[:0], t, td)
		return e.w.AppendRecord(e.dst)
	})
}

type metricsExporter struct{ base }

func (e *metricsExporter) ConsumeMetrics(_ context.Context, md pmetric.Metrics) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.append(func(t uint64) error {
		return e.w.AppendMetrics(metriclog.Batch{ReceivedUnixNano: t, DataPointCount: uint32(md.DataPointCount()),
			ResourceMetrics: metricbridge.ToValue(md).ResourceMetrics.V})
	})
}

type logsExporter struct{ base }

func (e *logsExporter) ConsumeLogs(_ context.Context, ld plog.Logs) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.append(func(t uint64) error {
		return e.w.AppendLogs(logrecordlog.Batch{ReceivedUnixNano: t, LogRecordCount: uint32(ld.LogRecordCount()),
			ResourceLogs: logbridge.ToValue(ld).ResourceLogs.V})
	})
}

type profilesExporter struct{ base }

func (e *profilesExporter) ConsumeProfiles(_ context.Context, pd pprofile.Profiles) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.append(func(t uint64) error {
		v := profilebridge.ToValue(pd)
		return e.w.AppendProfiles(profilelog.Batch{ReceivedUnixNano: t, SampleCount: uint32(pd.SampleCount()),
			ResourceProfiles: v.ResourceProfiles.V, Dictionary: v.Dictionary})
	})
}
