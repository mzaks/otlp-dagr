//go:build unix

package dagrshmreceiver

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/receiver"
	"go.uber.org/zap"

	mr "otlpgen/metricsregion"

	"otlpdagr/shm"
)

// Type is the component type as it appears in a collector config: `dagrshm`.
var Type = component.MustNewType("dagrshm")

// Config is the receiver's YAML surface.
type Config struct {
	// Directory holds the region files — the shared volume (spec/38 §7.1, D4). Required.
	Directory string `mapstructure:"directory"`
	// Pattern selects region files in Directory (default `*.dagrshm`).
	Pattern string `mapstructure:"pattern"`
	// Interval is how often the regions are read (default 10s).
	Interval time.Duration `mapstructure:"interval"`
	// Attempts bounds each seqlock read (default 1000): a region whose writer died
	// mid-write stays busy for good and must not stall the others.
	Attempts int `mapstructure:"attempts"`
	// StaleAfter skips a region whose last export is older than this (default 5m): an
	// app killed without shutting its exporter down leaves its file behind.
	StaleAfter time.Duration `mapstructure:"stale_after"`
}

func (c *Config) Validate() error {
	if c.Directory == "" {
		return errors.New("dagrshm: `directory` is required")
	}
	if c.Interval < 0 || c.Attempts < 0 || c.StaleAfter < 0 {
		return errors.New("dagrshm: `interval`, `attempts` and `stale_after` cannot be negative")
	}
	return nil
}

func createDefaultConfig() component.Config {
	return &Config{Pattern: "*.dagrshm", Interval: 10 * time.Second, Attempts: 1000, StaleAfter: 5 * time.Minute}
}

// NewFactory returns the `dagrshm` receiver factory, for an ocb manifest (metrics).
func NewFactory() receiver.Factory {
	return receiver.NewFactory(Type, createDefaultConfig,
		receiver.WithMetrics(createMetrics, component.StabilityLevelDevelopment))
}

func createMetrics(_ context.Context, set receiver.Settings, cfg component.Config, next consumer.Metrics) (receiver.Metrics, error) {
	return &shmReceiver{cfg: cfg.(*Config), log: set.Logger, next: next, open: map[string]*region{}}, nil
}

type region struct {
	r       *shm.Region
	ino     uint64
	exports uint64 // the last export forwarded
}

type shmReceiver struct {
	cfg  *Config
	log  *zap.Logger
	next consumer.Metrics

	open   map[string]*region
	snap   []byte
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

func (r *shmReceiver) Start(context.Context, component.Host) error {
	ctx, cancel := context.WithCancel(context.Background())
	r.cancel = cancel
	r.snap = make([]byte, mr.ByteSize)
	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		t := time.NewTicker(r.cfg.Interval)
		defer t.Stop()
		for {
			r.Poll(ctx)
			select {
			case <-ctx.Done():
				return
			case <-t.C:
			}
		}
	}()
	return nil
}

// Poll reads every region once and forwards those with a new export.
func (r *shmReceiver) Poll(ctx context.Context) {
	paths, err := filepath.Glob(filepath.Join(r.cfg.Directory, r.cfg.Pattern))
	if err != nil {
		r.log.Warn("dagrshm: bad pattern", zap.Error(err))
		return
	}
	seen := make(map[string]bool, len(paths))
	for _, p := range paths {
		seen[p] = true
		reg, err := r.attach(p)
		if err != nil {
			r.log.Debug("dagrshm: cannot attach", zap.String("path", p), zap.Error(err))
			continue
		}
		s, err := reg.r.Snapshot(r.snap, r.cfg.Attempts)
		if err != nil {
			r.log.Debug("dagrshm: no snapshot", zap.String("path", p), zap.Error(err))
			continue
		}
		if s.Exports() == reg.exports {
			continue // nothing new since the last forward
		}
		if age := time.Since(time.Unix(0, int64(s.WriteUnixNano()))); r.cfg.StaleAfter > 0 && age > r.cfg.StaleAfter {
			continue
		}
		reg.exports = s.Exports()
		if d := s.DroppedPoints(); d > 0 {
			r.log.Debug("dagrshm: region dropped points", zap.String("path", p), zap.Uint32("dropped", d))
		}
		if err := r.next.ConsumeMetrics(ctx, ToMetrics(s)); err != nil {
			r.log.Warn("dagrshm: consumer refused a region", zap.String("path", p), zap.Error(err))
		}
	}
	for p, reg := range r.open { // files that went away: their apps shut down
		if !seen[p] {
			reg.r.Close()
			delete(r.open, p)
		}
	}
}

// attach maps p once, and again when the file under the name is a new one (the app
// restarted and Created its region anew).
func (r *shmReceiver) attach(p string) (*region, error) {
	st, err := os.Stat(p)
	if err != nil {
		return nil, err
	}
	ino := st.Sys().(*syscall.Stat_t).Ino
	if reg, ok := r.open[p]; ok {
		if uint64(reg.ino) == uint64(ino) {
			return reg, nil
		}
		reg.r.Close()
		delete(r.open, p)
	}
	sr, err := shm.Open(p)
	if err != nil {
		return nil, err
	}
	reg := &region{r: sr, ino: uint64(ino)}
	r.open[p] = reg
	return reg, nil
}

func (r *shmReceiver) Shutdown(context.Context) error {
	if r.cancel != nil {
		r.cancel()
		r.wg.Wait()
	}
	for p, reg := range r.open {
		reg.r.Close()
		delete(r.open, p)
	}
	return nil
}
