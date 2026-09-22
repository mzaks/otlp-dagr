package dagrfilereceiver

import (
	"context"
	"os"
	"sync"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/receiver"
	"go.uber.org/zap"

	"otlpdagr/pdatabridge"
	"otlpdagr/spool"
)

// NewFactory returns the `dagrfile` traces receiver factory, for an ocb manifest.
func NewFactory() receiver.Factory {
	return receiver.NewFactory(Type, createDefaultConfig,
		receiver.WithTraces(createTraces, component.StabilityLevelDevelopment))
}

func createTraces(_ context.Context, set receiver.Settings, cfg component.Config, next consumer.Traces) (receiver.Traces, error) {
	return &dagrFileReceiver{cfg: cfg.(*Config), log: set.Logger, next: next}, nil
}

type dagrFileReceiver struct {
	cfg  *Config
	log  *zap.Logger
	next consumer.Traces

	cancel context.CancelFunc
	done   sync.WaitGroup

	// sent is how many records of the spool have already been forwarded, so a poll
	// picks up where the last one stopped instead of replaying the file.
	sent int
}

func (r *dagrFileReceiver) Start(ctx context.Context, _ component.Host) error {
	// The tail check is the first thing that happens, before a single batch is
	// forwarded: a spool left behind by a killed exporter may end in a partial record,
	// and a receiver that reads it forward would simply stop early and report nothing
	// wrong (spec/38 §6.1.1, gate 4).
	if buf, err := os.ReadFile(r.cfg.Path); err == nil {
		if rec, err := spool.CheckTail(buf); err == nil && rec.Torn > 0 {
			r.log.Warn("dagrfile: spool ends in a torn record; it will not be forwarded",
				zap.String("path", r.cfg.Path), zap.Int64("bytes", rec.Torn),
				zap.Int64("valid_end", rec.ValidEnd))
		}
	}

	ctx, r.cancel = context.WithCancel(context.WithoutCancel(ctx))
	if r.cfg.Poll <= 0 {
		return r.drain(ctx)
	}
	r.done.Add(1)
	go func() {
		defer r.done.Done()
		t := time.NewTicker(r.cfg.Poll)
		defer t.Stop()
		for {
			if err := r.drain(ctx); err != nil && ctx.Err() == nil {
				r.log.Error("dagrfile: drain failed", zap.Error(err))
			}
			select {
			case <-ctx.Done():
				return
			case <-t.C:
			}
		}
	}()
	return nil
}

func (r *dagrFileReceiver) Shutdown(context.Context) error {
	if r.cancel != nil {
		r.cancel()
	}
	r.done.Wait()
	return nil
}

// drain forwards every batch the spool holds that has not been forwarded yet.
func (r *dagrFileReceiver) drain(ctx context.Context) error {
	rd, err := spool.Open(r.cfg.Path)
	if err != nil {
		return err
	}
	limit := r.cfg.Limit
	n := 0
	var firstErr error

	forward := func(b spool.Batch) bool {
		if ctx.Err() != nil {
			return false
		}
		// The whole cost of this component, in one call: the buffer that was a
		// sub-slice of the spool becomes an allocated pdata tree.
		td, err := pdatabridge.ToTraces(b.Traces)
		if err != nil {
			if firstErr == nil {
				firstErr = err
			}
			return false
		}
		if err := r.next.ConsumeTraces(ctx, td); err != nil {
			if firstErr == nil {
				firstErr = err
			}
			return false
		}
		n++
		return limit == 0 || n < limit
	}

	if r.cfg.Newest {
		err = rd.Reverse(forward)
	} else {
		skip := r.sent
		err = rd.Forward(func(b spool.Batch) bool {
			if skip > 0 {
				skip--
				return true
			}
			return forward(b)
		})
		r.sent += n
	}
	if err != nil {
		return err
	}
	if n > 0 {
		r.log.Debug("dagrfile: forwarded batches", zap.Int("count", n))
	}
	return firstErr
}
