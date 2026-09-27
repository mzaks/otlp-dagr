//go:build unix

// shmbench is the application side of the same-host metrics measurement (spec/38 §7.1):
// one OTel Go SDK MeterProvider with a fixed load, exporting through ONE of three paths —
//
//	-mode shm    shmexporter: each export is a seqlock write into a shared-memory region
//	-mode otlp   otlpmetricgrpc: each export is an OTLP/gRPC request to localhost
//	-mode prom   the Prometheus exporter: the collector scrapes /metrics over HTTP
//	-mode none   no export at all — the recording cost every mode pays, to subtract
//	-mode direct no SDK: shmdirect's bound instruments, each recording an atomic
//	             operation in shared memory, read by the same dagrshm receiver
//
// The load is identical in every mode — -streams metric streams (40% counters, 20%
// gauges, 40% histograms over 10 attribute sets each), every stream recorded once per
// -interval — so what differs between the modes is the export path alone. At exit it
// prints its own CPU time (user + system), for ocb/samehost.sh.
package main

import (
	"context"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	promexp "go.opentelemetry.io/otel/exporters/prometheus"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"

	"otlpdagr/shmdirect"
	"otlpdagr/shmexporter"
)

func main() {
	mode := flag.String("mode", "shm", "shm | otlp | prom")
	dir := flag.String("dir", os.TempDir(), "shm: the region directory")
	endpoint := flag.String("endpoint", "127.0.0.1:4317", "otlp: the collector's gRPC endpoint")
	listen := flag.String("listen", "127.0.0.1:9464", "prom: the /metrics listen address")
	interval := flag.Duration("interval", 100*time.Millisecond, "export (and record) interval")
	duration := flag.Duration("duration", 20*time.Second, "how long to run")
	streams := flag.Int("streams", 500, "metric streams (a multiple of 10)")
	shards := flag.Int("shards", 0, "direct: per-core copies of the values region (0: GOMAXPROCS)")
	flag.Parse()
	ctx := context.Background()
	if *mode == "direct" {
		runDirect(filepath.Join(*dir, "shmbench.dagrshm"), *interval, *duration, *streams, *shards)
		return
	}

	res := resource.NewSchemaless(attribute.String("service.name", "shmbench-"+*mode))
	var reader sdkmetric.Reader
	switch *mode {
	case "shm":
		exp, err := shmexporter.New(filepath.Join(*dir, "shmbench.dagrshm"), "shmbench-shm")
		if err != nil {
			fail(err)
		}
		reader = sdkmetric.NewPeriodicReader(exp, sdkmetric.WithInterval(*interval))
	case "otlp":
		exp, err := otlpmetricgrpc.New(ctx, otlpmetricgrpc.WithEndpoint(*endpoint), otlpmetricgrpc.WithInsecure())
		if err != nil {
			fail(err)
		}
		reader = sdkmetric.NewPeriodicReader(exp, sdkmetric.WithInterval(*interval))
	case "prom":
		reg := prometheus.NewRegistry()
		exp, err := promexp.New(promexp.WithRegisterer(reg))
		if err != nil {
			fail(err)
		}
		reader = exp
		ln, err := net.Listen("tcp", *listen)
		if err != nil {
			fail(err)
		}
		go http.Serve(ln, promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	case "none":
		reader = sdkmetric.NewManualReader() // never collected
	default:
		fail(fmt.Errorf("unknown -mode %q", *mode))
	}
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithResource(res), sdkmetric.WithReader(reader))
	m := mp.Meter("otlp-dagr/shmbench")

	sets := make([]metric.MeasurementOption, 10)
	for i := range sets {
		sets[i] = metric.WithAttributes(attribute.String("route", fmt.Sprintf("/api/v1/r%d", i)),
			attribute.Int("code", 200+i))
	}
	per := *streams / 10
	nCounter, nGauge := per*4/10, per*2/10
	nHist := per - nCounter - nGauge
	counters := make([]metric.Int64Counter, nCounter)
	gauges := make([]metric.Float64Gauge, nGauge)
	hists := make([]metric.Float64Histogram, nHist)
	for i := range counters {
		counters[i], _ = m.Int64Counter(fmt.Sprintf("bench.counter.%d", i), metric.WithUnit("{op}"))
	}
	for i := range gauges {
		gauges[i], _ = m.Float64Gauge(fmt.Sprintf("bench.gauge.%d", i), metric.WithUnit("1"))
	}
	for i := range hists {
		hists[i], _ = m.Float64Histogram(fmt.Sprintf("bench.latency.%d", i), metric.WithUnit("ms"))
	}
	addOpts := make([]metric.AddOption, len(sets))
	recOpts := make([]metric.RecordOption, len(sets))
	for i, s := range sets {
		addOpts[i], recOpts[i] = s, s
	}

	t := time.NewTicker(*interval)
	defer t.Stop()
	end := time.Now().Add(*duration)
	for k := 0; time.Now().Before(end); k++ {
		for i, c := range counters {
			for s := range sets {
				c.Add(ctx, int64(1+(i+s+k)%7), addOpts[s])
			}
		}
		for i, g := range gauges {
			for s := range sets {
				g.Record(ctx, float64((i*s+k)%100), recOpts[s])
			}
		}
		for i, h := range hists {
			for s := range sets {
				h.Record(ctx, float64((i+s*37+k*13)%2000), recOpts[s])
			}
		}
		<-t.C
	}
	if err := mp.Shutdown(ctx); err != nil {
		fmt.Fprintln(os.Stderr, "shmbench: shutdown:", err)
	}
	var ru syscall.Rusage
	syscall.Getrusage(syscall.RUSAGE_SELF, &ru)
	cpu := time.Duration(ru.Utime.Nano() + ru.Stime.Nano())
	fmt.Printf("shmbench mode=%s streams=%d interval=%s duration=%s cpu=%.3fs\n",
		*mode, *streams, *interval, *duration, cpu.Seconds())
}

func fail(err error) {
	fmt.Fprintln(os.Stderr, "shmbench:", err)
	os.Exit(1)
}

// runDirect is the same load as the SDK modes — the same streams, each recorded once per
// interval with the same values — through shmdirect's bound instruments: no SDK, no
// collect, no export; the collector reads the values region at its own cadence.
func runDirect(path string, interval, duration time.Duration, streams, shards int) {
	p, err := shmdirect.NewProviderWith(path, "shmbench-direct", shmdirect.Options{Shards: shards})
	if err != nil {
		fail(err)
	}
	per := streams / 10
	nCounter, nGauge := per*4/10, per*2/10
	nHist := per - nCounter - nGauge
	set := func(s int) []shmdirect.Attr {
		return []shmdirect.Attr{{Key: "route", Value: fmt.Sprintf("/api/v1/r%d", s)}, {Key: "code", Value: 200 + s}}
	}
	const scope = "otlp-dagr/shmbench"
	counters := make([][10]*shmdirect.Int64Counter, nCounter)
	gauges := make([][10]*shmdirect.Float64Gauge, nGauge)
	hists := make([][10]*shmdirect.Float64Histogram, nHist)
	for s := 0; s < 10; s++ {
		for i := range counters {
			if counters[i][s], err = p.Int64Counter(scope, fmt.Sprintf("bench.counter.%d", i), "{op}", set(s)...); err != nil {
				fail(err)
			}
		}
		for i := range gauges {
			if gauges[i][s], err = p.Float64Gauge(scope, fmt.Sprintf("bench.gauge.%d", i), "1", set(s)...); err != nil {
				fail(err)
			}
		}
		for i := range hists {
			if hists[i][s], err = p.Float64Histogram(scope, fmt.Sprintf("bench.latency.%d", i), "ms", nil, set(s)...); err != nil {
				fail(err)
			}
		}
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	end := time.Now().Add(duration)
	for k := 0; time.Now().Before(end); k++ {
		for i := range counters {
			for s := 0; s < 10; s++ {
				counters[i][s].Add(int64(1 + (i+s+k)%7))
			}
		}
		for i := range gauges {
			for s := 0; s < 10; s++ {
				gauges[i][s].Record(float64((i*s + k) % 100))
			}
		}
		for i := range hists {
			for s := 0; s < 10; s++ {
				hists[i][s].Record(float64((i + s*37 + k*13) % 2000))
			}
		}
		<-t.C
	}
	p.Shutdown()
	var ru syscall.Rusage
	syscall.Getrusage(syscall.RUSAGE_SELF, &ru)
	fmt.Printf("shmbench mode=direct streams=%d shards=%d interval=%s duration=%s cpu=%.3fs\n",
		streams, p.Shards(), interval, duration, time.Duration(ru.Utime.Nano()+ru.Stime.Nano()).Seconds())
}
