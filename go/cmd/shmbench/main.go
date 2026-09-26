//go:build unix

// shmbench is the application side of the same-host metrics measurement (spec/38 §7.1):
// one OTel Go SDK MeterProvider with a fixed load, exporting through ONE of three paths —
//
//	-mode shm    shmexporter: each export is a seqlock write into a shared-memory region
//	-mode otlp   otlpmetricgrpc: each export is an OTLP/gRPC request to localhost
//	-mode prom   the Prometheus exporter: the collector scrapes /metrics over HTTP
//	-mode none   no export at all — the recording cost every mode pays, to subtract
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
	flag.Parse()
	ctx := context.Background()

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
