//go:build unix

// shmdemo is an application instrumented with the OTel Go SDK whose metrics exporter is
// a shared-memory region (package shmexporter, spec/38 §7.1) — the app half of
// `ocb/roundtrip.sh`'s shared-memory phase, and of the same-host benchmark.
//
//	shmdemo -dir /run/otlp-dagr -service demo -requests 1000 -interval 200ms
//
// It records `demo.requests` (a counter: -requests increments, spread over -routes
// routes), `demo.inflight` (an up-down counter), `demo.temp` (a gauge) and
// `demo.duration` (a histogram), exporting every -interval, then flushes, holds the
// final state for -hold so a collector polling slower than it exports still reads it,
// and shuts down — which removes the region.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"

	"otlpdagr/shmexporter"
)

func main() {
	dir := flag.String("dir", os.TempDir(), "the shared region directory")
	service := flag.String("service", "demo", "service.name")
	requests := flag.Int("requests", 1000, "counter increments to record")
	routes := flag.Int("routes", 4, "distinct route attribute values")
	interval := flag.Duration("interval", 200*time.Millisecond, "export interval")
	hold := flag.Duration("hold", 2*time.Second, "keep the final state readable this long")
	flag.Parse()

	path := filepath.Join(*dir, *service+".dagrshm")
	exp, err := shmexporter.New(path, *service)
	if err != nil {
		fail(err)
	}
	res := resource.NewSchemaless(attribute.String("service.name", *service))
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithResource(res),
		sdkmetric.WithReader(sdkmetric.NewPeriodicReader(exp, sdkmetric.WithInterval(*interval))))
	ctx := context.Background()
	m := mp.Meter("otlp-dagr/shmdemo")
	reqs, _ := m.Int64Counter("demo.requests", metric.WithUnit("{request}"))
	inflight, _ := m.Int64UpDownCounter("demo.inflight")
	temp, _ := m.Float64Gauge("demo.temp", metric.WithUnit("Cel"))
	dur, _ := m.Float64Histogram("demo.duration", metric.WithUnit("ms"))

	attrs := make([]metric.AddOption, *routes)
	for i := range attrs {
		attrs[i] = metric.WithAttributes(attribute.String("route", fmt.Sprintf("/r%d", i)))
	}
	step := *interval / 10
	for i := 0; i < *requests; i++ {
		reqs.Add(ctx, 1, attrs[i%*routes])
		inflight.Add(ctx, 1)
		dur.Record(ctx, float64(i%250))
		inflight.Add(ctx, -1)
		if i%(*requests/20+1) == 0 {
			temp.Record(ctx, 40+float64(i%10))
			time.Sleep(step) // spread the work over several exports
		}
	}
	temp.Record(ctx, 55)
	if err := mp.ForceFlush(ctx); err != nil {
		fail(err)
	}
	fmt.Printf("shmdemo: %d requests over %d routes in %s\n", *requests, *routes, path)
	time.Sleep(*hold)
	if err := mp.Shutdown(ctx); err != nil {
		fail(err)
	}
}

func fail(err error) {
	fmt.Fprintln(os.Stderr, "shmdemo:", err)
	os.Exit(1)
}
