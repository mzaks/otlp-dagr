package main

import (
	"bufio"
	"flag"
	"fmt"
	"os"

	"go.opentelemetry.io/collector/pdata/pmetric"
)

// shmCheck verifies what a collector's file exporter wrote from the `dagrshm` receiver
// against cmd/shmdemo's final state: the LAST forward of the region must carry every
// request (cumulative), spread over every route, the in-flight count back at zero, the
// last gauge reading, and a histogram that counted every request.
func shmCheck(args []string) error {
	fs := flag.NewFlagSet("shmcheck", flag.ExitOnError)
	path := fs.String("path", "", "the file exporter's output (JSON lines)")
	service := fs.String("service", "demo", "the region's service.name")
	requests := fs.Int("requests", 1000, "shmdemo -requests")
	routes := fs.Int("routes", 4, "shmdemo -routes")
	_ = fs.Parse(args)
	f, err := os.Open(*path)
	if err != nil {
		return err
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 64<<20)
	var last pmetric.Metrics
	forwards := 0
	for sc.Scan() {
		md, err := (&pmetric.JSONUnmarshaler{}).UnmarshalMetrics(sc.Bytes())
		if err != nil {
			return err
		}
		for i := 0; i < md.ResourceMetrics().Len(); i++ {
			if v, _ := md.ResourceMetrics().At(i).Resource().Attributes().Get("service.name"); v.Str() == *service {
				last, forwards = md, forwards+1
			}
		}
	}
	if err := sc.Err(); err != nil {
		return err
	}
	if forwards == 0 {
		return fmt.Errorf("shmcheck: no forward of service %q in %s", *service, *path)
	}
	got := map[string]pmetric.Metric{}
	rms := last.ResourceMetrics()
	for i := 0; i < rms.Len(); i++ {
		sms := rms.At(i).ScopeMetrics()
		for j := 0; j < sms.Len(); j++ {
			ms := sms.At(j).Metrics()
			for k := 0; k < ms.Len(); k++ {
				got[ms.At(k).Name()] = ms.At(k)
			}
		}
	}
	for _, n := range []string{"demo.requests", "demo.inflight", "demo.temp", "demo.duration"} {
		if _, ok := got[n]; !ok {
			return fmt.Errorf("shmcheck: the last forward has no %s", n)
		}
	}
	req := got["demo.requests"].Sum().DataPoints()
	var total int64
	for i := 0; i < req.Len(); i++ {
		total += req.At(i).IntValue()
	}
	switch {
	case req.Len() != *routes:
		return fmt.Errorf("shmcheck: demo.requests has %d routes, want %d", req.Len(), *routes)
	case total != int64(*requests):
		return fmt.Errorf("shmcheck: demo.requests totals %d, want %d", total, *requests)
	case got["demo.inflight"].Sum().DataPoints().At(0).IntValue() != 0:
		return fmt.Errorf("shmcheck: demo.inflight is not back at zero")
	case got["demo.temp"].Gauge().DataPoints().At(0).DoubleValue() != 55:
		return fmt.Errorf("shmcheck: demo.temp is not the last reading")
	case got["demo.duration"].Histogram().DataPoints().At(0).Count() != uint64(*requests):
		return fmt.Errorf("shmcheck: demo.duration counted %d", got["demo.duration"].Histogram().DataPoints().At(0).Count())
	}
	fmt.Printf("shmcheck: %d forwards of %q; the last carries %d requests over %d routes\n",
		forwards, *service, total, req.Len())
	return nil
}
