package main

import (
	"bufio"
	"flag"
	"fmt"
	"os"
	"strings"

	"go.opentelemetry.io/collector/pdata/pmetric"
)

// points reports how many `bench.*` data points the LAST line of a file exporter's
// output holds — ocb/samehost.sh's check that a mode's collector really received the
// app's streams (a path that silently delivers nothing would otherwise measure cheapest).
func points(args []string) error {
	fs := flag.NewFlagSet("points", flag.ExitOnError)
	path := fs.String("path", "", "the file exporter's output (JSON lines)")
	prefix := fs.String("prefix", "bench.", "count metrics whose name starts with this")
	_ = fs.Parse(args)
	f, err := os.Open(*path)
	if err != nil {
		return err
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 256<<20)
	n, lines := 0, 0
	for sc.Scan() {
		md, err := (&pmetric.JSONUnmarshaler{}).UnmarshalMetrics(sc.Bytes())
		if err != nil {
			return err
		}
		lines++
		n = 0
		rms := md.ResourceMetrics()
		for i := 0; i < rms.Len(); i++ {
			sms := rms.At(i).ScopeMetrics()
			for j := 0; j < sms.Len(); j++ {
				ms := sms.At(j).Metrics()
				for k := 0; k < ms.Len(); k++ {
					m := ms.At(k)
					if !strings.HasPrefix(strings.ReplaceAll(m.Name(), "_", "."), *prefix) {
						continue
					}
					switch m.Type() {
					case pmetric.MetricTypeSum:
						n += m.Sum().DataPoints().Len()
					case pmetric.MetricTypeGauge:
						n += m.Gauge().DataPoints().Len()
					case pmetric.MetricTypeHistogram:
						n += m.Histogram().DataPoints().Len()
					}
				}
			}
		}
	}
	if err := sc.Err(); err != nil {
		return err
	}
	fmt.Printf("%d %d\n", lines, n)
	return nil
}
