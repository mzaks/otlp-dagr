package bench

// signals_test.go — D6: do the traces conclusions hold for metrics, logs and profiles?
// Same questions the traces benchmark asks, per signal, against protobuf:
//
//	size        raw and gzipped (the transport question)
//	encode      pdata -> bytes (protobuf marshal vs the Dagr direct builder)
//	to pdata    bytes -> pdata (protobuf unmarshal vs the Dagr bridge's materialise)
//	sweep       read one field of every item (protobuf must unmarshal first; Dagr reads
//	            the buffer in place through the lazy accessors)
//
//	go test ./bench -run TestSignalSizes -v
//	go test ./bench -run '^$' -bench 'Signal' -benchmem

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"testing"

	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/pprofile"

	"otlpdagr/internal/fixture"
	"otlpdagr/logbridge"
	"otlpdagr/metricbridge"
	"otlpdagr/profilebridge"
	cg "otlpgen/otlpcommon"
	gl "otlpgen/otlplogs"
	dl "otlpgen/otlplogsdirect"
	gm "otlpgen/otlpmetrics"
	dm "otlpgen/otlpmetricsdirect"
	gp "otlpgen/otlpprofiles"
	dp "otlpgen/otlpprofilesdirect"
)

const (
	nMetrics, nDP             = 150, 4 // 600 data points
	nLogs                     = 500
	nSamples, nFuncs, nStacks = 2000, 400, 200
)

var (
	metricsIn  = fixture.MetricsBatch(nMetrics, nDP)
	logsIn     = fixture.LogsBatch(nLogs)
	profilesIn = fixture.ProfilesBatch(nSamples, nFuncs, nStacks)

	metricsPB  = must((&pmetric.ProtoMarshaler{}).MarshalMetrics(metricsIn))
	logsPB     = must((&plog.ProtoMarshaler{}).MarshalLogs(logsIn))
	profilesPB = must((&pprofile.ProtoMarshaler{}).MarshalProfiles(fixture.ProfilesBatch(nSamples, nFuncs, nStacks)))

	metricsDagr  = metricbridge.ToBytes(metricsIn)
	logsDagr     = logbridge.ToBytes(logsIn)
	profilesDagr = profilebridge.ToBytes(fixture.ProfilesBatch(nSamples, nFuncs, nStacks))
)

func must(b []byte, err error) []byte {
	if err != nil {
		panic(err)
	}
	return b
}

func gz(b []byte) int {
	var buf bytes.Buffer
	w := gzip.NewWriter(&buf)
	w.Write(b)
	w.Close()
	return buf.Len()
}

// The realistic batches round-trip too — a benchmark of a lossy encoder measures nothing.
func TestSignalBatchesRoundTrip(t *testing.T) {
	md, err := metricbridge.ToMetrics(metricsDagr)
	if err != nil || !bytes.Equal(must((&pmetric.ProtoMarshaler{}).MarshalMetrics(md)), metricsPB) {
		t.Fatalf("metrics batch did not round-trip (%v)", err)
	}
	ld, err := logbridge.ToLogs(logsDagr)
	if err != nil || !bytes.Equal(must((&plog.ProtoMarshaler{}).MarshalLogs(ld)), logsPB) {
		t.Fatalf("logs batch did not round-trip (%v)", err)
	}
	pd, err := profilebridge.ToProfiles(profilesDagr)
	if err != nil || !bytes.Equal(must((&pprofile.ProtoMarshaler{}).MarshalProfiles(pd)), profilesPB) {
		t.Fatalf("profiles batch did not round-trip (%v)", err)
	}
	if sweepMetricsProto() != sweepMetricsDagr() || sweepLogsProto() != sweepLogsDagr() ||
		sweepProfilesProto() != sweepProfilesDagr() {
		t.Fatal("a Dagr sweep computed a different checksum than the protobuf one")
	}
}

func TestSignalSizes(t *testing.T) {
	t.Logf("%-9s %10s %10s %7s | %10s %10s %7s", "signal", "proto", "dagr", "ratio", "proto.gz", "dagr.gz", "ratio")
	for _, r := range []struct {
		name     string
		pb, dagr []byte
	}{{"metrics", metricsPB, metricsDagr}, {"logs", logsPB, logsDagr}, {"profiles", profilesPB, profilesDagr}} {
		pz, dz := gz(r.pb), gz(r.dagr)
		t.Logf("%-9s %10d %10d %6.2fx | %10d %10d %6.2fx", r.name, len(r.pb), len(r.dagr),
			float64(len(r.dagr))/float64(len(r.pb)), pz, dz, float64(dz)/float64(pz))
	}
}

// ── sweeps: one field of every item ──────────────────────────────────────────

func sweepMetricsProto() float64 {
	md, err := (&pmetric.ProtoUnmarshaler{}).UnmarshalMetrics(metricsPB)
	if err != nil {
		panic(err)
	}
	var sum float64
	rms := md.ResourceMetrics()
	for i := 0; i < rms.Len(); i++ {
		sms := rms.At(i).ScopeMetrics()
		for j := 0; j < sms.Len(); j++ {
			ms := sms.At(j).Metrics()
			for k := 0; k < ms.Len(); k++ {
				m := ms.At(k)
				switch m.Type() {
				case pmetric.MetricTypeGauge:
					dps := m.Gauge().DataPoints()
					for q := 0; q < dps.Len(); q++ {
						sum += dps.At(q).DoubleValue()
					}
				case pmetric.MetricTypeSum:
					dps := m.Sum().DataPoints()
					for q := 0; q < dps.Len(); q++ {
						sum += float64(dps.At(q).IntValue())
					}
				case pmetric.MetricTypeHistogram:
					dps := m.Histogram().DataPoints()
					for q := 0; q < dps.Len(); q++ {
						sum += float64(dps.At(q).Count())
					}
				}
			}
		}
	}
	return sum
}

func sweepMetricsDagr() float64 {
	root, err := gm.OpenOtlpMetricsUnchecked(metricsDagr)
	if err != nil {
		panic(err)
	}
	var sum float64
	ri := root.ResourceMetricsIter()
	for rm, ok := ri.Next(); ok; rm, ok = ri.Next() {
		si := rm.ScopeMetricsIter()
		for sm, ok := si.Next(); ok; sm, ok = si.Next() {
			mi := sm.MetricsIter()
			for m, ok := mi.Next(); ok; m, ok = mi.Next() {
				data, ok := m.Data()
				if !ok {
					continue
				}
				switch data.Tag() {
				case gm.MetricDataTagGauge:
					it := data.Gauge().DataPointsIter()
					for p, ok := it.Next(); ok; p, ok = it.Next() {
						if v, ok := p.Value(); ok {
							sum += v.AsDouble()
						}
					}
				case gm.MetricDataTagSum:
					it := data.Sum().DataPointsIter()
					for p, ok := it.Next(); ok; p, ok = it.Next() {
						if v, ok := p.Value(); ok {
							sum += float64(v.AsInt())
						}
					}
				case gm.MetricDataTagHistogram:
					it := data.Histogram().DataPointsIter()
					for p, ok := it.Next(); ok; p, ok = it.Next() {
						c, _ := p.Count()
						sum += float64(c)
					}
				}
			}
		}
	}
	return sum
}

// logs: how many records are WARN or worse, and how many body bytes they carry
func sweepLogsProto() int {
	ld, err := (&plog.ProtoUnmarshaler{}).UnmarshalLogs(logsPB)
	if err != nil {
		panic(err)
	}
	n := 0
	rls := ld.ResourceLogs()
	for i := 0; i < rls.Len(); i++ {
		sls := rls.At(i).ScopeLogs()
		for j := 0; j < sls.Len(); j++ {
			lrs := sls.At(j).LogRecords()
			for k := 0; k < lrs.Len(); k++ {
				lr := lrs.At(k)
				if lr.SeverityNumber() >= plog.SeverityNumberWarn {
					n += 1 + len(lr.Body().Str())
				}
			}
		}
	}
	return n
}

func sweepLogsDagr() int {
	root, err := gl.OpenOtlpLogsUnchecked(logsDagr)
	if err != nil {
		panic(err)
	}
	n := 0
	ri := root.ResourceLogsIter()
	for rl, ok := ri.Next(); ok; rl, ok = ri.Next() {
		si := rl.ScopeLogsIter()
		for sl, ok := si.Next(); ok; sl, ok = si.Next() {
			li := sl.LogRecordsIter()
			for lr, ok := li.Next(); ok; lr, ok = li.Next() {
				if sev, _ := lr.SeverityNumber(); sev >= gl.SeverityNumber(plog.SeverityNumberWarn) {
					n++
					if b, ok := lr.Body(); ok && b.Tag() == cg.AnyValueTagStringValue {
						n += len(b.StringValueBytes())
					}
				}
			}
		}
	}
	return n
}

// profiles: total sampled value
func sweepProfilesProto() int64 {
	pd, err := (&pprofile.ProtoUnmarshaler{}).UnmarshalProfiles(profilesPB)
	if err != nil {
		panic(err)
	}
	var sum int64
	rps := pd.ResourceProfiles()
	for i := 0; i < rps.Len(); i++ {
		sps := rps.At(i).ScopeProfiles()
		for j := 0; j < sps.Len(); j++ {
			ps := sps.At(j).Profiles()
			for k := 0; k < ps.Len(); k++ {
				ss := ps.At(k).Samples()
				for q := 0; q < ss.Len(); q++ {
					vs := ss.At(q).Values()
					for v := 0; v < vs.Len(); v++ {
						sum += vs.At(v)
					}
				}
			}
		}
	}
	return sum
}

func sweepProfilesDagr() int64 {
	root, err := gp.OpenOtlpProfilesUnchecked(profilesDagr)
	if err != nil {
		panic(err)
	}
	var sum int64
	ri := root.ResourceProfilesIter()
	for rp, ok := ri.Next(); ok; rp, ok = ri.Next() {
		si := rp.ScopeProfilesIter()
		for sp, ok := si.Next(); ok; sp, ok = si.Next() {
			pi := sp.ProfilesIter()
			for p, ok := pi.Next(); ok; p, ok = pi.Next() {
				smi := p.SamplesIter()
				for s, ok := smi.Next(); ok; s, ok = smi.Next() {
					vi := s.ValuesIter()
					for v, ok := vi.Next(); ok; v, ok = vi.Next() {
						sum += v
					}
				}
			}
		}
	}
	return sum
}

// ── benchmarks ───────────────────────────────────────────────────────────────

var sinkAny any

func BenchmarkSignal(b *testing.B) {
	type pair struct {
		name string
		fn   func()
	}
	mb, lb, pb := metricbridge.NewBuilder(), logbridge.NewBuilder(), profilebridge.NewBuilder()
	ms, ls, ps := metricbridge.NewStreamBuilder(), logbridge.NewStreamBuilder(), profilebridge.NewStreamBuilder()
	var dst []byte
	// The serializer alone, from a value tree built once: encode/dagr minus the bridge's
	// tree construction — the split that located the traces encode cost (§10.2.1).
	mv, lv, pv := metricbridge.ToValue(metricsIn), logbridge.ToValue(logsIn), profilebridge.ToValue(profilesIn)
	mdb, ldb, pdb := dm.NewOtlpMetricsBuilder(), dl.NewOtlpLogsBuilder(), dp.NewOtlpProfilesBuilder()
	for _, sig := range []struct {
		name  string
		cases []pair
	}{
		{"metrics", []pair{
			{"encode/proto", func() { sinkAny = must((&pmetric.ProtoMarshaler{}).MarshalMetrics(metricsIn)) }},
			{"encode/dagr", func() { dst = mb.BuildAppend(dst[:0], metricsIn) }},
			{"encode/dagr_stream", func() { dst = ms.BuildAppend(dst[:0], metricsIn) }},
			{"encode/dagr_serialize_only", func() { dst = mdb.BuildAppend(dst[:0], mv) }},
			{"to_pdata/proto", func() { sinkAny, _ = (&pmetric.ProtoUnmarshaler{}).UnmarshalMetrics(metricsPB) }},
			{"to_pdata/dagr", func() { sinkAny, _ = metricbridge.ToMetrics(metricsDagr) }},
			{"sweep/proto", func() { sinkAny = sweepMetricsProto() }},
			{"sweep/dagr", func() { sinkAny = sweepMetricsDagr() }},
		}},
		{"logs", []pair{
			{"encode/proto", func() { sinkAny = must((&plog.ProtoMarshaler{}).MarshalLogs(logsIn)) }},
			{"encode/dagr", func() { dst = lb.BuildAppend(dst[:0], logsIn) }},
			{"encode/dagr_stream", func() { dst = ls.BuildAppend(dst[:0], logsIn) }},
			{"encode/dagr_serialize_only", func() { dst = ldb.BuildAppend(dst[:0], lv) }},
			{"to_pdata/proto", func() { sinkAny, _ = (&plog.ProtoUnmarshaler{}).UnmarshalLogs(logsPB) }},
			{"to_pdata/dagr", func() { sinkAny, _ = logbridge.ToLogs(logsDagr) }},
			{"sweep/proto", func() { sinkAny = sweepLogsProto() }},
			{"sweep/dagr", func() { sinkAny = sweepLogsDagr() }},
		}},
		{"profiles", []pair{
			// pprofile's marshaler mutates its input on the first call and is idempotent
			// after, so every iteration marshals the same (already-normalised) batch.
			{"encode/proto", func() { sinkAny = must((&pprofile.ProtoMarshaler{}).MarshalProfiles(profilesIn)) }},
			{"encode/dagr", func() { dst = pb.BuildAppend(dst[:0], profilesIn) }},
			{"encode/dagr_stream", func() { dst = ps.BuildAppend(dst[:0], profilesIn) }},
			{"encode/dagr_serialize_only", func() { dst = pdb.BuildAppend(dst[:0], pv) }},
			{"to_pdata/proto", func() { sinkAny, _ = (&pprofile.ProtoUnmarshaler{}).UnmarshalProfiles(profilesPB) }},
			{"to_pdata/dagr", func() { sinkAny, _ = profilebridge.ToProfiles(profilesDagr) }},
			{"sweep/proto", func() { sinkAny = sweepProfilesProto() }},
			{"sweep/dagr", func() { sinkAny = sweepProfilesDagr() }},
		}},
	} {
		for _, c := range sig.cases {
			b.Run(fmt.Sprintf("%s/%s", sig.name, c.name), func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					c.fn()
				}
			})
		}
	}
}
