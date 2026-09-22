// Package seed builds the OTLP trace batch every benchmark here measures.
//
// It is a byte-for-byte port of `otlp_seed()` in the Dagr repo's
// benchmarks/proto_vs_dagr/crate/src/main.rs — same LCG, same constants, same draw
// order — so the Go numbers are directly comparable to the Rust/protobuf table in
// that benchmark's README. One `ResourceSpans` batch: 500 spans, 8 attributes each,
// a 16-byte trace id, an 8-byte span id, a status, and an exception event on the
// errors.
//
// Built once and shared by every format, so no RNG draw-order drift can quietly make
// the comparison dishonest.
package seed

import "fmt"

const NSpans = 500

// AttrKeys are the attribute keys a typical instrumented HTTP service emits, on
// every span. SpanSeed.Attrs is parallel to this.
var AttrKeys = [8]string{
	"http.request.method",
	"url.path",
	"http.response.status_code",
	"server.address",
	"network.protocol.version",
	"user_agent.original",
	"db.system",
	"error",
}

// ExceptionType is the single attribute carried by the exception event on errors.
const ExceptionType = "UpstreamTimeout"

// AttrKind discriminates the AnyValue variant a seeded attribute carries.
type AttrKind uint8

const (
	AttrString AttrKind = iota
	AttrInt
	AttrBool
)

type AttrVal struct {
	Kind AttrKind
	S    string
	I    int64
	B    bool
}

func S(v string) AttrVal { return AttrVal{Kind: AttrString, S: v} }
func I(v int64) AttrVal  { return AttrVal{Kind: AttrInt, I: v} }
func B(v bool) AttrVal   { return AttrVal{Kind: AttrBool, B: v} }

type SpanSeed struct {
	TraceID      []byte
	SpanID       []byte
	ParentSpanID []byte
	Name         string
	Start        uint64
	End          uint64
	Attrs        [8]AttrVal
	OK           bool
	HasEvent     bool // errors carry an exception event
	EventName    string
	EventTime    uint64
}

type Seed struct {
	ResourceAttrs [][2]string
	ScopeName     string
	ScopeVersion  string
	Spans         []SpanSeed
}

// rng is the same LCG the Rust benchmark uses (Knuth's MMIX constants).
type rng struct{ s uint64 }

func (r *rng) next() uint64 {
	r.s = r.s*6364136223846793005 + 1442695040888963407
	return r.s
}

func be64(v uint64) []byte {
	return []byte{byte(v >> 56), byte(v >> 48), byte(v >> 40), byte(v >> 32),
		byte(v >> 24), byte(v >> 16), byte(v >> 8), byte(v)}
}

// New builds the batch. Deterministic: same bytes on every call.
func New() *Seed { return NewN(NSpans) }

// NewN builds the same batch with n spans — used to measure how a read scales with
// span count, which is how the O(n^2) in indexed packed-array access shows itself.
func NewN(n int) *Seed {
	methods := [4]string{"GET", "POST", "PUT", "DELETE"}
	routes := [6]string{
		"/api/v1/users/{id}",
		"/api/v1/orders",
		"/api/v1/orders/{id}/items",
		"/healthz",
		"/api/v1/search",
		"/api/v1/cart/checkout",
	}
	const ua = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
	codes := [6]uint32{200, 200, 200, 201, 404, 500}

	r := &rng{s: 0x07CE1234}
	const baseTS uint64 = 1_758_000_000_000_000_000

	spans := make([]SpanSeed, 0, n)
	for i := 0; i < n; i++ {
		traceID := make([]byte, 0, 16)
		traceID = append(traceID, be64(r.next())...)
		traceID = append(traceID, be64(r.next())...)
		spanID := be64(r.next())
		parentSpanID := be64(r.next())
		method := methods[r.next()%4]
		route := routes[r.next()%6]
		code := codes[r.next()%6]
		start := baseTS + uint64(i)*1_000_000
		dur := 250_000 + r.next()%40_000_000
		ok := code < 400

		s := SpanSeed{
			TraceID:      traceID,
			SpanID:       spanID,
			ParentSpanID: parentSpanID,
			Name:         fmt.Sprintf("%s %s", method, route),
			Start:        start,
			End:          start + dur,
			Attrs: [8]AttrVal{
				S(method),
				S(route),
				I(int64(code)),
				S("api.example.com"),
				S("1.1"),
				S(ua),
				S("postgresql"),
				B(!ok),
			},
			OK: ok,
		}
		if !ok {
			s.HasEvent = true
			s.EventName = "exception"
			s.EventTime = start + dur/2
		}
		spans = append(spans, s)
	}

	return &Seed{
		ResourceAttrs: [][2]string{
			{"service.name", "checkout-api"},
			{"service.version", "2.14.3"},
			{"service.instance.id", "checkout-api-7d9f8b6c4-xk2mq"},
			{"host.name", "ip-10-0-14-203.eu-central-1.compute.internal"},
			{"deployment.environment", "production"},
			{"telemetry.sdk.name", "opentelemetry"},
			{"telemetry.sdk.language", "rust"},
			{"telemetry.sdk.version", "0.31.0"},
		},
		ScopeName:    "opentelemetry-instrumentation-http",
		ScopeVersion: "0.31.0",
		Spans:        spans,
	}
}
