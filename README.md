# otlp-dagr

OTLP traces as a [Dagr](https://codeberg.org/mzaks/dagr) data graph, and the
measurements that say whether that is worth doing.

This implements steps 1–2 of `spec/38-otlp-on-dagr.md` in the Dagr repo: the schema
skeleton, a `pdata` bridge, and the benchmark that decides the rest — **Dagr's Go lazy
accessors against `ptrace.Unmarshal` + pdata accessors, in nanoseconds and in
allocations.**

## Layout

```
schema.py          the source of truth — transpiled from proto/otlp/, then hand-owned
dagr.lock.json     the generation receipt (committed; `./regen.sh check` diffs it)
proto/otlp/        vendored opentelemetry-proto (Apache-2.0, see PROVENANCE.txt)
regen.sh           dagr build + gofmt/vet
go/                see go/README.md
```

```bash
./regen.sh                                            # regenerate go/gen
cd go && go test ./bench -run Test -v                 # correctness gates
cd go && go test ./bench -bench . -benchmem -benchtime 500x
```

`schema.py` is `packed`, not `frozen+packed`, deliberately — frozen is positional and
gives up schema compatibility entirely, and OTLP is a schema nobody here controls. The
reasoning and the measured cost are in the schema's own docstring and in `spec/38 §2.3`.

## Gates

`go test ./bench -run Test` asserts, before any timing:

- **the batch is the same one the Rust benchmark measures** — `protobuf 233812 B`,
  `dagr 227780 B` (0.97×), byte-for-byte the numbers in `benchmarks/proto_vs_dagr`'s
  README. The Go seed port and the Go direct builder reproduce the Rust batch exactly.
- **both sweeps compute the same checksum**, so a faster read is not a read of
  something else.
- **round trip** — `pdata → Dagr → pdata` preserves the batch (compared as marshalled
  protobuf), and `Dagr → pdata → Dagr` is byte-identical.

## Results

Apple M4 Max, Go 1.22, `pdata v1.44.0`, 500 spans × 8 attributes, `-benchtime 500x`.

### Reads — the allocation story is the story

```
                            ns/op        B/op    allocs/op
Sweep_Proto                320521      691792        15124
Sweep_DagrLazy             475003           0            0
Attr_Proto                 319573      691770        15124
Attr_DagrLazy              508364           0            0
Pinpoint_Proto             323267      691780        15124
Pinpoint_DagrLazy            3557           0            0
Open_Proto                 323537      691770        15124
Open_DagrUnchecked             21.67        0            0
```

**Zero allocations on every read path.** That is the number that matters for a
collector: pdata's GC pressure, not its nanoseconds, is the chronic complaint, and a
lazy Dagr read allocates nothing because it indexes a borrowed `[]byte`.

- **Pinpoint — 91× faster.** Reading four specific spans costs protobuf a full decode
  (323 µs); Dagr indexes straight to them (3.6 µs).
- **Open — 14,900× faster.** `OpenOtlpTracesUnchecked` is 21.7 ns: framing only. There
  is no decode step to pay before the first field read.
- **Sweep — Dagr is currently 1.48× SLOWER, and that is a fixable API gap, not the
  format.** See below.

### The sweep gap: indexed access into a packed array is O(n²)

```
spans     Proto ns    DagrLazy ns
   50        32370           7521      dagr 4.3x FASTER
  100        65017          23785
  250       159319         125809
  500       319668         474070      dagr 1.48x slower
```

protobuf scales linearly (10× the spans → 9.9× the time). Dagr scales **quadratically**
(10× the spans → 63× the time), and crosses over around 300 spans.

The cause is in the generated accessor, not the wire format. A `packed` node array has
no offset table — elements are inlined and self-sizing — so `SpansAt(i)` reaches element
`i` by skipping `i` blobs from the array start:

```go
func (a ScopeSpansAccessor) SpansAt(i int) SpanAccessor {
	...
	for j := 0; j < i; j++ {
		p = dagr.SkipBlob(a.buf, p)   // O(i), and the cursor is thrown away
	}
	return NewSpanAccessor(a.buf, p)
}
```

Sweeping with `At(i)` is therefore O(n²). **The Go lazy API exposes no iterator**, so
there is no way to walk the array once — the reader already advances sequentially
inside `At(i)`, it just discards the cursor on every call. (The Rust lazy reader has
`.iter()`, which is why the Rust benchmark shows a 29× sweep win.)

Fitting `t = a·n + b·n²` to the four points gives `a = 61.8 ns/span`, `b = 1.77 ns`
(predicts the two middle points to within 0.5%). Dropping the quadratic term — i.e.
adding an iterator — projects a 500-span sweep at **≈31 µs against protobuf's 320 µs:
~10× faster, still at zero allocations.**

Until that iterator exists, the honest statement is: **Dagr-Go wins reads decisively
whenever the access is pinpoint or the arrays are small, allocates nothing ever, and
loses a large-array sweep to an O(n²) in its own accessor.**

### Validation is not free

```
Open_DagrValidated        1029903      627073         8026
```

`OpenOtlpTraces` walks the whole reachable graph once so later getters cannot panic on
hostile bytes. That costs **3.2× a protobuf decode** and allocates — it is what a
receiver on an untrusted hop would pay, and it matters for the transport case in
`spec/38 §3`/`§10.1`. Trusted, in-process buffers use `OpenOtlpTracesUnchecked` (21.7 ns).

### Materialising is the slow path, as predicted

```
Materialise_DagrToPdata    934923      547482        13621
Open_Proto (unmarshal)     323537      691770        15124
```

2.9× slower than a protobuf unmarshal. `spec/38 §0.1` predicted ~1.6× from the Rust
numbers; Go is worse. Either way the conclusion is unchanged and now measured in Go:
**a receiver that converts a Dagr batch into pdata is worse off than one that stayed on
protobuf.** The win exists only for a consumer that keeps the buffer.

### Encode — the Rust result does NOT transfer to Go

```
                              ns/op        B/op   allocs/op
Encode_ProtoMarshal           79374      237568           1
Encode_DagrDirect            423306     1809127        6459
Encode_DagrDirectReused      384997     1285744        6441
  ├─ DagrTreeOnly            147495      527042        5628   (bridge: pdata → value tree)
  └─ DagrSerializeOnly       287180     1282079         831   (Dagr: value tree → bytes)
```

In the Rust benchmark the direct builder encodes this batch **1.9× faster than
protobuf**. In Go it is **5.3× slower**. Two distinct causes:

1. **pdata gives protobuf an unbeatable marshal.** `pdata` *is* the generated protobuf
   structs, so `MarshalTraces` is a direct walk into one pre-sized buffer — **1
   allocation** for the whole 234 KB batch. There is no "convert to wire form" step to
   beat, which is exactly the advantage the Rust comparison (prost, from an owned tree)
   did not have to face.
2. **Dagr-Go's direct builder is allocation-heavy.** Even against a pre-built value
   tree it does 831 allocations and 1.28 MB per batch — `dagr.ToOpts` wraps every node
   array in a freshly allocated `[]Opt[T]`, and the builder's buffer growth and
   `MakeData` copy add the rest. The Rust direct builder writes into a caller-owned
   buffer with essentially none of this (`write_into`, 61 µs / 2.01× protobuf).

The bridge's own tree construction (147 µs) is pure overhead that a direct builder
walking pdata accessors straight to bytes would not pay — but even removing it entirely,
the serializer alone is 3.6× protobuf's marshal.

## What this says about the next step

| | verdict |
|---|---|
| **Reads, allocations** | **decisive win** — 0 vs 15,124 allocs/op on every path. This is the collector's actual pain. |
| **Reads, pinpoint / open** | **decisive win** — 91× and 14,900×. |
| **Reads, large-array sweep** | **blocked on a missing iterator** in the Go lazy API; projected ~10× once added. Highest-value fix. |
| **Validated open** | 3.2× a protobuf decode. Fine in-process, a real cost on an untrusted hop. |
| **Materialise to pdata** | loses, as designed. Do not build a pipeline that does this. |
| **Encode from pdata** | **loses today, 5.3×.** Needs a pdata-walking direct builder and allocation work in Dagr-Go's builder before the Rust encode claim can be repeated here. |

So the §14 step-1 gate — "if this does not win decisively on allocations, stop" — is
**passed**, and the two follow-ups it exposes are concrete: a lazy array iterator in
`dagr/codegen/go/core.py`, and the direct builder's allocation profile.

## Licence

The vendored protos under `proto/otlp/` are Apache-2.0, © The OpenTelemetry Authors —
see `proto/otlp/PROVENANCE.txt`. Nothing there is edited.
