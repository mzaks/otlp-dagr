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
Sweep_Proto                319980      691791        15124
Sweep_DagrAll               34277           0            0    9.3x   ← iterator
Sweep_DagrCursor            35200           0            0    9.1x
Sweep_DagrLazy (At)        491327           0            0    0.65x  ← O(n^2), see below
Attr_Proto                 327065      691769        15124
Attr_DagrAll                75321           0            0    4.3x
Attr_DagrLazy (At)         522258           0            0
Pinpoint_Proto             325232      691769        15124
Pinpoint_DagrLazy            3688           0            0    88x
Open_Proto                 321644      691769        15124
Open_DagrUnchecked             17.50        0            0    18400x
```

**Zero allocations on every read path.** That is the number that matters for a
collector: pdata's GC pressure, not its nanoseconds, is the chronic complaint, and a
lazy Dagr read allocates nothing because it indexes a borrowed `[]byte`.

- **Pinpoint — 91× faster.** Reading four specific spans costs protobuf a full decode
  (323 µs); Dagr indexes straight to them (3.6 µs).
- **Open — 14,900× faster.** `OpenOtlpTracesUnchecked` is 21.7 ns: framing only. There
  is no decode step to pay before the first field read.
- **Sweep — 9.3× faster**, once the array is walked with an iterator rather than
  indexed. See below: the first version of this benchmark measured 1.48× *slower*, and
  finding out why produced a fix in Dagr itself.

### The sweep: why `At(i)` is O(n²), and what the iterator fixed

A `packed` node array has no offset table — elements are inlined and self-sizing — so
`SpansAt(i)` reaches element `i` by skipping `i` blobs from the array start, and then
**throws the cursor away**:

```go
func (a ScopeSpansAccessor) SpansAt(i int) SpanAccessor {
	...
	for j := 0; j < i; j++ {
		p = dagr.SkipBlob(a.buf, p)   // O(i), and the cursor is discarded
	}
	return NewSpanAccessor(a.buf, p)
}
```

Sweeping with `At(i)` is therefore O(n²), and the Go lazy API had no iterator, so there
was no way to walk an array once. Dagr's Go generator now emits a cursor that keeps the
position between elements, plus a range-over-func `All()` built on it (Dagr `spec/38`
§10.2, decision D7):

```go
for sp := range ss.SpansAll() { ... }        // range-over-func
it := ss.SpansIter()                         // or the cursor primitive
for sp, ok := it.Next(); ok; sp, ok = it.Next() { ... }
```

The difference is the whole finding:

```
spans     Proto ns    At(i) ns    All() ns
   50        33559        7586        3528
  100        71414       24308        6920
  250       164666      128736       17372
  500       325002      490982       34857
```

`At(i)` scales quadratically (10× the spans → 65× the time, crossing over near 300
spans). `All()` scales **linearly** — 10× the spans, 9.9× the time — and at 500 spans it
is **9.3× faster than protobuf at zero allocations**. A least-squares fit on the old
numbers projected ≈31 µs; measured, it is 34.9 µs.

Range-over-func costs nothing measurable over the raw cursor (34.3 µs vs 35.2 µs), so
`All()` is the form to reach for.

`At(i)` is kept — random access into a short array is legitimate and it is still the
only way to index — but **any sweep should use `All()` / `Iter()`.**

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

### Encode — the serializer is fixed; the bridge is now the cost

```
                              ns/op        B/op   allocs/op
Encode_ProtoMarshal           78445      237570           1
Encode_DagrDirect            322773     1280841        5647   one-shot builder
Encode_DagrDirectAppend      265312      527683        5630   reused builder + dst
  ├─ DagrTreeOnly            141353      527042        5628     …fresh value tree
  └─ DagrSerializeAppend     126405         376           0     Dagr: tree → bytes
Encode_DagrReuseAppend       206989       52529        3317   + reused tree scratch
  └─ DagrTreeOnlyReuse        81063       52126        3316     …bridge fill only
```

The first version of this benchmark found the Dagr serializer doing **831 allocations
and 1.28 MB per batch**. Two fixes in Dagr (`spec/32 §1.46–1.47`) took it to **zero**:

- **`ToOpts` is gone from plain arrays.** Go had one array store per shape taking
  `[]Opt[T]` so it could serve `arrayWithOptionals`; a plain `[T]` was converted per
  array, per encode — ~800 of the 831 allocations. The `*Plain` twins take `[]T`.
- **`BuildAppend` replaces the copy-out.** `Build` ended in `MakeData()`, allocating and
  copying the whole 228 KB record. `BuildAppend(dst, root)` — the Go analogue of Rust's
  `write_into` — appends into a caller-owned slice.

**Serializing an OTLP batch now allocates nothing**, and with `StoreLEB`'s magnitude
fast path (`spec/32 §1.49`) the step went 287 µs → 126 µs (2.27×).

Then the bridge stopped allocating a fresh value tree per batch. `pdatabridge/scratch.go`
fills the tree into storage it owns and reuses; the trick that makes reuse safe is
**sizing first** — every node's children are handed to the builder as a sub-slice of a
shared backing, so a mid-fill `append` that grew the backing would invalidate sub-slices
already handed out. A counting pass over pdata (all `Len()` calls) gives exact totals, the
backings grow once, and the fill only writes indices that already exist. The tree step
went **141 µs → 81 µs** and end to end **265 µs → 207 µs**.

**Against `MarshalTraces` the gap is now 2.64×, from 5.3×.**

Two things still stand between Go and the Rust result:

1. **pdata gives protobuf an unbeatable marshal.** `pdata` *is* the generated protobuf
   structs, so `MarshalTraces` is a direct walk into one pre-sized buffer — **1
   allocation** for the whole 234 KB batch. There is no "convert to wire form" step to
   beat, which is exactly the advantage the Rust comparison (prost, from an owned tree)
   did not have to face.
2. **A true no-tree walker is worth ~81 µs and no more.** Serializing alone is 126 µs
   against protobuf's 78 µs, so even a zero-cost pdata walker lands at **1.61×** — it
   halves the remaining gap and does not win it. It also needs a real Dagr feature: the
   Go direct builder consumes value structs, so streaming from pdata means a
   second, source-driven store path generated per node. That is a `spec/33` addition,
   and it should be weighed against that 1.61× ceiling.

**The last allocations are not the tree's.** 97.2% of what the reused-scratch path still
allocates is Go boxing a `string` into the generated union value's `any`. Giving the value
a dedicated `str` slot removes them — **154 allocations per batch instead of 3,317** — and
costs **7.8%** wall-clock, because the serializer reads every widened element as well as
the filler writing it. Reverted as Dagr's default; `spec/32 §1.48` and `§1.50` have both
measurements. It is a legitimate choice for a collector that is GC-bound rather than
throughput-bound — which is what `pdata` is actually criticised for — just not a silent
one.

## What this says about the next step

| | verdict |
|---|---|
| **Reads, allocations** | **decisive win** — 0 vs 15,124 allocs/op on every path. This is the collector's actual pain. |
| **Reads, pinpoint / open** | **decisive win** — 91× and 14,900×. |
| **Reads, large-array sweep** | **fixed** — 9.3× via the new `All()` / `Iter()`, linear scaling, 0 allocs. `At(i)` in a loop is still O(n²) by construction. |
| **Validated open** | 3.2× a protobuf decode. Fine in-process, a real cost on an untrusted hop. |
| **Materialise to pdata** | loses, as designed. Do not build a pipeline that does this. |
| **Encode from pdata** | **loses, 2.64× — down from 5.3×.** Dagr's serializer allocates nothing and is 2.27× faster; the bridge reuses its tree scratch. A zero-cost no-tree walker would reach 1.61×, no further. |

So the §14 step-1 gate — "if this does not win decisively on allocations, stop" — is
**passed**; step 2 (the lazy array iterator) is **done**; step 3 (the direct builder's
allocation profile) is **done on both sides** — Dagr's serializer allocates nothing, and
the bridge reuses its tree scratch. What is left is a source-driven store path in Dagr's
Go direct builder, whose ceiling is 1.61× protobuf; whether that is worth a `spec/33`
feature is an open question, not a pending task.

## Licence

The vendored protos under `proto/otlp/` are Apache-2.0, © The OpenTelemetry Authors —
see `proto/otlp/PROVENANCE.txt`. Nothing there is edited.
