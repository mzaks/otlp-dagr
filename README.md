# otlp-dagr

OTLP traces as a [Dagr](https://codeberg.org/mzaks/dagr) data graph, and the
measurements that say whether that is worth doing.

This implements steps 1–3 and 6 of `spec/38-otlp-on-dagr.md` in the Dagr repo: the
schema, a `pdata` bridge, the benchmark that decides the rest — **Dagr's Go lazy
accessors against `ptrace.Unmarshal` + pdata accessors, in nanoseconds and in
allocations** — and a **collector exporter/receiver pair** that writes and reads an
append-only Dagr spool, built with `ocb` and round-tripped through a real collector.

## Layout

```
schema.py          the source of truth — transpiled from proto/otlp/, then hand-owned
dagr.lock.json     the generation receipt (committed; `./regen.sh check` diffs it)
proto/otlp/        vendored opentelemetry-proto (Apache-2.0, see PROVENANCE.txt)
regen.sh           dagr build + gofmt/vet
go/                see go/README.md
  spool/           the spool file: append, read forward/newest-first, check the tail
  exporter/        dagrfileexporter — pdata in, one Dagr record per batch
  receiver/        dagrfilereceiver  — the spool back into an unmodified pipeline
  cmd/otlpdagrctl/ the round-trip harness (send / spool / verify)
ocb/               collector builder manifest, two pipeline configs, roundtrip.sh
```

```bash
./regen.sh                                            # regenerate go/gen
cd go && go test ./...                                # every gate
cd go && go test ./bench -bench . -benchmem -benchtime 500x
./ocb/roundtrip.sh                                    # build a collector, round trip through it
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
Encode_ProtoMarshal           76388      237571           1
Encode_DagrDirect            280000     1228000        2331   one-shot builder
Encode_DagrDirectAppend      218818      475832        2313   reused builder + dst
  ├─ DagrTreeOnly             97858      475217        2312     …fresh value tree
  └─ DagrSerializeAppend     122886         376           0     Dagr: tree → bytes
Encode_DagrReuseAppend       124478         271           0   + reused tree scratch
  ├─ DagrTreeOnlyReuse        43067         121           0     …bridge fill only
  └─ DagrSerializeAppend      81460         150           0     …Dagr: 1.07× protobuf
Encode_DagrStreamAppend      114674         151           0   no value tree at all
```

**The whole encode path allocates nothing.**

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

Then the generated union value stopped boxing. `AnyValueValue` was `{tag, v any}`, and a
Go interface has **one data word** — a `string` header is two, so the runtime allocated 16
bytes per union value to park a pointer and a length the caller already had. (The text was
never copied either way; `StoreUtf8` does that into the buffer at build time.) Replacing
the interface with a discriminated payload — `num uint64` for scalars, `ptr` + `num` for
utf8/data/slices, a heap cell only for genuinely multi-word values — is **24 bytes, exactly
the width of `{tag, any}`**, so nothing pays for it on the read side:

```
                     {tag, any}          {tag, num, ptr}
  tree fill     81.1 µs / 3,316      42.7 µs / 0 allocs   1.90×
  serialize    126.4 µs / 0         122.9 µs / 0
  end to end   207.0 µs / 3,317     165.9 µs / 0 allocs   1.25×
```

Finally the serializer's *call shape*. Dagr writes the same number of varints gogo does —
about six per attribute — so the gap was never the encoding: it was that gogo's
`MarshalToSizedBuffer` keeps its buffer and cursor in registers for a whole message, while
every Dagr store reloaded a three-word slice header and a cursor through a pointer,
**47,170 times per batch**. Fusing payload + length + tag into one reservation cut that to
31,372 stores and took serialize-only **122.9 µs → 103.2 µs** (`spec/32 §1.52`).

Finally the stores stopped going through the builder at all. Dagr's Go direct builder now
emits `put{N}(n, b, buf, i) ([]byte, int)` beside each `store{N}`, threading the write
position through child calls instead of each node re-acquiring it — with array loops
emitted inline, because handing them to a generic helper with a closure makes every
element an indirect call and costs most of the win (`spec/32 §1.53`).

**The serializer is now 81.4 µs against protobuf's 76.4 — 1.07×, parity.** End to end the
gap against `MarshalTraces` is **1.61×, from 5.3×**, and the whole path still allocates
nothing.

Two things still stand between Go and the Rust result:

1. **pdata gives protobuf an unbeatable marshal.** `pdata` *is* the generated protobuf
   structs, so `MarshalTraces` is a direct walk into one pre-sized buffer — **1
   allocation** for the whole 234 KB batch. There is no "convert to wire form" step to
   beat, which is exactly the advantage the Rust comparison (prost, from an owned tree)
   did not have to face.
2. **A true no-tree walker is worth ~43 µs and no more.** Serializing alone is 103 µs
   against protobuf's ~78 µs, so even a zero-cost pdata walker lands at **1.33×** — and
   the tree fill it would remove is only 43 µs of the 148.
3. ~~The serializer's call shape.~~ **Done** — see above, 1.07× protobuf.
4. ~~This bridge's value tree.~~ **Gone** — `StreamBuilder` walks pdata once, building
   each span on the stack and reusing the scratch behind its attribute and event slices
   (`pdatabridge/stream.go`). 124.5 → 114.7 µs, **1.50× protobuf**. What is left is not
   Dagr: profiling shows the residue is pdata's own accessor API — `Map.Range` takes a
   closure and runs per attribute, 4,000 times a batch, each with a `pcommon.Value` type
   switch. It also needs a real Dagr feature: the
   Go direct builder consumes value structs, so streaming from pdata means a
   second, source-driven store path generated per node. That is a `spec/33` addition,
   and it should be weighed against that 1.61× ceiling.

Two earlier attempts at that boxing *added* typed fields beside `any`, keeping the
interface word and widening the struct to 80 and 40 bytes; both were slower and both were
reverted. The cost belonged to the widening, not to the idea — `spec/32 §1.48`, `§1.50`
and the correction in `§1.51`.

## The exporter/receiver pair

`spec/38 §6.1.1`, and the first thing here that someone outside this repo can run.

An OTLP batch leaves the exporter as one record in a **doubly-linked `DataSink`**
(`SpanLog` in `schema.py`) — an append-only file where every record also carries its own
byte length written backwards at its end. That trailing span is the whole design:

```
[framing] ( [typeId] [content_size] [received_unix_nano | span_count | batch] [span] )*
                                                                        ^^^^^     ^^^^
                                                            raw embedded graph    RLEB
```

- **The batch is stored verbatim.** `traces` is a `raw` embedded graph (`spec/18 §11`),
  so the exporter's job really is "frame and append": the bytes the direct builder
  produced go to disk untouched, and the receiver gets them back as a sub-slice of the
  file. `TestExporterWritesTheBuilderOutputVerbatim` gates that byte for byte. The leaf
  is a genuine standalone Dagr buffer — it reads through `OtlpTraces`' own accessors and
  through `dagr graph-explorer`.
- **The envelope answers questions without opening the batch.** `received_unix_nano` and
  `span_count` are two packed scalars per record, so "the last 500 spans before the
  error" walks backwards reading 24 bytes a record and never decodes a span.
- **Restart recovery is O(1), not O(spool).** The reverse span means a restarting
  exporter validates the *last* record instead of scanning a multi-GB file forward.

### The tail is the part that has to be right

A write-ahead log is only worth the guarantee it makes about its own tail, so this is
where most of the test weight sits.

The naive check — read the trailing span, step back, re-parse the record forward, see
whether the two agree — is not enough, and measurably so. The span's terminating byte
only needs its high bit clear, and a short span only needs two varints to line up, so
arbitrary body bytes satisfy that check by accident: sweeping **every** truncation of a
500-span batch, a depth-1 check accepted several torn files as intact. `spool.CheckTail`
therefore chains the check up to `ValidateDepth` (32) records back — a coincidence cannot
survive that repeatedly — while staying O(1) in the spool's size. Only a genuinely torn
tail pays a forward rescan, once, on a crash.

`TestTornTailIsDetected` runs that sweep: every prefix that cuts into the last record must
be reported torn *and* must recover to exactly the same offset. `spool.Recover` truncates
the damage away, and `TestResumeAfterCrashKeepsOneStream` asserts that the spool a
resumed exporter produces is **byte-identical** to one written in a single run.

### What the round trip actually runs

`./ocb/roundtrip.sh` builds a real collector with the OpenTelemetry Collector Builder —
our two components plus the stock `otlp` receiver and contrib's `file` exporter — and
then:

```
phase A   otlp receiver ──▶ dagrfile exporter ──▶ spans.dagr
phase B   dagrfile receiver ──▶ file exporter ──▶ out.json  ──▶ compared with what was sent
```

```
sent 4 batches of 500 spans to http://127.0.0.1:4318/v1/traces
spool …/spans.dagr: 911213 bytes, 4 batches, 2000 spans, intact tail at 911213 bytes (reverse walk)
  newest batch: 500 spans, received 2026-09-22T17:40:17+02:00, 227780 embedded bytes
verified 2000 spans through the collector round trip
otlpdagrctl: spool tail is torn: torn tail: dropped 97630 bytes after 683410 (forward rescan)
```

The `227780 embedded bytes` is the same number the benchmark reports for the batch, which
is the point: the spool stores the encoder's output and adds 24 bytes of envelope and
framing around it.

### Cost

`go test ./exporter -bench ConsumeTraces -benchtime 2s -count 3`, 500-span batch, spool
at `/dev/null` (median of three):

| | ns/op | allocs/op |
|---|---|---|
| encode (direct builder into a retained buffer) | 115,208 | **0** |
| encode + append one sink record | 124,899 | **0** |

Framing and appending a batch costs **8%** on top of encoding it, and the steady state
allocates nothing — the builder and its destination are held by the exporter and reused.

That second row did not start out at zero. The generated Go sink writer built each
record in a fresh `dagr.Builder` and copied it out with `MakeData()`, which on a
227 KB raw-embedded leaf cost 2 allocations and 496 KB **per batch**. Fixing it in
`dagr/codegen/go/sink.py` (reuse the writer's builder; hand `emit` the builder's own
bytes, which it copies into its scratch anyway) also surfaced a latent aliasing bug the
Dagr repo's own sink-header gate caught immediately: `EnsureFraming` runs from *inside*
`emit`, so writing the stream header through the shared builder would overwrite the
record being emitted. The header now uses a throwaway builder of its own.

### One thing the benchmark could not have caught

`StreamBuilder` — the one-pass pdata walker the encode numbers are measured on — did not
write span **links**. It was written against the seeded corpus, which has none, and it
carried a `HasLinks` guard so a caller could fall back to the value-tree builder. Nothing
called that guard except a test that used it to *skip itself*, so the fast path silently
dropped links, and step 6 is where that path stops being a benchmark and starts being an
exporter in a real collector.

It is fixed (links stream like events, into scratch the next span reuses), and it is now
gated three ways: the streamed bytes must equal the value-tree builder's on a
link-carrying batch, the links must survive a read-back, and the exporter/receiver round
trip runs on a batch with links on every third span. `fixture.TracesWithLinks` is
deliberately separate from `seed`, which stays a byte-for-byte port of the Rust
benchmark's `otlp_seed()`.

The general lesson is about corpora, not links: a fixture chosen to make two encoders
comparable is not a fixture that covers either one.

### The receiver is the slow path, on purpose

`dagrfilereceiver` materialises every batch into pdata, and `spec/38 §0.1` measures eager
restore at ~1.6× slower than a protobuf unmarshal. It exists so an **unmodified** pipeline
can consume a spool, and for nothing else. Do not benchmark otlp-dagr through it.

The fast path is the opposite shape — keep the buffer, read the fields you need in place,
29× a protobuf decode with zero allocations — and it cannot be a receiver, because the
type flowing between collector components is pdata. It is a standalone service reading
the spool directly, and `spool.Reader` is the whole API it needs.

## What this says about the next step

| | verdict |
|---|---|
| **Reads, allocations** | **decisive win** — 0 vs 15,124 allocs/op on every path. This is the collector's actual pain. |
| **Reads, pinpoint / open** | **decisive win** — 91× and 14,900×. |
| **Reads, large-array sweep** | **fixed** — 9.3× via the new `All()` / `Iter()`, linear scaling, 0 allocs. `At(i)` in a loop is still O(n²) by construction. |
| **Validated open** | 3.2× a protobuf decode. Fine in-process, a real cost on an untrusted hop. |
| **Materialise to pdata** | loses, as designed. Do not build a pipeline that does this. |
| **Encode from pdata** | **loses, 1.50× — down from 5.3×, allocation-free, one pass over pdata.** The Dagr serializer itself is at **1.07× — parity**; the residue is pdata's own accessor API. |
| **Exporter → spool** | **runs, in a real collector.** Framing and appending costs ~10% on top of encoding and allocates nothing; the record stores the encoder's output verbatim. |
| **Spool → receiver** | **runs, and loses, as designed** — it materialises. It is the compatibility path, not the fast one. |
| **Crash tail** | **detected, over every truncation of a batch** — and the naive one-record check is not enough to do it (see above). |

So the §14 step-1 gate — "if this does not win decisively on allocations, stop" — is
**passed**; step 2 (the lazy array iterator) is **done**; step 3 (the direct builder's
allocation profile and its call shape) is **done** — the serializer allocates nothing and
runs at 1.07× protobuf — and the bridge's intermediate value tree is **gone**: pdata is
walked once, straight into the buffer, at 1.50× protobuf overall.

Step 6 is **done** too, and it moved the question. Up to here everything was a number;
the exporter/receiver pair is a thing that runs inside an unmodified collector, and what
it demonstrates is not speed but *shape* — an append-only spool that frames what the
encoder produced without touching it, answers "the newest N" from the envelope without
decoding a span, and knows whether its own tail survived the last kill. None of that is
something a protobuf payload in a KV store gives you, and none of it depended on Dagr
being faster.

What step 6 does **not** settle is §5.3: the type flowing between collector components is
still pdata, so the lazy reads that win 29× cannot happen *inside* a pipeline. They
happen in something reading the spool directly. Step 7 (`spool.Reader` as a query
surface: the crash ring, then the tail-sampling buffer) is where that gets built.

## Licence

The vendored protos under `proto/otlp/` are Apache-2.0, © The OpenTelemetry Authors —
see `proto/otlp/PROVENANCE.txt`. Nothing there is edited.
