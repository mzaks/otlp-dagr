# otlp-dagr

Every OTLP signal — traces, metrics, logs and profiles — as a
[Dagr](https://codeberg.org/mzaks/dagr) data graph, and the measurements that say whether
that is worth doing.

This implements steps 1–3, 6 and 7 of `spec/38-otlp-on-dagr.md` in the Dagr repo, and
decision D6 (every signal, over one shared `OtlpCommon` — see [Every signal](#every-signal-d6)).
Most of the numbers below are traces, the signal it started with: the
schema, a `pdata` bridge, the benchmark that decides the rest — **Dagr's Go lazy
accessors against `ptrace.Unmarshal` + pdata accessors, in nanoseconds and in
allocations** — a **collector exporter/receiver pair** that writes and reads an
append-only Dagr spool, built with `ocb` and round-tripped through a real collector, and
the **store** that reads that spool backwards: a crash ring and a tail sampler whose
buffer is the file rather than the heap.

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
  store/           the query surface: crash ring, retention, tail sampling
  cmd/otlpdagrctl/ the round-trip harness (send / spool / verify)
ocb/               collector builder manifest, two pipeline configs, roundtrip.sh
```

```bash
./regen.sh                                            # regenerate go/gen
cd go && go test ./...                                # every gate
cd go && go test ./bench -bench . -benchmem -benchtime 500x
cd go && go test ./store -bench . -benchtime 2s        # the store's numbers
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
Materialise_DagrToPdata    488219      674210        14268
Open_Proto (unmarshal)     313386      691771        15124
```

1.56× slower than a protobuf unmarshal. **This used to read 2.9×, and that was the
decoder, not the format:** it looped `{F}At(i)` over every array — spans, attributes,
events, links — and a packed array's `At(i)` walks from the start, so the decode was
O(n²) (the same trap as [the sweep](#the-sweep-why-ati-is-on²-and-what-the-iterator-fixed)).
Walked with the cursors it matches the Rust ratio `spec/38 §0.1` gives, and a CPU
profile puts Dagr's own reads at a few percent: what remains is building a pdata tree
through pdata's setters and the GC it costs. The conclusion stands — **a receiver that
converts a Dagr batch into pdata is worse off than one that stayed on protobuf** — but
it is a cost of materialising pdata, not of reading Dagr. The win exists only for a
consumer that keeps the buffer.

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
[framing] ( [typeId] [content_size] [received_unix_nano | span_count | resource_spans] [span] )*
                                                                        ^^^^^^^^^^^^^^     ^^^^
                                                        the graph's own packed nodes      RLEB
```

- **The batch is stored inline, as the graph's own bytes.** The record holds the batch's
  `resource_spans` array directly — no `TracesData` wrapper, no `raw` embedding. Those
  nodes are packed, and a packed node is already relocatable, size-prefixed bytes; `raw`
  is for a node that is not. The sink writes and reads them with `OtlpTraces`' own
  generated code (Dagr `spec/11` §12), so every node in a record is byte-for-byte the
  node the direct builder writes into a graph buffer (`TestRecordNodesAreTheGraphsBytes`,
  all four signals), and a reader opens a record in place with the same `otlptraces`
  accessors a graph buffer uses. The exporter encodes the record in one pass
  (`StreamBuilder.AppendRecord`); `TestExporterWritesTheBuilderOutputVerbatim` gates that
  what reaches the disk is exactly that record.
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
traces spool …/spans.dagr: 911213 bytes, 4 batches, 2000 items, intact tail at 911213 bytes (reverse walk)
  newest batch: 500 items, received 2026-09-23T16:32:42.997729+02:00, 227799 record bytes
verified 2000 spans through the collector round trip
otlpdagrctl: spool tail is torn: torn tail: dropped 97630 bytes after 683410 (forward rescan)
```

A record is the batch's nodes plus 20 bytes: the envelope scalars and the array's own
tag, size and count. For the benchmark's 500-span batch the nodes are 227,771 B — the
same nodes the 227,780 B graph buffer holds behind its 9 B of framing and root — so the
record is 227,791 B, 9 B smaller than the `raw` embedded form it replaced.

### Cost

`go test ./exporter -bench ConsumeTraces -benchtime 2s -count 3`, 500-span batch, spool
at `/dev/null` (median of three):

| | ns/op | allocs/op |
|---|---|---|
| encode (one pass, straight into the record, retained buffer) | 109,224 | **0** |
| encode + append one sink record | 114,389 | **0** |

Framing and appending a batch costs **5%** on top of encoding it, and the steady state
allocates nothing — the builder and its destination are held by the exporter and reused.
(Measured 2026-09-23 after the batch moved inline; the `raw`-embedded form measured
115,208 / 124,899.)

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

## The store: the crash ring and the tail-sampling spool

`spec/38 §6.2` and `§6.3`, and the half of the plan a collector pipeline cannot host.

The exporter/receiver pair above speaks Dagr at the collector's *edges*, but the type
flowing between components is still pdata, so a processor that wants one field has to
materialise the batch to get it. `go/store/` is what the other half looks like: a
service that reads the spool directly and never builds a pdata object at all.

### Reading backwards, in place

```go
q, _ := store.Open("spans.dagr")
q.LastSpans(500, func(s store.Span) bool {   // newest first, across batch boundaries
    code, _ := s.Acc.Status()                // read out of the mapped file
    return true
})
```

A `store.Span` is a handle, not a span. Two orderings are in play and they are not the
same: batches come newest-first for free — that is the doubly-linked span — while spans
*within* a batch are stored oldest-first, and a packed array's indexed access is O(i).
So a batch is read forward once through the generated iterator into a reusable buffer
and emitted from the end. Sweeping with `At(i)` would be quadratic; the store never does.

The claims are measured rather than asserted, which is what `store.Stats` is for:

| query | batches opened | allocs |
|---|---|---|
| `LastSpans(500)` over 20 × 500-span batches | **1** | 2 |
| `LastSpans(501)` over the same | **2** | 2 |
| `SpanCount()` — every batch, every span | **0** (the envelope carries it) | — |
| a full scan of 1,000 spans vs 4,000 | — | **identical** |

The two allocations are opening the reverse reader, once per walk. Per span it is zero,
and the gate is written as a ratio — quadruple the spool, the number must not move —
because that is the claim that is actually true.

### Retention by compaction, not rotation

`store.Ring` bounds the spool by bytes or batches. When it goes over, it walks backwards
from EOF collecting records until the budget binds, and rewrites just those. **The prefix
it is dropping is never read.** A ring with a 4 MB budget sitting at 400 MB costs 4 MB of
reading to trim, and the test gates the walk length: keeping 4 of 12 records must look at
exactly 5 — the four kept and the one that broke the budget.

Segment files with a directory over them, which is how log systems do this, exist because
their readers cannot start from the end. This one can.

### Tail sampling where the buffer is the file

The collector's `tail_sampling` processor holds every span of every undecided trace in a
map and caps itself at `num_traces` — a cap that exists *because the spans are in RAM*.
Here they are not. The spans are already on disk in a file that reads backwards, so the
only thing memory has to hold is what the policy needs:

```
Decide   walk the window newest-first; read trace_id, start, end and status.code off each
         span in place; fold them into one flat 64-byte Summary per trace. No spans, no
         accessors, no batches are kept.
Emit     walk again for the traces the policy accepted, and hand out their spans. Only
         the sampled fraction is touched twice; only the sampled fraction is ever
         materialised.
```

Walking twice is precisely what the in-memory design cannot afford and this one can: the
second pass re-reads a file already in the page cache, against pinning every span of
every open trace in the heap for the length of the decision wait.

**The same decision, over the same spool, 400 traces / 1,199 spans / 13.2% sampled:**

| | ns/op | B/op | allocs/op |
|---|---|---|---|
| lazy — fields read in place | **204,248** | **10,648** | **19** |
| via pdata — `ToTraces`, then walk | 869,129 | 467,171 | 7,837 |
| | **4.3×** | **44× less** | **412× fewer** |

Both paths agree on the verdict before either is timed. The 19 allocations are the
*result* — the sampled summaries and the keep set; the working state is reused, and the
gate for that is again a ratio: deciding 400 traces must not allocate four times what
deciding 100 does.

`BenchmarkLastSpans` — the crash-ring query, 500 spans out of 10,000 — is **67.6 µs, 2
allocs, 1 batch opened.**

### A corpus bug, again

`fixture.NewSampling` derived its trace ids byte-wise from the trace index, which wraps
at 256: a 400-trace corpus quietly became a 256-trace one, surplus traces merged into
existing ids, every per-trace expectation silently wrong. Nothing failed — the benchmark
just printed a trace count that did not match its input, which is the only reason it was
noticed. The id is injective now and a test gates it at 60, 256, 400 and 1,000 traces.

That is the second time in two steps that the fixture, not the code, was the thing that
was wrong. The [links](#one-thing-the-benchmark-could-not-have-caught) case was a corpus
that did not cover a feature; this one was a corpus that did not contain what it said it
did.

## Nested attribute values

The third gap of the same family, and the one the schema had to move to close.

`AnyValue` can hold a slice or a map, and the bridge did neither: `attrValue` returned
"unsupported" for `ValueTypeSlice` and `ValueTypeMap`, and `keyValues` dropped the
attribute. Real OTLP traffic carries both — `http.request.header.*` is a slice on every
HTTP span — so the encoder was silently discarding data.

Three things had to be true for it to work:

**The wrappers had to go.** `ArrayValue` and `KeyValueList` exist in the proto only
because "`oneof` in AnyValue does not allow repeated fields". Collapsing them lets a
nested value map onto `array_value` / `kvlist_value` directly. That is a Dagr generator
fix away: Go emitted a one-return getter for a variant whose array view returns
`(element, present)`, an arity mismatch only a union variant holding an array *of a
union* ever reaches — and the spec corpus has `union_arr_node`, `_enum` and `_prim`, but
no array-of-union.

**`array_value` had to become `array_with_optionals`.** `repeated AnyValue` may contain
an element with no `oneof` set; pdata calls it `ValueTypeEmpty`. A plain Dagr array has
no way to say "absent", and it does not drop the element — it coerces it to the union's
zero variant, so `["a", , "c"]` came back as `["a", "", "c"]`, the right length and the
wrong types. The nil bitset costs `(count+7)/8` bytes per array and is the only faithful
encoding. `TestNestedEdgeShapes` pins it, along with empty slices, empty maps, and a
present key whose value is absent.

**Nested values allocate, and the flat path must not.** Each level hands the builder a
slice it keeps a pointer into, so that storage cannot be scratch the next attribute
overwrites. `StreamBuilder` therefore stays allocation-free for scalar-only batches —
which is nearly all traffic and all of the benchmark — and allocates in proportion to
nesting when nesting is present. `TestFlatEncodeStillAllocatesNothing` guards the first
half of that sentence.

The decoder is recursive over a structure a writer chooses, so `MaxValueDepth` (32)
bounds it; past the limit a value is left empty rather than truncated into something
that reads like data. Protobuf's own decoder takes the same precaution.

## Every signal (D6)

Metrics, logs and profiles (`v1development`) joined traces on 2026-09-23. `schema.py`
declares `common.proto` + `resource.proto` once, as `OtlpCommon`, and each signal imports
it; each signal has its own generated Go package (`otlpgen/otlp{traces,metrics,logs,profiles}`,
`layout="package-per-graph"`), its own pdata bridge (`pdatabridge`, `metricbridge`,
`logbridge`, `profilebridge`), its own spool (`spool.Traces` … `spool.Profiles`), and the
one `dagrfile` exporter/receiver serves all four. `./ocb/roundtrip.sh` runs every signal
through a real collector.

Realistic batches (metrics 600 data points; logs 500 records; profiles 2,000 samples over
a 400-function dictionary), `go test ./bench -run TestSignalSizes -v` and
`-bench BenchmarkSignal` (encode = `encode/dagr_stream`):

```
                 size    size.gz |  encode, 0 allocs (serializer only) |  to pdata        |  lazy sweep
metrics   proto 122,327   14,107 |  40.6us                             |  188us           |  191us (unmarshal+walk)
          dagr   92,626   13,046 |  82.0us (62.5us)   2.0x              |  358us     1.90x |  41.5us, 0 allocs   4.6x faster
logs      proto 119,923   23,697 |  32.3us                             |  149us           |  150us
          dagr  115,683   23,773 |  54.3us (36.8us)   1.7x              |  221us     1.48x |  27.8us, 0 allocs   5.4x faster
profiles  proto  93,407   28,997 |  52.7us                             |  229us           |  231us
          dagr   98,309   29,447 |   105us (73.5us)   2.0x              |  640us     2.80x |  112us, 0 allocs    2.1x faster
```

- **Size holds** — parity gzipped everywhere, and metrics is the best case in the whole
  benchmark (0.76× raw): a histogram's `bucket_counts` / `explicit_bounds` cost Dagr no
  per-array tag and length on every data point.
- **Lazy reads hold** — 2.1–5.4× with zero allocations.
- **Encode is 1.7–2.0×, allocation-free.** It was 2.5–3.5×. Two causes, both fixed on
  2026-09-23. Half the time went into copying pdata into a value tree first (1,004–6,612
  allocations a batch); each signal now has a one-pass `StreamBuilder`, like traces,
  that writes the containers by hand and builds one leaf at a time in reused scratch.
  And the serializer spent a third of its time in numeric arrays (`bucket_counts`,
  `explicit_bounds`, profiles' index and value arrays), through a generic store with a
  function call per element; Dagr's Go runtime now has a specialised loop per element
  type (serializer −8% metrics, −18% profiles). What is left: the serializer itself
  (1.15–1.4× protobuf — Dagr decides LEB-or-fixed per number, protobuf writes varints
  blind) and reading pdata into scratch (17–31 µs a batch).

**Profiles is faithful to the proto**: every cross-reference stays an integer index into
a `ProfilesDictionary` table, so the bridge is a straight copy. That is protobuf's
workaround for having no references — a graph-native variant (node refs, no string table)
is the obvious next thing to measure (`spec/38` §2.6). Beware `pprofile.ProtoMarshaler`:
it mutates the batch it marshals (attribute keys move into the string table), so compare
from fresh copies.

**Coverage is a gate.** Each signal has a WIDE fixture (`internal/fixture/wide.go`) and
`internal/coverage` proves it reaches every field and union variant by walking the value
tree — the fourth corpus bug in four steps was every traces encoder dropping the scope's
attributes and the resource's dropped count, which the seed never sets. Exempt are the
three fields pdata v1.67 cannot express: `Resource.entity_refs`, `KeyValue.key_strindex`,
`AnyValue.string_value_strindex`.

**Profiles as a graph — measured, and it loses.** `OtlpProfilesGraph` replaces every
dictionary index with a node reference (shared entries stored once, no string table;
`go/profilegraphbridge`, `go test ./bench -run TestProfilesGraphSizes -v`):

```
            size      size.gz  | encode    to pdata  | shallow sweep   deep read (stack -> fn names)
proto       93,407    28,997   | 54us      232us     | 235us           310us (unmarshal incl.)
faithful    98,309    29,447   | 195us     675us     | 113us           1.89ms
graph      130,817    56,836   | 1.06ms    580us     | 27.7us          1.36ms
```

(The graph's reads are after the Go generator stopped re-resolving an array field for
every element of a regular-node cursor — before that they were 71 µs and 2.02 ms.)

Samples dominate a profile, and a regular (vtable) sample node costs 36.9 B against
protobuf's 23.4; references are buffer offsets where protobuf has small repeated indices,
which gzip loves and offsets defeat. The deep walk loses in BOTH Dagr layouts — a stack
walk reaches the same functions thousands of times and the lazy reader resolves each one
every time, where protobuf decodes each once. The graph wins only the shallow sweep
(8.5× protobuf). Ship the faithful `OtlpProfiles`; `spec/38` §2.6 has the analysis.

**Every spool says what it holds.** Each of the four sinks carries the same
`SpoolHeader` (`schema.py`, Dagr `spec/15`), written once at stream open: the signal as
a code and as its graph's name, the creation time, the receipt's `wire_format_version`,
the vendored opentelemetry-proto version and the producer (the exporter's `producer`
setting, by default the collector's `service.instance.id` plus the component ID). Opening
or resuming a spool as the wrong signal is refused before anything is truncated,
`spool.OpenAny` / `otlpdagrctl spool` read the signal off the header, and resume and ring
compaction keep the header a spool was created with. It costs 91 bytes per spool, once, most of them the producer string.
Adding it was a wire break (`spec/15` §11), and spools written before it fail with
`spool.ErrNoHeader`.

## OTAP: Dagr into OpenTelemetry Arrow

`rust/otap` (binary `dagr2otap`) turns a traces, logs or metrics batch into an OTAP
`BatchArrowRecords` — the input of otel-arrow's receivers — reading the Dagr bytes in place
through the generated lazy accessors. No value tree and no protobuf anywhere in the chain.

```
rust/otap/check.sh     # fixtures -> dagr2otap -> otel-arrow's Go consumer AND its Rust decoder
rust/otap/bench.sh     # vs otel-arrow's own OTLP -> OTAP path, same batches
dagr2otap spool spans.dagr   # an exporter spool -> one OTAP batch per record
```

It reads the exporter's spools too — `Producer::spool`: the header says which signal, and
each record (which holds its batch inline, in the signal graph's layout) converts through
the same accessors a graph buffer does, straight from the file. A torn tail stops the
conversion where `spool.Recover` would cut it.

The gate is otel-arrow's own code: the Go consumer the collector's `otelarrowreceiver` runs
must give back each fixture (otel-arrow's `assert.Equiv`, plus exact protobuf bytes where no
kvlist is nested in a value), and the Rust decoder — the strict schema check — must too.

On a stream — 20 consecutive batches of 500, sharing vocabulary but not ids or values, one
producer each, as an exporter sends them:

```
stream     dagr µs/batch  otel-arrow  speedup   bytes dagr / o-a      zstd dagr / o-a
traces          361.4        502.0     1.39x   1,488,452 1,709,058    571,036  634,714
logs            216.5        218.6     1.01x   1,651,568 1,610,338    533,680  556,962
metrics         231.6        338.5     1.46x   1,864,890 1,829,432    379,088  416,014
```

Compressed, as OTAP is sent, the output is 4–10% smaller than otel-arrow's own producer,
which reads OTLP through zero-copy protobuf views and already skips prost. The producer
dictionary-encodes per column, keeps its IPC streams and dictionaries for the life of a
connection (one `Producer` per receiver stream), and zstd-compresses on request
(`with_zstd`). Profiles have no OTAP mapping (none upstream). Metric `metadata` is dropped
by default: the Go consumer rejects the payload that carries it
(`Producer::with_metric_metadata` keeps it, for Rust receivers). `spec/38` §9 has the contract, the three upstream findings
the gate turned up, and the numbers.

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
| **Crash ring** | **decisive** — 500 newest spans out of 10,000 in 67.6 µs, one batch opened, 2 allocs. Retention never reads what it drops. |
| **Tail sampling** | **decisive** — the same decision is 4.3× faster and allocates 412× less than taking it through pdata, and the buffer is the file rather than the heap. |

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

What step 6 did **not** settle was §5.3: the type flowing between collector components is
still pdata, so the lazy reads that win 29× cannot happen *inside* a pipeline. They
happen in something reading the spool directly — which is step 7, and it is built:
`go/store/` is that something. The crash ring answers "the last 500 spans" in 67.6 µs
touching one batch, and the tail sampler takes the same decision a collector processor
takes, 4.3× faster and allocating 412× less, with its buffer on disk instead of capped
in RAM.

So the shape the whole plan has been arguing toward is now assembled end to end: Dagr at
the collector's edges (step 6), and the reads that justify it in a service that owns the
buffer (step 7). What is left is §14 step 8 — the Arrow bridge and/or Go path query —
over all four signals now that D6 is done, and the open decisions in §12. D2 is settled
for the spools (they carry a header); what it still leaves open is a header on the
signal GRAPHS themselves.

## Licence

The vendored protos under `proto/otlp/` are Apache-2.0, © The OpenTelemetry Authors —
see `proto/otlp/PROVENANCE.txt`. Nothing there is edited.
