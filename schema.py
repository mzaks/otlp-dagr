"""
Dagr schema for otlp-dagr — OTLP traces as a Dagr DataGraph.

Transpiled from the vendored `opentelemetry-proto` (proto/otlp/, Apache-2.0) by
`dagr init --from-proto ... --node-style packed --root-message TracesData`.
Regenerate the *code* with `./regen.sh`; this file is the committed source of truth
and may be hand-edited (re-running the transpiler would overwrite it).

LAYOUT: every transpiled message is `packed`, deliberately — see
`spec/38-otlp-on-dagr.md` §2.3 in the Dagr repo. `frozen`/`frozen+packed` is smaller
gzipped (0.97x vs 1.00x protobuf) and reads 1.8x faster, but it is *positional*: no
field tag to skip, no unknown-field tail, and therefore no compatibility at all — not
even adding an optional field (`spec/16` §8.1). OTLP is a schema nobody here controls
and it is visibly in motion: this very snapshot carries `Resource.entity_refs` (marked
`Status: [Development]`), `KeyValue.key_strindex`, `Span.flags` / `Link.flags`, and a
`common.Resource.schema_url` whose comment says it obsoletes the `schema_url` in
ResourceSpans/ResourceMetrics/ResourceLogs. A frozen `Resource` or `KeyValue` would
have been broken by each of those with no forward path for buffers already written.

`packed` costs 3% of gzipped size and 1.8x of read speed against frozen+packed, and
*gains* ~4% on encode (frozen writes presence bitsets). It still meets or beats
protobuf on every dimension the benchmark measures: parity gzipped, 29.1x on a lazy
sweep, 1.91x on encode via the direct builder.

RAW (`spec/39`) is on exactly three fields: the two `Span` nanosecond timestamps and
`Event.time_unix_nano`. `raw` is a claim about MAGNITUDE, not about protobuf's `fixed64`
— a packed writer normally probes `LebLength(v) < width` per value, and `raw` says the
probe can never succeed, so skip it. For u64 that threshold is 2^49, about 6.5 days
after the epoch, so a wall-clock nanosecond timestamp clears it by a factor of ~3,000
and the probe is pure branch (`spec/32` §1.55, ~2% of serialize).

Note what is deliberately NOT raw, because the tempting rule is wrong: `Span.flags` and
`Link.flags` are `fixed32` in the proto too, but they hold small values — for u32 the
threshold is 2^21, and a flags word of 0x101 encodes as a 2-byte LEB. Marking those raw
would make every set flags field 2 bytes LARGER. Protobuf's `fixed32` is a statement
about the wire encoding its author chose; Dagr's `raw` is a statement about the values.
They coincide for timestamps and ids, and diverge for flags and small counters.

A packed ROOT is also what makes the graph direct-buildable (`spec/33` §3), and packed
propagates downward — so any node we later mark `frozen=True` (our own envelope /
index / header types, never a transpiled OTLP message) is promoted to frozen+packed in
place, with the direct builder preserved.
"""

from dagr.dsl import (
    DataGraph, DataSink, ImportedGraph, Node, Enum, UnionType, raw, required, t,
    int_val, float_val, bool_val, string_val, b64_val, array_val,
    union_ref_val, ref_val, nil,
)
from dagr.config import Library, Go  # Rust, Swift, TypeScript, Mojo, Odin, Python


# ENUMS are written as {code: name}, not as a list. The list form auto-indexes from 0,
# which happens to match these protos today — and would stop matching the moment someone
# inserted or reordered a case, silently, with no error and a changed wire value. OTLP's
# enum numbers are fixed by the proto, so the schema states them.
#
# `capacity=255` on both is headroom, and it is free here: an enum's capacity only
# reaches the wire through its raw width (<= 255 is one byte, either way) and through
# sub-byte packing in enum ARRAYS, of which this graph has none. Capacity cannot be
# changed compatibly later, so the choice is now or never — and OTLP has added span
# kinds before. The same argument does NOT extend to the AnyValue union: a union's
# capacity sets the bits per tag in union arrays (<= 16 -> 4 bits, <= 255 -> 8), so
# raising it would cost half a byte per element in every `array_value`, forever, to buy
# room for variants 17..255 that OTLP will not plausibly reach. It keeps its auto
# capacity of 16 — eight variants used, eight spare.
OtlpTraces = DataGraph(
    'OtlpTraces',
    root_type=t.ref('TracesData'),
    node_types=[
        Node('TracesData', fields=[
            'resource_spans' >> t.ref("ResourceSpans").array,
        ], packed=True),
        Node('ResourceSpans', fields=[
            'resource' >> t.ref("Resource"),
            'scope_spans' >> t.ref("ScopeSpans").array,
            'schema_url' >> t.utf8,
        ], packed=True),
        Node('ScopeSpans', fields=[
            'scope' >> t.ref("InstrumentationScope"),
            'spans' >> t.ref("Span").array,
            'schema_url' >> t.utf8,
        ], packed=True),
        Enum('SpanKind', {
            0: 'SPAN_KIND_UNSPECIFIED',
            1: 'SPAN_KIND_INTERNAL',
            2: 'SPAN_KIND_SERVER',
            3: 'SPAN_KIND_CLIENT',
            4: 'SPAN_KIND_PRODUCER',
            5: 'SPAN_KIND_CONSUMER',
        }, capacity=255),
        Node('Event', fields=[
            'time_unix_nano' >> t.u64 >> raw,
            'name' >> t.utf8,
            'attributes' >> t.ref("KeyValue").array,
            'dropped_attributes_count' >> t.u32,
        ], packed=True),
        Node('Link', fields=[
            'trace_id' >> t.data,
            'span_id' >> t.data,
            'trace_state' >> t.utf8,
            'attributes' >> t.ref("KeyValue").array,
            'dropped_attributes_count' >> t.u32,
            'flags' >> t.u32,
        ], packed=True),
        Node('Span', fields=[
            'trace_id' >> t.data,
            'span_id' >> t.data,
            'trace_state' >> t.utf8,
            'parent_span_id' >> t.data,
            'flags' >> t.u32,
            'name' >> t.utf8,
            'kind' >> t.ref("SpanKind"),
            'start_time_unix_nano' >> t.u64 >> raw,
            'end_time_unix_nano' >> t.u64 >> raw,
            'attributes' >> t.ref("KeyValue").array,
            'dropped_attributes_count' >> t.u32,
            'events' >> t.ref("Event").array,
            'dropped_events_count' >> t.u32,
            'links' >> t.ref("Link").array,
            'dropped_links_count' >> t.u32,
            'status' >> t.ref("Status"),
        ], packed=True),
        Enum('StatusCode', {
            0: 'STATUS_CODE_UNSET',
            1: 'STATUS_CODE_OK',
            2: 'STATUS_CODE_ERROR',
        }, capacity=255),
        Node('Status', fields=[
            'message' >> t.utf8,
            'code' >> t.ref("StatusCode"),
        ], packed=True),
        # `message AnyValue { oneof value }` — collapsed to the union itself (no wrapper
        # node), and so are ArrayValue and KeyValueList. Those two exist in the proto for
        # one reason, which the proto says out loud: "We need KeyValueList as a message
        # since `oneof` in AnyValue does not allow repeated fields." That is a protobuf
        # limitation, not a shape in the data — a Dagr union takes an array variant
        # directly. Collapsing them removes a node, an indirection and a length prefix
        # per nested value, and loses nothing: an empty array is still representable,
        # because the variant tag, not the payload, says which variant is present.
        UnionType('AnyValue', types=[
            ('string_value', t.utf8),
            ('bool_value', t.bool),
            ('int_value', t.i64),
            ('double_value', t.f64),
            # array_with_optionals, not array: `repeated AnyValue` can hold an element
            # with no oneof set — pdata calls it ValueTypeEmpty — and a plain array has
            # no way to say "absent". It does not drop the element, which would renumber
            # the slice; it coerces it to the union's zero variant, so ["a", , "c"] comes
            # back as ["a", "", "c"]. The nil bitset costs (count+7)/8 bytes per array
            # and is the only way to round-trip the shape faithfully.
            ('array_value', t.ref("AnyValue").array_with_optionals),
            ('kvlist_value', t.ref("KeyValue").array),
            ('bytes_value', t.data),
            ('string_value_strindex', t.i32),
        ]),
        Node('KeyValue', fields=[
            'key' >> t.utf8,
            'value' >> t.ref("AnyValue"),
            'key_strindex' >> t.i32,
        ], packed=True),
        Node('InstrumentationScope', fields=[
            'name' >> t.utf8,
            'version' >> t.utf8,
            'attributes' >> t.ref("KeyValue").array,
            'dropped_attributes_count' >> t.u32,
        ], packed=True),
        Node('EntityRef', fields=[
            'schema_url' >> t.utf8,
            'type' >> t.utf8,
            'id_keys' >> t.utf8.array,
            'description_keys' >> t.utf8.array,
        ], packed=True),
        Node('Resource', fields=[
            'attributes' >> t.ref("KeyValue").array,
            'dropped_attributes_count' >> t.u32,
            'entity_refs' >> t.ref("EntityRef").array,
        ], packed=True),
    ],
)

# ── The spool: an append-only, doubly-linked DataSink of OTLP batches ─────────
#
# `spec/11` §11: with `doubly_linked=True` every record also stores its byte span as a
# reverse-delimited LEB at the record's END, so a reader can walk newest-first from EOF
# with no forward pass and no index.  That is what makes this a WAL rather than a log
# file: restart recovery reads backwards to the last intact record instead of scanning a
# multi-GB spool forward, and a half-written tail is detectable rather than silently
# truncating the stream (`spec/38` §6.1).
#
# The batch is carried as a `raw` EMBEDDED GRAPH (`spec/18` §11), not as a flattened span
# record.  A `raw` sink field must target an ImportedType — a node from an external
# DataGraph, never a sink-local node — because the leaf must be serialized in the target
# graph's OWN format (cycles, dedup, alignment), which only a real graph has.  The
# consequences are exactly what an exporter/receiver pair wants:
#
#   * the exporter's job is "frame and append" — the bytes the direct builder produced are
#     stored verbatim, so exporter output is byte-identical to `StreamBuilder` output;
#   * the receiver gets the blob back as a sub-slice of the mapped file, so forwarding a
#     batch untouched costs no copy and no decode (`spec/38` §5.2);
#   * the leaf is a genuine standalone Dagr buffer — it pops back out and reads through
#     OtlpTraces' normal readers, including `dagr graph-explorer`.
#
# `received_unix_nano` and `span_count` are the sink's OWN fields, and they are the whole
# point of the envelope: a reverse reader answers "the last N batches, m spans" by reading
# two packed scalars per record and never opening the blob at all.
SpanLog = DataSink(
    'SpanLog',
    imports=[ImportedGraph(OtlpTraces)],
    doubly_linked=True,
    node_types=[
        Node('Batch', fields=[
            'received_unix_nano' >> t.u64 >> required,
            'span_count' >> t.u32 >> required,
            'traces' >> t.ref("TracesData") >> raw >> required,
        ]),
    ],
)


library = Library(
    'otlp_dagr',
    schemas=[OtlpTraces, SpanLog],
    targets=[
        # The Go emitter always writes the lazy accessors, the arena, the serializer
        # and the direct builder (spec/37 §16) — there is no feature list to opt into.
        Go(out="go/gen", package="otlptraces"),
        # Rust(out="rust/gen", features=["lazy", "direct"]),  # §9 Arrow / OTAP bridge
    ],
    wire_format_version=1,
)
