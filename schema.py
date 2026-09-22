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

A packed ROOT is also what makes the graph direct-buildable (`spec/33` §3), and packed
propagates downward — so any node we later mark `frozen=True` (our own envelope /
index / header types, never a transpiled OTLP message) is promoted to frozen+packed in
place, with the direct builder preserved.
"""

from dagr.dsl import (
    DataGraph, Node, Enum, UnionType, required, t,
    int_val, float_val, bool_val, string_val, b64_val, array_val,
    union_ref_val, ref_val, nil,
)
from dagr.config import Library, Go  # Rust, Swift, TypeScript, Mojo, Odin, Python


OtlpTraces = DataGraph(
    'OtlpTraces',
    root_type=t.ref('TracesData'),
    node_types=[
        Enum('SpanFlags', {0: 'SPAN_FLAGS_DO_NOT_USE', 255: 'SPAN_FLAGS_TRACE_FLAGS_MASK', 256: 'SPAN_FLAGS_CONTEXT_HAS_IS_REMOTE_MASK', 512: 'SPAN_FLAGS_CONTEXT_IS_REMOTE_MASK'}, capacity=65535),
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
        Enum('SpanKind', ['SPAN_KIND_UNSPECIFIED', 'SPAN_KIND_INTERNAL', 'SPAN_KIND_SERVER', 'SPAN_KIND_CLIENT', 'SPAN_KIND_PRODUCER', 'SPAN_KIND_CONSUMER']),
        Node('Event', fields=[
            'time_unix_nano' >> t.u64,
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
            'start_time_unix_nano' >> t.u64,
            'end_time_unix_nano' >> t.u64,
            'attributes' >> t.ref("KeyValue").array,
            'dropped_attributes_count' >> t.u32,
            'events' >> t.ref("Event").array,
            'dropped_events_count' >> t.u32,
            'links' >> t.ref("Link").array,
            'dropped_links_count' >> t.u32,
            'status' >> t.ref("Status"),
        ], packed=True),
        Enum('StatusCode', ['STATUS_CODE_UNSET', 'STATUS_CODE_OK', 'STATUS_CODE_ERROR']),
        Node('Status', fields=[
            'message' >> t.utf8,
            'code' >> t.ref("StatusCode"),
        ], packed=True),
        # `message AnyValue { oneof value }` — collapsed to the union itself (no wrapper node)
        UnionType('AnyValue', types=[
            ('string_value', t.utf8),
            ('bool_value', t.bool),
            ('int_value', t.i64),
            ('double_value', t.f64),
            ('array_value', t.ref("ArrayValue")),
            ('kvlist_value', t.ref("KeyValueList")),
            ('bytes_value', t.data),
            ('string_value_strindex', t.i32),
        ]),
        Node('ArrayValue', fields=[
            'values' >> t.ref("AnyValue").array,
        ], packed=True),
        Node('KeyValueList', fields=[
            'values' >> t.ref("KeyValue").array,
        ], packed=True),
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

library = Library(
    'otlp_dagr',
    schemas=[OtlpTraces],
    targets=[
        # The Go emitter always writes the lazy accessors, the arena, the serializer
        # and the direct builder (spec/37 §16) — there is no feature list to opt into.
        Go(out="go/gen", package="otlptraces"),
        # Rust(out="rust/gen", features=["lazy", "direct"]),  # §9 Arrow / OTAP bridge
    ],
    wire_format_version=1,
)
