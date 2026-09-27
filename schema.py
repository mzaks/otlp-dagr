"""
Dagr schema for otlp-dagr — every OTLP signal as a Dagr DataGraph.

Transpiled from the vendored `opentelemetry-proto` (proto/otlp/, Apache-2.0) by
`dagr init --from-proto ... --node-style packed --root-message TracesData`, and the other
three signals by the same importer (`MetricsData`, `LogsData`, `ProfilesData`).
Regenerate the *code* with `./regen.sh`; this file is the committed source of truth
and may be hand-edited (re-running the transpiler would overwrite it).

SIGNALS: traces, metrics, logs and profiles (`v1development` — the proto says `Status:
[Alpha]`, and pdata ships it as its own `pprofile` module, v0.x). The shared messages of
`common.proto` and `resource.proto` live ONCE, in `OtlpCommon`, and each signal imports
it — `spec/38` §2.5, and the D6 question of whether that split holds. The Go target is
built with `layout="package-per-graph"`, under which Dagr emits `OtlpCommon` once as its
own package pair (`otlpgen/otlpcommon` + `…direct`) that the four signal packages refer
to (Dagr spec/37 §18); Rust flattens the import, so each signal's crate module carries
its own copy. The wire is the same either way, since a packed field is addressed by its
position and never by a type id.

LAYOUT: every transpiled message is stored `packed`, deliberately — see
`spec/38-otlp-on-dagr.md` §2.3 in the Dagr repo. `frozen`/`frozen+packed` is smaller
gzipped (0.97x vs 1.00x protobuf) and reads 1.8x faster, but it is *positional*: no
field tag to skip, no unknown-field tail, and therefore no compatibility at all — not
even adding an optional field (`spec/16` §8.1). OTLP is a schema nobody here controls
and it is visibly in motion: this very snapshot carries `Resource.entity_refs` (marked
`Status: [Development]`), `KeyValue.key_strindex`, `Span.flags` / `Link.flags`, and a
`common.Resource.schema_url` whose comment says it obsoletes the `schema_url` in
ResourceSpans/ResourceMetrics/ResourceLogs. A frozen `Resource` or `KeyValue` would
have been broken by each of those with no forward path for buffers already written.
Profiles moves faster still (`stack_table` is `[Since v1.8.0]`).

`packed` costs 3% of gzipped size and 1.8x of read speed against frozen+packed, and
*gains* ~4% on encode (frozen writes presence bitsets). It still meets or beats
protobuf on every dimension the benchmark measures: parity gzipped, 29.1x on a lazy
sweep, 1.91x on encode via the direct builder.

RAW (`spec/39`) is on the nanosecond wall-clock timestamps and nothing else. `raw` is a
claim about MAGNITUDE, not about protobuf's `fixed64` — a packed writer normally probes
`LebLength(v) < width` per value, and `raw` says the probe can never succeed, so skip it.
For u64 that threshold is 2^49, about 6.5 days after the epoch, so a wall-clock
nanosecond timestamp clears it by a factor of ~3,000 and the probe is pure branch
(`spec/32` §1.55, ~2% of serialize).

What is deliberately NOT raw, although the proto says `fixed`, because the tempting rule
is wrong for each of them:

  * `Span.flags`, `Link.flags`, `LogRecord.flags` (`fixed32`) — small values. For u32 the
    threshold is 2^21, and a flags word of 0x101 encodes as a 2-byte LEB; raw would make
    every set flags field 2 bytes LARGER.
  * the metric `count`, `zero_count` and `bucket_counts` (`fixed64`) — counts, almost
    always far below 2^49.
  * `NumberValue.as_int` (`sfixed64`) — a gauge reading of any magnitude.
  * `Mapping.memory_start` / `memory_limit` / `Location.address` (`uint64` in the proto,
    listed because they LOOK like the case for raw) — user-space addresses on x86-64 and
    arm64 sit around 2^46–2^47, below the 2^49 threshold, so the probe does win there.
  * `Profile.duration_nano` — a duration, not a wall-clock time; a 10 s profile is 2^33.

`start_time_unix_nano` IS raw even though a gauge often leaves it unset: raw only governs
how a PRESENT value is written, and the bridge writes the field only when it is non-zero,
exactly as protobuf elides a proto3 default.

A packed ROOT is also what makes a graph direct-buildable (`spec/33` §3), and packed
propagates downward — so any node we later mark `frozen=True` (our own envelope /
index / header types, never a transpiled OTLP message) is promoted to frozen+packed in
place, with the direct builder preserved.
"""

from dagr.dsl import (
    DataGraph, DataSink, SharedBuffer, cap,
    ImportedGraph, Node, Enum, UnionType, raw, required, t, timestamp,
    int_val, float_val, bool_val, string_val, b64_val, array_val,
    union_ref_val, ref_val, nil,
)
from dagr.config import Library, Go, Rust  # Swift, TypeScript, Mojo, Odin, Python


# ENUMS are written as {code: name}, not as a list. The list form auto-indexes from 0,
# which happens to match these protos today — and would stop matching the moment someone
# inserted or reordered a case, silently, with no error and a changed wire value. OTLP's
# enum numbers are fixed by the proto, so the schema states them.
#
# `capacity=255` on every enum is headroom, and it is free here: an enum's capacity only
# reaches the wire through its raw width (<= 255 is one byte, either way) and through
# sub-byte packing in enum ARRAYS, of which these graphs have none. Capacity cannot be
# changed compatibly later, so the choice is now or never — and OTLP has added span
# kinds before. The same argument does NOT extend to the unions: a union's capacity sets
# the bits per tag in union arrays (<= 16 -> 4 bits, <= 255 -> 8), so raising it would
# cost half a byte per element in every `array_value`, forever, to buy room for variants
# 17..255 that OTLP will not plausibly reach. They keep the auto capacity of 16.
#
# Two proto enums are NOT here: `DataPointFlags` and `LogRecordFlags`. Both are bit masks
# for a `flags` field the proto itself types as a plain integer, so no field ever holds
# one — the importer emits them because they are declared, not because they are used.


# ── Shared: common.proto + resource.proto ─────────────────────────────────────
#
# No root: nothing serializes an `OtlpCommon` on its own. It exists to be imported.
#
# No layout either: these nodes are declared REGULAR and take the layout of whoever
# holds them, because packed propagates downward (`spec/16` §3). Under the four packed
# signals they are packed — byte-identical to declaring them so, and read through the
# generated `{Node}PackedAccessor` — while under OtlpProfilesGraph's regular nodes they
# are vtable nodes like their parents, and a shared Resource is stored once. Declaring
# them `packed` here had forced the packed layout on every importer, whatever it chose.
OtlpCommon = DataGraph(
    'OtlpCommon',
    node_types=[
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
        ]),
        Node('InstrumentationScope', fields=[
            'name' >> t.utf8,
            'version' >> t.utf8,
            'attributes' >> t.ref("KeyValue").array,
            'dropped_attributes_count' >> t.u32,
        ]),
        Node('EntityRef', fields=[
            'schema_url' >> t.utf8,
            'type' >> t.utf8,
            'id_keys' >> t.utf8.array,
            'description_keys' >> t.utf8.array,
        ]),
        Node('Resource', fields=[
            'attributes' >> t.ref("KeyValue").array,
            'dropped_attributes_count' >> t.u32,
            'entity_refs' >> t.ref("EntityRef").array,
        ]),
    ],
)


# ── Traces: trace/v1/trace.proto ──────────────────────────────────────────────
OtlpTraces = DataGraph(
    'OtlpTraces',
    root_type=t.ref('TracesData'),
    imports=[ImportedGraph(OtlpCommon)],
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
            'time_unix_nano' >> t.u64 >> raw >> timestamp(unit='ns'),
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
            'start_time_unix_nano' >> t.u64 >> raw >> timestamp(unit='ns'),
            'end_time_unix_nano' >> t.u64 >> raw >> timestamp(unit='ns'),
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
    ],
)


# ── Metrics: metrics/v1/metrics.proto ─────────────────────────────────────────
#
# Renames against the transpile, all API-only (a packed field is addressed by position,
# a union variant by index — no name reaches the wire):
#   * `Metric.description` -> `description_text`: `description` is rejected for every
#     target by `dagr/reserved_names.py` (Swift's CustomStringConvertible owns it), and
#     the importer's `description_` fallback would surface as `Description_()` in Go.
#   * the `Metric.data` oneof -> `MetricData`, not the importer's bare `Data`.
#   * `NumberDataPoint.value` and `Exemplar.value` are the SAME oneof (`as_double = 4`,
#     `as_int = 6`) declared twice; the importer named them `Value` and `ExemplarValue`.
#     One union, `NumberValue`, serves both.
OtlpMetrics = DataGraph(
    'OtlpMetrics',
    root_type=t.ref('MetricsData'),
    imports=[ImportedGraph(OtlpCommon)],
    node_types=[
        Node('MetricsData', fields=[
            'resource_metrics' >> t.ref("ResourceMetrics").array,
        ], packed=True),
        Node('ResourceMetrics', fields=[
            'resource' >> t.ref("Resource"),
            'scope_metrics' >> t.ref("ScopeMetrics").array,
            'schema_url' >> t.utf8,
        ], packed=True),
        Node('ScopeMetrics', fields=[
            'scope' >> t.ref("InstrumentationScope"),
            'metrics' >> t.ref("Metric").array,
            'schema_url' >> t.utf8,
        ], packed=True),
        Enum('AggregationTemporality', {
            0: 'AGGREGATION_TEMPORALITY_UNSPECIFIED',
            1: 'AGGREGATION_TEMPORALITY_DELTA',
            2: 'AGGREGATION_TEMPORALITY_CUMULATIVE',
        }, capacity=255),
        UnionType('MetricData', types=[
            ('gauge', t.ref("Gauge")),
            ('sum', t.ref("Sum")),
            ('histogram', t.ref("Histogram")),
            ('exponential_histogram', t.ref("ExponentialHistogram")),
            # Summary is collapsed into the variant: it is a node holding one array and
            # nothing else, and OTLP documents it as legacy ("not recommended for new
            # applications"), so it will not grow a field that would need the node back.
            ('summary', t.ref("SummaryDataPoint").array),
        ]),
        Node('Metric', fields=[
            'name' >> t.utf8,
            'description_text' >> t.utf8,
            'unit' >> t.utf8,
            'metadata' >> t.ref("KeyValue").array,
            'data' >> t.ref('MetricData'),
        ], packed=True),
        Node('Gauge', fields=[
            'data_points' >> t.ref("NumberDataPoint").array,
        ], packed=True),
        Node('Sum', fields=[
            'data_points' >> t.ref("NumberDataPoint").array,
            'aggregation_temporality' >> t.ref("AggregationTemporality"),
            'is_monotonic' >> t.bool,
        ], packed=True),
        Node('Histogram', fields=[
            'data_points' >> t.ref("HistogramDataPoint").array,
            'aggregation_temporality' >> t.ref("AggregationTemporality"),
        ], packed=True),
        Node('ExponentialHistogram', fields=[
            'data_points' >> t.ref("ExponentialHistogramDataPoint").array,
            'aggregation_temporality' >> t.ref("AggregationTemporality"),
        ], packed=True),
        UnionType('NumberValue', types=[
            ('as_double', t.f64),
            ('as_int', t.i64),
        ]),
        Node('NumberDataPoint', fields=[
            'attributes' >> t.ref("KeyValue").array,
            'start_time_unix_nano' >> t.u64 >> raw >> timestamp(unit='ns'),
            'time_unix_nano' >> t.u64 >> raw >> timestamp(unit='ns'),
            'exemplars' >> t.ref("Exemplar").array,
            'flags' >> t.u32,
            'value' >> t.ref('NumberValue'),
        ], packed=True),
        # `sum`, `min`, `max` are proto3 `optional double`: presence is part of the data
        # (a histogram of negative observations has no meaningful sum). A Dagr field is
        # optional by default and absence is the missing field, so the bridge writes them
        # exactly when pdata's HasSum/HasMin/HasMax is true — 0.0 stays representable.
        Node('HistogramDataPoint', fields=[
            'attributes' >> t.ref("KeyValue").array,
            'start_time_unix_nano' >> t.u64 >> raw >> timestamp(unit='ns'),
            'time_unix_nano' >> t.u64 >> raw >> timestamp(unit='ns'),
            'count' >> t.u64,
            'sum' >> t.f64,
            'bucket_counts' >> t.u64.array,
            'explicit_bounds' >> t.f64.array,
            'exemplars' >> t.ref("Exemplar").array,
            'flags' >> t.u32,
            'min' >> t.f64,
            'max' >> t.f64,
        ], packed=True),
        # FROZEN (positional): the one transpiled leaf stable enough to give up field tags
        # — unchanged since exponential histograms stabilised, and there are two per data
        # point. If OTLP ever changes it, the escape hatch is a new node on a NEW field of
        # ExponentialHistogramDataPoint (packed, so it has the unknown-field tail), not an
        # edit of this one (spec/38 §2.7).
        Node('Buckets', fields=[
            'offset' >> t.i32,
            'bucket_counts' >> t.u64.array,
        ], packed=True, frozen=True),
        Node('ExponentialHistogramDataPoint', fields=[
            'attributes' >> t.ref("KeyValue").array,
            'start_time_unix_nano' >> t.u64 >> raw >> timestamp(unit='ns'),
            'time_unix_nano' >> t.u64 >> raw >> timestamp(unit='ns'),
            'count' >> t.u64,
            'sum' >> t.f64,
            'scale' >> t.i32,
            'zero_count' >> t.u64,
            'positive' >> t.ref("Buckets"),
            'negative' >> t.ref("Buckets"),
            'flags' >> t.u32,
            'exemplars' >> t.ref("Exemplar").array,
            'min' >> t.f64,
            'max' >> t.f64,
            'zero_threshold' >> t.f64,
        ], packed=True),
        Node('ValueAtQuantile', fields=[
            'quantile' >> t.f64,
            'value' >> t.f64,
        ], packed=True),
        Node('SummaryDataPoint', fields=[
            'attributes' >> t.ref("KeyValue").array,
            'start_time_unix_nano' >> t.u64 >> raw >> timestamp(unit='ns'),
            'time_unix_nano' >> t.u64 >> raw >> timestamp(unit='ns'),
            'count' >> t.u64,
            'sum' >> t.f64,
            'quantile_values' >> t.ref("ValueAtQuantile").array,
            'flags' >> t.u32,
        ], packed=True),
        Node('Exemplar', fields=[
            'filtered_attributes' >> t.ref("KeyValue").array,
            'time_unix_nano' >> t.u64 >> raw >> timestamp(unit='ns'),
            'span_id' >> t.data,
            'trace_id' >> t.data,
            'value' >> t.ref('NumberValue'),
        ], packed=True),
    ],
)


# ── Logs: logs/v1/logs.proto ──────────────────────────────────────────────────
OtlpLogs = DataGraph(
    'OtlpLogs',
    root_type=t.ref('LogsData'),
    imports=[ImportedGraph(OtlpCommon)],
    node_types=[
        Node('LogsData', fields=[
            'resource_logs' >> t.ref("ResourceLogs").array,
        ], packed=True),
        Node('ResourceLogs', fields=[
            'resource' >> t.ref("Resource"),
            'scope_logs' >> t.ref("ScopeLogs").array,
            'schema_url' >> t.utf8,
        ], packed=True),
        Node('ScopeLogs', fields=[
            'scope' >> t.ref("InstrumentationScope"),
            'log_records' >> t.ref("LogRecord").array,
            'schema_url' >> t.utf8,
        ], packed=True),
        Enum('SeverityNumber', {
            0: 'SEVERITY_NUMBER_UNSPECIFIED',
            1: 'SEVERITY_NUMBER_TRACE',
            2: 'SEVERITY_NUMBER_TRACE2',
            3: 'SEVERITY_NUMBER_TRACE3',
            4: 'SEVERITY_NUMBER_TRACE4',
            5: 'SEVERITY_NUMBER_DEBUG',
            6: 'SEVERITY_NUMBER_DEBUG2',
            7: 'SEVERITY_NUMBER_DEBUG3',
            8: 'SEVERITY_NUMBER_DEBUG4',
            9: 'SEVERITY_NUMBER_INFO',
            10: 'SEVERITY_NUMBER_INFO2',
            11: 'SEVERITY_NUMBER_INFO3',
            12: 'SEVERITY_NUMBER_INFO4',
            13: 'SEVERITY_NUMBER_WARN',
            14: 'SEVERITY_NUMBER_WARN2',
            15: 'SEVERITY_NUMBER_WARN3',
            16: 'SEVERITY_NUMBER_WARN4',
            17: 'SEVERITY_NUMBER_ERROR',
            18: 'SEVERITY_NUMBER_ERROR2',
            19: 'SEVERITY_NUMBER_ERROR3',
            20: 'SEVERITY_NUMBER_ERROR4',
            21: 'SEVERITY_NUMBER_FATAL',
            22: 'SEVERITY_NUMBER_FATAL2',
            23: 'SEVERITY_NUMBER_FATAL3',
            24: 'SEVERITY_NUMBER_FATAL4',
        }, capacity=255),
        Node('LogRecord', fields=[
            'time_unix_nano' >> t.u64 >> raw >> timestamp(unit='ns'),
            'observed_time_unix_nano' >> t.u64 >> raw >> timestamp(unit='ns'),
            'severity_number' >> t.ref("SeverityNumber"),
            'severity_text' >> t.utf8,
            'body' >> t.ref("AnyValue"),
            'attributes' >> t.ref("KeyValue").array,
            'dropped_attributes_count' >> t.u32,
            'flags' >> t.u32,
            'trace_id' >> t.data,
            'span_id' >> t.data,
            'event_name' >> t.utf8,
        ], packed=True),
    ],
)


# ── Profiles: profiles/v1development/profiles.proto ───────────────────────────
#
# FAITHFUL to the proto: every cross-reference is an integer index into a
# `ProfilesDictionary` table (`*_index`, `*_strindex`, `attribute_indices`), and each
# table's entry 0 is the mandatory zero value. That is protobuf's way of expressing
# sharing without references — exactly what a Dagr graph expresses natively with node
# refs and dedup. Keeping the indices makes the pprofile bridge a straight copy and keeps
# this graph comparable with protobuf byte for byte; a graph-native variant (refs instead
# of indices, no string table) is the obvious follow-up to measure against it, not a
# replacement for it (`spec/38` §2.6).
#
# `Link` here is the profiles Link (trace_id + span_id only), not the traces one. The two
# never meet: each signal is its own graph, and its own Go package.
OtlpProfiles = DataGraph(
    'OtlpProfiles',
    root_type=t.ref('ProfilesData'),
    imports=[ImportedGraph(OtlpCommon)],
    node_types=[
        Node('ProfilesDictionary', fields=[
            'mapping_table' >> t.ref("Mapping").array,
            'location_table' >> t.ref("Location").array,
            'function_table' >> t.ref("Function").array,
            'link_table' >> t.ref("Link").array,
            'string_table' >> t.utf8.array,
            'attribute_table' >> t.ref("KeyValueAndUnit").array,
            'stack_table' >> t.ref("Stack").array,
        ], packed=True),
        Node('ProfilesData', fields=[
            'resource_profiles' >> t.ref("ResourceProfiles").array,
            'dictionary' >> t.ref("ProfilesDictionary"),
        ], packed=True),
        Node('ResourceProfiles', fields=[
            'resource' >> t.ref("Resource"),
            'scope_profiles' >> t.ref("ScopeProfiles").array,
            'schema_url' >> t.utf8,
        ], packed=True),
        Node('ScopeProfiles', fields=[
            'scope' >> t.ref("InstrumentationScope"),
            'profiles' >> t.ref("Profile").array,
            'schema_url' >> t.utf8,
        ], packed=True),
        Node('Profile', fields=[
            'sample_type' >> t.ref("ValueType"),
            'samples' >> t.ref("Sample").array,
            'time_unix_nano' >> t.u64 >> raw >> timestamp(unit='ns'),
            'duration_nano' >> t.u64,
            'period_type' >> t.ref("ValueType"),
            'period' >> t.i64,
            'profile_id' >> t.data,
            'dropped_attributes_count' >> t.u32,
            'original_payload_format' >> t.utf8,
            'original_payload' >> t.data,
            'attribute_indices' >> t.i32.array,
        ], packed=True),
        Node('Link', fields=[
            'trace_id' >> t.data,
            'span_id' >> t.data,
        ], packed=True),
        Node('ValueType', fields=[
            'type_strindex' >> t.i32,
            'unit_strindex' >> t.i32,
        ], packed=True),
        Node('Sample', fields=[
            'stack_index' >> t.i32,
            'attribute_indices' >> t.i32.array,
            'link_index' >> t.i32,
            'values' >> t.i64.array,
            # raw on an int ARRAY is encoding tag 1 (fixed width, O(1) index): every
            # element is a wall-clock nanosecond timestamp, so LEB never wins.
            'timestamps_unix_nano' >> t.u64.array >> raw >> timestamp(unit='ns'),
        ], packed=True),
        Node('Mapping', fields=[
            'memory_start' >> t.u64,
            'memory_limit' >> t.u64,
            'file_offset' >> t.u64,
            'filename_strindex' >> t.i32,
            'attribute_indices' >> t.i32.array,
        ], packed=True),
        Node('Stack', fields=[
            'location_indices' >> t.i32.array,
        ], packed=True),
        Node('Location', fields=[
            'mapping_index' >> t.i32,
            'address' >> t.u64,
            'lines' >> t.ref("Line").array,
            'attribute_indices' >> t.i32.array,
        ], packed=True),
        Node('Line', fields=[
            'function_index' >> t.i32,
            'line' >> t.i64,
            'column' >> t.i64,
        ], packed=True),
        Node('Function', fields=[
            'name_strindex' >> t.i32,
            'system_name_strindex' >> t.i32,
            'filename_strindex' >> t.i32,
            'start_line' >> t.i64,
        ], packed=True),
        Node('KeyValueAndUnit', fields=[
            'key_strindex' >> t.i32,
            'value' >> t.ref("AnyValue"),
            'unit_strindex' >> t.i32,
        ], packed=True),
    ],
)


# ── Profiles as a GRAPH: references instead of dictionary indices ─────────────
#
# The experiment spec/38 §2.6 calls for, beside the faithful OtlpProfiles — not instead of
# it. Everything protobuf spells as an integer index into a ProfilesDictionary table is a
# node reference here, and the arena stores a shared node ONCE however many parents point
# at it: a Function referenced from a thousand Lines, a Stack from a thousand Samples.
# There is no dictionary and no string table — a string lives in the one node that owns
# it.
#
# Every node that is shared, or lies on a path to one, is REGULAR (vtable), because a
# packed parent re-inlines its children and would undo the sharing. That rules out the
# direct builder (spec/33 §3 admits trees only): this graph encodes through the arena.
# ValueType is never shared, so it stays packed and inline. The imported OtlpCommon
# nodes declare no layout, so here they are regular like their parents. Names follow the proto so the
# two graphs read side by side; they never meet — separate graph, separate Go package.
#
# The bridge translates indices to references on the way in and rebuilds the dictionary
# on the way out, so a round trip preserves every sample's MEANING (each resolved to its
# stack of functions, attributes and link) but not the dictionary's order — which the
# proto itself says a processor may rewrite ("MUST NOT have any observable effects").
OtlpProfilesGraph = DataGraph(
    'OtlpProfilesGraph',
    root_type=t.ref('ProfilesData'),
    imports=[ImportedGraph(OtlpCommon)],
    node_types=[
        Node('ProfilesData', fields=[
            'resource_profiles' >> t.ref("ResourceProfiles").array,
        ]),
        Node('ResourceProfiles', fields=[
            'resource' >> t.ref("Resource"),
            'scope_profiles' >> t.ref("ScopeProfiles").array,
            'schema_url' >> t.utf8,
        ]),
        Node('ScopeProfiles', fields=[
            'scope' >> t.ref("InstrumentationScope"),
            'profiles' >> t.ref("Profile").array,
            'schema_url' >> t.utf8,
        ]),
        Node('ValueType', fields=[
            'type' >> t.utf8,
            'unit' >> t.utf8,
        ], packed=True),
        Node('Profile', fields=[
            'sample_type' >> t.ref("ValueType"),
            'samples' >> t.ref("Sample").array,
            'time_unix_nano' >> t.u64 >> raw >> timestamp(unit='ns'),
            'duration_nano' >> t.u64,
            'period_type' >> t.ref("ValueType"),
            'period' >> t.i64,
            'profile_id' >> t.data,
            'dropped_attributes_count' >> t.u32,
            'original_payload_format' >> t.utf8,
            'original_payload' >> t.data,
            'attributes' >> t.ref("KeyValueAndUnit").array,
        ]),
        Node('Sample', fields=[
            'stack' >> t.ref("Stack"),
            'attributes' >> t.ref("KeyValueAndUnit").array,
            'link' >> t.ref("Link"),
            'values' >> t.i64.array,
            'timestamps_unix_nano' >> t.u64.array >> raw >> timestamp(unit='ns'),
        ]),
        Node('Stack', fields=[
            'locations' >> t.ref("Location").array,
        ]),
        Node('Location', fields=[
            'mapping' >> t.ref("Mapping"),
            'address' >> t.u64,
            'lines' >> t.ref("Line").array,
            'attributes' >> t.ref("KeyValueAndUnit").array,
        ]),
        # Not shared itself, but it points at a shared Function, so it must be regular.
        Node('Line', fields=[
            'function' >> t.ref("Function"),
            'line' >> t.i64,
            'column' >> t.i64,
        ]),
        Node('Function', fields=[
            'name' >> t.utf8,
            'system_name' >> t.utf8,
            'filename' >> t.utf8,
            'start_line' >> t.i64,
        ]),
        Node('Mapping', fields=[
            'memory_start' >> t.u64,
            'memory_limit' >> t.u64,
            'file_offset' >> t.u64,
            'filename' >> t.utf8,
            'attributes' >> t.ref("KeyValueAndUnit").array,
        ]),
        Node('Link', fields=[
            'trace_id' >> t.data,
            'span_id' >> t.data,
        ]),
        Node('KeyValueAndUnit', fields=[
            'key' >> t.utf8,
            'value' >> t.ref("AnyValue"),
            'unit' >> t.utf8,
        ]),
    ],
)


# ── The spools: one append-only, doubly-linked DataSink per signal ────────────
#
# `spec/11` §11: with `doubly_linked=True` every record also stores its byte span as a
# reverse-delimited LEB at the record's END, so a reader can walk newest-first from EOF
# with no forward pass and no index.  That is what makes this a WAL rather than a log
# file: restart recovery reads backwards to the last intact record instead of scanning a
# multi-GB spool forward, and a half-written tail is detectable rather than silently
# truncating the stream (`spec/38` §6.1).
#
# The batch is INLINE: a record holds the signal's top-level array directly
# (`resource_spans`, ... — and profiles' `dictionary` beside it), with no `TracesData`-style
# wrapper node and no `raw` embedding. The referenced nodes are packed, and a packed node
# is already relocatable, size-prefixed, pointer-free bytes — `raw` earns its keep only
# for a NON-packed node (a vtable / cyclic graph) that has to be carried as a standalone
# embedded graph. A record field that holds a graph's node is stored and read with that
# graph's own generated code (Dagr `spec/11` §12): the Go sink package is a shim over
# `otlptracesdirect.Put{N}` (write) and `otlptraces.{N}Accessor` (read), so the array's
# bytes are exactly what the graph writes under a packed parent — frozen+packed
# `Buckets` included — and the pdata bridges read a record with the accessors they
# already use. Records are copied between spools verbatim (`AppendRaw`), with no decode.
#
# `received_unix_nano` and the item count are each sink's OWN fields, and they are the
# whole point of the envelope: a reverse reader answers "the last N batches, m items" by
# reading two packed scalars per record and never touching the batch itself.
# `received_unix_nano` is `raw` for the same reason the graphs' timestamps are.
#
# ONE SPOOL PER SIGNAL, not one spool with a record type per signal: a collector exporter
# is per signal type, and so is the queue behind it. The count is the unit each
# signal's pdata counts in (`SpanCount`, `DataPointCount`, `LogRecordCount`,
# `SampleCount`).
# THE HEADER (`spec/15`, D2 in `spec/38` §12). The four sinks would otherwise share one
# framing word and nothing else, so a spool on disk could not say which signal it holds.
# Every spool carries the SAME `SpoolHeader` (a flat packed node, written once at stream
# open, `spec/15` §9: metadata only), so any of the four generated readers — or a tool
# that knows none of them — reads it before choosing a decoder:
#
#   signal              0 traces, 1 metrics, 2 logs, 3 profiles — what code dispatches on
#   signal_name         the embedded graph's name ("OtlpTraces", ...) — what a person reads
#   created_unix_nano   when the spool was created; compaction keeps the original
#   format_version      the receipt's wire_format_version the writer was built from
#   otlp_proto_version  which vendored opentelemetry-proto the schema came from
#   producer            the collector instance that wrote it
#
# A packed header keeps an unknown-field tail, so fields can be ADDED later without a
# wire break; removing the header (or adding one, as this did) flips the framing word's
# header bit and is breaking by design (`spec/15` §11).
SpoolHeader = Node('SpoolHeader', fields=[
    'signal' >> t.u8 >> required,
    'signal_name' >> t.utf8 >> required,
    'created_unix_nano' >> t.u64 >> raw >> required >> timestamp(unit='ns'),
    'format_version' >> t.u32 >> required,
    'otlp_proto_version' >> t.utf8,
    'producer' >> t.utf8,
])

SpanLog = DataSink(
    'SpanLog',
    imports=[ImportedGraph(OtlpTraces)],
    doubly_linked=True,
    header=SpoolHeader,
    node_types=[
        Node('Batch', fields=[
            'received_unix_nano' >> t.u64 >> raw >> required >> timestamp(unit='ns'),
            'span_count' >> t.u32 >> required,
            'resource_spans' >> t.ref("ResourceSpans").array >> required,
        ]),
    ],
)

MetricLog = DataSink(
    'MetricLog',
    imports=[ImportedGraph(OtlpMetrics)],
    doubly_linked=True,
    header=SpoolHeader,
    node_types=[
        Node('Batch', fields=[
            'received_unix_nano' >> t.u64 >> raw >> required >> timestamp(unit='ns'),
            'data_point_count' >> t.u32 >> required,
            'resource_metrics' >> t.ref("ResourceMetrics").array >> required,
        ]),
    ],
)

LogRecordLog = DataSink(
    'LogRecordLog',
    imports=[ImportedGraph(OtlpLogs)],
    doubly_linked=True,
    header=SpoolHeader,
    node_types=[
        Node('Batch', fields=[
            'received_unix_nano' >> t.u64 >> raw >> required >> timestamp(unit='ns'),
            'log_record_count' >> t.u32 >> required,
            'resource_logs' >> t.ref("ResourceLogs").array >> required,
        ]),
    ],
)

ProfileLog = DataSink(
    'ProfileLog',
    imports=[ImportedGraph(OtlpProfiles)],
    doubly_linked=True,
    header=SpoolHeader,
    node_types=[
        Node('Batch', fields=[
            'received_unix_nano' >> t.u64 >> raw >> required >> timestamp(unit='ns'),
            'sample_count' >> t.u32 >> required,
            'resource_profiles' >> t.ref("ResourceProfiles").array >> required,
            'dictionary' >> t.ref("ProfilesDictionary"),
        ]),
    ],
)


# ── The same-host hop: a metrics region in shared memory (spec/38 §7.1) ─────────────────
#
# A SharedBuffer is a FIXED-LAYOUT overlay (Dagr spec/20): every field has a fixed slot,
# strings and arrays reserve a capacity, and two processes map the same bytes. An app's
# SDK exporter writes its CURRENT metric state here under a seqlock — one write per
# export, never blocking on a reader — and the collector's `dagrshm` receiver snapshots it
# at its own cadence. The values are cumulative: the region is the app's state, like a
# Prometheus endpoint, not a stream of deltas.
#
# It is generic — any app, one receiver — so it is a table of DATA POINTS, one per metric
# stream, each carrying its own name / unit / kind / attributes. What does not fit a cap
# (more points, attributes, buckets, longer strings) is dropped and counted, never
# written half. Exponential histograms and exemplars are not carried.
#
# There is no fingerprint on the wire (spec/20 §8, §13): a reader MUST check `magic` and
# `version` right after attaching, and every layout change is a version bump — the
# receipt pins the layout, and `dagr build` refuses a change that moves a byte.
MetricsRegionMagic = 0x524D4744    # "DGMR" little-endian
MetricsRegionVersion = 1

MetricsRegion = SharedBuffer(
    'MetricsRegion', root_type='State', concurrency='seqlock',
    node_types=[
        Enum('PointKind', {0: 'gauge_int', 1: 'gauge_double', 2: 'sum_int', 3: 'sum_double',
                           4: 'histogram'}),
        Enum('AttrKind', {0: 'string', 1: 'int', 2: 'double', 3: 'bool'}),
        # one attribute: its value in the slot its kind names (bool: int_value 0/1)
        Node('Attr', frozen=True, fields=[
            'key' >> t.utf8 >> cap(32) >> required,
            'kind' >> t.ref('AttrKind') >> required,
            'string_value' >> t.utf8 >> cap(64) >> required,
            'int_value' >> t.i64 >> required,
            'double_value' >> t.f64 >> required,
        ]),
        # one metric stream: an instrument x one attribute set
        Node('Point', frozen=True, fields=[
            'scope' >> t.utf8 >> cap(32) >> required,
            'name' >> t.utf8 >> cap(64) >> required,
            'unit' >> t.utf8 >> cap(16) >> required,
            'kind' >> t.ref('PointKind') >> required,
            'monotonic' >> t.bool >> required,
            'attrs' >> t.ref('Attr').array >> cap(8) >> required,
            'time_unix_nano' >> t.u64 >> required,
            # gauge / sum: the value; histogram: `double_value` is the sum
            'int_value' >> t.i64 >> required,
            'double_value' >> t.f64 >> required,
            # histogram (explicit buckets): len(bucket_counts) == len(bounds) + 1
            'count' >> t.u64 >> required,
            'bounds' >> t.f64.array >> cap(15) >> required,
            'bucket_counts' >> t.u64.array >> cap(16) >> required,
            'min' >> t.f64,
            'max' >> t.f64,
        ]),
        # the root — not `Region`: the Rust / Swift overlays declare a `Region` of their own
        Node('State', frozen=True, fields=[
            'magic' >> t.u32 >> required,
            'version' >> t.u16 >> required,
            'service' >> t.utf8 >> cap(64) >> required,
            'resource' >> t.ref('Attr').array >> cap(16) >> required,
            'start_unix_nano' >> t.u64 >> required,     # the cumulative start time
            'write_unix_nano' >> t.u64 >> required,     # the last export
            'exports' >> t.u64 >> required,
            'dropped_points' >> t.u32 >> required,      # in the last export
            'points' >> t.ref('Point').array >> cap(512) >> required,
        ]),
    ],
)


# ── Direct instruments: the values half, updated in place (Dagr spec/40 §2) ─────────────
#
# The SDK path above pays the SDK's collect pass on every export. Direct instruments skip
# it: an app registers each stream ONCE — its descriptor (name, unit, kind, attributes,
# bucket bounds) goes into a MetricsRegion — and then updates that stream's slot HERE with
# atomic operations: `counter.Add` is one atomic add into shared memory. The collector's
# `dagrshm` receiver reads the descriptors under their seqlock and these values word by
# word (spec/40 §2.3's two-region pattern: an atomic region holds no strings).
#
# Slot i belongs to the descriptor region's point i. Histograms: `double_value` is the sum,
# `buckets[k]` counts values in (bounds[k-1], bounds[k]], `min` / `max` start at +Inf /
# -Inf when the stream is registered. No multi-field snapshot: a reader may see a
# histogram's count and sum a few records apart (as Prometheus clients do).
#
# Per-core copies (spec/38 §7.1.1): the values file holds `shards` copies of this region
# back to back, each starting on a 128-byte boundary; a recording goes to the copy of the
# scheduler slot (P) it runs on, so cores never fight over a stream's cache line. The
# count is the app's, chosen at startup from GOMAXPROCS — a SharedBuffer's size is fixed
# at codegen, so it cannot be a capacity here. Every copy carries the same `shards`; the
# reader adds the copies up (min / max: the extremes; gauges live in copy 0 only).
MetricsValuesMagic = 0x564D4744    # "DGMV" little-endian
MetricsValuesVersion = 2

MetricsValues = SharedBuffer(
    'MetricsValues', root_type='Values', concurrency='atomic',
    node_types=[
        Node('Value', frozen=True, fields=[
            'int_value' >> t.i64 >> required,
            'double_value' >> t.f64 >> required,
            'count' >> t.u64 >> required,
            'min' >> t.f64 >> required,
            'max' >> t.f64 >> required,
            'buckets' >> t.u64.array >> cap(16) >> required,
        ]),
        Node('Values', frozen=True, fields=[
            'magic' >> t.u32 >> required,
            'version' >> t.u32 >> required,
            'shards' >> t.u32 >> required,         # copies in the file, this one included
            'points' >> t.ref('Value').array >> cap(512) >> required,
        ]),
    ],
)


library = Library(
    'otlp_dagr',
    # OtlpCommon is NOT listed: it has no root and is never serialized on its own — each
    # signal reaches it through its import.
    schemas=[OtlpTraces, OtlpMetrics, OtlpLogs, OtlpProfiles, OtlpProfilesGraph,
             SpanLog, MetricLog, LogRecordLog, ProfileLog, MetricsRegion, MetricsValues],
    targets=[
        # The Go emitter always writes the lazy accessors, the arena, the serializer
        # and the direct builder (spec/37 §16) — there is no feature list to opt into.
        # `package-per-graph`: each signal is its own Go package (`otlpgen/otlptraces`,
        # `otlpgen/otlpmetrics`, ...) — profiles has a `Link` of its own — and the
        # imported OtlpCommon is emitted once, as `otlpgen/otlpcommon` (+ `…direct`), which
        # the four signal packages refer to (Dagr spec/37 §18). OtlpProfilesGraph holds it
        # as vtable nodes, a different wire shape, and keeps its own copy.
        Go(out="go/gen", package="otlpgen", layout="package-per-graph"),
        # The Rust crate carries the lazy reader and spec/19's lazy Arrow producer
        # (`arrow` implies `lazy`): one normalised table per node type, and `AnyValue` —
        # a recursive union Arrow cannot inline — as its own table keyed by a surrogate
        # id. That is the input to the OTAP bridge (spec/38 §9, §14 step 8); it is not
        # the OTAP record-batch schema yet.
        Rust(out="rust/gen", features=["arrow"]),
    ],
    wire_format_version=1,
)
