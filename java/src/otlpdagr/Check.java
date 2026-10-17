package otlpdagr;

import com.google.protobuf.ByteString;
import io.opentelemetry.exporter.internal.otlp.traces.LowAllocationTraceRequestMarshaler;
import io.opentelemetry.exporter.internal.otlp.traces.TraceRequestMarshaler;
import io.opentelemetry.proto.common.v1.AnyValue;
import io.opentelemetry.proto.common.v1.ArrayValue;
import io.opentelemetry.proto.common.v1.InstrumentationScope;
import io.opentelemetry.proto.common.v1.KeyValue;
import io.opentelemetry.proto.resource.v1.Resource;
import io.opentelemetry.proto.trace.v1.ResourceSpans;
import io.opentelemetry.proto.trace.v1.ScopeSpans;
import io.opentelemetry.proto.trace.v1.Span;
import io.opentelemetry.proto.trace.v1.Status;
import io.opentelemetry.proto.trace.v1.TracesData;
import io.opentelemetry.sdk.trace.data.SpanData;
import java.io.ByteArrayOutputStream;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import otlpgen.OtlpTraces;

/**
 * The gates that run before any timing (spec/38 §10.3.2). Both encoders must say the same
 * thing about the same batch, or the benchmark compares two different jobs:
 *
 * <ol>
 *   <li>the fixture, as OTel Java SDK spans, through BOTH of opentelemetry-java's marshalers
 *       equals the fixture's own protobuf — with attributes in the SDK's sorted order and the
 *       span flags the Java marshaler always writes (the has-is-remote bit);
 *   <li>the Dagr buffer, restored and turned back into protobuf, equals that same message byte
 *       for byte — every field the protobuf carries, the Dagr buffer carries;
 *   <li>restore → build reproduces the Dagr buffer.
 * </ol>
 *
 * Writes the Dagr buffer for the Go gate (`go/pdatabridge/java_test.go`: Go's writer must
 * re-encode it to the same bytes) and prints the sizes.
 *
 * <pre>java otlpdagr.Check traces-batch.pb out.dagr</pre>
 */
public final class Check {
  public static void main(String[] args) throws Exception {
    TracesData fixture = TracesData.parseFrom(Files.readAllBytes(Path.of(args[0])));
    List<SpanData> spans = Fixture.spans(fixture);

    // 1. opentelemetry-java's protobuf of the fixture spans == the fixture
    TracesData.Builder want = Fixture.sorted(fixture).toBuilder();
    for (ResourceSpans.Builder rs : want.getResourceSpansBuilderList()) {
      for (ScopeSpans.Builder ss : rs.getScopeSpansBuilderList()) {
        for (Span.Builder s : ss.getSpansBuilderList()) s.setFlags(0x100);
      }
    }
    byte[] pb = want.build().toByteArray();
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    TraceRequestMarshaler.create(spans).writeBinaryTo(out);
    same("TraceRequestMarshaler", out.toByteArray(), pb);
    out.reset();
    LowAllocationTraceRequestMarshaler low = new LowAllocationTraceRequestMarshaler();
    low.initialize(spans);
    low.writeBinaryTo(out);
    low.reset();
    same("LowAllocationTraceRequestMarshaler", out.toByteArray(), pb);

    // 2. the Dagr buffer carries the same message
    DagrTraceEncoder enc = new DagrTraceEncoder();
    MemorySegment seg = enc.encode(spans);
    if (seg.byteSize() == 0) throw new AssertionError("Dagr encode failed: " + enc.builder().status());
    byte[] dagr = seg.toArray(ValueLayout.JAVA_BYTE);
    OtlpTraces.TracesData back = OtlpTraces.restoreValues(MemorySegment.ofArray(dagr));
    if (back == null) throw new AssertionError("Dagr restore failed");
    same("Dagr → protobuf", proto(back).toByteArray(), pb);

    // 3. restore → build is the identity
    same("Dagr restore → build", new DagrTraceEncoder().build(back).toArray(ValueLayout.JAVA_BYTE), dagr);

    Files.write(Path.of(args[1]), dagr);
    System.out.printf("ok: %d spans; OTLP protobuf %d B, Dagr %d B (%.2fx)%n", spans.size(), pb.length,
        dagr.length, (double) dagr.length / pb.length);
  }

  private static void same(String what, byte[] got, byte[] want) {
    if (Arrays.equals(got, want)) return;
    int i = Arrays.mismatch(got, want);
    throw new AssertionError(String.format("%s: %d B, want %d B; first difference at byte %d", what,
        got.length, want.length, i));
  }

  // ── Dagr values → protobuf, the inverse of the encoder's mapping ──────────────

  static TracesData proto(OtlpTraces.TracesData td) {
    TracesData.Builder b = TracesData.newBuilder();
    for (OtlpTraces.ResourceSpans rs : each(td.resourceSpans)) {
      ResourceSpans.Builder r = b.addResourceSpansBuilder();
      if (rs.resource != null) r.setResource(Resource.newBuilder().addAllAttributes(kvs(rs.resource.attributes)));
      if (rs.schemaUrl != null) r.setSchemaUrl(rs.schemaUrl);
      for (OtlpTraces.ScopeSpans ss : each(rs.scopeSpans)) {
        ScopeSpans.Builder s = r.addScopeSpansBuilder();
        if (ss.scope != null) {
          InstrumentationScope.Builder sc = s.getScopeBuilder();
          if (ss.scope.name != null) sc.setName(ss.scope.name);
          if (ss.scope.version != null) sc.setVersion(ss.scope.version);
          sc.addAllAttributes(kvs(ss.scope.attributes));
        }
        if (ss.schemaUrl != null) s.setSchemaUrl(ss.schemaUrl);
        for (OtlpTraces.Span sp : each(ss.spans)) s.addSpans(span(sp));
      }
    }
    return b.build();
  }

  private static Span span(OtlpTraces.Span sp) {
    Span.Builder b = Span.newBuilder();
    if (sp.traceId != null) b.setTraceId(bytes(sp.traceId));
    if (sp.spanId != null) b.setSpanId(bytes(sp.spanId));
    if (sp.traceState != null) b.setTraceState(sp.traceState);
    if (sp.parentSpanId != null) b.setParentSpanId(bytes(sp.parentSpanId));
    if (sp.hasFlags) b.setFlags((int) sp.flags);
    if (sp.name != null) b.setName(sp.name);
    if (sp.hasKind) b.setKindValue(sp.kind);
    if (sp.hasStartTimeUnixNano) b.setStartTimeUnixNano(sp.startTimeUnixNano);
    if (sp.hasEndTimeUnixNano) b.setEndTimeUnixNano(sp.endTimeUnixNano);
    b.addAllAttributes(kvs(sp.attributes));
    if (sp.hasDroppedAttributesCount) b.setDroppedAttributesCount((int) sp.droppedAttributesCount);
    for (OtlpTraces.Event e : each(sp.events)) {
      Span.Event.Builder eb = b.addEventsBuilder();
      if (e.hasTimeUnixNano) eb.setTimeUnixNano(e.timeUnixNano);
      if (e.name != null) eb.setName(e.name);
      eb.addAllAttributes(kvs(e.attributes));
      if (e.hasDroppedAttributesCount) eb.setDroppedAttributesCount((int) e.droppedAttributesCount);
    }
    if (sp.hasDroppedEventsCount) b.setDroppedEventsCount((int) sp.droppedEventsCount);
    for (OtlpTraces.Link l : each(sp.links)) {
      Span.Link.Builder lb = b.addLinksBuilder();
      if (l.traceId != null) lb.setTraceId(bytes(l.traceId));
      if (l.spanId != null) lb.setSpanId(bytes(l.spanId));
      if (l.traceState != null) lb.setTraceState(l.traceState);
      lb.addAllAttributes(kvs(l.attributes));
      if (l.hasDroppedAttributesCount) lb.setDroppedAttributesCount((int) l.droppedAttributesCount);
      if (l.hasFlags) lb.setFlags((int) l.flags);
    }
    if (sp.hasDroppedLinksCount) b.setDroppedLinksCount((int) sp.droppedLinksCount);
    if (sp.status != null) {
      Status.Builder st = b.getStatusBuilder();
      if (sp.status.message != null) st.setMessage(sp.status.message);
      if (sp.status.hasCode) st.setCodeValue(sp.status.code);
    }
    return b.build();
  }

  private static List<KeyValue> kvs(OtlpTraces.KeyValue[] kvs) {
    return each(kvs).stream().map(kv -> {
      KeyValue.Builder b = KeyValue.newBuilder();
      if (kv.key != null) b.setKey(kv.key);
      if (kv.value != null) b.setValue(any(kv.value));
      return b.build();
    }).toList();
  }

  private static AnyValue any(OtlpTraces.AnyValue v) {
    AnyValue.Builder b = AnyValue.newBuilder();
    switch (v) {
      case OtlpTraces.AnyValue.StringValue s -> b.setStringValue(s.value());
      case OtlpTraces.AnyValue.BoolValue s -> b.setBoolValue(s.value());
      case OtlpTraces.AnyValue.IntValue s -> b.setIntValue(s.value());
      case OtlpTraces.AnyValue.DoubleValue s -> b.setDoubleValue(s.value());
      case OtlpTraces.AnyValue.BytesValue s -> b.setBytesValue(bytes(s.value()));
      case OtlpTraces.AnyValue.ArrayValue s ->
          b.setArrayValue(ArrayValue.newBuilder().addAllValues(Arrays.stream(s.value()).map(Check::any).toList()));
      default -> throw new IllegalArgumentException("not in the fixture: " + v);
    }
    return b.build();
  }

  private static ByteString bytes(MemorySegment s) {
    return ByteString.copyFrom(s.toArray(ValueLayout.JAVA_BYTE));
  }

  private static <T> List<T> each(T[] a) {
    return a == null ? List.of() : Arrays.asList(a);
  }
}
