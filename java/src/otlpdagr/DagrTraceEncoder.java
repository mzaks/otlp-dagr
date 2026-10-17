package otlpdagr;

import dagr.runtime.Builder;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.api.trace.TraceState;
import io.opentelemetry.sdk.common.InstrumentationScopeInfo;
import io.opentelemetry.sdk.resources.Resource;
import io.opentelemetry.sdk.trace.data.EventData;
import io.opentelemetry.sdk.trace.data.LinkData;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.sdk.trace.data.StatusData;
import java.lang.foreign.MemorySegment;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import otlpgen.OtlpTraces;

/**
 * OTel Java SDK spans → an `OtlpTraces` Dagr buffer: what a Java exporter sends instead of an
 * OTLP `ExportTraceServiceRequest` (spec/38 §10.3.2). Spans are grouped by resource, then by
 * scope, in first-seen order, and mapped field by field as `go/pdatabridge` maps pdata — an
 * empty string, a zero time or count, an all-zero id is ABSENT — with the span and link
 * `flags` the OTel Java marshaler writes (the trace flags plus the is-remote bits of the
 * parent / linked context), so the Dagr buffer carries exactly what its protobuf does.
 *
 * <p>Two halves, like the Go bridge: {@link #toValue} builds the direct builder's value tree,
 * {@link #build} serializes one into a reused {@link Builder}. {@link #encode} is both.
 */
public final class DagrTraceEncoder {
  // SpanFlags in the OTLP proto: the context's trace flags, then whether the parent / linked
  // context's is-remote bit is known, and its value
  private static final int HAS_IS_REMOTE = 0x100;
  private static final int IS_REMOTE = 0x200;

  private final Builder b;

  public DagrTraceEncoder() {
    this(Builder.growing(1 << 18));
  }

  public DagrTraceEncoder(Builder b) {
    this.b = b;
  }

  /** The batch as a Dagr buffer: a view into this encoder's builder, valid until the next call. */
  public MemorySegment encode(Collection<SpanData> spans) {
    return build(toValue(spans));
  }

  /** Serializes a value tree; empty when it failed (`builder().status()` says why). */
  public MemorySegment build(OtlpTraces.TracesData v) {
    return OtlpTraces.build(b, v);
  }

  public Builder builder() {
    return b;
  }

  public static OtlpTraces.TracesData toValue(Collection<SpanData> spans) {
    Map<Resource, Map<InstrumentationScopeInfo, List<OtlpTraces.Span>>> groups = new LinkedHashMap<>();
    for (SpanData s : spans) {
      groups.computeIfAbsent(s.getResource(), r -> new LinkedHashMap<>())
          .computeIfAbsent(s.getInstrumentationScopeInfo(), sc -> new ArrayList<>())
          .add(span(s));
    }
    OtlpTraces.ResourceSpans[] rss = new OtlpTraces.ResourceSpans[groups.size()];
    int i = 0;
    for (var rg : groups.entrySet()) {
      OtlpTraces.ScopeSpans[] sss = new OtlpTraces.ScopeSpans[rg.getValue().size()];
      int j = 0;
      for (var sg : rg.getValue().entrySet()) {
        OtlpTraces.ScopeSpans ss = new OtlpTraces.ScopeSpans();
        ss.scope = scope(sg.getKey());
        ss.spans = optional(sg.getValue().toArray(new OtlpTraces.Span[0]));
        ss.schemaUrl = nonEmpty(sg.getKey().getSchemaUrl());
        sss[j++] = ss;
      }
      OtlpTraces.ResourceSpans rs = new OtlpTraces.ResourceSpans();
      rs.resource = resource(rg.getKey());
      rs.scopeSpans = optional(sss);
      rs.schemaUrl = nonEmpty(rg.getKey().getSchemaUrl());
      rss[i++] = rs;
    }
    OtlpTraces.TracesData td = new OtlpTraces.TracesData();
    td.resourceSpans = optional(rss);
    return td;
  }

  private static OtlpTraces.Resource resource(Resource r) {
    OtlpTraces.Resource out = new OtlpTraces.Resource();
    out.attributes = keyValues(r.getAttributes());
    return out;
  }

  private static OtlpTraces.InstrumentationScope scope(InstrumentationScopeInfo s) {
    OtlpTraces.InstrumentationScope out = new OtlpTraces.InstrumentationScope();
    out.name = nonEmpty(s.getName());
    out.version = nonEmpty(s.getVersion());
    out.attributes = keyValues(s.getAttributes());
    return out;
  }

  private static OtlpTraces.Span span(SpanData s) {
    OtlpTraces.Span out = new OtlpTraces.Span();
    SpanContext ctx = s.getSpanContext();
    if (ctx.isValid()) {
      out.traceId = MemorySegment.ofArray(ctx.getTraceIdBytes());
      out.spanId = MemorySegment.ofArray(ctx.getSpanIdBytes());
    }
    out.traceState = traceState(ctx.getTraceState());
    SpanContext parent = s.getParentSpanContext();
    if (parent.isValid()) out.parentSpanId = MemorySegment.ofArray(parent.getSpanIdBytes());
    setFlags(out, ctx, parent.isRemote());
    out.name = nonEmpty(s.getName());
    out.hasKind = true;
    out.kind = s.getKind().ordinal() + 1;   // INTERNAL..CONSUMER = proto 1..5
    if (s.getStartEpochNanos() != 0) {
      out.hasStartTimeUnixNano = true;
      out.startTimeUnixNano = s.getStartEpochNanos();
    }
    if (s.getEndEpochNanos() != 0) {
      out.hasEndTimeUnixNano = true;
      out.endTimeUnixNano = s.getEndEpochNanos();
    }
    Attributes attrs = s.getAttributes();
    out.attributes = keyValues(attrs);
    if (s.getTotalAttributeCount() > attrs.size()) {
      out.hasDroppedAttributesCount = true;
      out.droppedAttributesCount = s.getTotalAttributeCount() - attrs.size();
    }
    List<EventData> events = s.getEvents();
    if (!events.isEmpty()) {
      OtlpTraces.Event[] evs = new OtlpTraces.Event[events.size()];
      for (int i = 0; i < evs.length; i++) evs[i] = event(events.get(i));
      out.events = evs;
    }
    if (s.getTotalRecordedEvents() > events.size()) {
      out.hasDroppedEventsCount = true;
      out.droppedEventsCount = s.getTotalRecordedEvents() - events.size();
    }
    List<LinkData> links = s.getLinks();
    if (!links.isEmpty()) {
      OtlpTraces.Link[] lks = new OtlpTraces.Link[links.size()];
      for (int i = 0; i < lks.length; i++) lks[i] = link(links.get(i));
      out.links = lks;
    }
    if (s.getTotalRecordedLinks() > links.size()) {
      out.hasDroppedLinksCount = true;
      out.droppedLinksCount = s.getTotalRecordedLinks() - links.size();
    }
    StatusData st = s.getStatus();
    int code = switch (st.getStatusCode()) {
      case OK -> 1;
      case ERROR -> 2;
      default -> 0;
    };
    if (code != 0 || !st.getDescription().isEmpty()) {
      OtlpTraces.Status status = new OtlpTraces.Status();
      status.message = nonEmpty(st.getDescription());
      status.hasCode = true;
      status.code = code;
      out.status = status;
    }
    return out;
  }

  private static void setFlags(OtlpTraces.Span out, SpanContext ctx, boolean parentRemote) {
    long f = (ctx.getTraceFlags().asByte() & 0xff) | HAS_IS_REMOTE | (parentRemote ? IS_REMOTE : 0);
    out.hasFlags = true;
    out.flags = f;
  }

  private static OtlpTraces.Event event(EventData e) {
    OtlpTraces.Event out = new OtlpTraces.Event();
    if (e.getEpochNanos() != 0) {
      out.hasTimeUnixNano = true;
      out.timeUnixNano = e.getEpochNanos();
    }
    out.name = nonEmpty(e.getName());
    out.attributes = keyValues(e.getAttributes());
    if (e.getDroppedAttributesCount() != 0) {
      out.hasDroppedAttributesCount = true;
      out.droppedAttributesCount = e.getDroppedAttributesCount();
    }
    return out;
  }

  private static OtlpTraces.Link link(LinkData l) {
    OtlpTraces.Link out = new OtlpTraces.Link();
    SpanContext ctx = l.getSpanContext();
    if (ctx.isValid()) {
      out.traceId = MemorySegment.ofArray(ctx.getTraceIdBytes());
      out.spanId = MemorySegment.ofArray(ctx.getSpanIdBytes());
    }
    out.traceState = traceState(ctx.getTraceState());
    out.attributes = keyValues(l.getAttributes());
    int dropped = l.getTotalAttributeCount() - l.getAttributes().size();
    if (dropped > 0) {
      out.hasDroppedAttributesCount = true;
      out.droppedAttributesCount = dropped;
    }
    out.hasFlags = true;
    out.flags = (ctx.getTraceFlags().asByte() & 0xff) | HAS_IS_REMOTE | (ctx.isRemote() ? IS_REMOTE : 0);
    return out;
  }

  /** Attributes in the SDK's order (sorted by key); null when there are none. */
  private static OtlpTraces.KeyValue[] keyValues(Attributes attrs) {
    if (attrs.isEmpty()) return null;
    OtlpTraces.KeyValue[] out = new OtlpTraces.KeyValue[attrs.size()];
    int[] n = {0};
    attrs.forEach((k, v) -> {
      OtlpTraces.KeyValue kv = new OtlpTraces.KeyValue();
      kv.key = nonEmpty(k.getKey());
      kv.value = anyValue(k, v);
      out[n[0]++] = kv;
    });
    return out;
  }

  private static OtlpTraces.AnyValue anyValue(AttributeKey<?> k, Object v) {
    return switch (k.getType()) {
      case STRING -> new OtlpTraces.AnyValue.StringValue((String) v);
      case BOOLEAN -> new OtlpTraces.AnyValue.BoolValue((Boolean) v);
      case LONG -> new OtlpTraces.AnyValue.IntValue((Long) v);
      case DOUBLE -> new OtlpTraces.AnyValue.DoubleValue((Double) v);
      case STRING_ARRAY, BOOLEAN_ARRAY, LONG_ARRAY, DOUBLE_ARRAY -> {
        List<?> l = (List<?>) v;
        OtlpTraces.AnyValue[] elems = new OtlpTraces.AnyValue[l.size()];
        for (int i = 0; i < elems.length; i++) {
          Object e = l.get(i);
          elems[i] = switch (k.getType()) {
            case STRING_ARRAY -> new OtlpTraces.AnyValue.StringValue((String) e);
            case BOOLEAN_ARRAY -> new OtlpTraces.AnyValue.BoolValue((Boolean) e);
            case LONG_ARRAY -> new OtlpTraces.AnyValue.IntValue((Long) e);
            default -> new OtlpTraces.AnyValue.DoubleValue((Double) e);
          };
        }
        yield new OtlpTraces.AnyValue.ArrayValue(elems);
      }
      default -> null;   // a type this mapping does not know: present, without a value
    };
  }

  private static String traceState(TraceState ts) {
    if (ts.isEmpty()) return null;
    StringBuilder sb = new StringBuilder();
    ts.forEach((k, v) -> {
      if (sb.length() > 0) sb.append(',');
      sb.append(k).append('=').append(v);
    });
    return sb.toString();
  }

  private static String nonEmpty(String s) {
    return s == null || s.isEmpty() ? null : s;
  }

  private static <T> T[] optional(T[] a) {
    return a.length == 0 ? null : a;
  }
}
