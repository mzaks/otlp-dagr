package otlpdagr;

import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.common.AttributesBuilder;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.TraceFlags;
import io.opentelemetry.api.trace.TraceState;
import io.opentelemetry.proto.common.v1.AnyValue;
import io.opentelemetry.proto.common.v1.KeyValue;
import io.opentelemetry.proto.trace.v1.ResourceSpans;
import io.opentelemetry.proto.trace.v1.ScopeSpans;
import io.opentelemetry.proto.trace.v1.Span;
import io.opentelemetry.proto.trace.v1.TracesData;
import io.opentelemetry.sdk.common.InstrumentationLibraryInfo;
import io.opentelemetry.sdk.common.InstrumentationScopeInfo;
import io.opentelemetry.sdk.resources.Resource;
import io.opentelemetry.sdk.trace.data.EventData;
import io.opentelemetry.sdk.trace.data.LinkData;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.sdk.trace.data.StatusData;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HexFormat;
import java.util.List;

/**
 * The benchmark batch as OTel Java SDK spans: `go/cmd/otlpdagrctl fixtures` writes it as OTLP
 * protobuf (`traces-batch.pb`, the 500-span batch every otlp-dagr benchmark uses), and this
 * turns it into the {@link SpanData} an SDK exporter is handed. Covers what the fixture holds —
 * scalar attributes, events, status, one resource and scope — and refuses anything else.
 */
public final class Fixture {
  private Fixture() {}

  public static List<SpanData> spans(TracesData td) {
    List<SpanData> out = new ArrayList<>();
    for (ResourceSpans rs : td.getResourceSpansList()) {
      Resource res = Resource.create(attrs(rs.getResource().getAttributesList()));
      for (ScopeSpans ss : rs.getScopeSpansList()) {
        InstrumentationScopeInfo scope = InstrumentationScopeInfo.builder(ss.getScope().getName())
            .setVersion(ss.getScope().getVersion().isEmpty() ? null : ss.getScope().getVersion())
            .build();
        for (Span s : ss.getSpansList()) out.add(span(s, res, scope));
      }
    }
    return out;
  }

  /**
   * The fixture as the OTel Java SDK holds it: its `Attributes` keep keys SORTED (pdata keeps
   * insertion order), so this is the batch an SDK exporter's protobuf must equal.
   */
  public static TracesData sorted(TracesData td) {
    TracesData.Builder b = td.toBuilder();
    for (ResourceSpans.Builder rs : b.getResourceSpansBuilderList()) {
      sortInto(rs.getResourceBuilder().getAttributesList(), rs.getResourceBuilder()::clearAttributes,
          rs.getResourceBuilder()::addAllAttributes);
      for (ScopeSpans.Builder ss : rs.getScopeSpansBuilderList()) {
        for (Span.Builder s : ss.getSpansBuilderList()) {
          sortInto(s.getAttributesList(), s::clearAttributes, s::addAllAttributes);
          for (Span.Event.Builder e : s.getEventsBuilderList()) {
            sortInto(e.getAttributesList(), e::clearAttributes, e::addAllAttributes);
          }
        }
      }
    }
    return b.build();
  }

  private static void sortInto(List<KeyValue> kvs, Runnable clear, java.util.function.Consumer<List<KeyValue>> add) {
    List<KeyValue> s = new ArrayList<>(kvs);
    s.sort(Comparator.comparing(KeyValue::getKey));
    clear.run();
    add.accept(s);
  }

  private static Attributes attrs(List<KeyValue> kvs) {
    AttributesBuilder b = Attributes.builder();
    for (KeyValue kv : kvs) {
      AnyValue v = kv.getValue();
      switch (v.getValueCase()) {
        case STRING_VALUE -> b.put(kv.getKey(), v.getStringValue());
        case INT_VALUE -> b.put(kv.getKey(), v.getIntValue());
        case BOOL_VALUE -> b.put(kv.getKey(), v.getBoolValue());
        case DOUBLE_VALUE -> b.put(kv.getKey(), v.getDoubleValue());
        default -> throw new IllegalArgumentException("fixture attribute " + kv.getKey() + ": " + v.getValueCase());
      }
    }
    return b.build();
  }

  private static SpanData span(Span s, Resource res, InstrumentationScopeInfo scope) {
    if (s.getLinksCount() != 0 || !s.getTraceState().isEmpty() || s.getFlags() != 0) {
      throw new IllegalArgumentException("fixture span carries links / trace state / flags");
    }
    HexFormat hex = HexFormat.of();
    SpanContext ctx = SpanContext.create(hex.formatHex(s.getTraceId().toByteArray()),
        hex.formatHex(s.getSpanId().toByteArray()), TraceFlags.getDefault(), TraceState.getDefault());
    SpanContext parent = s.getParentSpanId().isEmpty() ? SpanContext.getInvalid()
        : SpanContext.create(ctx.getTraceId(), hex.formatHex(s.getParentSpanId().toByteArray()),
            TraceFlags.getDefault(), TraceState.getDefault());
    List<EventData> events = new ArrayList<>();
    for (Span.Event e : s.getEventsList()) {
      events.add(EventData.create(e.getTimeUnixNano(), e.getName(), attrs(e.getAttributesList())));
    }
    StatusData status = switch (s.getStatus().getCode()) {
      case STATUS_CODE_OK -> StatusData.ok();
      case STATUS_CODE_ERROR -> StatusData.create(StatusCode.ERROR, s.getStatus().getMessage());
      default -> StatusData.unset();
    };
    SpanKind kind = switch (s.getKind()) {
      case SPAN_KIND_SERVER -> SpanKind.SERVER;
      case SPAN_KIND_CLIENT -> SpanKind.CLIENT;
      case SPAN_KIND_PRODUCER -> SpanKind.PRODUCER;
      case SPAN_KIND_CONSUMER -> SpanKind.CONSUMER;
      default -> SpanKind.INTERNAL;
    };
    return new FixtureSpan(s.getName(), kind, ctx, parent, status, s.getStartTimeUnixNano(),
        s.getEndTimeUnixNano(), attrs(s.getAttributesList()), List.copyOf(events), res, scope);
  }

  /** A finished span, as the SDK's span processor hands it to an exporter. */
  record FixtureSpan(String getName, SpanKind getKind, SpanContext getSpanContext,
      SpanContext getParentSpanContext, StatusData getStatus, long getStartEpochNanos,
      long getEndEpochNanos, Attributes getAttributes, List<EventData> getEvents, Resource getResource,
      InstrumentationScopeInfo getInstrumentationScopeInfo) implements SpanData {
    @Override public List<LinkData> getLinks() { return List.of(); }
    @Override public boolean hasEnded() { return true; }
    @Override public int getTotalRecordedEvents() { return getEvents.size(); }
    @Override public int getTotalRecordedLinks() { return 0; }
    @Override public int getTotalAttributeCount() { return getAttributes.size(); }

    @SuppressWarnings("deprecation")
    @Override public InstrumentationLibraryInfo getInstrumentationLibraryInfo() {
      return InstrumentationLibraryInfo.create(getInstrumentationScopeInfo.getName(),
          getInstrumentationScopeInfo.getVersion());
    }
  }
}
