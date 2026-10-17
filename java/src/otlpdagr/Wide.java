package otlpdagr;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.TraceFlags;
import io.opentelemetry.api.trace.TraceState;
import io.opentelemetry.sdk.common.InstrumentationLibraryInfo;
import io.opentelemetry.sdk.common.InstrumentationScopeInfo;
import io.opentelemetry.sdk.resources.Resource;
import io.opentelemetry.sdk.trace.data.EventData;
import io.opentelemetry.sdk.trace.data.LinkData;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.sdk.trace.data.StatusData;
import java.util.ArrayList;
import java.util.List;

/**
 * A batch that reaches every branch of the span mapping, so `Check` can hold the one-pass
 * encoder to the value-tree one: resources and scopes interleaved (and one resource EQUAL to
 * another but a different instance), schema URLs, empty names, invalid / sampled / remote span
 * contexts, trace state, links with attributes, dropped counts at every level, every status, and
 * every attribute type — arrays included.
 */
final class Wide {
  private Wide() {}

  static List<SpanData> spans() {
    Attributes ra = Attributes.of(AttributeKey.stringKey("service.name"), "checkout",
        AttributeKey.longKey("process.pid"), 4242L);
    Resource r1 = Resource.create(ra, "https://opentelemetry.io/schemas/1.30.0");
    Resource r1copy = Resource.create(ra, "https://opentelemetry.io/schemas/1.30.0");   // equal, not same
    Resource r2 = Resource.empty();
    InstrumentationScopeInfo s1 = InstrumentationScopeInfo.builder("io.example.http").setVersion("1.2.3")
        .setSchemaUrl("https://opentelemetry.io/schemas/1.29.0")
        .setAttributes(Attributes.of(AttributeKey.booleanKey("scope.flag"), true)).build();
    InstrumentationScopeInfo s2 = InstrumentationScopeInfo.create("io.example.db");
    InstrumentationScopeInfo s3 = InstrumentationScopeInfo.create("");

    String tid = "0af7651916cd43dd8448eb211c80319c";
    SpanContext sampled = SpanContext.create(tid, "b7ad6b7169203331", TraceFlags.getSampled(),
        TraceState.builder().put("vendor", "x1").put("other", "y").build());
    SpanContext plain = SpanContext.create(tid, "00f067aa0ba902b7", TraceFlags.getDefault(), TraceState.getDefault());
    SpanContext remote = SpanContext.createFromRemoteParent(tid, "53995c3f42cd8ad8", TraceFlags.getSampled(),
        TraceState.getDefault());

    Attributes every = Attributes.builder()
        .put("s", "value").put("empty", "").put("l", -9_000_000_000L).put("small", 3L)
        .put("d", 0.1).put("d.int", 2.0).put("b", false)
        .put(AttributeKey.stringArrayKey("sa"), List.of("a", "", "c"))
        .put(AttributeKey.longArrayKey("la"), List.of(1L, -2L))
        .put(AttributeKey.doubleArrayKey("da"), List.of(1.5))
        .put(AttributeKey.booleanArrayKey("ba"), List.of(true, false))
        .put("unicode", "café ☕ 😀").build();
    List<EventData> events = List.of(
        EventData.create(1_700_000_000_000_000_123L, "exception",
            Attributes.of(AttributeKey.stringKey("exception.type"), "java.io.IOException"), 3),
        EventData.create(0, "", Attributes.empty()));
    List<LinkData> links = List.of(LinkData.create(remote, Attributes.of(AttributeKey.longKey("n"), 1L), 2),
        LinkData.create(plain));

    List<SpanData> out = new ArrayList<>();
    Object[][] layout = {{r1, s1}, {r2, s1}, {r1, s2}, {r1copy, s1}, {r2, s3}, {r1, s1}, {r1copy, s2}, {r2, s1}};
    int i = 0;
    for (Object[] rs : layout) {
      for (int k = 0; k < 3; k++, i++) {
        boolean odd = i % 2 == 1;
        out.add(new Span(
            i % 5 == 0 ? "" : "op-" + i,
            SpanKind.values()[i % SpanKind.values().length],
            i % 7 == 3 ? SpanContext.getInvalid() : (odd ? sampled : plain),
            i % 3 == 0 ? SpanContext.getInvalid() : (odd ? remote : plain),
            switch (i % 4) {
              case 0 -> StatusData.unset();
              case 1 -> StatusData.ok();
              case 2 -> StatusData.create(StatusCode.ERROR, "boom " + i);
              default -> StatusData.create(StatusCode.UNSET, "unset but described");
            },
            i % 6 == 0 ? 0 : 1_700_000_000_000_000_000L + i,
            i % 6 == 1 ? 0 : 1_700_000_000_000_500_000L + i,
            i % 4 == 0 ? Attributes.empty() : every,
            i % 3 == 1 ? events : List.of(),
            i % 4 == 2 ? links : List.of(),
            (i % 4 == 0 ? 0 : every.size()) + (i % 5 == 1 ? 4 : 0),
            (i % 3 == 1 ? events.size() : 0) + (i % 2 == 0 ? 1 : 0),
            (i % 4 == 2 ? links.size() : 0) + (i % 3 == 2 ? 5 : 0),
            (Resource) rs[0], (InstrumentationScopeInfo) rs[1]));
      }
    }
    return out;
  }

  record Span(String getName, SpanKind getKind, SpanContext getSpanContext, SpanContext getParentSpanContext,
      StatusData getStatus, long getStartEpochNanos, long getEndEpochNanos, Attributes getAttributes,
      List<EventData> getEvents, List<LinkData> getLinks, int getTotalAttributeCount,
      int getTotalRecordedEvents, int getTotalRecordedLinks, Resource getResource,
      InstrumentationScopeInfo getInstrumentationScopeInfo) implements SpanData {
    @Override public boolean hasEnded() { return true; }

    @SuppressWarnings("deprecation")
    @Override public InstrumentationLibraryInfo getInstrumentationLibraryInfo() {
      return InstrumentationLibraryInfo.create(getInstrumentationScopeInfo.getName(),
          getInstrumentationScopeInfo.getVersion());
    }
  }
}
