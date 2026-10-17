package otlpdagr;

import dagr.runtime.Builder;
import dagr.runtime.Writer;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.sdk.common.InstrumentationScopeInfo;
import io.opentelemetry.sdk.resources.Resource;
import io.opentelemetry.sdk.trace.data.EventData;
import io.opentelemetry.sdk.trace.data.LinkData;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.sdk.trace.data.StatusData;
import java.lang.foreign.MemorySegment;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.function.BiConsumer;
import otlpgen.OtlpTraces;

/**
 * OTel Java SDK spans → an `OtlpTraces` Dagr buffer in ONE pass (spec/38 §10.3.2): the Java twin
 * of `go/pdatabridge.StreamBuilder`. {@link DagrTraceEncoder} builds the direct builder's value
 * tree for the whole batch and then serializes it; this writes each node straight into the
 * builder as it walks the spans — back to front, because the builder stores back to front — so
 * nothing batch-sized is built and, at steady state, nothing is allocated.
 *
 * <p>The node writers below MIRROR the generated {@code put} methods of `OtlpTraces` (field order,
 * tags, forms) and the mapping of {@link DagrTraceEncoder} (absent when empty, zero or all-zero;
 * the marshaler's span flags). They do not generate from the schema, so `Check` gates them
 * against the value-tree path byte for byte, on the benchmark batch and on a batch that reaches
 * every branch — a schema change that moves a field fails there, never silently.
 *
 * <p>Allocation-free except where the SDK hands over something rare: a span with LINKS, and an
 * ARRAY-valued attribute, go through the generated value types.
 */
public final class DagrTraceStreamEncoder {
  private static final int HAS_IS_REMOTE = 0x100;
  private static final int IS_REMOTE = 0x200;

  private final Builder b;

  // grouping, reused: the distinct (resource, scope) pairs in first-seen order, each pair's
  // resource rank (first-seen order of resources), and the spans ordered by group
  private Resource[] gRes = new Resource[4];
  private InstrumentationScopeInfo[] gScope = new InstrumentationScopeInfo[4];
  private int[] gRank = new int[4];
  private int[] gOrder = new int[4];
  private int[] gStart = new int[5];
  private int nGroups;
  private Resource[] ranked = new Resource[4];
  private int nRanked;
  private SpanData[] in = new SpanData[64];
  private int[] spanGroup = new int[64];
  private SpanData[] sorted = new SpanData[64];

  // one attribute set, copied out so it can be written back to front
  private AttributeKey<?>[] keys = new AttributeKey<?>[16];
  private Object[] vals = new Object[16];
  private int nAttrs;
  private int attrsHigh;   // the most attributes copied out this encode: cleared once, at its end
  private final BiConsumer<AttributeKey<?>, Object> collect = (k, v) -> {
    if (nAttrs == keys.length) {
      keys = Arrays.copyOf(keys, nAttrs * 2);
      vals = Arrays.copyOf(vals, nAttrs * 2);
    }
    keys[nAttrs] = k;
    vals[nAttrs++] = v;
  };

  // ids decoded from the SDK's hex strings (getTraceIdBytes() allocates per call)
  private final byte[] traceId = new byte[16];
  private final byte[] spanId = new byte[8];
  private final byte[] parentId = new byte[8];
  private final MemorySegment traceIdSeg = MemorySegment.ofArray(traceId);
  private final MemorySegment spanIdSeg = MemorySegment.ofArray(spanId);
  private final MemorySegment parentIdSeg = MemorySegment.ofArray(parentId);

  public DagrTraceStreamEncoder() {
    this(Builder.growing(1 << 18));
  }

  public DagrTraceStreamEncoder(Builder b) {
    this.b = b;
  }

  public Builder builder() {
    return b;
  }

  /** The batch as a Dagr buffer: a view into this encoder's builder, valid until the next call. */
  public MemorySegment encode(Collection<SpanData> spans) {
    group(spans);
    b.reset();
    // TracesData { resource_spans: field 0, a packed node array }
    final long before = b.cursor();
    if (nGroups > 0) {
      final long arr = b.cursor();
      int nRes = 0;
      for (int g = nGroups; g > 0; ) {   // resources back to front; each is a run of groups
        final int end = g;
        final int rank = gRank[gOrder[g - 1]];
        while (g > 0 && gRank[gOrder[g - 1]] == rank) g--;
        putResourceSpans(g, end);
        nRes++;
      }
      Writer.pkFinishBlock(b, arr, nRes);
      Writer.storePackedTag(b, 0, true);
    }
    Writer.storeLeb(b, b.cursor() - before);
    Writer.storeGraphFraming(b, 1);
    Arrays.fill(keys, 0, attrsHigh, null);   // hold no span, key or value past the call
    Arrays.fill(vals, 0, attrsHigh, null);
    attrsHigh = 0;
    Arrays.fill(in, 0, spans.size(), null);
    Arrays.fill(sorted, 0, spans.size(), null);
    return b.bytes();
  }

  // ── grouping: by resource, then scope, in first-seen order (DagrTraceEncoder's LinkedHashMap) ──

  private void group(Collection<SpanData> spans) {
    final int n = spans.size();
    if (in.length < n) {
      in = new SpanData[n + n / 2];
      spanGroup = new int[in.length];
      sorted = new SpanData[in.length];
    }
    nGroups = 0;
    nRanked = 0;
    int last = -1;
    int i = 0;
    for (SpanData s : spans) {
      in[i] = s;
      final Resource r = s.getResource();
      final InstrumentationScopeInfo sc = s.getInstrumentationScopeInfo();
      int g = last;
      if (g < 0 || gRes[g] != r || gScope[g] != sc) g = findGroup(r, sc);
      spanGroup[i++] = g;
      last = g;
    }
    Arrays.fill(gRes, 0, nGroups, null);
    Arrays.fill(gScope, 0, nGroups, null);
    // groups ordered by resource rank, first-seen order within a rank (a stable insertion sort:
    // there are few groups)
    for (int g = 0; g < nGroups; g++) {
      int k = g;
      while (k > 0 && gRank[gOrder[k - 1]] > gRank[g]) {
        gOrder[k] = gOrder[k - 1];
        k--;
      }
      gOrder[k] = g;
    }
    // spans ordered by group, their order within a group kept (a counting sort)
    Arrays.fill(gStart, 0, nGroups + 1, 0);
    final int[] pos = gStart;
    final int[] slotOf = scratchSlots(nGroups);   // per group: its span count, then its next slot
    for (int k = 0; k < n; k++) slotOf[spanGroup[k]]++;
    int at = 0;
    for (int k = 0; k < nGroups; k++) {
      final int g = gOrder[k];
      final int c = slotOf[g];
      pos[k] = at;
      slotOf[g] = at;
      at += c;
    }
    pos[nGroups] = at;
    for (int k = 0; k < n; k++) sorted[slotOf[spanGroup[k]]++] = in[k];
    Arrays.fill(ranked, 0, nRanked, null);
  }

  private int[] slots = new int[4];

  private int[] scratchSlots(int n) {
    if (slots.length < n) slots = new int[n * 2];
    Arrays.fill(slots, 0, n, 0);
    return slots;
  }

  private int findGroup(Resource r, InstrumentationScopeInfo sc) {
    for (int g = 0; g < nGroups; g++) {
      if ((gRes[g] == r || gRes[g].equals(r)) && (gScope[g] == sc || gScope[g].equals(sc))) return g;
    }
    int rank = -1;
    for (int k = 0; k < nRanked; k++) {
      if (ranked[k] == r || ranked[k].equals(r)) {
        rank = k;
        break;
      }
    }
    if (rank < 0) {
      if (nRanked == ranked.length) ranked = Arrays.copyOf(ranked, nRanked * 2);
      ranked[nRanked] = r;
      rank = nRanked++;
    }
    if (nGroups == gRes.length) {
      final int m = nGroups * 2;
      gRes = Arrays.copyOf(gRes, m);
      gScope = Arrays.copyOf(gScope, m);
      gRank = Arrays.copyOf(gRank, m);
      gOrder = Arrays.copyOf(gOrder, m);
      gStart = Arrays.copyOf(gStart, m + 1);
    }
    gRes[nGroups] = r;
    gScope[nGroups] = sc;
    gRank[nGroups] = rank;
    return nGroups++;
  }

  // ── the container levels: OtlpTraces.ResourceSpans.put / ScopeSpans.put, mirrored ──

  /** One ResourceSpans: the ordered groups [from, to) share its resource. */
  private void putResourceSpans(int from, int to) {
    final SpanData first = sorted[gStart[from]];
    final Resource res = first.getResource();
    final long before = b.cursor();
    final String schemaUrl = res.getSchemaUrl();
    if (schemaUrl != null && !schemaUrl.isEmpty()) Writer.storePackedTextField(b, schemaUrl, 2);
    final long arr = b.cursor();
    for (int k = to - 1; k >= from; k--) putScopeSpans(k);
    Writer.pkFinishBlock(b, arr, to - from);
    Writer.storePackedTag(b, 1, true);
    // Resource { attributes: field 0 } — no dropped count, no entity refs in the SDK
    final long r = b.cursor();
    putAttributes(res.getAttributes(), 0);
    Writer.storeLeb(b, b.cursor() - r);
    Writer.storePackedTag(b, 0, true);
    Writer.storeLeb(b, b.cursor() - before);
  }

  /** One ScopeSpans: the k-th group in order. */
  private void putScopeSpans(int k) {
    final int from = gStart[k];
    final int to = gStart[k + 1];
    final InstrumentationScopeInfo sc = sorted[from].getInstrumentationScopeInfo();
    final long before = b.cursor();
    final String schemaUrl = sc.getSchemaUrl();
    if (schemaUrl != null && !schemaUrl.isEmpty()) Writer.storePackedTextField(b, schemaUrl, 2);
    final long arr = b.cursor();
    for (int i = to - 1; i >= from; i--) putSpan(sorted[i]);
    Writer.pkFinishBlock(b, arr, to - from);
    Writer.storePackedTag(b, 1, true);
    // InstrumentationScope { name 0, version 1, attributes 2 }
    final long s = b.cursor();
    putAttributes(sc.getAttributes(), 2);
    text(sc.getVersion(), 1);
    text(sc.getName(), 0);
    Writer.storeLeb(b, b.cursor() - s);
    Writer.storePackedTag(b, 0, true);
    Writer.storeLeb(b, b.cursor() - before);
  }

  // ── the leaves: OtlpTraces.Span.put / Event.put / Status.put / KeyValue.put, mirrored ──

  private void putSpan(SpanData s) {
    final long before = b.cursor();
    final StatusData st = s.getStatus();
    final int code = switch (st.getStatusCode()) {
      case OK -> 1;
      case ERROR -> 2;
      default -> 0;
    };
    final String message = st.getDescription();
    if (code != 0 || !message.isEmpty()) {
      final long at = b.cursor();
      Writer.storePackedByteField(b, code, 1);
      text(message, 0);
      Writer.storeLeb(b, b.cursor() - at);
      Writer.storePackedTag(b, 15, true);
    }
    final List<LinkData> links = s.getLinks();
    final int droppedLinks = s.getTotalRecordedLinks() - links.size();
    if (droppedLinks > 0) Writer.storePackedIntField(b, droppedLinks, 4, false, 14, false);
    if (!links.isEmpty()) {
      final long arr = b.cursor();
      for (int i = links.size() - 1; i >= 0; i--) OtlpTraces.Link.put(b, DagrTraceEncoder.link(links.get(i)));
      Writer.pkFinishBlock(b, arr, links.size());
      Writer.storePackedTag(b, 13, true);
    }
    final List<EventData> events = s.getEvents();
    final int droppedEvents = s.getTotalRecordedEvents() - events.size();
    if (droppedEvents > 0) Writer.storePackedIntField(b, droppedEvents, 4, false, 12, false);
    if (!events.isEmpty()) {
      final long arr = b.cursor();
      for (int i = events.size() - 1; i >= 0; i--) putEvent(events.get(i));
      Writer.pkFinishBlock(b, arr, events.size());
      Writer.storePackedTag(b, 11, true);
    }
    final Attributes attrs = s.getAttributes();
    final int droppedAttrs = s.getTotalAttributeCount() - attrs.size();
    if (droppedAttrs > 0) Writer.storePackedIntField(b, droppedAttrs, 4, false, 10, false);
    putAttributes(attrs, 9);
    if (s.getEndEpochNanos() != 0) Writer.storePackedIntField(b, s.getEndEpochNanos(), 8, false, 8, true);
    if (s.getStartEpochNanos() != 0) Writer.storePackedIntField(b, s.getStartEpochNanos(), 8, false, 7, true);
    Writer.storePackedByteField(b, s.getKind().ordinal() + 1, 6);   // INTERNAL..CONSUMER = proto 1..5
    text(s.getName(), 5);
    final SpanContext ctx = s.getSpanContext();
    final SpanContext parent = s.getParentSpanContext();
    Writer.storePackedIntField(b, (ctx.getTraceFlags().asByte() & 0xff) | HAS_IS_REMOTE
        | (parent.isRemote() ? IS_REMOTE : 0), 4, false, 4, false);
    if (parent.isValid()) {
      hex(parent.getSpanId(), parentId);
      Writer.storePackedBlobField(b, parentIdSeg, 3);
    }
    if (!ctx.getTraceState().isEmpty()) Writer.storePackedTextField(b, DagrTraceEncoder.traceState(ctx.getTraceState()), 2);
    if (ctx.isValid()) {
      hex(ctx.getSpanId(), spanId);
      Writer.storePackedBlobField(b, spanIdSeg, 1);
      hex(ctx.getTraceId(), traceId);
      Writer.storePackedBlobField(b, traceIdSeg, 0);
    }
    Writer.storeLeb(b, b.cursor() - before);
  }

  private void putEvent(EventData e) {
    final long before = b.cursor();
    if (e.getDroppedAttributesCount() != 0) Writer.storePackedIntField(b, e.getDroppedAttributesCount(), 4, false, 3, false);
    putAttributes(e.getAttributes(), 2);
    text(e.getName(), 1);
    if (e.getEpochNanos() != 0) Writer.storePackedIntField(b, e.getEpochNanos(), 8, false, 0, true);
    Writer.storeLeb(b, b.cursor() - before);
  }

  /** A `[KeyValue]` field at {@code index}, absent when there are no attributes. */
  private void putAttributes(Attributes attrs, long index) {
    if (attrs.isEmpty()) return;
    nAttrs = 0;
    attrs.forEach(collect);
    final int n = nAttrs;
    final long arr = b.cursor();
    for (int i = n - 1; i >= 0; i--) {
      final long before = b.cursor();
      final long header = putValue(keys[i], vals[i]);
      if (header >= 0) {
        Writer.storeLeb(b, header);
        Writer.storePackedTag(b, 1, false);
      }
      text(keys[i].getKey(), 0);
      Writer.storeLeb(b, b.cursor() - before);
    }
    if (n > attrsHigh) attrsHigh = n;
    Writer.pkFinishBlock(b, arr, n);
    Writer.storePackedTag(b, index, true);
  }

  /** An attribute value as an `AnyValue` payload; its header, or -1 when it has none. */
  private long putValue(AttributeKey<?> k, Object v) {
    switch (k.getType()) {
      case STRING -> {
        Writer.storeText(b, (String) v);
        return 6;                                                           // string_value, code 6
      }
      case BOOLEAN -> {
        Writer.storeU8(b, (Boolean) v ? 1 : 0);
        return (1 << 3) | 1;
      }
      case LONG -> {
        return (2 << 3) | Writer.storePackedUnionInt(b, (Long) v, 8, true);
      }
      case DOUBLE -> {
        return (3 << 3) | Writer.storePackedUnionF64(b, (Double) v);
      }
      default -> {
        final OtlpTraces.AnyValue av = DagrTraceEncoder.anyValue(k, v);   // arrays: rare, allocates
        return av == null ? -1 : OtlpTraces.AnyValue.putPayload(b, av);
      }
    }
  }

  private void text(String s, long index) {
    if (s != null && !s.isEmpty()) Writer.storePackedTextField(b, s, index);
  }

  // an id's hex digit → its value, by table: random hex makes a `c <= '9'` branch mispredict
  private static final byte[] NIBBLE = new byte[128];

  static {
    for (int c = 0; c < 10; c++) NIBBLE['0' + c] = (byte) c;
    for (int c = 0; c < 6; c++) {
      NIBBLE['a' + c] = (byte) (10 + c);
      NIBBLE['A' + c] = (byte) (10 + c);
    }
  }

  /** The SDK's id string (lowercase hex, validated by SpanContext) into {@code dst}. */
  private static void hex(String s, byte[] dst) {
    for (int i = 0; i < dst.length; i++) {
      dst[i] = (byte) ((NIBBLE[s.charAt(2 * i) & 0x7f] << 4) | NIBBLE[s.charAt(2 * i + 1) & 0x7f]);
    }
  }
}
