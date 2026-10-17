package otlpdagr;

import com.google.protobuf.CodedOutputStream;
import io.opentelemetry.exporter.internal.otlp.traces.LowAllocationTraceRequestMarshaler;
import io.opentelemetry.exporter.internal.otlp.traces.TraceRequestMarshaler;
import io.opentelemetry.proto.trace.v1.TracesData;
import io.opentelemetry.sdk.trace.data.SpanData;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import otlpgen.OtlpTraces;

/**
 * One export of the 500-span benchmark batch (spec/38 §10.3.2): OTel Java SDK spans in, the
 * encoded request in memory out, every buffer reused across calls. `Check` has proven first
 * that every row produces the same content.
 *
 * <p>Each call encodes the NEXT of {@value #BATCHES} copies of the batch, whose span names
 * and attribute values are distinct string instances — as an exporter sees new spans every
 * export — while attribute keys are shared {@code AttributeKey} constants, as instrumentation
 * declares them. Encoding one batch over and over would let any per-instance cache (Dagr's
 * text measure cache, opentelemetry-java's per-key bytes) hit on every value.
 *
 * <ul>
 *   <li>{@code otelDefault} — opentelemetry-java's OTLP exporter as configured out of the box
 *       (MemoryMode.IMMUTABLE_DATA): a marshaler tree per export, then the bytes;
 *   <li>{@code otelLowAllocation} — its REUSABLE_DATA mode: a stateless marshaler reused
 *       across exports;
 *   <li>{@code dagr} — the Dagr exporter: the direct builder's value tree from the spans, then
 *       the buffer;
 *   <li>{@code dagrBuildOnly} / {@code protobufJavaBuildOnly} — the serializers alone, from a
 *       value tree / protobuf message built in setup: what each format's writer costs once
 *       the data is in its own model.
 * </ul>
 */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 5, time = 2)
public class EncodeBench {
  static final int BATCHES = 16;
  List<SpanData>[] batches;
  OtlpTraces.TracesData[] values;
  int next;
  ByteArrayOutputStream bos;
  LowAllocationTraceRequestMarshaler low;
  DagrTraceEncoder dagr;
  TracesData message;
  byte[] pbBuf;

  private int turn() {
    final int i = next;
    next = (i + 1) % BATCHES;
    return i;
  }

  @Setup
  @SuppressWarnings("unchecked")
  public void setup() throws IOException {
    TracesData fixture = TracesData.parseFrom(Files.readAllBytes(Path.of(System.getProperty("otlpdagr.fixture"))));
    batches = new List[BATCHES];
    values = new OtlpTraces.TracesData[BATCHES];
    for (int i = 0; i < BATCHES; i++) {
      batches[i] = Fixture.spans(fixture);
      values[i] = DagrTraceEncoder.toValue(batches[i]);
    }
    bos = new ByteArrayOutputStream(1 << 19);
    low = new LowAllocationTraceRequestMarshaler();
    dagr = new DagrTraceEncoder();
    TraceRequestMarshaler.create(batches[0]).writeBinaryTo(bos);
    message = TracesData.parseFrom(bos.toByteArray());   // what the SDK path produces
    pbBuf = new byte[message.getSerializedSize()];
  }

  @Benchmark
  public int otelDefault() throws IOException {
    bos.reset();
    TraceRequestMarshaler.create(batches[turn()]).writeBinaryTo(bos);
    return bos.size();
  }

  @Benchmark
  public int otelLowAllocation() throws IOException {
    bos.reset();
    low.initialize(batches[turn()]);
    try {
      low.writeBinaryTo(bos);
    } finally {
      low.reset();
    }
    return bos.size();
  }

  @Benchmark
  public long dagr() {
    return dagr.encode(batches[turn()]).byteSize();
  }

  @Benchmark
  public long dagrBuildOnly() {
    return dagr.build(values[turn()]).byteSize();
  }

  @Benchmark
  public int protobufJavaBuildOnly() throws IOException {
    CodedOutputStream out = CodedOutputStream.newInstance(pbBuf);
    message.writeTo(out);
    return out.getTotalBytesWritten();
  }
}
