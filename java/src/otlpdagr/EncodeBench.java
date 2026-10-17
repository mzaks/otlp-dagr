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
  List<SpanData> spans;
  ByteArrayOutputStream bos;
  LowAllocationTraceRequestMarshaler low;
  DagrTraceEncoder dagr;
  OtlpTraces.TracesData values;
  TracesData message;
  byte[] pbBuf;

  @Setup
  public void setup() throws IOException {
    TracesData fixture = TracesData.parseFrom(Files.readAllBytes(Path.of(System.getProperty("otlpdagr.fixture"))));
    spans = Fixture.spans(fixture);
    bos = new ByteArrayOutputStream(1 << 19);
    low = new LowAllocationTraceRequestMarshaler();
    dagr = new DagrTraceEncoder();
    values = DagrTraceEncoder.toValue(spans);
    TraceRequestMarshaler.create(spans).writeBinaryTo(bos);
    message = TracesData.parseFrom(bos.toByteArray());   // what the SDK path produces
    pbBuf = new byte[message.getSerializedSize()];
  }

  @Benchmark
  public int otelDefault() throws IOException {
    bos.reset();
    TraceRequestMarshaler.create(spans).writeBinaryTo(bos);
    return bos.size();
  }

  @Benchmark
  public int otelLowAllocation() throws IOException {
    bos.reset();
    low.initialize(spans);
    try {
      low.writeBinaryTo(bos);
    } finally {
      low.reset();
    }
    return bos.size();
  }

  @Benchmark
  public long dagr() {
    return dagr.encode(spans).byteSize();
  }

  @Benchmark
  public long dagrBuildOnly() {
    return dagr.build(values).byteSize();
  }

  @Benchmark
  public int protobufJavaBuildOnly() throws IOException {
    CodedOutputStream out = CodedOutputStream.newInstance(pbBuf);
    message.writeTo(out);
    return out.getTotalBytesWritten();
  }
}
