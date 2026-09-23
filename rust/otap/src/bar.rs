//! The OTAP envelope: `BatchArrowRecords` (otel-arrow
//! `proto/opentelemetry/proto/experimental/arrow/v1/arrow_service.proto`), written by hand —
//! three fields do not justify a protobuf toolchain.
//!
//! ```text
//! message BatchArrowRecords { int64 batch_id = 1; repeated ArrowPayload arrow_payloads = 2; bytes headers = 3; }
//! message ArrowPayload      { string schema_id = 1; ArrowPayloadType type = 2; bytes record = 3; }
//! ```
//!
//! Each payload's `record` continues an Arrow IPC *stream* (`Streams`): a new stream's first
//! payload carries the schema message, every payload its changed dictionaries and exactly
//! one record batch.

use arrow::error::ArrowError;
use arrow::ipc::CompressionType;
use arrow::ipc::writer::{IpcWriteOptions, StreamWriter};
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;

/// `ArrowPayloadType` values used by traces, logs and metrics.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum PayloadType {
    ResourceAttrs = 1,
    ScopeAttrs = 2,
    UnivariateMetrics = 10,
    NumberDataPoints = 11,
    SummaryDataPoints = 12,
    HistogramDataPoints = 13,
    ExpHistogramDataPoints = 14,
    NumberDpAttrs = 15,
    SummaryDpAttrs = 16,
    HistogramDpAttrs = 17,
    ExpHistogramDpAttrs = 18,
    NumberDpExemplars = 19,
    HistogramDpExemplars = 20,
    ExpHistogramDpExemplars = 21,
    NumberDpExemplarAttrs = 22,
    HistogramDpExemplarAttrs = 23,
    ExpHistogramDpExemplarAttrs = 24,
    MetricAttrs = 26,
    Logs = 30,
    LogAttrs = 31,
    Spans = 40,
    SpanAttrs = 41,
    SpanEvents = 42,
    SpanLinks = 43,
    SpanEventAttrs = 44,
    SpanLinkAttrs = 45,
}

/// The producer's IPC streams, one per payload type, kept from batch to batch as OTAP
/// intends: a payload whose schema matches its type's previous one continues that stream
/// under the same `schema_id` — no schema message, and no dictionary the receiver already
/// holds (see `dict.rs`) — and a changed schema starts a new stream under a fresh id, which
/// both consumers treat as a reset for that payload type. `schema_id`s come from one
/// counter: both consumers key their readers by `schema_id` ALONE, not by (type, id).
///
/// A stream is scoped to one receiver connection (one gRPC stream): a receiver that
/// joins mid-stream cannot decode it. `Producer::reset_streams` starts over.
#[derive(Default)]
pub struct Streams {
    next_id: u64,
    zstd: bool,
    by_type: Vec<Option<Stream>>, // indexed by ArrowPayloadType
}

struct Stream {
    schema: SchemaRef,
    id: String,
    w: StreamWriter<Vec<u8>>,
}

impl Streams {
    pub fn new(zstd: bool) -> Self {
        Streams { zstd, ..Default::default() }
    }

    pub fn is_zstd(&self) -> bool {
        self.zstd
    }

    /// The IPC messages for `batch` on its type's stream, and the stream's schema_id.
    fn write(&mut self, ty: PayloadType, batch: &RecordBatch) -> Result<(Vec<u8>, String), ArrowError> {
        let slot = ty as usize;
        if self.by_type.len() <= slot {
            self.by_type.resize_with(slot + 1, || None);
        }
        let same = matches!(&self.by_type[slot], Some(s) if s.schema == batch.schema());
        if !same {
            // Let the writer number the dictionaries: every Field here carries dict_id 0,
            // and a schema with two dictionaries under one id is rejected ("conflicting
            // dictionary types for id 0").
            let mut opts = IpcWriteOptions::default().with_preserve_dict_id(false);
            if self.zstd {
                // IPC body compression; both consumers decompress it (Go reads WithZstd)
                opts = opts.try_with_compression(Some(CompressionType::ZSTD))?;
            }
            let w = StreamWriter::try_new_with_options(Vec::new(), &batch.schema(), opts)?;
            let id = self.next_id.to_string();
            self.next_id += 1;
            self.by_type[slot] = Some(Stream { schema: batch.schema(), id, w });
        }
        let s = self.by_type[slot].as_mut().unwrap();
        s.w.write(batch)?;
        // no end-of-stream marker: the stream continues with the next batch
        Ok((std::mem::take(s.w.get_mut()), s.id.clone()))
    }
}

/// Builds one `BatchArrowRecords` on the producer's streams. Payloads with no rows are
/// dropped (otap-spec §3.1: an empty payload SHOULD be omitted and equals an absent one).
pub struct Bar<'p> {
    streams: &'p mut Streams,
    out: Vec<u8>,
}

impl<'p> Bar<'p> {
    pub fn new(streams: &'p mut Streams, batch_id: i64) -> Self {
        let mut out = Vec::new();
        if batch_id != 0 {
            out.push(0x08);
            put_varint(&mut out, batch_id as u64);
        }
        Bar { streams, out }
    }

    pub fn payload(&mut self, ty: PayloadType, batch: RecordBatch) -> Result<(), ArrowError> {
        if batch.num_rows() == 0 {
            return Ok(());
        }
        let (record, schema_id) = self.streams.write(ty, &batch)?;
        let mut msg = Vec::with_capacity(record.len() + 16);
        msg.push(0x0a);
        put_varint(&mut msg, schema_id.len() as u64);
        msg.extend_from_slice(schema_id.as_bytes());
        msg.push(0x10);
        put_varint(&mut msg, ty as u64);
        msg.push(0x1a);
        put_varint(&mut msg, record.len() as u64);
        msg.extend_from_slice(&record);

        self.out.push(0x12);
        put_varint(&mut self.out, msg.len() as u64);
        self.out.extend_from_slice(&msg);
        Ok(())
    }

    pub fn finish(self) -> Vec<u8> {
        self.out
    }
}

fn put_varint(out: &mut Vec<u8>, mut v: u64) {
    while v >= 0x80 {
        out.push((v as u8) | 0x80);
        v >>= 7;
    }
    out.push(v as u8);
}
