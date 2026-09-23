//! The OTAP envelope: `BatchArrowRecords` (otel-arrow
//! `proto/opentelemetry/proto/experimental/arrow/v1/arrow_service.proto`), written by hand —
//! three fields do not justify a protobuf toolchain.
//!
//! ```text
//! message BatchArrowRecords { int64 batch_id = 1; repeated ArrowPayload arrow_payloads = 2; bytes headers = 3; }
//! message ArrowPayload      { string schema_id = 1; ArrowPayloadType type = 2; bytes record = 3; }
//! ```
//!
//! Each payload's `record` is an Arrow IPC *stream*: the schema message, then exactly one
//! record batch. Both decoders (Go `arrow_record.Consumer`, Rust `otel-arrow-dfe-pdata`)
//! key their IPC readers by `schema_id` ALONE, not by (type, schema_id), and treat an unseen
//! id as a schema reset for that payload type — so every payload here gets a fresh id from
//! one counter and carries its own schema. That costs the schema bytes per payload and buys
//! a producer with no cross-batch state to get wrong.

use arrow::error::ArrowError;
use arrow::ipc::writer::StreamWriter;
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

/// Builds one `BatchArrowRecords`. Payloads with no rows are dropped (otap-spec §3.1: an
/// empty payload SHOULD be omitted and equals an absent one).
pub struct Bar<'p> {
    ids: &'p mut u64,
    batch_id: i64,
    out: Vec<u8>,
}

impl<'p> Bar<'p> {
    pub fn new(ids: &'p mut u64, batch_id: i64) -> Self {
        let mut out = Vec::new();
        if batch_id != 0 {
            out.push(0x08);
            put_varint(&mut out, batch_id as u64);
        }
        Bar { ids, batch_id, out }
    }

    pub fn batch_id(&self) -> i64 {
        self.batch_id
    }

    pub fn payload(&mut self, ty: PayloadType, batch: RecordBatch) -> Result<(), ArrowError> {
        if batch.num_rows() == 0 {
            return Ok(());
        }
        let mut w = StreamWriter::try_new(Vec::new(), &batch.schema())?;
        w.write(&batch)?;
        // finish() appends the end-of-stream marker; readers stop after the one batch they
        // expect and never reach it.
        let record = w.into_inner()?;
        let schema_id = self.ids.to_string();
        *self.ids += 1;

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
