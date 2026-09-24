//! Dagr → OTAP: OTLP read in place from Dagr bytes (the generated lazy accessors — no value
//! tree, no protobuf) and written as OTAP (OpenTelemetry Arrow Protocol) record batches in
//! a `BatchArrowRecords` envelope, the input of otel-arrow's receivers.
//!
//! The wire contract implemented here, with sources, is `spec/38` §9 in the Dagr repo. The
//! short version of what makes it subtle: every `id` / `parent_id` column must be in its
//! transport encoding (DELTA or QUASI-DELTA), because the Go consumer the collector's
//! `otelarrowreceiver` runs ignores the `encoding: plain` metadata the Rust one honours —
//! plain ids reach it as silently wrong data.

pub mod attrs;
pub mod bar;
pub mod cbor;
pub mod cols;
pub mod dict;
pub mod envelope;
pub mod logs;
pub mod metrics;
pub mod spool;
pub mod timing;
pub mod traces;

use std::fmt;

use arrow::error::ArrowError;
use otlp_dagr::dagr_runtime::DagrError;

pub use bar::{Bar, PayloadType};

#[derive(Debug)]
pub enum Error {
    Dagr(DagrError),
    Arrow(ArrowError),
    /// A u16 id space (root items, resources, scopes) overflowed: OTAP caps each at 65,536
    /// per batch, so the input has to be split first.
    TooMany(&'static str),
    /// A fixed-width id (trace 16 B / span 8 B) of another length.
    BadId(&'static str, usize),
    /// A spool this build cannot convert (see `spool::header`).
    Spool(&'static str),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Dagr(e) => write!(f, "dagr: {e:?}"),
            Error::Arrow(e) => write!(f, "arrow: {e}"),
            Error::TooMany(what) => write!(f, "more than 65,536 {what} in one OTAP batch"),
            Error::BadId(what, n) => write!(f, "{what} is {n} bytes"),
            Error::Spool(why) => write!(f, "spool: {why}"),
        }
    }
}

impl std::error::Error for Error {}

impl From<DagrError> for Error {
    fn from(e: DagrError) -> Self {
        Error::Dagr(e)
    }
}

impl From<ArrowError> for Error {
    fn from(e: ArrowError) -> Self {
        Error::Arrow(e)
    }
}

pub(crate) fn id16(n: u32, what: &'static str) -> Result<u16, Error> {
    u16::try_from(n).map_err(|_| Error::TooMany(what))
}

/// A fixed-width id: absent or empty is all zeroes (pdata's "unset"), any other length is an
/// error rather than a truncation.
pub(crate) fn fixed<'a, const N: usize>(v: Option<&'a [u8]>, what: &'static str) -> Result<&'a [u8], Error> {
    const Z: [u8; 16] = [0; 16];
    match v {
        None => Ok(&Z[..N]),
        Some(b) if b.is_empty() => Ok(&Z[..N]),
        Some(b) if b.len() == N => Ok(b),
        Some(b) => Err(Error::BadId(what, b.len())),
    }
}

/// Converts Dagr batches into `BatchArrowRecords`, one after another on ONE OTAP stream (one
/// receiver connection): it holds the IPC streams (schemas sent once, see `bar::Streams`),
/// the column dictionaries (values sent once, see `dict`) and warm column builders.
#[derive(Default)]
pub struct Producer {
    streams: bar::Streams,
    batches: i64,
    tables: Tables,
}

/// The per-signal column builders (warm across batches).
#[derive(Default)]
struct Tables {
    traces: traces::Traces,
    logs: logs::Logs,
    metrics: metrics::Metrics,
}

impl Producer {
    /// A whole spool file → one serialized `BatchArrowRecords` per record, in order, on this
    /// producer's stream, each handed to `each`. The signal comes from the spool header.
    pub fn spool(&mut self, spool: &[u8], mut each: impl FnMut(Vec<u8>)) -> Result<spool::Summary, Error> {
        let (signal, h) = spool::header(spool)?;
        let mut records = 0;
        match signal {
            spool::Signal::Traces => {
                for r in spool::traces(spool)? {
                    each(self.one(|t, bar| t.traces.record(&r, bar))?);
                    records += 1;
                }
            }
            spool::Signal::Logs => {
                for r in spool::logs(spool)? {
                    each(self.one(|t, bar| t.logs.record(&r, bar))?);
                    records += 1;
                }
            }
            spool::Signal::Metrics => {
                for r in spool::metrics(spool)? {
                    each(self.one(|t, bar| t.metrics.record(&r, bar))?);
                    records += 1;
                }
            }
            spool::Signal::Profiles => unreachable!("refused by spool::header"),
        }
        Ok(spool::Summary { signal, producer: h.producer.clone(), created_unix_nano: h.created_unix_nano, records })
    }

    /// One SpanLog record's content — the bytes Go's `spool.Batch.Record` holds (content_size
    /// LEB + packed body) — → one serialized `BatchArrowRecords`.
    pub fn traces_record(&mut self, record: &[u8]) -> Result<Vec<u8>, Error> {
        let r = otlp_dagr::span_log_sink::BatchRecord::restore(record, 0);
        self.one(|t, bar| t.traces.record(&r, bar))
    }

    /// One LogRecordLog record's content → one serialized `BatchArrowRecords`.
    pub fn logs_record(&mut self, record: &[u8]) -> Result<Vec<u8>, Error> {
        let r = otlp_dagr::log_record_log_sink::BatchRecord::restore(record, 0);
        self.one(|t, bar| t.logs.record(&r, bar))
    }

    /// One MetricLog record's content → one serialized `BatchArrowRecords`.
    pub fn metrics_record(&mut self, record: &[u8]) -> Result<Vec<u8>, Error> {
        let r = otlp_dagr::metric_log_sink::BatchRecord::restore(record, 0);
        self.one(|t, bar| t.metrics.record(&r, bar))
    }

    /// One batch on this producer's stream: `fill` writes the payloads.
    fn one(&mut self, fill: impl FnOnce(&mut Tables, &mut Bar<'_>) -> Result<(), Error>) -> Result<Vec<u8>, Error> {
        self.batches += 1;
        let mut bar = Bar::new(&mut self.streams, self.batches);
        fill(&mut self.tables, &mut bar)?;
        Ok(bar.finish())
    }

    /// One OtlpTraces buffer → one serialized `BatchArrowRecords`.
    pub fn traces(&mut self, dagr: &[u8]) -> Result<Vec<u8>, Error> {
        self.one(|t, bar| t.traces.convert(dagr, bar))
    }

    /// One OtlpLogs buffer → one serialized `BatchArrowRecords`.
    pub fn logs(&mut self, dagr: &[u8]) -> Result<Vec<u8>, Error> {
        self.one(|t, bar| t.logs.convert(dagr, bar))
    }

    /// zstd-compress every IPC body (otel-arrow's producer does by default; both consumers
    /// read it).
    pub fn with_zstd(mut self, on: bool) -> Self {
        self.streams = bar::Streams::new(on);
        self
    }

    /// Starts every payload type's stream over (schemas and dictionaries re-sent) — for a
    /// new receiver connection. Column dictionaries are kept; the next batch re-sends them.
    pub fn reset_streams(&mut self) {
        let zstd = std::mem::take(&mut self.streams);
        self.streams = bar::Streams::new(zstd.is_zstd());
    }

    /// Emit metric `metadata` as METRIC_ATTRS — for a Rust (df-engine) receiver only; see
    /// `metrics::Metrics::metric_metadata`.
    pub fn with_metric_metadata(mut self, on: bool) -> Self {
        self.tables.metrics.metric_metadata = on;
        self
    }

    /// One OtlpMetrics buffer → one serialized `BatchArrowRecords`. A metric's `metadata`
    /// is dropped unless `with_metric_metadata` (the Go consumer rejects METRIC_ATTRS).
    pub fn metrics(&mut self, dagr: &[u8]) -> Result<Vec<u8>, Error> {
        self.one(|t, bar| t.metrics.convert(dagr, bar))
    }
}
