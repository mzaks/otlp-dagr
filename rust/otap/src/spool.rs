//! Spools as input: a SpanLog / LogRecordLog / MetricLog file (otlp-dagr's exporter output,
//! `go/spool`) turned into OTAP batches, record by record, straight from the file's bytes.
//!
//! A spool record holds its batch INLINE — the signal graph's own `ResourceSpans` / … array
//! (Dagr spec/11 §12) — so the generated sink reader hands the bridge the same lazy accessors
//! a graph buffer does, borrowed from the spool: no copy, no re-encode, no protobuf.
//!
//! The spool's `SpoolHeader` (spec/38 §2.4) says which signal it holds; the header is read
//! first and a spool of an unknown signal, a newer format, or profiles (which OTAP has no
//! mapping for) is refused before any record is read. A torn tail (a record cut off by a
//! crash, down to its trailing span) ends the conversion: the records converted are the
//! intact prefix, exactly what `spool.Recover` keeps, and `Summary::records` says how many.

use otlp_dagr::{log_record_log_sink, metric_log_sink, span_log_sink};

use crate::Error;

/// The `SpoolHeader.signal` codes (otlp-dagr `go/spool`, `spool.Signal`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Signal {
    Traces = 0,
    Metrics = 1,
    Logs = 2,
    Profiles = 3,
}

/// The newest `format_version` this build reads (the receipt's `wire_format_version`).
pub const FORMAT_VERSION: u32 = 1;

/// What a spool held.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Summary {
    pub signal: Signal,
    pub producer: Option<String>,
    pub created_unix_nano: u64,
    /// batch records converted — one OTAP batch each
    pub records: usize,
}

/// Reads and checks the header; every sink declares the same `SpoolHeader`, so any of the
/// generated readers decodes it.
pub fn header(spool: &[u8]) -> Result<(Signal, span_log_sink::SpoolHeader), Error> {
    let mut hdr = None;
    span_log_sink::Reader::new_with_header(spool, |h| {
        hdr = Some(h.clone());
        Ok(())
    })?;
    let h = hdr.ok_or(Error::Spool("empty spool (no header)"))?;
    let signal = match h.signal {
        0 => Signal::Traces,
        1 => Signal::Metrics,
        2 => Signal::Logs,
        3 => Signal::Profiles,
        _ => return Err(Error::Spool("unknown signal code in the spool header")),
    };
    let name = match signal {
        Signal::Traces => "OtlpTraces",
        Signal::Metrics => "OtlpMetrics",
        Signal::Logs => "OtlpLogs",
        Signal::Profiles => "OtlpProfiles",
    };
    if h.signal_name != name {
        return Err(Error::Spool("spool header: signal code and name disagree"));
    }
    if h.format_version > FORMAT_VERSION {
        return Err(Error::Spool("spool written by a newer format version"));
    }
    if signal == Signal::Profiles {
        return Err(Error::Spool("profiles have no OTAP mapping"));
    }
    Ok((signal, h))
}

/// Whether the record at `range` (the generated reader's `RecordRange`: relative to
/// `records_start`, END including the trailing span) is whole: its reverse-delimited span
/// LEB is there and names the record's own length. The generated forward reader returns a
/// record as soon as its BODY is complete; the spool's own integrity rule (Go
/// `spool.CheckTail`) also wants the span — a crash between the two leaves a record that
/// a restart truncates away, and exporting it would diverge from the recovered spool.
fn intact(data: &[u8], records_start: usize, start: usize, end: usize) -> bool {
    let (begin, end) = (records_start + start, records_start + end);
    if end > data.len() {
        return false;
    }
    for l in 1..=10usize {
        let Some(body_end) = end.checked_sub(l) else { return false };
        let Some(span) = body_end.checked_sub(begin) else { return false };
        let mut leb = [0u8; 10];
        let mut n = 0;
        let mut v = span as u64;
        loop {
            let b = (v & 0x7f) as u8;
            v >>= 7;
            leb[n] = if v != 0 { b | 0x80 } else { b };
            n += 1;
            if v == 0 {
                break;
            }
        }
        if n == l {
            // the RLEB is the LEB's bytes in reverse order (spec/11 §11.4)
            return leb[..n].iter().rev().eq(data[body_end..end].iter());
        }
    }
    false
}

macro_rules! records {
    ($name:ident, $sink:ident) => {
        pub(crate) fn $name<'a>(spool: &'a [u8]) -> Result<impl Iterator<Item = $sink::BatchRecord<'a>>, Error> {
            let r = $sink::Reader::new_with_header(spool, |_| Ok(()))?;
            let rs = r.records_start();
            Ok(r.indexed().map_while(move |(range, rec)| intact(spool, rs, range.start, range.end).then_some(rec)).filter_map(
                |rec| match rec {
                    $sink::Record::Batch(b) => Some(b),
                    _ => None,
                },
            ))
        }
    };
}

records!(traces, span_log_sink);
records!(logs, log_record_log_sink);
records!(metrics, metric_log_sink);

#[cfg(test)]
mod tests {
    use super::*;
    use otlp_dagr::span_log_sink::{Batch, SpoolHeader, Writer};

    /// A SpanLog spool holding `records` empty batches under a header with these fields.
    fn spool(signal: u8, name: &str, format_version: u32, records: usize) -> Vec<u8> {
        let mut w = Writer::new(SpoolHeader {
            signal,
            signal_name: name.into(),
            created_unix_nano: 1,
            format_version,
            otlp_proto_version: None,
            producer: Some("test".into()),
        });
        for i in 0..records {
            w.append_batch(&Batch { received_unix_nano: i as u64, span_count: 0, resource_spans: &[] });
        }
        w.buffer
    }

    fn refused(b: &[u8]) -> String {
        match crate::Producer::default().spool(b, |_| {}) {
            Err(e) => e.to_string(),
            Ok(s) => panic!("accepted: {s:?}"),
        }
    }

    #[test]
    fn a_good_spool_converts_every_record() {
        let mut n = 0;
        let s = crate::Producer::default().spool(&spool(0, "OtlpTraces", 1, 3), |_| n += 1).unwrap();
        assert_eq!((s.signal, s.records, n, s.producer.as_deref()), (Signal::Traces, 3, 3, Some("test")));
    }

    #[test]
    fn a_spool_the_bridge_cannot_convert_is_refused() {
        assert!(refused(&spool(3, "OtlpProfiles", 1, 1)).contains("profiles"));
        assert!(refused(&spool(9, "OtlpTraces", 1, 1)).contains("unknown signal"));
        assert!(refused(&spool(0, "OtlpLogs", 1, 1)).contains("disagree"));
        assert!(refused(&spool(0, "OtlpTraces", FORMAT_VERSION + 1, 1)).contains("newer format"));
        let _ = refused(&[2]); // a DataSink with no header: an error, not a panic
    }

    /// A crash mid-append leaves a torn last record: the intact prefix converts.
    #[test]
    fn a_torn_tail_converts_the_intact_prefix() {
        let b = spool(0, "OtlpTraces", 1, 3);
        let s = crate::Producer::default().spool(&b[..b.len() - 1], |_| {}).unwrap();
        assert_eq!(s.records, 2);
    }

    /// Every truncation: never a panic, and never more records than the uncut prefix holds.
    #[test]
    fn every_truncation_is_safe_and_monotone() {
        let b = spool(0, "OtlpTraces", 1, 3);
        let mut last = usize::MAX;
        for cut in (0..=b.len()).rev() {
            let n = match crate::Producer::default().spool(&b[..cut], |_| {}) {
                Ok(s) => s.records,
                Err(_) => 0, // cut inside the framing or header
            };
            assert!(n <= last, "cut {cut}: {n} records after {last}");
            last = n;
        }
        assert_eq!(last, 0);
    }
}
