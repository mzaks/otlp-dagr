//! OtlpTraces (Dagr) → OTAP traces: SPANS, SPAN_ATTRS, SPAN_EVENTS, SPAN_EVENT_ATTRS,
//! SPAN_LINKS, SPAN_LINK_ATTRS, RESOURCE_ATTRS, SCOPE_ATTRS.
//!
//! One pass over the lazy accessors in OTLP order; ids are assigned in that order, so every
//! DELTA column is non-decreasing by construction and events / links keep their order
//! within a span (otel-arrow's reference encoder sorts events by name and so reorders
//! them; this one does not have to).

use std::sync::Arc;

use arrow::array::{
    DurationNanosecondBuilder, FixedSizeBinaryBuilder, Int32Builder, StringBuilder, TimestampNanosecondBuilder,
    UInt16Array, UInt32Array, UInt32Builder,
};

use otlp_dagr::otlp_traces_lazy as lz;

use crate::attrs::Attrs;
use crate::bar::{Bar, PayloadType};
use crate::cols::{Cols, delta};
use crate::envelope::{Envelope, Row};
use crate::{Error, fixed, id16};

mod kv {
    crate::kv_walkers!(otlp_dagr::otlp_traces_lazy);
}

/// Reusable column builders for one traces batch (keep one per producer thread: after the
/// first batch the buffers are warm).
pub struct Traces {
    env: Envelope,
    // SPANS
    ids: Vec<u16>,
    start: TimestampNanosecondBuilder,
    duration: DurationNanosecondBuilder,
    trace_id: FixedSizeBinaryBuilder,
    span_id: FixedSizeBinaryBuilder,
    trace_state: StringBuilder,
    parent_span_id: FixedSizeBinaryBuilder,
    flags: UInt32Builder,
    name: StringBuilder,
    kind: Int32Builder,
    dropped_attrs: UInt32Builder,
    dropped_events: UInt32Builder,
    dropped_links: UInt32Builder,
    status_code: Int32Builder,
    status_msg: StringBuilder,
    status_valid: Vec<bool>,
    span_attrs: Attrs,
    // SPAN_EVENTS
    ev_parent: Vec<u16>,
    ev_time: TimestampNanosecondBuilder,
    ev_name: StringBuilder,
    ev_dropped: UInt32Builder,
    ev_attrs: Attrs,
    // SPAN_LINKS
    ln_parent: Vec<u16>,
    ln_trace_id: FixedSizeBinaryBuilder,
    ln_span_id: FixedSizeBinaryBuilder,
    ln_trace_state: StringBuilder,
    ln_flags: UInt32Builder,
    ln_dropped: UInt32Builder,
    ln_attrs: Attrs,
}

impl Default for Traces {
    fn default() -> Self {
        Traces {
            env: Envelope::default(),
            ids: Vec::new(),
            start: ts(),
            duration: DurationNanosecondBuilder::new(),
            trace_id: FixedSizeBinaryBuilder::new(16),
            span_id: FixedSizeBinaryBuilder::new(8),
            trace_state: StringBuilder::new(),
            parent_span_id: FixedSizeBinaryBuilder::new(8),
            flags: UInt32Builder::new(),
            name: StringBuilder::new(),
            kind: Int32Builder::new(),
            dropped_attrs: UInt32Builder::new(),
            dropped_events: UInt32Builder::new(),
            dropped_links: UInt32Builder::new(),
            status_code: Int32Builder::new(),
            status_msg: StringBuilder::new(),
            status_valid: Vec::new(),
            span_attrs: Attrs::new(false),
            ev_parent: Vec::new(),
            ev_time: ts(),
            ev_name: StringBuilder::new(),
            ev_dropped: UInt32Builder::new(),
            ev_attrs: Attrs::new(true),
            ln_parent: Vec::new(),
            ln_trace_id: FixedSizeBinaryBuilder::new(16),
            ln_span_id: FixedSizeBinaryBuilder::new(8),
            ln_trace_state: StringBuilder::new(),
            ln_flags: UInt32Builder::new(),
            ln_dropped: UInt32Builder::new(),
            ln_attrs: Attrs::new(true),
        }
    }
}

pub(crate) fn ts() -> TimestampNanosecondBuilder {
    // arrow-go's Timestamp_ns carries "UTC", which is what the Go producer emits; both
    // decoders accept any zone.
    TimestampNanosecondBuilder::new().with_timezone("UTC")
}

fn nz(v: Option<u32>) -> Option<u32> {
    v.filter(|d| *d != 0)
}

impl Traces {
    /// Converts one OtlpTraces buffer into the payloads of `bar`.
    pub fn convert(&mut self, buf: &[u8], bar: &mut Bar<'_>) -> Result<(), Error> {
        let root = lz::read_root(buf)?;
        for rs in root.resource_spans()?.iter() {
            let rs = rs?;
            self.resource_spans(&rs)?;
        }
        self.finish(bar)
    }

    /// Converts the `resource_spans` of a SpanLog spool record (the batch held inline).
    pub fn resource_spans(&mut self, rs: &lz::ResourceSpansAccessor<'_>) -> Result<(), Error> {
        let rid = self.env.next_resource()?;
        let res = rs.resource();
        if let Some(r) = &res {
            kv::push_kvs(&mut self.env.resource_attrs, rid as u32, r.attributes()?)?;
        }
        for ss in rs.scope_spans()?.iter() {
            let ss = ss?;
            let sid = self.env.next_scope()?;
            let scope = ss.scope();
            if let Some(s) = &scope {
                kv::push_kvs(&mut self.env.scope_attrs, sid as u32, s.attributes()?)?;
            }
            let row = Row {
                res_url: rs.schema_url(),
                res_dropped: res.as_ref().and_then(|r| r.dropped_attributes_count()),
                scope_name: scope.as_ref().and_then(|s| s.name()),
                scope_version: scope.as_ref().and_then(|s| s.version()),
                scope_dropped: scope.as_ref().and_then(|s| s.dropped_attributes_count()),
                scope_url: ss.schema_url(),
            };
            for sp in ss.spans()?.iter() {
                let sp = sp?;
                self.env.row(rid, sid, &row);
                self.span(&sp)?;
            }
        }
        Ok(())
    }

    fn span(&mut self, sp: &lz::SpanAccessor<'_>) -> Result<(), Error> {
        let id = id16(self.ids.len() as u32, "spans")?;
        self.ids.push(id);
        let start = sp.start_time_unix_nano().unwrap_or(0);
        let end = sp.end_time_unix_nano().unwrap_or(0);
        self.start.append_value(start as i64);
        self.duration.append_value((end as i64).wrapping_sub(start as i64));
        self.trace_id.append_value(fixed::<16>(sp.trace_id(), "trace_id")?)?;
        self.span_id.append_value(fixed::<8>(sp.span_id(), "span_id")?)?;
        self.trace_state.append_option(sp.trace_state().filter(|s| !s.is_empty()));
        match sp.parent_span_id().filter(|p| !p.is_empty()) {
            Some(p) => self.parent_span_id.append_value(fixed::<8>(Some(p), "parent_span_id")?)?,
            None => self.parent_span_id.append_null(),
        }
        self.flags.append_option(nz(sp.flags()));
        self.name.append_value(sp.name().unwrap_or(""));
        self.kind.append_option(sp.kind().map(|k| k.raw_value() as i32).filter(|k| *k != 0));
        self.dropped_attrs.append_option(nz(sp.dropped_attributes_count()));
        self.dropped_events.append_option(nz(sp.dropped_events_count()));
        self.dropped_links.append_option(nz(sp.dropped_links_count()));
        match sp.status() {
            Some(st) => {
                self.status_code.append_value(st.code().map(|c| c.raw_value() as i32).unwrap_or(0));
                self.status_msg.append_option(st.message().filter(|m| !m.is_empty()));
                self.status_valid.push(true);
            }
            None => {
                self.status_code.append_value(0);
                self.status_msg.append_null();
                self.status_valid.push(false);
            }
        }
        kv::push_kvs(&mut self.span_attrs, id as u32, sp.attributes()?)?;

        for ev in sp.events()?.iter() {
            let ev = ev?;
            let eid = self.ev_parent.len() as u32;
            // the absolute parent; `finish` turns equal-name runs into deltas
            self.ev_parent.push(id);
            let name = ev.name().unwrap_or("");
            self.ev_time.append_option(ev.time_unix_nano().map(|t| t as i64));
            self.ev_name.append_value(name);
            self.ev_dropped.append_option(nz(ev.dropped_attributes_count()));
            kv::push_kvs(&mut self.ev_attrs, eid, ev.attributes()?)?;
        }
        for ln in sp.links()?.iter() {
            let ln = ln?;
            let lid = self.ln_parent.len() as u32;
            self.ln_parent.push(id);
            let tid = fixed::<16>(ln.trace_id(), "link trace_id")?;
            self.ln_trace_id.append_value(tid)?;
            self.ln_span_id.append_value(fixed::<8>(ln.span_id(), "link span_id")?)?;
            self.ln_trace_state.append_option(ln.trace_state().filter(|s| !s.is_empty()));
            self.ln_flags.append_option(nz(ln.flags()));
            self.ln_dropped.append_option(nz(ln.dropped_attributes_count()));
            kv::push_kvs(&mut self.ln_attrs, lid, ln.attributes()?)?;
        }
        Ok(())
    }

    fn finish(&mut self, bar: &mut Bar<'_>) -> Result<(), Error> {
        // SPANS
        let n = self.ids.len();
        let mut c = Cols::default();
        let mut ids = std::mem::take(&mut self.ids);
        delta(&mut ids);
        c.req("id", Arc::new(UInt16Array::from(ids)));
        self.env.finish(&mut c);
        c.req("start_time_unix_nano", Arc::new(self.start.finish()));
        c.req("duration_time_unix_nano", Arc::new(self.duration.finish()));
        c.req("trace_id", Arc::new(self.trace_id.finish()));
        c.req("span_id", Arc::new(self.span_id.finish()));
        c.opt("trace_state", Arc::new(self.trace_state.finish()));
        c.opt("parent_span_id", Arc::new(self.parent_span_id.finish()));
        c.opt("flags", Arc::new(self.flags.finish()));
        c.req("name", Arc::new(self.name.finish()));
        c.opt("kind", Arc::new(self.kind.finish()));
        c.opt("dropped_attributes_count", Arc::new(self.dropped_attrs.finish()));
        c.opt("dropped_events_count", Arc::new(self.dropped_events.finish()));
        c.opt("dropped_links_count", Arc::new(self.dropped_links.finish()));
        let mut st = Cols::default();
        st.opt("code", Arc::new(self.status_code.finish()));
        st.opt("status_message", Arc::new(self.status_msg.finish()));
        if let Some(a) = st.into_struct(std::mem::take(&mut self.status_valid)) {
            c.opt("status", a);
        }
        debug_assert!(n == 0 || !c.is_empty());
        bar.payload(PayloadType::Spans, c.batch()?)?;

        bar.payload(PayloadType::ResourceAttrs, self.env.resource_attrs.finish()?)?;
        bar.payload(PayloadType::ScopeAttrs, self.env.scope_attrs.finish()?)?;
        bar.payload(PayloadType::SpanAttrs, self.span_attrs.finish()?)?;

        // SPAN_EVENTS: id DELTA (0,1,1,…); parent_id QUASI-DELTA on `name`.
        let names = self.ev_name.finish();
        let mut parent = std::mem::take(&mut self.ev_parent);
        quasi_delta(&mut parent, |i| names.value(i) == names.value(i - 1));
        let mut c = Cols::default();
        c.req("id", Arc::new(UInt32Array::from(delta_ids(parent.len()))));
        c.req("parent_id", Arc::new(UInt16Array::from(parent)));
        c.opt("time_unix_nano", Arc::new(self.ev_time.finish()));
        c.req("name", Arc::new(names));
        c.opt("dropped_attributes_count", Arc::new(self.ev_dropped.finish()));
        bar.payload(PayloadType::SpanEvents, c.batch()?)?;
        bar.payload(PayloadType::SpanEventAttrs, self.ev_attrs.finish()?)?;

        // SPAN_LINKS: id DELTA; parent_id QUASI-DELTA on `trace_id`.
        let tids = self.ln_trace_id.finish();
        let mut parent = std::mem::take(&mut self.ln_parent);
        quasi_delta(&mut parent, |i| tids.value(i) == tids.value(i - 1));
        let mut c = Cols::default();
        c.req("id", Arc::new(UInt32Array::from(delta_ids(parent.len()))));
        c.req("parent_id", Arc::new(UInt16Array::from(parent)));
        c.req("trace_id", Arc::new(tids));
        c.req("span_id", Arc::new(self.ln_span_id.finish()));
        c.opt("trace_state", Arc::new(self.ln_trace_state.finish()));
        c.opt("flags", Arc::new(self.ln_flags.finish()));
        c.opt("dropped_attributes_count", Arc::new(self.ln_dropped.finish()));
        bar.payload(PayloadType::SpanLinks, c.batch()?)?;
        bar.payload(PayloadType::SpanLinkAttrs, self.ln_attrs.finish()?)?;
        Ok(())
    }
}

/// `0, 1, 1, …` — ids 0..n DELTA encoded.
pub(crate) fn delta_ids(n: usize) -> Vec<u32> {
    (0..n as u32).map(|i| if i == 0 { 0 } else { 1 }).collect()
}

/// QUASI-DELTA by columns: row i is a delta from row i-1 iff `same(i)`; otherwise absolute.
/// Parents ascend in walk order, so every delta is non-negative.
pub(crate) fn quasi_delta(parent: &mut [u16], same: impl Fn(usize) -> bool) {
    for i in (1..parent.len()).rev() {
        if same(i) {
            parent[i] -= parent[i - 1];
        }
    }
}
