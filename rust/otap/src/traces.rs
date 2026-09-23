//! OtlpTraces (Dagr) → OTAP traces: SPANS, SPAN_ATTRS, SPAN_EVENTS, SPAN_EVENT_ATTRS,
//! SPAN_LINKS, SPAN_LINK_ATTRS, RESOURCE_ATTRS, SCOPE_ATTRS.
//!
//! One pass over the lazy accessors in OTLP order; ids are assigned in that order, so every
//! DELTA column is non-decreasing by construction and events / links keep their order
//! within a span (otel-arrow's reference encoder sorts events by name and so reorders
//! them; this one does not have to).

use std::sync::Arc;

use arrow::array::{
    FixedSizeBinaryBuilder, TimestampNanosecondBuilder,
    UInt16Array, UInt32Array, UInt32Builder,
};

use otlp_dagr::otlp_traces_lazy as lz;

use crate::attrs::Attrs;
use crate::bar::{Bar, PayloadType};
use crate::cols::{Cols, delta};
use crate::dict::{self, Keys};
use arrow::datatypes::{DurationNanosecondType, Int32Type};
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
    duration: dict::Prim<DurationNanosecondType>,
    trace_id: dict::Fsb,
    span_id: dict::Fsb,
    trace_state: dict::Str,
    parent_span_id: FixedSizeBinaryBuilder,
    flags: UInt32Builder,
    name: dict::Str,
    kind: dict::Prim<Int32Type>,
    dropped_attrs: UInt32Builder,
    dropped_events: UInt32Builder,
    dropped_links: UInt32Builder,
    status_code: dict::Prim<Int32Type>,
    status_msg: dict::Str,
    status_valid: Vec<bool>,
    span_attrs: Attrs,
    // SPAN_EVENTS
    ev_parent: Vec<u16>,
    ev_prev: (u16, String), // previous row's (absolute parent, name), for QUASI-DELTA
    ev_time: TimestampNanosecondBuilder,
    ev_name: dict::Str,
    ev_dropped: UInt32Builder,
    ev_attrs: Attrs,
    // SPAN_LINKS
    ln_parent: Vec<u16>,
    ln_prev: (u16, [u8; 16]), // previous row's (absolute parent, trace_id)
    ln_trace_id: dict::Fsb,
    ln_span_id: dict::Fsb,
    ln_trace_state: dict::Str,
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
            duration: dict::Prim::new(Keys::U8),
            trace_id: dict::Fsb::new(16, Keys::U8),
            span_id: dict::Fsb::new(8, Keys::U8),
            trace_state: dict::Str::new(Keys::U8),
            parent_span_id: FixedSizeBinaryBuilder::new(8),
            flags: UInt32Builder::new(),
            name: dict::Str::new(Keys::U8),
            kind: dict::Prim::new(Keys::U8),
            dropped_attrs: UInt32Builder::new(),
            dropped_events: UInt32Builder::new(),
            dropped_links: UInt32Builder::new(),
            status_code: dict::Prim::new(Keys::U8),
            status_msg: dict::Str::new(Keys::U8),
            status_valid: Vec::new(),
            span_attrs: Attrs::new(false),
            ev_parent: Vec::new(),
            ev_prev: (0, String::new()),
            ev_time: ts(),
            ev_name: dict::Str::new(Keys::U8),
            ev_dropped: UInt32Builder::new(),
            ev_attrs: Attrs::new(true),
            ln_parent: Vec::new(),
            ln_prev: (0, [0; 16]),
            ln_trace_id: dict::Fsb::new(16, Keys::U8),
            ln_span_id: dict::Fsb::new(8, Keys::U8),
            ln_trace_state: dict::Str::new(Keys::U8),
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
        self.trace_id.append_value(fixed::<16>(sp.trace_id(), "trace_id")?);
        self.span_id.append_value(fixed::<8>(sp.span_id(), "span_id")?);
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
            let name = ev.name().unwrap_or("");
            // QUASI-DELTA on `name`: a delta iff the previous row had the same name. Go's
            // decoder starts from name "" and parent 0, so the first row reads the same
            // either way.
            let same = !self.ev_parent.is_empty() && name == self.ev_prev.1;
            self.ev_parent.push(if same { id - self.ev_prev.0 } else { id });
            self.ev_prev.0 = id;
            self.ev_prev.1.clear();
            self.ev_prev.1.push_str(name);
            self.ev_time.append_option(ev.time_unix_nano().map(|t| t as i64));
            self.ev_name.append_value(name);
            self.ev_dropped.append_option(nz(ev.dropped_attributes_count()));
            kv::push_kvs(&mut self.ev_attrs, eid, ev.attributes()?)?;
        }
        for ln in sp.links()?.iter() {
            let ln = ln?;
            let lid = self.ln_parent.len() as u32;
            let tid = fixed::<16>(ln.trace_id(), "link trace_id")?;
            // QUASI-DELTA on `trace_id`
            let same = !self.ln_parent.is_empty() && tid == self.ln_prev.1;
            self.ln_parent.push(if same { id - self.ln_prev.0 } else { id });
            self.ln_prev.0 = id;
            self.ln_prev.1.copy_from_slice(tid);
            self.ln_trace_id.append_value(tid);
            self.ln_span_id.append_value(fixed::<8>(ln.span_id(), "link span_id")?);
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
        c.req("duration_time_unix_nano", self.duration.finish());
        c.req("trace_id", self.trace_id.finish());
        c.req("span_id", self.span_id.finish());
        c.opt("trace_state", self.trace_state.finish());
        c.opt("parent_span_id", Arc::new(self.parent_span_id.finish()));
        c.opt("flags", Arc::new(self.flags.finish()));
        c.req("name", self.name.finish());
        c.opt("kind", self.kind.finish());
        c.opt("dropped_attributes_count", Arc::new(self.dropped_attrs.finish()));
        c.opt("dropped_events_count", Arc::new(self.dropped_events.finish()));
        c.opt("dropped_links_count", Arc::new(self.dropped_links.finish()));
        let mut st = Cols::default();
        st.opt("code", self.status_code.finish());
        st.opt("status_message", self.status_msg.finish());
        if let Some(a) = st.into_struct(std::mem::take(&mut self.status_valid)) {
            c.opt("status", a);
        }
        debug_assert!(n == 0 || !c.is_empty());
        bar.payload(PayloadType::Spans, c.batch()?)?;

        bar.payload(PayloadType::ResourceAttrs, self.env.resource_attrs.finish()?)?;
        bar.payload(PayloadType::ScopeAttrs, self.env.scope_attrs.finish()?)?;
        bar.payload(PayloadType::SpanAttrs, self.span_attrs.finish()?)?;

        // SPAN_EVENTS: id DELTA (0,1,1,…); parent_id QUASI-DELTA on `name`.
        let parent = std::mem::take(&mut self.ev_parent);
        let mut c = Cols::default();
        c.req("id", Arc::new(UInt32Array::from(delta_ids(parent.len()))));
        c.req("parent_id", Arc::new(UInt16Array::from(parent)));
        c.opt("time_unix_nano", Arc::new(self.ev_time.finish()));
        c.req("name", self.ev_name.finish());
        c.opt("dropped_attributes_count", Arc::new(self.ev_dropped.finish()));
        bar.payload(PayloadType::SpanEvents, c.batch()?)?;
        bar.payload(PayloadType::SpanEventAttrs, self.ev_attrs.finish()?)?;

        // SPAN_LINKS: id DELTA; parent_id QUASI-DELTA on `trace_id`.
        let parent = std::mem::take(&mut self.ln_parent);
        let mut c = Cols::default();
        c.req("id", Arc::new(UInt32Array::from(delta_ids(parent.len()))));
        c.req("parent_id", Arc::new(UInt16Array::from(parent)));
        c.req("trace_id", self.ln_trace_id.finish());
        c.req("span_id", self.ln_span_id.finish());
        c.opt("trace_state", self.ln_trace_state.finish());
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

