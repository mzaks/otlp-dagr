//! The `resource` and `scope` struct columns and the scope-level `schema_url` every root
//! table (SPANS, LOGS, UNIVARIATE_METRICS) carries, plus RESOURCE_ATTRS / SCOPE_ATTRS.
//!
//! OTAP has no resource or scope table: each root row names its resource and scope by id,
//! and the decoders rebuild the hierarchy from those ids. Go opens a new Resource whenever
//! the decoded `resource.id` changes from one row to the next (it does not sort), Rust sorts
//! by (resource.id, scope.id, id) — so rows must come grouped, ids non-decreasing, and scope
//! ids unique across the whole batch (SCOPE_ATTRS is keyed by scope id alone). A walk in
//! OTLP order gives exactly that. Both ids are DELTA encoded.
//!
//! A Resource or Scope with no items has no root row to hang on, so it does not survive
//! OTAP — that is the protocol's shape, not this producer's (otel-arrow's own encoder
//! drops them too).

use std::sync::Arc;

use arrow::array::{UInt16Array, UInt32Builder};

use crate::attrs::Attrs;
use crate::cols::{Cols, delta};
use crate::dict::{self, Keys};

pub struct Envelope {
    res_id: Vec<u16>,
    res_url: dict::Str,
    res_dropped: UInt32Builder,
    scope_id: Vec<u16>,
    scope_name: dict::Str,
    scope_version: dict::Str,
    scope_dropped: UInt32Builder,
    url: dict::Str,
    pub resource_attrs: Attrs,
    pub scope_attrs: Attrs,
    resources: u32,
    scopes: u32,
}

/// What a root row says about its resource and scope.
pub struct Row<'a> {
    pub res_url: Option<&'a str>,
    pub res_dropped: Option<u32>,
    pub scope_name: Option<&'a str>,
    pub scope_version: Option<&'a str>,
    pub scope_dropped: Option<u32>,
    pub scope_url: Option<&'a str>,
}

impl Default for Envelope {
    fn default() -> Self {
        Envelope {
            res_id: Vec::new(),
            res_url: dict::Str::new(Keys::U8),
            res_dropped: UInt32Builder::new(),
            scope_id: Vec::new(),
            scope_name: dict::Str::new(Keys::U8),
            scope_version: dict::Str::new(Keys::U8),
            scope_dropped: UInt32Builder::new(),
            url: dict::Str::new(Keys::U8),
            resource_attrs: Attrs::new(false),
            scope_attrs: Attrs::new(false),
            resources: 0,
            scopes: 0,
        }
    }
}

impl Envelope {
    /// Starts the next resource; returns its id (for RESOURCE_ATTRS).
    pub fn next_resource(&mut self) -> Result<u16, crate::Error> {
        self.resources += 1;
        crate::id16(self.resources - 1, "resources")
    }

    /// Starts the next scope (ids are unique across resources); returns its id.
    pub fn next_scope(&mut self) -> Result<u16, crate::Error> {
        self.scopes += 1;
        crate::id16(self.scopes - 1, "scopes")
    }

    pub fn row(&mut self, resource: u16, scope: u16, r: &Row<'_>) {
        self.res_id.push(resource);
        self.res_url.append_option(r.res_url);
        self.res_dropped.append_option(r.res_dropped.filter(|d| *d != 0));
        self.scope_id.push(scope);
        self.scope_name.append_option(r.scope_name);
        self.scope_version.append_option(r.scope_version);
        self.scope_dropped.append_option(r.scope_dropped.filter(|d| *d != 0));
        self.url.append_option(r.scope_url);
    }

    /// Adds `resource`, `scope` and `schema_url` to a root table's columns.
    pub fn finish(&mut self, c: &mut Cols) {
        let n = self.res_id.len();
        let mut res = Cols::default();
        delta(&mut self.res_id);
        res.req("id", Arc::new(UInt16Array::from(std::mem::take(&mut self.res_id))));
        res.opt("schema_url", self.res_url.finish());
        res.opt("dropped_attributes_count", Arc::new(self.res_dropped.finish()));
        if let Some(a) = res.into_struct(vec![true; n]) {
            c.opt("resource", a);
        }
        let mut sc = Cols::default();
        delta(&mut self.scope_id);
        sc.req("id", Arc::new(UInt16Array::from(std::mem::take(&mut self.scope_id))));
        sc.opt("name", self.scope_name.finish());
        sc.opt("version", self.scope_version.finish());
        sc.opt("dropped_attributes_count", Arc::new(self.scope_dropped.finish()));
        if let Some(a) = sc.into_struct(vec![true; n]) {
            c.opt("scope", a);
        }
        c.opt("schema_url", self.url.finish());
        self.resources = 0;
        self.scopes = 0;
    }
}
