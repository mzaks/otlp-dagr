//! Attribute tables (`*_ATTRS`) and the walkers that fill them from Dagr's `KeyValue`s.
//!
//! One row per attribute: `parent_id` (u16 or u32), `key`, `type` (pcommon.ValueType) and
//! the one active value column; a kvlist or array value goes to `ser` as CBOR.
//!
//! `parent_id` is QUASI-DELTA encoded, and it is the one encoding both decoders agree on
//! only if the producer is careful: a row is a delta iff its (type, key, value) equal the
//! previous row's and the type is not Empty/Map/Slice; otherwise it is absolute. Rows are
//! appended in walk order — parents ascending, a parent's attributes in their OTLP order —
//! so a run of equal (key, value) rows always has ascending parents and every delta is
//! non-negative, and attribute order survives the Go decoder (which rebuilds each map in
//! row order). The active value column is never null: Go reads a null string as "" (equal),
//! Rust as null (not equal), so a null would decode differently in the two.

use std::sync::Arc;

use arrow::array::{
    ArrayRef, BinaryBuilder, BooleanBuilder, Float64Builder, Int64Builder, StringBuilder, UInt16Array,
    UInt32Array, UInt8Builder,
};
use arrow::error::ArrowError;
use arrow::record_batch::RecordBatch;

use crate::cols::Cols;

pub const EMPTY: u8 = 0;
pub const STR: u8 = 1;
pub const INT: u8 = 2;
pub const DOUBLE: u8 = 3;
pub const BOOL: u8 = 4;
pub const MAP: u8 = 5;
pub const SLICE: u8 = 6;
pub const BYTES: u8 = 7;

/// One attribute value, borrowed from the Dagr buffer (or, for Map/Slice, from the CBOR
/// scratch the walker encoded it into).
pub enum Val<'a> {
    Empty,
    Str(&'a str),
    Int(i64),
    Double(f64),
    Bool(bool),
    Bytes(&'a [u8]),
    Map(&'a [u8]),
    Slice(&'a [u8]),
}

impl Val<'_> {
    fn ty(&self) -> u8 {
        match self {
            Val::Empty => EMPTY,
            Val::Str(_) => STR,
            Val::Int(_) => INT,
            Val::Double(_) => DOUBLE,
            Val::Bool(_) => BOOL,
            Val::Bytes(_) => BYTES,
            Val::Map(_) => MAP,
            Val::Slice(_) => SLICE,
        }
    }
}

/// The AnyValue columns: `type` plus one active value column per row (`str`, `int`,
/// `double`, `bool`, `bytes`, or `ser` for a kvlist / array as CBOR). Shared by the
/// attribute tables and the LOGS `body` struct.
pub struct ValueCols {
    ty: UInt8Builder,
    s: StringBuilder,
    i: Int64Builder,
    d: Float64Builder,
    b: BooleanBuilder,
    by: BinaryBuilder,
    ser: BinaryBuilder,
}

impl Default for ValueCols {
    fn default() -> Self {
        ValueCols {
            ty: UInt8Builder::new(),
            s: StringBuilder::new(),
            i: Int64Builder::new(),
            d: Float64Builder::new(),
            b: BooleanBuilder::new(),
            by: BinaryBuilder::new(),
            ser: BinaryBuilder::new(),
        }
    }
}

impl ValueCols {
    pub fn push(&mut self, v: &Val<'_>) {
        self.ty.append_value(v.ty());
        self.s.append_option(if let Val::Str(x) = v { Some(*x) } else { None });
        self.i.append_option(if let Val::Int(x) = v { Some(*x) } else { None });
        self.d.append_option(if let Val::Double(x) = v { Some(*x) } else { None });
        self.b.append_option(if let Val::Bool(x) = v { Some(*x) } else { None });
        self.by.append_option(if let Val::Bytes(x) = v { Some(*x) } else { None });
        self.ser.append_option(match v {
            Val::Map(x) | Val::Slice(x) => Some(*x),
            _ => None,
        });
    }

    /// `type` (required) and every value column that holds anything.
    pub fn finish(&mut self, c: &mut Cols) {
        c.req("type", Arc::new(self.ty.finish()));
        c.opt("str", Arc::new(self.s.finish()));
        c.opt("int", Arc::new(self.i.finish()));
        c.opt("double", Arc::new(self.d.finish()));
        c.opt("bool", Arc::new(self.b.finish()));
        c.opt("bytes", Arc::new(self.by.finish()));
        c.opt("ser", Arc::new(self.ser.finish()));
    }
}

pub struct Attrs {
    wide: bool, // u32 parent_id (event / link / data-point attrs) vs u16
    parent: Vec<u32>,
    key: StringBuilder,
    vals: ValueCols,
    // the previous row, for the quasi-delta decision (buffers reused, never reallocated
    // once warm)
    has_prev: bool,
    prev_parent: u32,
    prev_ty: u8,
    prev_key: String,
    prev_s: String,
    prev_by: Vec<u8>,
    prev_num: u64, // i64 / bool; f64 below, compared with == (Go's rule), not bitwise
    prev_f: f64,
    /// CBOR scratch for the current Map/Slice value.
    pub scratch: Vec<u8>,
}

impl Attrs {
    pub fn new(wide: bool) -> Self {
        Attrs {
            wide,
            parent: Vec::new(),
            key: StringBuilder::new(),
            vals: ValueCols::default(),
            has_prev: false,
            prev_parent: 0,
            prev_ty: 0,
            prev_key: String::new(),
            prev_s: String::new(),
            prev_by: Vec::new(),
            prev_num: 0,
            prev_f: 0.0,
            scratch: Vec::new(),
        }
    }

    pub fn len(&self) -> usize {
        self.parent.len()
    }

    pub fn is_empty(&self) -> bool {
        self.parent.is_empty()
    }

    pub fn push(&mut self, parent: u32, key: &str, v: Val<'_>) {
        let ty = v.ty();
        let same = self.has_prev
            && ty == self.prev_ty
            && !matches!(ty, EMPTY | MAP | SLICE)
            && key == self.prev_key
            && match &v {
                Val::Str(s) => *s == self.prev_s,
                Val::Int(x) => *x as u64 == self.prev_num,
                Val::Bool(x) => *x as u64 == self.prev_num,
                Val::Double(x) => *x == self.prev_f,
                Val::Bytes(x) => *x == &self.prev_by[..],
                _ => false,
            };
        self.parent.push(if same { parent - self.prev_parent } else { parent });
        self.has_prev = true;
        self.prev_parent = parent;
        self.prev_ty = ty;
        self.prev_key.clear();
        self.prev_key.push_str(key);
        match &v {
            Val::Str(x) => {
                self.prev_s.clear();
                self.prev_s.push_str(x);
            }
            Val::Int(x) => self.prev_num = *x as u64,
            Val::Bool(x) => self.prev_num = *x as u64,
            Val::Double(x) => self.prev_f = *x,
            Val::Bytes(x) => {
                self.prev_by.clear();
                self.prev_by.extend_from_slice(x);
            }
            _ => {}
        }
        self.key.append_value(key);
        self.vals.push(&v);
    }

    pub fn finish(&mut self) -> Result<RecordBatch, ArrowError> {
        let mut c = Cols::default();
        let parent: ArrayRef = if self.wide {
            Arc::new(UInt32Array::from(std::mem::take(&mut self.parent)))
        } else {
            Arc::new(UInt16Array::from(self.parent.drain(..).map(|p| p as u16).collect::<Vec<_>>()))
        };
        c.req("parent_id", parent);
        c.req("key", Arc::new(self.key.finish()));
        self.vals.finish(&mut c);
        self.has_prev = false;
        c.batch()
    }
}

/// Instantiates the attribute walkers for one generated signal module: every signal carries
/// its own flattened copy of OtlpCommon's `KeyValue` / `AnyValue`, so the same code is
/// compiled against each set of types.
#[macro_export]
macro_rules! kv_walkers {
    ($lazy:path) => {
        use $lazy as lz;
        use $crate::attrs::{Attrs, Val};
        use $crate::cbor;

        type Dagr = otlp_dagr::dagr_runtime::DagrError;

        /// Appends every attribute of `kvs` to `a` under `parent`.
        pub fn push_kvs(a: &mut Attrs, parent: u32, kvs: lz::KeyValuePackedNodeArray<'_>) -> Result<(), Dagr> {
            for kv in kvs.iter() {
                let kv = kv?;
                let key = kv.key().unwrap_or("");
                match kv.value() {
                    None => a.push(parent, key, Val::Empty),
                    Some(v) => push_value(a, parent, key, v)?,
                }
            }
            Ok(())
        }

        pub fn push_value(a: &mut Attrs, parent: u32, key: &str, v: lz::AnyValuePackedArrayAccessor<'_>) -> Result<(), Dagr> {
            let mut buf = std::mem::take(&mut a.scratch);
            let r = value(&mut buf, v).map(|val| a.push(parent, key, val));
            a.scratch = buf;
            r
        }

        /// One AnyValue as a `Val`: scalars borrow the Dagr buffer, a kvlist / array is
        /// encoded as CBOR into `scratch` and borrows that.
        pub fn value<'b, 'a: 'b>(scratch: &'b mut Vec<u8>, v: lz::AnyValuePackedArrayAccessor<'a>) -> Result<Val<'b>, Dagr> {
            use lz::AnyValuePackedArrayAccessor as V;
            Ok(match v {
                V::StringValue(s) => Val::Str(s.unwrap_or("")),
                V::BoolValue(b) => Val::Bool(b),
                V::IntValue(i) => Val::Int(i),
                V::DoubleValue(d) => Val::Double(d),
                V::BytesValue(b) => Val::Bytes(b),
                V::ArrayValue(arr) => {
                    scratch.clear();
                    cbor_array(scratch, arr)?;
                    Val::Slice(scratch)
                }
                V::KvlistValue(kvs) => {
                    scratch.clear();
                    cbor_kvlist(scratch, kvs)?;
                    Val::Map(scratch)
                }
                // Profiles' dictionary indirection; no OTAP signal carries it.
                V::StringValueStrindex(_) | V::Unknown(_) => Val::Empty,
            })
        }

        pub fn cbor_value(out: &mut Vec<u8>, v: Option<lz::AnyValuePackedArrayAccessor<'_>>) -> Result<(), Dagr> {
            use lz::AnyValuePackedArrayAccessor as V;
            match v {
                None | Some(V::StringValueStrindex(_)) | Some(V::Unknown(_)) => cbor::null(out),
                Some(V::StringValue(s)) => cbor::text(out, s.unwrap_or("")),
                Some(V::BoolValue(b)) => cbor::boolean(out, b),
                Some(V::IntValue(i)) => cbor::int(out, i),
                Some(V::DoubleValue(d)) => cbor::double(out, d),
                Some(V::BytesValue(b)) => cbor::bytes(out, b),
                Some(V::ArrayValue(arr)) => cbor_array(out, arr)?,
                Some(V::KvlistValue(kvs)) => cbor_kvlist(out, kvs)?,
            }
            Ok(())
        }

        pub fn cbor_array(out: &mut Vec<u8>, arr: lz::AnyValuePackedOptArray<'_>) -> Result<(), Dagr> {
            cbor::head(out, 4, arr.len() as u64);
            for el in arr.iter() {
                cbor_value(out, el?)?;
            }
            Ok(())
        }

        pub fn cbor_kvlist(out: &mut Vec<u8>, kvs: lz::KeyValuePackedNodeArray<'_>) -> Result<(), Dagr> {
            cbor::head(out, 5, kvs.len() as u64);
            for kv in kvs.iter() {
                let kv = kv?;
                cbor::text(out, kv.key().unwrap_or(""));
                cbor_value(out, kv.value())?;
            }
            Ok(())
        }
    };
}
