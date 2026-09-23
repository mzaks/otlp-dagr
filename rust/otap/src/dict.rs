//! Dictionary-or-plain columns whose dictionaries live as long as the OTAP stream.
//!
//! OTAP lets most string, id and small-integer columns be dictionary-encoded, and
//! repetition is where a telemetry batch's bytes are: every span attribute repeats its key,
//! scope names repeat per span — and the NEXT batch repeats them again. So each column
//! interns its values for the life of the producer's stream, not just one batch:
//!
//! * a batch that brings no new values reuses the very same values array, and the IPC
//!   writer then sends no dictionary at all (arrow-rs skips a dictionary whose values are
//!   pointer-equal to the last one written on that stream) — the receiver still has it;
//! * a batch with new values re-sends the dictionary (arrow-rs 53 cannot write DELTA
//!   dictionaries, so it goes out whole, as a replacement);
//! * a dictionary that has grown stale — mostly values this batch did not use — is
//!   compacted to the values this batch did use, so a high-churn column (trace ids,
//!   free-text values) does not grow without bound.
//!
//! At `finish` each column picks plain, u8 keys or u16 keys by the bytes this batch would
//! put on the wire (a dictionary the receiver already holds costs nothing), within the key
//! widths OTAP allows for that column: the Rust decoder rejects any other width (`str` /
//! `int` / `bytes` / `ser` take u16 keys only).
//!
//! Dictionary values are unique by construction. Both decoders need that: the Rust one
//! decides QUASI-DELTA equality by comparing dictionary KEYS, the Go one by comparing
//! values, so a duplicated value would make them decode differently.

use std::collections::HashMap;
use std::hash::{BuildHasherDefault, Hasher};
use std::sync::Arc;

use arrow::array::{
    Array, ArrayData, ArrayRef, ArrowPrimitiveType, BinaryArray, DictionaryArray, FixedSizeBinaryArray, StringArray,
    UInt8Array, UInt16Array, UInt32Array, make_array, new_null_array,
};
use arrow::buffer::{Buffer, NullBuffer, OffsetBuffer, ScalarBuffer};
use arrow::compute::take;
use arrow::datatypes::{DataType, UInt8Type, UInt16Type};

/// The dictionary key widths a column may use.
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Keys {
    /// u8 or u16 (`Dict(u8+)` in the Rust validator)
    U8,
    /// u16 only (`Dict(u16)`)
    U16,
}

/// Bytes a finished array occupies in an IPC body (buffer lengths, children included; the
/// 8-byte padding is ignored — it is the same for every choice).
pub fn ipc_bytes(a: &dyn Array) -> usize {
    fn data(d: &ArrayData) -> usize {
        d.buffers().iter().map(|b| b.len()).sum::<usize>()
            + d.nulls().map_or(0, |n| n.buffer().len())
            + d.child_data().iter().map(data).sum::<usize>()
    }
    data(&a.to_data())
}

/// FxHash's word step: the dictionaries hash short keys and ids, where SipHash (std's
/// default) showed up in profiles; collision resistance is not needed here.
#[derive(Default, Clone, Copy)]
struct Fx(u64);

impl Hasher for Fx {
    fn write(&mut self, bytes: &[u8]) {
        let mut chunks = bytes.chunks_exact(8);
        for c in &mut chunks {
            self.add(u64::from_le_bytes(c.try_into().unwrap()));
        }
        let rest = chunks.remainder();
        if !rest.is_empty() {
            let mut w = [0u8; 8];
            w[..rest.len()].copy_from_slice(rest);
            self.add(u64::from_le_bytes(w) ^ ((rest.len() as u64) << 56));
        }
    }
    fn write_usize(&mut self, n: usize) {
        self.add(n as u64);
    }
    fn finish(&self) -> u64 {
        self.0
    }
}

impl Fx {
    #[inline]
    fn add(&mut self, w: u64) {
        self.0 = (self.0.rotate_left(5) ^ w).wrapping_mul(0x517c_c1b7_2722_0a95);
    }
}

/// How a column's values are laid out.
#[derive(Clone)]
enum Kind {
    Utf8,
    Binary,
    /// FixedSizeBinary(width)
    Fixed(i32),
    /// a primitive type whose native value is `usize` bytes wide
    Prim(DataType, usize),
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Enc {
    None,
    Plain,
    U8,
    U16,
}

/// What changing a column's encoding costs beyond its own bytes: the payload's schema
/// changes, so its stream restarts — schema message and every dictionary re-sent. A rough
/// figure; it only has to stop a column flapping between encodings batch to batch.
const SWITCH: usize = 512;

/// How often a column in plain mode tries the dictionary again.
const PROBE: u32 = 8;

/// One dictionary column: the stream-long interner plus this batch's rows.
pub struct Col {
    kind: Kind,
    keys: Keys,
    map: HashMap<Box<[u8]>, u32, BuildHasherDefault<Fx>>,
    bytes: Vec<u8>,
    starts: Vec<usize>, // value i is bytes[starts[i]..starts[i + 1]]
    seen: Vec<u32>,     // the batch (epoch) each value was last used in
    epoch: u32,
    added: bool,
    values: Option<ArrayRef>, // the values array last handed out, reused while unchanged
    last: Enc,                // what this column was written as last batch
    // Plain mode: the column went out plain last batch and its values were mostly
    // distinct (log bodies, ids), so this batch skips the interner — no hash, no copy into
    // the dictionary — and writes plain directly. Every PROBE-th batch interns again to
    // re-check.
    plain_mode: bool,
    batches: u32,
    pbytes: Vec<u8>,
    pstarts: Vec<usize>,
    held: bool,               // `values` went out as a dictionary last batch (receiver has it)
    rows: Vec<u32>,
    valid: Vec<bool>,
    nulls: usize,
    plain: usize, // bytes this batch's column would take unpacked
}

impl Col {
    fn new(kind: Kind, keys: Keys) -> Self {
        Col {
            kind,
            keys,
            map: HashMap::default(),
            bytes: Vec::new(),
            starts: vec![0],
            seen: Vec::new(),
            epoch: 1,
            added: false,
            values: None,
            last: Enc::None,
            held: false,
            plain_mode: false,
            batches: 0,
            pbytes: Vec::new(),
            pstarts: vec![0],
            rows: Vec::new(),
            valid: Vec::new(),
            nulls: 0,
            plain: 0,
        }
    }

    fn value_type(&self) -> DataType {
        match &self.kind {
            Kind::Utf8 => DataType::Utf8,
            Kind::Binary => DataType::Binary,
            Kind::Fixed(w) => DataType::FixedSizeBinary(*w),
            Kind::Prim(t, _) => t.clone(),
        }
    }

    #[inline]
    fn push(&mut self, v: &[u8]) {
        self.plain += match self.kind {
            Kind::Utf8 | Kind::Binary => v.len() + 4,
            _ => v.len(),
        };
        if self.plain_mode {
            self.pbytes.extend_from_slice(v);
            self.pstarts.push(self.pbytes.len());
            self.valid.push(true);
            self.rows.push(0);
            return;
        }
        let id = match self.map.get(v) {
            Some(id) => *id,
            None => {
                let id = self.seen.len() as u32;
                self.map.insert(v.into(), id);
                self.bytes.extend_from_slice(v);
                self.starts.push(self.bytes.len());
                self.seen.push(0);
                self.added = true;
                id
            }
        };
        self.seen[id as usize] = self.epoch;
        self.rows.push(id);
        self.valid.push(true);
    }

    #[inline]
    fn push_null(&mut self) {
        self.plain += match self.kind {
            Kind::Utf8 | Kind::Binary => 4,
            Kind::Fixed(w) => w as usize,
            Kind::Prim(_, w) => w,
        };
        self.rows.push(0);
        self.valid.push(false);
        self.nulls += 1;
        if self.plain_mode {
            // fixed-width columns keep a slot per row, even a null one
            let w = match self.kind {
                Kind::Fixed(w) => w as usize,
                Kind::Prim(_, w) => w,
                _ => 0,
            };
            self.pbytes.resize(self.pbytes.len() + w, 0);
            self.pstarts.push(self.pbytes.len());
        }
    }

    /// The plain column written directly in plain mode.
    fn plain_array(&mut self, n: usize) -> ArrayRef {
        let nulls = if self.nulls == 0 { None } else { Some(NullBuffer::from(self.valid.clone())) };
        let data = Buffer::from(self.pbytes.as_slice());
        let out: ArrayRef = match &self.kind {
            Kind::Utf8 | Kind::Binary => {
                let offsets =
                    OffsetBuffer::new(ScalarBuffer::from(self.pstarts.iter().map(|s| *s as i32).collect::<Vec<_>>()));
                if matches!(self.kind, Kind::Utf8) {
                    Arc::new(StringArray::new(offsets, data, nulls))
                } else {
                    Arc::new(BinaryArray::new(offsets, data, nulls))
                }
            }
            Kind::Fixed(w) => Arc::new(FixedSizeBinaryArray::new(*w, data, nulls)),
            Kind::Prim(t, _) => make_array(
                ArrayData::builder(t.clone()).len(n).add_buffer(data).nulls(nulls).build().expect("primitive column"),
            ),
        };
        self.pbytes.clear();
        self.pstarts.truncate(1);
        out
    }

    fn build_values(&self) -> ArrayRef {
        let n = self.seen.len();
        match &self.kind {
            Kind::Utf8 | Kind::Binary => {
                let offsets =
                    OffsetBuffer::new(ScalarBuffer::from(self.starts.iter().map(|s| *s as i32).collect::<Vec<_>>()));
                let data = Buffer::from(self.bytes.as_slice());
                if matches!(self.kind, Kind::Utf8) {
                    // every value was appended from a &str, so the bytes are valid UTF-8
                    Arc::new(StringArray::new(offsets, data, None))
                } else {
                    Arc::new(BinaryArray::new(offsets, data, None))
                }
            }
            Kind::Fixed(w) => Arc::new(FixedSizeBinaryArray::new(*w, Buffer::from(self.bytes.as_slice()), None)),
            Kind::Prim(t, _) => make_array(
                ArrayData::builder(t.clone())
                    .len(n)
                    .add_buffer(Buffer::from(self.bytes.as_slice()))
                    .build()
                    .expect("primitive values"),
            ),
        }
    }

    /// Keeps only the values this batch used, renumbering the rows: a stale dictionary
    /// would be re-sent whole every time it changes.
    fn compact(&mut self) {
        let mut remap = vec![u32::MAX; self.seen.len()];
        let (mut bytes, mut starts, mut seen) = (Vec::new(), vec![0], Vec::new());
        self.map.clear();
        for r in 0..self.rows.len() {
            if !self.valid[r] {
                continue;
            }
            let old = self.rows[r] as usize;
            if remap[old] == u32::MAX {
                remap[old] = seen.len() as u32;
                let v = &self.bytes[self.starts[old]..self.starts[old + 1]];
                self.map.insert(v.into(), remap[old]);
                bytes.extend_from_slice(v);
                starts.push(bytes.len());
                seen.push(self.epoch);
            }
            self.rows[r] = remap[old];
        }
        self.bytes = bytes;
        self.starts = starts;
        self.seen = seen;
        self.added = true;
        self.values = None;
    }

    pub fn finish(&mut self) -> ArrayRef {
        let n = self.rows.len();
        let out = if self.plain_mode {
            self.last = Enc::Plain;
            self.held = false;
            if self.nulls == n { new_null_array(&self.value_type(), n) } else { self.plain_array(n) }
        } else {
            let distinct = self.seen.iter().filter(|e| **e == self.epoch).count();
            let out = self.encode(n);
            // mostly distinct and written plain: skip the interner next time
            self.plain_mode = self.last == Enc::Plain && n > 0 && distinct * 2 > n;
            if self.plain_mode {
                // the dictionary is not kept up to date in plain mode; start it fresh on probe
                self.map.clear();
                self.bytes.clear();
                self.starts.truncate(1);
                self.seen.clear();
                self.values = None;
            }
            out
        };
        self.batches += 1;
        if self.plain_mode && self.batches % PROBE == 0 {
            self.plain_mode = false;
        }
        self.rows.clear();
        self.valid.clear();
        self.nulls = 0;
        self.plain = 0;
        self.added = false;
        self.epoch += 1;
        out
    }

    fn encode(&mut self, n: usize) -> ArrayRef {
        if self.nulls == n {
            return new_null_array(&self.value_type(), n);
        }
        let used = self.seen.iter().filter(|e| **e == self.epoch).count();
        if self.seen.len() > 2 * used + 64 || self.seen.len() > 65_536 {
            self.compact();
        }
        let total = self.seen.len();
        let fresh = self.added || self.values.is_none();
        if fresh {
            self.values = Some(self.build_values());
        }
        let values = self.values.clone().unwrap();
        let nulls = if self.nulls == 0 { None } else { Some(NullBuffer::from(self.valid.clone())) };
        // key width only widens within a stream: narrowing would change the schema
        let dict = if total > 65_536 {
            Enc::Plain
        } else if self.keys == Keys::U8 && total <= 256 && self.last != Enc::U16 {
            Enc::U8
        } else {
            Enc::U16
        };
        let width = if dict == Enc::U8 { 1 } else { 2 };
        // The dictionary is free only if the receiver already holds exactly these values:
        // sent as a dictionary last batch, and unchanged since.
        let dict_bytes = if self.held && !fresh { 0 } else { ipc_bytes(values.as_ref()) };
        let switch = |e: Enc| if self.last == Enc::None || self.last == e { 0 } else { SWITCH };
        let dict_cost = width * n + dict_bytes + switch(dict);
        let plain_cost = self.plain + switch(Enc::Plain);
        let choice = if dict == Enc::Plain || plain_cost <= dict_cost { Enc::Plain } else { dict };
        self.last = choice;
        self.held = choice != Enc::Plain;
        if choice == Enc::Plain {
            let idx = UInt32Array::new(self.rows.clone().into(), nulls.clone());
            let taken = take(values.as_ref(), &idx, None).expect("dictionary to plain");
            // arrow-rs 53's `take` drops the INDEX nulls for FixedSizeBinary (the stream
            // gate caught null trace ids decoding as real ones): apply them explicitly.
            let Some(nulls) = nulls else { return taken };
            let data = taken.to_data();
            let merged = NullBuffer::union(data.nulls(), Some(&nulls));
            return make_array(data.into_builder().nulls(merged).build().expect("plain column"));
        }
        if choice == Enc::U8 {
            let k = UInt8Array::new(self.rows.iter().map(|x| *x as u8).collect(), nulls);
            Arc::new(DictionaryArray::<UInt8Type>::try_new(k, values).expect("u8 dictionary"))
        } else {
            let k = UInt16Array::new(self.rows.iter().map(|x| *x as u16).collect(), nulls);
            Arc::new(DictionaryArray::<UInt16Type>::try_new(k, values).expect("u16 dictionary"))
        }
    }
}

pub struct Str(Col);

impl Str {
    pub fn new(keys: Keys) -> Self {
        Str(Col::new(Kind::Utf8, keys))
    }
    pub fn append_value(&mut self, v: &str) {
        self.0.push(v.as_bytes());
    }
    pub fn append_option(&mut self, v: Option<&str>) {
        match v {
            Some(v) => self.0.push(v.as_bytes()),
            None => self.0.push_null(),
        }
    }
    pub fn append_null(&mut self) {
        self.0.push_null();
    }
    pub fn finish(&mut self) -> ArrayRef {
        self.0.finish()
    }
}

pub struct Bin(Col);

impl Bin {
    pub fn new(keys: Keys) -> Self {
        Bin(Col::new(Kind::Binary, keys))
    }
    pub fn append_option(&mut self, v: Option<&[u8]>) {
        match v {
            Some(v) => self.0.push(v),
            None => self.0.push_null(),
        }
    }
    pub fn finish(&mut self) -> ArrayRef {
        self.0.finish()
    }
}

/// FixedSizeBinary (trace / span ids).
pub struct Fsb(Col);

impl Fsb {
    pub fn new(width: i32, keys: Keys) -> Self {
        Fsb(Col::new(Kind::Fixed(width), keys))
    }
    pub fn append_value(&mut self, v: &[u8]) {
        self.0.push(v);
    }
    pub fn append_null(&mut self) {
        self.0.push_null();
    }
    pub fn finish(&mut self) -> ArrayRef {
        self.0.finish()
    }
}

pub struct Prim<T: ArrowPrimitiveType>(Col, std::marker::PhantomData<T>);

impl<T: ArrowPrimitiveType> Prim<T> {
    pub fn new(keys: Keys) -> Self {
        Prim(Col::new(Kind::Prim(T::DATA_TYPE, std::mem::size_of::<T::Native>()), keys), std::marker::PhantomData)
    }
    pub fn append_value(&mut self, v: T::Native) {
        // SAFETY: an arrow primitive native type is plain old data of exactly this size
        let b = unsafe {
            std::slice::from_raw_parts(&v as *const T::Native as *const u8, std::mem::size_of::<T::Native>())
        };
        self.0.push(b);
    }
    pub fn append_option(&mut self, v: Option<T::Native>) {
        match v {
            Some(v) => self.append_value(v),
            None => self.0.push_null(),
        }
    }
    pub fn append_null(&mut self) {
        self.0.push_null();
    }
    pub fn finish(&mut self) -> ArrayRef {
        self.0.finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // arrow-rs 53's `take` dropped index nulls for FixedSizeBinary; a plain Fsb column
    // must keep them (null trace ids decoded as real ones before).
    #[test]
    fn plain_fixed_size_keeps_nulls() {
        let mut c = Fsb::new(16, Keys::U8);
        // all distinct, no nulls: plain
        for i in 0..40u8 {
            c.append_value(&[i; 16]);
        }
        assert!(matches!(c.finish().data_type(), DataType::FixedSizeBinary(16)));
        // with nulls a dictionary would be a little smaller, but not by the cost of
        // switching encodings: stays plain — and must keep its nulls
        for i in 0..40u8 {
            if i % 2 == 0 { c.append_value(&[100 + i; 16]) } else { c.append_null() }
        }
        let a = c.finish();
        assert!(matches!(a.data_type(), DataType::FixedSizeBinary(16)));
        assert_eq!(a.null_count(), 20);
    }

    // No new values: the very same values array (so the IPC writer sends no dictionary).
    #[test]
    fn unchanged_dictionary_is_the_same_array() {
        let mut c = Str::new(Keys::U8);
        let batch = |c: &mut Str| {
            for i in 0..200 {
                c.append_value(["GET", "POST", "PUT"][i % 3]);
            }
            c.finish()
        };
        let a = batch(&mut c);
        let b = batch(&mut c);
        let va = a.as_any().downcast_ref::<DictionaryArray<UInt8Type>>().unwrap().values().to_data();
        let vb = b.as_any().downcast_ref::<DictionaryArray<UInt8Type>>().unwrap().values().to_data();
        assert!(ArrayData::ptr_eq(&va, &vb));
    }

    // A tiny batch goes out plain; repeating it must not flip to a dictionary (a flip
    // changes the schema and restarts the stream — the repeat was bigger than the cold
    // batch before encodings were sticky).
    #[test]
    fn repeated_small_batch_keeps_its_encoding() {
        let mut c = Str::new(Keys::U8);
        let t1 = { c.append_value("a-fairly-long-scope-name"); c.finish().data_type().clone() };
        let t2 = { c.append_value("a-fairly-long-scope-name"); c.finish().data_type().clone() };
        assert_eq!(t1, t2);
    }

    // A stale dictionary (mostly values this batch did not use) is compacted.
    #[test]
    fn churning_column_is_compacted() {
        let mut c = Str::new(Keys::U16);
        for b in 0..5 {
            for i in 0..300 {
                c.append_value(&format!("value-{b}-{i}"));
                c.append_value("shared");
            }
            c.finish();
        }
        assert!(c.0.seen.len() <= 2 * 301 + 64, "dictionary grew to {}", c.0.seen.len());
    }
}
