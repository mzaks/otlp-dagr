//! Column assembly with OTAP's two rules on optional columns: an all-null optional column is
//! left out (otap-spec §4.2 — and the Rust decoder rejects any column it does not know, so
//! "leave it out" is also the only safe way to say "absent"), and a required column is
//! always present with no nulls.

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, StructArray};
use arrow::buffer::NullBuffer;
use arrow::datatypes::{Field, Fields, Schema};
use arrow::error::ArrowError;
use arrow::record_batch::RecordBatch;

#[derive(Default)]
pub struct Cols {
    fields: Vec<Field>,
    arrays: Vec<ArrayRef>,
}

impl Cols {
    pub fn req(&mut self, name: &str, a: ArrayRef) {
        debug_assert_eq!(a.null_count(), 0, "required column {name} has nulls");
        self.fields.push(Field::new(name, a.data_type().clone(), false));
        self.arrays.push(a);
    }

    /// Adds `a` unless every row is null.
    pub fn opt(&mut self, name: &str, a: ArrayRef) {
        if a.null_count() < a.len() {
            self.fields.push(Field::new(name, a.data_type().clone(), true));
            self.arrays.push(a);
        }
    }

    pub fn is_empty(&self) -> bool {
        self.fields.is_empty()
    }

    /// These columns as a struct column: null where `valid` is false. None when no child
    /// survived (a struct with no fields is never written).
    pub fn into_struct(self, valid: Vec<bool>) -> Option<ArrayRef> {
        if self.fields.is_empty() || !valid.iter().any(|v| *v) {
            return None;
        }
        let nulls = if valid.iter().all(|v| *v) { None } else { Some(NullBuffer::from(valid)) };
        Some(Arc::new(StructArray::new(Fields::from(self.fields), self.arrays, nulls)))
    }

    pub fn batch(self) -> Result<RecordBatch, ArrowError> {
        RecordBatch::try_new(Arc::new(Schema::new(self.fields)), self.arrays)
    }
}

/// Encodes ids assigned in row order (non-decreasing) as OTAP DELTA: first value absolute,
/// then differences.
pub fn delta<T: Copy + std::ops::Sub<Output = T>>(ids: &mut [T]) {
    for i in (1..ids.len()).rev() {
        ids[i] = ids[i] - ids[i - 1];
    }
}
