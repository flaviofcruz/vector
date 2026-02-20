//! This module is copied from:
//! https://github.com/databricks-eng/universe/tree/master/common/logging/redactor/filter/src/buffer.rs

/// A RedactionBuffer is the temporary state for performing a redaction.
/// Separating the mutable state from the redactor means you can share the
/// redactor itself across threads, including its state machines and whatnot.
#[derive(Clone)]
pub struct RedactionBuffer {
    buffers: (Vec<u8>, Vec<u8>),
}

impl RedactionBuffer {
    pub fn new(capacity: usize) -> Self {
        RedactionBuffer {
            buffers: (Vec::with_capacity(capacity), Vec::with_capacity(capacity)),
        }
    }

    pub(super) fn get_fresh(&mut self) -> (&mut Vec<u8>, &mut Vec<u8>) {
        self.buffers.0.clear();
        self.buffers.1.clear();

        (&mut self.buffers.0, &mut self.buffers.1)
    }
}

impl Default for RedactionBuffer {
    fn default() -> Self {
        RedactionBuffer::new(0)
    }
}
