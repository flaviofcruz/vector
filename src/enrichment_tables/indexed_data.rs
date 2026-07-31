//! A reusable, indexed, in-memory table body shared by enrichment tables that hold their
//! data as a set of named columns and rows of [`Value`]s (for example the `file` and `http`
//! tables).
//!
//! [`IndexedData`] owns the column headers, the row data, and any indexes built over exact-
//! match fields, and implements the lookup logic used by
//! [`vector_lib::enrichment::Table`]: exact-match search (index-backed when available) and
//! date-range search (sequential scan). Enrichment tables embed an `IndexedData` and forward
//! their `Table` methods to it, so the indexing/lookup engine lives in exactly one place.

use std::{collections::HashMap, hash::Hasher};

use vector_lib::enrichment::{Case, Condition, IndexHandle};
use vrl::value::{ObjectMap, Value};

/// An in-memory table body: ordered column headers, rows of values, and indexes built over
/// exact-match fields.
#[derive(Clone, Default)]
pub struct IndexedData {
    headers: Vec<String>,
    data: Vec<Vec<Value>>,
    indexes: Vec<(
        Case,
        Vec<usize>,
        HashMap<u64, Vec<usize>, hash_hasher::HashBuildHasher>,
    )>,
}

impl IndexedData {
    /// Creates a new [`IndexedData`] from column headers and rows. Rows are expected to be
    /// aligned with `headers` (one value per column, in header order). No indexes are built
    /// yet; callers register them via [`IndexedData::add_index`].
    pub fn new(headers: Vec<String>, data: Vec<Vec<Value>>) -> Self {
        Self {
            headers,
            data,
            indexes: Vec::new(),
        }
    }

    /// The column headers, in order.
    pub fn headers(&self) -> &[String] {
        &self.headers
    }

    /// The number of rows held.
    pub fn len(&self) -> usize {
        self.data.len()
    }

    /// Whether the table holds no rows.
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// The number of indexes built.
    pub fn index_count(&self) -> usize {
        self.indexes.len()
    }

    fn column_index(&self, col: &str) -> Option<usize> {
        self.headers.iter().position(|header| header == col)
    }

    /// Does the given row match all the conditions specified?
    fn row_equals(
        &self,
        case: Case,
        condition: &[Condition],
        row: &[Value],
        wildcard: Option<&Value>,
    ) -> bool {
        condition.iter().all(|condition| match condition {
            Condition::Equals { field, value } => match self.column_index(field) {
                None => false,
                Some(idx) => {
                    let current_row_value = &row[idx];

                    // Helper closure for comparing current_row_value with another value,
                    // respecting the specified case for Value::Bytes.
                    let compare_values = |val_to_compare: &Value| -> bool {
                        match (case, current_row_value, val_to_compare) {
                            (
                                Case::Insensitive,
                                Value::Bytes(bytes_row),
                                Value::Bytes(bytes_cmp),
                            ) => {
                                // Perform case-insensitive comparison for byte strings.
                                // If both are valid UTF-8, compare their lowercase versions.
                                // If both are non-UTF-8 bytes, compare them directly.
                                // If one is UTF-8 and the other is not, they are considered not equal.
                                match (
                                    std::str::from_utf8(bytes_row),
                                    std::str::from_utf8(bytes_cmp),
                                ) {
                                    (Ok(s_row), Ok(s_cmp)) => {
                                        s_row.to_lowercase() == s_cmp.to_lowercase()
                                    }
                                    (Err(_), Err(_)) => bytes_row == bytes_cmp,
                                    _ => false,
                                }
                            }
                            // For Case::Sensitive, or for Case::Insensitive with non-Bytes types,
                            // perform a direct equality check.
                            _ => current_row_value == val_to_compare,
                        }
                    };

                    // First, check if the row value matches the condition's value.
                    if compare_values(value) {
                        true
                    } else if let Some(wc_val) = wildcard {
                        // If not, and a wildcard is provided, check if the row value matches the wildcard.
                        compare_values(wc_val)
                    } else {
                        // Otherwise, no match.
                        false
                    }
                }
            },
            Condition::BetweenDates { field, from, to } => match self.column_index(field) {
                None => false,
                Some(idx) => match row[idx] {
                    Value::Timestamp(date) => from <= &date && &date <= to,
                    _ => false,
                },
            },
            Condition::FromDate { field, from } => match self.column_index(field) {
                None => false,
                Some(idx) => match row[idx] {
                    Value::Timestamp(date) => from <= &date,
                    _ => false,
                },
            },
            Condition::ToDate { field, to } => match self.column_index(field) {
                None => false,
                Some(idx) => match row[idx] {
                    Value::Timestamp(date) => &date <= to,
                    _ => false,
                },
            },
        })
    }

    fn add_columns(&self, select: Option<&[String]>, row: &[Value]) -> ObjectMap {
        self.headers
            .iter()
            .zip(row)
            .filter(|(header, _)| {
                select
                    .map(|select| select.contains(header))
                    // If no select is passed, we assume all columns are included
                    .unwrap_or(true)
            })
            .map(|(header, col)| (header.as_str().into(), col.clone()))
            .collect()
    }

    /// Order the fields in the index according to the position they are found in the header.
    fn normalize_index_fields(&self, index: &[&str]) -> Result<Vec<usize>, String> {
        // Get the positions of the fields we are indexing
        let normalized = self
            .headers
            .iter()
            .enumerate()
            .filter_map(|(idx, col)| {
                if index.contains(&col.as_ref()) {
                    Some(idx)
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();

        if normalized.len() != index.len() {
            let missing = index
                .iter()
                .filter_map(|col| {
                    if self.headers.iter().any(|header| header == *col) {
                        None
                    } else {
                        Some(col.to_string())
                    }
                })
                .collect::<Vec<_>>()
                .join(", ");
            Err(format!("field(s) '{missing}' missing from dataset"))
        } else {
            Ok(normalized)
        }
    }

    /// Creates an index with the given fields.
    /// Uses seahash to create a hash of the data that is used as the key in a hashmap lookup to
    /// the index of the row in the data.
    ///
    /// Ensure fields that are searched via a comparison are not included in the index!
    fn index_data(
        &self,
        fieldidx: &[usize],
        case: Case,
    ) -> Result<HashMap<u64, Vec<usize>, hash_hasher::HashBuildHasher>, String> {
        let mut index = HashMap::with_capacity_and_hasher(
            self.data.len(),
            hash_hasher::HashBuildHasher::default(),
        );

        for (idx, row) in self.data.iter().enumerate() {
            let mut hash = seahash::SeaHasher::default();

            for idx in fieldidx {
                hash_value(&mut hash, case, &row[*idx])?;
            }

            let key = hash.finish();

            let entry = index.entry(key).or_insert_with(Vec::new);
            entry.push(idx);
        }

        index.shrink_to_fit();

        Ok(index)
    }

    /// Sequentially searches through the iterator for the given condition.
    fn sequential<'a, I>(
        &'a self,
        data: I,
        case: Case,
        condition: &'a [Condition<'a>],
        select: Option<&'a [String]>,
        wildcard: Option<&'a Value>,
    ) -> impl Iterator<Item = ObjectMap> + 'a
    where
        I: Iterator<Item = &'a Vec<Value>> + 'a,
    {
        data.filter_map(move |row| {
            if self.row_equals(case, condition, row, wildcard) {
                Some(self.add_columns(select, row))
            } else {
                None
            }
        })
    }

    fn indexed<'a>(
        &'a self,
        case: Case,
        condition: &'a [Condition<'a>],
        handle: IndexHandle,
    ) -> Result<Option<&'a Vec<usize>>, String> {
        // The index to use has been passed, we can use this to search the data.
        // We are assuming that the caller has passed an index that represents the fields
        // being passed in the condition.
        let mut hash = seahash::SeaHasher::default();

        for header in self.headers.iter() {
            if let Some(Condition::Equals { value, .. }) = condition.iter().find(
                |condition| matches!(condition, Condition::Equals { field, .. } if field == header),
            ) {
                hash_value(&mut hash, case, value)?;
            }
        }

        let key = hash.finish();

        let IndexHandle(handle) = handle;
        // Resolve the handle defensively: a handle is a position allocated by the caller and is
        // expected to always be in range, but indexing directly would panic the per-event
        // lookup path if that invariant were ever violated. Degrade to an error instead.
        let index = self
            .indexes
            .get(handle)
            .ok_or_else(|| format!("enrichment table index handle {handle} out of range"))?;
        Ok(index.2.get(&key))
    }

    fn indexed_with_wildcard<'a>(
        &'a self,
        case: Case,
        wildcard: &'a Value,
        condition: &'a [Condition<'a>],
        handle: IndexHandle,
    ) -> Result<Option<&'a Vec<usize>>, String> {
        if let Some(result) = self.indexed(case, condition, handle)? {
            return Ok(Some(result));
        }

        // If lookup fails and a wildcard is provided, compute hash for the wildcard
        let mut wildcard_hash = seahash::SeaHasher::default();
        for header in self.headers.iter() {
            if condition.iter().any(
                |condition| matches!(condition, Condition::Equals { field, .. } if field == header),
            ) {
                hash_value(&mut wildcard_hash, case, wildcard)?;
            }
        }

        let wildcard_key = wildcard_hash.finish();
        let IndexHandle(handle) = handle;
        let index = self
            .indexes
            .get(handle)
            .ok_or_else(|| format!("enrichment table index handle {handle} out of range"))?;
        Ok(index.2.get(&wildcard_key))
    }

    /// Search the data for the single row matching all conditions. Errors if zero or more
    /// than one row matches.
    pub fn find_table_row<'a>(
        &self,
        case: Case,
        condition: &'a [Condition<'a>],
        select: Option<&'a [String]>,
        wildcard: Option<&Value>,
        index: Option<IndexHandle>,
    ) -> Result<ObjectMap, String> {
        match index {
            None => {
                // No index has been passed so we need to do a Sequential Scan.
                single_or_err(self.sequential(self.data.iter(), case, condition, select, wildcard))
            }
            Some(handle) => {
                let result = if let Some(wildcard) = wildcard {
                    self.indexed_with_wildcard(case, wildcard, condition, handle)?
                } else {
                    self.indexed(case, condition, handle)?
                }
                .ok_or_else(|| "no rows found in index".to_string())?
                .iter()
                .map(|idx| &self.data[*idx]);

                // Perform a sequential scan over the indexed result.
                single_or_err(self.sequential(result, case, condition, select, wildcard))
            }
        }
    }

    /// Search the data for all rows matching all conditions.
    pub fn find_table_rows<'a>(
        &self,
        case: Case,
        condition: &'a [Condition<'a>],
        select: Option<&'a [String]>,
        wildcard: Option<&Value>,
        index: Option<IndexHandle>,
    ) -> Result<Vec<ObjectMap>, String> {
        match index {
            None => {
                // No index has been passed so we need to do a Sequential Scan.
                Ok(self
                    .sequential(self.data.iter(), case, condition, select, wildcard)
                    .collect())
            }
            Some(handle) => {
                // Perform a sequential scan over the indexed result.
                let indexed_result = if let Some(wildcard) = wildcard {
                    self.indexed_with_wildcard(case, wildcard, condition, handle)?
                } else {
                    self.indexed(case, condition, handle)?
                };

                Ok(self
                    .sequential(
                        indexed_result
                            .iter()
                            .flat_map(|results| results.iter().map(|idx| &self.data[*idx])),
                        case,
                        condition,
                        select,
                        wildcard,
                    )
                    .collect())
            }
        }
    }

    /// Register an index over the given fields for the given case sensitivity, returning a
    /// handle to it. Reuses an existing matching index if one is already present.
    pub fn add_index(&mut self, case: Case, fields: &[&str]) -> Result<IndexHandle, String> {
        let normalized = self.normalize_index_fields(fields)?;
        match self
            .indexes
            .iter()
            .position(|index| index.0 == case && index.1 == normalized)
        {
            Some(pos) => {
                // This index already exists
                Ok(IndexHandle(pos))
            }
            None => {
                let index = self.index_data(&normalized, case)?;
                self.indexes.push((case, normalized, index));
                // The returned index handle is the position of the index in our list of indexes.
                Ok(IndexHandle(self.indexes.len() - 1))
            }
        }
    }

    /// Re-apply a set of index specifications (as returned by [`IndexedData::index_fields`])
    /// to this data, building an index for each. Used when a table's data is reloaded and the
    /// indexes registered against the previous snapshot must be rebuilt over the new rows.
    ///
    /// An [`IndexHandle`] is a position allocated against the caller's spec list, and lookups
    /// resolve it by indexing into `self.indexes`. This method therefore rebuilds the indexes
    /// **strictly one-to-one with `specs`, in order**: every spec produces exactly one entry at
    /// its matching position. A spec whose field(s) are absent from the refreshed data (or that
    /// fails to hash) gets a placeholder empty index — which matches nothing — rather than being
    /// skipped. Skipping would shorten `self.indexes` and shift every later position, leaving
    /// previously-issued handles pointing out of bounds or at the wrong index. Preserving the
    /// 1:1 mapping lets a source schema change (a dropped or renamed indexed column) be
    /// tolerated without corrupting handle resolution.
    pub fn reapply_indexes(&mut self, specs: &[(Case, Vec<String>)]) {
        self.indexes.clear();
        self.indexes.reserve(specs.len());
        for (case, fields) in specs {
            let refs = fields.iter().map(String::as_str).collect::<Vec<_>>();
            // Build the real index when the field(s) still exist and hash cleanly; otherwise
            // occupy this position with a placeholder empty index so later handles stay aligned.
            let entry = self
                .normalize_index_fields(&refs)
                .and_then(|normalized| {
                    let map = self.index_data(&normalized, *case)?;
                    Ok((*case, normalized, map))
                })
                .unwrap_or_else(|_| (*case, Vec::new(), HashMap::default()));
            self.indexes.push(entry);
        }
    }

    /// Returns a list of the field names that are in each index.
    pub fn index_fields(&self) -> Vec<(Case, Vec<String>)> {
        self.indexes
            .iter()
            .map(|index| {
                let (case, fields, _) = index;
                (
                    *case,
                    fields
                        .iter()
                        .map(|idx| self.headers[*idx].clone())
                        .collect::<Vec<_>>(),
                )
            })
            .collect::<Vec<_>>()
    }
}

/// Adds the bytes from the given value to the hash.
/// Each field is terminated by a `0` value to separate the fields
fn hash_value(hasher: &mut seahash::SeaHasher, case: Case, value: &Value) -> Result<(), String> {
    match value {
        Value::Bytes(bytes) => match case {
            Case::Sensitive => hasher.write(bytes),
            Case::Insensitive => hasher.write(
                std::str::from_utf8(bytes)
                    .map_err(|_| "column contains invalid utf".to_string())?
                    .to_lowercase()
                    .as_bytes(),
            ),
        },
        value => {
            let bytes: bytes::Bytes = value.encode_as_bytes()?;
            hasher.write(&bytes);
        }
    }

    hasher.write_u8(0);

    Ok(())
}

/// Returns an error if the iterator doesn't yield exactly one result.
fn single_or_err<I, T>(mut iter: T) -> Result<I, String>
where
    T: Iterator<Item = I>,
{
    let result = iter.next();

    if iter.next().is_some() {
        // More than one row has been found.
        Err("more than one row found".to_string())
    } else {
        result.ok_or_else(|| "no rows found".to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn data(headers: &[&str], rows: &[&[&str]]) -> IndexedData {
        IndexedData::new(
            headers.iter().map(|h| h.to_string()).collect(),
            rows.iter()
                .map(|r| r.iter().map(|c| Value::from(*c)).collect())
                .collect(),
        )
    }

    fn eq(field: &'static str, value: &str) -> Vec<Condition<'static>> {
        vec![Condition::Equals {
            field,
            value: Value::from(value),
        }]
    }

    // Regression test for the index-handle misalignment: when a refresh rebuilds indexes over
    // data that no longer contains a previously-indexed column, the surviving handle must still
    // resolve to the correct index — not shift position or panic out of bounds.
    #[test]
    fn reapply_indexes_keeps_handles_aligned_when_a_column_disappears() {
        let mut table = data(&["a", "b"], &[&["a1", "b1"], &["a2", "b2"]]);

        // Two indexes, handles 0 (column a) and 1 (column b).
        let h_a = table.add_index(Case::Sensitive, &["a"]).unwrap();
        let h_b = table.add_index(Case::Sensitive, &["b"]).unwrap();
        assert_eq!(h_a, IndexHandle(0));
        assert_eq!(h_b, IndexHandle(1));

        let specs = table.index_fields();

        // Refresh with data where column "a" is gone; "b" remains.
        let mut refreshed = data(&["b", "c"], &[&["b1", "c1"], &["b2", "c2"]]);
        refreshed.reapply_indexes(&specs);

        // Both positions still exist (placeholder for the missing "a" index).
        assert_eq!(refreshed.index_count(), 2);

        // Handle 1 (column b) must still resolve to b's index — not shift to position 0.
        let row = refreshed
            .find_table_row(Case::Sensitive, &eq("b", "b2"), None, None, Some(h_b))
            .unwrap();
        assert_eq!(row.get("c"), Some(&Value::from("c2")));

        // Handle 0 (now a placeholder for the dropped column) matches nothing but does not panic.
        let missing =
            refreshed.find_table_row(Case::Sensitive, &eq("a", "a1"), None, None, Some(h_a));
        assert!(missing.is_err());
    }

    // An out-of-range handle degrades to an error rather than panicking the lookup path.
    #[test]
    fn out_of_range_handle_errors_instead_of_panicking() {
        let table = data(&["a"], &[&["a1"]]);
        let err = table
            .find_table_row(
                Case::Sensitive,
                &eq("a", "a1"),
                None,
                None,
                Some(IndexHandle(5)),
            )
            .unwrap_err();
        assert!(err.contains("out of range"), "unexpected error: {err}");
    }
}
