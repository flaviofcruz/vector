//! This module is copied from:
//! https://github.com/databricks-eng/universe/tree/master/common/entropy/rust/src/lib.rs

/// Returns the Shannon entropy of the input text.
pub fn entropy(input: &[u8]) -> f64 {
    if input.is_empty() {
        return 0.0;
    }
    let mut hist = [0f64; u8::MAX as usize + 1];
    for b in input {
        let index: usize = (*b).into();
        hist[index] += 1.0;
    }

    let mut sum = 0.0;
    for count in hist {
        if count > 0.0 {
            sum += count * count.ln();
        }
    }
    let size: f64 = input.len() as _;
    -(sum / size - size.ln()) / (2.0f64).ln()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_entropy() {
        assert_eq!(entropy(b""), 0.0);
        assert_eq!(entropy(b"a"), 0.0);
        assert_eq!(entropy(b"ab"), 1.0);
        assert_eq!(entropy(b"abc"), 3.0f64.ln() / 2.0f64.ln());
        assert_eq!(entropy(b"aa"), 0.0);
        assert_eq!(
            entropy(b"aab"),
            -(2.0 * (2.0f64.ln()) / 3.0 - (3.0f64.ln())) / (2.0f64.ln())
        );
        assert_eq!(entropy(b"aaa"), 0.0);
        assert_eq!(entropy("a\u{5b57}".as_bytes()), 2.0);
    }
}
