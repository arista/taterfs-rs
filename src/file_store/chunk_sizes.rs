//! Chunk size constants and utilities for breaking files into chunks.
//!
//! Files are broken into chunks using a descending size table to optimize
//! for files that grow from their end, minimizing changes between versions.

/// Chunk sizes for breaking down files (in descending order).
///
/// When chunking a file, the largest size that fits the remaining bytes
/// is chosen. This continues until no size fits, at which point the
/// remainder becomes the final chunk.
pub const CHUNK_SIZES: &[u64] = &[
    4_194_304, // 4 MB
    1_048_576, // 1 MB
    262_144,   // 256 KB
    65_536,    // 64 KB
    16_384,    // 16 KB
];

/// Returns the next chunk size for the given remaining bytes.
///
/// Finds the largest chunk size from `CHUNK_SIZES` that is less than or
/// equal to `remaining`. If no size fits, returns `remaining` itself
/// (the final chunk).
pub fn next_chunk_size(remaining: u64) -> u64 {
    CHUNK_SIZES
        .iter()
        .copied()
        .find(|&size| size <= remaining)
        .unwrap_or(remaining)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_large_file_uses_4mb() {
        assert_eq!(next_chunk_size(10_000_000), 4_194_304);
    }

    #[test]
    fn test_exactly_4mb() {
        assert_eq!(next_chunk_size(4_194_304), 4_194_304);
    }

    #[test]
    fn test_between_1mb_and_4mb() {
        assert_eq!(next_chunk_size(2_000_000), 1_048_576);
    }

    #[test]
    fn test_small_file_uses_remainder() {
        assert_eq!(next_chunk_size(1000), 1000);
    }

    #[test]
    fn test_exactly_16kb() {
        assert_eq!(next_chunk_size(16_384), 16_384);
    }

    #[test]
    fn test_just_under_16kb() {
        assert_eq!(next_chunk_size(16_383), 16_383);
    }

    #[test]
    fn test_zero_remaining() {
        assert_eq!(next_chunk_size(0), 0);
    }
}
