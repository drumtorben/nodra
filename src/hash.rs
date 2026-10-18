//! Stable hashing for work-unit assignment.
//!
//! Assignment must be identical across processes, machines and Nodra versions, so it uses a
//! fixed, specified hash (FNV-1a, 64 bit) instead of `std`'s randomized hasher.

const OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
const PRIME: u64 = 0x0000_0100_0000_01b3;

/// FNV-1a 64-bit hash of `bytes`.
pub fn fnv1a64(bytes: &[u8]) -> u64 {
    let mut h = OFFSET;
    for b in bytes {
        h ^= u64::from(*b);
        h = h.wrapping_mul(PRIME);
    }
    h
}

/// The worker (0-based) responsible for a unit identified by `key`.
pub fn worker_for(key: &str, workers: u32) -> u32 {
    assert!(workers > 0, "worker count must be positive");
    (fnv1a64(key.as_bytes()) % u64::from(workers)) as u32
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn known_vectors() {
        // Reference values from the FNV specification.
        assert_eq!(fnv1a64(b""), 0xcbf29ce484222325);
        assert_eq!(fnv1a64(b"a"), 0xaf63dc4c8601ec8c);
        assert_eq!(fnv1a64(b"foobar"), 0x85944171f73967e8);
    }

    #[test]
    fn assignment_is_total_and_balanced() {
        let n = 4;
        let mut counts = [0u32; 4];
        for i in 0..4000 {
            counts[worker_for(&format!("part-{i:05}.parquet"), n) as usize] += 1;
        }
        for c in counts {
            assert!(c > 800 && c < 1200, "unbalanced: {counts:?}");
        }
    }
}
