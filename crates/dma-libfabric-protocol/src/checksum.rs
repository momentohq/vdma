//! CRC-32/ISCSI (Castagnoli, reflected) over a DMA payload. Both peers compute it the same way, so a
//! transfer verifies end-to-end: the server returns the checksum of the bytes it moved and the
//! client checks it against its local copy.
//!
//! Unlike the IEEE polynomial, Castagnoli has dedicated hardware — ARMv8 `crc32c*` on Graviton,
//! SSE4.2 `crc32` on x86 — plus carry-less-multiply folding, so verification stops being the
//! throughput ceiling at large payloads.

use crc_fast::CrcAlgorithm;

/// CRC-32c of `bytes`.
pub fn checksum(bytes: &[u8]) -> u32 {
    crc_fast::checksum(CrcAlgorithm::Crc32Iscsi, bytes) as u32
}

#[cfg(test)]
mod tests {
    use super::checksum;

    #[test]
    fn matches_known_crc32c_vectors() {
        // The standard CRC-32c/ISCSI check value.
        assert_eq!(checksum(b""), 0x0000_0000);
        assert_eq!(checksum(b"123456789"), 0xE306_9283);
    }

    #[test]
    fn detects_a_single_bit_flip() {
        let original = checksum(b"hello over the fabric");
        let flipped = checksum(b"hallo over the fabric");
        assert_ne!(original, flipped);
    }

    #[test]
    fn is_order_sensitive() {
        assert_ne!(checksum(b"ab"), checksum(b"ba"));
    }
}
