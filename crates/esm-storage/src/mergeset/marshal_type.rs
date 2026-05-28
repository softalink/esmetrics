//! Per-block compression discriminator. Matches `lib/mergeset/encoding.go:197-209`.

use thiserror::Error;

/// Per-block compression discriminator. Stored as a single uint8 in each
/// `BlockHeader`.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum MarshalType {
    /// Plain encoding (no zstd). Used for small blocks where compression is
    /// not worth the CPU cost. See `encoding.go:278-282`.
    #[default]
    Plain = 0,
    /// Zstd-compressed encoding with delta-prefix length encoding.
    Zstd = 1,
}

impl MarshalType {
    /// Construct from the on-disk byte. Returns an error for any value outside
    /// `[0, 1]`.
    ///
    /// # Errors
    /// Returns [`MarshalTypeError`] if `byte` is not 0 or 1.
    pub const fn from_byte(byte: u8) -> Result<Self, MarshalTypeError> {
        match byte {
            0 => Ok(Self::Plain),
            1 => Ok(Self::Zstd),
            other => Err(MarshalTypeError::Unknown(other)),
        }
    }

    /// On-disk byte representation.
    #[must_use]
    pub const fn as_byte(self) -> u8 {
        self as u8
    }
}

#[derive(Debug, Error)]
pub enum MarshalTypeError {
    #[error("unknown marshalType={0}; must be in [0, 1]")]
    Unknown(u8),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_plain() {
        assert_eq!(MarshalType::from_byte(0).unwrap(), MarshalType::Plain);
        assert_eq!(MarshalType::Plain.as_byte(), 0);
    }

    #[test]
    fn roundtrip_zstd() {
        assert_eq!(MarshalType::from_byte(1).unwrap(), MarshalType::Zstd);
        assert_eq!(MarshalType::Zstd.as_byte(), 1);
    }

    #[test]
    fn unknown_byte_rejected() {
        assert!(MarshalType::from_byte(2).is_err());
        assert!(MarshalType::from_byte(255).is_err());
    }
}
