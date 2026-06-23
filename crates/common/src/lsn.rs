use std::fmt;
use std::ops::{Add, AddAssign, Sub};
use serde::{Deserialize, Serialize};

/// Log Sequence Number — a monotonically increasing position in the Postgres WAL.
/// Every page version corresponds to "the state of this page as of LSN X."
/// An LSN of 0 is the "invalid" sentinel; valid LSNs start at 1.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Lsn(pub u64);

impl Lsn {
    pub const INVALID: Lsn = Lsn(0);
    pub const MIN: Lsn = Lsn(1);
    pub const MAX: Lsn = Lsn(u64::MAX);

    pub fn is_valid(self) -> bool {
        self.0 != 0
    }

    pub fn as_u64(self) -> u64 {
        self.0
    }

    /// Align to a multiple of `align` (used for WAL segment boundaries).
    pub fn align_up(self, align: u64) -> Lsn {
        Lsn((self.0 + align - 1) & !(align - 1))
    }
}

impl fmt::Display for Lsn {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Format as hex like Postgres does: X/XXXXXXXX
        write!(f, "{:X}/{:08X}", self.0 >> 32, self.0 & 0xFFFFFFFF)
    }
}

impl From<u64> for Lsn {
    fn from(v: u64) -> Self {
        Lsn(v)
    }
}

impl From<Lsn> for u64 {
    fn from(l: Lsn) -> Self {
        l.0
    }
}

impl Add<u64> for Lsn {
    type Output = Lsn;
    fn add(self, rhs: u64) -> Self::Output {
        Lsn(self.0 + rhs)
    }
}

impl AddAssign<u64> for Lsn {
    fn add_assign(&mut self, rhs: u64) {
        self.0 += rhs;
    }
}

impl Sub<Lsn> for Lsn {
    type Output = u64;
    fn sub(self, rhs: Lsn) -> Self::Output {
        self.0.saturating_sub(rhs.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ordering() {
        assert!(Lsn(10) < Lsn(20));
        assert!(Lsn(20) > Lsn(10));
        assert!(Lsn(10) == Lsn(10));
    }

    #[test]
    fn display() {
        let lsn = Lsn(0x0000000100000000);
        assert_eq!(format!("{}", lsn), "1/00000000");
    }

    #[test]
    fn arithmetic() {
        assert_eq!(Lsn(10) + 5, Lsn(15));
        assert_eq!(Lsn(20) - Lsn(10), 10u64);
    }
}
