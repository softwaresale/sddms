use std::cmp::Ordering;
use std::fmt::{Display, Formatter};
use crate::shared::{LockMode, LockRequest};

impl Display for LockMode {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            LockMode::Unspecified => write!(f, "unspecified"),
            LockMode::Exclusive => write!(f, "exclusive"),
            LockMode::Shared => write!(f, "shared")
        }
    }
}

impl LockRequest {
    pub fn new<StrT: Into<String>>(resource: StrT, mode: LockMode) -> Self {
        let mut request = Self::default();
        request.set_mode(mode);
        request.record = resource.into();
        request
    }
}

impl Display for LockRequest {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} {}", self.record, self.mode)
    }
}

impl Eq for LockRequest {
}

impl PartialOrd for LockRequest {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match self.mode() {
            LockMode::Unspecified => {
                None
            }
            LockMode::Exclusive => {
                match other.mode() {
                    LockMode::Unspecified => None,
                    LockMode::Exclusive => Some(Ordering::Equal),
                    LockMode::Shared => Some(Ordering::Greater),
                }
            }
            LockMode::Shared => {
                match other.mode() {
                    LockMode::Unspecified => None,
                    LockMode::Exclusive => Some(Ordering::Less),
                    LockMode::Shared => Some(Ordering::Equal),
                }
            }
        }
    }
}

impl Ord for LockRequest {
    fn cmp(&self, other: &Self) -> Ordering {
        match self.partial_cmp(other) {
            None => self.record.cmp(&other.record),
            Some(order) => order
        }
    }
}
