use std::fmt::{Display, Formatter};
use std::hash::{Hash, Hasher};
use crate::history_file_parser::action::Action;

#[derive(Debug, Copy, Clone, Ord, PartialOrd, Eq, PartialEq, Hash)]
pub struct TransactionId(pub(crate) u32, pub(crate) u32, pub(crate) u32);

impl From<&Action> for TransactionId {
    fn from(value: &Action) -> Self {
        Self(value.site_id, value.client_id, value.transaction_id)
    }
}

impl Display for TransactionId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "<{},{},{}>", self.0, self.1, self.2)
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialOrd)]
pub struct TransactionPair(TransactionId, TransactionId);

impl TransactionPair {
    pub fn new(left: TransactionId, right: TransactionId) -> Self {
        Self(left, right)
    }

    pub fn as_ascending_pair(&self) -> (TransactionId, TransactionId) {
        if self.0 < self.1 {
            (self.0, self.1)
        } else {
            (self.1, self.0)
        }
    }
}

impl PartialEq for TransactionPair {
    fn eq(&self, other: &Self) -> bool {

        let sorted_self = if self.0 < self.1 {
            (self.0, self.1)
        } else {
            (self.1, self.0)
        };

        let sorted_other = if other.0 < other.1 {
            (other.0, other.1)
        } else {
            (other.1, other.0)
        };

        sorted_self == sorted_other
    }
}

impl Hash for TransactionPair {
    fn hash<H: Hasher>(&self, state: &mut H) {
        let (less, more) = self.as_ascending_pair();
        less.hash(state);
        more.hash(state);
    }
}

impl Into<(TransactionId, TransactionId)> for TransactionPair {
    fn into(self) -> (TransactionId, TransactionId) {
        (self.0, self.1)
    }
}
