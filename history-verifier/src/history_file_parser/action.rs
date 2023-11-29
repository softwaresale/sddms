use std::collections::HashSet;
use std::fmt::{Display, Formatter};
use time::OffsetDateTime;
use crate::transaction_id::TransactionId;

#[derive(Debug, PartialEq, Eq)]
#[repr(u16)]
pub enum ActionKind {
    BeginTransaction = 0,
    CommitTransaction,
    RollbackTransaction,
    Query { read_set: HashSet<String>, write_set: HashSet<String> },
}

impl Display for ActionKind {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ActionKind::BeginTransaction => write!(f, "BEGIN TRANSACTION"),
            ActionKind::CommitTransaction => write!(f, "COMMIT"),
            ActionKind::RollbackTransaction => write!(f, "ROLLBACK"),
            ActionKind::Query { read_set, write_set } => write!(f, "Read({:?}),Write({:?})", read_set, write_set)
        }
    }
}

#[derive(Debug, PartialEq)]
pub struct Action {
    pub(crate) instant: OffsetDateTime,
    pub(crate) site_id: u32,
    pub(crate) client_id: u32,
    pub(crate) transaction_id: u32,
    pub(crate) action: ActionKind,
}

impl Display for Action {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let transaction_id = TransactionId::from(self);
        write!(f, "{} {} {}", self.instant, transaction_id, self.action)
    }
}
