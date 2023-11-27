use std::collections::HashMap;
use rand::distributions::Distribution;
use rand::Rng;
use rusqlite::types::Value;

#[derive(Clone)]
pub enum RandomQueryStmtKind {
    Select,
    Update,
    Insert
}

pub struct RandomQueryStmtKindGen;

impl Distribution<RandomQueryStmtKind> for RandomQueryStmtKindGen {
    fn sample<R: Rng + ?Sized>(&self, rng: &mut R) -> RandomQueryStmtKind {
        match rng.gen_range(0..3) {
            0 => RandomQueryStmtKind::Select,
            1 => RandomQueryStmtKind::Update,
            2 => RandomQueryStmtKind::Insert,
            _ => unreachable!()
        }
    }
}

pub enum RandomQueryStmt {
    Select {
        /// the columns we want to select
        columns: Vec<String>,
    },
    Update {
        /// the map of updates, where each key is a column name and the value is the updated value
        updates: HashMap<String, Value>,
        /// how to determine which record to update
        predicate: String,
    },
    Insert {
        /// which columns we are insert into
        columns: Vec<String>,
        /// the list of records we're going to insert
        values: Vec<HashMap<String, Value>>,
    },
}
