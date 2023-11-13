use serde_json::{Map, Value};

#[derive(Debug)]
pub enum QueryResults {
    AffectedRows(u32),
    Results(Vec<Map<String, Value>>)
}
