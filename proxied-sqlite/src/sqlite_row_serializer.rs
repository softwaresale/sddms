use rusqlite::Row;
use rusqlite::types::ValueRef;
use serde_json::{Map, Number};

pub fn serialize_row(row: &Row, col_names: &[String]) -> Map<String, serde_json::Value> {
    let mut obj: Map<String, serde_json::Value> = Map::new();
    for (col_idx, name) in col_names.iter().enumerate() {
        let col_value = row.get_ref_unwrap(col_idx);
        let serialized_value = match col_value {
            ValueRef::Blob(blob) => {
                let byte_vec = blob.iter()
                    .map(|blob_byte| serde_json::Value::Number(Number::from(*blob_byte)))
                    .collect::<Vec<_>>();
                serde_json::Value::Array(byte_vec)
            }
            ValueRef::Real(f_value) => {
                let num = Number::from_f64(f_value).or(Number::from_f64(0f64)).unwrap();
                serde_json::Value::Number(num)
            }
            ValueRef::Integer(i_value) => {
                serde_json::Value::Number(Number::from(i_value))
            }
            ValueRef::Text(string) => {
                let string = String::from_utf8(Vec::from(string)).unwrap();
                serde_json::Value::String(string)
            }
            ValueRef::Null => {
                serde_json::Value::Null
            }
        };

        obj.insert(name.to_string(), serialized_value);
    }

    obj
}