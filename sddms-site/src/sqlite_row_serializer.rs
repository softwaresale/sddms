use std::ops::Index;
use sqlite::{Row, Value};
use serde_json::{Map, Number};

pub fn serialize_row(row: &Row, col_names: &[String]) -> Map<String, serde_json::Value> {
    let mut obj: Map<String, serde_json::Value> = Map::new();
    for (col_idx, name) in col_names.iter().enumerate() {
        let col_value = row.index(col_idx);
        let serialized_value = match col_value {
            Value::Binary(blob) => {
                let byte_vec = blob.iter()
                    .map(|blob_byte| serde_json::Value::Number(Number::from(*blob_byte)))
                    .collect::<Vec<_>>();
                serde_json::Value::Array(byte_vec)
            }
            Value::Float(f_value) => {
                let num = Number::from_f64(*f_value).or(Number::from_f64(0f64)).unwrap();
                serde_json::Value::Number(num)
            }
            Value::Integer(i_value) => {
                serde_json::Value::Number(Number::from(*i_value))
            }
            Value::String(string) => {
                serde_json::Value::String(string.clone())
            }
            Value::Null => {
                serde_json::Value::Null
            }
        };

        obj.insert(name.to_string(), serialized_value);
    }

    obj
}
