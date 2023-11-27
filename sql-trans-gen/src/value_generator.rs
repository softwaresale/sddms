mod text_gen;
mod num_gen;


use std::collections::{HashMap};
use rusqlite::types::{Type, Value};
use sddms_shared::error::SddmsError;
use crate::config::TextGenRule;
use crate::db_schema::{TableInfo};
use crate::value_generator::num_gen::{FloatGenerator, IntegerGenerator};
use crate::value_generator::text_gen::TextValueGenerator;

pub trait ValueGenerator {
    fn generate(&self) -> Result<Value, SddmsError>;
}

pub struct ValueGeneratorMap {
    text: TextValueGenerator,
    real: FloatGenerator,
    integer: IntegerGenerator,
}

impl Default for ValueGeneratorMap {
    fn default() -> Self {
        Self {
            text: TextValueGenerator::new_random(TextGenRule::default()),
            real: FloatGenerator::new(0f64..=100f64),
            integer: IntegerGenerator::new(0..=100),
        }
    }
}

impl ValueGeneratorMap {
    pub fn generate(&self, tp: &Type) -> Value {
        match tp {
            Type::Null => Value::Null,
            Type::Integer => self.integer.generate().unwrap(),
            Type::Real => self.real.generate().unwrap(),
            Type::Text => self.text.generate().unwrap(),
            Type::Blob => panic!("Blob is not supported")
        }
    }
}

pub struct TableRecordGenerator {
    field_gens: HashMap<String, Box<dyn ValueGenerator>>,
}

impl TableRecordGenerator {

    pub fn new(table_info: &TableInfo, default_gen: &ValueGeneratorMap) -> Self {

        let mut field_gens: HashMap<String, Box<dyn ValueGenerator>> = HashMap::new();

        for (field_name, info) in table_info.fields() {
            match info.tp() {
                Type::Integer => {
                    let int_gen = info.int_range_inc_constraint()
                        .as_ref()
                        .map(|range| IntegerGenerator::new(range.clone()))
                        .or(
                            info.int_range_constraint()
                                .as_ref()
                                .map(|range| IntegerGenerator::new(range.clone()))
                        )
                        .unwrap_or(default_gen.integer.clone());

                    field_gens.insert(field_name.clone(), Box::new(int_gen));
                }
                Type::Real => {
                    let float_gen = info.real_range_inc_constraint()
                        .as_ref()
                        .map(|range| FloatGenerator::new(range.clone()))
                        .or(
                            info.real_range_constraint()
                                .as_ref()
                                .map(|range| FloatGenerator::new(range.clone()))
                        )
                        .unwrap_or(default_gen.real.clone());

                    field_gens.insert(field_name.clone(), Box::new(float_gen));
                }
                Type::Text => {
                    field_gens.insert(field_name.clone(), Box::new(default_gen.text.clone()));
                }
                _ => panic!(),
            }
        }

        Self {
            field_gens
        }
    }

    pub fn generate_for_column(&self, col: &str) -> Result<Value, SddmsError> {
        self.field_gens.get(col).unwrap().generate()
    }

    pub fn generate_record(&self, cols: &[String]) -> Result<HashMap<String, Value>, SddmsError> {
        let mut record = HashMap::new();
        for col in cols {
            let val = self.generate_for_column(col)?;
            record.insert(col.clone(), val);
        }

        Ok(record)
    }
}
