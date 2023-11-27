mod text_gen;
mod num_gen;


use rusqlite::types::{Type, Value};
use sddms_shared::error::SddmsError;
use crate::config::TextGenRule;
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
            real: FloatGenerator::new(0f64, 100f64),
            integer: IntegerGenerator::new(0, 100),
        }
    }
}

impl ValueGeneratorMap {
    pub fn generate(&self, tp: &Type) -> Value {
        match tp {
            Type::Null => Value::Null,
            Type::Integer => self.integer.generate().unwrap(),
            Type::Real => self.integer.generate().unwrap(),
            Type::Text => self.text.generate().unwrap(),
            Type::Blob => panic!("Blob is not supported")
        }
    }
}
