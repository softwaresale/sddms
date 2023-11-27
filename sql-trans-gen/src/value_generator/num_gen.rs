use std::ops::Range;
use rand::{Rng, thread_rng};
use rand::distributions::uniform::SampleUniform;
use rusqlite::types::Value;
use sddms_shared::error::SddmsError;
use crate::config::{IntegerGenRule, RealGenRule};
use crate::value_generator::ValueGenerator;

struct NumberGenerator<ReprT>
    where ReprT: Clone + PartialEq + PartialOrd + SampleUniform
{
    range: Range<ReprT>
}

impl<ReprT> NumberGenerator<ReprT>
    where ReprT: Clone + PartialEq + PartialOrd + SampleUniform
{
    fn new(min: ReprT, max: ReprT) -> Self {
        Self {
            range: min..max
        }
    }

    fn gen(&self) -> ReprT {
        let mut rng = thread_rng();
        rng.gen_range(self.range.clone())
    }
}

pub struct IntegerGenerator {
    gen: NumberGenerator<i64>
}

impl IntegerGenerator {
    pub fn new(min: i64, max: i64) -> Self {
        Self {
            gen: NumberGenerator::new(min, max)
        }
    }
}

impl From<IntegerGenRule> for IntegerGenerator {
    fn from(value: IntegerGenRule) -> Self {
        Self::new(value.min, value.max)
    }
}

impl ValueGenerator for IntegerGenerator {
    fn generate(&self) -> Result<Value, SddmsError> {
        let value = self.gen.gen();
        Ok(Value::Integer(value))
    }
}

pub struct FloatGenerator {
    gen: NumberGenerator<f64>
}

impl FloatGenerator {
    pub fn new(min: f64, max: f64) -> Self {
        Self {
            gen: NumberGenerator::new(min, max)
        }
    }
}

impl From<RealGenRule> for FloatGenerator {
    fn from(value: RealGenRule) -> Self {
        Self::new(value.min, value.max)
    }
}

impl ValueGenerator for FloatGenerator {
    fn generate(&self) -> Result<Value, SddmsError> {
        let value = self.gen.gen();
        Ok(Value::Real(value))
    }
}
