use std::collections::Bound;
use std::f64;
use std::ops::{RangeBounds, RangeInclusive};
use rand::{Rng, thread_rng};
use rand::distributions::uniform::SampleUniform;
use rusqlite::types::Value;
use sddms_shared::error::SddmsError;
use crate::config::{IntegerGenRule, RealGenRule};
use crate::value_generator::ValueGenerator;

#[derive(Clone)]
struct NumberGenerator<ReprT>
    where ReprT: Clone + PartialEq + PartialOrd + SampleUniform
{
    range: RangeInclusive<ReprT>
}

impl<ReprT> NumberGenerator<ReprT>
    where ReprT: Clone + PartialEq + PartialOrd + SampleUniform
{
    fn new(min: ReprT, max: ReprT) -> Self {
        Self {
            range: min..=max
        }
    }

    fn gen(&self) -> ReprT {
        let mut rng = thread_rng();
        rng.gen_range(self.range.clone())
    }
}

#[derive(Clone)]
pub struct IntegerGenerator {
    gen: NumberGenerator<i64>
}

impl IntegerGenerator {
    pub fn new<RangeT: RangeBounds<i64>>(range: RangeT) -> Self {

        let min = match range.start_bound() {
            Bound::Included(inc) => *inc,
            Bound::Excluded(exc) => *exc + 1,
            Bound::Unbounded => i64::MIN
        };

        let max = match range.end_bound() {
            Bound::Included(inc) => *inc,
            Bound::Excluded(exc) => *exc + 1,
            Bound::Unbounded => i64::MIN
        };

        Self {
            gen: NumberGenerator::new(min, max)
        }
    }
}

impl Default for IntegerGenerator {
    fn default() -> Self {
        Self::new(..)
    }
}

impl From<IntegerGenRule> for IntegerGenerator {
    fn from(value: IntegerGenRule) -> Self {
        Self::new(value.min..=value.max)
    }
}

impl ValueGenerator for IntegerGenerator {
    fn generate(&self) -> Result<Value, SddmsError> {
        let value = self.gen.gen();
        Ok(Value::Integer(value))
    }
}

#[derive(Clone)]
pub struct FloatGenerator {
    gen: NumberGenerator<f64>
}

impl FloatGenerator {
    pub fn new<RangeT: RangeBounds<f64>>(range: RangeT) -> Self {

        let min = match range.start_bound() {
            Bound::Included(inc) => *inc,
            Bound::Excluded(exc) => exc + 1f64,
            Bound::Unbounded => f64::MIN
        };

        let max = match range.end_bound() {
            Bound::Included(inc) => *inc,
            Bound::Excluded(exc) => *exc + 1f64,
            Bound::Unbounded => f64::MIN
        };

        Self {
            gen: NumberGenerator::new(min, max)
        }
    }
}

impl Default for FloatGenerator {
    fn default() -> Self {
        Self::new(..)
    }
}

impl From<RealGenRule> for FloatGenerator {
    fn from(value: RealGenRule) -> Self {
        Self::new(value.min..value.max)
    }
}

impl ValueGenerator for FloatGenerator {
    fn generate(&self) -> Result<Value, SddmsError> {
        let value = self.gen.gen();
        Ok(Value::Real(value))
    }
}
