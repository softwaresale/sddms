use std::collections::{HashMap, HashSet};
use std::f64;
use rusqlite::types::Type;
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize)]
pub enum TypeSpec {
    Null,
    Integer,
    Real,
    Text,
    Blob
}

impl From<Type> for TypeSpec {
    fn from(value: Type) -> Self {
        match value {
            Type::Null => Self::Null,
            Type::Integer => Self::Integer,
            Type::Real => Self::Real,
            Type::Text => Self::Text,
            Type::Blob => Self::Blob,
        }
    }
}

impl Into<Type> for TypeSpec {
    fn into(self) -> Type {
        match self {
            TypeSpec::Null => Type::Null,
            TypeSpec::Integer => Type::Integer,
            TypeSpec::Real => Type::Real,
            TypeSpec::Text => Type::Text,
            TypeSpec::Blob => Type::Blob,
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub enum GenRule {
    Text(TextGenRule),
    Integer(IntegerGenRule),
    Real(RealGenRule),
    Blob(BlobGenRule)
}

#[derive(Debug, Deserialize, Serialize)]
pub struct TextGenRule {
    /// Minimum length, inclusive
    pub min_len: usize,
    /// maximum length, inclusive
    pub max_len: usize,
    /// A regex to use to generate values
    pub format: Option<String>,
    /// Character classes available to choose characters from
    pub available_char_classes: Option<HashSet<String>>
}

impl Default for TextGenRule {
    fn default() -> Self {
        Self {
            min_len: 3,
            max_len: 15,
            format: None,
            available_char_classes: None
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub struct IntegerGenRule {
    pub min: i64,
    pub max: i64,
}

impl Default for IntegerGenRule {
    fn default() -> Self {
        Self {
            min: 0,
            max: i64::MAX / 2,
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub struct RealGenRule {
    pub min: f64,
    pub max: f64,
}

impl Default for RealGenRule {
    fn default() -> Self {
        Self {
            min: 0f64,
            max: f64::MAX / 2f64,
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub struct BlobGenRule {
    /// Minimum length, inclusive
    min_len: usize,
    /// maximum length, inclusive
    max_len: usize,
}

impl Default for BlobGenRule {
    fn default() -> Self {
        Self {
            min_len: 0,
            max_len: 100
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub struct GenerationStrategy {
    text: Option<TextGenRule>,
    integer: Option<IntegerGenRule>,
    real: Option<RealGenRule>,
    blob: Option<BlobGenRule>,
}


#[derive(Debug, Deserialize, Serialize)]
pub struct TableConfig {
    columns: HashMap<String, GenRule>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct Config {
    pub global: GenerationStrategy,
    pub tables: HashMap<String, TableConfig>,
}
