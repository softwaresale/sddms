use std::collections::HashSet;
use std::ops::Range;
use rand::{Rng, thread_rng};
use rand::distributions::Alphanumeric;
use rand_regex::{Error, Regex};
use rusqlite::types::Value;
use sddms_shared::error::SddmsError;
use crate::config::TextGenRule;
use crate::value_generator::ValueGenerator;

pub struct TextValueGenerator
{
    pattern: Alphanumeric,
    length_range: Range<usize>,
}

impl TextValueGenerator
{
    fn build_charsets_regex(char_sets: HashSet<String>, min: usize, max: usize) -> Result<Regex, Error> {
        let char_classes = char_sets.into_iter()
            .map(|cc| format!("({})", cc))
            .collect::<Vec<_>>();
        let inter_pattern = char_classes.join("|");
        let pattern = format!("({}){{{},{}}}", inter_pattern, min, max);

        Regex::compile(&pattern, 5)
    }

    fn build_default_regex(min: usize, max: usize) -> Result<Regex, Error> {
        let char_sets = HashSet::from([String::from(r"\w")]);
        Self::build_charsets_regex(char_sets, min, max)
    }

    #[cfg(unused)]
    pub fn new_regex(config: TextGenRule) -> Result<Self, SddmsError> {
        let pattern = if let Some(pattern) = config.format {
            Regex::compile(&pattern, 5)
                .map_err(|err| SddmsError::general("Failed to compile pattern").with_cause(err))
        } else if let Some(classes) = config.available_char_classes {
            Self::build_charsets_regex(classes, config.min_len, config.max_len)
                .map_err(|err| SddmsError::general("Failed to compile pattern").with_cause(err))
        } else {
            Self::build_default_regex(config.min_len, config.max_len)
                .map_err(|err| SddmsError::general("Failed to compile pattern").with_cause(err))
        }?;

        Ok(Self {
            pattern,
            length_range: config.min_len..config.max_len,
        })
    }

    pub fn new_random(config: TextGenRule) -> Self {
        Self {
            pattern: Alphanumeric,
            length_range: config.min_len..config.max_len
        }
    }
}

impl ValueGenerator for TextValueGenerator {
    fn generate(&self) -> Result<Value, SddmsError> {
        let mut rng = thread_rng();
        let len = rng.gen_range(self.length_range.clone());
        let random_string: String = (0..len)
            .map(|_| rng.sample(self.pattern) as char)
            .collect();
        Ok(Value::Text(random_string))
    }
}
