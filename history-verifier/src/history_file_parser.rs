pub mod action;

use std::collections::HashSet;
use std::io::BufRead;
use log::warn;
use regex::{Regex, RegexSet};
use time::{OffsetDateTime};
use time::format_description::well_known::Iso8601;
use crate::history_file_parser::action::{Action, ActionKind};

pub struct ActionParser<LineSourceT: BufRead> {
    reader: LineSourceT,
    line_identifier: RegexSet,
    action_identifier: RegexSet,
}

impl<LineSourceT: BufRead> ActionParser<LineSourceT> {
    pub fn new<CreateT: Into<LineSourceT>>(inner: CreateT) -> Self {
        let regex_set = RegexSet::new([
            r"^([^|]+) \| site=(\d+), client=(\d+), txn=(\d+): (.*)$",
            r"^([^|]+) \| replication: orig_site=(\d+): (.*)$"
        ])
            .unwrap();

        let action_kind_identifier = RegexSet::new([
            r"Begin Txn",
            r"ROLLBACK",
            r"COMMIT",
            r"(Read\(([^)]+)\))?,?(Write\(([^)]+)\))?",
        ]).unwrap();

        Self {
            reader: inner.into(),
            line_identifier: regex_set,
            action_identifier: action_kind_identifier
        }
    }

    fn parse_action_kind(&self, str: &str) -> ActionKind {
        let matching_index = self.action_identifier.matches(str).iter()
            .next().unwrap();

        match matching_index {
            0 => ActionKind::BeginTransaction,
            1 => ActionKind::RollbackTransaction,
            2 => ActionKind::CommitTransaction,
            3 => {
                let query_matcher = self.action_identifier.patterns().get(3).unwrap();
                let query_regex = Regex::new(query_matcher).unwrap();
                let captures_info = query_regex.captures(str).unwrap();
                let read_set = if let Some(read_set) = captures_info.get(2) {
                    serde_json::from_str::<HashSet<String>>(read_set.as_str()).unwrap()
                } else {
                    HashSet::default()
                };

                let write_set = if let Some(write_set) = captures_info.get(4) {
                    serde_json::from_str::<HashSet<String>>(write_set.as_str()).unwrap()
                } else {
                    HashSet::default()
                };

                ActionKind::Query { read_set, write_set }
            }
            _ => unreachable!()
        }
    }

    pub fn parse_next(&mut self) -> Option<Action> {
        loop {
            let mut line = String::new();
            let result = self.reader.read_line(&mut line);

            // on error or zero bytes, return
            if result.is_err() || result.is_ok_and(|byte_count| byte_count == 0) {
                break None;
            }

            let trimmed_line = line.trim();
            if trimmed_line.is_empty() {
                continue;
            }

            let match_result = self.line_identifier.matches(trimmed_line);
            let matching_index = match_result.iter().next();

            if let Some(matching_index) = matching_index {
                match matching_index {
                    0 => {
                        let info_extractor_pattern = self.line_identifier.patterns().get(0).unwrap();
                        let info_extractor = Regex::new(info_extractor_pattern).unwrap();
                        let captures = info_extractor.captures(trimmed_line).unwrap();
                        let timestamp_str = captures.get(1).unwrap().as_str().trim();
                        let format = Iso8601::DATE_TIME_OFFSET;
                        let Ok(instant) = OffsetDateTime::parse(timestamp_str, &format) else {
                            // not great
                            warn!("Skipping line '{}' due to bad timestamp", trimmed_line);
                            continue;
                        };

                        let site_id = captures.get(2).unwrap()
                            .as_str()
                            .parse::<u32>().unwrap();
                        let client_id = captures.get(3).unwrap().as_str().parse::<u32>().unwrap();
                        let transaction_id = captures.get(4).unwrap().as_str().parse::<u32>().unwrap();
                        let action_kind = self.parse_action_kind(captures.get(5).unwrap().as_str());

                        break Some(Action{ instant, site_id, client_id, transaction_id, action: action_kind })
                    }
                    1 => { /* skip replication lines */ }
                    _ => unreachable!()
                }
            } else {
                warn!("Skipping line '{}' because it was ill-formed", trimmed_line)
            }
            // just in case?
            line.clear();
        }
    }
}
