use std::collections::HashSet;
use std::fmt::{Display, Formatter, write};
use colored::Colorize;
use crate::history_file_parser::action::Action;

#[derive(Clone, PartialEq)]
pub struct ConflictEdge<'action> {
    /// The action of the transaction causing the conflict
    pub causing_action: &'action Action,
    /// The action of the transaction that's "in conflict"
    pub conflicted_action: &'action Action,
    /// which edges are actually in conflict
    pub conflicting_tables: HashSet<&'action String>,
}

impl<'action> ConflictEdge<'action> {
    pub fn new(causing_edge: &'action Action, conflict_edge: &'action Action, conflicting_tables: HashSet<&'action String>) -> Self {
        Self {
            causing_action: causing_edge,
            conflicted_action: conflict_edge,
            conflicting_tables
        }
    }

    pub fn causing_action(&self) -> &'action Action {
        self.causing_action
    }

    pub fn conflicted_action(&self) -> &'action Action {
        self.conflicted_action
    }

    pub fn conflicting_tables(&self) -> &HashSet<&'action String> {
        &self.conflicting_tables
    }
}

impl<'action> Display for ConflictEdge<'action> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} conflicts with {} on tables {:?}", self.causing_action, self.conflicted_action, self.conflicting_tables)
    }
}

#[derive(Clone, PartialEq)]
pub enum ConflictType<'action> {
    ReadWrite(ConflictEdge<'action>),
    WriteRead(ConflictEdge<'action>),
    WriteWrite(ConflictEdge<'action>),
}

impl<'action> Display for ConflictType<'action> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let (msg, edge) = match self {
            ConflictType::ReadWrite(edge) => {
                ("Read-Write conflict".black().on_bright_yellow(), edge)
            }
            ConflictType::WriteRead(edge) => {
                ("Write-Read conflict".black().on_yellow(), edge)
            }
            ConflictType::WriteWrite(edge) => {
                ("Write-Write conflict".white().on_red(), edge)
            }
        };

        write!(f, "{}\n{}", msg, edge)
    }
}

pub type ConflictVector<'action> = Vec<ConflictType<'action>>;
