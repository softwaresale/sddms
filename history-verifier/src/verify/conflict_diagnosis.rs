use std::collections::{HashMap, HashSet};
use std::fmt;
use std::fmt::{Display, Formatter};

use colored::{Color, Colorize};
use rand::seq::SliceRandom;
use rand::thread_rng;
use crate::history_file_parser::action::Action;
use crate::organize::AssociatedActionMap;
use crate::transaction_id::{TransactionId};
use crate::verify::conflict_graph::ConflictGraph;
use crate::verify::conflict_type::{ConflictType, ConflictVector};

fn choose_random_colors(transactions: &HashSet<TransactionId>) -> HashMap<TransactionId, Color> {
    let mut rng = thread_rng();
    let colors = vec![
        Color::Blue,
        Color::BrightGreen,
        Color::BrightMagenta,
        Color::Yellow,
    ];

    let sample = colors.choose_multiple(&mut rng, transactions.len()).collect::<Vec<_>>();

    transactions.into_iter()
        .zip(sample)
        .map(|(left, right)| (left.clone(), right.clone()))
        .collect::<HashMap<_, _>>()
}

pub struct ConflictDiagnosis<'action> {
    /// Flat set of transactions in conflict
    conflicting_transactions: HashSet<TransactionId>,
    /// the cycle of issues that cause the issue
    conflict_sequence: Vec<(TransactionId, TransactionId, ConflictVector<'action>)>,
    /// the range of actions involved with this conflict
    conflict_range: &'action [Action],
}

impl<'action> ConflictDiagnosis<'action> {
    pub fn new(mut path: Vec<TransactionId>, conflict_graph: &ConflictGraph<'action>, associated_action_map: &'action AssociatedActionMap) -> Self {

        // duplicate first item in path to end to complete cycle
        path.push(*path.first().unwrap());

        let conflicting_transactions = path.iter()
            .cloned()
            .collect::<HashSet<_>>();

        let mut sequence: Vec<(TransactionId, TransactionId, ConflictVector<'action>)> = Vec::new();

        let mut iter = path.into_iter().peekable();
        loop {
            let Some(left_transaction) = iter.next() else {
                break;
            };

            let Some(right_transaction) = iter.peek() else {
                break;
            };

            let conflict_vec = conflict_graph.get_conflict_vec(&left_transaction, right_transaction)
                .expect("Conflict vector between transactions in diagnosis should not be empty");

            sequence.push((left_transaction, *right_transaction, conflict_vec.clone()));
        }

        let range = associated_action_map.get_transactions_range(&conflicting_transactions);

        Self {
            conflicting_transactions,
            conflict_sequence: sequence,
            conflict_range: range,
        }
    }
}

fn format_conflicts<ColorGetterT>(f: &mut Formatter<'_>, conflict_vector: &ConflictVector, color_getter: ColorGetterT) -> fmt::Result
    where ColorGetterT: Fn(&TransactionId) -> Color
{
    for conflict in conflict_vector {
        let (msg, edge) = match conflict {
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

        let causing_txn_id = TransactionId::from(edge.causing_action);
        let conflicted_txn_id = TransactionId::from(edge.conflicted_action);

        writeln!(f, "{}", msg)?;
        writeln!(f, "{}", edge.causing_action.to_string().color(color_getter(&causing_txn_id)))?;
        writeln!(f, "conflicts with")?;
        writeln!(f, "{}", edge.conflicted_action.to_string().color(color_getter(&conflicted_txn_id)))?;
        writeln!(f, "over tables")?;
        writeln!(f, "{:?}", edge.conflicting_tables)?;
    }

    Ok(())
}

impl<'action> Display for ConflictDiagnosis<'action> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {

        let color_map = choose_random_colors(&self.conflicting_transactions);

        let color_map_getter = |txn_id: &TransactionId| color_map.get(txn_id).cloned().unwrap_or(Color::White);
        let color_txn_id = |txn_id: &TransactionId| txn_id.to_string().color(color_map_getter(txn_id));

        writeln!(f, "Conflict Error:")?;
        let conflicting_transactions_set_string = self.conflicting_transactions.iter()
            .map(|trans| format!("{trans}").color(color_map_getter(trans)).to_string())
            .collect::<Vec<_>>()
            .join(",");

        writeln!(f, "Transactions {{ {} }} are in conflict", conflicting_transactions_set_string)?;

        for (left, right, conflict_vector) in &self.conflict_sequence {
            writeln!(f, "{} ~> {} in the following {} way(s)", color_txn_id(left), color_txn_id(right), conflict_vector.len())?;
            format_conflicts(f, conflict_vector, color_map_getter)?;
        }

        writeln!(f, "Conflicts over range:")?;

        for action in self.conflict_range {
            let txn_id = TransactionId::from(action);
            let colored_string = format!("{}", action).color(color_map_getter(&txn_id));
            writeln!(f, "{}", colored_string)?;
        }

        Ok(())
    }
}
