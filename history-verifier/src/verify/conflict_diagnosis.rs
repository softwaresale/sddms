use std::collections::{HashMap, HashSet};
use std::fmt::{Display, Formatter};
use colored::{Color, Colorize};
use rand::seq::SliceRandom;
use rand::thread_rng;
use crate::history_file_parser::Action;
use crate::transaction_id::{TransactionId, TransactionPair};
use crate::verify::TableSets;

// get conflicting transactions
#[derive(Debug)]
pub struct ConflictDiagnosis<'actions_lifetime> {
    pub(super) conflicting_transactions: HashMap<TransactionId, Vec<&'actions_lifetime Action>>,
    pub(super) conflicting_sets: HashMap<TransactionId, TableSets>,
    pub(super) conflict_range: &'actions_lifetime [Action],
}

fn choose_random_colors<ValueT>(transactions: &HashMap<TransactionId, ValueT>) -> HashMap<TransactionId, Color> {
    let mut rng = thread_rng();
    let colors = vec![
        Color::Blue,
        Color::BrightGreen,
        Color::BrightMagenta,
        Color::Yellow,
    ];

    let sample = colors.choose_multiple(&mut rng, transactions.len()).collect::<Vec<_>>();

    transactions.keys()
        .zip(sample)
        .map(|(left, right)| (left.clone(), right.clone()))
        .collect::<HashMap<_, _>>()
}

fn format_conflicts(f: &mut Formatter<'_>, conflicting_sets: &HashMap<TransactionId, TableSets>, color_map: &HashMap<TransactionId, Color>) -> std::fmt::Result {
    // TODO refactor this into standalone method
    let mut transaction_pairs: HashSet<TransactionPair> = HashSet::new();
    for (left, _) in conflicting_sets {
        for (right, _) in conflicting_sets {
            if left != right {
                let pair = TransactionPair::new(*left, *right);
                transaction_pairs.insert(pair);
            }
        }
    }

    // now that we have a set of unique pairs to compare against, do that
    for pair in transaction_pairs {
        let (left, right) = pair.into();
        let left_color = color_map.get(&left).cloned().unwrap_or(Color::White);
        let right_color = color_map.get(&right).cloned().unwrap_or(Color::White);
        let left_set = conflicting_sets.get(&left).unwrap();
        let right_set = conflicting_sets.get(&right).unwrap();

        // check for read/write conflicts
        for intersecting_table in left_set.read_set.intersection(&right_set.write_set) {
            let left_msg = format!("{} reads {}", left, intersecting_table);
            let right_msg = format!("{} writes {}", right, intersecting_table);

            writeln!(f, "{} {} and {}", "read-write".black().on_bright_yellow(), left_msg.color(left_color.clone()), right_msg.color(right_color.clone()))?;
        }

        for intersecting_table in left_set.write_set.intersection(&right_set.read_set) {
            let left_msg = format!("{} writes {}", left, intersecting_table);
            let right_msg = format!("{} reads {}", right, intersecting_table);

            writeln!(f, "{} {} and {}", "read-write".black().on_bright_yellow(), left_msg.color(left_color.clone()), right_msg.color(right_color.clone()))?;
        }

        for intersecting_table in left_set.write_set.intersection(&right_set.write_set) {
            let left_msg = format!("{} writes {}", left, intersecting_table);
            let right_msg = format!("{} writes {}", right, intersecting_table);

            writeln!(f, "{} {} and {}", "write-write".black().on_bright_red(), left_msg.color(left_color.clone()), right_msg.color(right_color.clone()))?;
        }
    }

    Ok(())
}

impl<'actions_lifetime> Display for ConflictDiagnosis<'actions_lifetime> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {

        let color_map = choose_random_colors(&self.conflicting_transactions);

        for (id, stmts) in &self.conflicting_transactions {
            writeln!(f, "Transaction {}:", id)?;
            for stmt in stmts {
                let line_format = format!("{}", stmt);
                writeln!(f, "{}", line_format.color(color_map.get(id).unwrap().clone()))?;
            }
            writeln!(f, "")?;
        }

        writeln!(f, "Conflicting tables:")?;
        format_conflicts(f, &self.conflicting_sets, &color_map)?;
        writeln!(f, "")?;

        writeln!(f, "Conflicting range:")?;
        for action in self.conflict_range {
            let action_fmt = format!("{}", action);
            let action_trans_id = TransactionId::from(action);
            let color = color_map.get(&action_trans_id).cloned()
                .unwrap_or(Color::White);

            writeln!(f, "{}", action_fmt.color(color))?;
        }

        Ok(())
    }
}
