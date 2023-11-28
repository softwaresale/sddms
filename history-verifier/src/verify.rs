mod conflict_diagnosis;

use std::collections::{HashMap, HashSet};
use std::ops::Not;
use crate::history_file_parser::{ActionKind};
use crate::organize::AssociatedActionMap;
use crate::transaction_id::{TransactionId, TransactionPair};
use crate::verify::conflict_diagnosis::ConflictDiagnosis;

#[derive(Default, Clone, Debug)]
struct TableSets {
    read_set: HashSet<String>,
    write_set: HashSet<String>
}

impl TableSets {
    fn conflicts_with_other(&self, other: &TableSets) -> bool {
        if self.read_set.intersection(&other.write_set)
            .collect::<HashSet<&String>>()
            .is_empty()
            .not() {
            // read and writes conflict
            return true;
        }

        if self.write_set.intersection(&other.read_set)
            .collect::<HashSet<&String>>()
            .is_empty()
            .not() {
            // writes and reads conflict
            return true;
        }

        if self.write_set.intersection(&other.write_set)
            .collect::<HashSet<&String>>()
            .is_empty()
            .not() {
            // writes conflict
            return true;
        }

        false
    }
}

struct LiveTransactions<'actions_lifetime> {
    live_transactions: HashMap<TransactionId, TableSets>,
    actions: &'actions_lifetime AssociatedActionMap,
}

impl<'actions_lifetime> LiveTransactions<'actions_lifetime> {
    fn new(actions: &'actions_lifetime AssociatedActionMap) -> Self {
        Self {
            live_transactions: HashMap::default(),
            actions
        }
    }

    fn register_transaction(&mut self, transaction_id: TransactionId) {
        self.live_transactions.insert(transaction_id, TableSets::default());
    }

    fn register_single_stmt_transaction(&mut self, transaction_id: TransactionId, read_set: &HashSet<String>, write_set: &HashSet<String>) {
        let table_sets = TableSets {
            read_set: read_set.clone(),
            write_set: write_set.clone(),
        };

        self.live_transactions.insert(transaction_id, table_sets);
    }

    fn unregister_transaction(&mut self, transaction_id: &TransactionId) {
        self.live_transactions.remove(&transaction_id);
    }

    fn has_transaction(&self, transaction_id: &TransactionId) -> bool {
        self.live_transactions.contains_key(transaction_id)
    }

    fn update_sets(&mut self, transaction_id: &TransactionId, read_set: &HashSet<String>, write_set: &HashSet<String>) {
        let table_set = self.live_transactions.get_mut(transaction_id).unwrap();
        for read in read_set {
            table_set.read_set.insert(read.clone());
        }

        for write in write_set {
            table_set.write_set.insert(write.clone());
        }
    }

    fn has_conflicts(&self) -> Result<(), Vec<ConflictDiagnosis<'actions_lifetime>>> {
        // make all transaction pairs
        let mut transaction_pairs: HashSet<TransactionPair> = HashSet::new();
        for (left, _) in &self.live_transactions {
            for (right, _) in &self.live_transactions {
                if left != right {
                    let pair = TransactionPair::new(*left, *right);
                    transaction_pairs.insert(pair);
                }
            }
        }

        let mut conflicts: Vec<ConflictDiagnosis<'actions_lifetime>> = Vec::new();

        // now that we have a set of unique pairs to compare against, do that
        for pair in transaction_pairs {
            let (left, right) = pair.into();
            let left_table_set = self.live_transactions.get(&left).unwrap();
            let right_table_set = self.live_transactions.get(&right).unwrap();

            // if any two has a conflict, then there's conflict
            if left_table_set.conflicts_with_other(right_table_set) {

                let left_transaction = self.actions.borrow_transaction(left.0, left.1, left.2).unwrap();
                let right_transaction = self.actions.borrow_transaction(right.0, right.1, right.2).unwrap();

                let diagnosis = ConflictDiagnosis {
                    conflicting_transactions: HashMap::from([(left, left_transaction), (right, right_transaction)]),
                    conflicting_sets: HashMap::from([(left, left_table_set.clone()), (right, right_table_set.clone())]),
                    conflict_range: self.actions.get_transactions_range(&HashSet::from([left, right]))
                };

                conflicts.push(diagnosis)
            }
        }

        if conflicts.is_empty() {
            Ok(())
        } else {
            Err(conflicts)
        }
    }
}

pub fn verify_action_history(associated_action_map: &AssociatedActionMap) -> Result<(), Vec<ConflictDiagnosis>> {

    let mut live_transactions = LiveTransactions::new(associated_action_map);

    let mut all_conflicts: Vec<ConflictDiagnosis> = Vec::new();

    for action in associated_action_map.all_actions() {
        let mut single_stmt = false;

        let transaction_id = TransactionId::from(action);
        match &action.action {
            ActionKind::BeginTransaction => {
                live_transactions.register_transaction(transaction_id);
            }
            ActionKind::CommitTransaction |
            ActionKind::RollbackTransaction => {
                live_transactions.unregister_transaction(&transaction_id);
            }
            ActionKind::Query { write_set, read_set } => {
                if !live_transactions.has_transaction(&transaction_id) {
                    live_transactions.register_single_stmt_transaction(transaction_id, read_set, write_set);
                    single_stmt = true;
                } else {
                    live_transactions.update_sets(&transaction_id, read_set, write_set);
                }
            }
        }

        // check for transactions
        match live_transactions.has_conflicts() {
            Ok(_) => {}
            Err(mut conflicts) => {
                all_conflicts.append(&mut conflicts);
            }
        }

        if single_stmt {
            live_transactions.unregister_transaction(&transaction_id);
        }
    }

    if all_conflicts.is_empty() {
        Ok(())
    } else {
        Err(all_conflicts)
    }
}
