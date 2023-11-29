use std::collections::{HashMap, HashSet};
use crate::verify::conflict_type::{ConflictEdge, ConflictType, ConflictVector};
use crate::history_file_parser::action::ActionKind;
use crate::organize::AssociatedActionMap;
use crate::transaction_id::TransactionId;

pub struct ConflictGraph<'action> {
    /// Maps a transaction to the node id
    node_ids: HashMap<TransactionId, usize>,
    /// the actual graph
    graph: Vec<Vec<ConflictVector<'action>>>,
}

impl<'action> ConflictGraph<'action> {
    pub fn new(transaction_ids: Vec<TransactionId>) -> Self {
        let transaction_ids_len = transaction_ids.len();
        let id_map = transaction_ids.into_iter().enumerate()
            .map(|(idx, id)| (id, idx))
            .collect::<HashMap<_, _>>();

        assert_eq!(id_map.len(), transaction_ids_len);

        let inner = vec![Vec::default(); id_map.len()];
        let outer = vec![inner; id_map.len()];

        Self {
            node_ids: id_map,
            graph: outer
        }
    }

    fn add_edge(&mut self, causing: TransactionId, conflicting: TransactionId, edge: ConflictType<'action>) {
        let cause_id = self.node_ids.get(&causing).unwrap();
        let conflict_id = self.node_ids.get(&conflicting).unwrap();

        self.graph
            .get_mut(*cause_id).unwrap()
            .get_mut(*conflict_id).unwrap()
            .push(edge);
    }

    pub fn get_conflict_vec(&self, causing: &TransactionId, conflicting: &TransactionId) -> Option<&ConflictVector<'action>> {
        let Some(causing_idx) = self.node_ids.get(causing) else {
            return None;
        };
        let Some(conflicting_idx) = self.node_ids.get(conflicting) else {
            return None;
        };

        let Some(row) = self.graph.get(*causing_idx) else {
            return None;
        };

        row.get(*conflicting_idx)
    }

    pub fn build(mut self, actions_map: &'action AssociatedActionMap) -> Self {
        let mut transaction_ids: Vec<TransactionId> = self.node_ids.iter()
            .map(|(id, _)| id)
            .cloned()
            .collect();

        transaction_ids.sort();

        for outer_transaction_id in transaction_ids {
            // get the instructions found in the range of this transaction
            let transaction_range = actions_map.get_transaction_range(outer_transaction_id);

            let mut range_iter = transaction_range.into_iter();
            'outer: while let Some(outer_action) = range_iter.next() {

                // Only look at actions in this range from this transaction
                let outer_action_txn_id = TransactionId::from(outer_action);
                if outer_transaction_id != outer_action_txn_id {
                    continue;
                }

                // get the information about this action
                let ActionKind::Query { read_set: outer_read_set, write_set: outer_write_set } = &outer_action.action else {
                    continue 'outer;
                };

                'inner: for inner_action in range_iter.clone() {
                    let inner_transaction_id = TransactionId::from(inner_action);

                    if inner_transaction_id == outer_transaction_id {
                        // don't permit self-edges
                        continue;
                    }

                    let ActionKind::Query { read_set: inner_read_set, write_set: inner_write_set } = &inner_action.action else {
                        continue 'inner;
                    };

                    //
                    // check each overlap
                    //

                    let write_after_read_tables = outer_read_set.intersection(inner_write_set).collect::<HashSet<_>>();
                    if !write_after_read_tables.is_empty() {
                        let edge = ConflictType::ReadWrite(ConflictEdge::new(outer_action, inner_action, write_after_read_tables));
                        self.add_edge(outer_transaction_id, inner_transaction_id, edge);
                    }

                    let read_after_write_tables = outer_write_set.intersection(inner_read_set).collect::<HashSet<_>>();
                    if !read_after_write_tables.is_empty() {
                        let edge = ConflictType::WriteRead(ConflictEdge::new(outer_action, inner_action, read_after_write_tables));
                        self.add_edge(outer_transaction_id, inner_transaction_id, edge);
                    }

                    let write_after_write_tables = outer_write_set.intersection(inner_write_set).collect::<HashSet<_>>();
                    if !write_after_write_tables.is_empty() {
                        let edge = ConflictType::WriteWrite(ConflictEdge::new(outer_action, inner_action, write_after_write_tables));
                        self.add_edge(outer_transaction_id, inner_transaction_id, edge);
                    }
                }
            }
        }


        self
    }

    fn dfs(
        &self,
        current: usize,
        visited: &mut Vec<bool>,
        recursion_stack: &mut Vec<bool>,
        path: &mut Vec<usize>,
        cycles: &mut Vec<Vec<usize>>,
    ) {
        if recursion_stack[current] {
            // Cycle detected
            let start_index = path.iter().position(|&x| x == current).unwrap();
            let cycle: Vec<usize> = path[start_index..].to_vec();
            cycles.push(cycle);
            return;
        }

        if !visited[current] {
            visited[current] = true;
            recursion_stack[current] = true;
            path.push(current);

            for (neighbor, conflict_vector) in self.graph[current].iter().enumerate() {
                let has_edge = !conflict_vector.is_empty();
                if has_edge {
                    self.dfs(neighbor, visited, recursion_stack, path, cycles);
                }
            }

            path.pop();
            recursion_stack[current] = false;
        }
    }

    fn find_cycles(&self) -> (Vec<Vec<usize>>, HashMap<usize, TransactionId>) {

        let mut visited = vec![false; self.graph.len()];
        let mut recursion_stack = vec![false; self.graph.len()];
        let mut cycles = Vec::new();
        let mut path = Vec::new();

        let mut reverse_lookup: HashMap<usize, TransactionId> = HashMap::new();

        for (transaction_id, transaction_id_index) in &self.node_ids {
            reverse_lookup.insert(*transaction_id_index, *transaction_id);
            if !visited[*transaction_id_index] {
                self.dfs(*transaction_id_index, &mut visited, &mut recursion_stack, &mut path, &mut cycles);
            }
        }

        (cycles, reverse_lookup)
    }

    pub fn detect_cycles(&self) -> Vec<Vec<TransactionId>> {
        let (cycles, reverse_lookup) = self.find_cycles();
        cycles.into_iter()
            .map(|cycle| cycle.into_iter()
                .map(|index| *reverse_lookup.get(&index).unwrap())
                .collect::<Vec<_>>())
            .collect::<Vec<_>>()
    }
}
