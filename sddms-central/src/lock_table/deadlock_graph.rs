use std::collections::{HashMap, HashSet, VecDeque};
use crate::lock_table::resource_lock::ResourceLock;
use crate::transaction_id::TransactionId;

/// Maintains a record of dependencies for lock requests in order to prevent deadlocking
#[derive(Debug)]
pub struct DeadlockGraph<'wait_queue> {
    /// Sparse matrix of edges
    wait_graph: HashMap<&'wait_queue TransactionId, HashSet<&'wait_queue TransactionId>>,
    /// a stored copied to the queues, set after construct is called
    queues: Option<&'wait_queue HashMap<String, VecDeque<ResourceLock>>>
}

impl<'wait_queue> DeadlockGraph<'wait_queue> {

    pub fn new() -> Self {
        Self {
            wait_graph: HashMap::new(),
            queues: None
        }
    }

    fn add_transaction(&mut self, transaction: &'wait_queue TransactionId) {
        if !self.wait_graph.contains_key(transaction) {
            self.wait_graph.insert(transaction, HashSet::new());
        }
    }

    fn insert_edge(&mut self, source: &'wait_queue TransactionId, dest: &'wait_queue TransactionId) {
        // make sure that source and dest exist
        self.add_transaction(source);
        self.add_transaction(dest);
        // add the edge
        self.wait_graph.get_mut(source).unwrap().insert(dest);
    }

    pub fn construct(mut self, lock_queues: &'wait_queue HashMap<String, VecDeque<ResourceLock>>) -> Self {
        for (_, lock_queue) in lock_queues {
            let mut last_owners: Option<HashSet<&'wait_queue TransactionId>> = None;
            for lock in lock_queue {
                let lock_owners = lock.owners();
                let outgoing_edges = if let Some(previous) = last_owners {
                    previous
                } else {
                    HashSet::new()
                };

                // make transaction records for each transaction we encounter
                for lock_owner in lock_owners.union(&outgoing_edges) {
                    self.add_transaction(*lock_owner);
                }

                // Make the actual edges between everything
                for owner in &lock_owners {
                    for last_owner in &outgoing_edges {
                        self.insert_edge(*owner, *last_owner);
                    }
                }

                last_owners = Some(lock_owners);
            }
        }

        self.queues = Some(lock_queues);
        self
    }

    pub fn would_cause_deadlock(mut self, transaction_id: &'wait_queue TransactionId, resource: &str) -> bool {
        let lock_queues = self.queues.unwrap();

        let resource_queue_waiters = lock_queues.get(resource).unwrap().iter()
            .flat_map(|lock| lock.owners())
            .collect::<HashSet<&'wait_queue TransactionId>>();

        for waiter in resource_queue_waiters {
            self.insert_edge(transaction_id, waiter)
        }

        self.has_cycle()
    }

    fn detect_cycle_with_starting_point(
        &self,
        current: &'wait_queue TransactionId,
        visited: &mut HashSet<&'wait_queue TransactionId>,
        recursion_stack: &mut HashSet<&'wait_queue TransactionId>,
    ) -> bool {
        if recursion_stack.contains(&current) {
            // Cycle detected
            return true;
        }

        if !visited.contains(&current) {
            visited.insert(current);
            recursion_stack.insert(current);

            if let Some(neighbors) = self.wait_graph.get(&current) {
                for &neighbor in neighbors {
                    if self.detect_cycle_with_starting_point(neighbor, visited, recursion_stack) {
                        return true;
                    }
                }
            }

            recursion_stack.remove(&current);
        }

        false
    }

    fn has_cycle(self) -> bool {

        let mut visited = HashSet::new();
        let mut recursion_stack = HashSet::new();

        for &node in self.wait_graph.keys() {
            if !visited.contains(&node) {
                if self.detect_cycle_with_starting_point(node, &mut visited, &mut recursion_stack) {
                    return true;
                }
            }
        }

        false
    }
}
