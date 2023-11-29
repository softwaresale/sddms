use std::collections::{Bound, HashMap, HashSet};
use std::ops::{RangeBounds};
use rand::seq::SliceRandom;
use rand::thread_rng;
use crate::history_file_parser::action::Action;
use crate::transaction_id::TransactionId;

type TransactionMap = HashMap<u32, Vec<usize>>;
type ClientMap = HashMap<u32, TransactionMap>;
type SiteMap = HashMap<u32, ClientMap>;

/// Associates a set of actions together
#[derive(Debug)]
pub struct AssociatedActionMap {
    actions: Vec<Action>,
    site_map: SiteMap,
    chrono_sorted_order: Vec<usize>,
}

impl AssociatedActionMap {
    pub fn new() -> Self {
        Self {
            actions: Vec::new(),
            site_map: SiteMap::default(),
            chrono_sorted_order: Vec::new(),
        }
    }

    pub fn build(mut self, actions: Vec<Action>) -> Self {
        for (idx, action) in actions.iter().enumerate() {
            self.get_or_add_transaction_mut(action.site_id, action.client_id, action.transaction_id).push(idx);
        }

        self.actions = actions;

        self
    }

    pub fn make_random_transaction_order(&self) -> Vec<Vec<&Action>> {
        // make the coordinate space of all transactions
        let mut coordinates: Vec<(u32, u32, u32)> = Vec::new();
        for (site_id, client_map) in &self.site_map {
            for (client_id, transaction_map) in client_map {
                for (trans_id, _) in transaction_map {
                    coordinates.push((*site_id, *client_id, *trans_id));
                }
            }
        }

        let mut rng = thread_rng();

        coordinates.shuffle(&mut rng);

        let mut order: Vec<Vec<&Action>> = Vec::new();
        for (site, client, trans) in coordinates {
            let transaction = self.borrow_transaction_coordinates(site, client, trans);
            if let Some(transaction) = transaction {
                order.push(transaction);
            }
        }

        order
    }

    pub fn all_actions(&self) -> &[Action] {
        &self.actions
    }

    pub fn borrow_transaction(&self, transaction_id: &TransactionId) -> Option<Vec<&Action>> {
        self.borrow_transaction_coordinates(transaction_id.0, transaction_id.1, transaction_id.2)
    }

    pub fn borrow_transaction_coordinates(&self, site_id: u32, client_id: u32, transaction_id: u32) -> Option<Vec<&Action>> {
        if let Some(indices) = self.get_transaction_indices(site_id, client_id, transaction_id) {
            let transaction_vec = indices.into_iter()
                .map(|index| self.actions.get(*index).unwrap())
                .collect::<Vec<_>>();

            Some(transaction_vec)
        } else {
            None
        }
    }

    pub fn get_transaction_range(&self, transaction_id: TransactionId) -> &[Action] {
        self.get_transactions_range(&HashSet::from([transaction_id]))
    }

    pub fn get_transactions_range(&self, transactions: &HashSet<TransactionId>) -> &[Action] {
        let mut min = usize::MAX;
        let mut max = 0usize;
        for transaction_id in transactions {
            let ranges = self.get_transaction_indices(transaction_id.0, transaction_id.1, transaction_id.2).unwrap();
            // update smallest
            let smallest = ranges.iter().min().unwrap();
            if smallest < &min {
                min = *smallest;
            }

            // update largest
            let largest = ranges.iter().max().unwrap();
            if largest > &max {
                max = *largest;
            }
        }

        self.get_action_range(min..=max)
    }

    fn get_action_range<RangeT: RangeBounds<usize>>(&self, range: RangeT) -> &[Action] {

        let lower = match range.start_bound() {
            Bound::Included(lower) => *lower,
            Bound::Excluded(lower) => lower + 1,
            Bound::Unbounded => 0usize
        };

        let upper = match range.end_bound() {
            Bound::Included(included) => *included,
            Bound::Excluded(excluded) => *excluded - 1,
            Bound::Unbounded => self.actions.len() - 1,
        };

        &self.actions[lower..=upper]
    }

    pub fn get_all_transaction_ids(&self) -> Vec<TransactionId> {
        let mut vec: Vec<TransactionId> = Vec::new();
        for (site_id, client_map) in &self.site_map {
            for (client_id, transaction_map) in client_map {
                for (transaction_id, _) in transaction_map {
                    vec.push(TransactionId(*site_id, *client_id, *transaction_id));
                }
            }
        }

        vec.sort();
        vec
    }

    /// Get the other transactions that are running concurrently with the given transaction
    pub fn get_concurrent_transactions(&self, transaction_id: &TransactionId) -> HashSet<TransactionId> {
        self.get_transactions_range(&HashSet::from([*transaction_id])).into_iter()
            .map(|action| TransactionId::from(action))
            .filter(|txn_id| txn_id != transaction_id)
            .collect()
    }

    fn get_transaction_indices(&self, site_id: u32, client_id: u32, transaction_id: u32) -> Option<&Vec<usize>> {
        self.site_map.get(&site_id)
            .and_then(|client_map| client_map.get(&client_id))
            .and_then(|transaction_map| transaction_map.get(&transaction_id))
    }

    fn get_or_add_site_mut(&mut self, site_id: u32) -> &mut ClientMap {
        if !self.site_map.contains_key(&site_id) {
            self.site_map.insert(site_id, ClientMap::default());
        }

        self.site_map.get_mut(&site_id).unwrap()
    }

    fn get_or_add_client_mut(&mut self, site_id: u32, client_id: u32) -> &mut TransactionMap {
        let client_map = self.get_or_add_site_mut(site_id);
        if !client_map.contains_key(&client_id) {
            client_map.insert(client_id, TransactionMap::default());
        }

        client_map.get_mut(&client_id).unwrap()
    }

    fn get_or_add_transaction_mut(&mut self, site_id: u32, client_id: u32, trans_id: u32) -> &mut Vec<usize> {
        let transaction_map = self.get_or_add_client_mut(site_id, client_id);
        if !transaction_map.contains_key(&trans_id) {
            transaction_map.insert(trans_id, Vec::default());
        }

        transaction_map.get_mut(&trans_id).unwrap()
    }
}
