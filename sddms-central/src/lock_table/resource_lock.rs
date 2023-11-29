use std::collections::HashSet;
use sddms_services::shared::LockMode;
use crate::transaction_id::TransactionId;

pub enum AcquireLockMode {
    /// the resource currently has a shared lock, but the given transaction can make it exclusive
    CanPromoteToExclusive,
    /// the transaction has the lock with the given lock mode already
    HasLock,
    /// The transaction cannot acquire this lock, which either means it does not have the lock,
    /// or it cannot promote a shared lock into an exclusive lock
    CannotAcquire,
}

#[derive(Debug)]
pub enum ResourceLock {
    Shared {
        owners: HashSet<TransactionId>,
        order: Vec<TransactionId>
    },
    Exclusive {
        owner: TransactionId
    }
}

impl ResourceLock {
    pub fn shared(id: TransactionId) -> Self {
        Self::Shared {
            owners: HashSet::from([id]),
            order: vec![id]
        }
    }

    pub fn exclusive(id: TransactionId) -> Self {
        Self::Exclusive {
            owner: id
        }
    }

    pub fn is_shared(&self) -> bool {
        match self {
            ResourceLock::Shared { .. } => true,
            _ => false
        }
    }

    pub fn is_exclusive(&self) -> bool {
        match self {
            ResourceLock::Exclusive { .. } => true,
            _ => false,
        }
    }

    /// We can easily join two shared locks. Joins the current lock as the left lock with other as
    /// the right lock. The order between the two is preserved
    fn join_two_shared(self, other: Self) -> (Self, Option<Self>) {
        let Self::Shared { owners: mut self_owners, order: mut self_order } = self else {
            panic!("Self is not shared")
        };

        let Self::Shared { owners: other_owners, order: mut other_order } = other else {
            panic!("Other is not shared")
        };

        for owner in other_owners {
            self_owners.insert(owner);
        }

        self_order.append(&mut other_order);

        (Self::Shared { owners: self_owners, order: self_order }, None)
    }

    /// Try upgrading the left lock into an exclusive lock if the right lock is an exclusive lock
    /// request for one of the transactions holding the shared lock on the left.
    ///
    /// For now, this optimization will only work if the shared lock is first locked by the trailing
    /// request
    fn try_upgrade_enqueued_lock(self, other: Self) -> (Self, Option<Self>) {
        let Self::Exclusive { owner } = other else {
            panic!("Other is not exclusive");
        };

        // the shared lock can be split
        if self.is_first_locked_by(&owner) {
            self.to_exclusive(&owner)
        } else {
            (self, Some(other))
        }
    }

    /// Try join join self with another lock. Self is always on the left while other is always on
    /// the right
    pub fn try_join_with(self, other: Self) -> (Self, Option<Self>) {
        // fold two shared resource locks into each other
        if self.is_shared() && other.is_shared() {
            self.join_two_shared(other)
        } else if self.is_shared() && other.is_exclusive() {
            self.try_upgrade_enqueued_lock(other)
        } else {
            (self, Some(other))
        }
    }

    pub fn to_exclusive(self, owner: &TransactionId) -> (Self, Option<Self>) {
        match self {
            ResourceLock::Shared {
                mut owners,
                mut order
            } => {
                owners.remove(&owner);
                let remove_idx = order.iter().position(|tid| tid == owner).unwrap();
                order.remove(remove_idx);

                let right = if !owners.is_empty() {
                    Some(Self::Shared { order, owners })
                } else {
                    None
                };

                (Self::Exclusive { owner: *owner }, right)
            }
            ResourceLock::Exclusive { owner } => {
                (ResourceLock::Exclusive {owner}, None)
            }
        }
    }

    pub fn is_locked_by(&self, id: &TransactionId) -> bool {
        match self {
            ResourceLock::Shared { owners, .. } => {
                owners.contains(id)
            }
            ResourceLock::Exclusive { owner } => {
                owner == id
            }
        }
    }

    pub fn is_locked_by_shared(&self, id: &TransactionId) -> bool {
        match self {
            ResourceLock::Shared { owners, .. } => owners.contains(id),
            _ => false,
        }
    }

    pub fn is_locked_by_exclusive(&self, id: &TransactionId) -> bool {
        match self {
            ResourceLock::Exclusive { owner } => id == owner,
            _ => false,
        }
    }

    pub fn is_first_locked_by(&self, id: &TransactionId) -> bool {
        match self {
            ResourceLock::Shared { order, .. } => {
                order.first().unwrap().eq(id)
            }
            ResourceLock::Exclusive { owner } => {
                owner == id
            }
        }
    }

    pub fn has_or_can_acquire_lock(&self, requesting_trans_id: &TransactionId, mode: LockMode) -> AcquireLockMode {
        if self.is_locked_by(requesting_trans_id) {
            match mode {
                LockMode::Unspecified => {panic!("Can't be unspecified")}
                LockMode::Exclusive => {
                    if self.is_locked_by_shared(requesting_trans_id) {
                        AcquireLockMode::CanPromoteToExclusive
                    } else {
                        AcquireLockMode::CannotAcquire
                    }
                }
                LockMode::Shared => {
                    AcquireLockMode::HasLock
                }
            }
        } else {
            AcquireLockMode::CannotAcquire
        }
    }

    pub fn owners(&self) -> HashSet<&TransactionId> {
        match self {
            ResourceLock::Shared { owners, .. } => owners.iter().collect::<HashSet<_>>(),
            ResourceLock::Exclusive { owner } => HashSet::from([owner])
        }
    }
}
