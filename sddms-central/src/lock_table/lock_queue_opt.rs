use std::collections::VecDeque;
use crate::lock_table::resource_lock::ResourceLock;

fn optimize_lock_queue_pass(mut lock_queue: VecDeque<ResourceLock>) -> VecDeque<ResourceLock> {
    let mut new_lock_queue: VecDeque<ResourceLock> = VecDeque::new();
    while !lock_queue.is_empty() {
        // pop front two
        let left = lock_queue.pop_front().unwrap();
        let right = lock_queue.pop_front();

        let (new_left, new_right) = if right.is_some() {
            let right = right.unwrap();
            left.try_join_with(right)

        } else {
            (left, None)
        };

        new_lock_queue.push_back(new_left);
        if new_right.is_some() {
            // if there is a right value, we didn't fold these, so put it back into the original
            lock_queue.push_front(new_right.unwrap());
        }
    }

    new_lock_queue
}

pub fn optimize_lock_queue(lock_queue: VecDeque<ResourceLock>) -> VecDeque<ResourceLock> {
    let mut last = lock_queue;
    loop {
        let last_len = last.len();
        last = optimize_lock_queue_pass(last);
        if last_len == last.len() {
            break;
        }
    }

    last
}
