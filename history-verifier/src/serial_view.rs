use crate::history_file_parser::action::Action;
use crate::organize::AssociatedActionMap;

pub struct SerialView<'actions> {
    serial_view: Vec<&'actions Action>,
}

impl<'actions> SerialView<'actions> {
    pub fn new() -> Self {
        Self {
            serial_view: Vec::default()
        }
    }

    pub fn build(mut self, associated_action_map: &'actions AssociatedActionMap) -> Self {
        let transaction_ids = associated_action_map.get_all_transaction_ids();

        for trans_id in transaction_ids {
            let mut transaction_actions = associated_action_map.borrow_transaction(&trans_id).unwrap();
            self.serial_view.append(&mut transaction_actions);
        }

        self
    }
}
