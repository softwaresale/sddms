use sddms_shared::error::SddmsError;

#[derive(Debug)]
pub struct TransactionState {
    current: Option<u32>,
}

impl TransactionState {
    pub fn new() -> Self {
        Self {
            current: None
        }
    }

    pub fn push(&mut self, trans_id: u32) -> Result<(), SddmsError> {
        match &self.current {
            None => {
                self.current = Some(trans_id);
                Ok(())
            }
            Some(existing) => {
                Err(SddmsError::client(format!("Transaction already in progress with id {}", existing)))
            }
        }
    }

    pub fn has_transaction(&self) -> bool {
        self.current.is_some()
    }

    pub fn transaction_id(&self) -> Result<u32, SddmsError> {
        self.current.ok_or(SddmsError::client("No transaction is in progress"))
    }

    pub fn clear(&mut self) {
        self.current = None;
    }
}
