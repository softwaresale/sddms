use std::error::Error;
use std::fmt::{Display, Formatter};
use tarpc::derive_serde;

#[derive(Debug)]
pub enum SddmsErrorCategory {
    Client,
    Site,
    General,
    Central,
}

impl Display for SddmsErrorCategory {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            SddmsErrorCategory::Client => f.write_str("Client Error"),
            SddmsErrorCategory::Site => f.write_str("Site Error"),
            SddmsErrorCategory::Central => f.write_str("Central Error"),
            SddmsErrorCategory::General => f.write_str("General Error"),
        }
    }
}

#[derive(Debug)]
pub struct SddmsError {
    /// the category of error
    category: SddmsErrorCategory,
    /// a message associated with this error
    message: String,
    /// an optional error that caused this one
    cause: Option<Box<dyn Error>>,
}

impl SddmsError {
    fn new<MsgT: Into<String>>(category: SddmsErrorCategory, message: MsgT) -> Self {
        Self {
            category,
            message: message.into(),
            cause: None
        }
    }

    pub fn client<MsgT: Into<String>>(msg: MsgT) -> Self {
        Self::new(SddmsErrorCategory::Client, msg)
    }

    pub fn site<MsgT: Into<String>>(msg: MsgT) -> Self {
        Self::new(SddmsErrorCategory::Site, msg)
    }
    
    pub fn general<MsgT: Into<String>>(msg: MsgT) -> Self {
        Self::new(SddmsErrorCategory::General, msg)
    }

    pub fn central<MsgT: Into<String>>(msg: MsgT) -> Self {
        Self::new(SddmsErrorCategory::Central, msg)
    }

    pub fn with_cause<B: Into<Box<dyn Error>>>(mut self, cause: B) -> Self {
        self.cause = Some(cause.into());
        self
    }


    pub fn category(&self) -> &SddmsErrorCategory {
        &self.category
    }
    pub fn message(&self) -> &str {
        &self.message
    }
    pub fn inner_cause(&self) -> &Option<Box<dyn Error>> {
        &self.cause
    }
}

impl Display for SddmsError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("{}: {}", self.category, self.message))?;
        if let Some(cause) = &self.cause {
            f.write_fmt(format_args!("\ncaused by: {}", cause))?;
        }

        Ok(())
    }
}

impl Error for SddmsError {}

pub type SddmsResult<T> = Result<T, SddmsError>;

#[derive(Debug)]
pub struct SddmsTermError {
    /// the category of error
    category: SddmsErrorCategory,
    /// a message associated with this error
    message: String,
}

impl From<SddmsError> for SddmsTermError {
    fn from(value: SddmsError) -> Self {
        let message = format!("{}", &value);
        Self {
            category: value.category,
            message,
        }
    }
}

impl Display for SddmsTermError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("{}: {}", self.category, self.message))
    }
}

impl Error for SddmsTermError {}
