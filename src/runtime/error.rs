use std::fmt::{Display, Formatter};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RuntimeError {
    Message(String),
    FilterRejected,
}

impl RuntimeError {
    pub fn message(msg: impl Into<String>) -> Self {
        Self::Message(msg.into())
    }
}

impl Display for RuntimeError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            RuntimeError::Message(msg) => write!(f, "{}", msg),
            RuntimeError::FilterRejected => write!(f, "Filter condition failed"),
        }
    }
}

impl std::error::Error for RuntimeError {}

impl From<String> for RuntimeError {
    fn from(value: String) -> Self {
        RuntimeError::Message(value)
    }
}

impl From<&str> for RuntimeError {
    fn from(value: &str) -> Self {
        RuntimeError::Message(value.to_string())
    }
}

pub type RuntimeResult<T> = Result<T, RuntimeError>;
