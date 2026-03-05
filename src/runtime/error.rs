use std::fmt::{Display, Formatter};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RuntimeError {
    Message(String),
    FilterRejected,
    UnauthorizedAccess { capability: String, path: String },
    DeniedByDenyGlobs { path: String },
    RestrictedOperation { operation: String },
}

impl RuntimeError {
    pub fn message(msg: impl Into<String>) -> Self {
        Self::Message(msg.into())
    }

    pub fn unauthorized_access(capability: impl Into<String>, path: impl Into<String>) -> Self {
        Self::UnauthorizedAccess {
            capability: capability.into(),
            path: path.into(),
        }
    }

    pub fn denied_by_deny_globs(path: impl Into<String>) -> Self {
        Self::DeniedByDenyGlobs { path: path.into() }
    }

    pub fn restricted_operation(operation: impl Into<String>) -> Self {
        Self::RestrictedOperation {
            operation: operation.into(),
        }
    }

    pub fn is_security_denial(&self) -> bool {
        matches!(
            self,
            Self::UnauthorizedAccess { .. }
                | Self::DeniedByDenyGlobs { .. }
                | Self::RestrictedOperation { .. }
        )
    }
}

impl Display for RuntimeError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            RuntimeError::Message(msg) => write!(f, "{}", msg),
            RuntimeError::FilterRejected => write!(f, "Filter condition failed"),
            RuntimeError::UnauthorizedAccess { capability, path } => {
                write!(f, "Unauthorized {} access to '{}'", capability, path)
            }
            RuntimeError::DeniedByDenyGlobs { path } => {
                write!(f, "Path '{}' is denied by deny_globs", path)
            }
            RuntimeError::RestrictedOperation { operation } => {
                write!(f, "{} operation is disabled in restricted mode", operation)
            }
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
