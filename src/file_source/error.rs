use std::fmt;

/// Error type for file source operations.
#[derive(Debug)]
pub enum FileSourceError {
    /// The file or directory was not found.
    NotFound,
    /// An I/O error occurred.
    Io(std::io::Error),
    /// A custom error message.
    Other(String),
}

impl fmt::Display for FileSourceError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FileSourceError::NotFound => write!(f, "not found"),
            FileSourceError::Io(e) => write!(f, "I/O error: {}", e),
            FileSourceError::Other(msg) => write!(f, "{}", msg),
        }
    }
}

impl std::error::Error for FileSourceError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            FileSourceError::Io(e) => Some(e),
            _ => None,
        }
    }
}

impl From<std::io::Error> for FileSourceError {
    fn from(e: std::io::Error) -> Self {
        if e.kind() == std::io::ErrorKind::NotFound {
            FileSourceError::NotFound
        } else {
            FileSourceError::Io(e)
        }
    }
}

pub type Result<T> = std::result::Result<T, FileSourceError>;
