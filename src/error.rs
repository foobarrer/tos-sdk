use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter};

/// A stable, machine-readable SDK error.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct AppError {
    /// Stable error category suitable for programmatic matching.
    pub code: String,
    /// Safe user-facing explanation.
    pub message: String,
    /// Whether retrying the same operation may succeed.
    pub retryable: bool,
    /// Optional redacted diagnostics.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub details: Option<String>,
}

impl AppError {
    /// Creates a non-retryable error.
    pub fn new(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            code: code.into(),
            message: message.into(),
            retryable: false,
            details: None,
        }
    }

    /// Creates a retryable error.
    pub fn retryable(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            code: code.into(),
            message: message.into(),
            retryable: true,
            details: None,
        }
    }

    /// Attaches redacted diagnostics.
    pub fn with_details(mut self, details: impl Into<String>) -> Self {
        self.details = Some(redact(&details.into()));
        self
    }
}

impl Display for AppError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{}", self.message)
    }
}

impl std::error::Error for AppError {}

pub(crate) fn redact(input: &str) -> String {
    let mut result = input.to_string();
    for marker in ["secret", "token", "password", "credential"] {
        if result.to_ascii_lowercase().contains(marker) {
            result = "Sensitive provider error was redacted".into();
            break;
        }
    }
    result
}

impl From<std::io::Error> for AppError {
    fn from(value: std::io::Error) -> Self {
        Self::new("io_error", "The local file operation failed").with_details(value.to_string())
    }
}

impl From<serde_json::Error> for AppError {
    fn from(value: serde_json::Error) -> Self {
        Self::new("serialization_error", "Data serialization failed")
            .with_details(value.to_string())
    }
}
