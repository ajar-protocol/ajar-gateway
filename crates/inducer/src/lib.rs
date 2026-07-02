//! Inducer module.
//!
//! The Inducer owns Tier-3 build-time LLM assist for labeling sample pages,
//! drafting deterministic extraction rules, and drafting manifest text. It is
//! not reachable from request-serving code, does not publish anything, and
//! emits data that must be validated before entering the Store. Nothing
//! model-generated is executable.

#![forbid(unsafe_code)]

use thiserror::Error;

/// Errors raised by build-time induction.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum InducerError {
    /// A configured induction provider is unavailable.
    #[error("induction provider unavailable")]
    ProviderUnavailable,
    /// Generated data failed schema or conformance validation.
    #[error("induced artifact invalid")]
    InvalidArtifact,
}
