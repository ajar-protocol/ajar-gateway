//! Policy Engine module.
//!
//! The Policy Engine is a pure decision function over an injected request
//! context and policy document. It performs no I/O, clock reads, storage reads,
//! network calls, signature operations, logging writes, mutations, rendering,
//! or LLM calls. Startup code injects all inputs before evaluation so decisions
//! remain unit-testable in isolation.

#![forbid(unsafe_code)]

use thiserror::Error;

/// Errors raised by policy evaluation.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum PolicyEngineError {
    /// The policy document could not be evaluated and access must be denied.
    #[error("policy document invalid")]
    InvalidPolicy,
}

/// Minimal policy verdict placeholder for future T1.x work.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PolicyVerdict {
    /// The request is allowed by the injected policy inputs.
    Allow,
    /// The request is denied by the injected policy inputs.
    Deny,
}
