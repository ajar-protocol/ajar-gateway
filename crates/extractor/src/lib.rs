//! Extractor module.
//!
//! The Extractor owns Tier-2 deterministic extraction: crawl-once recovery,
//! boilerplate stripping, HTML-to-semantic-markdown transforms, template
//! clustering, and deterministic rule application. It is part of the conversion
//! pipeline and never runs in the request-serving path. Extraction rules are
//! data validated before entering the Store, not untrusted executable code.

#![forbid(unsafe_code)]

use thiserror::Error;

/// Errors raised by deterministic extraction.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum ExtractorError {
    /// The input document was malformed or unsupported.
    #[error("extractor input invalid")]
    InvalidInput,
    /// A deterministic extraction rule failed validation.
    #[error("extraction rule invalid")]
    InvalidRule,
}
