//! Harvester module.
//!
//! The Harvester owns Tier-1 structure recovery from CMS data, database
//! adapters, sitemaps, RSS, JSON-LD, OpenGraph, and OpenAPI inputs. It runs in
//! the conversion pipeline and never in the serve path. Its outputs are
//! prepared artifacts consumed later by the Serving Layer through the store,
//! preserving the `AGENTS.md` dependency direction: serving reads prepared
//! stores and the conversion pipeline never runs during request serving.

#![forbid(unsafe_code)]

use thiserror::Error;

/// Errors raised by harvest collectors.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum HarvesterError {
    /// The collector could not read its configured source.
    #[error("collector source unavailable")]
    SourceUnavailable,
    /// The collector rejected malformed source data.
    #[error("collector source invalid")]
    SourceInvalid,
}

/// Prepared record emitted by a harvest collector.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HarvestRecord {
    /// Stable source identifier for the harvested item.
    pub source_id: String,
    /// Raw structured payload owned by the collector format.
    pub payload: Vec<u8>,
}

/// Extension interface for harvest collectors.
///
/// Variation point: `ENGINEERING.md` extensibility rule 35 requires interface
/// documentation to cite the document section naming the variation. `AGENTS.md`
/// "The Harvester" and "Extension interfaces exist from day one" sections name
/// harvest collectors as an extension interface.
///
/// Ownership: the Harvester owns this trait. Stability: pre-1.0 and additive.
/// Error behavior: collectors must fail closed on unreadable or malformed
/// source data. Conformance: collectors must emit deterministic records for the
/// same source snapshot and must not publish anything directly.
pub trait HarvestCollector: Send + Sync {
    /// Collects prepared Tier-1 records from the configured source.
    fn collect(&self) -> Result<Vec<HarvestRecord>, HarvesterError>;
}

#[cfg(test)]
mod tests {
    use super::{HarvestCollector, HarvestRecord, HarvesterError};

    struct TestHarvestCollector;

    impl HarvestCollector for TestHarvestCollector {
        fn collect(&self) -> Result<Vec<HarvestRecord>, HarvesterError> {
            Ok(vec![HarvestRecord {
                source_id: "test-source".to_owned(),
                payload: b"{}".to_vec(),
            }])
        }
    }

    #[test]
    fn test_double_collects_records() -> Result<(), HarvesterError> {
        let collector = TestHarvestCollector;

        let records = collector.collect()?;

        assert_eq!(records.len(), 1);
        assert_eq!(records[0].source_id, "test-source");
        Ok(())
    }
}
