//! Store module.
//!
//! The Store owns access to prepared artifacts, audit events, receipts,
//! policies, and freshness metadata. It is the center-facing storage boundary
//! named by `AGENTS.md`: serving code reads prepared stores, while concrete
//! persistence adapters live at startup edges. The Store does not harvest,
//! crawl, render, induce, cluster, draft, sign, or make policy decisions.

#![forbid(unsafe_code)]

use thiserror::Error;

/// Errors raised by storage backends.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum StoreError {
    /// The requested prepared artifact is absent.
    #[error("record not found")]
    NotFound,
    /// The backend is unavailable and callers must fail closed.
    #[error("storage backend unavailable")]
    BackendUnavailable,
    /// The backend rejected malformed persisted data.
    #[error("stored record invalid")]
    RecordInvalid,
}

/// Stable identifier for a stored prepared artifact.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StoreKey {
    /// Namespace for the stored record family.
    pub namespace: String,
    /// Stable record key inside the namespace.
    pub key: String,
}

/// Extension interface for storage backends.
///
/// Variation point: `ENGINEERING.md` extensibility rule 35 requires interface
/// documentation to cite the document section naming the variation. `AGENTS.md`
/// "Extension interfaces exist from day one" names storage backends for
/// prepared artifacts, audit events, receipts, policies, and freshness metadata.
///
/// Ownership: the Store owns this trait. Stability: pre-1.0 and additive.
/// Error behavior: backends must fail closed on unavailable or invalid storage.
/// Conformance: backends must return the exact bytes stored for a stable key
/// and must not perform network fan-out from request-serving reads.
pub trait StorageBackend: Send + Sync {
    /// Reads a prepared artifact by stable key.
    fn read(&self, key: &StoreKey) -> Result<Vec<u8>, StoreError>;

    /// Writes a prepared artifact by stable key.
    fn write(&self, key: &StoreKey, value: &[u8]) -> Result<(), StoreError>;
}

#[cfg(test)]
mod tests {
    use super::{StorageBackend, StoreError, StoreKey};
    use std::collections::BTreeMap;
    use std::sync::Mutex;

    struct TestStorageBackend {
        records: Mutex<BTreeMap<(String, String), Vec<u8>>>,
    }

    impl TestStorageBackend {
        fn new() -> Self {
            Self {
                records: Mutex::new(BTreeMap::new()),
            }
        }
    }

    impl StorageBackend for TestStorageBackend {
        fn read(&self, key: &StoreKey) -> Result<Vec<u8>, StoreError> {
            let records = self
                .records
                .lock()
                .map_err(|_| StoreError::BackendUnavailable)?;
            records
                .get(&(key.namespace.clone(), key.key.clone()))
                .cloned()
                .ok_or(StoreError::NotFound)
        }

        fn write(&self, key: &StoreKey, value: &[u8]) -> Result<(), StoreError> {
            let mut records = self
                .records
                .lock()
                .map_err(|_| StoreError::BackendUnavailable)?;
            records.insert((key.namespace.clone(), key.key.clone()), value.to_vec());
            Ok(())
        }
    }

    #[test]
    fn test_double_round_trips_bytes() -> Result<(), StoreError> {
        let backend = TestStorageBackend::new();
        let key = StoreKey {
            namespace: "prepared".to_owned(),
            key: "home".to_owned(),
        };

        backend.write(&key, b"view")?;
        let value = backend.read(&key)?;

        assert_eq!(value, b"view");
        Ok(())
    }
}
