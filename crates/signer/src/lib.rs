//! Signer module.
//!
//! The Signer is the only Gateway module allowed to touch private keys. All
//! signing, key lookup, key rotation, revocation publication, and future
//! canonicalization flow through Signer-owned interfaces. Other Gateway modules
//! request signatures through these traits and never receive raw private key
//! bytes. This preserves the dependency rules in `AGENTS.md`: core Gateway
//! logic depends on interfaces, concrete key stores live at startup edges, and
//! key material does not cross module boundaries.

#![forbid(unsafe_code)]

use thiserror::Error;

/// Errors raised by Signer interfaces.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum SignerError {
    /// The requested key identifier is unknown or not available to the store.
    #[error("key not found")]
    KeyNotFound,
    /// The key store rejected the operation and signing must fail closed.
    #[error("key store unavailable")]
    KeyStoreUnavailable,
    /// The signing backend could not produce a signature.
    #[error("signature failed")]
    SignatureFailed,
}

/// Public metadata for a signing key without exposing private material.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SigningKeyHandle {
    /// Stable key identifier used by protocol artifacts.
    pub key_id: String,
}

/// Extension interface for key stores.
///
/// Variation point: `ENGINEERING.md` extensibility rule 35 requires interface
/// documentation to cite the document section naming the variation. `AGENTS.md`
/// "Extension interfaces exist from day one" and "The Signer" sections name
/// key stores, including OS keystore and HSM implementations, as extension
/// interfaces.
///
/// Ownership: the Signer owns this trait. Stability: pre-1.0 and additive.
/// Error behavior: implementations must fail closed and never expose raw
/// private key bytes. Conformance: stores must resolve keys by stable id and
/// deny unknown, revoked, or unavailable keys.
pub trait KeyStore: Send + Sync {
    /// Resolves a signing key handle by stable key id.
    fn resolve_key(&self, key_id: &str) -> Result<SigningKeyHandle, SignerError>;
}

/// Extension interface for signing operations.
///
/// Variation point: `ENGINEERING.md` extensibility rule 35 requires interface
/// documentation to cite the document section naming the variation. `AGENTS.md`
/// "The Signer" section requires all other modules to request signatures
/// through the Signer interface.
///
/// Ownership: the Signer owns this trait. Stability: pre-1.0 and additive.
/// Error behavior: implementations must fail closed on canonicalization, key
/// resolution, and signing failures. Conformance: signatures must bind the
/// requested key id to the exact message bytes supplied by the caller.
pub trait SignatureProvider: Send + Sync {
    /// Signs the supplied canonical message bytes with the requested key.
    fn sign(&self, key_id: &str, message: &[u8]) -> Result<Vec<u8>, SignerError>;
}

#[cfg(test)]
mod tests {
    use super::{KeyStore, SignatureProvider, SignerError, SigningKeyHandle};

    struct TestKeyStore {
        key_id: String,
    }

    impl KeyStore for TestKeyStore {
        fn resolve_key(&self, key_id: &str) -> Result<SigningKeyHandle, SignerError> {
            if key_id == self.key_id {
                Ok(SigningKeyHandle {
                    key_id: key_id.to_owned(),
                })
            } else {
                Err(SignerError::KeyNotFound)
            }
        }
    }

    struct TestSignatureProvider;

    impl SignatureProvider for TestSignatureProvider {
        fn sign(&self, key_id: &str, message: &[u8]) -> Result<Vec<u8>, SignerError> {
            let mut signature = key_id.as_bytes().to_vec();
            signature.extend_from_slice(b":");
            signature.extend_from_slice(message);
            Ok(signature)
        }
    }

    #[test]
    fn test_double_resolves_configured_key() -> Result<(), SignerError> {
        let store = TestKeyStore {
            key_id: "test-key".to_owned(),
        };

        let key = store.resolve_key("test-key")?;

        assert_eq!(key.key_id, "test-key");
        Ok(())
    }

    #[test]
    fn test_double_signs_without_private_key_material() -> Result<(), SignerError> {
        let signer = TestSignatureProvider;

        let signature = signer.sign("test-key", b"message")?;

        assert_eq!(signature, b"test-key:message");
        Ok(())
    }
}
