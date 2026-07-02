//! Store module.
//!
//! The Store owns access to prepared artifacts, audit events, receipts,
//! policies, and freshness metadata. It is the center-facing storage boundary
//! named by `AGENTS.md`: serving code reads prepared stores, while concrete
//! persistence adapters live at startup edges. The Store does not harvest,
//! crawl, render, induce, cluster, draft, sign, or make policy decisions.

#![forbid(unsafe_code)]

use serde::Deserialize;
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};
use thiserror::Error;

const MAX_MANIFEST_LIFETIME_SECONDS: i64 = 180 * 24 * 60 * 60;

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

/// Filesystem implementation of the storage backend extension interface.
#[derive(Clone, Debug)]
pub struct FilesystemStorageBackend {
    root: PathBuf,
}

impl FilesystemStorageBackend {
    /// Creates a backend rooted at a prepared content store directory.
    pub fn new(root: PathBuf) -> Self {
        Self { root }
    }

    fn path_for(&self, key: &StoreKey) -> PathBuf {
        self.root.join(&key.namespace).join(&key.key)
    }
}

impl StorageBackend for FilesystemStorageBackend {
    fn read(&self, key: &StoreKey) -> Result<Vec<u8>, StoreError> {
        fs::read(self.path_for(key)).map_err(|error| {
            if error.kind() == std::io::ErrorKind::NotFound {
                StoreError::NotFound
            } else {
                StoreError::BackendUnavailable
            }
        })
    }

    fn write(&self, key: &StoreKey, value: &[u8]) -> Result<(), StoreError> {
        let path = self.path_for(key);
        let parent = path.parent().ok_or(StoreError::RecordInvalid)?;
        fs::create_dir_all(parent).map_err(|_| StoreError::BackendUnavailable)?;
        fs::write(path, value).map_err(|_| StoreError::BackendUnavailable)
    }
}

/// Clock boundary for startup and runtime freshness checks.
pub trait Clock: Send + Sync {
    /// Returns seconds since the Unix epoch.
    fn now_unix_seconds(&self) -> i64;
}

/// Production wall-clock implementation.
#[derive(Clone, Debug)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now_unix_seconds(&self) -> i64 {
        match SystemTime::now().duration_since(UNIX_EPOCH) {
            Ok(duration) => i64::try_from(duration.as_secs()).unwrap_or(i64::MAX),
            Err(_) => 0,
        }
    }
}

/// Startup errors for a prepared content store.
#[derive(Debug, Error)]
pub enum StoreLoadError {
    /// Required prepared artifacts could not be read.
    #[error("prepared content store read failed")]
    ReadFailed(#[source] std::io::Error),
    /// The storage backend could not return a required prepared artifact.
    #[error("prepared content store read failed")]
    BackendReadFailed(#[source] StoreError),
    /// A JSON artifact could not be parsed.
    #[error("prepared content store JSON malformed")]
    JsonMalformed,
    /// A required manifest field is missing.
    #[error("prepared manifest required field missing")]
    ManifestFieldMissing,
    /// A required view field is missing.
    #[error("prepared view required field missing")]
    ViewFieldMissing,
    /// Manifest timestamps are invalid.
    #[error("prepared manifest time window invalid")]
    ManifestTimeInvalid,
    /// Manifest lifetime exceeds the protocol cap.
    #[error("prepared manifest lifetime exceeds 180 days")]
    ManifestLifetimeTooLong,
    /// Manifest is already expired.
    #[error("prepared manifest expired")]
    ManifestExpired,
    /// Manifest sequence is invalid.
    #[error("prepared manifest sequence invalid")]
    ManifestSequenceInvalid,
    /// More than one view resolves to the same request URL.
    #[error("prepared view URL duplicated")]
    DuplicateViewUrl,
    /// A configured view URL cannot be served by path and query.
    #[error("prepared view URL invalid")]
    InvalidViewUrl,
}

/// In-memory prepared content store used by the Serving Layer.
#[derive(Clone, Debug)]
pub struct PreparedContentStore {
    manifest: StoredManifest,
    view_index: StoredArtifact,
    views_by_request_target: BTreeMap<String, StoredView>,
}

impl PreparedContentStore {
    /// Loads all prepared artifacts from disk once at startup.
    pub fn load_from_dir(path: &Path, clock: &dyn Clock) -> Result<Self, StoreLoadError> {
        let backend = FilesystemStorageBackend::new(path.to_path_buf());
        let manifest_bytes = read_artifact(
            &backend,
            StoreKey {
                namespace: ".".to_owned(),
                key: "manifest.json".to_owned(),
            },
        )?;
        let manifest_json: Value =
            serde_json::from_slice(&manifest_bytes).map_err(|_| StoreLoadError::JsonMalformed)?;
        validate_manifest_required_fields(&manifest_json)?;
        let manifest: Manifest =
            serde_json::from_value(manifest_json).map_err(|_| StoreLoadError::JsonMalformed)?;
        validate_manifest_lifecycle(&manifest, clock.now_unix_seconds())?;

        let index_bytes = read_artifact(
            &backend,
            StoreKey {
                namespace: ".".to_owned(),
                key: "view-index.json".to_owned(),
            },
        )?;
        let _index_json: Value =
            serde_json::from_slice(&index_bytes).map_err(|_| StoreLoadError::JsonMalformed)?;
        let view_index_etag = strong_etag(b"view-index", &index_bytes, None);
        let view_index = StoredArtifact {
            bytes: index_bytes,
            etag: view_index_etag,
        };

        let views_dir = path.join("views");
        let mut view_paths = json_files(&views_dir)?;
        view_paths.sort();

        let mut views_by_request_target = BTreeMap::new();
        let mut seen_urls = BTreeSet::new();
        for view_path in view_paths {
            let key = view_path
                .file_name()
                .and_then(|name| name.to_str())
                .ok_or(StoreLoadError::InvalidViewUrl)?
                .to_owned();
            let bytes = read_artifact(
                &backend,
                StoreKey {
                    namespace: "views".to_owned(),
                    key,
                },
            )?;
            let view_json: Value =
                serde_json::from_slice(&bytes).map_err(|_| StoreLoadError::JsonMalformed)?;
            validate_view_required_fields(&view_json)?;
            let view: View =
                serde_json::from_value(view_json).map_err(|_| StoreLoadError::JsonMalformed)?;
            let request_target = request_target_from_view_url(&view.url)?;
            if !seen_urls.insert(request_target.clone()) {
                return Err(StoreLoadError::DuplicateViewUrl);
            }
            views_by_request_target.insert(request_target, StoredView { bytes, view });
        }

        let manifest_etag = strong_etag(b"manifest", &manifest_bytes, None);
        Ok(Self {
            view_index,
            views_by_request_target,
            manifest: StoredManifest {
                bytes: manifest_bytes,
                etag: manifest_etag,
                expires_at_unix: parse_rfc3339_utc(&manifest.expires_at)?,
                views_index: manifest.views.index.clone(),
                manifest,
            },
        })
    }

    /// Returns the stored manifest with exact original bytes.
    pub fn manifest(&self) -> &StoredManifest {
        &self.manifest
    }

    /// Returns the stored view index with exact original bytes.
    pub fn view_index(&self) -> &StoredArtifact {
        &self.view_index
    }

    /// Returns a stored view by request path and query.
    pub fn view_for_request_target(&self, request_target: &str) -> Option<&StoredView> {
        self.views_by_request_target.get(request_target)
    }
}

/// Exact prepared artifact bytes plus a strong entity tag.
#[derive(Clone, Debug)]
pub struct StoredArtifact {
    bytes: Vec<u8>,
    etag: String,
}

impl StoredArtifact {
    /// Exact bytes as persisted by the conversion pipeline.
    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }

    /// Strong HTTP entity tag for the stored bytes.
    pub fn etag(&self) -> &str {
        &self.etag
    }
}

/// Stored manifest metadata used by the Serving Layer.
#[derive(Clone, Debug)]
pub struct StoredManifest {
    bytes: Vec<u8>,
    etag: String,
    expires_at_unix: i64,
    views_index: String,
    manifest: Manifest,
}

impl StoredManifest {
    /// Exact bytes as persisted by the signing pipeline.
    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }

    /// Strong HTTP entity tag for the manifest bytes.
    pub fn etag(&self) -> &str {
        &self.etag
    }

    /// Expiry in Unix seconds.
    pub fn expires_at_unix(&self) -> i64 {
        self.expires_at_unix
    }

    /// Manifest view-index URL.
    pub fn views_index(&self) -> &str {
        &self.views_index
    }

    /// Typed manifest subset.
    pub fn manifest(&self) -> &Manifest {
        &self.manifest
    }
}

/// Stored view metadata used by content negotiation.
#[derive(Clone, Debug)]
pub struct StoredView {
    bytes: Vec<u8>,
    view: View,
}

impl StoredView {
    /// Exact bytes as persisted by the signing pipeline.
    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }

    /// Typed view subset.
    pub fn view(&self) -> &View {
        &self.view
    }
}

/// Typed manifest subset required by the Gateway serve path.
#[derive(Clone, Debug, Deserialize)]
pub struct Manifest {
    /// Protocol version.
    pub ajar_version: Value,
    /// Declared profiles.
    pub profiles: Value,
    /// Site identity.
    pub site: Value,
    /// Published keys.
    pub keys: Value,
    /// View configuration.
    pub views: ManifestViews,
    /// Manifest issuance timestamp.
    pub issued_at: String,
    /// Manifest expiry timestamp.
    pub expires_at: String,
    /// Monotonic manifest sequence.
    pub sequence: i64,
    /// Embedded manifest signature.
    pub signature: Signature,
}

/// Typed manifest view configuration.
#[derive(Clone, Debug, Deserialize)]
pub struct ManifestViews {
    /// Machine sitemap URL.
    pub index: String,
}

/// Typed view subset required by the Gateway serve path.
#[derive(Clone, Debug, Deserialize)]
pub struct View {
    /// Content URL represented by the view.
    pub url: String,
    /// Strong view-level entity tag.
    pub etag: String,
    /// Semantic chunks in canonical order.
    pub chunks: Vec<ViewChunk>,
    /// Embedded content signature.
    pub signature: Signature,
}

/// Typed view chunk subset required for markdown rendering.
#[derive(Clone, Debug, Deserialize)]
pub struct ViewChunk {
    /// Ajar chunk type.
    #[serde(rename = "type")]
    pub chunk_type: String,
    /// Chunk content.
    pub content: String,
}

/// Embedded Ajar signature subset.
#[derive(Clone, Debug, Deserialize)]
pub struct Signature {
    /// Signature value.
    pub sig: String,
}

fn validate_manifest_required_fields(value: &Value) -> Result<(), StoreLoadError> {
    let Some(object) = value.as_object() else {
        return Err(StoreLoadError::JsonMalformed);
    };
    for field in [
        "ajar_version",
        "profiles",
        "site",
        "keys",
        "views",
        "issued_at",
        "expires_at",
        "sequence",
        "signature",
    ] {
        if !object.contains_key(field) {
            return Err(StoreLoadError::ManifestFieldMissing);
        }
    }
    Ok(())
}

fn validate_view_required_fields(value: &Value) -> Result<(), StoreLoadError> {
    let Some(object) = value.as_object() else {
        return Err(StoreLoadError::JsonMalformed);
    };
    for field in ["url", "etag", "chunks", "signature"] {
        if !object.contains_key(field) {
            return Err(StoreLoadError::ViewFieldMissing);
        }
    }
    Ok(())
}

fn validate_manifest_lifecycle(manifest: &Manifest, now: i64) -> Result<(), StoreLoadError> {
    if manifest.sequence < 0 {
        return Err(StoreLoadError::ManifestSequenceInvalid);
    }
    let issued_at = parse_rfc3339_utc(&manifest.issued_at)?;
    let expires_at = parse_rfc3339_utc(&manifest.expires_at)?;
    if expires_at <= issued_at {
        return Err(StoreLoadError::ManifestTimeInvalid);
    }
    if expires_at - issued_at > MAX_MANIFEST_LIFETIME_SECONDS {
        return Err(StoreLoadError::ManifestLifetimeTooLong);
    }
    if expires_at <= now {
        return Err(StoreLoadError::ManifestExpired);
    }
    Ok(())
}

fn json_files(path: &Path) -> Result<Vec<PathBuf>, StoreLoadError> {
    let mut paths = Vec::new();
    for entry in fs::read_dir(path).map_err(StoreLoadError::ReadFailed)? {
        let entry = entry.map_err(StoreLoadError::ReadFailed)?;
        let path = entry.path();
        if path.extension().and_then(|value| value.to_str()) == Some("json") {
            paths.push(path);
        }
    }
    Ok(paths)
}

fn read_artifact(backend: &dyn StorageBackend, key: StoreKey) -> Result<Vec<u8>, StoreLoadError> {
    backend
        .read(&key)
        .map_err(StoreLoadError::BackendReadFailed)
}

fn request_target_from_view_url(url: &str) -> Result<String, StoreLoadError> {
    if url.is_empty() {
        return Err(StoreLoadError::InvalidViewUrl);
    }
    if url.starts_with('/') {
        return Ok(url.to_owned());
    }
    if let Some(after_scheme) = url
        .strip_prefix("http://")
        .or_else(|| url.strip_prefix("https://"))
    {
        let path_start = after_scheme.find('/').unwrap_or(after_scheme.len());
        if path_start == after_scheme.len() {
            return Ok("/".to_owned());
        }
        return Ok(after_scheme[path_start..].to_owned());
    }
    Err(StoreLoadError::InvalidViewUrl)
}

fn parse_rfc3339_utc(value: &str) -> Result<i64, StoreLoadError> {
    if value.len() != 20 || !value.ends_with('Z') {
        return Err(StoreLoadError::ManifestTimeInvalid);
    }
    let bytes = value.as_bytes();
    let year = parse_digits(&bytes[0..4])?;
    require(bytes.get(4), b'-')?;
    let month = parse_digits(&bytes[5..7])?;
    require(bytes.get(7), b'-')?;
    let day = parse_digits(&bytes[8..10])?;
    require(bytes.get(10), b'T')?;
    let hour = parse_digits(&bytes[11..13])?;
    require(bytes.get(13), b':')?;
    let minute = parse_digits(&bytes[14..16])?;
    require(bytes.get(16), b':')?;
    let second = parse_digits(&bytes[17..19])?;
    if !(1..=12).contains(&month)
        || !(1..=31).contains(&day)
        || !(0..=23).contains(&hour)
        || !(0..=59).contains(&minute)
        || !(0..=59).contains(&second)
    {
        return Err(StoreLoadError::ManifestTimeInvalid);
    }
    Ok(days_from_civil(year, month, day) * 86_400
        + i64::from(hour) * 3_600
        + i64::from(minute) * 60
        + i64::from(second))
}

fn parse_digits(bytes: &[u8]) -> Result<i32, StoreLoadError> {
    let mut value = 0_i32;
    for byte in bytes {
        if !byte.is_ascii_digit() {
            return Err(StoreLoadError::ManifestTimeInvalid);
        }
        value = value * 10 + i32::from(*byte - b'0');
    }
    Ok(value)
}

fn require(value: Option<&u8>, expected: u8) -> Result<(), StoreLoadError> {
    if value == Some(&expected) {
        Ok(())
    } else {
        Err(StoreLoadError::ManifestTimeInvalid)
    }
}

fn days_from_civil(year: i32, month: i32, day: i32) -> i64 {
    let adjusted_year = year - if month <= 2 { 1 } else { 0 };
    let era = if adjusted_year >= 0 {
        adjusted_year
    } else {
        adjusted_year - 399
    } / 400;
    let year_of_era = adjusted_year - era * 400;
    let month_prime = month + if month > 2 { -3 } else { 9 };
    let day_of_year = (153 * month_prime + 2) / 5 + day - 1;
    let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
    i64::from(era * 146_097 + day_of_era - 719_468)
}

fn strong_etag(prefix: &[u8], bytes: &[u8], extra: Option<&[u8]>) -> String {
    let mut hash = 14_695_981_039_346_656_037_u64;
    for byte in prefix
        .iter()
        .chain(bytes.iter())
        .chain(extra.unwrap_or(&[]).iter())
    {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(1_099_511_628_211);
    }
    format!("\"{:016x}-{}\"", hash, bytes.len())
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
