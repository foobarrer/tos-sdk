use serde::{Deserialize, Serialize};
use std::fmt::{Debug, Formatter};

/// Client connection and transport settings.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConnectionProfile {
    /// Caller-defined identifier included in request observations.
    pub id: String,
    /// Caller-defined display name used in safe error messages.
    pub name: String,
    /// Endpoint whose scheme controls HTTP versus HTTPS. Native TOS hosts are derived from region.
    pub endpoint: String,
    /// Default TOS region, for example `cn-beijing`.
    pub region: String,
    /// Accept invalid server certificates. Keep disabled outside controlled development environments.
    pub allow_invalid_certificates: bool,
    /// Optional PEM certificate added to the native trust roots.
    pub custom_ca_path: Option<String>,
    /// Optional HTTP(S) proxy URL.
    pub proxy_url: Option<String>,
    /// Overall request timeout in seconds, clamped to 5 through 600.
    pub timeout_seconds: u64,
    /// Credential source: `stored`, `profile`, or `environment`.
    pub credential_source: String,
    /// AWS-compatible named profile used only to resolve credentials.
    pub aws_profile: Option<String>,
}

impl ConnectionProfile {
    /// Creates a configuration for static credentials and the supplied region.
    pub fn new(region: impl Into<String>) -> Self {
        let region = region.into();
        Self {
            id: "tos-client".into(),
            name: "TOS".into(),
            endpoint: format!("https://tos-{region}.volces.com"),
            region,
            allow_invalid_certificates: false,
            custom_ca_path: None,
            proxy_url: None,
            timeout_seconds: 30,
            credential_source: "stored".into(),
            aws_profile: None,
        }
    }

    /// Selects credentials from process environment and the standard credential chain.
    pub fn with_environment_credentials(mut self) -> Self {
        self.credential_source = "environment".into();
        self.aws_profile = None;
        self
    }

    /// Selects a named AWS-compatible credential profile without using an AWS S3 client.
    pub fn with_profile_credentials(mut self, profile: impl Into<String>) -> Self {
        self.credential_source = "profile".into();
        self.aws_profile = Some(profile.into());
        self
    }
}

/// Static access credentials.
#[derive(Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct SecretBundle {
    /// Access-key identifier.
    pub access_key: String,
    /// Secret access key.
    pub secret_key: String,
    /// Optional temporary-session token.
    pub session_token: Option<String>,
}

impl Debug for SecretBundle {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("Credentials")
            .field("access_key", &"<redacted>")
            .field("secret_key", &"<redacted>")
            .field(
                "session_token",
                &self.session_token.as_ref().map(|_| "<redacted>"),
            )
            .finish()
    }
}

/// Supported capability flags.
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct CapabilitySet {
    /// Whether account-level bucket discovery succeeded.
    pub list_buckets: bool,
    /// Whether the configured identity can list objects.
    pub list_objects: bool,
    /// Whether multipart uploads are supported by this client.
    pub multipart_upload: bool,
    /// Whether bucket versioning operations are supported.
    pub versioning: bool,
    /// Whether object-lock operations are supported.
    pub object_lock: bool,
    /// Whether ACL operations are supported.
    pub acl: bool,
    /// Whether bucket-policy operations are supported.
    pub policy: bool,
    /// Whether lifecycle operations are supported.
    pub lifecycle: bool,
    /// Whether CORS operations are supported.
    pub cors: bool,
    /// Whether static-website operations are supported.
    pub website: bool,
    /// Whether tagging operations are supported.
    pub tagging: bool,
    /// Whether server-side encryption is supported.
    pub server_side_encryption: bool,
    /// Provider-reported or client compatibility version, when available.
    pub provider_version: Option<String>,
    /// Non-fatal capability-probe warnings.
    pub warnings: Vec<String>,
}

/// One bucket returned by account-level discovery.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct BucketSummary {
    /// Bucket name.
    pub name: String,
    /// Provider-formatted creation timestamp.
    pub created_at: Option<String>,
    /// Bucket region returned by native TOS discovery.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub location: Option<String>,
    /// Whether multi-availability-zone storage is enabled.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub multi_az: Option<bool>,
}

/// Storage usage for one TOS storage class.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct StorageClassStatistics {
    /// TOS storage-class name.
    pub name: String,
    /// Logical storage bytes.
    pub storage_bytes: u64,
    /// Billable storage bytes, when supplied by TOS.
    pub charge_storage_bytes: Option<u64>,
    /// Object count in this class.
    pub object_count: u64,
}

/// Native `GetBucketStat` result.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct BucketStatistics {
    /// Bucket name.
    pub bucket: String,
    /// Provider identifier (`tos`).
    pub provider: String,
    /// Total object count.
    pub object_count: u64,
    /// Total logical storage bytes.
    pub storage_bytes: u64,
    /// Total billable storage bytes, when supplied by TOS.
    pub charge_storage_bytes: Option<u64>,
    /// Per-storage-class totals.
    pub storage_classes: Vec<StorageClassStatistics>,
    /// UTC collection timestamp.
    pub collected_at: String,
    /// Native API operation used to collect the data.
    pub source: String,
}

/// Object or common-prefix entry.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ObjectEntry {
    /// Full object key or common prefix.
    pub key: String,
    /// Display name relative to the requested prefix.
    pub name: String,
    /// Whether this entry represents a common prefix rather than an object.
    pub is_prefix: bool,
    /// Object size in bytes; prefixes use zero.
    pub size: i64,
    /// Object entity tag, when returned.
    pub etag: Option<String>,
    /// Provider-formatted last-modified timestamp.
    pub modified_at: Option<String>,
    /// TOS storage class, when returned.
    pub storage_class: Option<String>,
}

/// Delimited object-list page.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ObjectPage {
    /// Objects and common prefixes in this page.
    pub items: Vec<ObjectEntry>,
    /// Continuation token for the next page.
    pub next_token: Option<String>,
    /// Longest shared directory-like prefix represented by the page.
    pub common_prefix: String,
}

/// One object header exposed as a property.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ObjectProperty {
    /// Header or normalized property name.
    pub name: String,
    /// Property value.
    pub value: String,
}

/// Common and provider-specific object properties.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ObjectProperties {
    /// Common object properties with stable names.
    pub common: Vec<ObjectProperty>,
    /// Provider-specific headers and user metadata.
    pub additional: Vec<ObjectProperty>,
}

/// Aggregate prefix statistics.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct PrefixSummary {
    /// Number of objects below the prefix.
    pub object_count: u64,
    /// Total object size in bytes.
    pub total_size: u64,
    /// Number of native requests made while collecting the summary.
    #[serde(default)]
    pub request_count: u64,
}

impl SecretBundle {
    /// Creates static access credentials without a session token.
    pub fn new(access_key: impl Into<String>, secret_key: impl Into<String>) -> Self {
        Self {
            access_key: access_key.into(),
            secret_key: secret_key.into(),
            session_token: None,
        }
    }

    /// Attaches a temporary-session token.
    pub fn with_session_token(mut self, session_token: impl Into<String>) -> Self {
        self.session_token = Some(session_token.into());
        self
    }
}
