use serde::Deserialize;

/// Pinned upstream Java SDK compatibility inventory.
pub const JAVA_V2_9_19_MANIFEST_JSON: &str = include_str!("../compat/java-v2.9.19-api.json");

/// Metadata for the Java SDK source used as the compatibility baseline.
#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct JavaUpstream {
    /// GitHub project in `owner/repository` form.
    pub project: String,
    /// Upstream SDK version.
    pub version: String,
    /// Immutable upstream Git commit.
    pub commit: String,
    /// Human-readable source URL pinned to `commit`.
    pub source: String,
    /// SHA-256 of the source interface used to create the inventory.
    pub source_sha256: String,
}

/// Aggregate method counts from the pinned Java interface.
#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct JavaParitySummary {
    /// Number of Java interface declarations, including overloads.
    pub declaration_count: usize,
    /// Number of distinct Java method names.
    pub unique_method_count: usize,
    /// Declarations with parameter and result parity.
    pub complete_method_count: usize,
    /// Declarations with only an existing high-level Rust mapping.
    pub partial_declaration_count: usize,
    /// Declarations that do not yet have a Rust mapping.
    pub missing_declaration_count: usize,
}

/// One public Java method declaration and its audited Rust parity status.
#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct JavaMethodParity {
    /// Java method name.
    pub name: String,
    /// Java return type.
    pub return_type: String,
    /// Java parameter declaration.
    pub parameters: String,
    /// Functional API group.
    pub category: String,
    /// Existing Rust compatibility method, when any.
    pub rust_method: Option<String>,
    /// `complete`, `partial`, or `missing`.
    pub status: String,
}

/// Machine-readable parity baseline for an upstream Java SDK release.
#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct JavaParityManifest {
    /// Manifest format version.
    pub schema_version: u32,
    /// Pinned upstream source metadata.
    pub upstream: JavaUpstream,
    /// Aggregate compatibility counts.
    pub summary: JavaParitySummary,
    /// Every public interface declaration, including overloads.
    pub declarations: Vec<JavaMethodParity>,
}

/// Parses the embedded Java v2.9.19 compatibility inventory.
pub fn java_v2_9_19_manifest() -> Result<JavaParityManifest, serde_json::Error> {
    serde_json::from_str(JAVA_V2_9_19_MANIFEST_JSON)
}
