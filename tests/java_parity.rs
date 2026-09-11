use s3desktop_tos_sdk::java_v2_9_19_manifest;
use std::collections::HashSet;

#[cfg(feature = "java-parity-core")]
#[test]
fn pinned_native_core_is_publicly_available() {
    let type_name = std::any::type_name::<s3desktop_tos_sdk::raw::bucket::ListBucketsInput>();
    assert!(type_name.ends_with("ListBucketsInput"));
}

#[test]
fn pinned_java_surface_is_complete_and_auditable() {
    let manifest = java_v2_9_19_manifest().expect("embedded Java parity manifest must parse");
    assert_eq!(manifest.upstream.version, "2.9.19");
    assert_eq!(manifest.upstream.commit.len(), 40);
    assert_eq!(manifest.summary.declaration_count, 165);
    assert_eq!(manifest.summary.unique_method_count, 161);
    assert_eq!(manifest.summary.complete_method_count, 116);
    assert_eq!(manifest.summary.partial_declaration_count, 2);
    assert_eq!(manifest.summary.missing_declaration_count, 47);
    assert_eq!(manifest.declarations.len(), 165);
    assert!(manifest
        .upstream
        .source
        .starts_with("https://github.com/volcengine/"));
    assert_eq!(manifest.upstream.source_sha256.len(), 64);

    let names = manifest
        .declarations
        .iter()
        .map(|item| item.name.as_str())
        .collect::<HashSet<_>>();
    assert_eq!(names.len(), 161);
    for required in [
        "listBuckets",
        "putObject",
        "uploadFile",
        "resumableCopyObject",
        "createAccessPoint",
        "putAudioConvertTemplate",
        "pointCloudCompress",
        "putObjectSet",
    ] {
        assert!(names.contains(required), "missing Java method {required}");
    }
}

#[test]
fn incomplete_parity_is_never_reported_as_complete() {
    let manifest = java_v2_9_19_manifest().expect("embedded Java parity manifest must parse");
    let calculated_complete = manifest
        .declarations
        .iter()
        .filter(|item| item.status == "complete")
        .count();
    let calculated_partial = manifest
        .declarations
        .iter()
        .filter(|item| item.status == "partial")
        .count();
    let calculated_missing = manifest
        .declarations
        .iter()
        .filter(|item| item.status == "missing")
        .count();
    assert_eq!(manifest.summary.complete_method_count, calculated_complete);
    assert_eq!(
        manifest.summary.partial_declaration_count,
        calculated_partial
    );
    assert_eq!(
        manifest.summary.missing_declaration_count,
        calculated_missing
    );
    assert_eq!(
        calculated_complete + calculated_partial + calculated_missing,
        165
    );
}
