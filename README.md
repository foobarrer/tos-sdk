# s3desktop-tos-sdk

An asynchronous Rust client for Volcengine TOS. It sends native TOS REST
requests signed with `TOS4-HMAC-SHA256`; it does not route storage operations
through an AWS S3 client or S3-compatible endpoint.

This reusable client was extracted from S3 Desktop. It is community-maintained
and is not an official Volcengine SDK. Version `0.2.0-alpha.3` continues a measured
compatibility track against the public Java SDK v2.9.19 surface. S3 Desktop has
switched to a commit-pinned dependency from this public repository.

## Add the dependency

Pin a public Git revision so every build consumes the same audited source:

```toml
[dependencies]
s3desktop-tos-sdk = { git = "https://github.com/foobarrer/tos-sdk", rev = "<commit-sha>", default-features = false }
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

## Quick start

```rust,no_run
use s3desktop_tos_sdk::{Client, ClientConfig, Credentials, TosOperations};

#[tokio::main]
async fn main() -> Result<(), s3desktop_tos_sdk::Error> {
    let config = ClientConfig::new("cn-beijing");
    let credentials = Credentials::new("your-access-key", "your-secret-key");
    let client = Client::new(config, Some(credentials), None).await?;

    for bucket in client.list_buckets().await? {
        println!("{} {:?}", bucket.name, bucket.location);
    }
    Ok(())
}
```

Use temporary credentials with `Credentials::with_session_token`. To load an
AWS-compatible environment credential chain without using the AWS S3 client,
use `ClientConfig::with_environment_credentials`; use
`ClientConfig::with_profile_credentials` for a named profile.

## Operations

`TosOperations` exposes the complete S3 Desktop TOS surface:

- discovery and diagnostics: capability probe, bucket listing, empty checks,
  native bucket statistics, object listing, recursive scan and prefix summary;
- metadata and preview: object properties, bounded text/binary reads, transfer
  metadata, verification and presigned GET URLs;
- mutation: bucket/folder creation, bucket deletion, multi-object deletion,
  object copy, bucket policy, and bucket versioning;
- transfer: single-part and multipart file upload, guarded download, duplex
  streaming download, and streaming multipart upload.

Bucket discovery records the native region returned by TOS. Later bucket
requests use that region automatically, so one client can access buckets across
regions. Direct access still works when the identity can access a bucket but
cannot list all buckets.

Least-privilege callers that already know a bucket's native region and endpoint
can register that trusted route without an account-level `ListBuckets` request:

```rust,no_run
# use s3desktop_tos_sdk::{Client, Error};
# async fn register(client: &Client) -> Result<(), Error> {
client
    .register_bucket_route(
        "example-bucket",
        "cn-beijing",
        "example-bucket.tos-cn-beijing.volces.com",
    )
    .await?;
# Ok(())
# }
```

## Java SDK compatibility

The default `java-parity-core` feature also exposes the pinned Volcengine Rust
SDK v2.9.20 as `s3desktop_tos_sdk::raw`. This supplies the mature native TOS
request/response model shared with 106 declarations (102 distinct method names)
from Java SDK v2.9.19. This crate currently adds native async implementations
for Java's deprecated marker-based `listObjects`, `setObjectExpires`, legacy
bucket notifications, and the six access-point control-plane operations. This
takes the measured total to 116 complete declarations, with 2 partial and 47
missing. Java-only operation families are being implemented incrementally and
are not yet advertised as complete.

The machine-readable status is available through
`java_v2_9_19_manifest()` and in `compat/java-v2.9.19-api.json`.

```sh
python3 tools/check_java_parity.py
python3 tools/check_python_baseline.py
python3 tools/check_java_parity.py --require-complete
```

The third command is intentionally a release gate: it fails until every Java
operation has complete request, response, error, and protocol-test coverage.
The project does not claim full Java parity while that command fails.

## Transport and security

- HTTPS with platform-native trust roots is the default.
- Redirects are disabled so signed requests and credentials are not forwarded.
- The configured object-storage endpoint must be the native
  `https://tos-<region>.volces.com` origin and must match the configured region.
  S3-compatible `tos-s3-*` endpoints are rejected instead of being silently
  reinterpreted.
- Native bucket hosts are taken from discovery and fall back to the configured
  native service endpoint when account-level discovery is unavailable.
- Custom CA and proxy settings are explicit opt-ins.
- Credential `Debug` output is redacted, and error diagnostics redact common
  credential markers.
- `unsafe` Rust is forbidden in this crate.

Do not hard-code credentials in source. Prefer temporary credentials or an
external credential source. Grant only the TOS actions required by your use
case.

## Testing

The default suite is hermetic and uses a local mock HTTP server. It covers
TOS4 signing, URL construction, regional routing, pagination, discovery,
permission/error mapping, metadata, preview, download, single/multipart and
streaming uploads, copy/delete, policies, versioning, statistics, cancellation,
request observation, and presigning.

```sh
cargo test --all-targets
cargo clippy --all-targets --all-features -- -D warnings
RUSTDOCFLAGS="-D warnings" cargo doc --no-deps --all-features
```

An ignored, read-only live smoke test is also available. It never writes remote
data and requires explicit environment variables:

```sh
TOS_SDK_LIVE_REGION=cn-beijing \
TOS_SDK_LIVE_ACCESS_KEY=... \
TOS_SDK_LIVE_SECRET_KEY=... \
cargo test --test live_read_only -- --ignored --nocapture
```

Set `TOS_SDK_LIVE_BUCKET` to additionally list one page from a bucket. Add
`TOS_SDK_LIVE_SESSION_TOKEN` when using temporary credentials.

An additional ignored integration test performs create, read, copy, stream,
list, and cleanup operations only below
`tos://it-audit/vdb/data_mining/test/tossdk/`. It requires the same credential
variables plus `TOS_SDK_LIVE_ENDPOINT`:

```sh
cargo test --test live_scoped_prefix -- --ignored --nocapture
```

## Compatibility and licensing

The crate requires Rust 1.94.1 or newer. Public API follows semantic versioning;
the Java-parity work is currently `0.2.0-alpha.3`. It is licensed under
AGPL-3.0-only, matching S3 Desktop. No database or configuration migration is
required to adopt it.
