# Changelog

## 0.2.0-alpha.3 - 2026-10-21

- Make the configured native TOS endpoint authoritative for account-level
  requests instead of using only its URL scheme and silently deriving a host.
- Reject S3-compatible `tos-s3-*` endpoints and native endpoints that do not
  match the configured region.
- Preserve discovered per-bucket endpoints and native fallback routing while
  adding protocol coverage for the configured service host.

## 0.2.0-alpha.2 - 2026-09-20

- Add `Client::register_bucket_route` so least-privilege callers can provide a
  validated native TOS region and endpoint without requiring account-level
  `ListBuckets` discovery.
- Add a credential-gated live integration test constrained to
  `tos://it-audit/vdb/data_mining/test/tossdk/`.
- Document commit-pinned public Git consumption for reproducible S3 Desktop
  builds while preserving the existing automatic discovery behavior.

## 0.2.0-alpha.1 - 2026-09-11

- Establish Java SDK v2.9.19 as an immutable, machine-readable compatibility
  baseline containing all 165 public declarations and 161 distinct method
  names.
- Expose the pinned native Volcengine Rust SDK v2.9.20 under `raw`, covering
  106 Java declarations without introducing an AWS S3 client.
- Add native async implementations and wire tests for Java-compatible
  marker-based `listObjects` and `setObjectExpires` operations.
- Add native async implementations for all six Java access-point operations,
  including validated control-endpoint routing and response models.
- Add Java-compatible legacy bucket-notification get/put operations alongside
  the existing v2 notification API exposed by the pinned Rust core.
- Pin and inventory the official Python SDK v2.9.0 public `TosClientV2`
  surface as a second comparison baseline.
- Keep the existing 29-operation S3 Desktop facade and cross-region routing
  behavior intact.
- Add CI and an explicit release gate that prevents incomplete parity from
  being reported as complete.


## 0.1.0 - 2026-09-11

- Extract the native TOS REST client used by S3 Desktop into a reusable crate.
- Cover all 29 high-level operations, including cross-region discovery,
  multipart transfers, policies, versioning, statistics, and presigning.
- Add hermetic protocol tests, a read-only opt-in live smoke test, API
  documentation, and runnable examples.
