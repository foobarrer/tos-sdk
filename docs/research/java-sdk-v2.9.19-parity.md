# Java SDK v2.9.19 parity baseline

Recorded: 2026-09-12T03:33:16+08:00

## Scope

The compatibility target is the public `TOSV2` interface in Volcengine's Java
SDK v2.9.19 at commit
`cd82b2345dbe0c3c15ebee4865ca37cae343512f`.  Java overloads are preserved as
separate declarations in the audit manifest, while Rust exposes idiomatic
snake-case methods and typed input/output structures rather than emulating
Java overload resolution.

Source: <https://github.com/volcengine/ve-tos-java-sdk/blob/cd82b2345dbe0c3c15ebee4865ca37cae343512f/src/main/java/com/volcengine/tos/TOSV2.java>

## Evidence

- The pinned Java interface contains 165 declarations and 161 distinct method
  names.
- The pinned Volcengine Rust SDK v2.9.20 provides native typed implementations
  corresponding to 106 Java declarations and 102 distinct Java method names,
  including equivalent credential and endpoint refresh operations.
- This crate adds tested native implementations for the deprecated marker-based
  `listObjects` contract, `setObjectExpires`, legacy bucket notifications, and
  all six access-point operations, bringing the current total to 116 complete
  declarations.
- The existing S3 Desktop facade partially maps two additional Java
  declarations (`uploadFile` and `downloadFile`).
- Forty-seven Java declarations remain absent. They are primarily
  media-processing, compression, ObjectSet, client mutation, and
  resumable-copy operations.

The generated inventory is `compat/java-v2.9.19-api.json`.  It records the
upstream commit and SHA-256, every signature, functional category, Rust mapping,
and current status.  Regenerate it with `tools/sync_java_api.py`; validate it
with `tools/check_java_parity.py`.  Release automation must eventually run the
validator with `--require-complete` before claiming Java parity.

## Architecture decision

The SDK has two additive surfaces:

1. `raw` exposes the pinned official Rust native protocol baseline, covering
   the mature common object-storage surface without an AWS S3 client.
2. The existing `Client` and `TosOperations` API remains the S3 Desktop
   compatibility facade with cross-region routing, preview, statistics,
   guarded downloads, and streaming transfers.

Java-only operations are implemented as native Rust modules beside the raw
baseline. They must not be represented by empty stubs. The current manifest is
an operation-level audit: `complete` means the principal typed request/response
and wire protocol exist and have hermetic tests. Full language-level parity,
including every `GenericInput` escape hatch, transfer checkpoint behavior, and
live service behavior, is a stricter release gate and is not yet certified.

## Limits and next work

This alpha baseline is not yet Java-feature-complete. Live write-operation
testing also requires a dedicated non-production TOS account and scoped test
buckets; no credentials or remote objects were used during this phase.

The access-point implementation follows Java's control-plane layout: callers
configure a control endpoint, requests use the
`{accountId}.{control-endpoint}` host, carry `x-tos-account-id`, and are signed
for the configured TOS region. Custom request hosts remain restricted to TOS
domains so credentials cannot be redirected to arbitrary hosts.
