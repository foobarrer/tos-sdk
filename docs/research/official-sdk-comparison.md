# Official SDK comparison baseline

Recorded: 2026-09-12T03:33:16+08:00

## Pinned sources

| Baseline | Version | Pinned revision | Measured public surface |
| --- | --- | --- | ---: |
| Java | 2.9.19 | `cd82b2345dbe0c3c15ebee4865ca37cae343512f` | 165 declarations, 161 distinct names |
| Rust | 2.9.20 | crates.io package checksum through `Cargo.lock` | 106 Java declarations, 102 distinct Java names |
| Python | 2.9.0 | `b6b93b6ca5e4c697688452cf04218ef200dd2996` | 106 public `TosClientV2` methods |
| This crate | 0.2.0-alpha.2 | public Git revision | 116 complete, 2 partial, 47 missing Java declarations |

Official sources:

- Java: <https://github.com/volcengine/ve-tos-java-sdk/tree/cd82b2345dbe0c3c15ebee4865ca37cae343512f>
- Rust: <https://crates.io/crates/ve-tos-rust-sdk/2.9.20>
- Python: <https://github.com/volcengine/ve-tos-python-sdk/tree/b6b93b6ca5e4c697688452cf04218ef200dd2996>

The Java and Python inventories are committed under `compat/`. They are
generated from the pinned interfaces rather than inferred from documentation.

## Functional comparison

The official Rust crate already covers the mature bucket, object, multipart,
policy, ACL, lifecycle, inventory, replication, tagging, fetch, and signing
families. Its missing surface is concentrated in newer Java-only control-plane
and data-processing families. This crate re-exports that Rust core and adds
native async operations instead of wrapping an AWS S3 client.

The Python v2.9.0 client has 106 public methods. After normalizing the file
transfer and versioning aliases, 102 align with Java method families. Python
also has `simple_query` and `semantic_query`, while Java v2.9.19 contains newer
access-point, media, compression, and ObjectSet families absent from Python
v2.9.0. Therefore Java is the broader compatibility target; Python is a useful
cross-check, not the source of the final method count.

The local additions currently cover legacy `listObjects`, object expiration,
legacy notifications, and all six Java access-point operations. The remaining
47 missing declarations and two partial file-transfer declarations are kept
visible in `compat/java-v2.9.19-api.json`; the release gate fails by design.

## Why native TOS lists different-region buckets correctly

Java and Python each issue one native `ListBuckets` request. The TOS response
contains `Location`, `ExtranetEndpoint`, and `IntranetEndpoint` for every
returned bucket. S3 Desktop stores those fields and signs later bucket requests
with that bucket's returned region and endpoint.

AWS S3's compatible `ListBuckets` model does not carry those TOS routing
fields, and an AWS client is normally configured around one endpoint and one
signing region. Consequently it cannot reproduce TOS cross-region routing from
that response alone. The difference is primarily protocol and response-model
semantics, not the console secretly retrying the same list operation. A console
may add pagination, project filtering, or separate authorization probes, but
those are additional product behaviors rather than a replacement for native
TOS `ListBuckets`.

## Intentional and remaining differences

- Rust methods are asynchronous and use typed inputs; Java exposes synchronous
  overloads, while Python exposes keyword-heavy synchronous methods.
- The high-level client validates native Volcengine hosts and disables
  redirects. Java's generic request input permits arbitrary host overrides;
  this crate does not relax credential-destination checks merely to imitate
  that escape hatch.
- The 29-operation S3 Desktop facade adds cross-region route caching, bounded
  preview reads, cancellation, transfer guards, and request observation. These
  are application-oriented additions, not Java incompatibilities.
- Method presence is not a performance result. Rust's async transport and
  connection reuse are suitable foundations, but “faster than Java/Python” is
  not claimed until identical workloads, payload sizes, concurrency, regions,
  retries, TLS settings, and checksums are benchmarked.

## Release gates

1. The Java manifest has no missing or partial declaration.
2. Each added operation has request/response/error wire tests.
3. Transfer operations pass interruption, resume, checksum, and bounded-memory
   tests.
4. A dedicated non-production TOS account passes read and mutation contract
   tests with least-privilege credentials.
5. Reproducible cross-language benchmarks demonstrate throughput, latency, CPU,
   and peak-memory results before making a performance claim.
