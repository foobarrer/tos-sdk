#![forbid(unsafe_code)]
#![warn(missing_docs)]

//! A native, asynchronous Volcengine TOS client.
//!
//! This crate speaks the TOS REST protocol directly and signs requests with
//! `TOS4-HMAC-SHA256`. It does not use the AWS S3 client or an S3-compatible
//! endpoint. The high-level operation set mirrors the TOS functionality used
//! by S3 Desktop, including cross-region bucket discovery, multipart transfer,
//! object metadata, bucket policy, versioning, and presigned downloads.

mod access_point;
mod bucket_statistics;
mod client;
mod compatibility;
mod error;
mod java_compat;
mod models;
mod notification;
mod operations;
mod request_observer;

pub use access_point::{
    Accelerator, AccessPoint, AccessPointEndpoints, AccessPointSummary, CreateAccessPointInput,
    CreateAccessPointOutput, DeleteAccessPointInput, DeleteAccessPointOutput, GetAccessPointInput,
    GetAccessPointOutput, ListAccessPointsInput, ListAccessPointsOutput,
    ListBindAcceleratorForAccessPointInput, ListBindAcceleratorForAccessPointOutput,
    ListBindAccessPointForAcceleratorInput, ListBindAccessPointForAcceleratorOutput,
};
pub use client::Client;
pub use compatibility::{
    java_v2_9_19_manifest, JavaMethodParity, JavaParityManifest, JavaParitySummary, JavaUpstream,
    JAVA_V2_9_19_MANIFEST_JSON,
};
pub use error::AppError as Error;
pub use java_compat::{
    ListObjectsV1Input, ListObjectsV1Output, ListedCommonPrefixV1, ListedObjectV1, ListedOwner,
    RequestInfo, SetObjectExpiresInput, SetObjectExpiresOutput,
};
pub use models::{
    BucketStatistics, BucketSummary, CapabilitySet, ConnectionProfile as ClientConfig, ObjectEntry,
    ObjectPage, ObjectProperties, ObjectProperty, PrefixSummary, SecretBundle,
    SecretBundle as Credentials, StorageClassStatistics,
};
pub use notification::{
    CloudFunctionConfiguration, GetBucketNotificationInput, GetBucketNotificationOutput,
    NotificationFilter, NotificationFilterKey, NotificationFilterRule, PutBucketNotificationInput,
    PutBucketNotificationOutput, RocketMqConfiguration, RocketMqTarget,
};
pub use operations::{
    DirectoryObjectPage, DownloadResult, TosOperations, TransferBandwidthLimiter,
    TransferObjectInfo,
};
pub use request_observer::{
    RequestStatisticEvent as RequestEvent, RequestStatisticsRecorder as RequestObserver,
};

/// Native TOS request/response models and the complete operation set supplied
/// by the pinned Volcengine Rust SDK baseline.
///
/// The higher-level [`Client`] remains source-compatible with S3 Desktop. Use
/// this module when a one-to-one service operation and its complete typed input
/// or output model are required.
#[cfg(feature = "java-parity-core")]
pub use ve_tos_rust_sdk as raw;
