use crate::{
    error::AppError,
    models::{
        BucketStatistics, BucketSummary, CapabilitySet, ObjectPage, ObjectProperties, PrefixSummary,
    },
};
use async_trait::async_trait;
use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::io::DuplexStream;
use tokio::sync::{Mutex as AsyncMutex, Semaphore};
use tokio_util::sync::CancellationToken;

const DISK_CHECK_INTERVAL_BYTES: u64 = 16 * 1024 * 1024;

/// One flat page used while scanning a directory-like prefix.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DirectoryObjectPage {
    /// Object keys and byte sizes in this page.
    pub items: Vec<(String, u64)>,
    /// Continuation token for the next page.
    pub next_token: Option<String>,
}

/// Metadata preserved by streaming object transfers.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TransferObjectInfo {
    /// Object size in bytes.
    pub size: u64,
    /// HTTP content type.
    pub content_type: Option<String>,
    /// HTTP cache-control value.
    pub cache_control: Option<String>,
    /// HTTP content-disposition value.
    pub content_disposition: Option<String>,
    /// HTTP content-encoding value.
    pub content_encoding: Option<String>,
    /// HTTP content-language value.
    pub content_language: Option<String>,
    /// User metadata without the `x-tos-meta-` prefix.
    pub metadata: HashMap<String, String>,
    /// Provider checksum in SHA-256 form, when returned.
    pub checksum_sha256: Option<String>,
    /// Provider checksum in CRC32C form, when returned.
    pub checksum_crc32c: Option<String>,
}

/// Shared token-bucket style transfer limiter.
#[derive(Clone)]
pub struct TransferBandwidthLimiter {
    bytes_per_second: u64,
    next_available: Arc<AsyncMutex<Instant>>,
}

impl TransferBandwidthLimiter {
    /// Creates a limiter. A rate of zero disables throttling.
    pub fn new(bytes_per_second: u64) -> Self {
        Self {
            bytes_per_second,
            next_available: Arc::new(AsyncMutex::new(Instant::now())),
        }
    }

    /// Waits until the configured rate allows the supplied byte count.
    pub async fn consume(&self, bytes: u64) {
        if self.bytes_per_second == 0 || bytes == 0 {
            return;
        }
        let duration = Duration::from_secs_f64(bytes as f64 / self.bytes_per_second as f64);
        let now = Instant::now();
        let scheduled = {
            let mut next = self.next_available.lock().await;
            let scheduled = (*next).max(now);
            *next = scheduled + duration;
            scheduled
        };
        if scheduled > now {
            tokio::time::sleep_until(scheduled.into()).await;
        }
    }
}

pub(crate) struct DiskUsageGuard {
    probe_path: PathBuf,
    bytes_since_check: u64,
    threshold_percent: u8,
}

impl DiskUsageGuard {
    pub(crate) fn new(target: &Path, threshold_percent: u8) -> Result<Self, AppError> {
        let probe_path = existing_disk_path(target);
        ensure_disk_below_limit(&probe_path, threshold_percent)?;
        Ok(Self {
            probe_path,
            bytes_since_check: 0,
            threshold_percent,
        })
    }

    pub(crate) fn record_write(&mut self, bytes: u64) -> Result<(), AppError> {
        self.bytes_since_check = self.bytes_since_check.saturating_add(bytes);
        if self.bytes_since_check >= DISK_CHECK_INTERVAL_BYTES {
            ensure_disk_below_limit(&self.probe_path, self.threshold_percent)?;
            self.bytes_since_check = 0;
        }
        Ok(())
    }

    pub(crate) fn check_now(&mut self) -> Result<(), AppError> {
        ensure_disk_below_limit(&self.probe_path, self.threshold_percent)?;
        self.bytes_since_check = 0;
        Ok(())
    }
}

fn existing_disk_path(target: &Path) -> PathBuf {
    let mut candidate = target.to_path_buf();
    while !candidate.exists() && candidate.pop() {}
    if candidate.as_os_str().is_empty() {
        PathBuf::from(".")
    } else {
        candidate
    }
}

fn ensure_disk_below_limit(path: &Path, threshold_percent: u8) -> Result<(), AppError> {
    let total = fs2::total_space(path).map_err(|error| {
        AppError::new(
            "disk_space_check_failed",
            "Unable to inspect the target disk before downloading",
        )
        .with_details(error.to_string())
    })?;
    let available = fs2::available_space(path).map_err(|error| {
        AppError::new(
            "disk_space_check_failed",
            "Unable to inspect the target disk before downloading",
        )
        .with_details(error.to_string())
    })?;
    if total > 0
        && (total.saturating_sub(available) as u128) * 100
            >= (total as u128) * (threshold_percent as u128)
    {
        return Err(AppError::new(
            "disk_usage_limit",
            format!(
                "Download stopped because the target disk usage has reached {threshold_percent}%"
            ),
        ));
    }
    Ok(())
}

pub(crate) fn normalized_list_page_size(max_keys: i32) -> i32 {
    max_keys.clamp(1, 1000)
}

pub(crate) fn multipart_part_size(total: u64) -> u64 {
    (16_u64 * 1024 * 1024).max(total.div_ceil(9_999))
}

pub(crate) fn display_name(key: &str, prefix: &str) -> String {
    key.strip_prefix(prefix)
        .unwrap_or(key)
        .trim_end_matches('/')
        .rsplit('/')
        .next()
        .unwrap_or(key)
        .to_string()
}

/// Completed file download details.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DownloadResult {
    /// Number of bytes written.
    pub bytes: u64,
    /// Downloaded object's entity tag, when returned.
    pub etag: Option<String>,
}

/// Full high-level TOS operation set implemented by [`crate::Client`].
#[async_trait]
pub trait TosOperations: Send + Sync {
    /// Probes the permissions and capabilities visible to the configured identity.
    async fn probe(&self) -> Result<CapabilitySet, AppError>;
    /// Discovers every bucket returned by the native TOS account API.
    async fn list_buckets(&self) -> Result<Vec<BucketSummary>, AppError>;
    /// Returns whether a bucket has no objects.
    async fn bucket_is_empty(&self, bucket: &str) -> Result<bool, AppError>;
    /// Returns whether a directory-like prefix has no objects.
    async fn directory_is_empty(&self, bucket: &str, prefix: &str) -> Result<bool, AppError>;
    /// Reads native bucket usage statistics when authorized.
    async fn bucket_statistics(&self, bucket: &str) -> Result<Option<BucketStatistics>, AppError>;
    /// Lists one delimiter-aware page of objects and prefixes.
    async fn list_objects(
        &self,
        bucket: &str,
        prefix: &str,
        token: Option<String>,
        max_keys: i32,
    ) -> Result<ObjectPage, AppError>;
    /// Lists one flat page for recursive scanning.
    async fn list_directory_page(
        &self,
        bucket: &str,
        prefix: &str,
        token: Option<String>,
    ) -> Result<DirectoryObjectPage, AppError>;
    /// Scans a prefix and reports batches suitable for operation planning.
    async fn scan_directory_operation_plan(
        &self,
        bucket: &str,
        prefix: &str,
        cancellation: CancellationToken,
        progress: Arc<
            dyn Fn(Vec<(String, u64)>, PrefixSummary) -> Result<(), AppError> + Send + Sync,
        >,
    ) -> Result<PrefixSummary, AppError>;
    /// Reads common headers and native TOS object metadata.
    async fn object_properties(
        &self,
        bucket: &str,
        key: &str,
    ) -> Result<ObjectProperties, AppError>;
    /// Recursively totals objects and bytes below a prefix.
    async fn summarize_prefix(
        &self,
        bucket: &str,
        prefix: &str,
        cancellation: CancellationToken,
        progress: Arc<dyn Fn(PrefixSummary) + Send + Sync>,
    ) -> Result<PrefixSummary, AppError>;
    /// Creates a bucket in the configured region.
    async fn create_bucket(&self, bucket: &str) -> Result<(), AppError>;
    /// Deletes an empty bucket.
    async fn delete_bucket(&self, bucket: &str) -> Result<(), AppError>;
    /// Creates an empty directory-marker object.
    async fn create_folder(&self, bucket: &str, key: &str) -> Result<(), AppError>;
    /// Deletes one or more object keys in a native multi-delete request.
    async fn delete_objects(&self, bucket: &str, keys: Vec<String>) -> Result<(), AppError>;
    /// Uploads a local file, selecting single-part or multipart transfer by size.
    async fn upload(
        &self,
        bucket: &str,
        key: &str,
        path: &Path,
        concurrency: usize,
        multipart_threshold: u64,
    ) -> Result<u64, AppError>;
    /// Downloads an object to a local file with disk-usage safeguards.
    async fn download(
        &self,
        bucket: &str,
        key: &str,
        path: &Path,
        concurrency: usize,
        multipart_threshold: u64,
        disk_usage_threshold_percent: u8,
    ) -> Result<DownloadResult, AppError>;
    /// Reads a bounded byte range intended for text preview.
    async fn preview_text(
        &self,
        bucket: &str,
        key: &str,
        limit: usize,
    ) -> Result<Vec<u8>, AppError>;
    /// Reads a bounded byte range intended for binary preview.
    async fn preview_binary(
        &self,
        bucket: &str,
        key: &str,
        limit: usize,
    ) -> Result<Vec<u8>, AppError>;
    /// Copies an object, including cross-bucket copies supported by TOS.
    async fn copy_object(
        &self,
        source_bucket: &str,
        source_key: &str,
        target_bucket: &str,
        target_key: &str,
    ) -> Result<(), AppError>;
    /// Reads metadata needed to preserve an object during streaming transfer.
    async fn transfer_object_info(
        &self,
        bucket: &str,
        key: &str,
    ) -> Result<TransferObjectInfo, AppError>;
    /// Streams an object into an asynchronous duplex writer.
    async fn stream_object(
        &self,
        bucket: &str,
        key: &str,
        writer: DuplexStream,
        stall_timeout_seconds: u64,
        limiter: TransferBandwidthLimiter,
        progress: Arc<dyn Fn(u64) + Send + Sync>,
    ) -> Result<(), AppError>;
    /// Uploads an asynchronous duplex stream while preserving supplied metadata.
    #[allow(clippy::too_many_arguments)]
    async fn upload_transfer_stream(
        &self,
        bucket: &str,
        key: &str,
        reader: DuplexStream,
        info: TransferObjectInfo,
        multipart_threshold: u64,
        part_size: u64,
        part_concurrency: usize,
        stall_timeout_seconds: u64,
        memory_budget: Arc<Semaphore>,
    ) -> Result<(), AppError>;
    /// Verifies size and available checksums after a transfer.
    async fn verify_transfer_object(
        &self,
        bucket: &str,
        key: &str,
        source: &TransferObjectInfo,
    ) -> Result<(), AppError>;
    /// Produces a signed native TOS GET URL.
    async fn presign_get(
        &self,
        bucket: &str,
        key: &str,
        expires_seconds: u64,
    ) -> Result<String, AppError>;
    /// Returns the bucket policy document, or `None` when absent.
    async fn get_bucket_policy(&self, bucket: &str) -> Result<Option<String>, AppError>;
    /// Replaces the bucket policy document.
    async fn put_bucket_policy(&self, bucket: &str, policy: &str) -> Result<(), AppError>;
    /// Removes the bucket policy document.
    async fn delete_bucket_policy(&self, bucket: &str) -> Result<(), AppError>;
    /// Reads the bucket versioning status.
    async fn get_bucket_versioning(&self, bucket: &str) -> Result<Option<String>, AppError>;
    /// Enables or suspends bucket versioning.
    async fn set_bucket_versioning(&self, bucket: &str, enabled: bool) -> Result<(), AppError>;
}
