use crate::{
    bucket_statistics::fetch_tos_bucket_statistics,
    error::{redact, AppError},
    models::{
        BucketStatistics, BucketSummary, CapabilitySet, ConnectionProfile, ObjectEntry, ObjectPage,
        ObjectProperties, ObjectProperty, PrefixSummary, SecretBundle,
    },
    operations::{
        display_name, multipart_part_size, normalized_list_page_size, DirectoryObjectPage,
        DiskUsageGuard, DownloadResult, TosOperations, TransferBandwidthLimiter,
        TransferObjectInfo,
    },
    request_observer::{record_http_request, RequestStatisticsRecorder},
};
use async_trait::async_trait;
use aws_config::BehaviorVersion;
use aws_credential_types::provider::ProvideCredentials;
use base64::{engine::general_purpose::STANDARD as BASE64, Engine};
use chrono::{DateTime, Utc};
use futures_util::{stream, StreamExt, TryStreamExt};
use hmac::{Hmac, Mac};
use md5::Md5;
use reqwest::{header::HeaderMap, Client as HttpClient, Method, Response};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, HashMap},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt, DuplexStream};
use tokio::sync::{OwnedSemaphorePermit, RwLock, Semaphore};
use tokio_util::{io::ReaderStream, sync::CancellationToken};
use url::Url;

type HmacSha256 = Hmac<Sha256>;

const EMPTY_SHA256: &str = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
const UNSIGNED_PAYLOAD: &str = "UNSIGNED-PAYLOAD";
const MAX_BUFFERED_UPLOAD_PARTS: usize = 64;

async fn upload_request_permit(permits: &Arc<Semaphore>) -> Result<OwnedSemaphorePermit, AppError> {
    permits.clone().acquire_owned().await.map_err(|_| {
        AppError::new(
            "upload_concurrency_closed",
            "The upload request limiter stopped unexpectedly",
        )
    })
}

#[derive(Debug, Clone)]
struct TosBucketRoute {
    region: String,
    endpoint: String,
}

/// Cloneable native TOS client.
#[derive(Clone)]
pub struct Client {
    profile: ConnectionProfile,
    service_endpoint: Url,
    credentials: SecretBundle,
    client: HttpClient,
    routes: Arc<RwLock<HashMap<String, TosBucketRoute>>>,
    request_statistics: Option<RequestStatisticsRecorder>,
    transport_base_url: Option<Url>,
    control_endpoint: Option<Url>,
}

#[derive(Debug, Deserialize)]
struct TosListBucketsResponse {
    #[serde(default, rename = "Buckets")]
    buckets: Vec<TosListedBucket>,
}

#[derive(Debug, Deserialize)]
struct TosListedBucket {
    #[serde(default, rename = "Name")]
    name: String,
    #[serde(default, rename = "CreationDate")]
    creation_date: String,
    #[serde(default, rename = "Location")]
    location: String,
    #[serde(default, rename = "ExtranetEndpoint")]
    extranet_endpoint: String,
    #[serde(default, rename = "IntranetEndpoint")]
    intranet_endpoint: String,
}

#[derive(Debug, Default, Deserialize)]
struct TosListObjectsResponse {
    #[serde(default, rename = "Contents")]
    contents: Vec<TosListedObject>,
    #[serde(default, rename = "CommonPrefixes")]
    common_prefixes: Vec<TosCommonPrefix>,
    #[serde(default, rename = "NextContinuationToken")]
    next_continuation_token: String,
}

#[derive(Debug, Deserialize)]
struct TosListedObject {
    #[serde(default, rename = "Key")]
    key: String,
    #[serde(default, rename = "LastModified")]
    last_modified: String,
    #[serde(default, rename = "ETag")]
    etag: String,
    #[serde(default, rename = "Size")]
    size: i64,
    #[serde(default, rename = "StorageClass")]
    storage_class: String,
}

#[derive(Debug, Deserialize)]
struct TosCommonPrefix {
    #[serde(default, rename = "Prefix")]
    prefix: String,
}

#[derive(Debug, Default, Deserialize)]
struct TosErrorResponse {
    #[serde(default, rename = "Code")]
    code: String,
    #[serde(default, rename = "Message")]
    message: String,
}

#[derive(Debug, Deserialize)]
struct TosCreateMultipartResponse {
    #[serde(default, rename = "UploadId")]
    upload_id: String,
}

#[derive(Debug, Serialize)]
struct TosCompleteMultipartRequest<'a> {
    #[serde(rename = "Parts")]
    parts: &'a [TosUploadedPart],
}

#[derive(Debug, Clone, Serialize)]
struct TosUploadedPart {
    #[serde(rename = "PartNumber")]
    part_number: i32,
    #[serde(rename = "ETag")]
    etag: String,
}

#[derive(Debug, Serialize)]
struct TosDeleteObjectsRequest<'a> {
    #[serde(rename = "Objects")]
    objects: Vec<TosDeleteObject<'a>>,
    #[serde(rename = "Quiet")]
    quiet: bool,
}

#[derive(Debug, Serialize)]
struct TosDeleteObject<'a> {
    #[serde(rename = "Key")]
    key: &'a str,
}

#[derive(Debug, Default, Deserialize)]
struct TosDeleteObjectsResponse {
    #[serde(default, rename = "Error")]
    errors: Vec<TosDeleteError>,
}

#[derive(Debug, Deserialize)]
struct TosDeleteError {
    #[serde(default, rename = "Key")]
    key: String,
    #[serde(default, rename = "Code")]
    code: String,
    #[serde(default, rename = "Message")]
    message: String,
}

#[derive(Debug, Deserialize)]
struct TosVersioningResponse {
    #[serde(default, rename = "Status")]
    status: String,
}

#[derive(Debug, Serialize)]
struct TosVersioningRequest<'a> {
    #[serde(rename = "Status")]
    status: &'a str,
}

impl Client {
    /// Builds a client and resolves credentials from the selected source.
    pub async fn new(
        profile: ConnectionProfile,
        stored_secret: Option<SecretBundle>,
        request_statistics: Option<RequestStatisticsRecorder>,
    ) -> Result<Self, AppError> {
        let service_endpoint = native_service_endpoint(&profile.endpoint, &profile.region)?;
        let credentials = resolve_credentials(&profile, stored_secret).await?;
        let mut builder = HttpClient::builder()
            .timeout(Duration::from_secs(profile.timeout_seconds.clamp(5, 600)))
            .redirect(reqwest::redirect::Policy::none())
            .danger_accept_invalid_certs(profile.allow_invalid_certificates);
        if let Some(proxy) = profile
            .proxy_url
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
        {
            builder = builder.proxy(reqwest::Proxy::all(proxy).map_err(|_| {
                AppError::new("invalid_proxy_url", "The configured proxy URL is invalid")
            })?);
        }
        if let Some(path) = profile
            .custom_ca_path
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
        {
            let pem = std::fs::read(path).map_err(|error| {
                AppError::new("custom_ca_read_failed", "Unable to read the custom CA file")
                    .with_details(error.to_string())
            })?;
            let certificate = reqwest::Certificate::from_pem(&pem).map_err(|_| {
                AppError::new("custom_ca_invalid", "The custom CA file is not valid PEM")
            })?;
            builder = builder.add_root_certificate(certificate);
        }
        let client = builder.build().map_err(|_| {
            AppError::new(
                "provider_client_failed",
                "Unable to initialize the native TOS client",
            )
        })?;
        Ok(Self {
            profile,
            service_endpoint,
            credentials,
            client,
            routes: Arc::new(RwLock::new(HashMap::new())),
            request_statistics,
            transport_base_url: None,
            control_endpoint: None,
        })
    }

    /// Configures the native TOS control endpoint used by account-level APIs
    /// such as access points.
    ///
    /// The endpoint is configured separately from the object-storage endpoint,
    /// matching the Java SDK. It must be an HTTP(S) Volcengine host without a
    /// path, query, fragment, or embedded credentials.
    pub fn with_control_endpoint(mut self, endpoint: impl AsRef<str>) -> Result<Self, AppError> {
        let endpoint = Url::parse(endpoint.as_ref()).map_err(|_| {
            AppError::new(
                "invalid_control_endpoint",
                "The TOS control endpoint is not a valid URL",
            )
        })?;
        let host = endpoint.host_str().ok_or_else(|| {
            AppError::new(
                "invalid_control_endpoint",
                "The TOS control endpoint must contain a host",
            )
        })?;
        if !matches!(endpoint.scheme(), "http" | "https")
            || endpoint.username() != ""
            || endpoint.password().is_some()
            || endpoint.path() != "/"
            || endpoint.query().is_some()
            || endpoint.fragment().is_some()
        {
            return Err(AppError::new(
                "invalid_control_endpoint",
                "The TOS control endpoint must be an HTTP(S) origin",
            ));
        }
        validate_tos_host(host)?;
        self.control_endpoint = Some(endpoint);
        Ok(self)
    }

    /// Registers a trusted route for a known bucket.
    ///
    /// Least-privilege callers can use this before their first bucket request
    /// to avoid the account-level `ListBuckets` discovery call. The endpoint
    /// is validated with the same native TOS host rules as discovered routes.
    pub async fn register_bucket_route(
        &self,
        bucket: &str,
        region: &str,
        endpoint: &str,
    ) -> Result<(), AppError> {
        validate_tos_identifier(bucket, "TOS bucket")?;
        validate_tos_identifier(region, "TOS region")?;
        let endpoint = normalize_bucket_endpoint(bucket, region, endpoint, "")?;
        self.routes.write().await.insert(
            bucket.to_string(),
            TosBucketRoute {
                region: region.to_string(),
                endpoint,
            },
        );
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn with_transport_base_url(mut self, url: Url) -> Self {
        self.transport_base_url = Some(url);
        self
    }

    fn statistics_context(&self) -> Option<(&RequestStatisticsRecorder, &str, &str)> {
        self.request_statistics.as_ref().map(|recorder| {
            (
                recorder,
                self.profile.id.as_str(),
                self.profile.name.as_str(),
            )
        })
    }

    fn scheme(&self) -> &str {
        self.service_endpoint.scheme()
    }

    fn service_host(&self) -> Result<String, AppError> {
        self.service_endpoint
            .host_str()
            .map(ToString::to_string)
            .ok_or_else(|| {
                AppError::new(
                    "invalid_tos_endpoint",
                    "The native TOS endpoint must contain a host",
                )
            })
    }

    async fn bucket_route(&self, bucket: &str) -> Result<TosBucketRoute, AppError> {
        if let Some(route) = self.routes.read().await.get(bucket).cloned() {
            return Ok(route);
        }
        // Direct bucket access must keep working for credentials that can use a
        // known bucket but are not allowed to enumerate the account. Discovery
        // enriches routing when available; the saved region remains the fallback.
        let _ = self.list_buckets_native().await;
        if let Some(route) = self.routes.read().await.get(bucket).cloned() {
            return Ok(route);
        }
        validate_tos_identifier(bucket, "TOS bucket")?;
        Ok(TosBucketRoute {
            region: self.profile.region.clone(),
            endpoint: format!("{bucket}.tos-{}.volces.com", self.profile.region),
        })
    }

    async fn list_buckets_native(&self) -> Result<Vec<BucketSummary>, AppError> {
        let host = self.service_host()?;
        let response = self
            .send_to(
                "ListBuckets",
                None,
                Method::GET,
                &host,
                &self.profile.region,
                "",
                BTreeMap::new(),
                BTreeMap::new(),
                None,
            )
            .await?;
        let output: TosListBucketsResponse = parse_json_response("List buckets", response).await?;
        let mut routes = self.routes.write().await;
        let mut buckets = Vec::with_capacity(output.buckets.len());
        for bucket in output.buckets {
            if bucket.name.is_empty() {
                continue;
            }
            let region = if bucket.location.is_empty() {
                self.profile.region.clone()
            } else {
                bucket.location.clone()
            };
            let endpoint = normalize_bucket_endpoint(
                &bucket.name,
                &region,
                &bucket.extranet_endpoint,
                &bucket.intranet_endpoint,
            )?;
            routes.insert(
                bucket.name.clone(),
                TosBucketRoute {
                    region: region.clone(),
                    endpoint,
                },
            );
            buckets.push(BucketSummary {
                name: bucket.name,
                created_at: nonempty(bucket.creation_date),
                location: Some(region),
                multi_az: None,
            });
        }
        Ok(buckets)
    }

    #[allow(clippy::too_many_arguments)]
    async fn send_to(
        &self,
        operation: &str,
        bucket: Option<&str>,
        method: Method,
        host: &str,
        region: &str,
        key: &str,
        query: BTreeMap<String, String>,
        headers: BTreeMap<String, String>,
        body: Option<reqwest::Body>,
    ) -> Result<Response, AppError> {
        self.send_to_with_scheme(
            operation,
            bucket,
            method,
            host,
            region,
            self.scheme(),
            key,
            query,
            headers,
            body,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn send_to_with_scheme(
        &self,
        operation: &str,
        bucket: Option<&str>,
        method: Method,
        host: &str,
        region: &str,
        scheme: &str,
        key: &str,
        query: BTreeMap<String, String>,
        mut headers: BTreeMap<String, String>,
        body: Option<reqwest::Body>,
    ) -> Result<Response, AppError> {
        validate_tos_host(host)?;
        let now = Utc::now();
        headers.insert("host".into(), host.to_string());
        headers.insert(
            "x-tos-date".into(),
            now.format("%Y%m%dT%H%M%SZ").to_string(),
        );
        if let Some(token) = self
            .credentials
            .session_token
            .as_deref()
            .filter(|value| !value.is_empty())
        {
            headers.insert("x-tos-security-token".into(), token.to_string());
        }
        let authorization = sign_tos_request(
            method.as_str(),
            key,
            &query,
            &headers,
            region,
            &self.credentials,
            now,
            EMPTY_SHA256,
        )?;
        headers.insert("authorization".into(), authorization);

        let url = self.request_url(scheme, host, key, &query);
        let mut request = self.client.request(method, url);
        for (name, value) in &headers {
            request = request.header(name, value);
        }
        if let Some(body) = body {
            request = request.body(body);
        }
        record_http_request(
            self.statistics_context(),
            bucket,
            operation,
            "TOS",
            "native",
        );
        let response = request.send().await.map_err(|error| {
            AppError::retryable(
                "provider_unavailable",
                format!("{operation} failed for {}", self.profile.name),
            )
            .with_details(redact(&error.to_string()))
        })?;
        ensure_success(operation, response).await
    }

    fn request_url(
        &self,
        scheme: &str,
        host: &str,
        key: &str,
        query: &BTreeMap<String, String>,
    ) -> String {
        let Some(base) = self.transport_base_url.as_ref() else {
            return build_url(scheme, host, key, query);
        };
        let mut url = format!(
            "{}/{}",
            base.as_str().trim_end_matches('/'),
            encode_path(key)
        );
        if !query.is_empty() {
            url.push('?');
            url.push_str(&canonical_query(query));
        }
        url
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn send_bucket(
        &self,
        operation: &str,
        method: Method,
        bucket: &str,
        key: &str,
        query: BTreeMap<String, String>,
        headers: BTreeMap<String, String>,
        body: Option<reqwest::Body>,
    ) -> Result<Response, AppError> {
        let route = self.bucket_route(bucket).await?;
        self.send_to(
            operation,
            Some(bucket),
            method,
            &route.endpoint,
            &route.region,
            key,
            query,
            headers,
            body,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn send_control(
        &self,
        operation: &str,
        method: Method,
        account_id: &str,
        key: &str,
        query: BTreeMap<String, String>,
        mut headers: BTreeMap<String, String>,
        body: Option<reqwest::Body>,
    ) -> Result<Response, AppError> {
        validate_tos_identifier(account_id, "TOS account ID")?;
        let endpoint = self.control_endpoint.as_ref().ok_or_else(|| {
            AppError::new(
                "control_endpoint_missing",
                "This TOS operation requires a configured control endpoint",
            )
        })?;
        let base_host = endpoint.host_str().ok_or_else(|| {
            AppError::new(
                "invalid_control_endpoint",
                "The TOS control endpoint must contain a host",
            )
        })?;
        let host = format!("{account_id}.{base_host}");
        headers.insert("x-tos-account-id".into(), account_id.to_string());
        self.send_to_with_scheme(
            operation,
            None,
            method,
            &host,
            &self.profile.region,
            endpoint.scheme(),
            key,
            query,
            headers,
            body,
        )
        .await
    }

    async fn list_objects_native(
        &self,
        bucket: &str,
        prefix: &str,
        delimiter: Option<&str>,
        token: Option<String>,
        max_keys: i32,
        operation: &str,
    ) -> Result<TosListObjectsResponse, AppError> {
        let mut query = BTreeMap::from([
            ("list-type".into(), "2".into()),
            (
                "max-keys".into(),
                normalized_list_page_size(max_keys).to_string(),
            ),
            ("prefix".into(), prefix.to_string()),
        ]);
        if let Some(delimiter) = delimiter {
            query.insert("delimiter".into(), delimiter.into());
        }
        if let Some(token) = token.filter(|value| !value.is_empty()) {
            query.insert("continuation-token".into(), token);
        }
        let response = self
            .send_bucket(
                operation,
                Method::GET,
                bucket,
                "",
                query,
                BTreeMap::new(),
                None,
            )
            .await?;
        parse_json_response(operation, response).await
    }

    async fn head_object_response(
        &self,
        bucket: &str,
        key: &str,
        operation: &str,
    ) -> Result<Response, AppError> {
        self.send_bucket(
            operation,
            Method::HEAD,
            bucket,
            key,
            BTreeMap::new(),
            BTreeMap::new(),
            None,
        )
        .await
    }

    async fn get_object_response(
        &self,
        bucket: &str,
        key: &str,
        range: Option<String>,
        operation: &str,
    ) -> Result<Response, AppError> {
        let mut headers = BTreeMap::new();
        if let Some(range) = range {
            headers.insert("range".into(), range);
        }
        self.send_bucket(
            operation,
            Method::GET,
            bucket,
            key,
            BTreeMap::new(),
            headers,
            None,
        )
        .await
    }

    async fn put_bytes(
        &self,
        bucket: &str,
        key: &str,
        bytes: Vec<u8>,
        mut headers: BTreeMap<String, String>,
        operation: &str,
    ) -> Result<Response, AppError> {
        headers.insert("content-length".into(), bytes.len().to_string());
        self.send_bucket(
            operation,
            Method::PUT,
            bucket,
            key,
            BTreeMap::new(),
            headers,
            Some(reqwest::Body::from(bytes)),
        )
        .await
    }

    async fn create_multipart(
        &self,
        bucket: &str,
        key: &str,
        headers: BTreeMap<String, String>,
        operation: &str,
    ) -> Result<String, AppError> {
        let response = self
            .send_bucket(
                operation,
                Method::POST,
                bucket,
                key,
                BTreeMap::from([("uploads".into(), String::new())]),
                headers,
                None,
            )
            .await?;
        let output: TosCreateMultipartResponse = parse_json_response(operation, response).await?;
        if output.upload_id.is_empty() {
            return Err(AppError::new(
                "multipart_missing_id",
                "TOS did not return a multipart upload ID",
            ));
        }
        Ok(output.upload_id)
    }

    async fn upload_part(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
        part_number: i32,
        bytes: Vec<u8>,
        operation: &str,
    ) -> Result<TosUploadedPart, AppError> {
        let length = bytes.len();
        let response = self
            .send_bucket(
                operation,
                Method::PUT,
                bucket,
                key,
                BTreeMap::from([
                    ("partNumber".into(), part_number.to_string()),
                    ("uploadId".into(), upload_id.to_string()),
                ]),
                BTreeMap::from([("content-length".into(), length.to_string())]),
                Some(reqwest::Body::from(bytes)),
            )
            .await?;
        Ok(TosUploadedPart {
            part_number,
            etag: header_string(response.headers(), "etag").unwrap_or_default(),
        })
    }

    async fn complete_multipart(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
        parts: &[TosUploadedPart],
    ) -> Result<(), AppError> {
        let bytes =
            serde_json::to_vec(&TosCompleteMultipartRequest { parts }).map_err(|error| {
                AppError::new(
                    "multipart_invalid",
                    "Unable to serialize multipart completion",
                )
                .with_details(error.to_string())
            })?;
        let headers = content_headers(&bytes, "application/json");
        self.send_bucket(
            "CompleteMultipartUpload",
            Method::POST,
            bucket,
            key,
            BTreeMap::from([("uploadId".into(), upload_id.to_string())]),
            headers,
            Some(reqwest::Body::from(bytes)),
        )
        .await?;
        Ok(())
    }

    async fn abort_multipart(&self, bucket: &str, key: &str, upload_id: &str) {
        let _ = self
            .send_bucket(
                "AbortMultipartUpload",
                Method::DELETE,
                bucket,
                key,
                BTreeMap::from([("uploadId".into(), upload_id.to_string())]),
                BTreeMap::new(),
                None,
            )
            .await;
    }

    async fn multipart_upload_file(
        &self,
        bucket: &str,
        key: &str,
        path: &Path,
        total: u64,
        request_permits: Arc<Semaphore>,
        progress: Arc<dyn Fn(u64) + Send + Sync>,
    ) -> Result<u64, AppError> {
        let upload_id = {
            let _permit = upload_request_permit(&request_permits).await?;
            self.create_multipart(bucket, key, BTreeMap::new(), "CreateMultipartUpload")
                .await?
        };
        let part_size = multipart_part_size(total);
        let completed_bytes = Arc::new(AtomicU64::new(0));
        let parts = (0..total.div_ceil(part_size)).map(|index| {
            let provider = self.clone();
            let path = path.to_path_buf();
            let bucket = bucket.to_string();
            let key = key.to_string();
            let upload_id = upload_id.clone();
            let request_permits = request_permits.clone();
            let completed_bytes = completed_bytes.clone();
            let progress = progress.clone();
            async move {
                let _permit = upload_request_permit(&request_permits).await?;
                let offset = index * part_size;
                let length = part_size.min(total - offset);
                let mut file = tokio::fs::File::open(path).await?;
                file.seek(std::io::SeekFrom::Start(offset)).await?;
                let mut bytes = vec![
                    0;
                    usize::try_from(length).map_err(|_| {
                        AppError::new("multipart_part_overflow", "Multipart part is too large")
                    })?
                ];
                file.read_exact(&mut bytes).await?;
                let part = provider
                    .upload_part(
                        &bucket,
                        &key,
                        &upload_id,
                        i32::try_from(index + 1).map_err(|_| {
                            AppError::new(
                                "multipart_part_overflow",
                                "Multipart upload exceeded the part limit",
                            )
                        })?,
                        bytes,
                        "UploadPart",
                    )
                    .await?;
                let completed = completed_bytes
                    .fetch_add(length, Ordering::Relaxed)
                    .saturating_add(length)
                    .min(total);
                progress(completed);
                Ok::<_, AppError>(part)
            }
        });
        let uploaded = stream::iter(parts)
            .buffer_unordered(MAX_BUFFERED_UPLOAD_PARTS)
            .collect::<Vec<_>>()
            .await;
        let mut completed = Vec::with_capacity(uploaded.len());
        for result in uploaded {
            match result {
                Ok(part) => completed.push(part),
                Err(error) => {
                    if let Ok(_permit) = upload_request_permit(&request_permits).await {
                        self.abort_multipart(bucket, key, &upload_id).await;
                    }
                    return Err(error);
                }
            }
        }
        completed.sort_by_key(|part| part.part_number);
        let completion = {
            let _permit = upload_request_permit(&request_permits).await?;
            self.complete_multipart(bucket, key, &upload_id, &completed)
                .await
        };
        if let Err(error) = completion {
            if let Ok(_permit) = upload_request_permit(&request_permits).await {
                self.abort_multipart(bucket, key, &upload_id).await;
            }
            return Err(error);
        }
        Ok(total)
    }

    async fn preview_range(
        &self,
        bucket: &str,
        key: &str,
        maximum: usize,
        operation: &str,
    ) -> Result<Vec<u8>, AppError> {
        let response = self
            .get_object_response(
                bucket,
                key,
                Some(format!("bytes=0-{}", maximum.saturating_sub(1))),
                operation,
            )
            .await?;
        let mut stream = response.bytes_stream();
        let mut preview = Vec::with_capacity(maximum.min(1024 * 1024));
        while let Some(bytes) = stream.try_next().await.map_err(|error| {
            AppError::retryable("preview_interrupted", "The TOS preview was interrupted")
                .with_details(redact(&error.to_string()))
        })? {
            let remaining = maximum.saturating_sub(preview.len());
            preview.extend_from_slice(&bytes[..bytes.len().min(remaining)]);
            if preview.len() == maximum {
                break;
            }
        }
        Ok(preview)
    }

    #[allow(clippy::too_many_arguments)]
    async fn parallel_download(
        &self,
        bucket: &str,
        key: &str,
        path: &Path,
        total: u64,
        concurrency: usize,
        disk_usage_threshold_percent: u8,
        etag: &str,
    ) -> Result<u64, AppError> {
        DiskUsageGuard::new(path, disk_usage_threshold_percent)?;
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        let part_size = multipart_part_size(total);
        let parts_directory = PathBuf::from(format!("{}.s3desktop.parts", path.to_string_lossy()));
        tokio::fs::create_dir_all(&parts_directory).await?;
        let identity = format!("{total}\n{part_size}\n{etag}");
        let manifest = parts_directory.join("manifest");
        let previous_identity = tokio::fs::read_to_string(&manifest)
            .await
            .unwrap_or_default();
        if !previous_identity.is_empty() && previous_identity != identity {
            tokio::fs::remove_dir_all(&parts_directory).await?;
            tokio::fs::create_dir_all(&parts_directory).await?;
        }
        tokio::fs::write(&manifest, identity).await?;

        let ranges = (0..total.div_ceil(part_size)).map(|index| {
            let provider = self.clone();
            let bucket = bucket.to_string();
            let key = key.to_string();
            let part_path = parts_directory.join(format!("{index:05}.part"));
            async move {
                let mut disk_guard = DiskUsageGuard::new(&part_path, disk_usage_threshold_percent)?;
                let start = index * part_size;
                let length = part_size.min(total - start);
                if tokio::fs::metadata(&part_path)
                    .await
                    .map(|value| value.len())
                    .unwrap_or(0)
                    == length
                {
                    return Ok::<_, AppError>((index, part_path, length));
                }
                let end = start + length - 1;
                let response = provider
                    .get_object_response(
                        &bucket,
                        &key,
                        Some(format!("bytes={start}-{end}")),
                        "GetObjectRange",
                    )
                    .await?;
                if response.status().as_u16() != 206
                    || response.headers().get("content-range").is_none()
                {
                    return Err(AppError::new(
                        "range_not_supported",
                        "TOS did not honor the multipart download Range request",
                    ));
                }
                let temporary = part_path.with_extension("part.tmp");
                let mut file = tokio::fs::File::create(&temporary).await?;
                let mut body = response.bytes_stream();
                let mut written = 0_u64;
                while let Some(bytes) = body.try_next().await.map_err(|error| {
                    AppError::retryable(
                        "download_interrupted",
                        "A TOS multipart download range was interrupted",
                    )
                    .with_details(redact(&error.to_string()))
                })? {
                    file.write_all(&bytes).await?;
                    disk_guard.record_write(bytes.len() as u64)?;
                    written = written.saturating_add(bytes.len() as u64);
                }
                file.flush().await?;
                disk_guard.check_now()?;
                drop(file);
                if written != length {
                    return Err(AppError::retryable(
                        "download_range_incomplete",
                        "A TOS multipart download range had an unexpected length",
                    ));
                }
                replace_file(&temporary, &part_path).await?;
                Ok((index, part_path, length))
            }
        });
        let downloaded = stream::iter(ranges)
            .buffer_unordered(concurrency.max(1))
            .collect::<Vec<_>>()
            .await;
        let mut parts = Vec::with_capacity(downloaded.len());
        for result in downloaded {
            parts.push(result?);
        }
        parts.sort_by_key(|part| part.0);

        let assembled = PathBuf::from(format!("{}.s3desktop.part", path.to_string_lossy()));
        let mut output = tokio::fs::File::create(&assembled).await?;
        let mut disk_guard = DiskUsageGuard::new(&assembled, disk_usage_threshold_percent)?;
        let mut assembled_size = 0_u64;
        for (_, part_path, expected) in parts {
            let mut input = tokio::fs::File::open(part_path).await?;
            let copied = tokio::io::copy(&mut input, &mut output).await?;
            disk_guard.record_write(copied)?;
            if copied != expected {
                return Err(AppError::new(
                    "download_part_corrupt",
                    "A cached TOS multipart download range has an unexpected length",
                ));
            }
            assembled_size = assembled_size.saturating_add(copied);
        }
        output.flush().await?;
        disk_guard.check_now()?;
        drop(output);
        if assembled_size != total {
            return Err(AppError::new(
                "download_size_mismatch",
                "The assembled TOS multipart download has an unexpected length",
            ));
        }
        replace_file(&assembled, path).await?;
        tokio::fs::remove_dir_all(parts_directory).await?;
        Ok(total)
    }
}

#[async_trait]
impl TosOperations for Client {
    async fn probe(&self) -> Result<CapabilitySet, AppError> {
        self.list_buckets_native().await?;
        Ok(CapabilitySet {
            list_buckets: true,
            list_objects: true,
            multipart_upload: true,
            versioning: true,
            object_lock: false,
            acl: true,
            policy: true,
            lifecycle: true,
            cors: true,
            website: true,
            tagging: true,
            server_side_encryption: true,
            provider_version: Some("native-rest".into()),
            warnings: Vec::new(),
        })
    }

    async fn list_buckets(&self) -> Result<Vec<BucketSummary>, AppError> {
        self.list_buckets_native().await
    }

    async fn bucket_is_empty(&self, bucket: &str) -> Result<bool, AppError> {
        let output = self
            .list_objects_native(bucket, "", None, None, 1, "ListObjectsV2")
            .await?;
        Ok(output.contents.is_empty() && output.common_prefixes.is_empty())
    }

    async fn directory_is_empty(&self, bucket: &str, prefix: &str) -> Result<bool, AppError> {
        let prefix = format!("{}/", prefix.trim().trim_end_matches('/'));
        if prefix == "/" {
            return Err(AppError::new(
                "invalid_directory_prefix",
                "An empty directory check requires a non-empty prefix",
            ));
        }
        let output = self
            .list_objects_native(bucket, &prefix, None, None, 2, "ListObjectsV2")
            .await?;
        Ok(output.contents.iter().all(|object| object.key == prefix))
    }

    async fn bucket_statistics(&self, bucket: &str) -> Result<Option<BucketStatistics>, AppError> {
        let route = self.bucket_route(bucket).await?;
        Ok(Some(
            fetch_tos_bucket_statistics(
                &route.region,
                bucket,
                &self.credentials,
                self.statistics_context(),
                &self.client,
                self.transport_base_url.as_ref(),
            )
            .await?,
        ))
    }

    async fn list_objects(
        &self,
        bucket: &str,
        prefix: &str,
        token: Option<String>,
        max_keys: i32,
    ) -> Result<ObjectPage, AppError> {
        let output = self
            .list_objects_native(bucket, prefix, Some("/"), token, max_keys, "ListObjectsV2")
            .await?;
        let mut items = Vec::with_capacity(output.contents.len() + output.common_prefixes.len());
        for common in output.common_prefixes {
            if !common.prefix.is_empty() {
                items.push(ObjectEntry {
                    name: display_name(&common.prefix, prefix),
                    key: common.prefix,
                    is_prefix: true,
                    size: 0,
                    etag: None,
                    modified_at: None,
                    storage_class: None,
                });
            }
        }
        for object in output.contents {
            if !object.key.is_empty() {
                items.push(ObjectEntry {
                    name: display_name(&object.key, prefix),
                    key: object.key,
                    is_prefix: false,
                    size: object.size,
                    etag: nonempty(object.etag),
                    modified_at: nonempty(object.last_modified),
                    storage_class: nonempty(object.storage_class),
                });
            }
        }
        Ok(ObjectPage {
            items,
            next_token: nonempty(output.next_continuation_token),
            common_prefix: prefix.to_string(),
        })
    }

    async fn list_directory_page(
        &self,
        bucket: &str,
        prefix: &str,
        token: Option<String>,
    ) -> Result<DirectoryObjectPage, AppError> {
        let output = self
            .list_objects_native(bucket, prefix, None, token, 1000, "ListObjectsV2")
            .await?;
        Ok(DirectoryObjectPage {
            items: output
                .contents
                .into_iter()
                .filter(|object| !object.key.is_empty())
                .map(|object| (object.key, object.size.max(0) as u64))
                .collect(),
            next_token: nonempty(output.next_continuation_token),
        })
    }

    async fn scan_directory_operation_plan(
        &self,
        bucket: &str,
        prefix: &str,
        cancellation: CancellationToken,
        progress: Arc<
            dyn Fn(Vec<(String, u64)>, PrefixSummary) -> Result<(), AppError> + Send + Sync,
        >,
    ) -> Result<PrefixSummary, AppError> {
        let mut token = None;
        let mut summary = PrefixSummary {
            object_count: 0,
            total_size: 0,
            request_count: 0,
        };
        loop {
            let request =
                self.list_objects_native(bucket, prefix, None, token, 1000, "ListObjectsV2");
            let output = tokio::select! {
                _ = cancellation.cancelled() => {
                    return Err(AppError::new("statistics_cancelled", "Folder deletion scan was cancelled"));
                }
                result = request => result?,
            };
            summary.request_count = summary.request_count.saturating_add(1);
            let mut items = Vec::with_capacity(output.contents.len());
            for object in output.contents {
                let size = object.size.max(0) as u64;
                summary.object_count = summary.object_count.saturating_add(1);
                summary.total_size = summary.total_size.saturating_add(size);
                items.push((object.key, size));
            }
            progress(items, summary.clone())?;
            token = nonempty(output.next_continuation_token);
            if token.is_none() {
                break;
            }
        }
        Ok(summary)
    }

    async fn object_properties(
        &self,
        bucket: &str,
        key: &str,
    ) -> Result<ObjectProperties, AppError> {
        let response = self.head_object_response(bucket, key, "HeadObject").await?;
        Ok(properties_from_headers(response.headers()))
    }

    async fn summarize_prefix(
        &self,
        bucket: &str,
        prefix: &str,
        cancellation: CancellationToken,
        progress: Arc<dyn Fn(PrefixSummary) + Send + Sync>,
    ) -> Result<PrefixSummary, AppError> {
        let mut token = None;
        let mut summary = PrefixSummary {
            object_count: 0,
            total_size: 0,
            request_count: 0,
        };
        loop {
            let request =
                self.list_objects_native(bucket, prefix, None, token, 1000, "ListObjectsV2");
            let output = tokio::select! {
                _ = cancellation.cancelled() => {
                    return Err(AppError::new("statistics_cancelled", "Folder statistics calculation was cancelled"));
                }
                result = request => result?,
            };
            summary.request_count = summary.request_count.saturating_add(1);
            for object in output.contents {
                if object.key == prefix {
                    continue;
                }
                summary.object_count = summary.object_count.saturating_add(1);
                summary.total_size = summary.total_size.saturating_add(object.size.max(0) as u64);
            }
            progress(summary.clone());
            token = nonempty(output.next_continuation_token);
            if token.is_none() {
                break;
            }
        }
        Ok(summary)
    }

    async fn create_bucket(&self, bucket: &str) -> Result<(), AppError> {
        validate_tos_identifier(bucket, "TOS bucket")?;
        let host = format!("{bucket}.{}", self.service_host()?);
        self.send_to(
            "CreateBucket",
            Some(bucket),
            Method::PUT,
            &host,
            &self.profile.region,
            "",
            BTreeMap::new(),
            BTreeMap::new(),
            None,
        )
        .await?;
        self.routes.write().await.insert(
            bucket.to_string(),
            TosBucketRoute {
                region: self.profile.region.clone(),
                endpoint: host,
            },
        );
        Ok(())
    }

    async fn delete_bucket(&self, bucket: &str) -> Result<(), AppError> {
        self.send_bucket(
            "DeleteBucket",
            Method::DELETE,
            bucket,
            "",
            BTreeMap::new(),
            BTreeMap::new(),
            None,
        )
        .await?;
        self.routes.write().await.remove(bucket);
        Ok(())
    }

    async fn create_folder(&self, bucket: &str, key: &str) -> Result<(), AppError> {
        let key = if key.ends_with('/') {
            key.to_string()
        } else {
            format!("{key}/")
        };
        self.put_bytes(
            bucket,
            &key,
            Vec::new(),
            BTreeMap::from([("content-type".into(), "application/x-directory".into())]),
            "PutObject",
        )
        .await?;
        Ok(())
    }

    async fn delete_objects(&self, bucket: &str, keys: Vec<String>) -> Result<(), AppError> {
        for chunk in keys.chunks(1000) {
            let body = serde_json::to_vec(&TosDeleteObjectsRequest {
                objects: chunk.iter().map(|key| TosDeleteObject { key }).collect(),
                quiet: true,
            })
            .map_err(|error| {
                AppError::new(
                    "invalid_delete_request",
                    "Unable to build the delete request",
                )
                .with_details(error.to_string())
            })?;
            let response = self
                .send_bucket(
                    "DeleteMultiObjects",
                    Method::POST,
                    bucket,
                    "",
                    BTreeMap::from([("delete".into(), String::new())]),
                    content_headers(&body, "application/json"),
                    Some(reqwest::Body::from(body)),
                )
                .await?;
            let output: TosDeleteObjectsResponse =
                parse_json_response("Delete objects", response).await?;
            if let Some(error) = output.errors.first() {
                return Err(AppError::new(
                    "provider_error",
                    "TOS did not delete every requested object",
                )
                .with_details(format!(
                    "key={} | code={} | message={}",
                    redact(&error.key),
                    redact(&error.code),
                    redact(&error.message)
                )));
            }
        }
        Ok(())
    }

    async fn upload_with_progress(
        &self,
        bucket: &str,
        key: &str,
        path: &Path,
        request_permits: Arc<Semaphore>,
        multipart_threshold: u64,
        progress: Arc<dyn Fn(u64) + Send + Sync>,
    ) -> Result<u64, AppError> {
        let metadata = tokio::fs::metadata(path).await?;
        if metadata.len() > multipart_threshold {
            return self
                .multipart_upload_file(bucket, key, path, metadata.len(), request_permits, progress)
                .await;
        }
        let _permit = upload_request_permit(&request_permits).await?;
        let file = tokio::fs::File::open(path).await.map_err(|error| {
            AppError::new("file_read_failed", "Unable to open the local upload file")
                .with_details(error.to_string())
        })?;
        let body = reqwest::Body::wrap_stream(ReaderStream::new(file));
        self.send_bucket(
            "PutObject",
            Method::PUT,
            bucket,
            key,
            BTreeMap::new(),
            BTreeMap::from([("content-length".into(), metadata.len().to_string())]),
            Some(body),
        )
        .await?;
        progress(metadata.len());
        Ok(metadata.len())
    }

    async fn download(
        &self,
        bucket: &str,
        key: &str,
        path: &Path,
        concurrency: usize,
        multipart_threshold: u64,
        disk_usage_threshold_percent: u8,
    ) -> Result<DownloadResult, AppError> {
        let head = self.head_object_response(bucket, key, "HeadObject").await?;
        let size = content_length(head.headers());
        let etag = header_string(head.headers(), "etag");
        if size > multipart_threshold {
            let bytes = self
                .parallel_download(
                    bucket,
                    key,
                    path,
                    size,
                    concurrency,
                    disk_usage_threshold_percent,
                    etag.as_deref().unwrap_or_default(),
                )
                .await?;
            return Ok(DownloadResult { bytes, etag });
        }
        let mut disk_guard = DiskUsageGuard::new(path, disk_usage_threshold_percent)?;
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        let temporary = PathBuf::from(format!("{}.s3desktop.part", path.to_string_lossy()));
        let mut resume_offset = tokio::fs::metadata(&temporary)
            .await
            .map(|value| value.len())
            .unwrap_or(0);
        if resume_offset == size && size > 0 {
            replace_file(&temporary, path).await?;
            return Ok(DownloadResult { bytes: size, etag });
        }
        if resume_offset > size {
            tokio::fs::File::create(&temporary).await?;
            resume_offset = 0;
        }
        let response = self
            .get_object_response(
                bucket,
                key,
                (resume_offset > 0).then(|| format!("bytes={resume_offset}-")),
                "GetObject",
            )
            .await?;
        let resumed = resume_offset > 0 && response.status().as_u16() == 206;
        let mut options = tokio::fs::OpenOptions::new();
        options.create(true).write(true);
        if resumed {
            options.append(true);
        } else {
            options.truncate(true);
        }
        let mut file = options.open(&temporary).await?;
        let mut written = if resumed { resume_offset } else { 0 };
        let mut stream = response.bytes_stream();
        while let Some(bytes) = stream.try_next().await.map_err(|error| {
            AppError::retryable(
                "download_interrupted",
                "The object download was interrupted",
            )
            .with_details(redact(&error.to_string()))
        })? {
            file.write_all(&bytes).await?;
            disk_guard.record_write(bytes.len() as u64)?;
            written = written.saturating_add(bytes.len() as u64);
        }
        file.flush().await?;
        disk_guard.check_now()?;
        drop(file);
        replace_file(&temporary, path).await?;
        Ok(DownloadResult {
            bytes: written,
            etag,
        })
    }

    async fn preview_text(
        &self,
        bucket: &str,
        key: &str,
        limit: usize,
    ) -> Result<Vec<u8>, AppError> {
        self.preview_range(bucket, key, limit.clamp(1, 10 * 1024 * 1024), "GetObject")
            .await
    }

    async fn preview_binary(
        &self,
        bucket: &str,
        key: &str,
        limit: usize,
    ) -> Result<Vec<u8>, AppError> {
        self.preview_range(bucket, key, limit.clamp(1, 100 * 1024 * 1024), "GetObject")
            .await
    }

    async fn copy_object(
        &self,
        source_bucket: &str,
        source_key: &str,
        target_bucket: &str,
        target_key: &str,
    ) -> Result<(), AppError> {
        let source = format!("{source_bucket}/{}", encode_path(source_key));
        self.send_bucket(
            "CopyObject",
            Method::PUT,
            target_bucket,
            target_key,
            BTreeMap::new(),
            BTreeMap::from([("x-tos-copy-source".into(), source)]),
            None,
        )
        .await?;
        Ok(())
    }

    async fn transfer_object_info(
        &self,
        bucket: &str,
        key: &str,
    ) -> Result<TransferObjectInfo, AppError> {
        let response = self.head_object_response(bucket, key, "HeadObject").await?;
        let headers = response.headers();
        Ok(TransferObjectInfo {
            size: content_length(headers),
            content_type: header_string(headers, "content-type"),
            cache_control: header_string(headers, "cache-control"),
            content_disposition: header_string(headers, "content-disposition"),
            content_encoding: header_string(headers, "content-encoding"),
            content_language: header_string(headers, "content-language"),
            metadata: tos_metadata(headers),
            checksum_sha256: header_string(headers, "x-tos-hash-sha256"),
            checksum_crc32c: header_string(headers, "x-tos-hash-crc32c"),
        })
    }

    async fn stream_object(
        &self,
        bucket: &str,
        key: &str,
        mut writer: DuplexStream,
        stall_timeout_seconds: u64,
        limiter: TransferBandwidthLimiter,
        progress: Arc<dyn Fn(u64) + Send + Sync>,
    ) -> Result<(), AppError> {
        let response = self
            .get_object_response(bucket, key, None, "GetObject")
            .await?;
        let stall = Duration::from_secs(stall_timeout_seconds.max(1));
        let mut stream = response.bytes_stream();
        loop {
            let next = tokio::time::timeout(stall, stream.try_next())
                .await
                .map_err(|_| {
                    AppError::retryable(
                        "migration_source_stalled",
                        "The migration source stopped producing data",
                    )
                })?
                .map_err(|error| {
                    AppError::retryable(
                        "migration_source_interrupted",
                        "The migration source stream was interrupted",
                    )
                    .with_details(redact(&error.to_string()))
                })?;
            let Some(bytes) = next else { break };
            limiter.consume(bytes.len() as u64).await;
            tokio::time::timeout(stall, writer.write_all(&bytes))
                .await
                .map_err(|_| {
                    AppError::retryable(
                        "migration_destination_stalled",
                        "The migration destination stopped accepting data",
                    )
                })??;
            progress(bytes.len() as u64);
        }
        writer.shutdown().await?;
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    async fn upload_transfer_stream(
        &self,
        bucket: &str,
        key: &str,
        mut reader: DuplexStream,
        info: TransferObjectInfo,
        multipart_threshold: u64,
        requested_part_size: u64,
        part_concurrency: usize,
        stall_timeout_seconds: u64,
        memory_budget: Arc<Semaphore>,
    ) -> Result<(), AppError> {
        let stall = Duration::from_secs(stall_timeout_seconds.max(1));
        let headers = transfer_headers(&info);
        if info.size <= multipart_threshold {
            let permits = u32::try_from(info.size.div_ceil(1024 * 1024)).map_err(|_| {
                AppError::new(
                    "migration_memory_limit_invalid",
                    "Migration object size exceeds the supported memory limit",
                )
            })?;
            let _permit = if permits == 0 {
                None
            } else {
                Some(
                    memory_budget
                        .clone()
                        .acquire_many_owned(permits)
                        .await
                        .map_err(|_| {
                            AppError::new(
                                "migration_cancelled",
                                "The migration memory budget was closed",
                            )
                        })?,
                )
            };
            let mut bytes = vec![
                0;
                usize::try_from(info.size).map_err(|_| {
                    AppError::new(
                        "migration_memory_limit_invalid",
                        "Migration object size exceeds this platform's address space",
                    )
                })?
            ];
            tokio::time::timeout(stall, reader.read_exact(&mut bytes))
                .await
                .map_err(|_| {
                    AppError::retryable(
                        "migration_source_stalled",
                        "The migration source stopped producing data",
                    )
                })??;
            self.put_bytes(bucket, key, bytes, headers, "PutObject")
                .await?;
            return Ok(());
        }

        let part_size = requested_part_size
            .max(5 * 1024 * 1024)
            .max(info.size.div_ceil(9_999));
        let upload_id = self
            .create_multipart(bucket, key, headers, "CreateMultipartUpload")
            .await?;
        let mut uploaded = tokio::task::JoinSet::new();
        let mut completed = Vec::new();
        let mut offset = 0_u64;
        let mut part_number = 1_i32;
        let result: Result<(), AppError> = async {
            while offset < info.size {
                while uploaded.len() >= part_concurrency.max(1) {
                    completed.push(
                        uploaded
                            .join_next()
                            .await
                            .expect("active multipart upload")
                            .map_err(|error| {
                                AppError::new(
                                    "migration_part_task_failed",
                                    "A destination multipart worker stopped unexpectedly",
                                )
                                .with_details(error.to_string())
                            })??,
                    );
                }
                let length = part_size.min(info.size - offset);
                let permits = u32::try_from(length.div_ceil(1024 * 1024)).map_err(|_| {
                    AppError::new(
                        "migration_memory_limit_invalid",
                        "Migration part size exceeds the supported memory limit",
                    )
                })?;
                let permit = memory_budget
                    .clone()
                    .acquire_many_owned(permits)
                    .await
                    .map_err(|_| {
                        AppError::new(
                            "migration_cancelled",
                            "The migration memory budget was closed",
                        )
                    })?;
                let mut bytes = vec![
                    0;
                    usize::try_from(length).map_err(|_| {
                        AppError::new(
                            "migration_memory_limit_invalid",
                            "Migration part size exceeds this platform's address space",
                        )
                    })?
                ];
                tokio::time::timeout(stall, reader.read_exact(&mut bytes))
                    .await
                    .map_err(|_| {
                        AppError::retryable(
                            "migration_source_stalled",
                            "The migration source stopped producing data",
                        )
                    })??;
                let provider = self.clone();
                let target_bucket = bucket.to_string();
                let target_key = key.to_string();
                let target_upload_id = upload_id.clone();
                let current_part = part_number;
                uploaded.spawn(async move {
                    let _permit = permit;
                    tokio::time::timeout(
                        stall,
                        provider.upload_part(
                            &target_bucket,
                            &target_key,
                            &target_upload_id,
                            current_part,
                            bytes,
                            "UploadPart",
                        ),
                    )
                    .await
                    .map_err(|_| {
                        AppError::retryable(
                            "migration_destination_stalled",
                            "A destination multipart upload stopped responding",
                        )
                    })?
                });
                offset = offset.saturating_add(length);
                part_number = part_number.saturating_add(1);
            }
            while let Some(part) = uploaded.join_next().await {
                completed.push(part.map_err(|error| {
                    AppError::new(
                        "migration_part_task_failed",
                        "A destination multipart worker stopped unexpectedly",
                    )
                    .with_details(error.to_string())
                })??);
            }
            completed.sort_by_key(|part| part.part_number);
            self.complete_multipart(bucket, key, &upload_id, &completed)
                .await
        }
        .await;
        if result.is_err() {
            uploaded.abort_all();
            self.abort_multipart(bucket, key, &upload_id).await;
        }
        result
    }

    async fn verify_transfer_object(
        &self,
        bucket: &str,
        key: &str,
        source: &TransferObjectInfo,
    ) -> Result<(), AppError> {
        let target = self.transfer_object_info(bucket, key).await?;
        if target.size != source.size {
            return Err(AppError::new(
                "migration_size_mismatch",
                "The migrated object size does not match the source",
            )
            .with_details(format!("source={} target={}", source.size, target.size)));
        }
        let mismatch = source
            .checksum_sha256
            .as_ref()
            .zip(target.checksum_sha256.as_ref())
            .is_some_and(|(left, right)| left != right)
            || source
                .checksum_crc32c
                .as_ref()
                .zip(target.checksum_crc32c.as_ref())
                .is_some_and(|(left, right)| left != right);
        if mismatch {
            return Err(AppError::new(
                "migration_checksum_mismatch",
                "The migrated object checksum does not match the source",
            ));
        }
        Ok(())
    }

    async fn presign_get(
        &self,
        bucket: &str,
        key: &str,
        expires_seconds: u64,
    ) -> Result<String, AppError> {
        let route = self.bucket_route(bucket).await?;
        presign_tos_get(
            self.scheme(),
            &route.endpoint,
            &route.region,
            key,
            expires_seconds.clamp(60, 604_800),
            &self.credentials,
            Utc::now(),
        )
    }

    async fn get_bucket_policy(&self, bucket: &str) -> Result<Option<String>, AppError> {
        let result = self
            .send_bucket(
                "GetBucketPolicy",
                Method::GET,
                bucket,
                "",
                BTreeMap::from([("policy".into(), String::new())]),
                BTreeMap::new(),
                None,
            )
            .await;
        match result {
            Ok(response) => response.text().await.map(Some).map_err(|error| {
                AppError::new(
                    "provider_error",
                    "Unable to read the TOS bucket policy response",
                )
                .with_details(redact(&error.to_string()))
            }),
            Err(error)
                if error
                    .details
                    .as_deref()
                    .is_some_and(|value| value.contains("NoSuchBucketPolicy")) =>
            {
                Ok(None)
            }
            Err(error) => Err(error),
        }
    }

    async fn put_bucket_policy(&self, bucket: &str, policy: &str) -> Result<(), AppError> {
        serde_json::from_str::<serde_json::Value>(policy).map_err(|error| {
            AppError::new("invalid_policy", "Bucket policy must be valid JSON")
                .with_details(error.to_string())
        })?;
        let bytes = policy.as_bytes().to_vec();
        self.send_bucket(
            "PutBucketPolicy",
            Method::PUT,
            bucket,
            "",
            BTreeMap::from([("policy".into(), String::new())]),
            content_headers(&bytes, "application/json"),
            Some(reqwest::Body::from(bytes)),
        )
        .await?;
        Ok(())
    }

    async fn delete_bucket_policy(&self, bucket: &str) -> Result<(), AppError> {
        let result = self
            .send_bucket(
                "DeleteBucketPolicy",
                Method::DELETE,
                bucket,
                "",
                BTreeMap::from([("policy".into(), String::new())]),
                BTreeMap::new(),
                None,
            )
            .await;
        match result {
            Ok(_) => Ok(()),
            Err(error)
                if error
                    .details
                    .as_deref()
                    .is_some_and(|value| value.contains("NoSuchBucketPolicy")) =>
            {
                Ok(())
            }
            Err(error) => Err(error),
        }
    }

    async fn get_bucket_versioning(&self, bucket: &str) -> Result<Option<String>, AppError> {
        let response = self
            .send_bucket(
                "GetBucketVersioning",
                Method::GET,
                bucket,
                "",
                BTreeMap::from([("versioning".into(), String::new())]),
                BTreeMap::new(),
                None,
            )
            .await?;
        let output: TosVersioningResponse =
            parse_json_response("Get bucket versioning", response).await?;
        Ok(nonempty(output.status))
    }

    async fn set_bucket_versioning(&self, bucket: &str, enabled: bool) -> Result<(), AppError> {
        let bytes = serde_json::to_vec(&TosVersioningRequest {
            status: if enabled { "Enabled" } else { "Suspended" },
        })
        .map_err(|error| {
            AppError::new(
                "provider_error",
                "Unable to serialize TOS versioning configuration",
            )
            .with_details(error.to_string())
        })?;
        self.send_bucket(
            "PutBucketVersioning",
            Method::PUT,
            bucket,
            "",
            BTreeMap::from([("versioning".into(), String::new())]),
            content_headers(&bytes, "application/json"),
            Some(reqwest::Body::from(bytes)),
        )
        .await?;
        Ok(())
    }
}

async fn resolve_credentials(
    profile: &ConnectionProfile,
    stored_secret: Option<SecretBundle>,
) -> Result<SecretBundle, AppError> {
    if profile.credential_source == "stored" {
        return stored_secret.ok_or_else(|| {
            AppError::new("credentials_missing", "Stored credentials were not found")
        });
    }
    if !matches!(
        profile.credential_source.as_str(),
        "profile" | "environment"
    ) {
        return Err(AppError::new(
            "invalid_credential_source",
            format!(
                "Unsupported credential source: {}",
                profile.credential_source
            ),
        ));
    }
    let mut loader = aws_config::defaults(BehaviorVersion::latest())
        .region(aws_types::region::Region::new(profile.region.clone()));
    if profile.credential_source == "profile" {
        loader = loader.profile_name(
            profile
                .aws_profile
                .clone()
                .unwrap_or_else(|| "default".into()),
        );
    }
    let config = loader.load().await;
    let provider = config.credentials_provider().ok_or_else(|| {
        AppError::new(
            "credentials_missing",
            "No credentials provider is available for this TOS connection",
        )
    })?;
    let credentials = provider.provide_credentials().await.map_err(|error| {
        AppError::new(
            "credentials_missing",
            "Unable to resolve credentials for this TOS connection",
        )
        .with_details(redact(&error.to_string()))
    })?;
    Ok(SecretBundle {
        access_key: credentials.access_key_id().to_string(),
        secret_key: credentials.secret_access_key().to_string(),
        session_token: credentials.session_token().map(ToString::to_string),
    })
}

pub(crate) fn validate_tos_identifier(value: &str, label: &str) -> Result<(), AppError> {
    if value.is_empty()
        || value.len() > 128
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'.' | b'_'))
    {
        return Err(AppError::new(
            "invalid_tos_target",
            format!("{label} contains unsupported characters"),
        ));
    }
    Ok(())
}

fn validate_tos_host(host: &str) -> Result<(), AppError> {
    if host.is_empty()
        || host.len() > 253
        || !host
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'.'))
        || !host.ends_with(".volces.com")
    {
        return Err(AppError::new(
            "invalid_tos_endpoint",
            "The TOS API returned an invalid endpoint",
        ));
    }
    Ok(())
}

fn native_service_endpoint(value: &str, region: &str) -> Result<Url, AppError> {
    validate_tos_identifier(region, "TOS region")?;
    let endpoint = Url::parse(value).map_err(|_| {
        AppError::new(
            "invalid_tos_endpoint",
            "The native TOS endpoint must be a valid absolute URL",
        )
    })?;
    let host = endpoint.host_str().ok_or_else(|| {
        AppError::new(
            "invalid_tos_endpoint",
            "The native TOS endpoint must contain a host",
        )
    })?;
    if !matches!(endpoint.scheme(), "http" | "https")
        || endpoint.username() != ""
        || endpoint.password().is_some()
        || endpoint.path() != "/"
        || endpoint.query().is_some()
        || endpoint.fragment().is_some()
    {
        return Err(AppError::new(
            "invalid_tos_endpoint",
            "The native TOS endpoint must be an HTTP(S) origin",
        ));
    }
    if host.starts_with("tos-s3-") {
        return Err(AppError::new(
            "s3_compatible_endpoint_not_native",
            "The configured endpoint is the TOS S3-compatible API; use a native tos-<region>.volces.com endpoint",
        ));
    }
    let expected_host = format!("tos-{region}.volces.com");
    if host != expected_host {
        return Err(AppError::new(
            "native_tos_endpoint_region_mismatch",
            format!("The native TOS endpoint must match the configured region: {expected_host}"),
        ));
    }
    validate_tos_host(host)?;
    Ok(endpoint)
}

fn normalize_bucket_endpoint(
    bucket: &str,
    region: &str,
    extranet_endpoint: &str,
    intranet_endpoint: &str,
) -> Result<String, AppError> {
    validate_tos_identifier(bucket, "TOS bucket")?;
    validate_tos_identifier(region, "TOS region")?;
    let provided = [extranet_endpoint, intranet_endpoint]
        .into_iter()
        .find(|value| !value.trim().is_empty())
        .map(str::trim)
        .map(|value| value.trim_start_matches("https://"))
        .map(|value| value.trim_start_matches("http://"))
        .map(|value| value.trim_end_matches('/'));
    let service = provided
        .filter(|value| !value.is_empty())
        .map(ToString::to_string)
        .unwrap_or_else(|| format!("tos-{region}.volces.com"));
    validate_tos_host(&service)?;
    let host = if service.starts_with(&format!("{bucket}.")) {
        service
    } else {
        format!("{bucket}.{service}")
    };
    validate_tos_host(&host)?;
    Ok(host)
}

fn nonempty(value: String) -> Option<String> {
    (!value.trim().is_empty()).then_some(value)
}

fn tos_encode(value: &str, preserve_slash: bool) -> String {
    let mut encoded = String::with_capacity(value.len());
    for byte in value.bytes() {
        let unreserved = byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'~');
        if unreserved || (preserve_slash && byte == b'/') {
            encoded.push(char::from(byte));
        } else {
            encoded.push_str(&format!("%{byte:02X}"));
        }
    }
    encoded
}

fn encode_path(key: &str) -> String {
    tos_encode(key.trim_start_matches('/'), true)
}

fn canonical_query(query: &BTreeMap<String, String>) -> String {
    query
        .iter()
        .map(|(name, value)| format!("{}={}", tos_encode(name, false), tos_encode(value, false)))
        .collect::<Vec<_>>()
        .join("&")
}

fn canonical_headers(headers: &BTreeMap<String, String>) -> (String, String) {
    let normalized = headers
        .iter()
        .filter(|(name, _)| {
            let name = name.to_ascii_lowercase();
            name == "host" || name == "content-type" || name.starts_with("x-tos-")
        })
        .map(|(name, value)| {
            (
                name.to_ascii_lowercase(),
                value.split_whitespace().collect::<Vec<_>>().join(" "),
            )
        })
        .collect::<BTreeMap<_, _>>();
    let signed = normalized.keys().cloned().collect::<Vec<_>>().join(";");
    let values = normalized
        .iter()
        .map(|(name, value)| format!("{name}:{value}\n"))
        .collect::<String>();
    (values, signed)
}

fn signing_key(secret: &str, date: &str, region: &str) -> Result<Vec<u8>, AppError> {
    let date_key = hmac_sha256(secret.as_bytes(), date.as_bytes())?;
    let region_key = hmac_sha256(&date_key, region.as_bytes())?;
    let service_key = hmac_sha256(&region_key, b"tos")?;
    hmac_sha256(&service_key, b"request")
}

fn hmac_sha256(key: &[u8], value: &[u8]) -> Result<Vec<u8>, AppError> {
    let mut mac = HmacSha256::new_from_slice(key)
        .map_err(|_| AppError::new("tos_signing_failed", "Unable to sign the TOS API request"))?;
    mac.update(value);
    Ok(mac.finalize().into_bytes().to_vec())
}

#[allow(clippy::too_many_arguments)]
fn sign_tos_request(
    method: &str,
    key: &str,
    query: &BTreeMap<String, String>,
    headers: &BTreeMap<String, String>,
    region: &str,
    credentials: &SecretBundle,
    now: DateTime<Utc>,
    payload_hash: &str,
) -> Result<String, AppError> {
    let request_date = now.format("%Y%m%dT%H%M%SZ").to_string();
    let date = now.format("%Y%m%d").to_string();
    let (canonical_headers, signed_headers) = canonical_headers(headers);
    let canonical_request = format!(
        "{method}\n/{}\n{}\n{canonical_headers}\n{signed_headers}\n{payload_hash}",
        encode_path(key),
        canonical_query(query)
    );
    let scope = format!("{date}/{region}/tos/request");
    let canonical_hash = hex::encode(Sha256::digest(canonical_request.as_bytes()));
    let string_to_sign = format!("TOS4-HMAC-SHA256\n{request_date}\n{scope}\n{canonical_hash}");
    let signature = hex::encode(hmac_sha256(
        &signing_key(&credentials.secret_key, &date, region)?,
        string_to_sign.as_bytes(),
    )?);
    Ok(format!(
        "TOS4-HMAC-SHA256 Credential={}/{scope},SignedHeaders={signed_headers},Signature={signature}",
        credentials.access_key
    ))
}

fn build_url(scheme: &str, host: &str, key: &str, query: &BTreeMap<String, String>) -> String {
    let mut url = format!("{scheme}://{host}/{}", encode_path(key));
    if !query.is_empty() {
        url.push('?');
        url.push_str(&canonical_query(query));
    }
    url
}

async fn ensure_success(operation: &str, response: Response) -> Result<Response, AppError> {
    if response.status().is_success() {
        return Ok(response);
    }
    let status = response.status();
    let request_id = header_string(response.headers(), "x-tos-request-id");
    let bytes = response.bytes().await.unwrap_or_default();
    let parsed = serde_json::from_slice::<TosErrorResponse>(&bytes).unwrap_or_default();
    let code = if status.as_u16() == 401 || status.as_u16() == 403 {
        "access_denied"
    } else if status.as_u16() == 404 {
        "not_found"
    } else if status.as_u16() == 409 {
        "provider_conflict"
    } else if status.as_u16() == 429 || status.is_server_error() {
        "provider_unavailable"
    } else {
        "provider_error"
    };
    let message = format!("{operation} failed for the TOS service");
    let mut error = if status.as_u16() == 429 || status.is_server_error() {
        AppError::retryable(code, message)
    } else {
        AppError::new(code, message)
    };
    error.details = Some(
        [
            Some(format!("HTTP {}", status.as_u16())),
            nonempty(parsed.code).map(|value| format!("code={}", redact(&value))),
            nonempty(parsed.message).map(|value| format!("message={}", redact(&value))),
            request_id.map(|value| format!("request_id={}", redact(&value))),
        ]
        .into_iter()
        .flatten()
        .collect::<Vec<_>>()
        .join(" | "),
    );
    Err(error)
}

pub(crate) async fn parse_json_response<T: DeserializeOwned>(
    operation: &str,
    response: Response,
) -> Result<T, AppError> {
    response.json::<T>().await.map_err(|error| {
        AppError::new(
            "provider_invalid_response",
            format!("{operation} returned an invalid TOS response"),
        )
        .with_details(redact(&error.to_string()))
    })
}

pub(crate) fn content_headers(bytes: &[u8], content_type: &str) -> BTreeMap<String, String> {
    let digest = Md5::digest(bytes);
    BTreeMap::from([
        ("content-length".into(), bytes.len().to_string()),
        ("content-md5".into(), BASE64.encode(digest)),
        ("content-type".into(), content_type.into()),
    ])
}

pub(crate) fn header_string(headers: &HeaderMap, name: &str) -> Option<String> {
    headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .map(ToString::to_string)
        .filter(|value| !value.is_empty())
}

fn content_length(headers: &HeaderMap) -> u64 {
    header_string(headers, "content-length")
        .and_then(|value| value.parse().ok())
        .unwrap_or(0)
}

fn properties_from_headers(headers: &HeaderMap) -> ObjectProperties {
    let mut common = Vec::new();
    let mut additional = Vec::new();
    for name in [
        "content-length",
        "last-modified",
        "etag",
        "content-type",
        "x-tos-storage-class",
        "x-tos-version-id",
        "x-tos-server-side-encryption",
    ] {
        if let Some(value) = header_string(headers, name) {
            common.push(ObjectProperty {
                name: name.to_string(),
                value,
            });
        }
    }
    let common_names = common
        .iter()
        .map(|property| property.name.as_str())
        .collect::<Vec<_>>();
    for (name, value) in headers {
        let name = name.as_str();
        if !common_names.contains(&name)
            && (name.starts_with("x-tos-")
                || matches!(
                    name,
                    "accept-ranges"
                        | "cache-control"
                        | "content-disposition"
                        | "content-encoding"
                        | "content-language"
                        | "content-range"
                        | "expires"
                ))
        {
            if let Ok(value) = value.to_str() {
                additional.push(ObjectProperty {
                    name: name.to_string(),
                    value: value.to_string(),
                });
            }
        }
    }
    additional.sort_by(|left, right| left.name.cmp(&right.name));
    ObjectProperties { common, additional }
}

fn tos_metadata(headers: &HeaderMap) -> HashMap<String, String> {
    headers
        .iter()
        .filter_map(|(name, value)| {
            name.as_str()
                .strip_prefix("x-tos-meta-")
                .zip(value.to_str().ok())
                .map(|(name, value)| (name.to_string(), value.to_string()))
        })
        .collect()
}

async fn replace_file(source: &Path, target: &Path) -> Result<(), AppError> {
    if tokio::fs::try_exists(target).await.unwrap_or(false) {
        tokio::fs::remove_file(target).await?;
    }
    tokio::fs::rename(source, target).await?;
    Ok(())
}

fn transfer_headers(info: &TransferObjectInfo) -> BTreeMap<String, String> {
    let mut headers = BTreeMap::new();
    for (name, value) in [
        ("content-type", info.content_type.as_ref()),
        ("cache-control", info.cache_control.as_ref()),
        ("content-disposition", info.content_disposition.as_ref()),
        ("content-encoding", info.content_encoding.as_ref()),
        ("content-language", info.content_language.as_ref()),
    ] {
        if let Some(value) = value.filter(|value| !value.is_empty()) {
            headers.insert(name.into(), value.clone());
        }
    }
    for (name, value) in &info.metadata {
        headers.insert(format!("x-tos-meta-{name}"), value.clone());
    }
    headers
}

#[allow(clippy::too_many_arguments)]
fn presign_tos_get(
    scheme: &str,
    host: &str,
    region: &str,
    key: &str,
    expires_seconds: u64,
    credentials: &SecretBundle,
    now: DateTime<Utc>,
) -> Result<String, AppError> {
    validate_tos_host(host)?;
    let request_date = now.format("%Y%m%dT%H%M%SZ").to_string();
    let date = now.format("%Y%m%d").to_string();
    let scope = format!("{date}/{region}/tos/request");
    let mut query = BTreeMap::from([
        ("X-Tos-Algorithm".into(), "TOS4-HMAC-SHA256".into()),
        (
            "X-Tos-Credential".into(),
            format!("{}/{}", credentials.access_key, scope),
        ),
        ("X-Tos-Date".into(), request_date.clone()),
        ("X-Tos-Expires".into(), expires_seconds.to_string()),
        ("X-Tos-SignedHeaders".into(), "host".into()),
    ]);
    if let Some(token) = credentials
        .session_token
        .as_deref()
        .filter(|value| !value.is_empty())
    {
        query.insert("X-Tos-Security-Token".into(), token.into());
    }
    let canonical = format!(
        "GET\n/{}\n{}\nhost:{host}\n\nhost\n{UNSIGNED_PAYLOAD}",
        encode_path(key),
        canonical_query(&query)
    );
    let canonical_hash = hex::encode(Sha256::digest(canonical.as_bytes()));
    let string_to_sign = format!("TOS4-HMAC-SHA256\n{request_date}\n{scope}\n{canonical_hash}");
    let signature = hex::encode(hmac_sha256(
        &signing_key(&credentials.secret_key, &date, region)?,
        string_to_sign.as_bytes(),
    )?);
    query.insert("X-Tos-Signature".into(), signature);
    Ok(build_url(scheme, host, key, &query))
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    use std::sync::Mutex;
    use tempfile::tempdir;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use wiremock::{
        matchers::{header, header_exists, method, path, query_param, query_param_is_missing},
        Mock, MockBuilder, MockServer, ResponseTemplate,
    };

    fn credentials() -> SecretBundle {
        SecretBundle {
            access_key: "test-ak".into(),
            secret_key: "test-sk".into(),
            session_token: None,
        }
    }

    async fn test_client(server: &MockServer) -> Client {
        let client = Client::new(
            ConnectionProfile::new("cn-beijing"),
            Some(credentials()),
            None,
        )
        .await
        .unwrap()
        .with_transport_base_url(Url::parse(&server.uri()).unwrap());
        client.routes.write().await.insert(
            "test-bucket".into(),
            TosBucketRoute {
                region: "cn-beijing".into(),
                endpoint: "test-bucket.tos-cn-beijing.volces.com".into(),
            },
        );
        client
    }

    #[tokio::test]
    async fn registered_bucket_route_skips_account_discovery() {
        let server = MockServer::start().await;
        signed_get("/")
            .and(query_param("list-type", "2"))
            .and(query_param("prefix", "vdb/data_mining/test/tossdk/"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "Contents": [],
                "CommonPrefixes": [],
                "NextContinuationToken": ""
            })))
            .expect(1)
            .mount(&server)
            .await;
        let client = Client::new(
            ConnectionProfile::new("cn-beijing"),
            Some(credentials()),
            None,
        )
        .await
        .unwrap()
        .with_transport_base_url(Url::parse(&server.uri()).unwrap());
        client
            .register_bucket_route(
                "it-audit",
                "cn-beijing",
                "it-audit.tos-cn-beijing.volces.com",
            )
            .await
            .unwrap();

        client
            .list_objects("it-audit", "vdb/data_mining/test/tossdk/", None, 1)
            .await
            .unwrap();
    }

    fn signed_get(path_value: &str) -> MockBuilder {
        Mock::given(method("GET"))
            .and(path(path_value))
            .and(header_exists("authorization"))
            .and(header_exists("x-tos-date"))
    }

    #[tokio::test]
    async fn list_buckets_uses_the_configured_native_service_endpoint() {
        let server = MockServer::start().await;
        signed_get("/")
            .and(header("host", "tos-cn-beijing.volces.com"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "Buckets": []
            })))
            .expect(1)
            .mount(&server)
            .await;
        let client = Client::new(
            ConnectionProfile::new("cn-beijing"),
            Some(credentials()),
            None,
        )
        .await
        .unwrap()
        .with_transport_base_url(Url::parse(&server.uri()).unwrap());

        assert!(client.list_buckets().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn rejects_an_s3_compatible_endpoint_for_the_native_client() {
        let mut profile = ConnectionProfile::new("cn-beijing");
        profile.endpoint = "https://tos-s3-cn-beijing.volces.com".into();

        let error = Client::new(profile, Some(credentials()), None)
            .await
            .err()
            .unwrap();

        assert_eq!(error.code, "s3_compatible_endpoint_not_native");
    }

    #[tokio::test]
    async fn rejects_a_native_endpoint_that_does_not_match_the_region() {
        let mut profile = ConnectionProfile::new("cn-beijing");
        profile.endpoint = "https://tos-cn-shanghai.volces.com".into();

        let error = Client::new(profile, Some(credentials()), None)
            .await
            .err()
            .unwrap();

        assert_eq!(error.code, "native_tos_endpoint_region_mismatch");
    }

    #[test]
    fn list_buckets_response_retains_native_route_fields() {
        let parsed: TosListBucketsResponse = serde_json::from_str(
            r#"{"Buckets":[{"Name":"ads-static","CreationDate":"2026-01-01T00:00:00Z","Location":"cn-beijing-selfdrive","ExtranetEndpoint":"tos-cn-beijing-selfdrive.volces.com"}]}"#,
        )
        .unwrap();
        let bucket = &parsed.buckets[0];
        assert_eq!(bucket.name, "ads-static");
        assert_eq!(bucket.location, "cn-beijing-selfdrive");
        assert_eq!(
            normalize_bucket_endpoint(
                &bucket.name,
                &bucket.location,
                &bucket.extranet_endpoint,
                &bucket.intranet_endpoint,
            )
            .unwrap(),
            "ads-static.tos-cn-beijing-selfdrive.volces.com"
        );
    }

    #[test]
    fn canonical_url_preserves_path_separators_and_sorts_query() {
        let query = BTreeMap::from([
            ("prefix".into(), "\u{4f60}\u{597d}/a b".into()),
            ("list-type".into(), "2".into()),
        ]);
        assert_eq!(
            build_url(
                "https",
                "bucket.tos-cn-beijing.volces.com",
                "a b/\u{4f60}\u{597d}.jpg",
                &query,
            ),
            "https://bucket.tos-cn-beijing.volces.com/a%20b/%E4%BD%A0%E5%A5%BD.jpg?list-type=2&prefix=%E4%BD%A0%E5%A5%BD%2Fa%20b"
        );
    }

    #[test]
    fn presigned_url_uses_native_bucket_region_and_host() {
        let now = Utc.with_ymd_and_hms(2026, 9, 10, 3, 4, 5).unwrap();
        let url = presign_tos_get(
            "https",
            "ads-static.tos-cn-beijing-selfdrive.volces.com",
            "cn-beijing-selfdrive",
            "folder/a b.jpg",
            900,
            &credentials(),
            now,
        )
        .unwrap();
        assert!(url.starts_with(
            "https://ads-static.tos-cn-beijing-selfdrive.volces.com/folder/a%20b.jpg?"
        ));
        assert!(url.contains(
            "X-Tos-Credential=test-ak%2F20260910%2Fcn-beijing-selfdrive%2Ftos%2Frequest"
        ));
        assert!(url.contains("X-Tos-Signature="));
    }

    #[test]
    fn request_signature_matches_the_official_tos_sdk_vector() {
        let now = Utc.with_ymd_and_hms(2026, 8, 7, 2, 34, 23).unwrap();
        let headers = BTreeMap::from([
            ("host".into(), "it-audit.tos-cn-beijing.volces.com".into()),
            ("x-tos-date".into(), "20260807T023423Z".into()),
        ]);
        let signature = sign_tos_request(
            "GET",
            "",
            &BTreeMap::from([("stat".into(), String::new())]),
            &headers,
            "cn-beijing",
            &SecretBundle {
                access_key: "test-ak".into(),
                secret_key: "test-secret".into(),
                session_token: None,
            },
            now,
            EMPTY_SHA256,
        )
        .unwrap();
        assert_eq!(signature, "TOS4-HMAC-SHA256 Credential=test-ak/20260807/cn-beijing/tos/request,SignedHeaders=host;x-tos-date,Signature=cccd46cbb5f35713353f67bf8854f9b6ffd972bb85c6da22b64bb5ff2a9eb1e8");
    }

    #[tokio::test]
    async fn account_discovery_probe_and_delimited_listing_use_native_requests() {
        let server = MockServer::start().await;
        signed_get("/")
            .and(query_param_is_missing("list-type"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "Buckets": [{
                    "Name": "ads-static",
                    "CreationDate": "2026-01-01T00:00:00Z",
                    "Location": "cn-shanghai",
                    "ExtranetEndpoint": "tos-cn-shanghai.volces.com"
                }]
            })))
            .expect(2)
            .mount(&server)
            .await;
        signed_get("/")
            .and(query_param("list-type", "2"))
            .and(query_param("delimiter", "/"))
            .and(query_param("prefix", "folder/"))
            .and(query_param("max-keys", "25"))
            .and(header("host", "ads-static.tos-cn-shanghai.volces.com"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "Contents": [{
                    "Key": "folder/file.txt",
                    "LastModified": "2026-01-02T00:00:00Z",
                    "ETag": "etag-1",
                    "Size": 7,
                    "StorageClass": "STANDARD"
                }],
                "CommonPrefixes": [{"Prefix": "folder/sub/"}],
                "NextContinuationToken": "next-page"
            })))
            .mount(&server)
            .await;

        let client = Client::new(
            ConnectionProfile::new("cn-beijing"),
            Some(credentials()),
            None,
        )
        .await
        .unwrap()
        .with_transport_base_url(Url::parse(&server.uri()).unwrap());
        let buckets = client.list_buckets().await.unwrap();
        assert_eq!(buckets[0].location.as_deref(), Some("cn-shanghai"));
        assert!(client.probe().await.unwrap().list_buckets);
        let page = client
            .list_objects("ads-static", "folder/", None, 25)
            .await
            .unwrap();
        assert_eq!(page.items.len(), 2);
        assert_eq!(page.items[0].name, "sub");
        assert_eq!(page.items[1].name, "file.txt");
        assert_eq!(page.next_token.as_deref(), Some("next-page"));
    }

    #[tokio::test]
    async fn empty_directory_and_scan_operations_cover_pagination_and_cancellation() {
        let server = MockServer::start().await;
        signed_get("/")
            .and(query_param("list-type", "2"))
            .and(query_param("prefix", ""))
            .and(query_param("max-keys", "1"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({})))
            .mount(&server)
            .await;
        signed_get("/")
            .and(query_param("prefix", "dir/"))
            .and(query_param("max-keys", "2"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "Contents": [{"Key": "dir/", "Size": 0}]
            })))
            .mount(&server)
            .await;
        signed_get("/")
            .and(query_param("prefix", "flat/"))
            .and(query_param("max-keys", "1000"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "Contents": [
                    {"Key": "flat/a", "Size": 3},
                    {"Key": "flat/b", "Size": 4}
                ],
                "NextContinuationToken": ""
            })))
            .mount(&server)
            .await;
        signed_get("/")
            .and(query_param("prefix", "scan/"))
            .and(query_param("max-keys", "1000"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "Contents": [{"Key": "scan/a", "Size": 9}],
                "NextContinuationToken": ""
            })))
            .mount(&server)
            .await;
        signed_get("/")
            .and(query_param("prefix", "sum/"))
            .and(query_param("max-keys", "1000"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "Contents": [
                    {"Key": "sum/", "Size": 0},
                    {"Key": "sum/a", "Size": 11}
                ],
                "NextContinuationToken": ""
            })))
            .mount(&server)
            .await;

        let client = test_client(&server).await;
        assert!(client.bucket_is_empty("test-bucket").await.unwrap());
        assert!(client
            .directory_is_empty("test-bucket", "dir")
            .await
            .unwrap());
        assert_eq!(
            client
                .list_directory_page("test-bucket", "flat/", None)
                .await
                .unwrap()
                .items,
            vec![("flat/a".into(), 3), ("flat/b".into(), 4)]
        );

        let scanned = Arc::new(Mutex::new(Vec::new()));
        let scanned_copy = Arc::clone(&scanned);
        let scan = client
            .scan_directory_operation_plan(
                "test-bucket",
                "scan/",
                CancellationToken::new(),
                Arc::new(move |items, _| {
                    scanned_copy.lock().unwrap().extend(items);
                    Ok(())
                }),
            )
            .await
            .unwrap();
        assert_eq!(
            (scan.object_count, scan.total_size, scan.request_count),
            (1, 9, 1)
        );
        assert_eq!(scanned.lock().unwrap().as_slice(), &[("scan/a".into(), 9)]);

        let summaries = Arc::new(Mutex::new(Vec::new()));
        let summaries_copy = Arc::clone(&summaries);
        let summary = client
            .summarize_prefix(
                "test-bucket",
                "sum/",
                CancellationToken::new(),
                Arc::new(move |value| summaries_copy.lock().unwrap().push(value)),
            )
            .await
            .unwrap();
        assert_eq!((summary.object_count, summary.total_size), (1, 11));
        assert_eq!(summaries.lock().unwrap().len(), 1);

        let cancelled = CancellationToken::new();
        cancelled.cancel();
        let error = client
            .summarize_prefix("test-bucket", "cancel/", cancelled, Arc::new(|_| {}))
            .await
            .unwrap_err();
        assert_eq!(error.code, "statistics_cancelled");
    }

    #[tokio::test]
    async fn metadata_preview_download_and_stream_paths_preserve_bytes_and_headers() {
        let server = MockServer::start().await;
        Mock::given(method("HEAD"))
            .and(path("/meta.bin"))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header("content-length", "5")
                    .insert_header("content-type", "application/octet-stream")
                    .insert_header("etag", "etag-meta")
                    .insert_header("x-tos-meta-owner", "sdk")
                    .insert_header("x-tos-hash-sha256", "sha"),
            )
            .expect(3)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/preview.txt"))
            .and(header("range", "bytes=0-3"))
            .respond_with(ResponseTemplate::new(206).set_body_bytes(b"abcdef"))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/preview.bin"))
            .and(header("range", "bytes=0-4"))
            .respond_with(ResponseTemplate::new(206).set_body_bytes(b"12345"))
            .mount(&server)
            .await;
        Mock::given(method("HEAD"))
            .and(path("/download.bin"))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header("content-length", "8")
                    .insert_header("etag", "etag-download"),
            )
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/download.bin"))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(b"download"))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/stream.bin"))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(b"streamed"))
            .mount(&server)
            .await;

        let client = test_client(&server).await;
        let properties = client
            .object_properties("test-bucket", "meta.bin")
            .await
            .unwrap();
        assert!(properties
            .additional
            .iter()
            .any(|item| item.name == "x-tos-meta-owner" && item.value == "sdk"));
        let info = client
            .transfer_object_info("test-bucket", "meta.bin")
            .await
            .unwrap();
        assert_eq!(info.metadata.get("owner").map(String::as_str), Some("sdk"));
        client
            .verify_transfer_object("test-bucket", "meta.bin", &info)
            .await
            .unwrap();
        assert_eq!(
            client
                .preview_text("test-bucket", "preview.txt", 4)
                .await
                .unwrap(),
            b"abcd"
        );
        assert_eq!(
            client
                .preview_binary("test-bucket", "preview.bin", 5)
                .await
                .unwrap(),
            b"12345"
        );

        let directory = tempdir().unwrap();
        let target = directory.path().join("download.bin");
        let result = client
            .download("test-bucket", "download.bin", &target, 1, 1024, 99)
            .await
            .unwrap();
        assert_eq!(result.bytes, 8);
        assert_eq!(tokio::fs::read(target).await.unwrap(), b"download");

        let (writer, mut reader) = tokio::io::duplex(64);
        let stream_client = client.clone();
        let task = tokio::spawn(async move {
            stream_client
                .stream_object(
                    "test-bucket",
                    "stream.bin",
                    writer,
                    5,
                    TransferBandwidthLimiter::new(0),
                    Arc::new(|_| {}),
                )
                .await
        });
        let mut streamed = Vec::new();
        reader.read_to_end(&mut streamed).await.unwrap();
        task.await.unwrap().unwrap();
        assert_eq!(streamed, b"streamed");
    }

    #[tokio::test]
    async fn bucket_object_policy_and_versioning_mutations_use_native_shapes() {
        let server = MockServer::start().await;
        Mock::given(method("PUT"))
            .and(path("/"))
            .and(header("host", "new-bucket.tos-cn-beijing.volces.com"))
            .respond_with(ResponseTemplate::new(200))
            .mount(&server)
            .await;
        Mock::given(method("DELETE"))
            .and(path("/"))
            .and(header("host", "test-bucket.tos-cn-beijing.volces.com"))
            .respond_with(ResponseTemplate::new(204))
            .mount(&server)
            .await;
        Mock::given(method("PUT"))
            .and(path("/folder/"))
            .and(header("content-type", "application/x-directory"))
            .respond_with(ResponseTemplate::new(200))
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/"))
            .and(query_param("delete", ""))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({})))
            .mount(&server)
            .await;
        Mock::given(method("PUT"))
            .and(path("/copy.txt"))
            .and(header(
                "x-tos-copy-source",
                "source-bucket/source%20file.txt",
            ))
            .respond_with(ResponseTemplate::new(200))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/"))
            .and(query_param("policy", ""))
            .respond_with(ResponseTemplate::new(200).set_body_string(r#"{"Version":"2012-10-17"}"#))
            .mount(&server)
            .await;
        Mock::given(method("PUT"))
            .and(path("/"))
            .and(query_param("policy", ""))
            .respond_with(ResponseTemplate::new(200))
            .mount(&server)
            .await;
        Mock::given(method("DELETE"))
            .and(path("/"))
            .and(query_param("policy", ""))
            .respond_with(ResponseTemplate::new(204))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/"))
            .and(query_param("versioning", ""))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "Status": "Enabled"
            })))
            .mount(&server)
            .await;
        Mock::given(method("PUT"))
            .and(path("/"))
            .and(query_param("versioning", ""))
            .respond_with(ResponseTemplate::new(200))
            .mount(&server)
            .await;

        let client = test_client(&server).await;
        client.create_bucket("new-bucket").await.unwrap();
        client.create_folder("test-bucket", "folder").await.unwrap();
        client
            .delete_objects("test-bucket", vec!["a".into(), "b".into()])
            .await
            .unwrap();
        client
            .copy_object(
                "source-bucket",
                "source file.txt",
                "test-bucket",
                "copy.txt",
            )
            .await
            .unwrap();
        assert!(client
            .get_bucket_policy("test-bucket")
            .await
            .unwrap()
            .unwrap()
            .contains("Version"));
        client
            .put_bucket_policy("test-bucket", r#"{"Version":"2012-10-17"}"#)
            .await
            .unwrap();
        client.delete_bucket_policy("test-bucket").await.unwrap();
        assert_eq!(
            client
                .get_bucket_versioning("test-bucket")
                .await
                .unwrap()
                .as_deref(),
            Some("Enabled")
        );
        client
            .set_bucket_versioning("test-bucket", false)
            .await
            .unwrap();
        client.delete_bucket("test-bucket").await.unwrap();
    }

    #[tokio::test]
    async fn file_and_stream_uploads_cover_single_and_multipart_paths() {
        let server = MockServer::start().await;
        Mock::given(method("PUT"))
            .and(path("/small.bin"))
            .respond_with(ResponseTemplate::new(200))
            .mount(&server)
            .await;
        Mock::given(method("PUT"))
            .and(path("/stream-upload.bin"))
            .respond_with(ResponseTemplate::new(200))
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/multipart.bin"))
            .and(query_param("uploads", ""))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "UploadId": "upload-1"
            })))
            .mount(&server)
            .await;
        Mock::given(method("PUT"))
            .and(path("/multipart.bin"))
            .and(query_param("partNumber", "1"))
            .and(query_param("uploadId", "upload-1"))
            .respond_with(ResponseTemplate::new(200).insert_header("etag", "part-1"))
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/multipart.bin"))
            .and(query_param("uploadId", "upload-1"))
            .respond_with(ResponseTemplate::new(200))
            .mount(&server)
            .await;

        let client = test_client(&server).await;
        let directory = tempdir().unwrap();
        let small = directory.path().join("small.bin");
        tokio::fs::write(&small, b"small").await.unwrap();
        assert_eq!(
            client
                .upload("test-bucket", "small.bin", &small, 1, 1024)
                .await
                .unwrap(),
            5
        );
        let progress_events = Arc::new(Mutex::new(Vec::new()));
        let observed_progress = progress_events.clone();
        assert_eq!(
            client
                .upload_with_progress(
                    "test-bucket",
                    "multipart.bin",
                    &small,
                    Arc::new(Semaphore::new(2)),
                    1,
                    Arc::new(move |bytes| observed_progress.lock().unwrap().push(bytes)),
                )
                .await
                .unwrap(),
            5
        );
        assert_eq!(*progress_events.lock().unwrap(), vec![5]);

        let (mut writer, reader) = tokio::io::duplex(64);
        writer.write_all(b"stream").await.unwrap();
        writer.shutdown().await.unwrap();
        client
            .upload_transfer_stream(
                "test-bucket",
                "stream-upload.bin",
                reader,
                TransferObjectInfo {
                    size: 6,
                    content_type: Some("application/octet-stream".into()),
                    ..Default::default()
                },
                1024,
                5 * 1024 * 1024,
                1,
                5,
                Arc::new(Semaphore::new(16)),
            )
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn bucket_statistics_errors_observer_and_presigning_are_stable() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/"))
            .and(query_param("stat", ""))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "TotalStorageStat": {"Storage": "12", "ChargeStorage": "16", "ObjectCount": 2},
                "StandardStorageStat": {"Storage": "12", "ChargeStorage": "16", "ObjectCount": 2}
            })))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/denied"))
            .respond_with(ResponseTemplate::new(403).set_body_json(serde_json::json!({
                "Code": "AccessDenied",
                "Message": "denied"
            })))
            .mount(&server)
            .await;

        let events = Arc::new(Mutex::new(Vec::new()));
        let events_copy = Arc::clone(&events);
        let observer = RequestStatisticsRecorder::new(move |event| {
            events_copy.lock().unwrap().push(event);
        });
        let client = Client::new(
            ConnectionProfile::new("cn-beijing"),
            Some(credentials()),
            Some(observer),
        )
        .await
        .unwrap()
        .with_transport_base_url(Url::parse(&server.uri()).unwrap());
        client.routes.write().await.insert(
            "test-bucket".into(),
            TosBucketRoute {
                region: "cn-beijing".into(),
                endpoint: "test-bucket.tos-cn-beijing.volces.com".into(),
            },
        );
        let statistics = client
            .bucket_statistics("test-bucket")
            .await
            .unwrap()
            .unwrap();
        assert_eq!((statistics.object_count, statistics.storage_bytes), (2, 12));
        let error = client
            .preview_binary("test-bucket", "denied", 4)
            .await
            .unwrap_err();
        assert_eq!(error.code, "access_denied");
        assert!(!error.retryable);
        assert!(events
            .lock()
            .unwrap()
            .iter()
            .any(|event| event.action == "GetBucketStat"));
        let url = client
            .presign_get("test-bucket", "a b.txt", 900)
            .await
            .unwrap();
        assert!(url.starts_with("https://test-bucket.tos-cn-beijing.volces.com/a%20b.txt?"));
        assert!(url.contains("X-Tos-Signature="));
    }
}
