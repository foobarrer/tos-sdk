use crate::{
    error::AppError,
    models::{BucketStatistics, SecretBundle, StorageClassStatistics},
    request_observer::{record_http_request, RequestStatisticsContext},
};
use chrono::{DateTime, Utc};
use hmac::{Hmac, Mac};
use serde::Deserialize;
use sha2::{Digest, Sha256};
use url::Url;

type HmacSha256 = Hmac<Sha256>;
const EMPTY_SHA256: &str = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct TosStorageStat {
    storage: String,
    charge_storage: String,
    object_count: u64,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct TosBucketStatResponse {
    total_storage_stat: TosStorageStat,
    standard_storage_stat: Option<TosStorageStat>,
    ia_storage_stat: Option<TosStorageStat>,
    archive_fr_storage_stat: Option<TosStorageStat>,
    archive_storage_stat: Option<TosStorageStat>,
    cold_archive_storage_stat: Option<TosStorageStat>,
    deep_cold_archive_storage_stat: Option<TosStorageStat>,
}

pub(crate) async fn fetch_tos_bucket_statistics(
    region: &str,
    bucket: &str,
    credentials: &SecretBundle,
    request_statistics: Option<RequestStatisticsContext<'_>>,
    client: &reqwest::Client,
    transport_base_url: Option<&Url>,
) -> Result<BucketStatistics, AppError> {
    let host = tos_bucket_host(region, bucket)?;
    let now = Utc::now();
    let authorization = tos_authorization(&host, region, credentials, now)?;
    let url = transport_base_url
        .map(|base| format!("{}/?stat=", base.as_str().trim_end_matches('/')))
        .unwrap_or_else(|| format!("https://{host}/?stat="));
    let mut request = client
        .get(url)
        .header("Host", &host)
        .header("Authorization", authorization)
        .header("X-Tos-Date", now.format("%Y%m%dT%H%M%SZ").to_string());
    if let Some(token) = credentials
        .session_token
        .as_deref()
        .filter(|value| !value.is_empty())
    {
        request = request.header("X-Tos-Security-Token", token);
    }
    record_http_request(
        request_statistics,
        Some(bucket),
        "GetBucketStat",
        "bucket-statistics",
        "reqwest",
    );
    let response = request.send().await.map_err(|_| {
        AppError::retryable(
            "bucket_statistics_unavailable",
            "Unable to reach the TOS bucket statistics endpoint",
        )
    })?;
    let status = response.status();
    if !status.is_success() {
        let (code, message) = if status.as_u16() == 401 || status.as_u16() == 403 {
            (
                "access_denied",
                "The credentials do not have permission to read TOS bucket statistics",
            )
        } else {
            (
                "bucket_statistics_failed",
                "TOS rejected the bucket statistics request",
            )
        };
        return Err(if status.as_u16() == 429 || status.is_server_error() {
            AppError::retryable(code, message)
        } else {
            AppError::new(code, message)
        });
    }
    let output = response
        .json::<TosBucketStatResponse>()
        .await
        .map_err(|_| {
            AppError::new(
                "bucket_statistics_invalid_response",
                "TOS returned an invalid bucket statistics response",
            )
        })?;
    let mut storage_classes = Vec::new();
    add_storage_class(
        &mut storage_classes,
        "STANDARD",
        output.standard_storage_stat,
    )?;
    add_storage_class(&mut storage_classes, "IA", output.ia_storage_stat)?;
    add_storage_class(
        &mut storage_classes,
        "ARCHIVE_FR",
        output.archive_fr_storage_stat,
    )?;
    add_storage_class(&mut storage_classes, "ARCHIVE", output.archive_storage_stat)?;
    add_storage_class(
        &mut storage_classes,
        "COLD_ARCHIVE",
        output.cold_archive_storage_stat,
    )?;
    add_storage_class(
        &mut storage_classes,
        "DEEP_COLD_ARCHIVE",
        output.deep_cold_archive_storage_stat,
    )?;
    Ok(BucketStatistics {
        bucket: bucket.to_string(),
        provider: "tos".into(),
        object_count: output.total_storage_stat.object_count,
        storage_bytes: parse_storage(&output.total_storage_stat.storage)?,
        charge_storage_bytes: Some(parse_storage(&output.total_storage_stat.charge_storage)?),
        storage_classes,
        collected_at: now.to_rfc3339(),
        source: "TOS GetBucketStat".into(),
    })
}

fn tos_bucket_host(region: &str, bucket: &str) -> Result<String, AppError> {
    let valid_region = !region.is_empty()
        && region
            .bytes()
            .all(|value| value.is_ascii_alphanumeric() || value == b'-');
    let valid_bucket = !bucket.is_empty()
        && bucket.bytes().all(|value| {
            value.is_ascii_lowercase() || value.is_ascii_digit() || matches!(value, b'.' | b'-')
        });
    if !valid_region || !valid_bucket {
        return Err(AppError::new(
            "invalid_bucket_statistics_target",
            "The provider Region or bucket name is invalid",
        ));
    }
    Ok(format!("{bucket}.tos-{region}.volces.com"))
}

fn tos_authorization(
    host: &str,
    region: &str,
    credentials: &SecretBundle,
    now: DateTime<Utc>,
) -> Result<String, AppError> {
    let request_date = now.format("%Y%m%dT%H%M%SZ").to_string();
    let date = now.format("%Y%m%d").to_string();
    let mut headers = vec![
        ("host", host.to_string()),
        ("x-tos-date", request_date.clone()),
    ];
    if let Some(token) = credentials
        .session_token
        .as_deref()
        .filter(|value| !value.is_empty())
    {
        headers.push(("x-tos-security-token", token.to_string()));
    }
    headers.sort_by_key(|(name, _)| *name);
    let signed_headers = headers
        .iter()
        .map(|(name, _)| *name)
        .collect::<Vec<_>>()
        .join(";");
    let canonical_headers = headers
        .iter()
        .map(|(name, value)| {
            format!(
                "{name}:{}\n",
                value.split_whitespace().collect::<Vec<_>>().join(" ")
            )
        })
        .collect::<String>();
    let canonical_request =
        format!("GET\n/\nstat=\n{canonical_headers}\n{signed_headers}\n{EMPTY_SHA256}");
    let scope = format!("{date}/{region}/tos/request");
    let canonical_hash = hex::encode(Sha256::digest(canonical_request.as_bytes()));
    let string_to_sign = format!("TOS4-HMAC-SHA256\n{request_date}\n{scope}\n{canonical_hash}");
    let date_key = hmac_sha256(credentials.secret_key.as_bytes(), date.as_bytes())?;
    let region_key = hmac_sha256(&date_key, region.as_bytes())?;
    let service_key = hmac_sha256(&region_key, b"tos")?;
    let signing_key = hmac_sha256(&service_key, b"request")?;
    let signature = hex::encode(hmac_sha256(&signing_key, string_to_sign.as_bytes())?);
    Ok(format!(
        "TOS4-HMAC-SHA256 Credential={}/{scope},SignedHeaders={signed_headers},Signature={signature}",
        credentials.access_key
    ))
}

fn hmac_sha256(key: &[u8], value: &[u8]) -> Result<Vec<u8>, AppError> {
    let mut mac = HmacSha256::new_from_slice(key).map_err(|_| {
        AppError::new(
            "bucket_statistics_signing_failed",
            "Unable to sign the TOS bucket statistics request",
        )
    })?;
    mac.update(value);
    Ok(mac.finalize().into_bytes().to_vec())
}

fn parse_storage(value: &str) -> Result<u64, AppError> {
    value.parse().map_err(|_| {
        AppError::new(
            "bucket_statistics_invalid_response",
            "TOS returned an invalid storage size",
        )
    })
}

fn add_storage_class(
    target: &mut Vec<StorageClassStatistics>,
    name: &str,
    value: Option<TosStorageStat>,
) -> Result<(), AppError> {
    let Some(value) = value else { return Ok(()) };
    target.push(StorageClassStatistics {
        name: name.into(),
        storage_bytes: parse_storage(&value.storage)?,
        charge_storage_bytes: Some(parse_storage(&value.charge_storage)?),
        object_count: value.object_count,
    });
    Ok(())
}
