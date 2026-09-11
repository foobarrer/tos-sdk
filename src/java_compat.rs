//! Native implementations for Java SDK operations absent from the async Rust SDK.
//!
//! Names are idiomatic Rust while request and response fields preserve the
//! service contract used by Java SDK v2.9.19.

use crate::{
    client::{content_headers, header_string, parse_json_response},
    Client, Error,
};
use reqwest::Method;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// The deprecated marker-based object listing request used by Java `listObjects`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ListObjectsV1Input {
    /// Bucket to list.
    pub bucket: String,
    /// Optional key prefix.
    pub prefix: Option<String>,
    /// Optional grouping delimiter.
    pub delimiter: Option<String>,
    /// Optional exclusive start marker.
    pub marker: Option<String>,
    /// Maximum number of results requested from TOS.
    pub max_keys: Option<i32>,
    /// Request reverse ordering.
    pub reverse: bool,
    /// Optional response encoding, normally `url`.
    pub encoding_type: Option<String>,
}

/// Owner metadata returned by legacy object listing.
#[derive(Debug, Clone, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "PascalCase")]
pub struct ListedOwner {
    /// Account identifier.
    #[serde(rename = "ID", default)]
    pub id: String,
    /// Human-readable account name.
    #[serde(default)]
    pub display_name: String,
}

/// One object returned by legacy marker-based listing.
#[derive(Debug, Clone, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "PascalCase")]
pub struct ListedObjectV1 {
    /// Object key.
    #[serde(default)]
    pub key: String,
    /// Provider-formatted modification timestamp.
    #[serde(default)]
    pub last_modified: String,
    /// Entity tag.
    #[serde(rename = "ETag", default)]
    pub etag: String,
    /// Object size in bytes.
    #[serde(default)]
    pub size: i64,
    /// Object owner, when requested by TOS.
    #[serde(default)]
    pub owner: Option<ListedOwner>,
    /// TOS storage class.
    #[serde(default)]
    pub storage_class: String,
    /// TOS object type.
    #[serde(rename = "Type", default)]
    pub object_type: String,
    /// CRC64-ECMA checksum returned by TOS.
    #[serde(rename = "HashCrc64ecma", default)]
    pub hash_crc64_ecma: String,
    /// CRC32C checksum returned by TOS.
    #[serde(rename = "HashCrc32c", default)]
    pub hash_crc32c: String,
    /// User metadata in the native TOS response shape.
    #[serde(default)]
    pub user_meta: Vec<BTreeMap<String, String>>,
}

/// One common prefix returned by a delimited legacy listing.
#[derive(Debug, Clone, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "PascalCase")]
pub struct ListedCommonPrefixV1 {
    /// Common prefix.
    #[serde(default)]
    pub prefix: String,
    /// Provider-formatted last-modified timestamp.
    #[serde(default)]
    pub last_modified: String,
}

/// Response from the Java-compatible marker-based object listing operation.
#[derive(Debug, Clone, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "PascalCase")]
pub struct ListObjectsV1Output {
    /// HTTP and TOS request metadata.
    #[serde(skip)]
    pub request_info: RequestInfo,
    /// Bucket name.
    #[serde(default)]
    pub name: String,
    /// Applied prefix.
    #[serde(default)]
    pub prefix: String,
    /// Applied marker.
    #[serde(default)]
    pub marker: String,
    /// Applied maximum result count.
    #[serde(default)]
    pub max_keys: i32,
    /// Marker for the next page.
    #[serde(default)]
    pub next_marker: String,
    /// Applied delimiter.
    #[serde(default)]
    pub delimiter: String,
    /// Whether more results remain.
    #[serde(rename = "IsTruncated", default)]
    pub is_truncated: bool,
    /// Response encoding.
    #[serde(default)]
    pub encoding_type: String,
    /// Common prefixes in this page.
    #[serde(default)]
    pub common_prefixes: Vec<ListedCommonPrefixV1>,
    /// Objects in this page.
    #[serde(default)]
    pub contents: Vec<ListedObjectV1>,
}

/// Request used by Java `setObjectExpires`.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
#[serde(rename_all = "PascalCase")]
pub struct SetObjectExpiresInput {
    /// Bucket containing the object.
    #[serde(skip)]
    pub bucket: String,
    /// Object key.
    #[serde(skip)]
    pub key: String,
    /// Expiration timestamp or service-defined expiration value.
    pub object_expires: i64,
}

/// Response metadata for a Java-compatible mutation.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SetObjectExpiresOutput {
    /// HTTP and TOS request metadata.
    pub request_info: RequestInfo,
}

/// Java-compatible request metadata returned with every compatibility output.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RequestInfo {
    /// TOS request identifier, when returned.
    pub request_id: Option<String>,
    /// Secondary TOS request identifier, when returned.
    pub id2: Option<String>,
    /// HTTP status code.
    pub status_code: u16,
    /// Response headers, normalized to lowercase names.
    pub headers: BTreeMap<String, String>,
}

impl Client {
    /// Lists objects with the marker-based API exposed as Java `listObjects`.
    ///
    /// This is intentionally separate from [`crate::TosOperations::list_objects`],
    /// which uses the newer continuation-token protocol.
    pub async fn list_objects_v1(
        &self,
        input: &ListObjectsV1Input,
    ) -> Result<ListObjectsV1Output, Error> {
        let mut query = BTreeMap::new();
        insert_optional_query(&mut query, "prefix", input.prefix.as_deref());
        insert_optional_query(&mut query, "delimiter", input.delimiter.as_deref());
        insert_optional_query(&mut query, "marker", input.marker.as_deref());
        insert_optional_query(&mut query, "encoding-type", input.encoding_type.as_deref());
        if let Some(max_keys) = input.max_keys {
            query.insert("max-keys".into(), max_keys.to_string());
        }
        query.insert("reverse".into(), input.reverse.to_string());
        let response = self
            .send_bucket(
                "ListObjects",
                Method::GET,
                &input.bucket,
                "",
                query,
                BTreeMap::new(),
                None,
            )
            .await?;
        let request_info = request_info(&response);
        let mut output: ListObjectsV1Output = parse_json_response("ListObjects", response).await?;
        output.request_info = request_info;
        Ok(output)
    }

    /// Sets the service-side object expiration value exposed by Java
    /// `setObjectExpires`.
    pub async fn set_object_expires(
        &self,
        input: &SetObjectExpiresInput,
    ) -> Result<SetObjectExpiresOutput, Error> {
        let bytes = serde_json::to_vec(input)?;
        let response = self
            .send_bucket(
                "SetObjectExpires",
                Method::POST,
                &input.bucket,
                &input.key,
                BTreeMap::from([("objectExpires".into(), String::new())]),
                content_headers(&bytes, "application/json"),
                Some(reqwest::Body::from(bytes)),
            )
            .await?;
        Ok(SetObjectExpiresOutput {
            request_info: request_info(&response),
        })
    }
}

pub(crate) fn request_info(response: &reqwest::Response) -> RequestInfo {
    RequestInfo {
        request_id: header_string(response.headers(), "x-tos-request-id"),
        id2: header_string(response.headers(), "x-tos-id-2"),
        status_code: response.status().as_u16(),
        headers: response
            .headers()
            .iter()
            .filter_map(|(name, value)| {
                value
                    .to_str()
                    .ok()
                    .map(|value| (name.as_str().to_string(), value.to_string()))
            })
            .collect(),
    }
}

fn insert_optional_query(query: &mut BTreeMap<String, String>, name: &str, value: Option<&str>) {
    if let Some(value) = value.filter(|value| !value.is_empty()) {
        query.insert(name.to_string(), value.to_string());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ClientConfig, Credentials};
    use serde_json::json;
    use wiremock::{
        matchers::{body_json, method, path, query_param},
        Mock, MockServer, ResponseTemplate,
    };

    async fn client(server: &MockServer) -> Client {
        Client::new(
            ClientConfig::new("cn-beijing"),
            Some(Credentials::new("test-ak", "test-sk")),
            None,
        )
        .await
        .expect("client")
        .with_transport_base_url(server.uri().parse().expect("mock URL"))
    }

    #[tokio::test]
    async fn legacy_list_objects_preserves_java_query_contract() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/"))
            .and(query_param("prefix", "logs/"))
            .and(query_param("marker", "logs/001"))
            .and(query_param("max-keys", "25"))
            .and(query_param("reverse", "true"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "Name": "example",
                "Prefix": "logs/",
                "Marker": "logs/001",
                "MaxKeys": 25,
                "NextMarker": "logs/026",
                "Delimiter": "/",
                "IsTruncated": true,
                "EncodingType": "url",
                "CommonPrefixes": [{"Prefix": "logs/archive/", "LastModified": "2026-09-11T00:00:00Z"}],
                "Contents": [{
                    "Key": "logs/002",
                    "LastModified": "2026-09-11T00:00:00Z",
                    "ETag": "etag",
                    "Size": 12,
                    "Owner": {"ID": "owner-id", "DisplayName": "owner"},
                    "StorageClass": "STANDARD",
                    "Type": "Normal",
                    "HashCrc64ecma": "42",
                    "HashCrc32c": "7",
                    "UserMeta": [{"x-tos-meta-a": "b"}]
                }]
            })))
            .mount(&server)
            .await;

        let output = client(&server)
            .await
            .list_objects_v1(&ListObjectsV1Input {
                bucket: "example".into(),
                prefix: Some("logs/".into()),
                delimiter: None,
                marker: Some("logs/001".into()),
                max_keys: Some(25),
                reverse: true,
                encoding_type: None,
            })
            .await
            .expect("list objects");
        assert!(output.is_truncated);
        assert_eq!(output.next_marker, "logs/026");
        assert_eq!(
            output.contents[0].owner.as_ref().expect("owner").id,
            "owner-id"
        );
    }

    #[tokio::test]
    async fn set_object_expires_preserves_java_wire_contract() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/folder/object.txt"))
            .and(query_param("objectExpires", ""))
            .and(body_json(json!({"ObjectExpires": 1_800})))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header("x-tos-request-id", "request-1")
                    .insert_header("x-tos-id-2", "request-2"),
            )
            .mount(&server)
            .await;

        let output = client(&server)
            .await
            .set_object_expires(&SetObjectExpiresInput {
                bucket: "example".into(),
                key: "folder/object.txt".into(),
                object_expires: 1_800,
            })
            .await
            .expect("set expiration");
        assert_eq!(output.request_info.request_id.as_deref(), Some("request-1"));
        assert_eq!(output.request_info.id2.as_deref(), Some("request-2"));
        assert_eq!(output.request_info.status_code, 200);
    }
}
