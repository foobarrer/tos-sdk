//! Java-compatible TOS access-point control-plane operations.

use crate::{
    client::{content_headers, parse_json_response, validate_tos_identifier},
    java_compat::request_info,
    Client, Error, RequestInfo,
};
use reqwest::Method;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// Input for creating a TOS access point.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
#[serde(rename_all = "PascalCase")]
pub struct CreateAccessPointInput {
    /// Volcengine account identifier.
    #[serde(skip)]
    pub account_id: String,
    /// New access-point name.
    #[serde(skip)]
    pub access_point_name: String,
    /// Backing bucket.
    pub bucket: String,
    /// Bucket owner account, when different from the caller.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bucket_account_id: Option<String>,
    /// Network origin, such as `internet` or `vpc`.
    pub network_origin: String,
    /// VPC identifier for VPC-scoped access points.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub vpc_id: Option<String>,
}

/// Output from creating a TOS access point.
#[derive(Debug, Clone, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "PascalCase")]
pub struct CreateAccessPointOutput {
    /// HTTP and TOS request metadata.
    #[serde(skip)]
    pub request_info: RequestInfo,
    /// TOS resource name.
    #[serde(default)]
    pub access_point_trn: String,
    /// Generated access-point alias.
    #[serde(default)]
    pub alias: String,
}

/// Input for retrieving one access point.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GetAccessPointInput {
    /// Volcengine account identifier.
    pub account_id: String,
    /// Access-point name.
    pub access_point_name: String,
}

/// Input for deleting one access point.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeleteAccessPointInput {
    /// Volcengine account identifier.
    pub account_id: String,
    /// Access-point name.
    pub access_point_name: String,
}

/// Input for listing access points.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ListAccessPointsInput {
    /// Volcengine account identifier.
    pub account_id: String,
    /// Optional backing-bucket filter.
    pub bucket: Option<String>,
    /// Optional maximum result count. `None` preserves the service default.
    pub max_results: Option<i32>,
    /// Continuation token.
    pub next_token: Option<String>,
}

/// Public and private access-point endpoints.
#[derive(Debug, Clone, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "PascalCase")]
pub struct AccessPointEndpoints {
    /// Intranet endpoint.
    #[serde(default)]
    pub intranet_endpoint: String,
    /// Internet endpoint.
    #[serde(default)]
    pub extranet_endpoint: String,
}

/// Full access-point description.
#[derive(Debug, Clone, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "PascalCase")]
pub struct AccessPoint {
    /// Access-point name.
    #[serde(default)]
    pub name: String,
    /// Generated alias.
    #[serde(default)]
    pub alias: String,
    /// Backing bucket.
    #[serde(default)]
    pub bucket: String,
    /// Backing-bucket account identifier.
    #[serde(default)]
    pub bucket_account_id: String,
    /// Access-point type.
    #[serde(default)]
    pub access_point_type: String,
    /// Service status.
    #[serde(default)]
    pub status: String,
    /// Network origin.
    #[serde(default)]
    pub network_origin: String,
    /// VPC identifier.
    #[serde(default)]
    pub vpc_id: String,
    /// TOS resource name.
    #[serde(default)]
    pub access_point_trn: String,
    /// Provider-formatted creation timestamp.
    #[serde(default)]
    pub creation_date: String,
    /// Resolved service endpoints.
    #[serde(default)]
    pub endpoints: Option<AccessPointEndpoints>,
}

/// Output from retrieving one access point.
#[derive(Debug, Clone, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "PascalCase")]
pub struct GetAccessPointOutput {
    /// HTTP and TOS request metadata.
    #[serde(skip)]
    pub request_info: RequestInfo,
    /// Access-point name.
    #[serde(default)]
    pub name: String,
    /// Generated alias.
    #[serde(default)]
    pub alias: String,
    /// Backing bucket.
    #[serde(default)]
    pub bucket: String,
    /// Backing-bucket account identifier.
    #[serde(default)]
    pub bucket_account_id: String,
    /// Access-point type.
    #[serde(default)]
    pub access_point_type: String,
    /// Service status.
    #[serde(default)]
    pub status: String,
    /// Network origin.
    #[serde(default)]
    pub network_origin: String,
    /// VPC identifier.
    #[serde(default)]
    pub vpc_id: String,
    /// TOS resource name.
    #[serde(default)]
    pub access_point_trn: String,
    /// Provider-formatted creation timestamp.
    #[serde(default)]
    pub creation_date: String,
    /// Resolved service endpoints.
    #[serde(default)]
    pub endpoints: Option<AccessPointEndpoints>,
}

/// Output from listing access points.
#[derive(Debug, Clone, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "PascalCase")]
pub struct ListAccessPointsOutput {
    /// HTTP and TOS request metadata.
    #[serde(skip)]
    pub request_info: RequestInfo,
    /// Whether another page exists.
    #[serde(rename = "IsTruncated", default)]
    pub is_truncated: bool,
    /// Continuation token.
    #[serde(default)]
    pub next_token: String,
    /// Access points in this page.
    #[serde(default)]
    pub access_points: Vec<AccessPoint>,
}

/// Output from deleting an access point.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DeleteAccessPointOutput {
    /// HTTP and TOS request metadata.
    pub request_info: RequestInfo,
}

/// Input for listing accelerators bound to an access point.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListBindAcceleratorForAccessPointInput {
    /// Volcengine account identifier.
    pub account_id: String,
    /// Access-point name.
    pub access_point_name: String,
}

/// One accelerator bound to an access point.
#[derive(Debug, Clone, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "PascalCase")]
pub struct Accelerator {
    /// Accelerator identifier.
    #[serde(rename = "Id", default)]
    pub id: String,
    /// Accelerator name.
    #[serde(default)]
    pub name: String,
    /// Accelerator region.
    #[serde(default)]
    pub region: String,
    /// Availability zone.
    #[serde(rename = "AZ", default)]
    pub az: String,
}

/// Output from listing accelerators bound to an access point.
#[derive(Debug, Clone, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "PascalCase")]
pub struct ListBindAcceleratorForAccessPointOutput {
    /// HTTP and TOS request metadata.
    #[serde(skip)]
    pub request_info: RequestInfo,
    /// Bound accelerators.
    #[serde(default)]
    pub accelerators: Vec<Accelerator>,
}

/// Input for listing access points bound to an accelerator.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListBindAccessPointForAcceleratorInput {
    /// Volcengine account identifier.
    pub account_id: String,
    /// Accelerator identifier.
    pub accelerator_id: String,
}

/// Compact access-point description returned for an accelerator.
#[derive(Debug, Clone, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "PascalCase")]
pub struct AccessPointSummary {
    /// Access-point name.
    #[serde(default)]
    pub name: String,
    /// Generated alias.
    #[serde(default)]
    pub alias: String,
    /// Resolved service endpoints.
    #[serde(default)]
    pub endpoints: Option<AccessPointEndpoints>,
}

/// Output from listing access points bound to an accelerator.
#[derive(Debug, Clone, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "PascalCase")]
pub struct ListBindAccessPointForAcceleratorOutput {
    /// HTTP and TOS request metadata.
    #[serde(skip)]
    pub request_info: RequestInfo,
    /// Bound access points.
    #[serde(default)]
    pub access_points: Vec<AccessPointSummary>,
}

impl Client {
    /// Creates an access point through the native TOS control endpoint.
    pub async fn create_access_point(
        &self,
        input: &CreateAccessPointInput,
    ) -> Result<CreateAccessPointOutput, Error> {
        validate_access_point_target(&input.account_id, &input.access_point_name)?;
        validate_tos_identifier(&input.bucket, "TOS bucket")?;
        if input.network_origin.is_empty() {
            return Err(Error::new(
                "invalid_network_origin",
                "TOS access-point network origin is required",
            ));
        }
        let body = serde_json::to_vec(input)?;
        let response = self
            .send_control(
                "CreateAccessPoint",
                Method::PUT,
                &input.account_id,
                &format!("accesspoint/{}", input.access_point_name),
                BTreeMap::new(),
                content_headers(&body, "application/json"),
                Some(reqwest::Body::from(body)),
            )
            .await?;
        parse_with_request_info("CreateAccessPoint", response).await
    }

    /// Retrieves one access point through the native TOS control endpoint.
    pub async fn get_access_point(
        &self,
        input: &GetAccessPointInput,
    ) -> Result<GetAccessPointOutput, Error> {
        validate_access_point_target(&input.account_id, &input.access_point_name)?;
        let response = self
            .send_control(
                "GetAccessPoint",
                Method::GET,
                &input.account_id,
                &format!("accesspoint/{}", input.access_point_name),
                BTreeMap::new(),
                BTreeMap::new(),
                None,
            )
            .await?;
        parse_with_request_info("GetAccessPoint", response).await
    }

    /// Lists access points through the native TOS control endpoint.
    pub async fn list_access_points(
        &self,
        input: &ListAccessPointsInput,
    ) -> Result<ListAccessPointsOutput, Error> {
        validate_tos_identifier(&input.account_id, "TOS account ID")?;
        if let Some(bucket) = input.bucket.as_deref() {
            validate_tos_identifier(bucket, "TOS bucket")?;
        }
        let mut query = BTreeMap::new();
        insert_query(&mut query, "bucket", input.bucket.as_deref());
        insert_query(&mut query, "nextToken", input.next_token.as_deref());
        if let Some(max_results) = input.max_results.filter(|value| *value != 0) {
            query.insert("maxResults".into(), max_results.to_string());
        }
        let response = self
            .send_control(
                "ListAccessPoints",
                Method::GET,
                &input.account_id,
                "accesspoint",
                query,
                BTreeMap::new(),
                None,
            )
            .await?;
        parse_with_request_info("ListAccessPoints", response).await
    }

    /// Deletes one access point through the native TOS control endpoint.
    pub async fn delete_access_point(
        &self,
        input: &DeleteAccessPointInput,
    ) -> Result<DeleteAccessPointOutput, Error> {
        validate_access_point_target(&input.account_id, &input.access_point_name)?;
        let response = self
            .send_control(
                "DeleteAccessPoint",
                Method::DELETE,
                &input.account_id,
                &format!("accesspoint/{}", input.access_point_name),
                BTreeMap::new(),
                BTreeMap::new(),
                None,
            )
            .await?;
        Ok(DeleteAccessPointOutput {
            request_info: request_info(&response),
        })
    }

    /// Lists accelerators bound to one access point.
    pub async fn list_bind_accelerator_for_access_point(
        &self,
        input: &ListBindAcceleratorForAccessPointInput,
    ) -> Result<ListBindAcceleratorForAccessPointOutput, Error> {
        validate_access_point_target(&input.account_id, &input.access_point_name)?;
        let response = self
            .send_control(
                "ListBindAcceleratorForAccessPoint",
                Method::GET,
                &input.account_id,
                &format!("accesspoint/{}/accelerator", input.access_point_name),
                BTreeMap::new(),
                BTreeMap::new(),
                None,
            )
            .await?;
        parse_with_request_info("ListBindAcceleratorForAccessPoint", response).await
    }

    /// Lists access points bound to one accelerator.
    pub async fn list_bind_access_point_for_accelerator(
        &self,
        input: &ListBindAccessPointForAcceleratorInput,
    ) -> Result<ListBindAccessPointForAcceleratorOutput, Error> {
        validate_tos_identifier(&input.account_id, "TOS account ID")?;
        validate_tos_identifier(&input.accelerator_id, "TOS accelerator ID")?;
        let response = self
            .send_control(
                "ListBindAccessPointForAccelerator",
                Method::GET,
                &input.account_id,
                &format!("accelerator/{}/accesspoint", input.accelerator_id),
                BTreeMap::new(),
                BTreeMap::new(),
                None,
            )
            .await?;
        parse_with_request_info("ListBindAccessPointForAccelerator", response).await
    }
}

async fn parse_with_request_info<T>(
    operation: &str,
    response: reqwest::Response,
) -> Result<T, Error>
where
    T: serde::de::DeserializeOwned + HasRequestInfo,
{
    let info = request_info(&response);
    let mut output: T = parse_json_response(operation, response).await?;
    output.set_request_info(info);
    Ok(output)
}

trait HasRequestInfo {
    fn set_request_info(&mut self, request_info: RequestInfo);
}

macro_rules! impl_request_info {
    ($($type:ty),+ $(,)?) => {
        $(impl HasRequestInfo for $type {
            fn set_request_info(&mut self, request_info: RequestInfo) {
                self.request_info = request_info;
            }
        })+
    };
}

impl_request_info!(
    CreateAccessPointOutput,
    GetAccessPointOutput,
    ListAccessPointsOutput,
    ListBindAcceleratorForAccessPointOutput,
    ListBindAccessPointForAcceleratorOutput,
);

fn validate_access_point_target(account_id: &str, access_point_name: &str) -> Result<(), Error> {
    validate_tos_identifier(account_id, "TOS account ID")?;
    validate_tos_identifier(access_point_name, "TOS access-point name")
}

fn insert_query(query: &mut BTreeMap<String, String>, name: &str, value: Option<&str>) {
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
        matchers::{body_json, header, header_exists, method, path, query_param},
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
        .with_control_endpoint("https://tos-control-cn-beijing.volces.com")
        .expect("control endpoint")
        .with_transport_base_url(server.uri().parse().expect("mock URL"))
    }

    #[tokio::test]
    async fn create_access_point_matches_java_wire_contract() {
        let server = MockServer::start().await;
        Mock::given(method("PUT"))
            .and(path("/accesspoint/sdk-ap"))
            .and(header(
                "host",
                "2100000000.tos-control-cn-beijing.volces.com",
            ))
            .and(header("x-tos-account-id", "2100000000"))
            .and(header_exists("content-md5"))
            .and(body_json(json!({
                "Bucket": "example-bucket",
                "BucketAccountId": "2100000000",
                "NetworkOrigin": "internet"
            })))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "AccessPointTrn": "trn:tos:cn-beijing:2100000000:accesspoint/sdk-ap",
                "Alias": "sdk-ap-2100000000"
            })))
            .mount(&server)
            .await;

        let output = client(&server)
            .await
            .create_access_point(&CreateAccessPointInput {
                account_id: "2100000000".into(),
                access_point_name: "sdk-ap".into(),
                bucket: "example-bucket".into(),
                bucket_account_id: Some("2100000000".into()),
                network_origin: "internet".into(),
                vpc_id: None,
            })
            .await
            .expect("create access point");
        assert_eq!(output.alias, "sdk-ap-2100000000");
    }

    #[tokio::test]
    async fn read_and_delete_access_point_operations_match_java_paths() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/accesspoint/sdk-ap"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "Name": "sdk-ap",
                "NetworkOrigin": "internet",
                "Endpoints": {"IntranetEndpoint": "internal", "ExtranetEndpoint": "external"}
            })))
            .mount(&server)
            .await;
        Mock::given(method("DELETE"))
            .and(path("/accesspoint/sdk-ap"))
            .respond_with(ResponseTemplate::new(204).insert_header("x-tos-request-id", "delete-1"))
            .mount(&server)
            .await;
        let client = client(&server).await;
        let target = GetAccessPointInput {
            account_id: "2100000000".into(),
            access_point_name: "sdk-ap".into(),
        };
        let output = client.get_access_point(&target).await.expect("get");
        assert_eq!(output.name, "sdk-ap");
        assert_eq!(
            output.endpoints.expect("endpoints").extranet_endpoint,
            "external"
        );
        let output = client
            .delete_access_point(&DeleteAccessPointInput {
                account_id: target.account_id,
                access_point_name: target.access_point_name,
            })
            .await
            .expect("delete");
        assert_eq!(output.request_info.status_code, 204);
    }

    #[tokio::test]
    async fn list_access_point_and_accelerator_operations_match_java_paths() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/accesspoint"))
            .and(query_param("bucket", "example-bucket"))
            .and(query_param("maxResults", "100"))
            .and(query_param("nextToken", "page-2"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "IsTruncated": false,
                "AccessPoints": [{"Name": "sdk-ap", "Alias": "sdk-ap-alias"}]
            })))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/accesspoint/sdk-ap/accelerator"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "Accelerators": [{"Id": "acc-1", "Name": "accelerator", "Region": "cn-beijing", "AZ": "az-a"}]
            })))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/accelerator/acc-1/accesspoint"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "AccessPoints": [{"Name": "sdk-ap", "Alias": "sdk-ap-alias"}]
            })))
            .mount(&server)
            .await;
        let client = client(&server).await;
        let listed = client
            .list_access_points(&ListAccessPointsInput {
                account_id: "2100000000".into(),
                bucket: Some("example-bucket".into()),
                max_results: Some(100),
                next_token: Some("page-2".into()),
            })
            .await
            .expect("list access points");
        assert_eq!(listed.access_points[0].name, "sdk-ap");

        let accelerators = client
            .list_bind_accelerator_for_access_point(&ListBindAcceleratorForAccessPointInput {
                account_id: "2100000000".into(),
                access_point_name: "sdk-ap".into(),
            })
            .await
            .expect("list accelerators");
        assert_eq!(accelerators.accelerators[0].id, "acc-1");

        let access_points = client
            .list_bind_access_point_for_accelerator(&ListBindAccessPointForAcceleratorInput {
                account_id: "2100000000".into(),
                accelerator_id: "acc-1".into(),
            })
            .await
            .expect("list accelerator access points");
        assert_eq!(access_points.access_points[0].name, "sdk-ap");
    }
}
