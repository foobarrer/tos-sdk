//! Java-compatible legacy bucket notification operations.

use crate::{
    client::{content_headers, parse_json_response, validate_tos_identifier},
    java_compat::request_info,
    Client, Error, RequestInfo,
};
use reqwest::Method;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// One key filter rule in a legacy notification configuration.
#[derive(Debug, Clone, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "PascalCase")]
pub struct NotificationFilterRule {
    /// Rule name, normally `prefix` or `suffix`.
    pub name: String,
    /// Rule value.
    pub value: String,
}

/// Key rules nested below a notification filter.
#[derive(Debug, Clone, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "PascalCase")]
pub struct NotificationFilterKey {
    /// Filter rules.
    #[serde(default)]
    pub filter_rules: Vec<NotificationFilterRule>,
}

/// Optional object-key filter for a notification destination.
#[derive(Debug, Clone, Default, Deserialize, Serialize, PartialEq, Eq)]
pub struct NotificationFilter {
    /// TOS key filter.
    #[serde(rename = "TOSKey", skip_serializing_if = "Option::is_none")]
    pub key: Option<NotificationFilterKey>,
}

/// One legacy cloud-function notification destination.
#[derive(Debug, Clone, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "PascalCase")]
pub struct CloudFunctionConfiguration {
    /// Rule identifier.
    #[serde(rename = "RuleId", default)]
    pub id: String,
    /// TOS event names that trigger the rule.
    #[serde(default)]
    pub events: Vec<String>,
    /// Optional key filter.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub filter: Option<NotificationFilter>,
    /// Cloud-function target.
    #[serde(default)]
    pub cloud_function: String,
}

/// RocketMQ destination details.
#[derive(Debug, Clone, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "PascalCase")]
pub struct RocketMqTarget {
    /// RocketMQ instance identifier.
    #[serde(rename = "InstanceId", default)]
    pub instance_id: String,
    /// RocketMQ topic.
    #[serde(default)]
    pub topic: String,
    /// RocketMQ access-key identifier.
    #[serde(rename = "AccessKeyId", default)]
    pub access_key_id: String,
}

/// One legacy RocketMQ notification destination.
#[derive(Debug, Clone, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "PascalCase")]
pub struct RocketMqConfiguration {
    /// Rule identifier.
    #[serde(rename = "RuleId", default)]
    pub id: String,
    /// IAM role used by TOS.
    #[serde(default)]
    pub role: String,
    /// TOS event names that trigger the rule.
    #[serde(default)]
    pub events: Vec<String>,
    /// Optional key filter.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub filter: Option<NotificationFilter>,
    /// RocketMQ destination.
    #[serde(rename = "RocketMQ")]
    pub rocket_mq: RocketMqTarget,
}

/// Input for Java `putBucketNotification`.
#[derive(Debug, Clone, Default, Serialize, PartialEq, Eq)]
#[serde(rename_all = "PascalCase")]
pub struct PutBucketNotificationInput {
    /// Bucket whose legacy notification rules are replaced.
    #[serde(skip)]
    pub bucket: String,
    /// Cloud-function destinations.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub cloud_function_configurations: Vec<CloudFunctionConfiguration>,
    /// RocketMQ destinations.
    #[serde(
        rename = "RocketMQConfigurations",
        default,
        skip_serializing_if = "Vec::is_empty"
    )]
    pub rocket_mq_configurations: Vec<RocketMqConfiguration>,
}

/// Output from Java `putBucketNotification`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PutBucketNotificationOutput {
    /// HTTP and TOS request metadata.
    pub request_info: RequestInfo,
}

/// Input for Java `getBucketNotification`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct GetBucketNotificationInput {
    /// Bucket whose legacy notification rules are read.
    pub bucket: String,
}

/// Output from Java `getBucketNotification`.
#[derive(Debug, Clone, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "PascalCase")]
pub struct GetBucketNotificationOutput {
    /// HTTP and TOS request metadata.
    #[serde(skip)]
    pub request_info: RequestInfo,
    /// Cloud-function destinations.
    #[serde(default)]
    pub cloud_function_configurations: Vec<CloudFunctionConfiguration>,
    /// RocketMQ destinations, retained when the service returns them.
    #[serde(rename = "RocketMQConfigurations", default)]
    pub rocket_mq_configurations: Vec<RocketMqConfiguration>,
}

impl Client {
    /// Replaces the legacy (`notification`) bucket notification configuration.
    pub async fn put_bucket_notification(
        &self,
        input: &PutBucketNotificationInput,
    ) -> Result<PutBucketNotificationOutput, Error> {
        validate_tos_identifier(&input.bucket, "TOS bucket")?;
        let body = serde_json::to_vec(input)?;
        let response = self
            .send_bucket(
                "PutBucketNotification",
                Method::PUT,
                &input.bucket,
                "",
                BTreeMap::from([("notification".into(), String::new())]),
                content_headers(&body, "application/json"),
                Some(reqwest::Body::from(body)),
            )
            .await?;
        Ok(PutBucketNotificationOutput {
            request_info: request_info(&response),
        })
    }

    /// Reads the legacy (`notification`) bucket notification configuration.
    pub async fn get_bucket_notification(
        &self,
        input: &GetBucketNotificationInput,
    ) -> Result<GetBucketNotificationOutput, Error> {
        validate_tos_identifier(&input.bucket, "TOS bucket")?;
        let response = self
            .send_bucket(
                "GetBucketNotification",
                Method::GET,
                &input.bucket,
                "",
                BTreeMap::from([("notification".into(), String::new())]),
                BTreeMap::new(),
                None,
            )
            .await?;
        let info = request_info(&response);
        let mut output: GetBucketNotificationOutput =
            parse_json_response("GetBucketNotification", response).await?;
        output.request_info = info;
        Ok(output)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ClientConfig, Credentials};
    use serde_json::json;
    use wiremock::{
        matchers::{body_json, header_exists, method, path, query_param},
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

    fn cloud_function() -> CloudFunctionConfiguration {
        CloudFunctionConfiguration {
            id: "rule-1".into(),
            events: vec!["tos:ObjectCreated:Put".into()],
            filter: Some(NotificationFilter {
                key: Some(NotificationFilterKey {
                    filter_rules: vec![NotificationFilterRule {
                        name: "prefix".into(),
                        value: "images/".into(),
                    }],
                }),
            }),
            cloud_function: "trn:fc:cn-beijing:2100000000:function/demo".into(),
        }
    }

    #[tokio::test]
    async fn put_legacy_notification_matches_java_wire_contract() {
        let server = MockServer::start().await;
        Mock::given(method("PUT"))
            .and(path("/"))
            .and(query_param("notification", ""))
            .and(header_exists("content-md5"))
            .and(body_json(json!({
                "CloudFunctionConfigurations": [{
                    "RuleId": "rule-1",
                    "Events": ["tos:ObjectCreated:Put"],
                    "Filter": {"TOSKey": {"FilterRules": [{"Name": "prefix", "Value": "images/"}]}},
                    "CloudFunction": "trn:fc:cn-beijing:2100000000:function/demo"
                }]
            })))
            .respond_with(
                ResponseTemplate::new(200).insert_header("x-tos-request-id", "put-notify-1"),
            )
            .mount(&server)
            .await;

        let output = client(&server)
            .await
            .put_bucket_notification(&PutBucketNotificationInput {
                bucket: "example".into(),
                cloud_function_configurations: vec![cloud_function()],
                rocket_mq_configurations: Vec::new(),
            })
            .await
            .expect("put notification");
        assert_eq!(
            output.request_info.request_id.as_deref(),
            Some("put-notify-1")
        );
    }

    #[tokio::test]
    async fn get_legacy_notification_matches_java_wire_contract() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/"))
            .and(query_param("notification", ""))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "CloudFunctionConfigurations": [{
                    "RuleId": "rule-1",
                    "Events": ["tos:ObjectCreated:Put"],
                    "CloudFunction": "trn:fc:cn-beijing:2100000000:function/demo"
                }]
            })))
            .mount(&server)
            .await;

        let output = client(&server)
            .await
            .get_bucket_notification(&GetBucketNotificationInput {
                bucket: "example".into(),
            })
            .await
            .expect("get notification");
        assert_eq!(output.cloud_function_configurations[0].id, "rule-1");
    }
}
