use s3desktop_tos_sdk::{Client, ClientConfig, Credentials, TosOperations};

/// Exercises only read operations against an explicitly configured real TOS account.
#[tokio::test]
#[ignore = "requires explicit TOS credentials and performs live read-only requests"]
async fn live_read_only_smoke_test() {
    let region = required("TOS_SDK_LIVE_REGION");
    let access_key = required("TOS_SDK_LIVE_ACCESS_KEY");
    let secret_key = required("TOS_SDK_LIVE_SECRET_KEY");
    let mut credentials = Credentials::new(access_key, secret_key);
    if let Ok(token) = std::env::var("TOS_SDK_LIVE_SESSION_TOKEN") {
        credentials = credentials.with_session_token(token);
    }

    let client = Client::new(ClientConfig::new(region), Some(credentials), None)
        .await
        .expect("live client should initialize");
    let buckets = client
        .list_buckets()
        .await
        .expect("native ListBuckets should succeed");

    if let Ok(bucket) = std::env::var("TOS_SDK_LIVE_BUCKET") {
        let page = client
            .list_objects(&bucket, "", None, 10)
            .await
            .expect("native ListObjectsV2 should succeed");
        assert!(page.items.len() <= 10);
    }

    eprintln!("native TOS discovery returned {} buckets", buckets.len());
}

fn required(name: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| panic!("missing required environment variable {name}"))
}
