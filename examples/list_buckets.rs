use s3desktop_tos_sdk::{Client, ClientConfig, Credentials, TosOperations};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let region = required("TOS_SDK_REGION")?;
    let access_key = required("TOS_SDK_ACCESS_KEY")?;
    let secret_key = required("TOS_SDK_SECRET_KEY")?;
    let mut credentials = Credentials::new(access_key, secret_key);
    if let Ok(token) = std::env::var("TOS_SDK_SESSION_TOKEN") {
        credentials = credentials.with_session_token(token);
    }

    let client = Client::new(ClientConfig::new(region), Some(credentials), None).await?;
    for bucket in client.list_buckets().await? {
        println!("{}\t{}", bucket.name, bucket.location.unwrap_or_default());
    }
    Ok(())
}

fn required(name: &str) -> Result<String, Box<dyn std::error::Error>> {
    std::env::var(name).map_err(|_| format!("missing required environment variable {name}").into())
}
