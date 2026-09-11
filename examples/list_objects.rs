use s3desktop_tos_sdk::{Client, ClientConfig, Credentials, TosOperations};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let region = required("TOS_SDK_REGION")?;
    let bucket = required("TOS_SDK_BUCKET")?;
    let access_key = required("TOS_SDK_ACCESS_KEY")?;
    let secret_key = required("TOS_SDK_SECRET_KEY")?;
    let credentials = Credentials::new(access_key, secret_key);
    let client = Client::new(ClientConfig::new(region), Some(credentials), None).await?;

    let mut token = None;
    loop {
        let page = client.list_objects(&bucket, "", token, 1_000).await?;
        for item in page.items {
            println!("{}\t{}", item.size, item.key);
        }
        token = page.next_token;
        if token.is_none() {
            break;
        }
    }
    Ok(())
}

fn required(name: &str) -> Result<String, Box<dyn std::error::Error>> {
    std::env::var(name).map_err(|_| format!("missing required environment variable {name}").into())
}
