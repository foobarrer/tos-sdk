use s3desktop_tos_sdk::{
    Client, ClientConfig, Credentials, TosOperations, TransferBandwidthLimiter, TransferObjectInfo,
};
use std::{
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};
use tempfile::tempdir;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    sync::Semaphore,
};
use tokio_util::sync::CancellationToken;

const AUTHORIZED_BUCKET: &str = "it-audit";
const AUTHORIZED_PREFIX: &str = "vdb/data_mining/test/tossdk/";
const AUTHORIZED_REGION: &str = "cn-beijing";
const AUTHORIZED_BUCKET_HOST: &str = "it-audit.tos-cn-beijing.volces.com";

/// Exercises S3 Desktop's SDK dependency against one explicitly authorized
/// TOS prefix. It never performs ListBuckets or any bucket-level mutation.
#[tokio::test]
#[ignore = "requires the saved S3 Desktop TOS credential and mutates only the authorized test prefix"]
async fn live_scoped_prefix_crud_and_transfer() {
    let mut config = ClientConfig::new(AUTHORIZED_REGION);
    config.endpoint = required("TOS_SDK_LIVE_ENDPOINT");
    let mut credentials = Credentials::new(
        required("TOS_SDK_LIVE_ACCESS_KEY"),
        required("TOS_SDK_LIVE_SECRET_KEY"),
    );
    if let Ok(token) = std::env::var("TOS_SDK_LIVE_SESSION_TOKEN") {
        if !token.is_empty() {
            credentials = credentials.with_session_token(token);
        }
    }

    let client = Client::new(config, Some(credentials), None)
        .await
        .expect("live client should initialize");
    client
        .register_bucket_route(AUTHORIZED_BUCKET, AUTHORIZED_REGION, AUTHORIZED_BUCKET_HOST)
        .await
        .expect("authorized bucket route should validate");

    let run_id = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock should follow the Unix epoch")
        .as_nanos();
    let run_prefix = scoped(&format!("codex-sdk-{run_id}/"));
    let folder_key = scoped(&format!("codex-sdk-{run_id}/folder/"));
    let source_key = scoped(&format!("codex-sdk-{run_id}/source.txt"));
    let copy_key = scoped(&format!("codex-sdk-{run_id}/copy.txt"));
    let stream_key = scoped(&format!("codex-sdk-{run_id}/stream.txt"));
    let cleanup_keys = vec![
        source_key.clone(),
        copy_key.clone(),
        stream_key.clone(),
        folder_key.clone(),
    ];

    let result = exercise_scoped_operations(
        &client,
        &run_prefix,
        &folder_key,
        &source_key,
        &copy_key,
        &stream_key,
    )
    .await;
    let cleanup = client.delete_objects(AUTHORIZED_BUCKET, cleanup_keys).await;

    if let Err(error) = result {
        panic!("scoped live SDK exercise failed: {error}");
    }
    cleanup.expect("scoped live objects should be deleted after the test");

    let remaining = client
        .list_objects(AUTHORIZED_BUCKET, &run_prefix, None, 10)
        .await
        .expect("cleanup verification should be listable");
    assert!(
        remaining.items.is_empty(),
        "test cleanup left scoped objects behind"
    );
}

async fn exercise_scoped_operations(
    client: &Client,
    run_prefix: &str,
    folder_key: &str,
    source_key: &str,
    copy_key: &str,
    stream_key: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    assert_scope(run_prefix);
    for key in [folder_key, source_key, copy_key, stream_key] {
        assert_scope(key);
    }

    client
        .list_objects(AUTHORIZED_BUCKET, AUTHORIZED_PREFIX, None, 1)
        .await?;
    assert!(
        client
            .directory_is_empty(AUTHORIZED_BUCKET, run_prefix)
            .await?
    );

    client.create_folder(AUTHORIZED_BUCKET, folder_key).await?;
    let content = b"S3 Desktop scoped TOS SDK integration test\n";
    let directory = tempdir()?;
    let source_path = directory.path().join("source.txt");
    tokio::fs::write(&source_path, content).await?;
    assert_eq!(
        client
            .upload(AUTHORIZED_BUCKET, source_key, &source_path, 1, 1024 * 1024)
            .await?,
        content.len() as u64
    );

    let properties = client
        .object_properties(AUTHORIZED_BUCKET, source_key)
        .await?;
    assert!(properties.common.iter().any(|property| {
        property.name == "content-length" && property.value == content.len().to_string()
    }));
    assert_eq!(
        client
            .preview_text(AUTHORIZED_BUCKET, source_key, content.len())
            .await?,
        content
    );
    assert_eq!(
        client
            .preview_binary(AUTHORIZED_BUCKET, source_key, content.len())
            .await?,
        content
    );

    let download_path = directory.path().join("downloaded.txt");
    let downloaded = client
        .download(
            AUTHORIZED_BUCKET,
            source_key,
            &download_path,
            1,
            1024 * 1024,
            99,
        )
        .await?;
    assert_eq!(downloaded.bytes, content.len() as u64);
    assert_eq!(tokio::fs::read(&download_path).await?, content);

    let source_info = client
        .transfer_object_info(AUTHORIZED_BUCKET, source_key)
        .await?;
    assert_eq!(source_info.size, content.len() as u64);
    let signed = client
        .presign_get(AUTHORIZED_BUCKET, source_key, 60)
        .await?;
    assert!(signed.contains("X-Tos-Signature="));

    client
        .copy_object(AUTHORIZED_BUCKET, source_key, AUTHORIZED_BUCKET, copy_key)
        .await?;
    client
        .verify_transfer_object(AUTHORIZED_BUCKET, copy_key, &source_info)
        .await?;

    let (writer, mut reader) = tokio::io::duplex(1024);
    let stream_client = client.clone();
    let stream_source = source_key.to_string();
    let stream_task = tokio::spawn(async move {
        stream_client
            .stream_object(
                AUTHORIZED_BUCKET,
                &stream_source,
                writer,
                10,
                TransferBandwidthLimiter::new(0),
                Arc::new(|_| {}),
            )
            .await
    });
    let mut streamed = Vec::new();
    reader.read_to_end(&mut streamed).await?;
    stream_task.await??;
    assert_eq!(streamed, content);

    let (mut writer, reader) = tokio::io::duplex(1024);
    writer.write_all(content).await?;
    writer.shutdown().await?;
    client
        .upload_transfer_stream(
            AUTHORIZED_BUCKET,
            stream_key,
            reader,
            TransferObjectInfo {
                size: content.len() as u64,
                content_type: Some("text/plain".into()),
                ..Default::default()
            },
            1024 * 1024,
            5 * 1024 * 1024,
            1,
            10,
            Arc::new(Semaphore::new(1)),
        )
        .await?;

    let page = client
        .list_directory_page(AUTHORIZED_BUCKET, run_prefix, None)
        .await?;
    assert!(page.items.iter().any(|(key, _)| key == source_key));
    let summary = client
        .summarize_prefix(
            AUTHORIZED_BUCKET,
            run_prefix,
            CancellationToken::new(),
            Arc::new(|_| {}),
        )
        .await?;
    assert!(summary.object_count >= 4);
    let planned = client
        .scan_directory_operation_plan(
            AUTHORIZED_BUCKET,
            run_prefix,
            CancellationToken::new(),
            Arc::new(|_, _| Ok(())),
        )
        .await?;
    assert_eq!(planned.object_count, summary.object_count);
    Ok(())
}

fn scoped(suffix: &str) -> String {
    let key = format!("{AUTHORIZED_PREFIX}{suffix}");
    assert_scope(&key);
    key
}

fn assert_scope(key: &str) {
    assert!(
        key.starts_with(AUTHORIZED_PREFIX) && key.len() > AUTHORIZED_PREFIX.len(),
        "refusing a live TOS request outside the authorized prefix"
    );
}

fn required(name: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| panic!("missing required environment variable {name}"))
}
