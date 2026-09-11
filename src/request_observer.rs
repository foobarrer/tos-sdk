use std::sync::Arc;

/// Metadata emitted immediately before a native TOS HTTP request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RequestStatisticEvent {
    /// Caller-defined connection identifier.
    pub connection_id: String,
    /// Caller-defined connection display name.
    pub connection_name: String,
    /// Bucket associated with the request, or an empty string for account-level requests.
    pub bucket: String,
    /// Stable high-level operation name.
    pub action: String,
    /// Service identifier (`tos`).
    pub service: String,
    /// Transport implementation identifier.
    pub client: String,
}

/// Cloneable callback used for request metrics and auditing.
#[derive(Clone)]
pub struct RequestStatisticsRecorder {
    callback: Arc<dyn Fn(RequestStatisticEvent) + Send + Sync>,
}

impl RequestStatisticsRecorder {
    /// Creates an observer from a thread-safe callback.
    pub fn new(callback: impl Fn(RequestStatisticEvent) + Send + Sync + 'static) -> Self {
        Self {
            callback: Arc::new(callback),
        }
    }

    pub(crate) fn record(
        &self,
        connection_id: &str,
        connection_name: &str,
        bucket: Option<&str>,
        action: &str,
        service: &str,
        client: &str,
    ) {
        (self.callback)(RequestStatisticEvent {
            connection_id: connection_id.to_owned(),
            connection_name: connection_name.to_owned(),
            bucket: bucket.unwrap_or_default().to_owned(),
            action: action.to_owned(),
            service: service.to_owned(),
            client: client.to_owned(),
        });
    }
}

pub(crate) type RequestStatisticsContext<'a> = (&'a RequestStatisticsRecorder, &'a str, &'a str);

pub(crate) fn record_http_request(
    context: Option<RequestStatisticsContext<'_>>,
    bucket: Option<&str>,
    action: &str,
    service: &str,
    client: &str,
) {
    if let Some((recorder, connection_id, connection_name)) = context {
        recorder.record(
            connection_id,
            connection_name,
            bucket,
            action,
            service,
            client,
        );
    }
}
