use reqwest::Client;

use crate::ore_utils::MineEventWithBoosts;

#[derive(Debug)]
pub enum AppMetricsError {
    FailedToSendMetrics(String),
}

#[derive(Debug)]
pub enum AppMetricsEvent {
    MineEvent(MineEventWithBoosts),
}

pub struct AppMetrics {
    client: Client,
    url: String,
    token: String,
    org: String,
    bucket: String,
    pub hostname: String,
}

impl AppMetrics {
    pub fn new(url: String, token: String, org: String, bucket: String, hostname: String) -> Self {
        let client = reqwest::Client::new();
        AppMetrics {
            client,
            url,
            token: format!("Token {}", token),
            org,
            bucket,
            hostname,
        }
    }

    pub async fn send_data_to_influxdb(
        &self,
        data: String,
    ) -> Result<(), AppMetricsError> {
        match self.client.post(format!(
            "{}/api/v2/write?org={}&bucket={}&precision=ns", 
            self.url,
            self.org,
            self.bucket
        )).header("Authorization", self.token.clone())
        .body(data)
        .send()
        .await {
            Ok(res) => {
                let status = res.status();
                if !res.status().is_success() {
                    let error_body = res.text().await.unwrap_or_else(|_| "Failed to get error body".to_string());
                    Err(AppMetricsError::FailedToSendMetrics(format!("Status Code: {}.\nError: {}", status, error_body)))
                } else {
                    Ok(())
                }
            },
            Err(e) => {
                tracing::error!(target: "server_log", "Failed to send metrics data to influxdb.\nError: {:?}", e);
                Err(AppMetricsError::FailedToSendMetrics(format!("{:?}", e)))
            }
        }

    }
}
