use std::time::{SystemTime, UNIX_EPOCH};

use tokio::sync::mpsc::UnboundedReceiver;

use crate::app_metrics::{AppMetrics, AppMetricsEvent};

pub async fn metrics_system(
    url: String,
    token: String,
    org: String,
    bucket: String,
    host: String,
    mut metrics_event: UnboundedReceiver<AppMetricsEvent>
) {
    let app_metrics = AppMetrics::new(url, token, org, bucket, host);
    loop {
        while let Some(me) = metrics_event.recv().await {
            match me {
                AppMetricsEvent::MineEvent(data) => {
                    let ts_ns = match SystemTime::now().duration_since(UNIX_EPOCH) {
                        Ok(d) => {
                            d.as_nanos()
                        },
                        Err(_d) => {
                            tracing::error!(target: "server_log", "Time went backwards...");
                            continue;
                        }
                    };
                    let formatted_data = format!("mine_event,host={} balance={}u,difficulty={}u,last_hash_at={}i,timing={}i,reward={}u,boost_1={}u,boost_2={}u,boost_3={}u {}",
                        app_metrics.hostname,
                        data.balance,
                        data.difficulty,
                        data.last_hash_at,
                        data.timing,
                        data.reward,
                        data.boost_1,
                        data.boost_2,
                        data.boost_3,
                        ts_ns
                    );
                    match app_metrics.send_data_to_influxdb(formatted_data).await {
                        Ok(_) => {
                        },
                        Err(e) => {
                            tracing::error!(target: "server_log", "Failed to send metrics data to influxdb.\nError: {:?}", e);
                        }
                    }

                }
            }
        }
    }
}
