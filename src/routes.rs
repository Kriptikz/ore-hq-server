use app_rr_database::AppRRDatabase;
use axum::{
    http::{Response, StatusCode},
    response::IntoResponse,
    Extension, Json,
};
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::pubkey::Pubkey;
use spl_associated_token_account::get_associated_token_address;
use tokio::{sync::{mpsc::UnboundedSender, RwLock}, time::Instant};
use tracing::error;

use crate::{
    app_metrics::{AppMetricsEvent, MetricsRouteEventData}, app_rr_database, ore_utils::{get_ore_mint, get_proof}, ChallengeWithDifficulty, ChallengesCache, Config, Txn
};
use std::{str::FromStr, sync::Arc, time::{SystemTime, UNIX_EPOCH}};

pub async fn get_challenges(
    Extension(app_config): Extension<Arc<Config>>,
    Extension(app_cache_challenges): Extension<Arc<RwLock<ChallengesCache>>>,
    Extension(app_metrics_channel): Extension<UnboundedSender<AppMetricsEvent>>,
) -> Result<Json<Vec<ChallengeWithDifficulty>>, String> {
    let metrics_start = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards")
        .as_millis();
    if app_config.stats_enabled {
        let reader = app_cache_challenges.read().await;
        let cached_challenges = reader.clone();
        drop(reader);
        let metrics_end = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards")
            .as_millis();

        let metrics_data = MetricsRouteEventData {
            route: "challenges".to_string(),
            method: "GET".to_string(),
            status_code: 200,
            request: metrics_start,
            response: metrics_end,
            latency: metrics_end - metrics_start,
            ts_ns: metrics_end,

        };
        if let Err(e) = app_metrics_channel.send(AppMetricsEvent::RouteEvent(metrics_data)) {
            tracing::error!(target: "server_log", "Failed to send msg down app metrics channel.");
        };
        return Ok(Json(cached_challenges.item));
    } else {
        let metrics_end = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards")
            .as_millis();

        let metrics_data = MetricsRouteEventData {
            route: "challenges".to_string(),
            method: "GET".to_string(),
            status_code: 400,
            request: metrics_start,
            response: metrics_end,
            latency: metrics_end - metrics_start,
            ts_ns: metrics_end,

        };
        if let Err(e) = app_metrics_channel.send(AppMetricsEvent::RouteEvent(metrics_data)) {
            tracing::error!(target: "server_log", "Failed to send msg down app metrics channel.");
        };
        return Err("Stats not enabled for this server.".to_string());
    }
}

pub async fn get_latest_mine_txn(
    Extension(app_rr_database): Extension<Arc<AppRRDatabase>>,
    Extension(app_config): Extension<Arc<Config>>,
    Extension(app_metrics_channel): Extension<UnboundedSender<AppMetricsEvent>>,
) -> Result<Json<Txn>, String> {
    let metrics_start = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards")
        .as_millis();

    if app_config.stats_enabled {
        let res = app_rr_database.get_latest_mine_txn().await;

        match res {
            Ok(txn) => {
                let metrics_end = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .expect("Time went backwards")
                    .as_millis();

                let metrics_data = MetricsRouteEventData {
                    route: "txns/latest-mine".to_string(),
                    method: "GET".to_string(),
                    status_code: 400,
                    request: metrics_start,
                    response: metrics_end,
                    latency: metrics_end - metrics_start,
                    ts_ns: metrics_end,

                };
                if let Err(e) = app_metrics_channel.send(AppMetricsEvent::RouteEvent(metrics_data)) {
                    tracing::error!(target: "server_log", "Failed to send msg down app metrics channel.");
                };
                Ok(Json(txn))
            },
            Err(_) => {
                let metrics_end = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .expect("Time went backwards")
                    .as_millis();

                let metrics_data = MetricsRouteEventData {
                    route: "txns/latest-mine".to_string(),
                    method: "GET".to_string(),
                    status_code: 500,
                    request: metrics_start,
                    response: metrics_end,
                    latency: metrics_end - metrics_start,
                    ts_ns: metrics_end,

                };
                if let Err(e) = app_metrics_channel.send(AppMetricsEvent::RouteEvent(metrics_data)) {
                    tracing::error!(target: "server_log", "Failed to send msg down app metrics channel.");
                };
                Err("Failed to get latest mine txn".to_string())
            }
        }
    } else {
        let metrics_end = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards")
            .as_millis();

        let metrics_data = MetricsRouteEventData {
            route: "txns/latest-mine".to_string(),
            method: "GET".to_string(),
            status_code: 400,
            request: metrics_start,
            response: metrics_end,
            latency: metrics_end - metrics_start,
            ts_ns: metrics_end,

        };
        if let Err(e) = app_metrics_channel.send(AppMetricsEvent::RouteEvent(metrics_data)) {
            tracing::error!(target: "server_log", "Failed to send msg down app metrics channel.");
        };
        return Err("Stats not enabled for this server.".to_string());
    }
}

pub async fn get_pool(
    Extension(app_rr_database): Extension<Arc<AppRRDatabase>>,
    Extension(app_config): Extension<Arc<Config>>,
) -> Result<Json<crate::models::Pool>, String> {
    if app_config.stats_enabled {
        let pubkey = Pubkey::from_str("mineXqpDeBeMR8bPQCyy9UneJZbjFywraS3koWZ8SSH").unwrap();
        let res = app_rr_database
            .get_pool_by_authority_pubkey(pubkey.to_string())
            .await;

        match res {
            Ok(pool) => Ok(Json(pool)),
            Err(_) => Err("Failed to get pool data".to_string()),
        }
    } else {
        return Err("Stats not enabled for this server.".to_string());
    }
}

pub async fn get_pool_staked(
    Extension(app_config): Extension<Arc<Config>>,
    Extension(rpc_client): Extension<Arc<RpcClient>>,
    Extension(app_metrics_channel): Extension<UnboundedSender<AppMetricsEvent>>,
) -> impl IntoResponse {
    let metrics_start = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards")
        .as_millis();
    if app_config.stats_enabled {
        let pubkey = Pubkey::from_str("mineXqpDeBeMR8bPQCyy9UneJZbjFywraS3koWZ8SSH").unwrap();
        let proof = if let Ok(loaded_proof) = get_proof(&rpc_client, pubkey).await {
            loaded_proof
        } else {
            error!("get_pool_staked: Failed to load proof.");
            return Err("Stats not enabled for this server.".to_string());
        };

        let metrics_end = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards")
            .as_millis();

        let metrics_data = MetricsRouteEventData {
            route: "pool/staked".to_string(),
            method: "GET".to_string(),
            status_code: 200,
            request: metrics_start,
            response: metrics_end,
            latency: metrics_end - metrics_start,
            ts_ns: metrics_end,

        };
        if let Err(e) = app_metrics_channel.send(AppMetricsEvent::RouteEvent(metrics_data)) {
            tracing::error!(target: "server_log", "Failed to send msg down app metrics channel.");
        };

        return Ok(Json(proof.balance));
    } else {
        let metrics_end = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards")
            .as_millis();

        let metrics_data = MetricsRouteEventData {
            route: "pool/staked".to_string(),
            method: "GET".to_string(),
            status_code: 400,
            request: metrics_start,
            response: metrics_end,
            latency: metrics_end - metrics_start,
            ts_ns: metrics_end,

        };
        if let Err(e) = app_metrics_channel.send(AppMetricsEvent::RouteEvent(metrics_data)) {
            tracing::error!(target: "server_log", "Failed to send msg down app metrics channel.");
        };
        return Err("Stats not enabled for this server.".to_string());
    }
}

pub async fn get_pool_balance(
    Extension(app_config): Extension<Arc<Config>>,
    Extension(rpc_client): Extension<Arc<RpcClient>>,
    Extension(app_metrics_channel): Extension<UnboundedSender<AppMetricsEvent>>,
) -> impl IntoResponse {
    let metrics_start = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards")
        .as_millis();
    if app_config.stats_enabled {
        let pubkey = Pubkey::from_str("mineXqpDeBeMR8bPQCyy9UneJZbjFywraS3koWZ8SSH").unwrap();
        let miner_token_account = get_associated_token_address(&pubkey, &get_ore_mint());
        if let Ok(response) = rpc_client
            .get_token_account_balance(&miner_token_account)
            .await
        {
            let metrics_end = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("Time went backwards")
                .as_millis();

            let metrics_data = MetricsRouteEventData {
                route: "pool/balance".to_string(),
                method: "GET".to_string(),
                status_code: 200,
                request: metrics_start,
                response: metrics_end,
                latency: metrics_end - metrics_start,
                ts_ns: metrics_end,

            };
            if let Err(e) = app_metrics_channel.send(AppMetricsEvent::RouteEvent(metrics_data)) {
                tracing::error!(target: "server_log", "Failed to send msg down app metrics channel.");
            };
            return Response::builder()
                .status(StatusCode::OK)
                .body(response.ui_amount_string)
                .unwrap();
        } else {
            let metrics_end = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("Time went backwards")
                .as_millis();

            let metrics_data = MetricsRouteEventData {
                route: "pool/balance".to_string(),
                method: "GET".to_string(),
                status_code: 500,
                request: metrics_start,
                response: metrics_end,
                latency: metrics_end - metrics_start,
                ts_ns: metrics_end,

            };
            if let Err(e) = app_metrics_channel.send(AppMetricsEvent::RouteEvent(metrics_data)) {
                tracing::error!(target: "server_log", "Failed to send msg down app metrics channel.");
            };
            return Response::builder()
                .status(StatusCode::BAD_REQUEST)
                .body("Failed to get token account balance".to_string())
                .unwrap();
        }
    } else {
        let metrics_end = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards")
            .as_millis();

        let metrics_data = MetricsRouteEventData {
            route: "pool/balance".to_string(),
            method: "GET".to_string(),
            status_code: 400,
            request: metrics_start,
            response: metrics_end,
            latency: metrics_end - metrics_start,
            ts_ns: metrics_end,

        };
        if let Err(e) = app_metrics_channel.send(AppMetricsEvent::RouteEvent(metrics_data)) {
            tracing::error!(target: "server_log", "Failed to send msg down app metrics channel.");
        };
        return Response::builder()
            .status(StatusCode::SERVICE_UNAVAILABLE)
            .body("Stats not available on this server.".to_string())
            .unwrap();
    }
}
