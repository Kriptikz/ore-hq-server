use app_rr_database::AppRRDatabase;
use axum::{
    http::{Response, StatusCode}, response::IntoResponse, Extension, Json
};
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::pubkey::Pubkey;
use spl_associated_token_account::get_associated_token_address;
use tracing::error;

use crate::{app_rr_database, ore_utils::{get_ore_mint, get_proof}, ChallengeWithDifficulty, Config, Txn};
use std::{str::FromStr, sync::Arc};


pub async fn get_challenges(
    Extension(app_rr_database): Extension<Arc<AppRRDatabase>>,
    Extension(app_config): Extension<Arc<Config>>,
) -> Result<Json<Vec<ChallengeWithDifficulty>>, String> {
    if app_config.stats_enabled {
        let res = app_rr_database
            .get_challenges()
            .await;

        match res {
            Ok(challenges) => {
                Ok(Json(challenges))
            }
            Err(_) => {
                Err("Failed to get submissions for miner".to_string())
            }
        }
    } else {
        return Err("Stats not enabled for this server.".to_string());
    }
}

pub async fn get_latest_mine_txn(
    Extension(app_rr_database): Extension<Arc<AppRRDatabase>>,
    Extension(app_config): Extension<Arc<Config>>,
) -> Result<Json<Txn>, String> {
    if app_config.stats_enabled {
        let res = app_rr_database
            .get_latest_mine_txn()
            .await;

        match res {
            Ok(txn) => {
                Ok(Json(txn))
            }
            Err(_) => {
                Err("Failed to get latest mine txn".to_string())
            }
        }
    } else {
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
            .get_pool_by_authority_pubkey(
                pubkey.to_string()
            )
            .await;

        match res {
            Ok(pool) => {
                Ok(Json(pool))
            }
            Err(_) => {
                Err("Failed to get pool data".to_string())
            }
        }
    } else {
        return Err("Stats not enabled for this server.".to_string());
    }
}

pub async fn get_pool_staked(
    Extension(app_config): Extension<Arc<Config>>,
    Extension(rpc_client): Extension<Arc<RpcClient>>,
) -> impl IntoResponse {
    if app_config.stats_enabled {
        let pubkey = Pubkey::from_str("mineXqpDeBeMR8bPQCyy9UneJZbjFywraS3koWZ8SSH").unwrap();
        let proof = if let Ok(loaded_proof) = get_proof(&rpc_client, pubkey).await {
            loaded_proof
        } else {
            error!("get_pool_staked: Failed to load proof.");
            return Err("Stats not enabled for this server.".to_string());
        };

        return Ok(Json(proof.balance))
    } else {
        return Err("Stats not enabled for this server.".to_string());
    }
}

pub async fn get_pool_balance(
    Extension(app_config): Extension<Arc<Config>>,
    Extension(rpc_client): Extension<Arc<RpcClient>>,
) -> impl IntoResponse {
    if app_config.stats_enabled {
        let pubkey = Pubkey::from_str("mineXqpDeBeMR8bPQCyy9UneJZbjFywraS3koWZ8SSH").unwrap();
        let miner_token_account = get_associated_token_address(&pubkey, &get_ore_mint());
        if let Ok(response) = rpc_client
            .get_token_account_balance(&miner_token_account)
            .await
        {
            return Response::builder()
                .status(StatusCode::OK)
                .body(response.ui_amount_string)
                .unwrap();
        } else {
            return Response::builder()
                .status(StatusCode::BAD_REQUEST)
                .body("Failed to get token account balance".to_string())
                .unwrap();
        }
    } else {
        return Response::builder()
            .status(StatusCode::SERVICE_UNAVAILABLE)
            .body("Stats not available on this server.".to_string())
            .unwrap();
    }
}
