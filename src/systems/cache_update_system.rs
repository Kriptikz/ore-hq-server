use std::{ops::Div, str::FromStr as _, sync::Arc, time::Duration};

use ore_boost_api::state::{boost_pda, stake_pda};
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::commitment_config::{CommitmentConfig, CommitmentLevel};
use steel::{AccountDeserialize as _, Pubkey};
use tokio::{sync::RwLock, time::Instant};
use base64::{prelude::BASE64_STANDARD, Engine};

use crate::{app_rr_database::AppRRDatabase, ore_utils::ORE_TOKEN_DECIMALS, BoostMultiplierCache, BoostMultiplierData, ChallengesCache, Config, LastChallengeSubmissionsCache, LatestBlockhashCache, WalletExtension};

const CACHED_BOOST_MULTIPLIER_UPDATE_INTERVAL: u64 = 15;
const CACHED_LAST_CHALLENGE_SUBMISSIONS_UPDATE_INTERVAL: u64 = 15;
const CACHED_CHALLENGES_UPDATE_INTERVAL: u64 = 15;
const CACHED_LATEST_BLOCKHASH_UPDATE_INTERVAL: u64 = 5;


pub async fn cache_update_system(
    app_config: Arc<Config>,
    rpc_client: Arc<RpcClient>,
    app_rr_database: Arc<AppRRDatabase>,
    boost_multiplier_cache: Arc<RwLock<BoostMultiplierCache>>,
    last_challenge_submission_cache: Arc<RwLock<LastChallengeSubmissionsCache>>,
    challenges_cache: Arc<RwLock<ChallengesCache>>,
    latest_blockhash_cache: Arc<RwLock<LatestBlockhashCache>>,
) {
    // Cached LatestBlockhash
    let cached_item = latest_blockhash_cache.clone();
    let app_rpc_client = rpc_client.clone();
    tokio::spawn(async move {
        let latest_blockhash_cache = cached_item;
        let rpc_client = app_rpc_client;
        loop {
            let lbhash = loop {
                match rpc_client.get_latest_blockhash_with_commitment(CommitmentConfig { commitment: CommitmentLevel::Finalized }).await {
                        Ok(lb) => {
                            //tracing::info!(target: "server_log", "Successfully updated latest blockhash");
                            break lb
                        },
                        Err(e) => {
                            tracing::error!(target: "server_log", "Failed to get latest blockhash in cache system. E: {:?}\n Retrying in 2 secs...", e);
                            tokio::time::sleep(Duration::from_secs(2000)).await;
                        }
                };
            };
            let serialized_blockhash = bincode::serialize(&lbhash).unwrap();
            let encoded_blockhash = BASE64_STANDARD.encode(serialized_blockhash);
            let mut writer = latest_blockhash_cache.write().await;
            writer.item = encoded_blockhash.clone();
            writer.last_updated_at = Instant::now();
            drop(writer);

            tokio::time::sleep(Duration::from_secs(CACHED_LATEST_BLOCKHASH_UPDATE_INTERVAL)).await;
        }
    });

    if app_config.stats_enabled {
        // Cached Boost Multiplier
        let bm_cache = boost_multiplier_cache.clone();
        let app_rpc_client = rpc_client.clone();
        tokio::spawn(async move {
            let boost_multiplier_cache = bm_cache;
            let rpc_client = app_rpc_client;
            loop {
                let mut boost_multiplier_datas = vec![];
                let mut writer = boost_multiplier_cache.write().await;
                writer.item = boost_multiplier_datas.clone();
                writer.last_updated_at = Instant::now();
                drop(writer);

                tokio::time::sleep(Duration::from_secs(CACHED_BOOST_MULTIPLIER_UPDATE_INTERVAL)).await;
            }
        });

        // Cached Last Challenge Submissions
        let cached_item = last_challenge_submission_cache.clone();
        let app_rr_db = app_rr_database.clone();
        tokio::spawn(async move {
            let last_challenge_submission_cache = cached_item;
            let app_rr_database = app_rr_db;
            loop {
                let res = app_rr_database.get_last_challenge_submissions().await;

                match res {
                    Ok(submissions) => {
                        let mut writer = last_challenge_submission_cache.write().await;
                        writer.item = submissions.clone();
                        writer.last_updated_at = Instant::now();
                        drop(writer);
                    }
                    Err(_) => {},
                }

                tokio::time::sleep(Duration::from_secs(CACHED_LAST_CHALLENGE_SUBMISSIONS_UPDATE_INTERVAL)).await;
            }
        });

        // Cached Challenges
        let cached_item = challenges_cache.clone();
        let app_rr_db = app_rr_database.clone();
        tokio::spawn(async move {
            let challenges_cache = cached_item;
            let app_rr_database = app_rr_db;
            loop {
                let res = app_rr_database.get_challenges().await;

                match res {
                    Ok(challenges) => {
                        let mut writer = challenges_cache.write().await;
                        writer.item = challenges.clone();
                        writer.last_updated_at = Instant::now();
                        drop(writer);
                    }
                    Err(_) => {},
                }

                tokio::time::sleep(Duration::from_secs(CACHED_CHALLENGES_UPDATE_INTERVAL)).await;
            }
        });
    }
}
