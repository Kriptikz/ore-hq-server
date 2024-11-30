use std::{ops::Div, str::FromStr as _, sync::Arc, time::Duration};

use ore_boost_api::state::{boost_pda, stake_pda};
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::commitment_config::{CommitmentConfig, CommitmentLevel};
use steel::{AccountDeserialize as _, Pubkey};
use tokio::{sync::RwLock, time::Instant};
use base64::{prelude::BASE64_STANDARD, Engine};

use crate::{app_database::AppDatabase, app_rr_database::AppRRDatabase, ore_utils::ORE_TOKEN_DECIMALS, BoostMultiplierCache, BoostMultiplierData, ChallengesCache, Config, LastChallengeSubmissionsCache, LatestBlockhashCache, WalletExtension};

const CACHED_BOOST_MULTIPLIER_UPDATE_INTERVAL: u64 = 30;
const CACHED_LAST_CHALLENGE_SUBMISSIONS_UPDATE_INTERVAL: u64 = 60;
const CACHED_CHALLENGES_UPDATE_INTERVAL: u64 = 60;
const CACHED_LATEST_BLOCKHASH_UPDATE_INTERVAL: u64 = 20;


pub async fn cache_update_system(
    app_config: Arc<Config>,
    rpc_client: Arc<RpcClient>,
    app_rr_database: Arc<AppRRDatabase>,
    boost_multiplier_cache: Arc<RwLock<BoostMultiplierCache>>,
    last_challenge_submission_cache: Arc<RwLock<LastChallengeSubmissionsCache>>,
    challenges_cache: Arc<RwLock<ChallengesCache>>,
    latest_blockhash_cache: Arc<RwLock<LatestBlockhashCache>>,
) {
    if app_config.stats_enabled {
        loop {
            let reader = boost_multiplier_cache.read().await;
            let cached_boost_multiplier = reader.clone();
            drop(reader);
            // Cached Boost Multiplier
            if cached_boost_multiplier.item.len() <= 0 || cached_boost_multiplier.last_updated_at.elapsed().as_secs() > CACHED_BOOST_MULTIPLIER_UPDATE_INTERVAL {
                tracing::info!(target: "server_log", "get_boost_multiplier");
                let pubkey = Pubkey::from_str("mineXqpDeBeMR8bPQCyy9UneJZbjFywraS3koWZ8SSH").unwrap();
                let managed_proof = Pubkey::find_program_address(
                    &[b"managed-proof-account", pubkey.as_ref()],
                    &ore_miner_delegation::id(),
                );

                let boost_mints = vec![
                    Pubkey::from_str("oreoU2P8bN6jkk3jbaiVxYnG1dCXcYxwhwyK9jSybcp").unwrap(),
                    Pubkey::from_str("DrSS5RM7zUd9qjUEdDaf31vnDUSbCrMto6mjqTrHFifN").unwrap(),
                    Pubkey::from_str("meUwDp23AaxhiNKaQCyJ2EAF2T4oe1gSkEkGXSRVdZb").unwrap()
                ];

                // Get pools boost stake accounts
                let mut boost_stake_acct_pdas = vec![];
                let mut boost_acct_pdas = vec![];

                for boost_mint in boost_mints {
                    let boost_account_pda = boost_pda(boost_mint);
                    let boost_stake_pda = stake_pda(managed_proof.0, boost_account_pda.0);
                    tracing::info!(target: "server_log", "Boost stake PDA: {}", boost_stake_pda.0.to_string());
                    tracing::info!(target: "server_log", "Boost PDA: {}", boost_account_pda.0.to_string());
                    boost_stake_acct_pdas.push(boost_stake_pda.0);
                    boost_acct_pdas.push(boost_account_pda.0);
                }

                let mut stake_acct = vec![];
                let mut boost_acct = vec![];
                if let Ok(accounts) = rpc_client.get_multiple_accounts(&[boost_stake_acct_pdas, boost_acct_pdas].concat()).await {
                    tracing::info!(target: "server_log", "Got {} accounts", accounts.len());
                    for account in accounts {
                        if let Some(acc) = account {
                            if let Ok(a) = ore_boost_api::state::Stake::try_from_bytes(&acc.data) {
                                tracing::info!(target: "server_log", "Boost stake account: {:?}", a);
                                stake_acct.push(a.clone());
                                continue;
                            }
                            if let Ok(a) = ore_boost_api::state::Boost::try_from_bytes(&acc.data) {
                                tracing::info!(target: "server_log", "Boost account: {:?}", a);
                                boost_acct.push(a.clone());
                                continue;
                            }
                        }
                    }
                } else {
                    tracing::error!(target: "server_log", "Failed to get accounts.")
                }
                let decimals = 10f64.powf(ORE_TOKEN_DECIMALS as f64);

                let mut boost_multiplier_datas = vec![];
                for (index,stake_a) in stake_acct.iter().enumerate() {
                    boost_multiplier_datas.push(BoostMultiplierData {
                        boost_mint: boost_acct[index].mint.to_string(),
                        staked_balance: (stake_a.balance as f64).div(decimals),
                        total_stake_balance: (boost_acct[index].total_stake as f64).div(decimals),
                        multiplier: boost_acct[index].multiplier,
                    })
                }
                let mut writer = boost_multiplier_cache.write().await;
                writer.item = boost_multiplier_datas.clone();
                writer.last_updated_at = Instant::now();
                drop(writer);
            }

            // Cached Last Challenge Submissions
            let reader = last_challenge_submission_cache.read().await;
            let cached_boost_multiplier = reader.clone();
            drop(reader);

            if cached_boost_multiplier.item.len() <= 0 || cached_boost_multiplier.last_updated_at.elapsed().as_secs() > CACHED_LAST_CHALLENGE_SUBMISSIONS_UPDATE_INTERVAL {
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
            }

            // Cached Challenges
            let reader = challenges_cache.read().await;
            let cached_challenges = reader.clone();
            drop(reader);
            if cached_challenges.item.len() <= 0 || cached_challenges.last_updated_at.elapsed().as_secs() > CACHED_CHALLENGES_UPDATE_INTERVAL {
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
            }

            // Cached LatestBlockhash
            let reader = latest_blockhash_cache.read().await;
            let cached_lb = reader.clone();
            drop(reader);
            if cached_lb.last_updated_at.elapsed().as_secs() > CACHED_LATEST_BLOCKHASH_UPDATE_INTERVAL {
                let lbhash;
                loop {
                    match rpc_client.get_latest_blockhash_with_commitment(CommitmentConfig { commitment: CommitmentLevel::Finalized }).await {
                            Ok(lb) => {
                                lbhash = lb;
                                break;
                            },
                            Err(e) => {
                                tracing::error!(target: "server_log", "Failed to get latest blockhash in cache system. E: {:?}\n Retrying in 2 secs...", e);
                                tokio::time::sleep(Duration::from_secs(2000)).await;
                            }
                    };
                }
                let serialized_blockhash = bincode::serialize(&lbhash).unwrap();
                let encoded_blockhash = BASE64_STANDARD.encode(serialized_blockhash);
                let mut writer = latest_blockhash_cache.write().await;
                writer.item = encoded_blockhash.clone();
                writer.last_updated_at = Instant::now();
                drop(writer);
            }

            tokio::time::sleep(Duration::from_secs(5)).await;
        }
    }
}
