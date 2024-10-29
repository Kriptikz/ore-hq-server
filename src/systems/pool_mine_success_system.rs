use axum::extract::ws::Message;
use base64::{prelude::BASE64_STANDARD, Engine};
use futures::SinkExt;
use std::{
    ops::Div,
    sync::Arc,
    time::Duration,
};

use solana_sdk::
    signer::Signer
;
use tokio::{
    sync::{mpsc::UnboundedReceiver, RwLock}, time::Instant}
;
use tracing::info;

use crate::{
    app_database::AppDatabase, message::ServerMessagePoolSubmissionResult, ore_utils::
        ORE_TOKEN_DECIMALS, AppState, ClientVersion, Config, InsertEarning, InsertSubmission, MessageInternalMineSuccess, UpdateReward, UpdateStakeAccountRewards, WalletExtension
};

pub const ORE_STAKE_PERCENTAGE: u64 = 16;
pub const ORE_SOL_STAKE_PERCENTAGE: u64 = 24;
pub const ORE_ISC_STAKE_PERCENTAGE: u64 = 20;
pub const TOTAL_STAKER_PERCENTAGE: u64 = ORE_STAKE_PERCENTAGE + ORE_SOL_STAKE_PERCENTAGE + ORE_ISC_STAKE_PERCENTAGE;

pub async fn pool_mine_success_system(
    app_shared_state: Arc<RwLock<AppState>>,
    app_database: Arc<AppDatabase>,
    app_config: Arc<Config>,
    app_wallet: Arc<WalletExtension>,
    mut mine_success_receiver: UnboundedReceiver<MessageInternalMineSuccess>
) {
    loop {
        while let Some(msg) = mine_success_receiver.recv().await {
            let id = uuid::Uuid::new_v4();
            let c = BASE64_STANDARD.encode(msg.challenge);
            info!(target: "server_log", "{} - Processing internal mine success for challenge: {}", id, c);
            {
                let instant = Instant::now();
                info!(target: "server_log", "{} - Getting sockets.", id);
                let shared_state = app_shared_state.read().await;
                let len = shared_state.sockets.len();
                let socks = shared_state.sockets.clone();
                drop(shared_state);
                info!(target: "server_log", "{} - Got sockets in {}.", id, instant.elapsed().as_millis());

                let mut i_earnings = Vec::new();
                let mut i_rewards = Vec::new();
                let mut i_submissions = Vec::new();

                let instant = Instant::now();
                info!(target: "server_log", "{} - Processing submission results for challenge: {}.", id, c);
                let staker_rewards = (msg.rewards as u128).saturating_mul(TOTAL_STAKER_PERCENTAGE as u128).saturating_div(100) as u64;
                let total_rewards = msg.rewards - msg.commissions - staker_rewards;
                info!(target: "server_log", "{} - Miners Rewards: {}", id, total_rewards);
                info!(target: "server_log", "{} - Commission: {}", id, msg.commissions);
                info!(target: "server_log", "{} - Staker Rewards: {}", id, staker_rewards);
                let mut total_miners_earned_rewards = 0;
                for (miner_pubkey, msg_submission) in msg.submissions.iter() {
                    let decimals = 10f64.powf(ORE_TOKEN_DECIMALS as f64);
                    let earned_rewards = (total_rewards as u128).saturating_mul(msg_submission.hashpower as u128).saturating_div(msg.total_hashpower as u128) as u64;
                    total_miners_earned_rewards += earned_rewards;

                    let new_earning = InsertEarning {
                        miner_id: msg_submission.miner_id,
                        pool_id: app_config.pool_id,
                        challenge_id: msg.challenge_id,
                        amount: earned_rewards,
                    };

                    let new_submission = InsertSubmission {
                        miner_id: msg_submission.miner_id,
                        challenge_id: msg.challenge_id,
                        nonce: msg_submission.supplied_nonce,
                        difficulty: msg_submission.supplied_diff as i8,
                    };

                    let new_reward = UpdateReward {
                        miner_id: msg_submission.miner_id,
                        balance: earned_rewards,
                    };

                    i_earnings.push(new_earning);
                    i_rewards.push(new_reward);
                    i_submissions.push(new_submission);
                    //let _ = app_database.add_new_earning(new_earning).await.unwrap();

                    let earned_rewards_dec = (earned_rewards as f64).div(decimals);
                    let pool_rewards_dec = (msg.rewards as f64).div(decimals);

                    let percentage = if pool_rewards_dec != 0.0 {
                        (earned_rewards_dec / pool_rewards_dec) * 100.0
                    } else {
                        0.0 // Handle the case where pool_rewards_dec is 0 to avoid division by zero
                    };

                    let top_stake = if let Some(config) = msg.ore_config {
                        (config.top_balance as f64).div(decimals)
                    } else {
                        1.0f64
                    };

                    for (_addr, client_connection) in socks.iter() {
                        if client_connection.pubkey.eq(&miner_pubkey) {
                            let socket_sender = client_connection.socket.clone();

                            match client_connection.client_version {
                                ClientVersion::V1 => {
                                    let message = format!(
                                        "Pool Submitted Difficulty: {}\nPool Earned:  {:.11} ORE\nPool Balance: {:.11} ORE\nTop Stake:    {:.11} ORE\nPool Multiplier: {:.2}x\n----------------------\nActive Miners: {}\n----------------------\nMiner Submitted Difficulty: {}\nMiner Earned: {:.11} ORE\n{:.2}% of total pool reward",
                                        msg.difficulty,
                                        pool_rewards_dec,
                                        msg.total_balance,
                                        top_stake,
                                        msg.multiplier,
                                        len,
                                        msg_submission.supplied_diff,
                                        earned_rewards_dec,
                                        percentage
                                    );
                                    tokio::spawn(async move {
                                        if let Ok(_) = socket_sender
                                            .lock()
                                            .await
                                            .send(Message::Text(message))
                                            .await
                                        {
                                        } else {
                                            tracing::error!(target: "server_log", "Failed to send client text");
                                        }
                                    });
                                }
                                ClientVersion::V2 => {
                                    let server_message = ServerMessagePoolSubmissionResult::new(
                                        msg.difficulty,
                                        msg.total_balance,
                                        pool_rewards_dec,
                                        top_stake,
                                        msg.multiplier,
                                        len as u32,
                                        msg.challenge,
                                        msg.best_nonce,
                                        msg_submission.supplied_diff as u32,
                                        earned_rewards_dec,
                                        percentage,
                                    );
                                    tokio::spawn(async move {
                                        if let Ok(_) = socket_sender
                                            .lock()
                                            .await
                                            .send(Message::Binary(
                                                server_message.to_message_binary(),
                                            ))
                                            .await
                                        {
                                        } else {
                                            tracing::error!(target: "server_log", "Failed to send client pool submission result binary message");
                                        }
                                    });
                                }
                            }
                        }
                    }
                }

                info!(target: "server_log", "{} - Finished processing submission results in {}ms for challenge: {}.", id, instant.elapsed().as_millis(), c);

                let instant = Instant::now();
                info!(target: "server_log", "{} - Adding earnings", id);
                let batch_size = 200;
                if i_earnings.len() > 0 {
                    for batch in i_earnings.chunks(batch_size) {
                        while let Err(_) =
                            app_database.add_new_earnings_batch(batch.to_vec()).await
                        {
                            tracing::error!(target: "server_log", "{} - Failed to add new earnings batch to db. Retrying...", id);
                            tokio::time::sleep(Duration::from_millis(500)).await;
                        }
                        tokio::time::sleep(Duration::from_millis(200)).await;
                    }
                    info!(target: "server_log", "{} - Successfully added earnings batch", id);
                }
                info!(target: "server_log", "{} - Added earnings in {}ms", id, instant.elapsed().as_millis());

                tokio::time::sleep(Duration::from_millis(500)).await;

                let instant = Instant::now();
                info!(target: "server_log", "{} - Updating rewards", id);
                if i_rewards.len() > 0 {
                    let mut batch_num = 1;
                    for batch in i_rewards.chunks(batch_size) {
                        let instant = Instant::now();
                        info!(target: "server_log", "{} - Updating reward batch {}", id, batch_num);
                        while let Err(_) = app_database.update_rewards(batch.to_vec()).await {
                            tracing::error!(target: "server_log", "{} - Failed to update rewards in db. Retrying...", id);
                            tokio::time::sleep(Duration::from_millis(500)).await;
                        }
                        info!(target: "server_log", "{} - Updated reward batch {} in {}ms", id, batch_num, instant.elapsed().as_millis());
                        batch_num += 1;
                        tokio::time::sleep(Duration::from_millis(200)).await;
                    }
                    info!(target: "server_log", "{} - Successfully updated rewards", id);
                }
                info!(target: "server_log", "{} - Updated rewards in {}ms", id, instant.elapsed().as_millis());

                tokio::time::sleep(Duration::from_millis(500)).await;

                let instant = Instant::now();
                info!(target: "server_log", "{} - Adding submissions", id);
                if i_submissions.len() > 0 {
                    for batch in i_submissions.chunks(batch_size) {
                        info!(target: "server_log", "{} - Submissions batch size: {}", id, i_submissions.len());
                        while let Err(_) =
                            app_database.add_new_submissions_batch(batch.to_vec()).await
                        {
                            tracing::error!(target: "server_log", "{} - Failed to add new submissions batch. Retrying...", id);
                            tokio::time::sleep(Duration::from_millis(500)).await;
                        }
                        tokio::time::sleep(Duration::from_millis(200)).await;
                    }

                    info!(target: "server_log", "{} - Successfully added submissions batch", id);
                }
                info!(target: "server_log", "{} - Added submissions in {}ms", id, instant.elapsed().as_millis());

                tokio::time::sleep(Duration::from_millis(500)).await;

                let instant = Instant::now();
                info!(target: "server_log", "{} - Updating pool rewards", id);
                while let Err(_) = app_database
                    .update_pool_rewards(
                        app_wallet.miner_wallet.pubkey().to_string(),
                        msg.rewards,
                    )
                    .await
                {
                    tracing::error!(target: "server_log",
                        "{} - Failed to update pool rewards! Retrying...", id
                    );
                    tokio::time::sleep(Duration::from_millis(1000)).await;
                }
                info!(target: "server_log", "{} - Updated pool rewards in {}ms", id, instant.elapsed().as_millis());

                tokio::time::sleep(Duration::from_millis(200)).await;

                let instant = Instant::now();
                info!(target: "server_log", "{} - Updating challenge rewards", id);
                if let Ok(s) = app_database
                    .get_submission_id_with_nonce(msg.best_nonce)
                    .await
                {
                    if let Err(_) = app_database
                        .update_challenge_rewards(msg.challenge.to_vec(), s, msg.rewards)
                        .await
                    {
                        tracing::error!(target: "server_log", "{} - Failed to update challenge rewards! Skipping! Devs check!", id);
                        let err_str = format!("{} - Challenge UPDATE FAILED - Challenge: {:?}\nSubmission ID: {}\nRewards: {}\n", id, msg.challenge.to_vec(), s, msg.rewards);
                        tracing::error!(target: "server_log", err_str);
                    }
                    info!(target: "server_log", "{} - Updated challenge rewards in {}ms", id, instant.elapsed().as_millis());
                } else {
                    tracing::error!(target: "server_log", "{} - Failed to get submission id with nonce: {} for challenge_id: {}", id, msg.best_nonce, msg.challenge_id);
                    tracing::error!(target: "server_log", "{} - Failed update challenge rewards!", id);
                    let mut found_best_nonce = false;
                    for submission in i_submissions {
                        if submission.nonce == msg.best_nonce {
                            found_best_nonce = true;
                            break;
                        }
                    }

                    if found_best_nonce {
                        info!(target: "server_log", "{} - Found best nonce in i_submissions", id);
                    } else {
                        info!(target: "server_log", "{} - Failed to find best nonce in i_submissions", id);
                    }
                }

                info!(target: "server_log", "{} - Processing stakers rewards", id);
                process_stakers_rewards(msg.rewards, staker_rewards, &app_database, &app_config).await;
                info!(target: "server_log", "{} - Total Distributed For Miners: {}", id, total_miners_earned_rewards);


                info!(target: "server_log", "{} - Finished processing internal mine success for challenge: {}", id, c);
            }
        }
    }
}

pub async fn process_stakers_rewards(total_rewards: u64, staker_rewards: u64, app_database: &Arc<AppDatabase>, app_config: &Arc<Config>) {
    let ore_rewards = (total_rewards as u128).saturating_mul(ORE_STAKE_PERCENTAGE as u128).saturating_div(100) as u64;
    let ore_sol_rewards = (total_rewards as u128).saturating_mul(ORE_SOL_STAKE_PERCENTAGE as u128).saturating_div(100) as u64;
    let ore_isc_rewards = (total_rewards as u128).saturating_mul(ORE_ISC_STAKE_PERCENTAGE as u128).saturating_div(100) as u64;

    info!(target: "server_log", "Total Rewards: {}", total_rewards);
    info!(target: "server_log", "ore Rewards ({}%): {}", ORE_STAKE_PERCENTAGE, ore_rewards);
    info!(target: "server_log", "ore-sol Rewards ({}%): {}", ORE_SOL_STAKE_PERCENTAGE, ore_sol_rewards);
    info!(target: "server_log", "ore-isc Rewards ({}%): {}", ORE_ISC_STAKE_PERCENTAGE, ore_isc_rewards);

    if ore_rewards + ore_sol_rewards + ore_isc_rewards > staker_rewards {
        tracing::error!(target: "server_log", "Calculations exceeded max staker rewards of 40%!!!");
        return;
    }

    // get all the stake accounts for ore mint
    let mut ore_stake_accounts = vec![]; 
    let mut total_ore_boosted = 0;
    let mut last_id: i32 = 0;
    loop {
        tokio::time::sleep(Duration::from_millis(100)).await;
        match app_database.get_staker_accounts_for_mint(app_config.pool_id, "oreoU2P8bN6jkk3jbaiVxYnG1dCXcYxwhwyK9jSybcp".to_string(), last_id, 1).await {
            Ok(d) => {
                if d.len() > 0 {
                    for ac in d.iter() {
                        last_id = ac.id;
                        total_ore_boosted += ac.staked_balance;
                        ore_stake_accounts.push(ac.clone());
                    }
                }
                
                if d.len() < 500 {
                    break;
                }
            },
            Err(e) => {
                tracing::error!(target: "server_log", "Failed to get staker accounts for ore");
                tracing::error!(target: "server_log", "Error: {:?}", e);
            }
        };
    }

    tracing::info!(target: "server_log", "Found {} ore stake accounts.", ore_stake_accounts.len());
    tracing::info!(target: "server_log", "Total {} ore boosted.", total_ore_boosted as f64 / 10f64.powf(ORE_TOKEN_DECIMALS as f64));

    // get all the stake accounts for ore-sol mint
    let mut ore_sol_stake_accounts = vec![]; 
    let mut total_ore_sol_boosted = 0;
    let mut last_id: i32 = 0;
    loop {
        tokio::time::sleep(Duration::from_millis(100)).await;
        match app_database.get_staker_accounts_for_mint(app_config.pool_id, "DrSS5RM7zUd9qjUEdDaf31vnDUSbCrMto6mjqTrHFifN".to_string(), last_id, 1).await {
            Ok(d) => {
                if d.len() > 0 {
                    for ac in d.iter() {
                        last_id = ac.id;
                        total_ore_sol_boosted += ac.staked_balance;
                        ore_sol_stake_accounts.push(ac.clone());
                    }
                }
                
                if d.len() < 500 {
                    break;
                }
            },
            Err(e) => {
                tracing::error!(target: "server_log", "Failed to get staker accounts for ore-sol");
                tracing::error!(target: "server_log", "Error: {:?}", e);
            }
        };
    }

    tracing::info!(target: "server_log", "Found {} ore-sol stake accounts.", ore_sol_stake_accounts.len());
    tracing::info!(target: "server_log", "Total {} ore-sol boosted.", total_ore_sol_boosted as f64 / 10f64.powf(ORE_TOKEN_DECIMALS as f64));

    // get all the stake accounts for ore-isc mint
    let mut ore_isc_stake_accounts = vec![]; 
    let mut total_ore_isc_boosted = 0;
    let mut last_id: i32 = 0;
    loop {
        tokio::time::sleep(Duration::from_millis(100)).await;
        match app_database.get_staker_accounts_for_mint(app_config.pool_id, "meUwDp23AaxhiNKaQCyJ2EAF2T4oe1gSkEkGXSRVdZb".to_string(), last_id, 1).await {
            Ok(d) => {
                if d.len() > 0 {
                    for ac in d.iter() {
                        last_id = ac.id;
                        total_ore_isc_boosted += ac.staked_balance;
                        ore_isc_stake_accounts.push(ac.clone());
                    }
                }
                
                if d.len() < 500 {
                    break;
                }
            },
            Err(e) => {
                tracing::error!(target: "server_log", "Failed to get staker accounts for ore-isc");
                tracing::error!(target: "server_log", "Error: {:?}", e);
            }
        };
    }

    tracing::info!(target: "server_log", "Found {} ore-isc stake accounts.", ore_isc_stake_accounts.len());
    tracing::info!(target: "server_log", "Total {} ore-isc boosted.", total_ore_isc_boosted as f64 / 10f64.powf(ORE_TOKEN_DECIMALS as f64));

    let mut update_stake_rewards = vec![];
    let mut total_distributed_for_ore = 0;
    if total_ore_boosted > 0 {
        for ore_stake_account in ore_stake_accounts.iter() {
            let rewards_balance = (ore_rewards as u128 * ore_stake_account.staked_balance as u128 / total_ore_boosted as u128) as u64;
            let stake_rewards = UpdateStakeAccountRewards {
                stake_pda: ore_stake_account.stake_pda.clone(),
                rewards_balance,
            };
            total_distributed_for_ore += rewards_balance;
            update_stake_rewards.push(stake_rewards);
        }
    }

    let mut total_distributed_for_ore_sol = 0;
    if total_ore_sol_boosted > 0 {
        for ore_sol_stake_account in ore_sol_stake_accounts.iter() {
            let rewards_balance = (ore_sol_rewards as u128 * ore_sol_stake_account.staked_balance as u128 / total_ore_sol_boosted as u128) as u64;
            let stake_rewards = UpdateStakeAccountRewards {
                stake_pda: ore_sol_stake_account.stake_pda.clone(),
                rewards_balance,
            };
            total_distributed_for_ore_sol += rewards_balance;
            update_stake_rewards.push(stake_rewards);
        }
    }

    let mut total_distributed_for_ore_isc = 0;
    if total_ore_isc_boosted > 0 {
        for ore_isc_stake_account in ore_isc_stake_accounts.iter() {
            let rewards_balance = (ore_isc_rewards as u128 * ore_isc_stake_account.staked_balance as u128 / total_ore_isc_boosted as u128) as u64;
            let stake_rewards = UpdateStakeAccountRewards {
                stake_pda: ore_isc_stake_account.stake_pda.clone(),
                rewards_balance,
            };
            total_distributed_for_ore_isc += rewards_balance;
            update_stake_rewards.push(stake_rewards);
        }
    }



    let instant = Instant::now();
    info!(target: "server_log", "Total distributed to stakers: {}", total_distributed_for_ore + total_distributed_for_ore_sol + total_distributed_for_ore_isc);
    info!(target: "server_log", "Total distributed for ore: {}", total_distributed_for_ore);
    info!(target: "server_log", "Total distributed for ore_sol: {}", total_distributed_for_ore_sol);
    info!(target: "server_log", "Total distributed for ore_isc: {}", total_distributed_for_ore_isc);

    let batch_size = 200;
     info!(target: "server_log", "Updating staking rewards");
     if update_stake_rewards.len() > 0 {
         let mut batch_num = 1;
         for batch in update_stake_rewards.chunks(batch_size) {
             let instant = Instant::now();
             info!(target: "server_log", "Updating stake reward batch {}", batch_num);
             while let Err(_) = app_database.update_stake_accounts_rewards(batch.to_vec()).await {
                 tracing::error!(target: "server_log", "Failed to update rewards in db. Retrying...");
                 tokio::time::sleep(Duration::from_millis(500)).await;
             }
             info!(target: "server_log", "Updated reward batch {} in {}ms", batch_num, instant.elapsed().as_millis());
             batch_num += 1;
             tokio::time::sleep(Duration::from_millis(200)).await;
         }
         info!(target: "server_log", "Successfully updated rewards");
     }
    info!(target: "server_log", "Updated rewards in {}ms", instant.elapsed().as_millis());
}








