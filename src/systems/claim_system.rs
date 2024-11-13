use std::{sync::Arc, time::Duration};

use solana_client::{nonblocking::rpc_client::RpcClient, rpc_config::RpcSendTransactionConfig};
use solana_sdk::{
    compute_budget::ComputeBudgetInstruction,
    signature::{Keypair, Signature},
    signer::Signer,
    transaction::Transaction,
};
use solana_transaction_status::TransactionConfirmationStatus;
use spl_associated_token_account::get_associated_token_address;
use steel::Pubkey;
use tokio::{sync::mpsc::UnboundedSender, time::Instant};
use tracing::{error, info};

use crate::{
    app_database::AppDatabase, app_metrics::{AppMetricsEvent, MetricsProcessingClaimsEventData}, ore_utils::{get_ore_mint, ORE_TOKEN_DECIMALS}, ClaimsQueue, ClaimsQueueItem, InsertClaim, InsertTxn
};

pub async fn claim_system(
    claims_queue: Arc<ClaimsQueue>,
    rpc_client: Arc<RpcClient>,
    wallet: Arc<Keypair>,
    app_database: Arc<AppDatabase>,
    app_metrics_sender: UnboundedSender<AppMetricsEvent>,
) {
    loop {
        let mut handles = Vec::new();

        let mut first_claim = None;
        let mut second_claim = None;
        let mut third_claim = None;
        let reader = claims_queue.queue.read().await;
        let claims_queue_len = reader.len();
        info!(target: "server_log", "Claims queue length: {}", claims_queue_len);

        if claims_queue_len > 0 {
            let processing_claims_event_data = MetricsProcessingClaimsEventData {
                claims_queue_length: claims_queue_len,
            };
            match app_metrics_sender.send(AppMetricsEvent::ProcessingClaimsEvent(processing_claims_event_data)) {
                Ok(_) => {}
                Err(_) => {
                    tracing::error!(target: "server_log", "Failed to send AppMetricsEvent down app_metrics_sender mpsc channel.");
                }
            }
            let mut reader_iter = reader.iter();
            let first_item = reader_iter.next();
            if let Some(item) = first_item {
                first_claim = Some((item.0.clone(), item.1.clone()));
            }
            let second_item = reader_iter.next();
            if let Some(item) = second_item {
                second_claim = Some((item.0.clone(), item.1.clone()));
            }
            let third_item = reader_iter.next();
            if let Some(item) = third_item {
                third_claim = Some((item.0.clone(), item.1.clone()));
            }
            drop(reader);

            let cq = claims_queue.clone();
            let rpc = rpc_client.clone();
            let w = wallet.clone();
            let adb = app_database.clone();
            if let Some(((user_pubkey, _mint_pubkey), claim_queue_item)) = first_claim {
                handles.push(tokio::spawn(async move {
                    let claims_queue = cq;
                    let rpc_client = rpc;
                    let wallet = w;
                    let app_database = adb;

                        process_claim(user_pubkey, claim_queue_item, rpc_client, wallet, app_database, claims_queue).await;
                }));
            }
            let cq = claims_queue.clone();
            let rpc = rpc_client.clone();
            let w = wallet.clone();
            let adb = app_database.clone();
            if let Some(((user_pubkey, _mint_pubkey), claim_queue_item)) = second_claim {
                handles.push(tokio::spawn(async move {
                    let claims_queue = cq;
                    let rpc_client = rpc;
                    let wallet = w;
                    let app_database = adb;

                        process_claim(user_pubkey, claim_queue_item, rpc_client, wallet, app_database, claims_queue).await;
                }));
            }

            let cq = claims_queue.clone();
            let rpc = rpc_client.clone();
            let w = wallet.clone();
            let adb = app_database.clone();
            if let Some(((user_pubkey, _mint_pubkey), claim_queue_item)) = third_claim {
                handles.push(tokio::spawn(async move {
                    let claims_queue = cq;
                    let rpc_client = rpc;
                    let wallet = w;
                    let app_database = adb;

                        process_claim(user_pubkey, claim_queue_item, rpc_client, wallet, app_database, claims_queue).await;
                }));
            }

            for handle in handles {
                // wait for spawned tasks to finish
                let _ = handle.await;
            }
        }

        tokio::time::sleep(Duration::from_secs(2)).await;
    }
}

async fn process_claim(user_pubkey: Pubkey, claim_queue_item: ClaimsQueueItem, rpc_client: Arc<RpcClient>, wallet: Arc<Keypair>, app_database: Arc<AppDatabase>, claims_queue: Arc<ClaimsQueue>) {
    if let Some(mint_pubkey) = claim_queue_item.mint {
        info!(target: "server_log", "Processing stakers claim");
        let staker_pubkey = user_pubkey;
        let ore_mint = get_ore_mint();
        let receiver_pubkey = claim_queue_item.receiver_pubkey;
        let receiver_token_account = get_associated_token_address(&receiver_pubkey, &ore_mint);
        let amount = claim_queue_item.amount;

        let mut claim_amount = amount;

        if let Ok(stake_account) = app_database
            .get_staker_rewards(staker_pubkey.to_string(), mint_pubkey.to_string())
            .await
        {
            if amount > stake_account.rewards_balance {
                let mut writer = claims_queue.queue.write().await;
                writer.remove(&(staker_pubkey, Some(mint_pubkey)));
                drop(writer);
                return;
            }
        } else {
            let mut writer = claims_queue.queue.write().await;
            writer.remove(&(staker_pubkey, Some(mint_pubkey)));
            drop(writer);
            return;
        }

        let prio_fee: u32 = 20_000;

        let mut is_creating_ata = false;
        let mut ixs = Vec::new();
        let prio_fee_ix = ComputeBudgetInstruction::set_compute_unit_price(prio_fee as u64);
        ixs.push(prio_fee_ix);
        if let Ok(response) = rpc_client
            .get_token_account_balance(&receiver_token_account)
            .await
        {
            if let Some(_amount) = response.ui_amount {
                info!(target: "server_log", "staker claim beneficiary has valid token account.");
            } else {
                info!(target: "server_log", "will create token account for staker claim beneficiary");
                ixs.push(
                    spl_associated_token_account::instruction::create_associated_token_account(
                        &wallet.pubkey(),
                        &receiver_pubkey,
                        &ore_api::consts::MINT_ADDRESS,
                        &spl_token::id(),
                    ),
                )
            }
        } else {
            info!(target: "server_log", "Adding create ata ix for staker claim");
            is_creating_ata = true;
            ixs.push(
                spl_associated_token_account::instruction::create_associated_token_account(
                    &wallet.pubkey(),
                    &receiver_pubkey,
                    &ore_api::consts::MINT_ADDRESS,
                    &spl_token::id(),
                ),
            )
        }

        // 0.02_000_000_000
        if is_creating_ata {
            claim_amount = amount - 2_000_000_000
        }
        let ix =
            crate::ore_utils::get_claim_ix(wallet.pubkey(), receiver_token_account, claim_amount);
        ixs.push(ix);

        if let Ok((hash, _slot)) = rpc_client
            .get_latest_blockhash_with_commitment(rpc_client.commitment())
            .await
        {
            let expired_timer = Instant::now();
            let mut tx = Transaction::new_with_payer(&ixs, Some(&wallet.pubkey()));

            tx.sign(&[&wallet], hash);

            let rpc_config = RpcSendTransactionConfig {
                preflight_commitment: Some(rpc_client.commitment().commitment),
                ..RpcSendTransactionConfig::default()
            };

            let signature;
            let mut send_attempts = 1;
            loop {
                match rpc_client
                    .send_transaction_with_config(&tx, rpc_config)
                    .await 
                {
                        Ok(sig) => {
                            signature = sig;
                            break;
                        },
                        Err(e) => {
                            if send_attempts > 10 {
                                error!(target: "server_log", "Failed to send stakers claim transaction.\nError: {:?}.\nRetry Limit Reached. Removing claim from queue.", e);
                                let mut writer = claims_queue.queue.write().await;
                                writer.remove(&(staker_pubkey, Some(mint_pubkey)));
                                drop(writer);
                                return;
                            } else {
                                send_attempts += 1;
                                error!(target: "server_log", "Failed to send stakers claim transaction.\nError: {:?}.\nRetrying in 2 seconds...", e);
                                tokio::time::sleep(Duration::from_millis(2000)).await;
                            }
                        }
                }
            }

            let result: Result<Signature, String> = loop {
                if expired_timer.elapsed().as_secs() >= 360 {
                    break Err("Transaction Expired".to_string());
                }
                let results = rpc_client.get_signature_statuses(&[signature]).await;
                if let Ok(response) = results {
                    let statuses = response.value;
                    if let Some(status) = &statuses[0] {
                        if status.confirmation_status()
                            == TransactionConfirmationStatus::Confirmed
                        {
                            if status.err.is_some() {
                                let e_str = format!("Stake Claim Transaction Failed: {:?}", status.err);
                                break Err(e_str);
                            }
                            break Ok(signature);
                        }
                    }
                }
                tokio::time::sleep(Duration::from_millis(2000)).await;
            };

            match result {
                Ok(sig) => {
                    let amount_dec = amount as f64 / 10f64.powf(ORE_TOKEN_DECIMALS as f64);
                    info!(target: "server_log", "Staker {} successfully claimed {}.\nSig: {}", staker_pubkey.to_string(), amount_dec, sig.to_string());

                    // TODO: use transacions, or at least put them into one query
                    let db_pool = app_database
                        .get_pool_by_authority_pubkey(wallet.pubkey().to_string())
                        .await
                        .unwrap();
                    let staker = app_database
                        .get_stake_account_for_staker(db_pool.id, staker_pubkey.to_string(), mint_pubkey.to_string())
                        .await
                        .unwrap();
                    while let Err(_) =
                        app_database.decrease_stakers_rewards(staker.id, amount).await
                    {
                        error!(target: "server_log", "Failed to decrease stakers rewards! Retrying...");
                        tokio::time::sleep(Duration::from_millis(2000)).await;
                    }
                    while let Err(_) = app_database
                        .update_pool_claimed(wallet.pubkey().to_string(), amount)
                        .await
                    {
                        error!(target: "server_log", "Failed to increase pool claimed amount! Retrying...");
                        tokio::time::sleep(Duration::from_millis(2000)).await;
                    }

                    let itxn = InsertTxn {
                        txn_type: "staker-claim".to_string(),
                        signature: sig.to_string(),
                        priority_fee: prio_fee,
                    };
                    while let Err(_) = app_database.add_new_txn(itxn.clone()).await {
                        error!(target: "server_log", "Failed to add new staker-claim txn! Retrying...");
                        tokio::time::sleep(Duration::from_millis(2000)).await;
                    }

                    // TODO: InsertStakerClaim
                    let mut writer = claims_queue.queue.write().await;
                    writer.remove(&(staker_pubkey, Some(mint_pubkey)));
                    drop(writer);

                    info!(target: "server_log", "Stake rewards claim successfully processed!");
                }
                Err(e) => {
                    error!(target: "server_log", "ERROR: {:?}", e);
                }
            }
        } else {
            error!(target: "server_log", "Failed to confirm transaction, will retry on next iteration.");
        }
    } else {
        info!(target: "server_log", "Processing miners claim");
        let miner_pubkey = user_pubkey;
        let ore_mint = get_ore_mint();
        let receiver_pubkey = claim_queue_item.receiver_pubkey;
        let receiver_token_account = get_associated_token_address(&receiver_pubkey, &ore_mint);
        let amount = claim_queue_item.amount;

        let mut claim_amount = amount;

        if let Ok(miner_rewards) = app_database
            .get_miner_rewards(miner_pubkey.to_string())
            .await
        {
            if amount > miner_rewards.balance {
                let mut writer = claims_queue.queue.write().await;
                writer.remove(&(miner_pubkey, None));
                drop(writer);
                return;
            }
        } else {
            let mut writer = claims_queue.queue.write().await;
            writer.remove(&(miner_pubkey, None));
            drop(writer);
            return;
        }

        let prio_fee: u32 = 20_000;

        let mut is_creating_ata = false;
        let mut ixs = Vec::new();
        let prio_fee_ix = ComputeBudgetInstruction::set_compute_unit_price(prio_fee as u64);
        ixs.push(prio_fee_ix);
        if let Ok(response) = rpc_client
            .get_token_account_balance(&receiver_token_account)
            .await
        {
            if let Some(_amount) = response.ui_amount {
                info!(target: "server_log", "miner has valid token account.");
            } else {
                info!(target: "server_log", "will create token account for miner");
                ixs.push(
                    spl_associated_token_account::instruction::create_associated_token_account(
                        &wallet.pubkey(),
                        &receiver_pubkey,
                        &ore_api::consts::MINT_ADDRESS,
                        &spl_token::id(),
                    ),
                )
            }
        } else {
            info!(target: "server_log", "Adding create ata ix for miner claim");
            is_creating_ata = true;
            ixs.push(
                spl_associated_token_account::instruction::create_associated_token_account(
                    &wallet.pubkey(),
                    &receiver_pubkey,
                    &ore_api::consts::MINT_ADDRESS,
                    &spl_token::id(),
                ),
            )
        }

        // 0.02_000_000_000
        if is_creating_ata {
            claim_amount = amount - 2_000_000_000
        }
        let ix =
            crate::ore_utils::get_claim_ix(wallet.pubkey(), receiver_token_account, claim_amount);
        ixs.push(ix);

        if let Ok((hash, _slot)) = rpc_client
            .get_latest_blockhash_with_commitment(rpc_client.commitment())
            .await
        {
            let expired_timer = Instant::now();
            let mut tx = Transaction::new_with_payer(&ixs, Some(&wallet.pubkey()));

            tx.sign(&[&wallet], hash);

            let rpc_config = RpcSendTransactionConfig {
                preflight_commitment: Some(rpc_client.commitment().commitment),
                ..RpcSendTransactionConfig::default()
            };

            let signature;
            let mut send_attempts = 1;
            loop {
                match rpc_client
                    .send_transaction_with_config(&tx, rpc_config)
                    .await 
                {
                        Ok(sig) => {
                            signature = sig;
                            break;
                        },
                        Err(e) => {
                            if send_attempts > 10 {
                                error!(target: "server_log", "Failed to send claim transaction.\nError: {:?}\nRetry limit reached. Removing claim from queue.", e);
                                let mut writer = claims_queue.queue.write().await;
                                writer.remove(&(miner_pubkey, None));
                                drop(writer);
                                return;
                            } else {
                                send_attempts += 1;
                                error!(target: "server_log", "Failed to send claim transaction.\nError: {:?}.\n retrying in 2 seconds...", e);
                                tokio::time::sleep(Duration::from_millis(2000)).await;
                            }
                        }
                }
            }

            let result: Result<Signature, String> = loop {
                if expired_timer.elapsed().as_secs() >= 360 {
                    break Err("Transaction Expired".to_string());
                }
                let results = rpc_client.get_signature_statuses(&[signature]).await;
                if let Ok(response) = results {
                    let statuses = response.value;
                    if let Some(status) = &statuses[0] {
                        if status.confirmation_status()
                            == TransactionConfirmationStatus::Confirmed
                        {
                            if status.err.is_some() {
                                let e_str = format!("Transaction Failed: {:?}", status.err);
                                break Err(e_str);
                            }
                            break Ok(signature);
                        }
                    }
                }
                tokio::time::sleep(Duration::from_millis(2000)).await;
            };

            match result {
                Ok(sig) => {
                    let amount_dec = amount as f64 / 10f64.powf(ORE_TOKEN_DECIMALS as f64);
                    info!(target: "server_log", "Miner {} successfully claimed {}.\nSig: {}", miner_pubkey.to_string(), amount_dec, sig.to_string());

                    // TODO: use transacions, or at least put them into one query
                    let miner = app_database
                        .get_miner_by_pubkey_str(miner_pubkey.to_string())
                        .await
                        .unwrap();
                    let db_pool = app_database
                        .get_pool_by_authority_pubkey(wallet.pubkey().to_string())
                        .await
                        .unwrap();
                    while let Err(_) =
                        app_database.decrease_miner_reward(miner.id, amount).await
                    {
                        error!(target: "server_log", "Failed to decrease stakers rewards! Retrying...");
                        tokio::time::sleep(Duration::from_millis(2000)).await;
                    }
                    while let Err(_) = app_database
                        .update_pool_claimed(wallet.pubkey().to_string(), amount)
                        .await
                    {
                        error!(target: "server_log", "Failed to increase pool claimed amount! Retrying...");
                        tokio::time::sleep(Duration::from_millis(2000)).await;
                    }

                    let itxn = InsertTxn {
                        txn_type: "claim".to_string(),
                        signature: sig.to_string(),
                        priority_fee: prio_fee,
                    };
                    while let Err(_) = app_database.add_new_txn(itxn.clone()).await {
                        error!(target: "server_log", "Failed to increase pool claimed amount! Retrying...");
                        tokio::time::sleep(Duration::from_millis(2000)).await;
                    }

                    let txn_id;
                    loop {
                        if let Ok(ntxn) = app_database.get_txn_by_sig(sig.to_string()).await {
                            txn_id = ntxn.id;
                            break;
                        } else {
                            error!(target: "server_log", "Failed to get tx by sig! Retrying...");
                            tokio::time::sleep(Duration::from_millis(2000)).await;
                        }
                    }

                    let iclaim = InsertClaim {
                        miner_id: miner.id,
                        pool_id: db_pool.id,
                        txn_id,
                        amount,
                    };
                    while let Err(_) = app_database.add_new_claim(iclaim).await {
                        error!(target: "server_log", "Failed add new claim to db! Retrying...");
                        tokio::time::sleep(Duration::from_millis(2000)).await;
                    }

                    let mut writer = claims_queue.queue.write().await;
                    writer.remove(&(miner_pubkey, None));
                    drop(writer);

                    info!(target: "server_log", "Claim successfully processed!");
                }
                Err(e) => {
                    error!(target: "server_log", "ERROR: {:?}", e);
                }
            }
        } else {
            error!(target: "server_log", "Failed to confirm transaction, will retry on next iteration.");
        }
    }
}
