use std::{sync::Arc, time::Duration};

use solana_client::{nonblocking::rpc_client::RpcClient, rpc_config::RpcSendTransactionConfig};
use solana_sdk::{compute_budget::ComputeBudgetInstruction, native_token::lamports_to_sol, signature::{Keypair, Signature}, signer::Signer, transaction::Transaction};
use solana_transaction_status::TransactionConfirmationStatus;
use spl_associated_token_account::get_associated_token_address;
use tokio::time::Instant;
use tracing::{error, info};

use crate::{app_database::AppDatabase, ore_utils::{get_ore_mint, ORE_TOKEN_DECIMALS}, ClaimsQueue, InsertClaim, InsertTxn};


pub async fn claim_system(claims_queue: Arc<ClaimsQueue>, rpc_client: Arc<RpcClient>, wallet: Arc<Keypair>, app_database: Arc<AppDatabase>) {
    loop {
        let mut claim = None;
        let reader = claims_queue.queue.read().await;
        let item = reader.iter().next();
        if let Some(item) = item {
            claim = Some((item.0.clone(), item.1.clone()));
        }
        drop(reader);

        if let Some((user_pubkey, amount)) = claim {
            info!("Processing claim");
            let ore_mint = get_ore_mint();
            let miner_token_account = get_associated_token_address(&user_pubkey, &ore_mint);

            let prio_fee: u32 = 20_000;

            let mut ixs = Vec::new();
            let prio_fee_ix = ComputeBudgetInstruction::set_compute_unit_price(prio_fee as u64);
            ixs.push(prio_fee_ix);
            if let Ok(response) = rpc_client
                .get_token_account_balance(&miner_token_account)
                .await
            {
                if let Some(_amount) = response.ui_amount {
                    info!("miner has valid token account.");
                } else {
                    info!("will create token account for miner");
                    ixs.push(
                        spl_associated_token_account::instruction::create_associated_token_account(
                            &wallet.pubkey(),
                            &user_pubkey,
                            &ore_api::consts::MINT_ADDRESS,
                            &spl_token::id(),
                        ),
                    )
                }
            } else {
                info!("Adding create ata ix for miner claim");
                ixs.push(
                    spl_associated_token_account::instruction::create_associated_token_account(
                        &wallet.pubkey(),
                        &user_pubkey,
                        &ore_api::consts::MINT_ADDRESS,
                        &spl_token::id(),
                    ),
                )
            }

            let ix = crate::ore_utils::get_claim_ix(wallet.pubkey(), miner_token_account, amount);
            ixs.push(ix);

            if let Ok((hash, _slot)) = rpc_client
                .get_latest_blockhash_with_commitment(rpc_client.commitment())
                .await
            {
                let expired_timer = Instant::now();
                let mut tx = Transaction::new_with_payer(&ixs, Some(&wallet.pubkey()));

                tx.sign(&[&wallet], hash);

                let rpc_config =  RpcSendTransactionConfig {
                    preflight_commitment: Some(rpc_client.commitment().commitment),
                    ..RpcSendTransactionConfig::default()
                };

                let signature;
                loop {
                    if let Ok(sig) = rpc_client.send_transaction_with_config(&tx, rpc_config).await {
                        signature = sig;
                        break;
                    } else {
                        error!("Failed to send claim transaction. retrying in 2 seconds...");
                        tokio::time::sleep(Duration::from_millis(2000)).await;
                    }
                }

                let result: Result<Signature, String> = loop {
                    if expired_timer.elapsed().as_secs() >= 200 {
                        break Err("Transaction Expired".to_string());
                    }
                    let results = rpc_client.get_signature_statuses(&[signature]).await;
                    if let Ok(response) = results {
                        let statuses = response.value;
                        if let Some(status) = &statuses[0] {
                            if status.confirmation_status() == TransactionConfirmationStatus::Confirmed {
                                if status.err.is_some() {
                                    let e_str = format!("Transaction Failed: {:?}", status.err);
                                    break Err(e_str);
                                }
                                break Ok(signature);
                            }
                        }
                    }
                    tokio::time::sleep(Duration::from_millis(500)).await;
                };

                match result {
                    Ok(sig) => {
                        let amount_dec = amount as f64 / 10f64.powf(ORE_TOKEN_DECIMALS as f64);
                        info!("Miner {} successfully claimed {}.\nSig: {}", user_pubkey.to_string(), amount_dec, sig.to_string());

                        // TODO: use transacions, or at least put them into one query
                        let miner = app_database
                            .get_miner_by_pubkey_str(user_pubkey.to_string())
                            .await
                            .unwrap();
                        let db_pool = app_database
                            .get_pool_by_authority_pubkey(wallet.pubkey().to_string())
                            .await
                            .unwrap();
                        while let Err(_) = app_database
                            .decrease_miner_reward(miner.id, amount)
                            .await 
                        {
                            error!("Failed to decrease miner rewards! Retrying...");
                            tokio::time::sleep(Duration::from_millis(2000)).await;
                        }
                        while let Err(_) = app_database
                            .update_pool_claimed(wallet.pubkey().to_string(), amount)
                            .await
                        {
                            error!("Failed to increase pool claimed amount! Retrying...");
                            tokio::time::sleep(Duration::from_millis(2000)).await;
                        }

                        let itxn = InsertTxn {
                            txn_type: "claim".to_string(),
                            signature: sig.to_string(),
                            priority_fee: prio_fee,
                        };
                        while let Err(_) = app_database.add_new_txn(itxn.clone()).await {
                            error!("Failed to increase pool claimed amount! Retrying...");
                            tokio::time::sleep(Duration::from_millis(2000)).await;
                        }

                        let txn_id;
                        loop {
                            if let Ok(ntxn) = app_database.get_txn_by_sig(sig.to_string()).await {
                                txn_id = ntxn.id;
                                break;
                            } else {
                                error!("Failed to get tx by sig! Retrying...");
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
                            error!("Failed add new claim to db! Retrying...");
                            tokio::time::sleep(Duration::from_millis(2000)).await;
                        }

                        let mut writer = claims_queue.queue.write().await;
                        writer.remove(&user_pubkey);
                        drop(writer);

                        info!("Claim successfully processed!");

                    }
                    Err(e) => {
                        error!("ERROR: {:?}", e);
                    }
                }
            } else {
                error!("Failed to confirm transaction, will retry on next iteration.");
            }

        }

        tokio::time::sleep(Duration::from_secs(10)).await;
    }
}
