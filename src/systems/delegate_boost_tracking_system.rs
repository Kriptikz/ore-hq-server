use std::{collections::HashMap, str::FromStr, sync::Arc, time::Duration};

use futures::StreamExt;
use ore_miner_delegation::{pda::managed_proof_pda, state::DelegatedBoostV2, utils::AccountDeserialize};
use solana_account_decoder::UiAccountEncoding;
use solana_client::{nonblocking::pubsub_client::PubsubClient, rpc_config::{RpcAccountInfoConfig, RpcProgramAccountsConfig}, rpc_filter::{Memcmp, RpcFilterType}};
use solana_sdk::{commitment_config::CommitmentConfig, pubkey::Pubkey};
use tokio::{sync::mpsc::{self, UnboundedReceiver}, time::Instant};

use crate::{app_database::AppDatabase, UpdateStakeAccount};

const STAKE_ACCOUNT_DB_UPDATE_INTERVAL_SECS: u64 = 2;

pub async fn delegate_boost_tracking_system(
    ws_url: String,
    mining_pubkey: Pubkey,
    app_database: Arc<AppDatabase>,
) {
    let (updates_sender, update_receiver) = mpsc::unbounded_channel::<(Pubkey, DelegatedBoostV2)>();

    tokio::spawn(async move {
        update_database_staked_balances(update_receiver, app_database).await;
    });

    loop {
        tracing::info!(target: "server_log", "Establishing rpc websocket connection for delegate boost tracking...");
        let mut ps_client = PubsubClient::new(&ws_url).await;
        let mut attempts = 0;

        while ps_client.is_err() && attempts < 3 {
            tracing::error!(target: "server_log", "Failed to connect to websocket, retrying...");
            ps_client = PubsubClient::new(&ws_url).await;
            tokio::time::sleep(Duration::from_millis(1000)).await;
            attempts += 1;
        }
        tracing::info!(target: "server_log", "RPC WS connection established!");

        if let Ok(ps_client) = ps_client {
            let ps_client = Arc::new(ps_client);
            let managed_proof_authority_pda = managed_proof_pda(mining_pubkey);
            let pubsub = ps_client
                .program_subscribe(
                    &ore_miner_delegation::id(),
                    Some(RpcProgramAccountsConfig {
                        filters: Some(vec![RpcFilterType::DataSize(152), RpcFilterType::Memcmp(Memcmp::new_raw_bytes(16, managed_proof_authority_pda.0.to_bytes().into()))]),
                        account_config: RpcAccountInfoConfig {
                            encoding: Some(UiAccountEncoding::Base64),
                            data_slice: None,
                            commitment: Some(CommitmentConfig::confirmed()),
                            min_context_slot: None,
                        },
                        with_context: None,
                    })
                )
                .await;

            tracing::info!(target: "server_log", "Tracking pool boost stake updates with websocket");
            if let Ok((mut account_sub_notifications, _account_unsub)) = pubsub {
                let sender = updates_sender.clone();
                while let Some(response) = account_sub_notifications.next().await {
                    let data = response.value.account.data.decode();
                    if let Some(data_bytes) = data {
                        if let Ok(new_delegate_boost_data) = DelegatedBoostV2::try_from_bytes(&data_bytes) {
                            tracing::info!(target: "server_log", "Got new delegated boost data");
                            let _ = sender.send((Pubkey::from_str(&response.value.pubkey).unwrap(), *new_delegate_boost_data));
                        }
                    }
                }
            }
        }
    }
}

async fn update_database_staked_balances(mut receiver: UnboundedReceiver<(Pubkey, DelegatedBoostV2)>, app_database: Arc<AppDatabase>) {
    let mut delegate_boost_accounts = HashMap::new();
    let mut update_stake_accounts_timer = Instant::now();
    loop {
        // Cannot use recv with await as the next batch processing cannot rely on 
        // a new message to trigger db updates in time
        if let Ok(msg) = receiver.try_recv() {
            delegate_boost_accounts.insert(msg.0, msg.1);
            tracing::info!(target: "server_log", "Added new delegate boost account data to hashmap for update.");
        } else {
            tokio::time::sleep(Duration::from_millis(300)).await;
        }

        if delegate_boost_accounts.len() > 0 {
            if update_stake_accounts_timer.elapsed().as_secs() >= STAKE_ACCOUNT_DB_UPDATE_INTERVAL_SECS {
                // Update database staked balances
                let mut ba_updates = Vec::new();
                for ba in delegate_boost_accounts {
                    ba_updates.push(UpdateStakeAccount {
                        stake_pda: ba.0.to_string(),
                        staked_balance: ba.1.amount,
                    });
                }

                let instant = Instant::now();
                let batch_size = 200;
                println!("Updating stake accounts.");
                if ba_updates.len() > 0 {
                    for (i, batch) in ba_updates.chunks(batch_size).enumerate() {
                        let instant = Instant::now();
                        println!("Updating batch {}", i);
                        while let Err(_) = app_database.update_stake_accounts_staked_balance(batch.to_vec()).await {
                            println!("Failed to update stake_account staked_balance in db. Retrying...");
                            tokio::time::sleep(Duration::from_millis(500)).await;
                        }
                        println!("Updated staked_account batch {} in {}ms", i, instant.elapsed().as_millis());
                        tokio::time::sleep(Duration::from_millis(200)).await;
                    }
                    println!("Successfully updated stake_accounts");
                }
                println!("Updated stake_accounts in {}ms", instant.elapsed().as_millis());


                // reset timer
                update_stake_accounts_timer = Instant::now();

                // reset hashmap
                delegate_boost_accounts = HashMap::new();
            }
        }
    }
}
