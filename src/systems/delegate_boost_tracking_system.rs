use std::{collections::{HashMap, HashSet}, str::FromStr, sync::Arc, time::Duration};

use futures::StreamExt;
use ore_miner_delegation::{pda::managed_proof_pda, state::DelegatedBoostV2, utils::AccountDeserializeV1};
use solana_account_decoder::UiAccountEncoding;
use solana_client::{nonblocking::{pubsub_client::PubsubClient, rpc_client::RpcClient}, rpc_config::{RpcAccountInfoConfig, RpcProgramAccountsConfig}, rpc_filter::{Memcmp, RpcFilterType}};
use solana_sdk::{commitment_config::{CommitmentConfig, CommitmentLevel}, pubkey::Pubkey};
use tokio::{sync::mpsc::{self, UnboundedReceiver}, time::Instant};

use crate::{app_database::AppDatabase, InsertStakeAccount, UpdateStakeAccount};

const STAKE_ACCOUNT_DB_UPDATE_INTERVAL_SECS: u64 = 2;

pub async fn delegate_boost_tracking_system(
    ws_url: String,
    mining_pubkey: Pubkey,
    rpc_client: Arc<RpcClient>,
    app_database: Arc<AppDatabase>,
) {
    let (updates_sender, update_receiver) = mpsc::unbounded_channel::<(Pubkey, DelegatedBoostV2)>();

    let pool = match app_database.get_pool_by_authority_pubkey(mining_pubkey.to_string()).await {
        Ok(p) => {
            p
        },
        Err(_) => {
            panic!("Failed to get pool data from database");
        }
    };
    tokio::spawn(async move {
        update_database_staked_balances(update_receiver, app_database, rpc_client, pool.id, mining_pubkey).await;
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

async fn update_database_staked_balances(
    mut receiver: UnboundedReceiver<(Pubkey, DelegatedBoostV2)>,
    app_database: Arc<AppDatabase>,
    rpc_client: Arc<RpcClient>,
    pool_id: i32,
    mining_pubkey: Pubkey
) {
    tracing::info!(target: "server_log", "Fetching boost stake accounts from db...");
    let mut db_boost_stake_accounts = HashSet::new(); 
    let mut delegate_boost_accounts = HashMap::new();
    let mut last_id: i32 = 0;
    let managed_proof_authority_pda = managed_proof_pda(mining_pubkey);
    // Load initial on-chain accounts via gpa
    let program_accounts = match rpc_client.get_program_accounts_with_config(
        &ore_miner_delegation::id(),
        RpcProgramAccountsConfig {
            filters: Some(vec![RpcFilterType::DataSize(152), RpcFilterType::Memcmp(Memcmp::new_raw_bytes(16, managed_proof_authority_pda.0.to_bytes().into()))]),
            account_config: RpcAccountInfoConfig {
                encoding: Some(UiAccountEncoding::Base64),
                data_slice: None,
                commitment: Some(CommitmentConfig { commitment: CommitmentLevel::Finalized}),
                min_context_slot: None,
            },
            with_context: None,
        }
    ).await {
            Ok(pa) => {
                pa
            },
            Err(e) => {
                println!("Failed to get program_accounts for gpa. Error: {:?}", e);
                vec![]
            }
    };

    tracing::info!(target: "server_log", "Found {} program accounts", program_accounts.len());

    for program_account in program_accounts.iter() {
        if let Ok(delegate_boost_acct) = DelegatedBoostV2::try_from_bytes(&program_account.1.data) {
            delegate_boost_accounts.insert(program_account.0, *delegate_boost_acct);
        }
    }
    loop {
        tokio::time::sleep(Duration::from_millis(400)).await;
        match app_database.get_stake_accounts(pool_id, last_id).await {
            Ok(d) => {
                if d.len() > 0 {
                    for ac in d.iter() {
                        last_id = ac.id;
                        db_boost_stake_accounts.insert(Pubkey::from_str(&ac.stake_pda).expect("Db should only store a pubkey string here!"));
                    }
                }
                
                if d.len() < 500 {
                    break;
                }
            },
            Err(e) => {
                tracing::error!(target: "server_log", "Failed to get staker accounts for stake account updates.");
                tracing::error!(target: "server_log", "Error: {:?}", e);
            }
        };
    }
    let mut update_stake_accounts_timer = Instant::now();
    let mut gpa_timer = Instant::now();
    loop {
        // Cannot use recv with await as the next batch processing cannot rely on 
        // a new message to trigger db updates in time
        if let Ok(msg) = receiver.try_recv() {
            delegate_boost_accounts.insert(msg.0, msg.1);
            tracing::info!(target: "server_log", "Added new delegate boost account data to hashmap for update.");
        } else {
            tokio::time::sleep(Duration::from_millis(300)).await;
        }

        // Every 5 minutes, perform gpa call to get all on-chain accounts for updates/inserts
        if gpa_timer.elapsed().as_secs() >= 300 {
            tracing::info!(target: "server_log", "Performaing staking GPA rpc call.");
            let program_accounts = match rpc_client.get_program_accounts_with_config(
                &ore_miner_delegation::id(),
                RpcProgramAccountsConfig {
                    filters: Some(vec![RpcFilterType::DataSize(152), RpcFilterType::Memcmp(Memcmp::new_raw_bytes(16, managed_proof_authority_pda.0.to_bytes().into()))]),
                    account_config: RpcAccountInfoConfig {
                        encoding: Some(UiAccountEncoding::Base64),
                        data_slice: None,
                        commitment: Some(CommitmentConfig { commitment: CommitmentLevel::Finalized}),
                        min_context_slot: None,
                    },
                    with_context: None,
                }
            ).await {
                    Ok(pa) => {
                        pa
                    },
                    Err(e) => {
                        println!("Failed to get program_accounts for gpa. Error: {:?}", e);
                        vec![]
                    }
            };

            tracing::info!(target: "server_log", "Found {} program accounts", program_accounts.len());

            for program_account in program_accounts.iter() {
                if let Ok(delegate_boost_acct) = DelegatedBoostV2::try_from_bytes(&program_account.1.data) {
                    delegate_boost_accounts.insert(program_account.0, *delegate_boost_acct);
                }
            }

            // Refresh gpa timer
            gpa_timer = Instant::now();
        }

        if delegate_boost_accounts.len() > 0 {
            tracing::info!(target: "server_log", "Processing {} delegate boost accounts updates...", delegate_boost_accounts.len());
            if update_stake_accounts_timer.elapsed().as_secs() >= STAKE_ACCOUNT_DB_UPDATE_INTERVAL_SECS {
                // Update database staked balances
                let mut ba_inserts = Vec::new();
                let mut ba_updates = Vec::new();
                for ba in delegate_boost_accounts {
                    if let Some(_) = db_boost_stake_accounts.get(&ba.0) {
                        ba_updates.push(UpdateStakeAccount {
                            stake_pda: ba.0.to_string(),
                            staked_balance: ba.1.amount,
                        });
                    } else {
                        ba_inserts.push(InsertStakeAccount {
                            pool_id,
                            mint_pubkey: ba.1.mint.to_string(),
                            staker_pubkey: ba.1.authority.to_string(),
                            stake_pda: ba.0.to_string(),
                            staked_balance: ba.1.amount,
                        })
                    }
                }

                let instant = Instant::now();
                let batch_size = 200;
                tracing::info!(target: "server_log", "Updating stake accounts.");
                if ba_updates.len() > 0 {
                    for (i, batch) in ba_updates.chunks(batch_size).enumerate() {
                        let instant = Instant::now();
                        tracing::info!(target: "server_log", "Updating batch {}", i);
                        while let Err(_) = app_database.update_stake_accounts_staked_balance(batch.to_vec()).await {
                            tracing::info!(target: "server_log", "Failed to update stake_account staked_balance in db. Retrying...");
                            tokio::time::sleep(Duration::from_millis(500)).await;
                        }
                        tracing::info!(target: "server_log", "Updated staked_account batch {} in {}ms", i, instant.elapsed().as_millis());
                        tokio::time::sleep(Duration::from_millis(200)).await;
                    }
                    tracing::info!(target: "server_log", "Successfully updated stake_accounts");
                }
                tracing::info!(target: "server_log", "Updated stake_accounts in {}ms", instant.elapsed().as_millis());

                let batch_size = 500;
                if ba_inserts.len() > 0 {
                    tracing::info!(target: "server_log", "Inserting {} new generated stake accounts into db.", ba_inserts.len());
                    for (i, batch) in ba_inserts.chunks(batch_size).enumerate() {
                        tracing::info!(target: "server_log", "Batch {}, Size: {}", i, batch_size);
                        while let Err(_) =
                            app_database.add_new_stake_accounts_batch(batch.to_vec()).await
                        {
                            tracing::info!(target: "server_log", "Failed to add new stake accounts batch to db. Retrying...");
                            tokio::time::sleep(Duration::from_millis(500)).await;
                        }

                        for item in batch {
                            db_boost_stake_accounts.insert(Pubkey::from_str(&item.stake_pda).expect("Db value should always be valid pubkey"));
                        }
                        tokio::time::sleep(Duration::from_millis(200)).await;
                    }
                    tracing::info!(target: "server_log", "Successfully added new stake accounts batch");
                }


                // reset timer
                update_stake_accounts_timer = Instant::now();

                // reset hashmap
                delegate_boost_accounts = HashMap::new();
            }
        }
    }
}
