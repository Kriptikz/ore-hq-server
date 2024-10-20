use std::{collections::HashMap, str::FromStr, sync::Arc, time::Duration};

use ore_api::state::proof_pda;
use ore_miner_delegation::{pda::{delegated_boost_pda, managed_proof_pda}, state::DelegatedBoost, utils::AccountDeserialize};
use solana_account_decoder::UiAccountEncoding;
use solana_client::{nonblocking::rpc_client::RpcClient, rpc_config::{RpcAccountInfoConfig, RpcProgramAccountsConfig}, rpc_filter::{Memcmp, RpcFilterType}};
use solana_sdk::{
    commitment_config::{CommitmentConfig, CommitmentLevel}, pubkey::Pubkey, signature::read_keypair_file, signer::Signer
};
use tokio::time::Instant;

use crate::{app_database::AppDatabase, InsertStakeAccount, UpdateStakeAccount};

pub async fn gen_stake_accounts() -> Result<(), Box<dyn std::error::Error>> {
    println!("Generating stake accounts for miners");

    // load envs
    let wallet_path_str = std::env::var("WALLET_PATH").expect("WALLET_PATH must be set.");
    let database_url = std::env::var("DATABASE_URL").expect("DATABASE_URL must be set.");

    let wallet_path = std::path::Path::new(&wallet_path_str);

    if !wallet_path.exists() {
        println!("Failed to load wallet at: {}", wallet_path_str);
        return Err("Failed to find wallet path.".into());
    }

    let wallet = read_keypair_file(wallet_path)
        .expect("Failed to load keypair from file: {wallet_path_str}");
    println!("loaded wallet {}", wallet.pubkey().to_string());

    let app_database = Arc::new(AppDatabase::new(database_url));

    let pool = match app_database.get_pool_by_authority_pubkey(wallet.pubkey().to_string()).await {
        Ok(p) => {
            p
        },
        Err(_) => {
            println!("Failed to get pool data from database");
            return Ok(());
        }
    };

    println!("Fetching all miner accounts");
    let mut miners = vec![]; 
    let mut last_id: i32 = 0;
    loop {
        tokio::time::sleep(Duration::from_millis(400)).await;
        match app_database.get_miner_accounts(last_id).await {
            Ok(d) => {
                if d.len() > 0 {
                    for ac in d.iter() {
                        last_id = ac.id;
                        miners.push(ac.clone());
                    }
                }
                
                if d.len() < 500 {
                    break;
                }
            },
            Err(e) => {
                println!("Failed to get miner accounts for stake account generation.");
                println!("Error: {:?}", e);
            }
        };
    }

    println!("Found {} miners", miners.len());

    let boost_mints = vec![
        Pubkey::from_str("oreoU2P8bN6jkk3jbaiVxYnG1dCXcYxwhwyK9jSybcp").unwrap(),
        Pubkey::from_str("DrSS5RM7zUd9qjUEdDaf31vnDUSbCrMto6mjqTrHFifN").unwrap(),
        Pubkey::from_str("meUwDp23AaxhiNKaQCyJ2EAF2T4oe1gSkEkGXSRVdZb").unwrap()
    ];

    let mut new_stake_accounts = vec![];

    println!("Generating miners new stake account insertion datas");
    for miner in miners {
        for boost_mint in &boost_mints {
            let delegated_boost_pda = delegated_boost_pda(wallet.pubkey(), Pubkey::from_str(&miner.pubkey).unwrap(), *boost_mint);
            let new_stake_account = InsertStakeAccount {
                pool_id: pool.id,
                mint_pubkey: boost_mint.to_string(),
                staker_pubkey: miner.pubkey.clone(),
                stake_pda: delegated_boost_pda.0.to_string(),
            };

            new_stake_accounts.push(new_stake_account);
        }
    }

    let batch_size = 500;
    if new_stake_accounts.len() > 0 {
        println!("Inserting {} newly generated stake accounts into db.", new_stake_accounts.len());
        for (i, batch) in new_stake_accounts.chunks(batch_size).enumerate() {
            println!("Batch {}, Size: {}", i, new_stake_accounts.len());
            while let Err(_) =
                app_database.add_new_stake_accounts_batch(batch.to_vec()).await
            {
                println!("Failed to add new stake accounts batch to db. Retrying...");
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
        println!("Successfully added new stake accounts batch");
    }

    Ok(())
}

pub async fn update_stake_accounts() -> Result<(), Box<dyn std::error::Error>> {
    println!("Updating stake accounts from on-chain data");

    // load envs
    let wallet_path_str = std::env::var("WALLET_PATH").expect("WALLET_PATH must be set.");
    let database_url = std::env::var("DATABASE_URL").expect("DATABASE_URL must be set.");
    let rpc_url = std::env::var("RPC_URL").expect("RPC_URL must be set.");

    let wallet_path = std::path::Path::new(&wallet_path_str);

    if !wallet_path.exists() {
        println!("Failed to load wallet at: {}", wallet_path_str);
        return Err("Failed to find wallet path.".into());
    }

    let wallet = read_keypair_file(wallet_path)
        .expect("Failed to load keypair from file: {wallet_path_str}");
    println!("loaded wallet {}", wallet.pubkey().to_string());

    let app_database = Arc::new(AppDatabase::new(database_url));
    let rpc_client = RpcClient::new_with_commitment(rpc_url, CommitmentConfig::confirmed());

    // let pool = match app_database.get_pool_by_authority_pubkey(wallet.pubkey().to_string()).await {
    //     Ok(p) => {
    //         p
    //     },
    //     Err(_) => {
    //         println!("Failed to get pool data from database");
    //         return Ok(());
    //     }
    // };

    // let boost_mints = vec![
    //     Pubkey::from_str("oreoU2P8bN6jkk3jbaiVxYnG1dCXcYxwhwyK9jSybcp").unwrap(),
    //     Pubkey::from_str("DrSS5RM7zUd9qjUEdDaf31vnDUSbCrMto6mjqTrHFifN").unwrap(),
    //     Pubkey::from_str("meUwDp23AaxhiNKaQCyJ2EAF2T4oe1gSkEkGXSRVdZb").unwrap()
    // ];

    // println!("Fetching ore stake accounts from db...");
    // let mut ore_stake_accounts = vec![]; 
    // let mut last_id: i32 = 0;
    // loop {
    //     tokio::time::sleep(Duration::from_millis(400)).await;
    //     match app_database.get_staker_accounts_for_mint(pool.id, boost_mints[0].to_string(), last_id, 0).await {
    //         Ok(d) => {
    //             if d.len() > 0 {
    //                 for ac in d.iter() {
    //                     last_id = ac.id;
    //                     ore_stake_accounts.push(ac.clone());
    //                 }
    //             }
                
    //             if d.len() < 500 {
    //                 break;
    //             }
    //         },
    //         Err(e) => {
    //             println!("Failed to get staker accounts for stake account updates.");
    //             println!("Error: {:?}", e);
    //         }
    //     };
    // }

    // println!("Got {} ore stake accounts.", ore_stake_accounts.len());

    // println!("Fetching ore-sol stake accounts from db...");
    // let mut ore_sol_stake_accounts = vec![]; 
    // let mut last_id: i32 = 0;
    // loop {
    //     tokio::time::sleep(Duration::from_millis(400)).await;
    //     match app_database.get_staker_accounts_for_mint(pool.id, boost_mints[1].to_string(), last_id, 0).await {
    //         Ok(d) => {
    //             if d.len() > 0 {
    //                 for ac in d.iter() {
    //                     last_id = ac.id;
    //                     ore_sol_stake_accounts.push(ac.clone());
    //                 }
    //             }
                
    //             if d.len() < 500 {
    //                 break;
    //             }
    //         },
    //         Err(e) => {
    //             println!("Failed to get staker accounts for stake account updates.");
    //             println!("Error: {:?}", e);
    //         }
    //     };
    // }
    // println!("Got {} ore-sol stake accounts.", ore_sol_stake_accounts.len());

    // println!("Fetching ore-isc stake accounts from db...");
    // let mut ore_isc_stake_accounts = vec![]; 
    // let mut last_id: i32 = 0;
    // loop {
    //     tokio::time::sleep(Duration::from_millis(400)).await;
    //     match app_database.get_staker_accounts_for_mint(pool.id, boost_mints[2].to_string(), last_id, 0).await {
    //         Ok(d) => {
    //             if d.len() > 0 {
    //                 for ac in d.iter() {
    //                     last_id = ac.id;
    //                     ore_isc_stake_accounts.push(ac.clone());
    //                 }
    //             }
                
    //             if d.len() < 500 {
    //                 break;
    //             }
    //         },
    //         Err(e) => {
    //             println!("Failed to get staker accounts for stake account updates.");
    //             println!("Error: {:?}", e);
    //         }
    //     };
    // }
    // println!("Got {} ore-isc stake accounts.", ore_isc_stake_accounts.len());


    let managed_proof_authority_pda = managed_proof_pda(wallet.pubkey());
    let program_accounts = match rpc_client.get_program_accounts_with_config(
        &ore_miner_delegation::id(),
        RpcProgramAccountsConfig {
            filters: Some(vec![RpcFilterType::DataSize(56), RpcFilterType::Memcmp(Memcmp::new_raw_bytes(16, managed_proof_authority_pda.0.to_bytes().into()))]),
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
                println!("Failed to get program_accounts. Error: {:?}", e);
                return Ok(());
            }

    };

    println!("Found {} program accounts", program_accounts.len());

    let mut delegated_boosts = HashMap::new();
    for program_account in program_accounts.iter() {
        if let Ok(delegate_boost_acct) = DelegatedBoost::try_from_bytes(&program_account.1.data) {
            delegated_boosts.insert(program_account.0, delegate_boost_acct);
        }
    }

    println!("Found {} delegated_boosts.", delegated_boosts.len());
    let mut updated_stake_accounts = vec![];


    for delegate_boost in delegated_boosts.iter() {
        let updated_stake_account = UpdateStakeAccount {
            stake_pda: delegate_boost.0.to_string(),
            staked_balance: delegate_boost.1.amount,
        };

        updated_stake_accounts.push(updated_stake_account);
    }

    let instant = Instant::now();
    let batch_size = 200;
    println!("Updating stake accounts.");
    if updated_stake_accounts.len() > 0 {
        for (i, batch) in updated_stake_accounts.chunks(batch_size).enumerate() {
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

    Ok(())
}



