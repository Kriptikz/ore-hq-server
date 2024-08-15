use std::{
    collections::{HashMap, HashSet},
    net::SocketAddr,
    ops::{ControlFlow, Div, Range},
    path::Path,
    str::FromStr,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use self::models::*;
use ::ore_utils::AccountDeserialize;
use app_database::{AppDatabase, AppDatabaseError};
use axum::{
    extract::{
        ws::{Message, WebSocket},
        ConnectInfo, Query, State, WebSocketUpgrade,
    },
    http::{Response, StatusCode},
    response::IntoResponse,
    routing::{get, post},
    Extension, Router,
};
use axum_extra::{headers::authorization::Basic, TypedHeader};
use base64::{prelude::BASE64_STANDARD, Engine};
use clap::Parser;
use drillx::Solution;
use futures::{stream::SplitSink, SinkExt, StreamExt};
use ore_api::{consts::BUS_COUNT, state::Proof};
use ore_utils::{
    get_auth_ix, get_cutoff, get_mine_ix, get_ore_mint, get_proof,
    get_proof_and_config_with_busses, get_register_ix, get_reset_ix, proof_pubkey,
    ORE_TOKEN_DECIMALS,
};
use rand::Rng;
use serde::Deserialize;
use solana_account_decoder::UiAccountEncoding;
use solana_client::{
    nonblocking::{pubsub_client::PubsubClient, rpc_client::RpcClient},
    rpc_config::RpcAccountInfoConfig,
};
use solana_sdk::{
    commitment_config::CommitmentConfig,
    compute_budget::ComputeBudgetInstruction,
    native_token::LAMPORTS_PER_SOL,
    pubkey::Pubkey,
    signature::{read_keypair_file, Keypair, Signature},
    signer::Signer,
    system_instruction,
    transaction::Transaction,
};
use spl_associated_token_account::get_associated_token_address;
use tokio::{
    io::AsyncReadExt,
    sync::{
        mpsc::{UnboundedReceiver, UnboundedSender},
        Mutex, RwLock,
    },
};
use tower_http::trace::{DefaultMakeSpan, TraceLayer};
use tracing::{debug, error, info};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

mod app_database;
mod models;
mod schema;

const MIN_DIFF: u32 = 8;
const MIN_HASHPOWER: u64 = 5;

struct AppState {
    sockets: HashMap<SocketAddr, (Pubkey, Mutex<SplitSink<WebSocket, Message>>)>,

pub struct MessageInternalAllClients {
    text: String,
}

pub struct MessageInternalMineSuccess {
    difficulty: u32,
    total_balance: f64,
    rewards: u64,
    challenge_id: i32,
    total_hashpower: u64,
    submissions: HashMap<Pubkey, (i32, u32, u64)>,
}

#[derive(Debug)]
pub enum ClientMessage {
    Ready(SocketAddr),
    Mining(SocketAddr),
    BestSolution(SocketAddr, Solution, Pubkey),
}

pub struct EpochHashes {
    best_hash: BestHash,
    submissions: HashMap<Pubkey, (i32, u32, u64)>,
}

pub struct BestHash {
    solution: Option<Solution>,
    difficulty: u32,
}

pub struct Config {
    password: String,
    whitelist: Option<HashSet<Pubkey>>,
    pool_id: i32,
}

mod ore_utils;

#[derive(Parser, Debug)]
#[command(version, author, about, long_about = None)]
struct Args {
    #[arg(
        long,
        value_name = "priority fee",
        help = "Number of microlamports to pay as priority fee per transaction",
        default_value = "0",
        global = true
    )]
    priority_fee: u64,
    #[arg(
        long,
        value_name = "whitelist",
        help = "Path to whitelist of allowed miners",
        default_value = None,
        global = true
    )]
    whitelist: Option<String>,
    #[arg(
        long,
        value_name = "signup cost",
        help = "Amount of sol users must send to sign up for the pool",
        default_value = "0",
        global = true
    )]
    signup_cost: u64,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    dotenv::dotenv().ok();
    let args = Args::parse();
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "ore_hq_server=debug,tower_http=debug".into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    // load envs
    let wallet_path_str = std::env::var("WALLET_PATH").expect("WALLET_PATH must be set.");
    let rpc_url = std::env::var("RPC_URL").expect("RPC_URL must be set.");
    let rpc_ws_url = std::env::var("RPC_WS_URL").expect("RPC_WS_URL must be set.");
    let password = std::env::var("PASSWORD").expect("PASSWORD must be set.");
    let database_url = std::env::var("DATABASE_URL").expect("DATABASE_URL must be set.");

    let app_database = Arc::new(AppDatabase::new(database_url));

    let whitelist = if let Some(whitelist) = args.whitelist {
        let file = Path::new(&whitelist);
        if file.exists() {
            // load file
            let mut pubkeys = HashSet::new();
            if let Ok(mut file) = tokio::fs::File::open(file).await {
                let mut file_contents = String::new();
                file.read_to_string(&mut file_contents)
                    .await
                    .ok()
                    .expect("Failed to read whitelist file");
                drop(file);

                for (i, line) in file_contents.lines().enumerate() {
                    if let Ok(pubkey) = Pubkey::from_str(line) {
                        pubkeys.insert(pubkey);
                    } else {
                        let err = format!(
                            "Failed to create pubkey from line {} with value: {}",
                            i, line
                        );
                        error!(err);
                    }
                }
            } else {
                return Err("Failed to open whitelist file".into());
            }
            Some(pubkeys)
        } else {
            return Err("Whitelist at specified file path doesn't exist".into());
        }
    } else {
        None
    };

    let priority_fee = Arc::new(Mutex::new(args.priority_fee));

    // load wallet
    let wallet_path = Path::new(&wallet_path_str);

    if !wallet_path.exists() {
        tracing::error!("Failed to load wallet at: {}", wallet_path_str);
        return Err("Failed to find wallet path.".into());
    }

    let wallet = read_keypair_file(wallet_path)
        .expect("Failed to load keypair from file: {wallet_path_str}");
    println!("loaded wallet {}", wallet.pubkey().to_string());

    println!("establishing rpc connection...");
    let rpc_client = RpcClient::new_with_commitment(rpc_url, CommitmentConfig::confirmed());

    println!("loading sol balance...");
    let balance = if let Ok(balance) = rpc_client.get_balance(&wallet.pubkey()).await {
        balance
    } else {
        return Err("Failed to load balance".into());
    };

    println!("Balance: {:.2}", balance as f64 / LAMPORTS_PER_SOL as f64);

    if balance < 1_000_000 {
        return Err("Sol balance is too low!".into());
    }

    let proof = if let Ok(loaded_proof) = get_proof(&rpc_client, wallet.pubkey()).await {
        loaded_proof
    } else {
        println!("Failed to load proof.");
        println!("Creating proof account...");

        let ix = get_register_ix(wallet.pubkey());

        if let Ok((hash, _slot)) = rpc_client
            .get_latest_blockhash_with_commitment(rpc_client.commitment())
            .await
        {
            let mut tx = Transaction::new_with_payer(&[ix], Some(&wallet.pubkey()));

            tx.sign(&[&wallet], hash);

            let result = rpc_client
                .send_and_confirm_transaction_with_spinner_and_commitment(
                    &tx,
                    rpc_client.commitment(),
                )
                .await;

            if let Ok(sig) = result {
                println!("Sig: {}", sig.to_string());
            } else {
                return Err("Failed to create proof account".into());
            }
        }
        let proof = if let Ok(loaded_proof) = get_proof(&rpc_client, wallet.pubkey()).await {
            loaded_proof
        } else {
            return Err("Failed to get newly created proof".into());
        };
        proof
    };

    println!("Validating pool exists in db");
    let db_pool = app_database
        .get_pool_by_authority_pubkey(wallet.pubkey().to_string())
        .await;

    match db_pool {
        Ok(_) => {}
        Err(AppDatabaseError::FailedToGetConnectionFromPool) => {
            panic!("Failed to get database pool connection");
        }
        Err(_) => {
            println!("Pool missing from database. Inserting...");
            let proof_pubkey = proof_pubkey(wallet.pubkey());
            let result = app_database
                .add_new_pool(wallet.pubkey().to_string(), proof_pubkey.to_string())
                .await;

            if result.is_err() {
                panic!("Failed to create pool in database");
            }
        }
    }

    let db_pool = app_database
        .get_pool_by_authority_pubkey(wallet.pubkey().to_string())
        .await
        .unwrap();

    println!("Validating current challenge for pool exists in db");
    let result = app_database
        .get_challenge_by_challenge(proof.challenge.to_vec())
        .await;

    match result {
        Ok(_) => {}
        Err(AppDatabaseError::FailedToGetConnectionFromPool) => {
            panic!("Failed to get database pool connection");
        }
        Err(_) => {
            println!("Challenge missing from database. Inserting...");
            let new_challenge = models::InsertChallenge {
                pool_id: db_pool.id,
                challenge: proof.challenge.to_vec(),
                rewards_earned: None,
            };
            let result = app_database.add_new_challenge(new_challenge).await;

            if result.is_err() {
                panic!("Failed to create pool in database");
            }
        }
    }

    let config = Arc::new(Config {
        password,
        whitelist,
        pool_id: db_pool.id,
    });

    let epoch_hashes = Arc::new(RwLock::new(EpochHashes {
        best_hash: BestHash {
            solution: None,
            difficulty: 0,
        },
        submissions: HashMap::new(),
    }));

    let wallet_extension = Arc::new(wallet);
    let proof_ext = Arc::new(Mutex::new(proof));
    let nonce_ext = Arc::new(Mutex::new(0u64));

    let client_nonce_ranges = Arc::new(RwLock::new(HashMap::new()));

    let shared_state = Arc::new(RwLock::new(AppState {
        sockets: HashMap::new(),
    }));
    let ready_clients = Arc::new(Mutex::new(HashSet::new()));

    let app_wallet = wallet_extension.clone();
    let app_proof = proof_ext.clone();
    // Establish webocket connection for tracking pool proof changes.
    tokio::spawn(async move {
        proof_tracking_system(rpc_ws_url, app_wallet, app_proof).await;
    });

    let (client_message_sender, client_message_receiver) =
        tokio::sync::mpsc::unbounded_channel::<ClientMessage>();

    // Handle client messages
    let app_shared_state = shared_state.clone();
    let app_ready_clients = ready_clients.clone();
    let app_proof = proof_ext.clone();
    let app_epoch_hashes = epoch_hashes.clone();
    let app_app_database = app_database.clone();
    let app_client_nonce_ranges = client_nonce_ranges.clone();
    tokio::spawn(async move {
        client_message_handler_system(
            client_message_receiver,
            &app_shared_state,
            app_app_database,
            app_ready_clients,
            app_proof,
            app_epoch_hashes,
            app_client_nonce_ranges,
        )
        .await;
    });

    // Handle ready clients
    let app_shared_state = shared_state.clone();
    let app_proof = proof_ext.clone();
    let app_epoch_hashes = epoch_hashes.clone();
    let app_nonce = nonce_ext.clone();
    let app_client_nonce_ranges = client_nonce_ranges.clone();
    tokio::spawn(async move {
        loop {
            let mut clients = Vec::new();
            {
                let ready_clients_lock = ready_clients.lock().await;
                for ready_client in ready_clients_lock.iter() {
                    clients.push(ready_client.clone());
                }
            };

            let proof = { app_proof.lock().await.clone() };

            let cutoff = get_cutoff(proof, 5);
            let mut should_mine = true;
            let cutoff = if cutoff <= 0 {
                let solution = app_epoch_hashes.read().await.best_hash.solution;
                if solution.is_some() {
                    should_mine = false;
                }
                0
            } else {
                cutoff
            };

            if should_mine {
                let challenge = proof.challenge;

                for client in clients {
                    let nonce_range = {
                        let mut nonce = app_nonce.lock().await;
                        let start = *nonce;
                        // max hashes possible in 60s for a single client
                        *nonce += 2_000_000;
                        let end = *nonce;
                        start..end
                    };
                    {
                        let shared_state = app_shared_state.read().await;
                        // message type is 8 bytes = 1 u8
                        // challenge is 256 bytes = 32 u8
                        // cutoff is 64 bytes = 8 u8
                        // nonce_range is 128 bytes, start is 64 bytes, end is 64 bytes = 16 u8
                        let mut bin_data = [0; 57];
                        bin_data[00..1].copy_from_slice(&0u8.to_le_bytes());
                        bin_data[01..33].copy_from_slice(&challenge);
                        bin_data[33..41].copy_from_slice(&cutoff.to_le_bytes());
                        bin_data[41..49].copy_from_slice(&nonce_range.start.to_le_bytes());
                        bin_data[49..57].copy_from_slice(&nonce_range.end.to_le_bytes());

                        if let Some(sender) = shared_state.sockets.get(&client) {
                            let _ = sender
                                .1
                                .lock()
                                .await
                                .send(Message::Binary(bin_data.to_vec()))
                                .await;
                            let _ = ready_clients.lock().await.remove(&client);
                            let _ = app_client_nonce_ranges
                                .write()
                                .await
                                .insert(sender.0, nonce_range);
                        }
                    }
                }
            }

            tokio::time::sleep(Duration::from_secs(5)).await;
        }
    });

    let (mine_success_sender, mut mine_success_receiver) =
        tokio::sync::mpsc::unbounded_channel::<MessageInternalMineSuccess>();

    let (all_clients_sender, mut all_clients_receiver) =
        tokio::sync::mpsc::unbounded_channel::<MessageInternalAllClients>();

    let rpc_client = Arc::new(rpc_client);
    let app_proof = proof_ext.clone();
    let app_epoch_hashes = epoch_hashes.clone();
    let app_wallet = wallet_extension.clone();
    let app_nonce = nonce_ext.clone();
    let app_prio_fee = priority_fee.clone();
    let app_rpc_client = rpc_client.clone();
    let app_config = config.clone();
    let app_app_database = app_database.clone();
    let app_all_clients_sender = all_clients_sender.clone();
    tokio::spawn(async move {
        let rpc_client = app_rpc_client;
        let app_database = app_app_database;
        loop {
            let mut old_proof = { app_proof.lock().await.clone() };

            let cutoff = get_cutoff(old_proof, 0);
            if cutoff <= 0 {
                // process solutions
                let solution = { app_epoch_hashes.read().await.best_hash.solution.clone() };
                if let Some(solution) = solution {
                    let signer = app_wallet.clone();

                    let mut bus = rand::thread_rng().gen_range(0..BUS_COUNT);
                    let mut loaded_config = None;
                    if let (Ok(_), Ok(config), Ok(busses)) =
                        get_proof_and_config_with_busses(&rpc_client, signer.pubkey()).await
                    {
                        let mut best_bus = 0;
                        for (i, bus) in busses.iter().enumerate() {
                            if let Ok(bus) = bus {
                                if bus.rewards > busses[best_bus].unwrap().rewards {
                                    best_bus = i;
                                }
                            }
                        }
                        bus = best_bus;
                        loaded_config = Some(config);
                    }

                    let difficulty = solution.to_hash().difficulty();

                    info!(
                        "Starting mine submission attempts with difficulty {}.",
                        difficulty
                    );

                    for i in 0..10 {
                        let now = SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .expect("Time went backwards")
                            .as_secs();
                        let mut ixs = vec![];
                        let prio_fee = { app_prio_fee.lock().await.clone() };

                        info!("using priority fee of {}", prio_fee);
                        let _ = app_all_clients_sender.send(MessageInternalAllClients {
                            text: String::from("Sending mine transaction...")
                        });

                        let cu_limit_ix = ComputeBudgetInstruction::set_compute_unit_limit(480000);
                        ixs.push(cu_limit_ix);

                        let prio_fee_ix =
                            ComputeBudgetInstruction::set_compute_unit_price(prio_fee);
                        ixs.push(prio_fee_ix);

                        let noop_ix = get_auth_ix(signer.pubkey());
                        ixs.push(noop_ix);

                        if let Some(config) = loaded_config {
                            let time_until_reset = (config.last_reset_at + 60) - now as i64;
                            if time_until_reset <= 5 {
                                let reset_ix = get_reset_ix(signer.pubkey());
                                ixs.push(reset_ix);
                            }
                        }

                        let ix_mine = get_mine_ix(signer.pubkey(), solution, bus);
                        ixs.push(ix_mine);

                        if let Ok((hash, _slot)) = rpc_client
                            .get_latest_blockhash_with_commitment(rpc_client.commitment())
                            .await
                        {
                            let mut tx = Transaction::new_with_payer(&ixs, Some(&signer.pubkey()));

                            tx.sign(&[&signer], hash);
                            info!("Sending signed tx...");
                            info!("attempt: {}", i + 1);
                            let sig = rpc_client
                                .send_and_confirm_transaction_with_spinner(&tx)
                                .await;
                            if let Ok(sig) = sig {
                                // success
                                info!("Success!!");
                                info!("Sig: {}", sig);
                                let itxn = InsertTxn {
                                    txn_type: "mine".to_string(),
                                    signature: sig.to_string(),
                                    priority_fee: prio_fee as u32,
                                };
                                let _ = app_database.add_new_txn(itxn).await.unwrap();
                                // update proof
                                loop {
                                    info!("Waiting for proof hash update");
                                    let latest_proof = { app_proof.lock().await.clone() };

                                    if old_proof.challenge.eq(&latest_proof.challenge) {
                                        info!("Proof challenge not updated yet..");
                                        old_proof = latest_proof;
                                        tokio::time::sleep(Duration::from_millis(1000)).await;
                                        continue;
                                    } else {
                                        info!("Proof challenge updated! Checking rewards earned.");
                                        let balance = (latest_proof.balance as f64)
                                            / 10f64.powf(ORE_TOKEN_DECIMALS as f64);
                                        info!("New balance: {}", balance);
                                        let rewards = latest_proof.balance - old_proof.balance;
                                        let dec_rewards = (rewards as f64)
                                            / 10f64.powf(ORE_TOKEN_DECIMALS as f64);
                                        info!("Earned: {} ORE", dec_rewards);
                                        println!("Adding new challenge to db");
                                        let new_challenge = InsertChallenge {
                                            pool_id: app_config.pool_id,
                                            challenge: latest_proof.challenge.to_vec(),
                                            rewards_earned: None,
                                        };
                                        let _result =
                                            app_database.add_new_challenge(new_challenge).await;

                                        tokio::time::sleep(Duration::from_millis(200)).await;
                                        let submission_id = app_database
                                            .get_submission_id_with_nonce(u64::from_le_bytes(
                                                solution.n,
                                            ))
                                            .await
                                            .unwrap();

                                        tokio::time::sleep(Duration::from_millis(200)).await;
                                        let _ = app_database
                                            .update_challenge_rewards(
                                                old_proof.challenge.to_vec(),
                                                submission_id,
                                                rewards,
                                            )
                                            .await
                                            .unwrap();
                                        tokio::time::sleep(Duration::from_millis(200)).await;
                                        let _ = app_database
                                            .update_pool_rewards(
                                                app_wallet.pubkey().to_string(),
                                                rewards,
                                            )
                                            .await
                                            .unwrap();

                                        tokio::time::sleep(Duration::from_millis(200)).await;
                                        let challenge = app_database
                                            .get_challenge_by_challenge(
                                                old_proof.challenge.to_vec(),
                                            )
                                            .await
                                            .unwrap();

                                        let submissions =
                                            { app_epoch_hashes.read().await.submissions.clone() };

                                        let mut total_hashpower: u64 = 0;
                                        for submission in submissions.iter() {
                                            total_hashpower += submission.1 .2
                                        }

                                        let _ =
                                            mine_success_sender.send(MessageInternalMineSuccess {
                                                difficulty,
                                                total_balance: balance,
                                                rewards,
                                                challenge_id: challenge.id,
                                                total_hashpower,
                                                submissions,
                                            });

                                        {
                                            let mut prio_fee = app_prio_fee.lock().await;
                                            let mut decrease_amount = 0;
                                            if *prio_fee >= 1_000 {
                                                decrease_amount = 1_000;
                                            }
                                            if *prio_fee >= 50_000 {
                                                decrease_amount = 5_000;
                                            }
                                            if *prio_fee >= 100_000 {
                                                decrease_amount = 10_000;
                                            }

                                            *prio_fee = prio_fee.saturating_sub(decrease_amount);
                                        }
                                        // reset nonce
                                        {
                                            let mut nonce = app_nonce.lock().await;
                                            *nonce = 0;
                                        }
                                        // reset epoch hashes
                                        {
                                            info!("reset epoch hashes");
                                            let mut mut_epoch_hashes =
                                                app_epoch_hashes.write().await;
                                            mut_epoch_hashes.best_hash.solution = None;
                                            mut_epoch_hashes.best_hash.difficulty = 0;
                                            mut_epoch_hashes.submissions = HashMap::new();
                                        }
                                        break;
                                    }
                                }

                                break;
                            } else {
                                info!("increasing prio fees");
                                {
                                    let mut prio_fee = app_prio_fee.lock().await;
                                    if *prio_fee < 1_000_000 {
                                        *prio_fee += 10_000;
                                    }
                                }
                                tokio::time::sleep(Duration::from_millis(10_000)).await;
                            }
                        } else {
                            error!("Failed to get latest blockhash. retrying...");
                            tokio::time::sleep(Duration::from_millis(1_000)).await;
                        }
                    }
                    info!("Failed to send after 3 attempts. Discarding and refreshing data.");
                    // reset nonce
                    {
                        let mut nonce = app_nonce.lock().await;
                        *nonce = 0;
                    }
                    // reset epoch hashes
                    {
                        info!("reset epoch hashes");
                        let mut mut_epoch_hashes = app_epoch_hashes.write().await;
                        mut_epoch_hashes.best_hash.solution = None;
                        mut_epoch_hashes.best_hash.difficulty = 0;
                        mut_epoch_hashes.submissions = HashMap::new();
                    }
                    tokio::time::sleep(Duration::from_millis(500)).await;
                }
            } else {
                tokio::time::sleep(Duration::from_secs(cutoff as u64)).await;
            };
        }
    });

    let app_shared_state = shared_state.clone();
    let app_app_database = app_database.clone();
    let app_config = config.clone();
    tokio::spawn(async move {
        let app_database = app_app_database;
        loop {
            while let Some(msg) = mine_success_receiver.recv().await {
                {
                    let mut i_earnings = Vec::new();
                    let mut i_rewards = Vec::new();
                    let shared_state = app_shared_state.read().await;
                    for (_socket_addr, socket_sender) in shared_state.sockets.iter() {
                        let pubkey = socket_sender.0;

                        if let Some((miner_id, supplied_diff, pubkey_hashpower)) =
                            msg.submissions.get(&pubkey)
                        {
                            let hashpower_percent = (*pubkey_hashpower as u128)
                                .saturating_mul(1_000_000)
                                .saturating_div(msg.total_hashpower as u128);

                            // TODO: handle overflow/underflow and float imprecision issues
                            let decimals = 10f64.powf(ORE_TOKEN_DECIMALS as f64);
                            let earned_rewards = hashpower_percent
                                .saturating_mul(msg.rewards as u128)
                                .saturating_div(1_000_000)
                                as u64;

                            let new_earning = InsertEarning {
                                miner_id: *miner_id,
                                pool_id: app_config.pool_id,
                                challenge_id: msg.challenge_id,
                                amount: earned_rewards,
                            };

                            let new_reward = UpdateReward {
                                miner_id: *miner_id,
                                balance: earned_rewards,
                            };

                            i_earnings.push(new_earning);
                            i_rewards.push(new_reward);
                            //let _ = app_database.add_new_earning(new_earning).await.unwrap();

                            let earned_rewards_dec = (earned_rewards as f64).div(decimals);
                            let pool_rewards_dec = (msg.rewards as f64).div(decimals);

                            let message = format!(
                                "Submitted Difficulty: {}\nPool Earned: {} ORE.\nPool Balance: {}\nMiner Earned: {} ORE for difficulty: {}",
                                msg.difficulty,
                                pool_rewards_dec,
                                msg.total_balance,
                                earned_rewards_dec,
                                supplied_diff
                            );
                            if let Ok(_) = socket_sender
                                .1
                                .lock()
                                .await
                                .send(Message::Text(message))
                                .await
                            {
                            } else {
                                println!("Failed to send client text");
                            }
                        }
                    }
                    if i_earnings.len() > 0 {
                        if let Ok(_) = app_database
                            .add_new_earnings_batch(i_earnings.clone())
                            .await
                        {
                            debug!("Successfully added earnings batch");
                        } else {
                            error!("Failed to insert earnings batch");
                        }
                    }
                    if i_rewards.len() > 0 {
                        if let Ok(_) = app_database.update_rewards(i_rewards).await {
                            debug!("Successfully updated rewards");
                        } else {
                            error!("Failed to bulk update rewards");
                        }
                    }
                }
            }
        }
    });

    let app_shared_state = shared_state.clone();
    tokio::spawn(async move {
        loop {
            while let Some(msg) = all_clients_receiver.recv().await {
                {
                    let shared_state = app_shared_state.read().await;
                    for (_socket_addr, socket_sender) in shared_state.sockets.iter() {
                        let text = msg.text.clone();
                        let socket = socket_sender.clone();
                        tokio::spawn(async move {
                            if let Ok(_) = socket
                                .1
                                .lock()
                                .await
                                .send(Message::Text(text))
                                .await
                            {
                            } else {
                                println!("Failed to send client text");
                            }
                       });
                    }
                }
            }
        }
    });
    let client_channel = client_message_sender.clone();
    let app_shared_state = shared_state.clone();
    let app = Router::new()
        .route("/", get(ws_handler))
        .route("/latest-blockhash", get(get_latest_blockhash))
        .route("/pool/authority/pubkey", get(get_pool_authority_pubkey))
        .route("/signup", post(post_signup))
        .route("/claim", post(post_claim))
        .route("/miner/rewards", get(get_miner_rewards))
        .route("/miner/balance", get(get_miner_balance))
        .with_state(app_shared_state)
        .layer(Extension(app_database))
        .layer(Extension(config))
        .layer(Extension(wallet_extension))
        .layer(Extension(client_channel))
        .layer(Extension(rpc_client))
        .layer(Extension(client_nonce_ranges))
        // Logging
        .layer(
            TraceLayer::new_for_http()
                .make_span_with(DefaultMakeSpan::default().include_headers(true)),
        );

    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await.unwrap();

    tracing::debug!("listening on {}", listener.local_addr().unwrap());

    let app_shared_state = shared_state.clone();
    tokio::spawn(async move {
        ping_check_system(&app_shared_state).await;
    });

    axum::serve(
        listener,
        app.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .await
    .unwrap();

    Ok(())
}

async fn get_pool_authority_pubkey(
    Extension(wallet): Extension<Arc<Keypair>>,
) -> impl IntoResponse {
    Response::builder()
        .status(StatusCode::OK)
        .header("Content-Type", "text/text")
        .body(wallet.pubkey().to_string())
        .unwrap()
}

async fn get_latest_blockhash(
    Extension(rpc_client): Extension<Arc<RpcClient>>,
) -> impl IntoResponse {
    let latest_blockhash = rpc_client.get_latest_blockhash().await.unwrap();

    let serialized_blockhash = bincode::serialize(&latest_blockhash).unwrap();

    let encoded_blockhash = BASE64_STANDARD.encode(serialized_blockhash);
    Response::builder()
        .status(StatusCode::OK)
        .header("Content-Type", "text/text")
        .body(encoded_blockhash)
        .unwrap()
}

#[derive(Deserialize)]
struct SignupParams {
    pubkey: String,
}

async fn post_signup(
    query_params: Query<SignupParams>,
    Extension(app_database): Extension<Arc<AppDatabase>>,
    Extension(rpc_client): Extension<Arc<RpcClient>>,
    Extension(wallet): Extension<Arc<Keypair>>,
    Extension(app_config): Extension<Arc<Config>>,
    body: String,
) -> impl IntoResponse {
    if let Ok(user_pubkey) = Pubkey::from_str(&query_params.pubkey) {
        let db_miner = app_database
            .get_miner_by_pubkey_str(user_pubkey.to_string())
            .await;

        match db_miner {
            Ok(miner) => {
                if miner.enabled {
                    println!("Miner account already enabled!");
                    return Response::builder()
                        .status(StatusCode::OK)
                        .header("Content-Type", "text/text")
                        .body("SUCCESS".to_string())
                        .unwrap();
                }
            }
            Err(AppDatabaseError::FailedToGetConnectionFromPool) => {
                error!("Failed to get database pool connection");
                return Response::builder()
                    .status(StatusCode::INTERNAL_SERVER_ERROR)
                    .body("Failed to get db pool connection".to_string())
                    .unwrap();
            }
            Err(_) => {
                println!("No miner account exists. Signing up new user.");
            }
        }

        if let Some(whitelist) = &app_config.whitelist {
            if whitelist.contains(&user_pubkey) {
                let result = app_database
                    .add_new_miner(user_pubkey.to_string(), true)
                    .await;
                let miner = app_database
                    .get_miner_by_pubkey_str(user_pubkey.to_string())
                    .await
                    .unwrap();

                let wallet_pubkey = wallet.pubkey();
                let pool = app_database
                    .get_pool_by_authority_pubkey(wallet_pubkey.to_string())
                    .await
                    .unwrap();

                if result.is_ok() {
                    let new_reward = InsertReward {
                        miner_id: miner.id,
                        pool_id: pool.id,
                    };
                    let result = app_database.add_new_reward(new_reward).await;

                    if result.is_ok() {
                        return Response::builder()
                            .status(StatusCode::OK)
                            .header("Content-Type", "text/text")
                            .body("SUCCESS".to_string())
                            .unwrap();
                    } else {
                        error!("Failed to add miner rewards tracker to database");
                        return Response::builder()
                            .status(StatusCode::INTERNAL_SERVER_ERROR)
                            .body("Failed to add miner rewards tracker to database".to_string())
                            .unwrap();
                    }
                } else {
                    error!("Failed to add miner to database");
                    return Response::builder()
                        .status(StatusCode::INTERNAL_SERVER_ERROR)
                        .body("Failed to add miner to database".to_string())
                        .unwrap();
                }
            }
        }

        let serialized_tx = BASE64_STANDARD.decode(body.clone()).unwrap();
        let tx: Transaction = if let Ok(tx) = bincode::deserialize(&serialized_tx) {
            tx
        } else {
            error!("Failed to deserialize tx");
            return Response::builder()
                .status(StatusCode::BAD_REQUEST)
                .body("Invalid Tx".to_string())
                .unwrap();
        };

        if !tx.is_signed() {
            error!("Tx missing signer");
            return Response::builder()
                .status(StatusCode::BAD_REQUEST)
                .body("Invalid Tx".to_string())
                .unwrap();
        }

        let ixs = tx.message.instructions.clone();

        if ixs.len() > 1 {
            error!("Too many instructions");
            return Response::builder()
                .status(StatusCode::BAD_REQUEST)
                .body("Invalid Tx".to_string())
                .unwrap();
        }

        let base_ix = system_instruction::transfer(&user_pubkey, &wallet.pubkey(), 1_000_000);
        let mut accts = Vec::new();
        for account_index in ixs[0].accounts.clone() {
            accts.push(tx.key(0, account_index.into()));
        }

        if accts.len() != 2 {
            error!("too many accts");
            return Response::builder()
                .status(StatusCode::BAD_REQUEST)
                .body("Invalid Tx".to_string())
                .unwrap();
        }

        if ixs[0].data.ne(&base_ix.data) {
            error!("data missmatch");
            return Response::builder()
                .status(StatusCode::BAD_REQUEST)
                .body("Invalid Tx".to_string())
                .unwrap();
        } else {
            println!("Valid signup tx, submitting.");

            if let Ok(_sig) = rpc_client.send_and_confirm_transaction(&tx).await {
                let res = app_database
                    .add_new_miner(user_pubkey.to_string(), true)
                    .await;
                let miner = app_database
                    .get_miner_by_pubkey_str(user_pubkey.to_string())
                    .await
                    .unwrap();

                let wallet_pubkey = wallet.pubkey();
                let pool = app_database
                    .get_pool_by_authority_pubkey(wallet_pubkey.to_string())
                    .await
                    .unwrap();

                if res.is_ok() {
                    let new_reward = InsertReward {
                        miner_id: miner.id,
                        pool_id: pool.id,
                    };
                    let result = app_database.add_new_reward(new_reward).await;

                    if result.is_ok() {
                        return Response::builder()
                            .status(StatusCode::OK)
                            .header("Content-Type", "text/text")
                            .body("SUCCESS".to_string())
                            .unwrap();
                    } else {
                        error!("Failed to add miner rewards tracker to database");
                        return Response::builder()
                            .status(StatusCode::INTERNAL_SERVER_ERROR)
                            .body("Failed to add miner rewards tracker to database".to_string())
                            .unwrap();
                    }
                } else {
                    error!("Failed to add miner to database");
                    return Response::builder()
                        .status(StatusCode::INTERNAL_SERVER_ERROR)
                        .body("Failed to add user to database".to_string())
                        .unwrap();
                }
            } else {
                return Response::builder()
                    .status(StatusCode::INTERNAL_SERVER_ERROR)
                    .body("Failed to send tx".to_string())
                    .unwrap();
            }
        }
    } else {
        error!("Signup with invalid pubkey");
        return Response::builder()
            .status(StatusCode::BAD_REQUEST)
            .body("Invalid Pubkey".to_string())
            .unwrap();
    }
}

#[derive(Deserialize)]
struct PubkeyParam {
    pubkey: String,
}

async fn get_miner_rewards(
    query_params: Query<PubkeyParam>,
    Extension(app_database): Extension<Arc<AppDatabase>>,
) -> impl IntoResponse {
    if let Ok(user_pubkey) = Pubkey::from_str(&query_params.pubkey) {
        let res = app_database
            .get_miner_rewards(user_pubkey.to_string())
            .await;

        match res {
            Ok(rewards) => {
                let decimal_bal =
                    rewards.balance as f64 / 10f64.powf(ore_api::consts::TOKEN_DECIMALS as f64);
                let response = format!("{}", decimal_bal);
                return Response::builder()
                    .status(StatusCode::OK)
                    .body(response)
                    .unwrap();
            }
            Err(_) => {
                return Response::builder()
                    .status(StatusCode::INTERNAL_SERVER_ERROR)
                    .body("Failed to get balance".to_string())
                    .unwrap();
            }
        }
    } else {
        return Response::builder()
            .status(StatusCode::BAD_REQUEST)
            .body("Invalid public key".to_string())
            .unwrap();
    }
}

async fn get_miner_balance(
    query_params: Query<PubkeyParam>,
    Extension(rpc_client): Extension<Arc<RpcClient>>,
) -> impl IntoResponse {
    if let Ok(user_pubkey) = Pubkey::from_str(&query_params.pubkey) {
        let miner_token_account = get_associated_token_address(&user_pubkey, &get_ore_mint());
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
            .status(StatusCode::BAD_REQUEST)
            .body("Invalid public key".to_string())
            .unwrap();
    }
}

#[derive(Deserialize)]
struct ClaimParams {
    pubkey: String,
    amount: u64,
}

async fn post_claim(
    query_params: Query<ClaimParams>,
    Extension(app_database): Extension<Arc<AppDatabase>>,
    Extension(rpc_client): Extension<Arc<RpcClient>>,
    Extension(wallet): Extension<Arc<Keypair>>,
) -> impl IntoResponse {
    if let Ok(user_pubkey) = Pubkey::from_str(&query_params.pubkey) {
        let amount = query_params.amount;
        if let Ok(miner_rewards) = app_database
            .get_miner_rewards(user_pubkey.to_string())
            .await
        {
            if amount > miner_rewards.balance {
                return Response::builder()
                    .status(StatusCode::BAD_REQUEST)
                    .body("claim amount exceeds miner rewards balance".to_string())
                    .unwrap();
            }

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

            let ix = ore_api::instruction::claim(wallet.pubkey(), miner_token_account, amount);
            ixs.push(ix);

            if let Ok((hash, _slot)) = rpc_client
                .get_latest_blockhash_with_commitment(rpc_client.commitment())
                .await
            {
                let mut tx = Transaction::new_with_payer(&ixs, Some(&wallet.pubkey()));

                tx.sign(&[&wallet], hash);

                let result = rpc_client
                    .send_and_confirm_transaction_with_spinner_and_commitment(
                        &tx,
                        rpc_client.commitment(),
                    )
                    .await;
                match result {
                    Ok(sig) => {
                        info!("Miner successfully claimed.\nSig: {}", sig.to_string());

                        // TODO: use transacions, or at least put them into one query
                        let miner = app_database
                            .get_miner_by_pubkey_str(user_pubkey.to_string())
                            .await
                            .unwrap();
                        let db_pool = app_database
                            .get_pool_by_authority_pubkey(wallet.pubkey().to_string())
                            .await
                            .unwrap();
                        let _ = app_database
                            .decrease_miner_reward(miner.id, amount)
                            .await
                            .unwrap();
                        let _ = app_database
                            .update_pool_claimed(wallet.pubkey().to_string(), amount)
                            .await
                            .unwrap();

                        let itxn = InsertTxn {
                            txn_type: "claim".to_string(),
                            signature: sig.to_string(),
                            priority_fee: prio_fee,
                        };
                        let _ = app_database.add_new_txn(itxn).await.unwrap();

                        let ntxn = app_database.get_txn_by_sig(sig.to_string()).await.unwrap();

                        let iclaim = InsertClaim {
                            miner_id: miner.id,
                            pool_id: db_pool.id,
                            txn_id: ntxn.id,
                            amount,
                        };
                        let _ = app_database.add_new_claim(iclaim).await.unwrap();

                        return Response::builder()
                            .status(StatusCode::OK)
                            .body("SUCCESS".to_string())
                            .unwrap();
                    }
                    Err(e) => {
                        println!("ERROR: {:?}", e);
                        return Response::builder()
                            .status(StatusCode::INTERNAL_SERVER_ERROR)
                            .body("FAILED".to_string())
                            .unwrap();
                    }
                }
            } else {
                return Response::builder()
                    .status(StatusCode::INTERNAL_SERVER_ERROR)
                    .body("FAILED".to_string())
                    .unwrap();
            }
        } else {
            return Response::builder()
                .status(StatusCode::INTERNAL_SERVER_ERROR)
                .body("failed to get miner account from database".to_string())
                .unwrap();
        }
    } else {
        error!("Claim with invalid pubkey");
        return Response::builder()
            .status(StatusCode::BAD_REQUEST)
            .body("Invalid Pubkey".to_string())
            .unwrap();
    }
}

#[derive(Deserialize)]
struct WsQueryParams {
    timestamp: u64,
}

async fn ws_handler(
    ws: WebSocketUpgrade,
    TypedHeader(auth_header): TypedHeader<axum_extra::headers::Authorization<Basic>>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    State(app_state): State<Arc<RwLock<AppState>>>,
    //Extension(app_config): Extension<Arc<Config>>,
    Extension(client_channel): Extension<UnboundedSender<ClientMessage>>,
    Extension(app_database): Extension<Arc<AppDatabase>>,
    query_params: Query<WsQueryParams>,
) -> impl IntoResponse {
    let msg_timestamp = query_params.timestamp;

    let pubkey = auth_header.username();
    let signed_msg = auth_header.password();

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards")
        .as_secs();

    // Signed authentication message is only valid for 30 seconds
    if (now - query_params.timestamp) >= 30 {
        return Err((StatusCode::UNAUTHORIZED, "Timestamp too old."));
    }

    // verify client
    if let Ok(user_pubkey) = Pubkey::from_str(pubkey) {
        {
            let mut already_connected = false;
            for (_, (socket_pubkey, _)) in app_state.read().await.sockets.iter() {
                if user_pubkey == *socket_pubkey {
                    already_connected = true;
                    break;
                }
            }
            if already_connected {
                return Err((
                    StatusCode::TOO_MANY_REQUESTS,
                    "A client is already connected with that wallet",
                ));
            }
        };

        let db_miner = app_database
            .get_miner_by_pubkey_str(pubkey.to_string())
            .await;

        let miner;
        match db_miner {
            Ok(db_miner) => {
                miner = db_miner;
            }
            Err(AppDatabaseError::QueryFailed) => {
                return Err((
                    StatusCode::UNAUTHORIZED,
                    "pubkey is not authorized to mine. please sign up.",
                ));
            }
            Err(AppDatabaseError::InteractionFailed) => {
                return Err((
                    StatusCode::UNAUTHORIZED,
                    "pubkey is not authorized to mine. please sign up.",
                ));
            }
            Err(AppDatabaseError::FailedToGetConnectionFromPool) => {
                error!("Failed to get database pool connection.");
                return Err((StatusCode::INTERNAL_SERVER_ERROR, "Internal Server Error"));
            }
            Err(_) => {
                error!("DB Error: Catch all.");
                return Err((StatusCode::INTERNAL_SERVER_ERROR, "Internal Server Error"));
            }
        }

        if !miner.enabled {
            return Err((StatusCode::UNAUTHORIZED, "pubkey is not authorized to mine"));
        }

        if let Ok(signature) = Signature::from_str(signed_msg) {
            let ts_msg = msg_timestamp.to_le_bytes();

            if signature.verify(&user_pubkey.to_bytes(), &ts_msg) {
                println!("Client: {addr} connected with pubkey {pubkey}.");
                return Ok(ws.on_upgrade(move |socket| {
                    handle_socket(socket, addr, user_pubkey, app_state, client_channel)
                }));
            } else {
                return Err((StatusCode::UNAUTHORIZED, "Sig verification failed"));
            }
        } else {
            return Err((StatusCode::UNAUTHORIZED, "Invalid signature"));
        }
    } else {
        return Err((StatusCode::UNAUTHORIZED, "Invalid pubkey"));
    }
}

async fn handle_socket(
    mut socket: WebSocket,
    who: SocketAddr,
    who_pubkey: Pubkey,
    rw_app_state: Arc<RwLock<AppState>>,
    client_channel: UnboundedSender<ClientMessage>,
) {
    if socket
        .send(axum::extract::ws::Message::Ping(vec![1, 2, 3]))
        .await
        .is_ok()
    {
        println!("Pinged {who}...");
    } else {
        println!("could not ping {who}");

        // if we can't ping we can't do anything, return to close the connection
        return;
    }

    let (sender, mut receiver) = socket.split();
    let mut app_state = rw_app_state.write().await;
    if app_state.sockets.contains_key(&who) {
        println!("Socket addr: {who} already has an active connection");
        return;
    } else {
        app_state
            .sockets
            .insert(who, (who_pubkey, Mutex::new(sender)));
    }
    drop(app_state);

    let _ = tokio::spawn(async move {
        while let Some(Ok(msg)) = receiver.next().await {
            if process_message(msg, who, client_channel.clone()).is_break() {
                break;
            }
        }
    })
    .await;

    let mut app_state = rw_app_state.write().await;
    app_state.sockets.remove(&who);
    drop(app_state);

    info!("Client: {} disconnected!", who_pubkey.to_string());
}

fn process_message(
    msg: Message,
    who: SocketAddr,
    client_channel: UnboundedSender<ClientMessage>,
) -> ControlFlow<(), ()> {
    match msg {
        Message::Text(t) => {
            println!(">>> {who} sent str: {t:?}");
        }
        Message::Binary(d) => {
            // first 8 bytes are message type
            let message_type = d[0];
            match message_type {
                0 => {
                    println!("Got Ready message");
                    let mut b_index = 1;

                    let mut pubkey = [0u8; 32];
                    for i in 0..32 {
                        pubkey[i] = d[i + b_index];
                    }
                    b_index += 32;

                    let mut ts = [0u8; 8];
                    for i in 0..8 {
                        ts[i] = d[i + b_index];
                    }

                    let ts = u64::from_le_bytes(ts);

                    let now = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .expect("Time went backwards")
                        .as_secs();

                    let time_since = now - ts;
                    if time_since > 30 {
                        error!("Client tried to ready up with expired signed message");
                        return ControlFlow::Break(());
                    }

                    let msg = ClientMessage::Ready(who);
                    let _ = client_channel.send(msg);
                }
                1 => {
                    let msg = ClientMessage::Mining(who);
                    let _ = client_channel.send(msg);
                }
                2 => {
                    // parse solution from message data
                    let mut solution_bytes = [0u8; 16];
                    // extract (16 u8's) from data for hash digest
                    let mut b_index = 1;
                    for i in 0..16 {
                        solution_bytes[i] = d[i + b_index];
                    }
                    b_index += 16;

                    // extract 64 bytes (8 u8's)
                    let mut nonce = [0u8; 8];
                    for i in 0..8 {
                        nonce[i] = d[i + b_index];
                    }
                    b_index += 8;

                    let mut pubkey = [0u8; 32];
                    for i in 0..32 {
                        pubkey[i] = d[i + b_index];
                    }

                    b_index += 32;

                    let signature_bytes = d[b_index..].to_vec();
                    if let Ok(sig_str) = String::from_utf8(signature_bytes.clone()) {
                        if let Ok(sig) = Signature::from_str(&sig_str) {
                            let pubkey = Pubkey::new_from_array(pubkey);

                            let mut hash_nonce_message = [0; 24];
                            hash_nonce_message[0..16].copy_from_slice(&solution_bytes);
                            hash_nonce_message[16..24].copy_from_slice(&nonce);

                            if sig.verify(&pubkey.to_bytes(), &hash_nonce_message) {
                                let solution = Solution::new(solution_bytes, nonce);

                                let msg = ClientMessage::BestSolution(who, solution, pubkey);
                                let _ = client_channel.send(msg);
                            } else {
                                error!("Client submission sig verification failed.");
                            }
                        } else {
                            error!("Failed to parse into Signature.");
                        }
                    } else {
                        error!("Failed to parse signed message from client.");
                    }
                }
                _ => {
                    println!(">>> {} sent an invalid message", who);
                }
            }
        }
        Message::Close(c) => {
            if let Some(cf) = c {
                println!(
                    ">>> {} sent close with code {} and reason `{}`",
                    who, cf.code, cf.reason
                );
            } else {
                println!(">>> {who} somehow sent close message without CloseFrame");
            }
            return ControlFlow::Break(());
        }
        Message::Pong(_v) => {
            //println!(">>> {who} sent pong with {v:?}");
        }
        Message::Ping(_v) => {
            //println!(">>> {who} sent ping with {v:?}");
        }
    }

    ControlFlow::Continue(())
}

async fn proof_tracking_system(ws_url: String, wallet: Arc<Keypair>, proof: Arc<Mutex<Proof>>) {
    loop {
        println!("Establishing rpc websocket connection...");
        let mut ps_client = PubsubClient::new(&ws_url).await;
        let mut attempts = 0;

        while ps_client.is_err() && attempts < 3 {
            error!("Failed to connect to websocket, retrying...");
            ps_client = PubsubClient::new(&ws_url).await;
            tokio::time::sleep(Duration::from_millis(1000)).await;
            attempts += 1;
        }
        info!("RPC WS connection established!");

        let app_wallet = wallet.clone();
        if let Ok(ps_client) = ps_client {
            let ps_client = Arc::new(ps_client);
            let app_proof = proof.clone();
            let account_pubkey = proof_pubkey(app_wallet.pubkey());
            let pubsub = ps_client
                .account_subscribe(
                    &account_pubkey,
                    Some(RpcAccountInfoConfig {
                        encoding: Some(UiAccountEncoding::Base64),
                        data_slice: None,
                        commitment: Some(CommitmentConfig::confirmed()),
                        min_context_slot: None,
                    }),
                )
                .await;

            info!("Tracking pool proof updates with websocket");
            if let Ok((mut account_sub_notifications, _account_unsub)) = pubsub {
                while let Some(response) = account_sub_notifications.next().await {
                    let data = response.value.data.decode();
                    if let Some(data_bytes) = data {
                        // if let Ok(bus) = Bus::try_from_bytes(&data_bytes) {
                        //     let _ = sender.send(AccountUpdatesData::BusData(*bus));
                        // }
                        // if let Ok(ore_config) = ore_api::state::Config::try_from_bytes(&data_bytes) {
                        //     let _ = sender.send(AccountUpdatesData::TreasuryConfigData(*ore_config));
                        // }
                        if let Ok(new_proof) = Proof::try_from_bytes(&data_bytes) {
                            // let _ = sender.send(AccountUpdatesData::ProofData(*proof));
                            //
                            {
                                let mut app_proof = app_proof.lock().await;
                                *app_proof = *new_proof;
                            }
                        }
                    }
                }
            }
        }
    }
}

async fn client_message_handler_system(
    mut receiver_channel: UnboundedReceiver<ClientMessage>,
    shared_state: &Arc<RwLock<AppState>>,
    app_database: Arc<AppDatabase>,
    ready_clients: Arc<Mutex<HashSet<SocketAddr>>>,
    proof: Arc<Mutex<Proof>>,
    epoch_hashes: Arc<RwLock<EpochHashes>>,
    client_nonce_ranges: Arc<RwLock<HashMap<Pubkey, Range<u64>>>>,
) {
    while let Some(client_message) = receiver_channel.recv().await {
        match client_message {
            ClientMessage::Ready(addr) => {
                println!("Client {} is ready!", addr.to_string());
                {
                    let shared_state = shared_state.read().await;
                    if let Some(sender) = shared_state.sockets.get(&addr) {
                        {
                            let mut ready_clients = ready_clients.lock().await;
                            ready_clients.insert(addr);
                        }

                        if let Ok(_) = sender
                            .1
                            .lock()
                            .await
                            .send(Message::Text(String::from("Client successfully added.")))
                            .await
                        {
                        } else {
                            println!("Failed notify client they were readied up!");
                        }
                    }
                }
            }
            ClientMessage::Mining(addr) => {
                println!("Client {} has started mining!", addr.to_string());
            }
            ClientMessage::BestSolution(_addr, solution, pubkey) => {
                let app_epoch_hashes = epoch_hashes.clone();
                let app_app_database = app_database.clone();
                let app_proof = proof.clone();
                let app_client_nonce_ranges = client_nonce_ranges.clone();
                tokio::spawn(async move {
                    let epoch_hashes = app_epoch_hashes;
                    let app_database = app_app_database;
                    let proof = app_proof;
                    let client_nonce_ranges = app_client_nonce_ranges;

                    let pubkey_str = pubkey.to_string();
                    let challenge = {
                        let proof = proof.lock().await;
                        proof.challenge
                    };

                    let nonce_range: Range<u64> = {
                        if let Some(nr) = client_nonce_ranges.read().await.get(&pubkey) {
                            nr.clone()
                        } else {
                            error!("Client nonce range not set!");
                            return;
                        }
                    };

                    let nonce = u64::from_le_bytes(solution.n);

                    if !nonce_range.contains(&nonce) {
                        error!("Client submitted nonce out of assigned range");
                        return;
                    }

                    if solution.is_valid(&challenge) {
                        let diff = solution.to_hash().difficulty();
                        println!("{} found diff: {}", pubkey_str, diff);
                        if diff >= MIN_DIFF {
                            // calculate rewards
                            let hashpower = MIN_HASHPOWER * 2u64.pow(diff - MIN_DIFF);
                            tokio::time::sleep(Duration::from_millis(100)).await;
                            if let Ok(challenge) = app_database
                                .get_challenge_by_challenge(challenge.to_vec())
                                .await
                            {
                                tokio::time::sleep(Duration::from_millis(100)).await;
                                let miner = app_database
                                    .get_miner_by_pubkey_str(pubkey_str)
                                    .await
                                    .unwrap();

                                let new_submission = InsertSubmission {
                                    miner_id: miner.id,
                                    challenge_id: challenge.id,
                                    nonce,
                                    difficulty: diff as i8,
                                };

                                tokio::time::sleep(Duration::from_millis(100)).await;
                                let _ = app_database
                                    .add_new_submission(new_submission)
                                    .await
                                    .unwrap();

                                {
                                    let mut epoch_hashes = epoch_hashes.write().await;
                                    epoch_hashes
                                        .submissions
                                        .insert(pubkey, (miner.id, diff, hashpower));
                                    if diff > epoch_hashes.best_hash.difficulty {
                                        epoch_hashes.best_hash.difficulty = diff;
                                        epoch_hashes.best_hash.solution = Some(solution);
                                    }
                                }
                            } else {
                                error!("Challenge not found in db, :(");
                            }
                        } else {
                            println!("Diff to low, skipping");
                        }
                    } else {
                        println!("{} returned an invalid solution!", pubkey);
                    }
                });
            }
        }
    }
}

async fn ping_check_system(shared_state: &Arc<RwLock<AppState>>) {
    loop {
        // send ping to all sockets
        let mut failed_sockets = Vec::new();
        let app_state = shared_state.read().await;
        // I don't like doing all this work while holding this lock...
        for (who, socket) in app_state.sockets.iter() {
            if socket
                .1
                .lock()
                .await
                .send(Message::Ping(vec![1, 2, 3]))
                .await
                .is_ok()
            {
                //println!("Pinged: {who}...");
            } else {
                failed_sockets.push(who.clone());
            }
        }
        drop(app_state);

        // remove any sockets where ping failed
        let mut app_state = shared_state.write().await;
        for address in failed_sockets {
            app_state.sockets.remove(&address);
        }
        drop(app_state);

        tokio::time::sleep(Duration::from_secs(5)).await;
    }
}
