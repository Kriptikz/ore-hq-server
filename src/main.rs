use std::{
    collections::{HashMap, HashSet},
    net::SocketAddr,
    ops::{ControlFlow, Div, Range},
    path::Path,
    str::FromStr,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use rand::seq::SliceRandom;
use systems::claim_system::claim_system;

use crate::ore_utils::{get_managed_proof_token_ata, get_proof_pda};

use self::models::*;
use app_rr_database::AppRRDatabase;
use ::ore_utils::AccountDeserialize;
use app_database::{AppDatabase, AppDatabaseError};
use axum::{
    extract::{
        ws::{Message, WebSocket},
        ConnectInfo, Query, State, WebSocketUpgrade,
    }, http::{Method, Response, StatusCode}, response::IntoResponse, routing::{get, post}, Extension, Json, Router
};
use axum_extra::{headers::authorization::Basic, TypedHeader};
use base64::{prelude::BASE64_STANDARD, Engine};
use clap::Parser;
use drillx::Solution;
use futures::{stream::SplitSink, SinkExt, StreamExt};
use ore_api::{consts::BUS_COUNT, event::MineEvent, state::Proof};
use ore_utils::{
    get_auth_ix, get_config, get_cutoff, get_delegated_stake_account, get_mine_ix, get_ore_mint, get_original_proof, get_proof, get_proof_and_config_with_busses, get_register_ix, get_reset_ix, ORE_TOKEN_DECIMALS
};
use rand::Rng;
use routes::{get_challenges, get_latest_mine_txn, get_pool_balance};
use serde::Deserialize;
use solana_account_decoder::UiAccountEncoding;
use solana_client::{
    nonblocking::{pubsub_client::PubsubClient, rpc_client::RpcClient}, rpc_client::SerializableTransaction, rpc_config::{RpcAccountInfoConfig, RpcSendTransactionConfig, RpcSimulateTransactionConfig, RpcTransactionConfig}
};
use solana_sdk::{
    commitment_config::{CommitmentConfig, CommitmentLevel}, compute_budget::ComputeBudgetInstruction, instruction::InstructionError, native_token::{lamports_to_sol, LAMPORTS_PER_SOL}, pubkey::Pubkey, signature::{read_keypair_file, Keypair, Signature}, signer::Signer, system_instruction::{self, transfer}, transaction::{Transaction, TransactionError}
};
use solana_transaction_status::{TransactionConfirmationStatus, UiTransactionEncoding};
use spl_associated_token_account::{get_associated_token_address, instruction::create_associated_token_account};
use tokio::{
    io::AsyncReadExt,
    sync::{
        mpsc::{UnboundedReceiver, UnboundedSender},
        Mutex, RwLock,
    }, time::Instant,
};
use tower_http::{cors::CorsLayer, trace::{DefaultMakeSpan, TraceLayer}};
use tracing::{error, info};

mod app_rr_database;
mod app_database;
mod models;
mod routes;
mod schema;
mod systems;
mod proof_migration;


const MIN_DIFF: u32 = 8;
const MIN_HASHPOWER: u64 = 5;

#[derive(Clone)]
struct AppClientConnection {
    pubkey: Pubkey,
    miner_id: i32,
    socket: Arc<Mutex<SplitSink<WebSocket, Message>>>,
}

#[derive(Clone)]
struct WalletExtension {
    miner_wallet: Arc<Keypair>,
    fee_wallet: Arc<Keypair>,
}

struct AppState {
    sockets: HashMap<SocketAddr, AppClientConnection>,
    paused: bool,
}

struct ClaimsQueue {
    queue: RwLock<HashMap<Pubkey, u64>>,
}

struct SubmissionWindow {
    closed: bool
}

pub struct MessageInternalAllClients {
    text: String,
}

#[derive(Debug, Clone, Copy)]
pub struct InternalMessageSubmission {
    miner_id: i32,
    supplied_diff: u32,
    supplied_nonce: u64,
    hashpower: u64,
}

pub struct MessageInternalMineSuccess {
    difficulty: u32,
    total_balance: f64,
    rewards: u64,
    challenge_id: i32,
    challenge: [u8; 32],
    best_nonce: u64,
    total_hashpower: u64,
    ore_config: Option<ore_api::state::Config>,
    multiplier: f64,
    submissions: HashMap<Pubkey, InternalMessageSubmission>,
}

pub struct LastPong {
    pongs: HashMap<SocketAddr, Instant>
}

#[derive(Debug)]
pub enum ClientMessage {
    Ready(SocketAddr),
    Mining(SocketAddr),
    Pong(SocketAddr),
    BestSolution(SocketAddr, Solution, Pubkey),
}

pub struct EpochHashes {
    challenge: [u8; 32],
    best_hash: BestHash,
    submissions: HashMap<Pubkey, InternalMessageSubmission>,
}

pub struct BestHash {
    solution: Option<Solution>,
    difficulty: u32,
}

pub struct Config {
    password: String,
    whitelist: Option<HashSet<Pubkey>>,
    pool_id: i32,
    stats_enabled: bool,
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
        value_name = "jito tip",
        help = "Number of lamports to pay as jito tip per transaction",
        default_value = "0",
        global = true
    )]
    jito_tip: u64,
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
    #[arg(
        long,
        short,
        action,
        help = "Enable stats endpoints",
    )]
    stats: bool,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    dotenv::dotenv().ok();
    let args = Args::parse();

    let file_appender = tracing_appender::rolling::daily("./logs", "ore-hq-server.log");
    let (non_blocking, _guard) = tracing_appender::non_blocking(file_appender);
    tracing_subscriber::fmt().with_writer(non_blocking).init();

    // load envs
    let wallet_path_str = std::env::var("WALLET_PATH").expect("WALLET_PATH must be set.");
    let fee_wallet_path_str = std::env::var("FEE_WALLET_PATH").expect("FEE_WALLET_PATH must be set.");
    let rpc_url = std::env::var("RPC_URL").expect("RPC_URL must be set.");
    let rpc_ws_url = std::env::var("RPC_WS_URL").expect("RPC_WS_URL must be set.");
    let password = std::env::var("PASSWORD").expect("PASSWORD must be set.");
    let database_url = std::env::var("DATABASE_URL").expect("DATABASE_URL must be set.");
    let database_rr_url = std::env::var("DATABASE_RR_URL").expect("DATABASE_RR_URL must be set.");

    let app_database = Arc::new(AppDatabase::new(database_url));
    let app_rr_database = Arc::new(AppRRDatabase::new(database_rr_url));

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

    let priority_fee = Arc::new(args.priority_fee);
    let jito_tip = Arc::new(args.jito_tip);

    // load wallet
    let wallet_path = Path::new(&wallet_path_str);

    if !wallet_path.exists() {
        tracing::error!("Failed to load wallet at: {}", wallet_path_str);
        return Err("Failed to find wallet path.".into());
    }

    let wallet = read_keypair_file(wallet_path)
        .expect("Failed to load keypair from file: {wallet_path_str}");
    info!("loaded wallet {}", wallet.pubkey().to_string());

    let wallet_path = Path::new(&fee_wallet_path_str);

    if !wallet_path.exists() {
        tracing::error!("Failed to load fee wallet at: {}", fee_wallet_path_str);
        return Err("Failed to find fee wallet path.".into());
    }

    let fee_wallet = read_keypair_file(wallet_path)
        .expect("Failed to load keypair from file: {wallet_path_str}");
    info!("loaded fee wallet {}", wallet.pubkey().to_string());

    info!("establishing rpc connection...");
    let rpc_client = RpcClient::new_with_commitment(rpc_url, CommitmentConfig::confirmed());
    let jito_url = "https://mainnet.block-engine.jito.wtf/api/v1/transactions".to_string();
    let jito_client = RpcClient::new(jito_url);

    info!("loading sol balance...");
    let balance = if let Ok(balance) = rpc_client.get_balance(&wallet.pubkey()).await {
        balance
    } else {
        return Err("Failed to load balance".into());
    };

    info!("Balance: {:.2}", balance as f64 / LAMPORTS_PER_SOL as f64);

    if balance < 1_000_000 {
        return Err("Sol balance is too low!".into());
    }

    let proof = if let Ok(loaded_proof) = get_proof(&rpc_client, wallet.pubkey()).await {
        info!("LOADED PROOF: \n{:?}", loaded_proof);
        loaded_proof
    } else {
        error!("Failed to load proof.");
        info!("Creating proof account...");

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
                info!("Sig: {}", sig.to_string());
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

    info!("Validating miners delegate stake account is created");
    match get_delegated_stake_account(&rpc_client, wallet.pubkey(), wallet.pubkey()).await {
        Ok(data) => {
            info!("Miner Delegated Stake Account: {:?}", data);
            info!("Miner delegate stake account already created.");
        },
        Err(_) => {
            info!("Creating miner delegate stake account");
            let ix = ore_miner_delegation::instruction::init_delegate_stake(
                wallet.pubkey(),
                wallet.pubkey(),
                wallet.pubkey(),
            );

            let mut tx = Transaction::new_with_payer(&[ix], Some(&wallet.pubkey()));

            let blockhash = rpc_client
                .get_latest_blockhash()
                .await
                .expect("should get latest blockhash");

            tx.sign(&[&wallet], blockhash);

            match rpc_client.send_and_confirm_transaction_with_spinner_and_commitment(&tx, CommitmentConfig {
                commitment: CommitmentLevel::Confirmed
            }).await {
                Ok(_) => {
                    info!("Successfully created miner delegate stake account");
                },
                Err(_) => {
                    error!("Failed to send and confirm tx.");
                    panic!("Failed to create miner delegate stake account");
                }
            }
        }
    }

    info!("Validating managed proof token account is created");
    let managed_proof = Pubkey::find_program_address(&[b"managed-proof-account", wallet.pubkey().as_ref()], &ore_miner_delegation::id());

    let managed_proof_token_account_addr = get_managed_proof_token_ata(wallet.pubkey());
    match rpc_client.get_token_account_balance(&managed_proof_token_account_addr).await {
        Ok(_) => {
            info!("Managed proof token account already created.");
        },
        Err(_) => {
            info!("Creating managed proof token account");
            let ix = create_associated_token_account(&wallet.pubkey(), &managed_proof.0, &ore_api::consts::MINT_ADDRESS, &spl_token::id());

            let mut tx = Transaction::new_with_payer(&[ix], Some(&wallet.pubkey()));

            let blockhash = rpc_client
                .get_latest_blockhash()
                .await
                .expect("should get latest blockhash");

            tx.sign(&[&wallet], blockhash);

            match rpc_client.send_and_confirm_transaction_with_spinner_and_commitment(&tx, CommitmentConfig {
                commitment: CommitmentLevel::Confirmed
            }).await {
                Ok(_) => {
                    info!("Successfully created managed proof token account");
                },
                Err(e) => {
                    error!("Failed to send and confirm tx.\nE: {:?}", e);
                    panic!("Failed to create managed proof token account");
                }
            }
        }
    }

    let miner_ore_token_account_addr = get_associated_token_address(&wallet.pubkey(), &ore_api::consts::MINT_ADDRESS);
    let token_balance = if let Ok(token_balance) = rpc_client.get_token_account_balance(&miner_ore_token_account_addr).await {
        let bal = token_balance.ui_amount.unwrap() * 10f64.powf(token_balance.decimals as f64);
        bal as u64
    } else {
        error!("Failed to get miner ORE token account balance");
        panic!("Failed to get ORE token account balance.");
    };

    info!("Checking original proof, and token account balances.");
    let original_proof = if let Ok(loaded_proof) = get_original_proof(&rpc_client, wallet.pubkey()).await {
        loaded_proof
    } else {
    panic!("Failed to get original proof!");
    };
    if original_proof.balance > 0 || token_balance > 0 {
        info!("Proof balance has {} tokens. Miner ORE token account has {} tokens.\nMigrating...", original_proof.balance, token_balance);
        if let Err(e) = proof_migration::migrate(&rpc_client, &wallet, original_proof.balance, token_balance).await {
            info!("Failed to migrate proof balance.\nError: {}", e);
            panic!("Failed to migrate proof balance.");
        } else {
            info!("Successfully migrated proof balance");
        }
    }

    info!("Validating pool exists in db");
    let db_pool = app_database
        .get_pool_by_authority_pubkey(wallet.pubkey().to_string())
        .await;

    match db_pool {
        Ok(_) => {}
        Err(AppDatabaseError::FailedToGetConnectionFromPool) => {
            panic!("Failed to get database pool connection");
        }
        Err(_) => {
            info!("Pool missing from database. Inserting...");
            let proof_pubkey = get_proof_pda(wallet.pubkey());
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

    info!("Validating current challenge for pool exists in db");
    let result = app_database
        .get_challenge_by_challenge(proof.challenge.to_vec())
        .await;

    match result {
        Ok(_) => {}
        Err(AppDatabaseError::FailedToGetConnectionFromPool) => {
            panic!("Failed to get database pool connection");
        }
        Err(_) => {
            info!("Challenge missing from database. Inserting...");
            let new_challenge = models::InsertChallenge {
                pool_id: db_pool.id,
                challenge: proof.challenge.to_vec(),
                rewards_earned: None,
            };
            let result = app_database.add_new_challenge(new_challenge).await;

            if result.is_err() {
                panic!("Failed to create challenge in database");
            }
        }
    }

    let config = Arc::new(Config {
        password,
        whitelist,
        pool_id: db_pool.id,
        stats_enabled: args.stats,
    });

    let epoch_hashes = Arc::new(RwLock::new(EpochHashes {
        challenge: proof.challenge,
        best_hash: BestHash {
            solution: None,
            difficulty: 0,
        },
        submissions: HashMap::new(),
    }));

    let wallet_extension = Arc::new(WalletExtension {
        miner_wallet: Arc::new(wallet),
        fee_wallet: Arc::new(fee_wallet),
    });
    let proof_ext = Arc::new(Mutex::new(proof));
    let nonce_ext = Arc::new(Mutex::new(0u64));

    let client_nonce_ranges = Arc::new(RwLock::new(HashMap::new()));

    let shared_state = Arc::new(RwLock::new(AppState {
        sockets: HashMap::new(),
        paused: false,
    }));
    let ready_clients = Arc::new(Mutex::new(HashSet::new()));

    let pongs = Arc::new(RwLock::new(LastPong { pongs: HashMap::new() }));

    let claims_queue = Arc::new(ClaimsQueue {
        queue: RwLock::new(HashMap::new())
    });

    let submission_window = Arc::new(RwLock::new(SubmissionWindow { closed: false }));

    let rpc_client = Arc::new(rpc_client);
    let jito_client = Arc::new(jito_client);

    let app_rpc_client = rpc_client.clone();
    let app_wallet = wallet_extension.clone();
    let app_claims_queue = claims_queue.clone();
    let app_app_database = app_database.clone();
    tokio::spawn(async move {
        claim_system(app_claims_queue, app_rpc_client, app_wallet.miner_wallet.clone(), app_app_database).await;
    });

    // Track client pong timings
    let app_pongs = pongs.clone();
    let app_state = shared_state.clone();
    tokio::spawn(async move {
        pong_tracking_system(app_pongs, app_state).await;
    });

    let app_wallet = wallet_extension.clone();
    let app_proof = proof_ext.clone();
    // Establish webocket connection for tracking pool proof changes.
    tokio::spawn(async move {
        proof_tracking_system(rpc_ws_url, app_wallet.miner_wallet.clone(), app_proof).await;
    });

    let (client_message_sender, client_message_receiver) =
        tokio::sync::mpsc::unbounded_channel::<ClientMessage>();

    // Handle client messages
    let app_ready_clients = ready_clients.clone();
    let app_proof = proof_ext.clone();
    let app_epoch_hashes = epoch_hashes.clone();
    let app_client_nonce_ranges = client_nonce_ranges.clone();
    let app_state = shared_state.clone();
    let app_pongs = pongs.clone();
    let app_submission_window = submission_window.clone();
    tokio::spawn(async move {
        client_message_handler_system(
            client_message_receiver,
            app_ready_clients,
            app_proof,
            app_epoch_hashes,
            app_client_nonce_ranges,
            app_state,
            app_pongs,
            app_submission_window
        )
        .await;
    });

    // Handle ready clients
    let app_shared_state = shared_state.clone();
    let app_proof = proof_ext.clone();
    let app_epoch_hashes = epoch_hashes.clone();
    let app_nonce = nonce_ext.clone();
    let app_client_nonce_ranges = client_nonce_ranges.clone();
    let app_state = shared_state.clone();
    tokio::spawn(async move {
        loop {

            let reader = app_state.read().await;
            let paused = reader.paused.clone();
            drop(reader);

            if !paused {
                let mut clients = Vec::new();
                {
                    let ready_clients_lock = ready_clients.lock().await;
                    for ready_client in ready_clients_lock.iter() {
                        clients.push(ready_client.clone());
                    }
                    drop(ready_clients_lock);
                };

                if clients.len() > 0 {
                    let lock = app_proof.lock().await;
                    let latest_proof = lock.clone();
                    drop(lock);

                    let cutoff = get_cutoff(latest_proof, 7);
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
                        let lock = app_proof.lock().await;
                        let latest_proof = lock.clone();
                        drop(lock);
                        let challenge = latest_proof.challenge;


                        info!("Giving clients challenge: {}", BASE64_STANDARD.encode(challenge));
                        info!("With cutoff: {}", cutoff);
                        for client in clients {
                            let nonce_range = {
                                let mut nonce = app_nonce.lock().await;
                                let start = *nonce;
                                // max hashes possible in 60s for a single client
                                *nonce += 4_000_000;
                                let end = *nonce;
                                start..end
                            };
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

                            let app_client_nonce_ranges = app_client_nonce_ranges.clone();
                            let shared_state = app_shared_state.read().await;
                            let sockets = shared_state.sockets.clone();
                            drop(shared_state);
                            if let Some(sender) = sockets.get(&client) {
                                let sender = sender.clone();
                                let ready_clients = ready_clients.clone();
                                tokio::spawn(async move {
                                    let _ = sender
                                        .socket
                                        .lock()
                                        .await
                                        .send(Message::Binary(bin_data.to_vec()))
                                        .await;
                                    let _ = ready_clients.lock().await.remove(&client);
                                    let _ = app_client_nonce_ranges
                                        .write()
                                        .await
                                        .insert(sender.pubkey, nonce_range);
                                });
                            }
                        }
                    }
                }

            } else {
                info!("Mining is paused");
                tokio::time::sleep(Duration::from_secs(30)).await;
            }

            tokio::time::sleep(Duration::from_millis(400)).await;
        }
    });

    let (mine_success_sender, mut mine_success_receiver) =
        tokio::sync::mpsc::unbounded_channel::<MessageInternalMineSuccess>();

    let (all_clients_sender, mut all_clients_receiver) =
        tokio::sync::mpsc::unbounded_channel::<MessageInternalAllClients>();

    let app_proof = proof_ext.clone();
    let app_epoch_hashes = epoch_hashes.clone();
    let app_wallet = wallet_extension.clone();
    let app_nonce = nonce_ext.clone();
    let app_prio_fee = priority_fee.clone();
    let app_jito_tip = jito_tip.clone();
    let app_rpc_client = rpc_client.clone();
    let app_jito_client = jito_client.clone();
    let app_config = config.clone();
    let app_app_database = app_database.clone();
    let app_all_clients_sender = all_clients_sender.clone();
    let app_submission_window = submission_window.clone();
    tokio::spawn(async move {
        let rpc_client = app_rpc_client;
        let jito_client = app_jito_client;
        let app_database = app_app_database;
        loop {
            let lock = app_proof.lock().await;
            let old_proof = lock.clone();
            drop(lock);

            let cutoff = get_cutoff(old_proof, 0);
            if cutoff <= 0 {
                // process solutions
                let reader = app_epoch_hashes.read().await;
                let solution = reader.best_hash.solution.clone();
                drop(reader);
                if solution.is_some() {
                    // Close submission window
                    info!("Submission window closed.");
                    let mut writer = app_submission_window.write().await;
                    writer.closed = true;
                    drop(writer);

                    let signer = app_wallet.clone().miner_wallet.clone();

                    let bus = rand::thread_rng().gen_range(0..BUS_COUNT);

                    let mut success = false;
                    let reader = app_epoch_hashes.read().await;
                    let best_solution = reader.best_hash.solution.clone();
                    let submissions = reader.submissions.clone();
                    drop(reader);

                    for i in 0..10 {
                        if let Some(best_solution) = best_solution {
                            let difficulty = best_solution.to_hash().difficulty();

                            info!(
                                "Starting mine submission attempt {} with difficulty {}.",
                                i, difficulty
                            );
                            info!("Submission Challenge: {}", BASE64_STANDARD.encode(old_proof.challenge));
                            let mut loaded_config = None;
                            info!("Getting latest config and busses data.");
                            if let (Ok(p), Ok(config), Ok(_busses)) =
                                get_proof_and_config_with_busses(&rpc_client, signer.pubkey()).await
                            {
                                loaded_config = Some(config);

                                info!("Latest Challenge: {}", BASE64_STANDARD.encode(p.challenge));

                                if !best_solution.is_valid(&p.challenge) {
                                    error!("SOLUTION IS NOT VALID ANYMORE!");
                                    info!("Updating to latest proof.");
                                    let mut lock = app_proof.lock().await;
                                    *lock = p;
                                    drop(lock);
                                    break;
                                }
                            }
                            let now = SystemTime::now()
                                .duration_since(UNIX_EPOCH)
                                .expect("Time went backwards")
                                .as_secs();
                            let mut ixs = vec![];
                            let mut prio_fee = *app_prio_fee;

                            let _ = app_all_clients_sender.send(MessageInternalAllClients {
                                text: String::from("Server is sending mine transaction..."),
                            });

                            let mut cu_limit = 485_000;
                            let should_add_reset_ix = if let Some(config) = loaded_config {
                                let time_until_reset = (config.last_reset_at + 300) - now as i64;
                                if time_until_reset <= 5 {
                                    cu_limit = 500_000;
                                    prio_fee += 50_000;
                                    info!("Including reset tx.");
                                    true
                                } else {
                                    false
                                }
                            } else {
                                false
                            };

                            info!("using priority fee of {}", prio_fee);

                            let cu_limit_ix =
                                ComputeBudgetInstruction::set_compute_unit_limit(cu_limit);
                            ixs.push(cu_limit_ix);

                            let prio_fee_ix =
                                ComputeBudgetInstruction::set_compute_unit_price(prio_fee);
                            ixs.push(prio_fee_ix);

                            let jito_tip = *app_jito_tip;
                            if jito_tip > 0 {
                                        let tip_accounts = [
                                            "96gYZGLnJYVFmbjzopPSU6QiEV5fGqZNyN9nmNhvrZU5",
                                            "HFqU5x63VTqvQss8hp11i4wVV8bD44PvwucfZ2bU7gRe",
                                            "Cw8CFyM9FkoMi7K7Crf6HNQqf4uEMzpKw6QNghXLvLkY",
                                            "ADaUMid9yfUytqMBgopwjb2DTLSokTSzL1zt6iGPaS49",
                                            "DfXygSm4jCyNCybVYYK6DwvWqjKee8pbDmJGcLWNDXjh",
                                            "ADuUkR4vqLUMWXxW9gh6D6L8pMSawimctcNZ5pGwDcEt",
                                            "DttWaMuVvTiduZRnguLF7jNxTgiMBZ1hyAumKUiL2KRL",
                                            "3AVi9Tg9Uo68tJfuvoKvqKNWKkC5wPdSSdeBnizKZ6jT",
                                        ];
                                        ixs.push(transfer(
                                            &signer.pubkey(),
                                            &Pubkey::from_str(
                                                &tip_accounts
                                                    .choose(&mut rand::thread_rng())
                                                    .unwrap()
                                                    .to_string(),
                                            )
                                            .unwrap(),
                                            jito_tip,
                                        ));

                                        info!("Jito tip: {} SOL", lamports_to_sol(jito_tip));
                                    }

                            let noop_ix = get_auth_ix(signer.pubkey());
                            ixs.push(noop_ix);

                            if should_add_reset_ix {
                                let reset_ix = get_reset_ix(signer.pubkey());
                                ixs.push(reset_ix);
                            }


                            let ix_mine = get_mine_ix(signer.pubkey(), best_solution, bus);
                            ixs.push(ix_mine);

                            if let Ok((hash, _slot)) = rpc_client
                                .get_latest_blockhash_with_commitment(rpc_client.commitment())
                                .await
                            {
                                let mut tx =
                                    Transaction::new_with_payer(&ixs, Some(&signer.pubkey()));

                                let expired_timer = Instant::now();
                                tx.sign(&[&signer], hash);
                                info!("Sending signed tx...");
                                info!("attempt: {}", i + 1);
                                let send_client = if jito_tip > 0 {
                                    jito_client.clone()
                                } else {
                                    rpc_client.clone()
                                };

                                let rpc_config =  RpcSendTransactionConfig {
                                    skip_preflight: false,
                                    preflight_commitment: Some(rpc_client.commitment().commitment),
                                    ..RpcSendTransactionConfig::default()
                                };

                                let rpc_sim_config =  RpcSimulateTransactionConfig {
                                    sig_verify: false,
                                    ..RpcSimulateTransactionConfig::default()
                                };


                                let sim_tx = tx.clone();

                                if let Ok(result) = rpc_client.simulate_transaction_with_config(&sim_tx, rpc_sim_config).await {
                                    if let Some(tx_error) = result.value.err {
                                        if tx_error == TransactionError::InstructionError(4, InstructionError::Custom(1))
                                        || tx_error == TransactionError::InstructionError(5, InstructionError::Custom(1)) {
                                            error!("Custom program error: Invalid Hash");
                                            break;
                                        }
                                    }
                                }

                                let mut rpc_send_attempts = 1;
                                let signature = loop {
                                    match send_client.send_transaction_with_config(&tx, rpc_config).await {
                                        Ok(sig) => {
                                            break Ok(sig);
                                        
                                        },
                                        Err(_e) => {
                                            error!("Attempt {} Failed to send mine transaction. retrying in 1 seconds...", rpc_send_attempts);
                                            rpc_send_attempts += 1;

                                            if rpc_send_attempts >= 5 {
                                                break Err("Failed to send tx");

                                            }
                                            tokio::time::sleep(Duration::from_millis(1500)).await;
                                        }
                                    }
                                };

                                let signature = if signature.is_err() {
                                    break;
                                } else {
                                    signature.unwrap()
                                };
                                let (tx_message_sender, tx_message_receiver) =
                                    tokio::sync::oneshot::channel::<u8>();
                                let app_app_nonce = app_nonce.clone();
                                let app_app_database = app_database.clone();
                                let app_app_config = app_config.clone();
                                let app_app_rpc_client = rpc_client.clone();
                                let app_send_client = send_client.clone();
                                let app_app_proof = app_proof.clone();
                                let app_app_wallet = app_wallet.clone();
                                let app_app_epoch_hashes = app_epoch_hashes.clone();
                                let app_app_submission_window = app_submission_window.clone();
                                tokio::spawn(async move {
                                    let mut stop_reciever = tx_message_receiver;
                                    let app_nonce = app_app_nonce;
                                    let app_database = app_app_database;
                                    let app_config = app_app_config;
                                    let app_rpc_client = app_app_rpc_client;
                                    let app_proof = app_app_proof;
                                    let app_wallet = app_app_wallet;
                                    let app_epoch_hashes = app_app_epoch_hashes;
                                    let app_submission_window = app_app_submission_window;
                                    tokio::time::sleep(Duration::from_millis(2000)).await;
                                    loop {
                                        if let Ok(_) = stop_reciever.try_recv() {
                                            // Transaction has succeeded or expired
                                            break;
                                        } else {
                                            info!("Resending signed tx...");
                                            let _ = app_send_client.send_transaction_with_config(&tx, rpc_config).await;

                                            // Wait 500ms then check for updated proof
                                            tokio::time::sleep(Duration::from_millis(500)).await;

                                            info!("Checking for proof hash update.");
                                            let lock = app_proof.lock().await;
                                            let latest_proof = lock.clone();
                                            drop(lock);

                                            if old_proof.challenge.eq(&latest_proof.challenge) {
                                                info!("Proof challenge not updated yet..");
                                                if let Ok(p) = get_proof(&app_rpc_client, app_wallet.miner_wallet.pubkey()).await {
                                                    info!("OLD PROOF CHALLENGE: {}", BASE64_STANDARD.encode(old_proof.challenge));
                                                    info!("RPC PROOF CHALLENGE: {}", BASE64_STANDARD.encode(p.challenge));
                                                    if old_proof.challenge.ne(&p.challenge) {
                                                        info!("Found new proof from rpc call, not websocket...");
                                                        let mut lock = app_proof.lock().await;
                                                        *lock = p;
                                                        drop(lock);

                                                        // Add new db challenge, reset epoch_hashes,
                                                        // and open the submission window

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
                                                            mut_epoch_hashes.challenge = p.challenge;
                                                            mut_epoch_hashes.best_hash.solution = None;
                                                            mut_epoch_hashes.best_hash.difficulty = 0;
                                                            mut_epoch_hashes.submissions = HashMap::new();
                                                        }
                                                        // Open submission window
                                                        info!("openning submission window.");
                                                        let mut writer = app_submission_window.write().await;
                                                        writer.closed = false;
                                                        drop(writer);

                                                        info!("Adding new challenge to db");
                                                        let new_challenge = InsertChallenge {
                                                            pool_id: app_config.pool_id,
                                                            challenge: latest_proof.challenge.to_vec(),
                                                            rewards_earned: None,
                                                        };

                                                        while let Err(_) = app_database
                                                            .add_new_challenge(new_challenge.clone())
                                                            .await
                                                        {
                                                            error!("Failed to add new challenge to db.");
                                                            info!("Verifying challenge does not already exist.");
                                                            if let Ok(_) = app_database.get_challenge_by_challenge(new_challenge.challenge.clone()).await {
                                                                info!("Challenge already exists, continuing");
                                                                break;
                                                            }

                                                            tokio::time::sleep(Duration::from_millis(1000))
                                                                .await;
                                                        }
                                                        info!("New challenge successfully added to db");

                                                        break;
                                                    }
                                                }
                                            } else {
                                                info!("Adding new challenge to db");
                                                let new_challenge = InsertChallenge {
                                                    pool_id: app_config.pool_id,
                                                    challenge: latest_proof.challenge.to_vec(),
                                                    rewards_earned: None,
                                                };

                                                while let Err(_) = app_database
                                                    .add_new_challenge(new_challenge.clone())
                                                    .await
                                                {
                                                    error!("Failed to add new challenge to db.");
                                                    info!("Verifying challenge does not already exist.");
                                                    if let Ok(_) = app_database.get_challenge_by_challenge(new_challenge.challenge.clone()).await {
                                                        info!("Challenge already exists, continuing");
                                                        break;
                                                    }

                                                    tokio::time::sleep(Duration::from_millis(1000))
                                                        .await;
                                                }
                                                info!("New challenge successfully added to db");


                                                // Reset mining data
                                                // {
                                                //     let mut prio_fee = app_prio_fee.lock().await;
                                                //     let mut decrease_amount = 0;
                                                //     if *prio_fee > 20_000 {
                                                //         decrease_amount = 1_000;
                                                //     }
                                                //     if *prio_fee >= 50_000 {
                                                //         decrease_amount = 5_000;
                                                //     }
                                                //     if *prio_fee >= 100_000 {
                                                //         decrease_amount = 10_000;
                                                //     }

                                                //     *prio_fee =
                                                //         prio_fee.saturating_sub(decrease_amount);
                                                // }
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
                                                    mut_epoch_hashes.challenge = latest_proof.challenge;
                                                    mut_epoch_hashes.best_hash.solution = None;
                                                    mut_epoch_hashes.best_hash.difficulty = 0;
                                                    mut_epoch_hashes.submissions = HashMap::new();
                                                }
                                                // Open submission window
                                                info!("openning submission window.");
                                                let mut writer = app_submission_window.write().await;
                                                writer.closed = false;
                                                drop(writer);

                                                break;
                                            }
                                        }
                                        tokio::time::sleep(Duration::from_millis(1000)).await;
                                    }
                                    return

                                });

                                let result: Result<Signature, String> = loop {
                                    if expired_timer.elapsed().as_secs() >= 200 {
                                        break Err("Transaction Expired".to_string());
                                    }
                                    let results = rpc_client.get_signature_statuses(&[signature]).await;
                                    if let Ok(response) = results {
                                        let statuses = response.value;
                                        if let Some(status) = &statuses[0] {
                                            info!("Status: {:?}", status);
                                            if status.confirmation_status() == TransactionConfirmationStatus::Finalized {
                                                    if status.err.is_some() {
                                                        let e_str = format!("Transaction Failed: {:?}", status.err);
                                                        break Err(e_str);
                                                    }
                                                    break Ok(signature);
                                            }
                                        }
                                    }
                                    // wait 500ms before checking status
                                    tokio::time::sleep(Duration::from_millis(500)).await;
                                };
                                // stop the tx sender
                                let _ = tx_message_sender.send(0);

                                match result {
                                    Ok(sig) => {
                                        // success
                                        success = true;
                                        info!("Success!!");
                                        info!("Sig: {}", sig);
                                        let itxn = InsertTxn {
                                            txn_type: "mine".to_string(),
                                            signature: sig.to_string(),
                                            priority_fee: prio_fee as u32,
                                        };
                                        let app_db = app_database.clone();
                                        tokio::spawn(async move {
                                            while let Err(_) = app_db.add_new_txn(itxn.clone()).await {
                                                error!("Failed to add tx to db! Retrying...");
                                                tokio::time::sleep(Duration::from_millis(2000)).await;
                                            }
                                        });

                                        // get reward amount from MineEvent data and update database
                                        // and clients
                                        let app_rpc_client = rpc_client.clone();
                                        let app_app_database = app_database.clone();
                                        let app_mine_success_sender = Arc::new(mine_success_sender.clone());
                                        let app_app_proof = app_proof.clone();
                                        let app_app_config = app_config.clone();
                                        tokio::spawn(async move {
                                            let rpc_client = app_rpc_client;
                                            let app_database = app_app_database;
                                            let mine_success_sender = app_mine_success_sender;
                                            let app_proof = app_app_proof;
                                            let app_config = app_app_config;
                                            loop {
                                                if let Ok(txn_result) = rpc_client.get_transaction_with_config(&sig, RpcTransactionConfig {
                                                    encoding: Some(UiTransactionEncoding::Base64),
                                                    commitment: Some(rpc_client.commitment()),
                                                    max_supported_transaction_version: None,
                                                }).await {
                                                    let data = txn_result.transaction.meta.unwrap().return_data;

                                                    match data {
                                                        solana_transaction_status::option_serializer::OptionSerializer::Some(data) => {
                                                            let bytes = BASE64_STANDARD.decode(data.data.0).unwrap();

                                                            if let Ok(mine_event) = bytemuck::try_from_bytes::<MineEvent>(&bytes) {
                                                                info!("MineEvent: {:?}", mine_event);
                                                                let rewards = mine_event.reward;
                                                                // handle sending mine success message
                                                                let mut total_hashpower: u64 = 0;
                                                                for submission in submissions.iter() {
                                                                    total_hashpower += submission.1.hashpower
                                                                }
                                                                let challenge;
                                                                loop {
                                                                    if let Ok(c) = app_database
                                                                        .get_challenge_by_challenge(
                                                                            old_proof.challenge.to_vec(),
                                                                        )
                                                                        .await
                                                                    {
                                                                        challenge = c;
                                                                        break;
                                                                    } else {
                                                                        error!(
                                                                            "Failed to get challenge by challenge! Inserting if necessary..."
                                                                        );
                                                                        let new_challenge = InsertChallenge {
                                                                            pool_id: app_config.pool_id,
                                                                            challenge: old_proof.challenge.to_vec(),
                                                                            rewards_earned: None,
                                                                        };
                                                                        while let Err(_) = app_database
                                                                            .add_new_challenge(new_challenge.clone())
                                                                            .await
                                                                        {
                                                                            error!("Failed to add new challenge to db.");
                                                                            info!("Verifying challenge does not already exist.");
                                                                            if let Ok(_) = app_database.get_challenge_by_challenge(new_challenge.challenge.clone()).await {
                                                                                info!("Challenge already exists, continuing");
                                                                                break;
                                                                            }

                                                                            tokio::time::sleep(Duration::from_millis(1000))
                                                                                .await;
                                                                        }
                                                                        info!("New challenge successfully added to db");
                                                                        tokio::time::sleep(Duration::from_millis(1000)).await;
                                                                    }
                                                                }

                                                                tokio::time::sleep(Duration::from_millis(1000)).await;
                                                                let latest_proof = { app_proof.lock().await.clone() };
                                                                let balance = (latest_proof.balance as f64)
                                                                    / 10f64.powf(ORE_TOKEN_DECIMALS as f64);


                                                                let multiplier = if let Some(config) = loaded_config {
                                                                    if config.top_balance > 0 {
                                                                        1.0 + (latest_proof.balance as f64 / config.top_balance as f64).min(1.0f64)
                                                                    } else {
                                                                        1.0f64
                                                                    }
                                                                } else {
                                                                    1.0f64
                                                                };

                                                                let _ = mine_success_sender.send(
                                                                    MessageInternalMineSuccess {
                                                                        difficulty,
                                                                        total_balance: balance,
                                                                        rewards,
                                                                        challenge_id: challenge.id,
                                                                        challenge: old_proof.challenge,
                                                                        best_nonce: u64::from_le_bytes(best_solution.n),
                                                                        total_hashpower,
                                                                        ore_config: loaded_config,
                                                                        multiplier,
                                                                        submissions,
                                                                    },
                                                                );
                                                                tokio::time::sleep(Duration::from_millis(200)).await;
                                                            } else {
                                                                error!("Failed get MineEvent data from transaction... wtf...");
                                                                break;
                                                            }

                                                        },
                                                        solana_transaction_status::option_serializer::OptionSerializer::None => {
                                                            error!("RPC gave no transaction metadata....");
                                                            tokio::time::sleep(Duration::from_millis(2000)).await;
                                                            continue;
                                                        },
                                                        solana_transaction_status::option_serializer::OptionSerializer::Skip => {
                                                            error!("RPC gave transaction metadata should skip...");
                                                            tokio::time::sleep(Duration::from_millis(2000)).await;
                                                            continue;

                                                        },
                                                    }
                                                    break;
                                                } else {
                                                    error!("Failed to get confirmed transaction... Come on rpc...");
                                                    tokio::time::sleep(Duration::from_millis(2000)).await;
                                                }
                                            }
                                        });

                                        loop {
                                            info!("Checking for proof hash update.");
                                            let lock = app_proof.lock().await;
                                            let latest_proof = lock.clone();
                                            drop(lock);

                                            if old_proof.challenge.eq(&latest_proof.challenge) {
                                                info!("Proof challenge not updated yet..");
                                                if let Ok(p) = get_proof(&rpc_client, app_wallet.miner_wallet.pubkey()).await {
                                                    info!("OLD PROOF CHALLENGE: {}", BASE64_STANDARD.encode(old_proof.challenge));
                                                    info!("RPC PROOF CHALLENGE: {}", BASE64_STANDARD.encode(p.challenge));
                                                    if old_proof.challenge.ne(&p.challenge) {
                                                        info!("Found new proof after finalized from rpc call, not websocket...");
                                                        let mut lock = app_proof.lock().await;
                                                        *lock = p;
                                                        drop(lock);

                                                        // Add new db challenge, reset epoch_hashes,
                                                        // and open the submission window

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
                                                            mut_epoch_hashes.challenge = p.challenge;
                                                            mut_epoch_hashes.best_hash.solution = None;
                                                            mut_epoch_hashes.best_hash.difficulty = 0;
                                                            mut_epoch_hashes.submissions = HashMap::new();
                                                        }
                                                        // Open submission window
                                                        info!("openning submission window.");
                                                        let mut writer = app_submission_window.write().await;
                                                        writer.closed = false;
                                                        drop(writer);

                                                        info!("Adding new challenge to db");
                                                        let new_challenge = InsertChallenge {
                                                            pool_id: app_config.pool_id,
                                                            challenge: latest_proof.challenge.to_vec(),
                                                            rewards_earned: None,
                                                        };

                                                        while let Err(_) = app_database
                                                            .add_new_challenge(new_challenge.clone())
                                                            .await
                                                        {
                                                            error!("Failed to add new challenge to db.");
                                                            info!("Verifying challenge does not already exist.");
                                                            if let Ok(_) = app_database.get_challenge_by_challenge(new_challenge.challenge.clone()).await {
                                                                info!("Challenge already exists, continuing");
                                                                break;
                                                            }

                                                            tokio::time::sleep(Duration::from_millis(1000))
                                                                .await;
                                                        }
                                                        info!("New challenge successfully added to db");

                                                        break;
                                                    }
                                                }
                                            } else {
                                                let reader = app_epoch_hashes.read().await;
                                                let epoch_hashes_challenge = reader.challenge;
                                                drop(reader);

                                                if latest_proof.challenge.eq(&epoch_hashes_challenge) {
                                                    // epoch_hashes challenge was already updated
                                                    info!("Epoch hashes challenge already up to date!");
                                                    break;
                                                } else {
                                                    info!("Epoch hashes challenge was not updated yet. Updating...");
                                                    // Reset mining data
                                                    // {
                                                    //     let mut prio_fee = app_prio_fee.lock().await;
                                                    //     let mut decrease_amount = 0;
                                                    //     if *prio_fee > 20_000 {
                                                    //         decrease_amount = 1_000;
                                                    //     }
                                                    //     if *prio_fee >= 50_000 {
                                                    //         decrease_amount = 5_000;
                                                    //     }
                                                    //     if *prio_fee >= 100_000 {
                                                    //         decrease_amount = 10_000;
                                                    //     }

                                                    //     *prio_fee =
                                                    //         prio_fee.saturating_sub(decrease_amount);
                                                    // }
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
                                                        mut_epoch_hashes.challenge = latest_proof.challenge;
                                                        mut_epoch_hashes.best_hash.solution = None;
                                                        mut_epoch_hashes.best_hash.difficulty = 0;
                                                        mut_epoch_hashes.submissions = HashMap::new();
                                                    }
                                                    // Open submission window
                                                    info!("openning submission window.");
                                                    let mut writer = app_submission_window.write().await;
                                                    writer.closed = false;
                                                    drop(writer);
                                                    info!("Adding new challenge to db");
                                                    let new_challenge = InsertChallenge {
                                                        pool_id: app_config.pool_id,
                                                        challenge: latest_proof.challenge.to_vec(),
                                                        rewards_earned: None,
                                                    };

                                                    while let Err(_) = app_database
                                                        .add_new_challenge(new_challenge.clone())
                                                        .await
                                                    {
                                                        error!("Failed to add new challenge to db.");
                                                        info!("Verifying challenge does not already exist.");
                                                        if let Ok(_) = app_database.get_challenge_by_challenge(new_challenge.challenge.clone()).await {
                                                            info!("Challenge already exists, continuing");
                                                            break;
                                                        }

                                                        tokio::time::sleep(Duration::from_millis(1000))
                                                            .await;
                                                    }
                                                    info!("New challenge successfully added to db");
                                                    break;
                                                }
                                            }
                                        }
                                        break;
                                    },
                                    Err(e) => {
                                        error!("Failed to send and confirm txn");
                                        error!("Error: {:?}", e);
                                        // info!("increasing prio fees");
                                        // {
                                        //     let mut prio_fee = app_prio_fee.lock().await;
                                        //     if *prio_fee < 1_000_000 {
                                        //         *prio_fee += 15_000;
                                        //     }
                                        // }
                                        tokio::time::sleep(Duration::from_millis(2_000)).await;
                                    }
                                }
                            } else {
                                error!("Failed to get latest blockhash. retrying...");
                                tokio::time::sleep(Duration::from_millis(1_000)).await;
                            }
                        } else {
                            error!("Solution is_some but got none on best hash re-check?");
                            tokio::time::sleep(Duration::from_millis(1_000)).await;
                        }
                    }
                    if !success {
                        info!("Failed to send tx. Discarding and refreshing data.");
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
                        // Open submission window
                        info!("openning submission window.");
                        let mut writer = app_submission_window.write().await;
                        writer.closed = false;
                        drop(writer);

                    }
                    tokio::time::sleep(Duration::from_millis(500)).await;
                } else {
                    error!("No best solution yet.");
                    tokio::time::sleep(Duration::from_millis(1000)).await;
                }
            } else {
                tokio::time::sleep(Duration::from_secs(cutoff as u64)).await;
            };
        }
    });

    let app_shared_state = shared_state.clone();
    let app_app_database = app_database.clone();
    let app_config = config.clone();
    let app_wallet = wallet_extension.clone();
    tokio::spawn(async move {
        let app_database = app_app_database;
        loop {
            while let Some(msg) = mine_success_receiver.recv().await {
                {
                    let mut i_earnings = Vec::new();
                    let mut i_rewards = Vec::new();
                    let mut i_submissions = Vec::new();

                    for (miner_pubkey, msg_submission) in msg.submissions.iter() {
                        let hashpower_percent = (msg_submission.hashpower as u128)
                            .saturating_mul(1_000_000)
                            .saturating_div(msg.total_hashpower as u128);

                        // TODO: handle overflow/underflow and float imprecision issues
                        let decimals = 10f64.powf(ORE_TOKEN_DECIMALS as f64);
                        let earned_rewards = hashpower_percent
                            .saturating_mul(msg.rewards as u128)
                            .saturating_div(1_000_000)
                            as u64;

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
                        
                        let shared_state = app_shared_state.read().await;
                        let len = shared_state.sockets.len();
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

                        for (_addr, client_connection) in shared_state.sockets.iter() {
                            let client_message = message.clone();
                            if client_connection.pubkey.eq(&miner_pubkey) {
                                let socket_sender = client_connection.socket.clone();
                                tokio::spawn(async move {
                                    if let Ok(_) = socket_sender
                                        .lock()
                                        .await
                                        .send(Message::Text(client_message))
                                        .await
                                    {
                                    } else {
                                        error!("Failed to send client text");
                                    }
                                });

                            }
                        }
                    
                    }

                    if i_earnings.len() > 0 {
                        while let Err(_) = app_database
                            .add_new_earnings_batch(i_earnings.clone())
                            .await
                        {
                            error!("Failed to add new earnings batch to db. Retrying...");
                            tokio::time::sleep(Duration::from_millis(500))
                                .await;
                        }
                        info!("Successfully added earnings batch");
                    }
                    tokio::time::sleep(Duration::from_millis(500)).await;
                    if i_rewards.len() > 0 {
                        while let Err(_) = app_database
                            .update_rewards(i_rewards.clone())
                            .await
                        {
                            error!("Failed to update rewards in db. Retrying...");
                            tokio::time::sleep(Duration::from_millis(500))
                                .await;
                        }
                        info!("Successfully updated rewards");
                    }
                    tokio::time::sleep(Duration::from_millis(500)).await;
                    if i_submissions.len() > 0 {
                        while let Err(_) = app_database
                            .add_new_submissions_batch(i_submissions.clone())
                            .await
                        {
                            error!("Failed to add new submissions batch. Retrying...");
                            tokio::time::sleep(Duration::from_millis(500))
                                .await;
                        }
                        info!("Successfully added submissions batch");
                    }
                    tokio::time::sleep(Duration::from_millis(500)).await;
                    while let Err(_) = app_database
                        .update_pool_rewards(
                            app_wallet.miner_wallet.pubkey().to_string(),
                            msg.rewards,
                        )
                        .await
                    {
                        error!(
                            "Failed to update pool rewards! Retrying..."
                        );
                        tokio::time::sleep(Duration::from_millis(1000))
                            .await;
                    }

                    tokio::time::sleep(Duration::from_millis(200)).await;
                    let submission_id;
                    loop {
                        if let Ok(s) = app_database.get_submission_id_with_nonce(msg.best_nonce)
                        .await {
                            submission_id = s;
                            break;
                        } else {
                            error!("Failed to get submission id with nonce! Retrying...");
                            tokio::time::sleep(Duration::from_millis(1000))
                                .await;
                        }
                    }
                    tokio::time::sleep(Duration::from_millis(200)).await;
                    if let Err(_) = app_database
                        .update_challenge_rewards(
                            msg.challenge.to_vec(),
                            submission_id,
                            msg.rewards,
                        )
                        .await
                    {
                        error!("Failed to update challenge rewards! Skipping! Devs check!");
                        let err_str = format!("Challenge UPDATE FAILED - Challenge: {:?}\nSubmission ID: {}\nRewards: {}\n", msg.challenge.to_vec(), submission_id, msg.rewards);
                        error!(err_str);
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
                            if let Ok(_) =
                                socket.socket.lock().await.send(Message::Text(text)).await
                            {
                            } else {
                                error!("Failed to send client text");
                            }
                        });
                    }
                }
            }
        }
    });

    let cors = CorsLayer::new()
        .allow_methods([Method::GET])
        .allow_origin(tower_http::cors::Any);

    let client_channel = client_message_sender.clone();
    let app_shared_state = shared_state.clone();
    let app = Router::new()
        .route("/", get(ws_handler))
        .route("/pause", post(post_pause))
        .route("/latest-blockhash", get(get_latest_blockhash))
        .route("/pool/authority/pubkey", get(get_pool_authority_pubkey))
        .route("/pool/fee_payer/pubkey", get(get_pool_fee_payer_pubkey))
        .route("/signup", post(post_signup))
        .route("/claim", post(post_claim))
        .route("/stake", post(post_stake))
        .route("/unstake", post(post_unstake))
        .route("/active-miners", get(get_connected_miners))
        .route("/timestamp", get(get_timestamp))
        .route("/miner/balance", get(get_miner_balance))
        .route("/miner/stake", get(get_miner_stake))
        .route("/stake-multiplier", get(get_stake_multiplier))
        // App RR Database routes
        .route("/last-challenge-submissions", get(get_last_challenge_submissions))
        .route("/miner/rewards", get(get_miner_rewards))
        .route("/miner/submissions", get(get_miner_submissions))
        .route("/challenges", get(get_challenges))
        .route("/pool", get(routes::get_pool))
        .route("/pool/staked", get(routes::get_pool_staked))
        .route("/pool/balance", get(get_pool_balance))
        .route("/txns/latest-mine", get(get_latest_mine_txn))
        .with_state(app_shared_state)
        .layer(Extension(app_database))
        .layer(Extension(app_rr_database))
        .layer(Extension(config))
        .layer(Extension(wallet_extension))
        .layer(Extension(client_channel))
        .layer(Extension(rpc_client))
        .layer(Extension(client_nonce_ranges))
        .layer(Extension(claims_queue))
        .layer(Extension(submission_window))
        // Logging
        .layer(
            TraceLayer::new_for_http()
                .make_span_with(DefaultMakeSpan::default().include_headers(true)),
        )
        .layer(cors);

    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await.unwrap();

    tracing::info!("listening on {}", listener.local_addr().unwrap());

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
    Extension(wallet): Extension<Arc<WalletExtension>>,
) -> impl IntoResponse {
    Response::builder()
        .status(StatusCode::OK)
        .header("Content-Type", "text/text")
        .body(wallet.miner_wallet.pubkey().to_string())
        .unwrap()
}

async fn get_pool_fee_payer_pubkey(
    Extension(wallet): Extension<Arc<WalletExtension>>,
) -> impl IntoResponse {
    Response::builder()
        .status(StatusCode::OK)
        .header("Content-Type", "text/text")
        .body(wallet.fee_wallet.pubkey().to_string())
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
struct PauseParams {
    p: String,
}

async fn post_pause(
    query_params: Query<PauseParams>,
    Extension(app_config): Extension<Arc<Config>>,
    State(app_state): State<Arc<RwLock<AppState>>>,
) -> impl IntoResponse {
    if query_params.p.eq(app_config.password.as_str()) {
        let mut writer = app_state.write().await;
        writer.paused = true;
        drop(writer);
        return Response::builder()
            .status(StatusCode::OK)
            .header("Content-Type", "text/text")
            .body("SUCCESS".to_string())
            .unwrap();
    }

    return Response::builder()
        .status(StatusCode::UNAUTHORIZED)
        .header("Content-Type", "text/text")
        .body("Unauthorized".to_string())
        .unwrap();
}

#[derive(Deserialize)]
struct SignupParams {
    pubkey: String,
}

async fn post_signup(
    query_params: Query<SignupParams>,
    Extension(app_database): Extension<Arc<AppDatabase>>,
    Extension(rpc_client): Extension<Arc<RpcClient>>,
    Extension(wallet): Extension<Arc<WalletExtension>>,
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
                    info!("Miner account already enabled!");
                    return Response::builder()
                        .status(StatusCode::OK)
                        .header("Content-Type", "text/text")
                        .body("EXISTS".to_string())
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
                info!("No miner account exists. Signing up new user.");
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

                let wallet_pubkey = wallet.miner_wallet.pubkey();
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

        let base_ix = system_instruction::transfer(&user_pubkey, &wallet.miner_wallet.pubkey(), 1_000_000);
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
            info!("Valid signup tx, submitting.");

            let result = rpc_client.send_and_confirm_transaction(&tx).await;

            match result {
                Ok(_sig) => {
                    let res = app_database
                        .add_new_miner(user_pubkey.to_string(), true)
                        .await;
                    let miner = app_database
                        .get_miner_by_pubkey_str(user_pubkey.to_string())
                        .await
                        .unwrap();

                    let wallet_pubkey = wallet.miner_wallet.pubkey();
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
                },
                Err(e) => {
                    error!("{} signup transaction failed...", user_pubkey.to_string());
                    error!("Signup Tx Error: {:?}", e);
                    return Response::builder()
                        .status(StatusCode::INTERNAL_SERVER_ERROR)
                        .body("Failed to send tx".to_string())
                        .unwrap();
                }
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
    Extension(app_rr_database): Extension<Arc<AppRRDatabase>>,
) -> impl IntoResponse {
    if let Ok(user_pubkey) = Pubkey::from_str(&query_params.pubkey) {
        let res = app_rr_database
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
                error!("get_miner_rewards: failed to get rewards balance from db for {}", user_pubkey.to_string());
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

async fn get_last_challenge_submissions(
    Extension(app_rr_database): Extension<Arc<AppRRDatabase>>,
    Extension(app_config): Extension<Arc<Config>>,
) -> Result<Json<Vec<SubmissionWithPubkey>>, String> {
    if app_config.stats_enabled {
        let res = app_rr_database
            .get_last_challenge_submissions()
            .await;

        match res {
            Ok(submissions) => {
                Ok(Json(submissions))
            }
            Err(_) => {
                Err("Failed to get submissions for miner".to_string())
            }
        }
    } else {
        return Err("Stats not enabled for this server.".to_string());
    }
}

#[derive(Deserialize)]
struct GetSubmissionsParams {
    pubkey: String,
}

async fn get_miner_submissions(
    query_params: Query<GetSubmissionsParams>,
    Extension(app_rr_database): Extension<Arc<AppRRDatabase>>,
    Extension(app_config): Extension<Arc<Config>>,
) -> Result<Json<Vec<Submission>>, String> {
    if app_config.stats_enabled {
        if let Ok(user_pubkey) = Pubkey::from_str(&query_params.pubkey) {
            let res = app_rr_database
                .get_miner_submissions(user_pubkey.to_string())
                .await;

            match res {
                Ok(submissions) => {
                    Ok(Json(submissions))
                }
                Err(_) => {
                    Err("Failed to get submissions for miner".to_string())
                }
            }
        } else {
            Err("Invalid public key".to_string())
        }
    } else {
        return Err("Stats not enabled for this server.".to_string());
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

async fn get_miner_stake(
    query_params: Query<PubkeyParam>,
    Extension(rpc_client): Extension<Arc<RpcClient>>,
    Extension(wallet): Extension<Arc<WalletExtension>>,
) -> impl IntoResponse {
    if let Ok(user_pubkey) = Pubkey::from_str(&query_params.pubkey) {
        if let Ok(account) = get_delegated_stake_account(&rpc_client, user_pubkey, wallet.miner_wallet.pubkey()).await
        {
            let decimals = 10f64.powf(ORE_TOKEN_DECIMALS as f64);
            let dec_amount = (account.amount as f64).div(decimals);
            return Ok(dec_amount.to_string());
        } else {
            return Err("Failed to get token account balance".to_string());
        }
    } else {
        return Err("Invalid pubkey".to_string());
    }
}

async fn get_stake_multiplier(
    Extension(rpc_client): Extension<Arc<RpcClient>>,
    Extension(app_config): Extension<Arc<Config>>,
) -> impl IntoResponse {
    if app_config.stats_enabled {
        let pubkey = Pubkey::from_str("mineXqpDeBeMR8bPQCyy9UneJZbjFywraS3koWZ8SSH").unwrap();
        let proof = if let Ok(loaded_proof) = get_proof(&rpc_client, pubkey).await {
            loaded_proof
        } else {
            error!("get_pool_staked: Failed to load proof.");
            return Err("Stats not enabled for this server.".to_string());
        };

        if let Ok(config) = get_config(&rpc_client).await
        {
            let multiplier = 1.0 + (proof.balance as f64 / config.top_balance as f64).min(1.0f64);
            return Ok(Json(multiplier));
        } else {
            return Err("Failed to get ore config account".to_string());
        }
    } else {
        return Err("Stats not enabled for this server.".to_string());
    }
}

async fn get_connected_miners(State(app_state): State<Arc<RwLock<AppState>>>) -> impl IntoResponse {
    let len = app_state.read().await.sockets.len();
    return Response::builder()
        .status(StatusCode::OK)
        .body(len.to_string())
        .unwrap();
}

async fn get_timestamp() -> impl IntoResponse {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards")
        .as_secs();
    return Response::builder()
        .status(StatusCode::OK)
        .body(now.to_string())
        .unwrap();
}

#[derive(Deserialize)]
struct ClaimParams {
    pubkey: String,
    amount: u64,
}

async fn post_claim(
    query_params: Query<ClaimParams>,
    Extension(app_database): Extension<Arc<AppDatabase>>,
    Extension(claims_queue): Extension<Arc<ClaimsQueue>>,
) -> impl IntoResponse {
    if let Ok(user_pubkey) = Pubkey::from_str(&query_params.pubkey) {
        let reader = claims_queue.queue.read().await;
        let queue = reader.clone();
        drop(reader);

        if queue.contains_key(&user_pubkey) {
            return Response::builder()
                .status(StatusCode::TOO_MANY_REQUESTS)
                .body("QUEUED".to_string())
                .unwrap();
        }

        let amount = query_params.amount;

        // 0.00500000000
        if amount < 500_000_000 {
            return Response::builder()
                .status(StatusCode::BAD_REQUEST)
                .body("claim minimum is 0.005".to_string())
                .unwrap();
        }

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

            if let Ok(last_claim) = app_database.get_last_claim(miner_rewards.miner_id).await {
                let last_claim_ts = last_claim.created_at.and_utc().timestamp();
                let now = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .expect("Time went backwards")
                    .as_secs() as i64;
                let time_difference = now - last_claim_ts;
                if time_difference  <= 1800 {
                    return Response::builder()
                        .status(StatusCode::TOO_MANY_REQUESTS)
                        .body(time_difference.to_string())
                        .unwrap();
                }
            }

            let mut writer = claims_queue.queue.write().await;
            writer.insert(user_pubkey, amount);
            drop(writer);
            return Response::builder()
                .status(StatusCode::OK)
                .body("SUCCESS".to_string())
                .unwrap();
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
struct StakeParams {
    pubkey: String,
    amount: u64,
}

async fn post_stake(
    query_params: Query<StakeParams>,
    Extension(rpc_client): Extension<Arc<RpcClient>>,
    Extension(wallet): Extension<Arc<WalletExtension>>,
    body: String,
) -> impl IntoResponse {
    if let Ok(user_pubkey) = Pubkey::from_str(&query_params.pubkey) {
        let serialized_tx = BASE64_STANDARD.decode(body.clone()).unwrap();
        let mut tx: Transaction = if let Ok(tx) = bincode::deserialize(&serialized_tx) {
            tx
        } else {
            error!("Failed to deserialize tx");
            return Response::builder()
                .status(StatusCode::BAD_REQUEST)
                .body("Invalid Tx".to_string())
                .unwrap();
        };

        let ixs = tx.message.instructions.clone();

        if ixs.len() > 1 {
            error!("Too many instructions");
            return Response::builder()
                .status(StatusCode::BAD_REQUEST)
                .body("Invalid Tx".to_string())
                .unwrap();
        }

        if let Err(_) = get_delegated_stake_account(&rpc_client, user_pubkey, wallet.miner_wallet.pubkey()).await {
            let init_ix = ore_miner_delegation::instruction::init_delegate_stake(user_pubkey, wallet.miner_wallet.pubkey(), wallet.fee_wallet.pubkey());

            let prio_fee: u32 = 20_000;

            let mut ixs = Vec::new();
            let prio_fee_ix = ComputeBudgetInstruction::set_compute_unit_price(prio_fee as u64);
            ixs.push(prio_fee_ix);
            ixs.push(init_ix);
            if let Ok((hash, _slot)) = rpc_client
                .get_latest_blockhash_with_commitment(rpc_client.commitment())
                .await
            {
                let expired_timer = Instant::now();
                let mut tx = Transaction::new_with_payer(&ixs, Some(&wallet.fee_wallet.pubkey()));

                tx.sign(&[wallet.fee_wallet.as_ref()], hash);

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
                    Ok(_sig) => {
                        info!("Successfully created delegate stake account for: {}", user_pubkey.to_string());
                    }
                    Err(e) => {
                        error!("ERROR: {:?}", e);
                        return Response::builder()
                            .status(StatusCode::INTERNAL_SERVER_ERROR)
                            .body("Failed to init delegate stake account.".to_string())
                            .unwrap();

                    }
                }

            } else {
                error!("Failed to confirm transaction for init delegate stake.");
            }
        }


        let base_ix = ore_miner_delegation::instruction::delegate_stake(user_pubkey, wallet.miner_wallet.pubkey(), query_params.amount);
        let mut accts = Vec::new();
        for account_index in ixs[0].accounts.clone() {
            accts.push(tx.key(0, account_index.into()));
        }

        if ixs[0].data.ne(&base_ix.data) {
            error!("data missmatch");
            return Response::builder()
                .status(StatusCode::BAD_REQUEST)
                .body("Invalid Tx".to_string())
                .unwrap();
        } else {
            info!("Valid stake tx, submitting.");

            let hash = tx.get_recent_blockhash();
            if let Err(_) = tx.try_partial_sign(&[wallet.fee_wallet.as_ref()], *hash) {
                error!("Failed to partially sign tx");
                return Response::builder()
                    .status(StatusCode::INTERNAL_SERVER_ERROR)
                    .body("Server Failed to sign tx.".to_string())
                    .unwrap();
            }

            if !tx.is_signed() {
                error!("Tx missing signer");
                return Response::builder()
                    .status(StatusCode::BAD_REQUEST)
                    .body("Invalid Tx".to_string())
                    .unwrap();
            }

            let result = rpc_client.send_and_confirm_transaction(&tx).await;

            match result {
                Ok(sig) => {
                    let amount_dec = query_params.amount as f64 / 10f64.powf(ORE_TOKEN_DECIMALS as f64);
                    info!("Miner {} successfully delegated stake of {}.\nSig: {}", user_pubkey.to_string(), amount_dec, sig.to_string());
                    return Response::builder()
                        .status(StatusCode::OK)
                        .body("SUCCESS".to_string())
                        .unwrap();
                },
                Err(e) => {
                    error!("{} stake transaction failed...", user_pubkey.to_string());
                    error!("Stake Tx Error: {:?}", e);
                    return Response::builder()
                        .status(StatusCode::INTERNAL_SERVER_ERROR)
                        .body("Failed to send tx".to_string())
                        .unwrap();
                }
            }
        }
    } else {
        error!("stake with invalid pubkey");
        return Response::builder()
            .status(StatusCode::BAD_REQUEST)
            .body("Invalid Pubkey".to_string())
            .unwrap();
    }
}

#[derive(Deserialize)]
struct UnstakeParams {
    pubkey: String,
    amount: u64,
}

async fn post_unstake(
    query_params: Query<UnstakeParams>,
    Extension(rpc_client): Extension<Arc<RpcClient>>,
    Extension(wallet): Extension<Arc<WalletExtension>>,
    body: String,
) -> impl IntoResponse {
    if let Ok(user_pubkey) = Pubkey::from_str(&query_params.pubkey) {
        let serialized_tx = BASE64_STANDARD.decode(body.clone()).unwrap();
        let mut tx: Transaction = if let Ok(tx) = bincode::deserialize(&serialized_tx) {
            tx
        } else {
            error!("Failed to deserialize tx");
            return Response::builder()
                .status(StatusCode::BAD_REQUEST)
                .body("Invalid Tx".to_string())
                .unwrap();
        };

        let ixs = tx.message.instructions.clone();

        if ixs.len() > 1 {
            error!("Too many instructions");
            return Response::builder()
                .status(StatusCode::BAD_REQUEST)
                .body("Invalid Tx".to_string())
                .unwrap();
        }

        if let Err(_) = get_delegated_stake_account(&rpc_client, user_pubkey, wallet.miner_wallet.pubkey()).await {
            error!("Cannot unstake, no delegate stake account is created for {}", user_pubkey.to_string());
            return Response::builder()
                .status(StatusCode::BAD_REQUEST)
                .body("No delegate stake account exists".to_string())
                .unwrap();
        }

        let staker_ata = get_associated_token_address(&user_pubkey, &ore_api::consts::MINT_ADDRESS);

        let base_ix = ore_miner_delegation::instruction::undelegate_stake(user_pubkey, wallet.miner_wallet.pubkey(), staker_ata, query_params.amount);
        let mut accts = Vec::new();
        for account_index in ixs[0].accounts.clone() {
            accts.push(tx.key(0, account_index.into()));
        }

        if ixs[0].data.ne(&base_ix.data) {
            error!("data missmatch");
            return Response::builder()
                .status(StatusCode::BAD_REQUEST)
                .body("Invalid Tx".to_string())
                .unwrap();
        } else {
            info!("Valid unstake tx, submitting.");

            let hash = tx.get_recent_blockhash();
            if let Err(_) = tx.try_partial_sign(&[wallet.fee_wallet.as_ref()], *hash) {
                error!("Failed to partially sign tx");
                return Response::builder()
                    .status(StatusCode::INTERNAL_SERVER_ERROR)
                    .body("Server Failed to sign tx.".to_string())
                    .unwrap();
            }

            if !tx.is_signed() {
                error!("Tx missing signer");
                return Response::builder()
                    .status(StatusCode::BAD_REQUEST)
                    .body("Invalid Tx".to_string())
                    .unwrap();
            }

            let result = rpc_client.send_and_confirm_transaction(&tx).await;

            match result {
                Ok(sig) => {
                    let amount_dec = query_params.amount as f64 / 10f64.powf(ORE_TOKEN_DECIMALS as f64);
                    info!("Miner {} successfully undelegated stake of {}.\nSig: {}", user_pubkey.to_string(), amount_dec, sig.to_string());
                    return Response::builder()
                        .status(StatusCode::OK)
                        .body("SUCCESS".to_string())
                        .unwrap();
                },
                Err(e) => {
                    error!("{} unstake transaction failed...", user_pubkey.to_string());
                    error!("Unstake Tx Error: {:?}", e);
                    return Response::builder()
                        .status(StatusCode::INTERNAL_SERVER_ERROR)
                        .body("Failed to send tx".to_string())
                        .unwrap();
                }
            }
        }
    } else {
        error!("unstake with invalid pubkey");
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
            for (_, app_client_connection) in app_state.read().await.sockets.iter() {
                if user_pubkey == app_client_connection.pubkey {
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
                info!("Client: {addr} connected with pubkey {pubkey}.");
                return Ok(ws.on_upgrade(move |socket| {
                    handle_socket(
                        socket,
                        addr,
                        user_pubkey,
                        miner.id,
                        app_state,
                        client_channel,
                    )
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
    who_miner_id: i32,
    rw_app_state: Arc<RwLock<AppState>>,
    client_channel: UnboundedSender<ClientMessage>,
) {
    if socket
        .send(axum::extract::ws::Message::Ping(vec![1, 2, 3]))
        .await
        .is_ok()
    {
        tracing::debug!("Pinged {who}...");
    } else {
        error!("could not ping {who}");

        // if we can't ping we can't do anything, return to close the connection
        return;
    }

    let (sender, mut receiver) = socket.split();
    let mut app_state = rw_app_state.write().await;
    if app_state.sockets.contains_key(&who) {
        info!("Socket addr: {who} already has an active connection");
        return;
    } else {
        let new_app_client_connection = AppClientConnection {
            pubkey: who_pubkey,
            miner_id: who_miner_id,
            socket: Arc::new(Mutex::new(sender)),
        };
        app_state.sockets.insert(who, new_app_client_connection);
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
        Message::Text(_t) => {
            //println!(">>> {who} sent str: {t:?}");
        }
        Message::Binary(d) => {
            // first 8 bytes are message type
            let message_type = d[0];
            match message_type {
                0 => {
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
                    error!(">>> {} sent an invalid message", who);
                }
            }
        }
        Message::Close(c) => {
            if let Some(cf) = c {
                info!(
                    ">>> {} sent close with code {} and reason `{}`",
                    who, cf.code, cf.reason
                );
            } else {
                info!(">>> {who} somehow sent close message without CloseFrame");
            }
            return ControlFlow::Break(());
        }
        Message::Pong(_v) => {
            let msg = ClientMessage::Pong(who);
            let _ = client_channel.send(msg);
        }
        Message::Ping(_v) => {
            //println!(">>> {who} sent ping with {v:?}");
        }
    }

    ControlFlow::Continue(())
}

async fn proof_tracking_system(ws_url: String, wallet: Arc<Keypair>, proof: Arc<Mutex<Proof>>) {
    loop {
        info!("Establishing rpc websocket connection...");
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
            let account_pubkey = get_proof_pda(app_wallet.pubkey());
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
                            info!("Got new proof data");
                            info!("Challenge: {}", BASE64_STANDARD.encode(new_proof.challenge));
                            // let _ = sender.send(AccountUpdatesData::ProofData(*proof));
                            //
                            {
                                // let mut app_proof = app_proof.lock().await;
                                // *app_proof = *new_proof;
                            }
                        }
                    }
                }
            }
        }
    }
}

async fn pong_tracking_system(
    app_pongs: Arc<RwLock<LastPong>>,
    app_state: Arc<RwLock<AppState>>,
) {
    loop {
        let reader = app_pongs.read().await;
        let pongs = reader.pongs.clone();
        drop(reader);

        for pong in pongs.iter() {
            if pong.1.elapsed().as_secs() > 45 {
                let mut writer = app_state.write().await;
                writer.sockets.remove(pong.0);
                drop(writer);

                let mut writer = app_pongs.write().await;
                writer.pongs.remove(pong.0);
                drop(writer)
            }
        }

        tokio::time::sleep(Duration::from_secs(15)).await;
    }
}

async fn client_message_handler_system(
    mut receiver_channel: UnboundedReceiver<ClientMessage>,
    ready_clients: Arc<Mutex<HashSet<SocketAddr>>>,
    proof: Arc<Mutex<Proof>>,
    epoch_hashes: Arc<RwLock<EpochHashes>>,
    client_nonce_ranges: Arc<RwLock<HashMap<Pubkey, Range<u64>>>>,
    app_state: Arc<RwLock<AppState>>,
    app_pongs: Arc<RwLock<LastPong>>,
    app_submission_window: Arc<RwLock<SubmissionWindow>>,
) {
    while let Some(client_message) = receiver_channel.recv().await {
        match client_message {
            ClientMessage::Pong(addr) => {
                let mut writer = app_pongs.write().await;
                writer.pongs.insert(addr, Instant::now());
                drop(writer);
            }
            ClientMessage::Ready(addr) => {
                let ready_clients = ready_clients.clone();
                tokio::spawn(async move {
                    let mut ready_clients = ready_clients.lock().await;
                    ready_clients.insert(addr);
                });
            }
            ClientMessage::Mining(addr) => {
                info!("Client {} has started mining!", addr.to_string());
            }
            ClientMessage::BestSolution(addr, solution, pubkey) => {
                let app_epoch_hashes = epoch_hashes.clone();
                let app_proof = proof.clone();
                let app_client_nonce_ranges = client_nonce_ranges.clone();
                let app_state = app_state.clone();
                let app_submission_window = app_submission_window.clone();
                tokio::spawn(async move {
                    let epoch_hashes = app_epoch_hashes;
                    let proof = app_proof;
                    let client_nonce_ranges = app_client_nonce_ranges;

                    let reader = app_submission_window.read().await;
                    let submission_windows_closed = reader.closed;
                    drop(reader);

                    if submission_windows_closed {
                        error!("{} submitted after submission window was closed!", pubkey);

                        let reader = app_state.read().await;
                        if let Some(app_client_socket) = reader.sockets.get(&addr) {
                            let msg = format!("Late submission. Please make sure your hash time is under 60 seconds.");
                            let _ = app_client_socket.socket.lock().await.send(Message::Text(msg)).await;
                        } else {
                            error!("Failed to get client socket for addr: {}", addr);
                            return;
                        }
                        drop(reader);
                        return;
                    }

                    let pubkey_str = pubkey.to_string();

                    let reader = client_nonce_ranges.read().await;
                    let nonce_range: Range<u64> = {
                        if let Some(nr) = reader.get(&pubkey) {
                            nr.clone()
                        } else {
                            error!("Client nonce range not set!");
                            return;
                        }
                    };
                    drop(reader);

                    let nonce = u64::from_le_bytes(solution.n);

                    if !nonce_range.contains(&nonce) {
                        error!("Client submitted nonce out of assigned range");
                        return;
                    }

                    let reader = app_state.read().await;
                    let miner_id;
                    if let Some(app_client_socket) = reader.sockets.get(&addr) {
                        miner_id = app_client_socket.miner_id;
                    } else {
                        error!("Failed to get client socket for addr: {}", addr);
                        return;
                    }
                    drop(reader);

                    let lock = proof.lock().await;
                    let challenge = lock.challenge;
                    drop(lock);
                    if solution.is_valid(&challenge) {
                        let diff = solution.to_hash().difficulty();
                        info!("{} found diff: {}", pubkey_str, diff);
                        if diff >= MIN_DIFF {
                            // calculate rewards
                            let mut hashpower = MIN_HASHPOWER * 2u64.pow(diff - MIN_DIFF);
                            if hashpower > 81_920 {
                                hashpower = 81_920;
                            }
                            {
                                let mut epoch_hashes = epoch_hashes.write().await;
                                epoch_hashes
                                    .submissions
                                    .insert(pubkey, InternalMessageSubmission {
                                        miner_id,
                                        supplied_nonce: nonce,
                                        supplied_diff: diff,
                                        hashpower,
                                    }
                                );
                                if diff > epoch_hashes.best_hash.difficulty {
                                    info!("New best diff: {}", diff);
                                    epoch_hashes.best_hash.difficulty = diff;
                                    epoch_hashes.best_hash.solution = Some(solution);
                                }
                                drop(epoch_hashes);
                            }
                        } else {
                            error!("Diff to low, skipping");
                        }
                    } else {
                        error!("{} returned an invalid solution!", pubkey);

                        let reader = app_state.read().await;
                        if let Some(app_client_socket) = reader.sockets.get(&addr) {
                            let _ = app_client_socket.socket.lock().await.send(Message::Text("Invalid solution. If this keeps happening, please contact support.".to_string())).await;
                        } else {
                            error!("Failed to get client socket for addr: {}", addr);
                            return;
                        }
                        drop(reader);
                    }
                });
            }
        }
    }
}

async fn ping_check_system(shared_state: &Arc<RwLock<AppState>>) {
    loop {
        // send ping to all sockets
        let app_state = shared_state.read().await;

        let mut handles = Vec::new();
        for (who, socket) in app_state.sockets.iter() {
            let who = who.clone();
            let socket = socket.clone();
            handles.push(tokio::spawn(async move {
                if socket
                    .socket
                    .lock()
                    .await
                    .send(Message::Ping(vec![1, 2, 3]))
                    .await
                    .is_ok()
                {
                    return None;
                } else {
                    return Some(who.clone());
                }
            }));
        }
        drop(app_state);

        // remove any sockets where ping failed
        for handle in handles {
            match handle.await {
                Ok(Some(who)) => {
                    let mut app_state = shared_state.write().await;
                    app_state.sockets.remove(&who);
                }
                Ok(None) => {}
                Err(_) => {
                    error!("Got error sending ping to client.");
                }
            }
        }

        tokio::time::sleep(Duration::from_secs(30)).await;
    }
}
