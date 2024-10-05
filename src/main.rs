use std::{
    collections::{HashMap, HashSet},
    net::SocketAddr,
    ops::{ControlFlow, Div},
    path::Path,
    str::FromStr,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use systems::{
    claim_system::claim_system, client_message_handler_system::client_message_handler_system,
    handle_ready_clients_system::handle_ready_clients_system,
    pong_tracking_system::pong_tracking_system, proof_tracking_system::proof_tracking_system,
};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, Layer};

use crate::{
    ore_utils::{get_managed_proof_token_ata, get_proof_pda},
    systems::{message_text_all_clients_system::message_text_all_clients_system, pool_mine_success_system::pool_mine_success_system, pool_submission_system::pool_submission_system},
};

use self::models::*;
use app_database::{AppDatabase, AppDatabaseError};
use app_rr_database::AppRRDatabase;
use axum::{
    extract::{
        ws::{Message, WebSocket},
        ConnectInfo, Query, State, WebSocketUpgrade,
    },
    http::{Method, Response, StatusCode},
    response::IntoResponse,
    routing::{get, post},
    Extension, Json, Router,
};
use axum_extra::{headers::authorization::Basic, TypedHeader};
use base64::{prelude::BASE64_STANDARD, Engine};
use clap::Parser;
use drillx::Solution;
use futures::{stream::SplitSink, SinkExt, StreamExt};
use ore_utils::{
    get_config, get_delegated_stake_account, get_ore_mint,
    get_original_proof, get_proof, get_register_ix,
    ORE_TOKEN_DECIMALS,
};
use routes::{get_challenges, get_latest_mine_txn, get_pool_balance};
use serde::Deserialize;
use solana_client::{
    nonblocking::rpc_client::RpcClient,
    rpc_client::SerializableTransaction,
    rpc_config::RpcSendTransactionConfig,
};
use solana_sdk::{
    commitment_config::{CommitmentConfig, CommitmentLevel}, compute_budget::ComputeBudgetInstruction, native_token::{lamports_to_sol, sol_to_lamports, LAMPORTS_PER_SOL}, pubkey::Pubkey, signature::{read_keypair_file, Keypair, Signature}, signer::Signer, system_instruction, transaction::Transaction
};
use solana_transaction_status::TransactionConfirmationStatus;
use spl_associated_token_account::{
    get_associated_token_address, instruction::create_associated_token_account,
};
use tokio::{
    io::AsyncReadExt,
    sync::{mpsc::UnboundedSender, Mutex, RwLock},
    time::Instant,
};
use tower_http::{
    cors::CorsLayer,
    trace::{DefaultMakeSpan, TraceLayer},
};
use tracing::{error, info};

mod app_database;
mod app_rr_database;
mod message;
mod models;
mod proof_migration;
mod routes;
mod schema;
mod systems;

const MIN_DIFF: u32 = 8;
const MIN_HASHPOWER: u64 = 5;

#[derive(Clone)]
enum ClientVersion {
    V1,
    V2,
}

#[derive(Clone)]
struct AppClientConnection {
    pubkey: Pubkey,
    miner_id: i32,
    client_version: ClientVersion,
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

#[derive(Clone, Copy)]
struct ClaimsQueueItem {
    receiver_pubkey: Pubkey,
    amount: u64
}

struct ClaimsQueue {
    queue: RwLock<HashMap<Pubkey, ClaimsQueueItem>>,
}

struct SubmissionWindow {
    closed: bool,
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
    pongs: HashMap<SocketAddr, Instant>,
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
    signup_fee: f64,
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
        value_name = "signup fee",
        help = "Amount of sol users must send to sign up for the pool",
        default_value = "0.001",
        global = true
    )]
    signup_fee: f64,
    #[arg(long, short, action, help = "Enable stats endpoints")]
    stats: bool,
    #[arg(
        long,
        short,
        action,
        help = "Migrate balance from original proof to delegate stake managed proof"
    )]
    migrate: bool,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    dotenv::dotenv().ok();
    let args = Args::parse();

    let server_logs = tracing_appender::rolling::daily("./logs", "ore-hq-server.log");
    let (server_logs, _guard) = tracing_appender::non_blocking(server_logs);
    let server_log_layer = tracing_subscriber::fmt::layer()
        .with_writer(server_logs)
        .with_filter(tracing_subscriber::filter::filter_fn(|metadata| {
            metadata.target() == "server_log"
        }));

    let submission_logs = tracing_appender::rolling::daily("./logs", "ore-hq-submissions.log");
    let (submission_logs, _guard) = tracing_appender::non_blocking(submission_logs);
    let submission_log_layer = tracing_subscriber::fmt::layer()
        .with_writer(submission_logs)
        .with_filter(tracing_subscriber::filter::filter_fn(|metadata| {
            metadata.target() == "submission_log"
        }));

    tracing_subscriber::registry()
        .with(server_log_layer)
        .with(submission_log_layer)
        .init();

    // load envs
    let wallet_path_str = std::env::var("WALLET_PATH").expect("WALLET_PATH must be set.");
    let fee_wallet_path_str =
        std::env::var("FEE_WALLET_PATH").expect("FEE_WALLET_PATH must be set.");
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
                        error!(target: "server_log", err);
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
        tracing::error!(target: "server_log", "Failed to load wallet at: {}", wallet_path_str);
        return Err("Failed to find wallet path.".into());
    }

    let wallet = read_keypair_file(wallet_path)
        .expect("Failed to load keypair from file: {wallet_path_str}");
    info!(target: "server_log", "loaded wallet {}", wallet.pubkey().to_string());

    let wallet_path = Path::new(&fee_wallet_path_str);

    if !wallet_path.exists() {
        tracing::error!(target: "server_log", "Failed to load fee wallet at: {}", fee_wallet_path_str);
        return Err("Failed to find fee wallet path.".into());
    }

    let fee_wallet = read_keypair_file(wallet_path)
        .expect("Failed to load keypair from file: {wallet_path_str}");
    info!(target: "server_log", "loaded fee wallet {}", wallet.pubkey().to_string());

    info!(target: "server_log", "establishing rpc connection...");
    let rpc_client = RpcClient::new_with_commitment(rpc_url, CommitmentConfig::confirmed());
    let jito_url = "https://mainnet.block-engine.jito.wtf/api/v1/transactions".to_string();
    let jito_client = RpcClient::new(jito_url);

    info!(target: "server_log", "loading sol balance...");
    let balance = if let Ok(balance) = rpc_client.get_balance(&wallet.pubkey()).await {
        balance
    } else {
        return Err("Failed to load balance".into());
    };

    info!(target: "server_log", "Balance: {:.2}", balance as f64 / LAMPORTS_PER_SOL as f64);

    if balance < 1_000_000 {
        return Err("Sol balance is too low!".into());
    }

    let proof = if let Ok(loaded_proof) = get_proof(&rpc_client, wallet.pubkey()).await {
        info!(target: "server_log", "LOADED PROOF: \n{:?}", loaded_proof);
        loaded_proof
    } else {
        error!(target: "server_log", "Failed to load proof.");
        info!(target: "server_log", "Creating proof account...");

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
                info!(target: "server_log", "Sig: {}", sig.to_string());
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

    info!(target: "server_log", "Validating miners delegate stake account is created");
    match get_delegated_stake_account(&rpc_client, wallet.pubkey(), wallet.pubkey()).await {
        Ok(data) => {
            info!(target: "server_log", "Miner Delegated Stake Account: {:?}", data);
            info!(target: "server_log", "Miner delegate stake account already created.");
        }
        Err(_) => {
            info!(target: "server_log", "Creating miner delegate stake account");
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

            match rpc_client
                .send_and_confirm_transaction_with_spinner_and_commitment(
                    &tx,
                    CommitmentConfig {
                        commitment: CommitmentLevel::Confirmed,
                    },
                )
                .await
            {
                Ok(_) => {
                    info!(target: "server_log", "Successfully created miner delegate stake account");
                }
                Err(_) => {
                    error!(target: "server_log", "Failed to send and confirm tx.");
                    panic!("Failed to create miner delegate stake account");
                }
            }
        }
    }

    info!(target: "server_log", "Validating managed proof token account is created");
    let managed_proof = Pubkey::find_program_address(
        &[b"managed-proof-account", wallet.pubkey().as_ref()],
        &ore_miner_delegation::id(),
    );

    let managed_proof_token_account_addr = get_managed_proof_token_ata(wallet.pubkey());
    match rpc_client
        .get_token_account_balance(&managed_proof_token_account_addr)
        .await
    {
        Ok(_) => {
            info!(target: "server_log", "Managed proof token account already created.");
        }
        Err(_) => {
            info!(target: "server_log", "Creating managed proof token account");
            let ix = create_associated_token_account(
                &wallet.pubkey(),
                &managed_proof.0,
                &ore_api::consts::MINT_ADDRESS,
                &spl_token::id(),
            );

            let mut tx = Transaction::new_with_payer(&[ix], Some(&wallet.pubkey()));

            let blockhash = rpc_client
                .get_latest_blockhash()
                .await
                .expect("should get latest blockhash");

            tx.sign(&[&wallet], blockhash);

            match rpc_client
                .send_and_confirm_transaction_with_spinner_and_commitment(
                    &tx,
                    CommitmentConfig {
                        commitment: CommitmentLevel::Confirmed,
                    },
                )
                .await
            {
                Ok(_) => {
                    info!(target: "server_log", "Successfully created managed proof token account");
                }
                Err(e) => {
                    error!(target: "server_log", "Failed to send and confirm tx.\nE: {:?}", e);
                    panic!("Failed to create managed proof token account");
                }
            }
        }
    }

    let miner_ore_token_account_addr =
        get_associated_token_address(&wallet.pubkey(), &ore_api::consts::MINT_ADDRESS);
    let token_balance = if let Ok(token_balance) = rpc_client
        .get_token_account_balance(&miner_ore_token_account_addr)
        .await
    {
        let bal = token_balance.ui_amount.unwrap() * 10f64.powf(token_balance.decimals as f64);
        bal as u64
    } else {
        error!(target: "server_log", "Failed to get miner ORE token account balance");
        panic!("Failed to get ORE token account balance.");
    };

    if args.migrate {
        info!(target: "server_log", "Checking original proof, and token account balances for migration.");
        let original_proof =
            if let Ok(loaded_proof) = get_original_proof(&rpc_client, wallet.pubkey()).await {
                loaded_proof
            } else {
                panic!("Failed to get original proof!");
            };
        if original_proof.balance > 0 || token_balance > 0 {
            info!(target: "server_log", "Proof balance has {} tokens. Miner ORE token account has {} tokens.\nMigrating...", original_proof.balance, token_balance);
            if let Err(e) = proof_migration::migrate(
                &rpc_client,
                &wallet,
                original_proof.balance,
                token_balance,
            )
            .await
            {
                info!(target: "server_log", "Failed to migrate proof balance.\nError: {}", e);
                panic!("Failed to migrate proof balance.");
            } else {
                info!(target: "server_log", "Successfully migrated proof balance");
            }
        } else {
            info!(target: "server_log", "Balances are 0, nothing to migrate.");
        }
    }

    info!(target: "server_log", "Validating pool exists in db");
    let db_pool = app_database
        .get_pool_by_authority_pubkey(wallet.pubkey().to_string())
        .await;

    match db_pool {
        Ok(_) => {}
        Err(AppDatabaseError::FailedToGetConnectionFromPool) => {
            panic!("Failed to get database pool connection");
        }
        Err(_) => {
            info!(target: "server_log", "Pool missing from database. Inserting...");
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

    info!(target: "server_log", "Validating current challenge for pool exists in db");
    let result = app_database
        .get_challenge_by_challenge(proof.challenge.to_vec())
        .await;

    match result {
        Ok(_) => {}
        Err(AppDatabaseError::FailedToGetConnectionFromPool) => {
            panic!("Failed to get database pool connection");
        }
        Err(_) => {
            info!(target: "server_log", "Challenge missing from database. Inserting...");
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
        signup_fee: args.signup_fee,
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

    let pongs = Arc::new(RwLock::new(LastPong {
        pongs: HashMap::new(),
    }));

    let claims_queue = Arc::new(ClaimsQueue {
        queue: RwLock::new(HashMap::new()),
    });

    let submission_window = Arc::new(RwLock::new(SubmissionWindow { closed: false }));

    let rpc_client = Arc::new(rpc_client);
    let jito_client = Arc::new(jito_client);

    let last_challenge = Arc::new(Mutex::new([0u8; 32]));

    let app_rpc_client = rpc_client.clone();
    let app_wallet = wallet_extension.clone();
    let app_claims_queue = claims_queue.clone();
    let app_app_database = app_database.clone();
    tokio::spawn(async move {
        claim_system(
            app_claims_queue,
            app_rpc_client,
            app_wallet.miner_wallet.clone(),
            app_app_database,
        )
        .await;
    });

    // Track client pong timings
    let app_pongs = pongs.clone();
    let app_state = shared_state.clone();
    tokio::spawn(async move {
        pong_tracking_system(app_pongs, app_state).await;
    });

    let app_wallet = wallet_extension.clone();
    let app_proof = proof_ext.clone();
    let app_last_challenge = last_challenge.clone();
    // Establish webocket connection for tracking pool proof changes.
    tokio::spawn(async move {
        proof_tracking_system(
            rpc_ws_url,
            app_wallet.miner_wallet.clone(),
            app_proof,
            app_last_challenge
        ).await;
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
            app_submission_window,
        )
        .await;
    });

    // Handle ready clients
    let app_shared_state = shared_state.clone();
    let app_proof = proof_ext.clone();
    let app_epoch_hashes = epoch_hashes.clone();
    let app_nonce = nonce_ext.clone();
    let app_client_nonce_ranges = client_nonce_ranges.clone();
    let app_ready_clients = ready_clients.clone();
    let app_submission_window = submission_window.clone();
    tokio::spawn(async move {
        handle_ready_clients_system(
            app_shared_state,
            app_proof,
            app_epoch_hashes,
            app_ready_clients,
            app_nonce,
            app_client_nonce_ranges,
            app_submission_window,
        )
        .await;
    });

    let (mine_success_sender, mine_success_receiver) =
        tokio::sync::mpsc::unbounded_channel::<MessageInternalMineSuccess>();

    let (all_clients_sender, all_clients_receiver) =
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
    let app_client_nonce_ranges = client_nonce_ranges.clone();
    let app_last_challenge = last_challenge.clone();
    tokio::spawn(async move {
        pool_submission_system(
            app_proof,
            app_epoch_hashes,
            app_wallet,
            app_nonce,
            app_prio_fee,
            app_jito_tip,
            app_rpc_client,
            app_jito_client,
            app_config,
            app_app_database,
            app_all_clients_sender,
            mine_success_sender,
            app_submission_window,
            app_client_nonce_ranges,
            app_last_challenge,
        )
        .await;
    });

    let app_shared_state = shared_state.clone();
    let app_app_database = app_database.clone();
    let app_config = config.clone();
    let app_wallet = wallet_extension.clone();
    tokio::spawn(async move {
        let app_database = app_app_database;
        pool_mine_success_system(
            app_shared_state,
            app_database,
            app_config,
            app_wallet,
            mine_success_receiver,
        ).await;
    });

    let app_shared_state = shared_state.clone();
    tokio::spawn(async move {
        message_text_all_clients_system(
            app_shared_state,
            all_clients_receiver
        ).await;
    });

    let cors = CorsLayer::new()
        .allow_methods([Method::GET])
        .allow_origin(tower_http::cors::Any);

    let client_channel = client_message_sender.clone();
    let app_shared_state = shared_state.clone();
    let app = Router::new()
        .route("/", get(ws_handler))
        .route("/v2/ws", get(ws_handler_v2))
        .route("/pause", post(post_pause))
        .route("/latest-blockhash", get(get_latest_blockhash))
        .route("/pool/authority/pubkey", get(get_pool_authority_pubkey))
        .route("/pool/fee_payer/pubkey", get(get_pool_fee_payer_pubkey))
        .route("/signup", post(post_signup))
        .route("/v2/signup", post(post_signup_v2))
        .route("/signup-fee", get(get_signup_fee))
        .route("/sol-balance", get(get_sol_balance))
        .route("/claim", post(post_claim))
        .route("/v2/claim", post(post_claim_v2))
        .route("/stake", post(post_stake))
        .route("/unstake", post(post_unstake))
        .route("/active-miners", get(get_connected_miners))
        .route("/timestamp", get(get_timestamp))
        .route("/miner/balance", get(get_miner_balance))
        .route("/miner/stake", get(get_miner_stake))
        .route("/stake-multiplier", get(get_stake_multiplier))
        // App RR Database routes
        .route(
            "/last-challenge-submissions",
            get(get_last_challenge_submissions),
        )
        .route("/miner/rewards", get(get_miner_rewards))
        .route("/miner/submissions", get(get_miner_submissions))
        .route("/miner/last-claim", get(get_miner_last_claim))
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

    tracing::info!(target: "server_log", "listening on {}", listener.local_addr().unwrap());

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
                    info!(target: "server_log", "Miner account already enabled!");
                    return Response::builder()
                        .status(StatusCode::OK)
                        .header("Content-Type", "text/text")
                        .body("EXISTS".to_string())
                        .unwrap();
                }
            }
            Err(AppDatabaseError::FailedToGetConnectionFromPool) => {
                error!(target: "server_log", "Failed to get database pool connection");
                return Response::builder()
                    .status(StatusCode::INTERNAL_SERVER_ERROR)
                    .body("Failed to get db pool connection".to_string())
                    .unwrap();
            }
            Err(_) => {
                info!(target: "server_log", "No miner account exists. Signing up new user.");
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
                        error!(target: "server_log", "Failed to add miner rewards tracker to database");
                        return Response::builder()
                            .status(StatusCode::INTERNAL_SERVER_ERROR)
                            .body("Failed to add miner rewards tracker to database".to_string())
                            .unwrap();
                    }
                } else {
                    error!(target: "server_log", "Failed to add miner to database");
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
            error!(target: "server_log", "Failed to deserialize tx");
            return Response::builder()
                .status(StatusCode::BAD_REQUEST)
                .body("Invalid Tx".to_string())
                .unwrap();
        };

        if !tx.is_signed() {
            error!(target: "server_log", "Tx missing signer");
            return Response::builder()
                .status(StatusCode::BAD_REQUEST)
                .body("Invalid Tx".to_string())
                .unwrap();
        }

        let ixs = tx.message.instructions.clone();

        if ixs.len() > 1 {
            error!(target: "server_log", "Too many instructions");
            return Response::builder()
                .status(StatusCode::BAD_REQUEST)
                .body("Invalid Tx".to_string())
                .unwrap();
        }


        let signup_fee = sol_to_lamports(app_config.signup_fee);
        let base_ix =
            system_instruction::transfer(&user_pubkey, &wallet.miner_wallet.pubkey(), signup_fee);
        let mut accts = Vec::new();
        for account_index in ixs[0].accounts.clone() {
            accts.push(tx.key(0, account_index.into()));
        }

        if accts.len() != 2 {
            error!(target: "server_log", "too many accts");
            return Response::builder()
                .status(StatusCode::BAD_REQUEST)
                .body("Invalid Tx".to_string())
                .unwrap();
        }

        if ixs[0].data.ne(&base_ix.data) {
            error!(target: "server_log", "data missmatch");
            return Response::builder()
                .status(StatusCode::BAD_REQUEST)
                .body("Invalid Tx".to_string())
                .unwrap();
        } else {
            info!(target: "server_log", "Valid signup tx, submitting.");

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
                            error!(target: "server_log", "Failed to add miner rewards tracker to database");
                            return Response::builder()
                                .status(StatusCode::INTERNAL_SERVER_ERROR)
                                .body("Failed to add miner rewards tracker to database".to_string())
                                .unwrap();
                        }
                    } else {
                        error!(target: "server_log", "Failed to add miner to database");
                        return Response::builder()
                            .status(StatusCode::INTERNAL_SERVER_ERROR)
                            .body("Failed to add user to database".to_string())
                            .unwrap();
                    }
                }
                Err(e) => {
                    error!(target: "server_log", "{} signup transaction failed...", user_pubkey.to_string());
                    error!(target: "server_log", "Signup Tx Error: {:?}", e);
                    return Response::builder()
                        .status(StatusCode::INTERNAL_SERVER_ERROR)
                        .body("Failed to send tx".to_string())
                        .unwrap();
                }
            }
        }
    } else {
        error!(target: "server_log", "Signup with invalid pubkey");
        return Response::builder()
            .status(StatusCode::BAD_REQUEST)
            .body("Invalid Pubkey".to_string())
            .unwrap();
    }
}

#[derive(Deserialize)]
struct SignupParamsV2 {
    miner: String,
    fee_payer: String
}

async fn post_signup_v2(
    query_params: Query<SignupParamsV2>,
    Extension(app_database): Extension<Arc<AppDatabase>>,
    Extension(rpc_client): Extension<Arc<RpcClient>>,
    Extension(wallet): Extension<Arc<WalletExtension>>,
    Extension(app_config): Extension<Arc<Config>>,
    body: String,
) -> impl IntoResponse {
    let fee_payer_pubkey = match Pubkey::from_str(&query_params.fee_payer) {
        Ok(pk) => {
            pk
        },
        Err(_) => {
            error!(target: "server_log", "SignupV2 with invalid fee_payer pubkey");
            return Response::builder()
                .status(StatusCode::BAD_REQUEST)
                .body("Invalid fee_payer pubkey".to_string())
                .unwrap();
        }
    };
    if let Ok(miner_pubkey) = Pubkey::from_str(&query_params.miner) {
        let db_miner = app_database
            .get_miner_by_pubkey_str(miner_pubkey.to_string())
            .await;

        match db_miner {
            Ok(miner) => {
                if miner.enabled {
                    info!(target: "server_log", "Miner account already enabled!");
                    return Response::builder()
                        .status(StatusCode::OK)
                        .header("Content-Type", "text/text")
                        .body("EXISTS".to_string())
                        .unwrap();
                }
            }
            Err(AppDatabaseError::FailedToGetConnectionFromPool) => {
                error!(target: "server_log", "Failed to get database pool connection");
                return Response::builder()
                    .status(StatusCode::INTERNAL_SERVER_ERROR)
                    .body("Failed to get db pool connection".to_string())
                    .unwrap();
            }
            Err(_) => {
                info!(target: "server_log", "No miner account exists. Signing up new user.");
            }
        }

        if let Some(whitelist) = &app_config.whitelist {
            if whitelist.contains(&miner_pubkey) {
                let result = app_database
                    .add_new_miner(miner_pubkey.to_string(), true)
                    .await;
                let miner = app_database
                    .get_miner_by_pubkey_str(miner_pubkey.to_string())
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
                        error!(target: "server_log", "Failed to add miner rewards tracker to database");
                        return Response::builder()
                            .status(StatusCode::INTERNAL_SERVER_ERROR)
                            .body("Failed to add miner rewards tracker to database".to_string())
                            .unwrap();
                    }
                } else {
                    error!(target: "server_log", "Failed to add miner to database");
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
            error!(target: "server_log", "Failed to deserialize tx");
            return Response::builder()
                .status(StatusCode::BAD_REQUEST)
                .body("Invalid Tx".to_string())
                .unwrap();
        };

        if !tx.is_signed() {
            error!(target: "server_log", "Tx missing signer");
            return Response::builder()
                .status(StatusCode::BAD_REQUEST)
                .body("Invalid Tx".to_string())
                .unwrap();
        }

        let ixs = tx.message.instructions.clone();

        if ixs.len() > 1 {
            error!(target: "server_log", "Too many instructions");
            return Response::builder()
                .status(StatusCode::BAD_REQUEST)
                .body("Invalid Tx".to_string())
                .unwrap();
        }


        let signup_fee = sol_to_lamports(app_config.signup_fee);
        let base_ix =
            system_instruction::transfer(&fee_payer_pubkey, &wallet.miner_wallet.pubkey(), signup_fee);
        let mut accts = Vec::new();
        for account_index in ixs[0].accounts.clone() {
            accts.push(tx.key(0, account_index.into()));
        }

        if accts.len() != 2 {
            error!(target: "server_log", "too many accts");
            return Response::builder()
                .status(StatusCode::BAD_REQUEST)
                .body("Invalid Tx".to_string())
                .unwrap();
        }

        if ixs[0].data.ne(&base_ix.data) {
            error!(target: "server_log", "data missmatch");
            return Response::builder()
                .status(StatusCode::BAD_REQUEST)
                .body("Invalid Tx".to_string())
                .unwrap();
        } else {
            info!(target: "server_log", "Valid signup tx, submitting.");

            let result = rpc_client.send_and_confirm_transaction(&tx).await;

            match result {
                Ok(_sig) => {
                    let res = app_database
                        .add_new_miner(miner_pubkey.to_string(), true)
                        .await;
                    let miner = app_database
                        .get_miner_by_pubkey_str(miner_pubkey.to_string())
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
                            error!(target: "server_log", "Failed to add miner rewards tracker to database");
                            return Response::builder()
                                .status(StatusCode::INTERNAL_SERVER_ERROR)
                                .body("Failed to add miner rewards tracker to database".to_string())
                                .unwrap();
                        }
                    } else {
                        error!(target: "server_log", "Failed to add miner to database");
                        return Response::builder()
                            .status(StatusCode::INTERNAL_SERVER_ERROR)
                            .body("Failed to add user to database".to_string())
                            .unwrap();
                    }
                }
                Err(e) => {
                    error!(target: "server_log", "{} signup transaction failed...", miner_pubkey.to_string());
                    error!(target: "server_log", "Signup Tx Error: {:?}", e);
                    return Response::builder()
                        .status(StatusCode::INTERNAL_SERVER_ERROR)
                        .body("Failed to send tx".to_string())
                        .unwrap();
                }
            }
        }
    } else {
        error!(target: "server_log", "Signup with invalid miner_pubkey");
        return Response::builder()
            .status(StatusCode::BAD_REQUEST)
            .body("Invalid miner pubkey".to_string())
            .unwrap();
    }
}

async fn get_signup_fee(
    Extension(app_config): Extension<Arc<Config>>,
) -> impl IntoResponse {
    return Response::builder()
        .status(StatusCode::OK)
        .body(app_config.signup_fee.to_string())
        .unwrap();
}

async fn get_sol_balance(
    Extension(rpc_client): Extension<Arc<RpcClient>>,
    query_params: Query<PubkeyParam>,
) -> impl IntoResponse {
    if let Ok(user_pubkey) = Pubkey::from_str(&query_params.pubkey) {
        let res = rpc_client.get_balance(&user_pubkey).await;

        match res {
            Ok(balance) => {
                let response = format!("{}", lamports_to_sol(balance));
                return Response::builder()
                    .status(StatusCode::OK)
                    .body(response)
                    .unwrap();
            }
            Err(_) => {
                error!(target: "server_log", "get_sol_balance: failed to get sol balance for {}", user_pubkey.to_string());
                return Response::builder()
                    .status(StatusCode::INTERNAL_SERVER_ERROR)
                    .body("Failed to get sol balance".to_string())
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
                error!(target: "server_log", "get_miner_rewards: failed to get rewards balance from db for {}", user_pubkey.to_string());
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
        let res = app_rr_database.get_last_challenge_submissions().await;

        match res {
            Ok(submissions) => Ok(Json(submissions)),
            Err(_) => Err("Failed to get submissions for miner".to_string()),
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
                Ok(submissions) => Ok(Json(submissions)),
                Err(_) => Err("Failed to get submissions for miner".to_string()),
            }
        } else {
            Err("Invalid public key".to_string())
        }
    } else {
        return Err("Stats not enabled for this server.".to_string());
    }
}

#[derive(Deserialize)]
struct GetLastClaimParams {
    pubkey: String,
}

async fn get_miner_last_claim(
    query_params: Query<GetLastClaimParams>,
    Extension(app_rr_database): Extension<Arc<AppRRDatabase>>,
    Extension(app_config): Extension<Arc<Config>>,
) -> Result<Json<LastClaim>, String> {
    if app_config.stats_enabled {
        if let Ok(user_pubkey) = Pubkey::from_str(&query_params.pubkey) {
            let res = app_rr_database
                .get_last_claim_by_pubkey(user_pubkey.to_string())
                .await;

            match res {
                Ok(last_claim) => Ok(Json(last_claim)),
                Err(_) => Err("Failed to get last claim for miner".to_string()),
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
        if let Ok(account) =
            get_delegated_stake_account(&rpc_client, user_pubkey, wallet.miner_wallet.pubkey())
                .await
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
            error!(target: "server_log", "get_pool_staked: Failed to load proof.");
            return Err("Stats not enabled for this server.".to_string());
        };

        if let Ok(config) = get_config(&rpc_client).await {
            let multiplier = 1.0 + (proof.balance as f64 / config.top_balance as f64).min(1.0f64);
            return Ok(Json(multiplier));
        } else {
            return Err("Failed to get ore config account".to_string());
        }
    } else {
        return Err("Stats not enabled for this server.".to_string());
    }
}

#[derive(Deserialize)]
struct ConnectedMinersParams {
    pubkey: Option<String>,
}

async fn get_connected_miners(
    query_params: Query<ConnectedMinersParams>,
    State(app_state): State<Arc<RwLock<AppState>>>,
) -> impl IntoResponse {
    let reader = app_state.read().await;
    let socks = reader.sockets.clone();
    drop(reader);

    if let Some(pubkey_str) = &query_params.pubkey {
        if let Ok(user_pubkey) = Pubkey::from_str(&pubkey_str) {
            let mut connection_count = 0;

            for (_addr, client_connection) in socks.iter() {
                if user_pubkey.eq(&client_connection.pubkey) {
                    connection_count += 1;
                }
            }

            return Response::builder()
                .status(StatusCode::OK)
                .body(connection_count.to_string())
                .unwrap();
        } else {
            error!(target: "server_log", "Get connected miners with invalid pubkey");
            return Response::builder()
                .status(StatusCode::BAD_REQUEST)
                .body("Invalid Pubkey".to_string())
                .unwrap();
        }
    } else {
        return Response::builder()
            .status(StatusCode::OK)
            .body(socks.len().to_string())
            .unwrap();
    }
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
    if let Ok(miner_pubkey) = Pubkey::from_str(&query_params.pubkey) {
        let reader = claims_queue.queue.read().await;
        let queue = reader.clone();
        drop(reader);

        if queue.contains_key(&miner_pubkey) {
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
            .get_miner_rewards(miner_pubkey.to_string())
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
                if time_difference <= 1800 {
                    return Response::builder()
                        .status(StatusCode::TOO_MANY_REQUESTS)
                        .body(time_difference.to_string())
                        .unwrap();
                }
            }

            let mut writer = claims_queue.queue.write().await;
            writer.insert(miner_pubkey, ClaimsQueueItem{
                receiver_pubkey: miner_pubkey,
                amount,
            });
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
        error!(target: "server_log", "Claim with invalid pubkey");
        return Response::builder()
            .status(StatusCode::BAD_REQUEST)
            .body("Invalid Pubkey".to_string())
            .unwrap();
    }
}

#[derive(Deserialize)]
struct ClaimParamsV2 {
    timestamp: u64,
    receiver_pubkey: String,
    amount: u64,
}

async fn post_claim_v2(
    TypedHeader(auth_header): TypedHeader<axum_extra::headers::Authorization<Basic>>,
    Extension(app_database): Extension<Arc<AppDatabase>>,
    Extension(claims_queue): Extension<Arc<ClaimsQueue>>,
    query_params: Query<ClaimParamsV2>,
) -> impl IntoResponse {
    let msg_timestamp = query_params.timestamp;

    let miner_pubkey_str = auth_header.username();
    let signed_msg = auth_header.password();

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards")
        .as_secs();

    // Signed authentication message is only valid for 30 seconds
    if (now - msg_timestamp) >= 30 {
        return Err((StatusCode::UNAUTHORIZED, "Timestamp too old.".to_string()));
    }
    let receiver_pubkey = match Pubkey::from_str(&query_params.receiver_pubkey) {
        Ok(pubkey) => {
            pubkey
        },
        Err(_) => {
            return Err((StatusCode::BAD_REQUEST, "Invalid receiver_pubkey provided.".to_string()))
        }
    };

    if let Ok(miner_pubkey) = Pubkey::from_str(miner_pubkey_str) {
        if let Ok(signature) = Signature::from_str(signed_msg) {
            let amount = query_params.amount;
            let mut signed_msg = vec![];
            signed_msg.extend(msg_timestamp.to_le_bytes());
            signed_msg.extend(receiver_pubkey.to_bytes());
            signed_msg.extend(amount.to_le_bytes());

            if signature.verify(&miner_pubkey.to_bytes(), &signed_msg) {

                let reader = claims_queue.queue.read().await;
                let queue = reader.clone();
                drop(reader);

                if queue.contains_key(&miner_pubkey) {
                    return Err((StatusCode::TOO_MANY_REQUESTS, "QUEUED".to_string()));
                }

                let amount = query_params.amount;

                // 0.00500000000
                if amount < 500_000_000 {
                    return Err((StatusCode::BAD_REQUEST, "claim minimum is 0.005".to_string()));
                }

                if let Ok(miner_rewards) = app_database
                    .get_miner_rewards(miner_pubkey.to_string())
                    .await
                {
                    if amount > miner_rewards.balance {
                        return Err((StatusCode::BAD_REQUEST, "claim amount exceeds miner rewards balance.".to_string()));
                    }

                    if let Ok(last_claim) = app_database.get_last_claim(miner_rewards.miner_id).await {
                        let last_claim_ts = last_claim.created_at.and_utc().timestamp();
                        let now = SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .expect("Time went backwards")
                            .as_secs() as i64;
                        let time_difference = now - last_claim_ts;
                        if time_difference <= 1800 {
                            return Err((StatusCode::TOO_MANY_REQUESTS, time_difference.to_string()));
                        }
                    }

                    let mut writer = claims_queue.queue.write().await;
                    writer.insert(miner_pubkey, ClaimsQueueItem{
                        receiver_pubkey,
                        amount,
                    });
                    drop(writer);
                    return Ok((StatusCode::OK, "SUCCESS"));
                } else {
                    return Err((StatusCode::INTERNAL_SERVER_ERROR, "failed to get miner account from database".to_string()));
                }
            } else {
                return Err((StatusCode::UNAUTHORIZED, "Sig verification failed".to_string()));
            }
        } else {
            return Err((StatusCode::UNAUTHORIZED, "Invalid signature".to_string()));
        }
    } else {
        error!(target: "server_log", "Claim with invalid pubkey");
        return Err((StatusCode::BAD_REQUEST, "Invalid Pubkey".to_string()));
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
            error!(target: "server_log", "Failed to deserialize tx");
            return Response::builder()
                .status(StatusCode::BAD_REQUEST)
                .body("Invalid Tx".to_string())
                .unwrap();
        };

        let ixs = tx.message.instructions.clone();

        if ixs.len() > 1 {
            error!(target: "server_log", "Too many instructions");
            return Response::builder()
                .status(StatusCode::BAD_REQUEST)
                .body("Invalid Tx".to_string())
                .unwrap();
        }

        if let Err(_) =
            get_delegated_stake_account(&rpc_client, user_pubkey, wallet.miner_wallet.pubkey())
                .await
        {
            let init_ix = ore_miner_delegation::instruction::init_delegate_stake(
                user_pubkey,
                wallet.miner_wallet.pubkey(),
                wallet.fee_wallet.pubkey(),
            );

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

                let rpc_config = RpcSendTransactionConfig {
                    preflight_commitment: Some(rpc_client.commitment().commitment),
                    ..RpcSendTransactionConfig::default()
                };

                let signature;
                loop {
                    if let Ok(sig) = rpc_client
                        .send_transaction_with_config(&tx, rpc_config)
                        .await
                    {
                        signature = sig;
                        break;
                    } else {
                        error!(target: "server_log", "Failed to send claim transaction. retrying in 2 seconds...");
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
                    tokio::time::sleep(Duration::from_millis(500)).await;
                };

                match result {
                    Ok(_sig) => {
                        info!(target: "server_log", "Successfully created delegate stake account for: {}", user_pubkey.to_string());
                    }
                    Err(e) => {
                        error!(target: "server_log", "ERROR: {:?}", e);
                        return Response::builder()
                            .status(StatusCode::INTERNAL_SERVER_ERROR)
                            .body("Failed to init delegate stake account.".to_string())
                            .unwrap();
                    }
                }
            } else {
                error!(target: "server_log", "Failed to confirm transaction for init delegate stake.");
            }
        }

        let base_ix = ore_miner_delegation::instruction::delegate_stake(
            user_pubkey,
            wallet.miner_wallet.pubkey(),
            query_params.amount,
        );
        let mut accts = Vec::new();
        for account_index in ixs[0].accounts.clone() {
            accts.push(tx.key(0, account_index.into()));
        }

        if ixs[0].data.ne(&base_ix.data) {
            error!(target: "server_log", "data missmatch");
            return Response::builder()
                .status(StatusCode::BAD_REQUEST)
                .body("Invalid Tx".to_string())
                .unwrap();
        } else {
            info!(target: "server_log", "Valid stake tx, submitting.");

            let hash = tx.get_recent_blockhash();
            if let Err(_) = tx.try_partial_sign(&[wallet.fee_wallet.as_ref()], *hash) {
                error!(target: "server_log", "Failed to partially sign tx");
                return Response::builder()
                    .status(StatusCode::INTERNAL_SERVER_ERROR)
                    .body("Server Failed to sign tx.".to_string())
                    .unwrap();
            }

            if !tx.is_signed() {
                error!(target: "server_log", "Tx missing signer");
                return Response::builder()
                    .status(StatusCode::BAD_REQUEST)
                    .body("Invalid Tx".to_string())
                    .unwrap();
            }

            let result = rpc_client.send_and_confirm_transaction(&tx).await;

            match result {
                Ok(sig) => {
                    let amount_dec =
                        query_params.amount as f64 / 10f64.powf(ORE_TOKEN_DECIMALS as f64);
                    info!(target: "server_log", "Miner {} successfully delegated stake of {}.\nSig: {}", user_pubkey.to_string(), amount_dec, sig.to_string());
                    return Response::builder()
                        .status(StatusCode::OK)
                        .body("SUCCESS".to_string())
                        .unwrap();
                }
                Err(e) => {
                    error!(target: "server_log", "{} stake transaction failed...", user_pubkey.to_string());
                    error!(target: "server_log", "Stake Tx Error: {:?}", e);
                    return Response::builder()
                        .status(StatusCode::INTERNAL_SERVER_ERROR)
                        .body("Failed to send tx".to_string())
                        .unwrap();
                }
            }
        }
    } else {
        error!(target: "server_log", "stake with invalid pubkey");
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
            error!(target: "server_log", "Failed to deserialize tx");
            return Response::builder()
                .status(StatusCode::BAD_REQUEST)
                .body("Invalid Tx".to_string())
                .unwrap();
        };

        let ixs = tx.message.instructions.clone();

        if ixs.len() > 1 {
            error!(target: "server_log", "Too many instructions");
            return Response::builder()
                .status(StatusCode::BAD_REQUEST)
                .body("Invalid Tx".to_string())
                .unwrap();
        }

        if let Err(_) =
            get_delegated_stake_account(&rpc_client, user_pubkey, wallet.miner_wallet.pubkey())
                .await
        {
            error!(target: "server_log", "Cannot unstake, no delegate stake account is created for {}", user_pubkey.to_string());
            return Response::builder()
                .status(StatusCode::BAD_REQUEST)
                .body("No delegate stake account exists".to_string())
                .unwrap();
        }

        let staker_ata = get_associated_token_address(&user_pubkey, &ore_api::consts::MINT_ADDRESS);

        let base_ix = ore_miner_delegation::instruction::undelegate_stake(
            user_pubkey,
            wallet.miner_wallet.pubkey(),
            staker_ata,
            query_params.amount,
        );
        let mut accts = Vec::new();
        for account_index in ixs[0].accounts.clone() {
            accts.push(tx.key(0, account_index.into()));
        }

        if ixs[0].data.ne(&base_ix.data) {
            error!(target: "server_log", "data missmatch");
            return Response::builder()
                .status(StatusCode::BAD_REQUEST)
                .body("Invalid Tx".to_string())
                .unwrap();
        } else {
            info!(target: "server_log", "Valid unstake tx, submitting.");

            let hash = tx.get_recent_blockhash();
            if let Err(_) = tx.try_partial_sign(&[wallet.fee_wallet.as_ref()], *hash) {
                error!(target: "server_log", "Failed to partially sign tx");
                return Response::builder()
                    .status(StatusCode::INTERNAL_SERVER_ERROR)
                    .body("Server Failed to sign tx.".to_string())
                    .unwrap();
            }

            if !tx.is_signed() {
                error!(target: "server_log", "Tx missing signer");
                return Response::builder()
                    .status(StatusCode::BAD_REQUEST)
                    .body("Invalid Tx".to_string())
                    .unwrap();
            }

            let result = rpc_client.send_and_confirm_transaction(&tx).await;

            match result {
                Ok(sig) => {
                    let amount_dec =
                        query_params.amount as f64 / 10f64.powf(ORE_TOKEN_DECIMALS as f64);
                    info!(target: "server_log", "Miner {} successfully undelegated stake of {}.\nSig: {}", user_pubkey.to_string(), amount_dec, sig.to_string());
                    return Response::builder()
                        .status(StatusCode::OK)
                        .body("SUCCESS".to_string())
                        .unwrap();
                }
                Err(e) => {
                    error!(target: "server_log", "{} unstake transaction failed...", user_pubkey.to_string());
                    error!(target: "server_log", "Unstake Tx Error: {:?}", e);
                    return Response::builder()
                        .status(StatusCode::INTERNAL_SERVER_ERROR)
                        .body("Failed to send tx".to_string())
                        .unwrap();
                }
            }
        }
    } else {
        error!(target: "server_log", "unstake with invalid pubkey");
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
                error!(target: "server_log", "Failed to get database pool connection.");
                return Err((StatusCode::INTERNAL_SERVER_ERROR, "Internal Server Error"));
            }
            Err(_) => {
                error!(target: "server_log", "DB Error: Catch all.");
                return Err((StatusCode::INTERNAL_SERVER_ERROR, "Internal Server Error"));
            }
        }

        if !miner.enabled {
            return Err((StatusCode::UNAUTHORIZED, "pubkey is not authorized to mine"));
        }

        if let Ok(signature) = Signature::from_str(signed_msg) {
            let ts_msg = msg_timestamp.to_le_bytes();

            if signature.verify(&user_pubkey.to_bytes(), &ts_msg) {
                info!(target: "server_log", "Client: {addr} connected with pubkey {pubkey}.");
                return Ok(ws.on_upgrade(move |socket| {
                    handle_socket(
                        socket,
                        addr,
                        user_pubkey,
                        miner.id,
                        ClientVersion::V1,
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

async fn ws_handler_v2(
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
                error!(target: "server_log", "Failed to get database pool connection.");
                return Err((StatusCode::INTERNAL_SERVER_ERROR, "Internal Server Error"));
            }
            Err(_) => {
                error!(target: "server_log", "DB Error: Catch all.");
                return Err((StatusCode::INTERNAL_SERVER_ERROR, "Internal Server Error"));
            }
        }

        if !miner.enabled {
            return Err((StatusCode::UNAUTHORIZED, "pubkey is not authorized to mine"));
        }

        if let Ok(signature) = Signature::from_str(signed_msg) {
            let ts_msg = msg_timestamp.to_le_bytes();

            if signature.verify(&user_pubkey.to_bytes(), &ts_msg) {
                info!(target: "server_log", "Client: {addr} connected with pubkey {pubkey} on V2.");
                return Ok(ws.on_upgrade(move |socket| {
                    handle_socket(
                        socket,
                        addr,
                        user_pubkey,
                        miner.id,
                        ClientVersion::V2,
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
    client_version: ClientVersion,
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
        error!(target: "server_log", "could not ping {who}");

        // if we can't ping we can't do anything, return to close the connection
        return;
    }

    let (sender, mut receiver) = socket.split();
    let mut app_state = rw_app_state.write().await;
    if app_state.sockets.contains_key(&who) {
        info!(target: "server_log", "Socket addr: {who} already has an active connection");
        return;
    } else {
        let new_app_client_connection = AppClientConnection {
            pubkey: who_pubkey,
            miner_id: who_miner_id,
            client_version,
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

    info!(target: "server_log", "Client: {} disconnected!", who_pubkey.to_string());
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
                                error!(target: "server_log", "Client submission sig verification failed.");
                            }
                        } else {
                            error!(target: "server_log", "Failed to parse into Signature.");
                        }
                    } else {
                        error!(target: "server_log", "Failed to parse signed message from client.");
                    }
                }
                _ => {
                    error!(target: "server_log", ">>> {} sent an invalid message", who);
                }
            }
        }
        Message::Close(c) => {
            if let Some(cf) = c {
                info!(
                    target: "server_log",
                    ">>> {} sent close with code {} and reason `{}`",
                    who, cf.code, cf.reason
                );
            } else {
                info!(target: "server_log", ">>> {who} somehow sent close message without CloseFrame");
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

async fn ping_check_system(shared_state: &Arc<RwLock<AppState>>) {
    loop {
        // send ping to all sockets
        let app_state = shared_state.read().await;
        let socks = app_state.sockets.clone();
        drop(app_state);

        let mut handles = Vec::new();
        for (who, socket) in socks.iter() {
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
                    return Some((who.clone(), socket.pubkey.clone()));
                }
            }));
        }

        // remove any sockets where ping failed
        for handle in handles {
            match handle.await {
                Ok(Some((who, pubkey))) => {
                    error!(target: "server_log", "Got error sending ping to client: {} on pk: {}.", who, pubkey);
                    let mut app_state = shared_state.write().await;
                    app_state.sockets.remove(&who);
                }
                Ok(None) => {}
                Err(_) => {
                    error!(target: "server_log", "Got error sending ping to client.");
                }
            }
        }

        tokio::time::sleep(Duration::from_secs(30)).await;
    }
}
