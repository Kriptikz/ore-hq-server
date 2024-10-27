use std::{
    collections::{HashMap, HashSet},
    net::SocketAddr,
    ops::{ControlFlow, Div},
    path::Path,
    str::FromStr,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use ore_boost_api::state::{boost_pda, stake_pda};
use ore_miner_delegation::{pda::{delegated_boost_pda, managed_proof_pda}, state::DelegatedBoost, utils::AccountDeserialize};
use solana_account_decoder::UiAccountEncoding;
use steel::AccountDeserialize as _;
use systems::{
    claim_system::claim_system, client_message_handler_system::client_message_handler_system,
    handle_ready_clients_system::handle_ready_clients_system,
    pong_tracking_system::pong_tracking_system, proof_tracking_system::proof_tracking_system,
};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, Layer};

use crate::{
    ore_utils::{get_managed_proof_token_ata, get_proof_pda},
    systems::{delegate_boost_tracking_system::delegate_boost_tracking_system, message_text_all_clients_system::message_text_all_clients_system, pool_mine_success_system::pool_mine_success_system, pool_submission_system::pool_submission_system},
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
use clap::{Parser, Subcommand};
use drillx::Solution;
use futures::{stream::SplitSink, SinkExt, StreamExt};
use ore_utils::{
    get_config, get_delegated_boost_account, get_delegated_boost_account_v2, get_delegated_stake_account, get_ore_mint, get_original_proof, get_proof, get_register_ix, ORE_TOKEN_DECIMALS
};
use routes::{get_challenges, get_latest_mine_txn, get_pool_balance};
use serde::{Deserialize, Serialize};
use solana_client::{
    nonblocking::rpc_client::RpcClient,
    rpc_client::SerializableTransaction,
    rpc_config::{RpcAccountInfoConfig, RpcProgramAccountsConfig, RpcSendTransactionConfig}, rpc_filter::{Memcmp, RpcFilterType},
};
use solana_sdk::{
    commitment_config::{CommitmentConfig, CommitmentLevel}, compute_budget::ComputeBudgetInstruction, native_token::{lamports_to_sol, LAMPORTS_PER_SOL}, pubkey::Pubkey, signature::{read_keypair_file, Keypair, Signature}, signer::Signer, transaction::Transaction
};
use solana_transaction_status::TransactionConfirmationStatus;
use spl_associated_token_account::{
    get_associated_token_address, instruction::create_associated_token_account,
};
use tokio::{
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
mod scripts;

const MIN_DIFF: u32 = 8;
const MIN_HASHPOWER: u64 = 5;

const ORE_BOOST_MINT: &str = "oreoU2P8bN6jkk3jbaiVxYnG1dCXcYxwhwyK9jSybcp";
const ORE_SOL_BOOST_MINT: &str = "DrSS5RM7zUd9qjUEdDaf31vnDUSbCrMto6mjqTrHFifN";
const ORE_ISC_BOOST_MINT: &str = "meUwDp23AaxhiNKaQCyJ2EAF2T4oe1gSkEkGXSRVdZb";

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
    amount: u64,
    mint: Option<Pubkey>,
}

struct ClaimsQueue {
    queue: RwLock<HashMap<(Pubkey, Option<Pubkey>), ClaimsQueueItem>>,
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
    commissions: u64,
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
    pool_id: i32,
    stats_enabled: bool,
    signup_fee: f64,
    commissions_pubkey: String,
    commissions_miner_id: i32,
}

mod ore_utils;

#[derive(Parser, Debug)]
#[command(version, author, about, long_about = None)]
struct Args {
    #[command(subcommand)]
    command: Commands,

}

#[derive(Parser, Debug)]
struct ServeArgs {
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



#[derive(Debug, Subcommand)]
enum Commands {
    #[command(about = "Serve the pool webserver for mining.")]
    Serve(ServeArgs),
    #[command(about = "Manually run the update for stake accounts balances from on-chain")]
    UpdateStakeAccounts,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    dotenv::dotenv().ok();
    let cmd_args = Args::parse();
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

    match cmd_args.command {
        Commands::Serve(args) => {
            serve(args).await
        }
        Commands::UpdateStakeAccounts => {
            scripts::update_stake_accounts().await
        }
    }
}

async fn serve(args: ServeArgs) -> Result<(), Box<dyn std::error::Error>> {
    // load envs
    let wallet_path_str = std::env::var("WALLET_PATH").expect("WALLET_PATH must be set.");
    let fee_wallet_path_str =
        std::env::var("FEE_WALLET_PATH").expect("FEE_WALLET_PATH must be set.");
    let rpc_url = std::env::var("RPC_URL").expect("RPC_URL must be set.");
    let rpc_ws_url = std::env::var("RPC_WS_URL").expect("RPC_WS_URL must be set.");
    let password = std::env::var("PASSWORD").expect("PASSWORD must be set.");
    let database_url = std::env::var("DATABASE_URL").expect("DATABASE_URL must be set.");
    let database_rr_url = std::env::var("DATABASE_RR_URL").expect("DATABASE_RR_URL must be set.");
    let commission_env = std::env::var("COMMISSION_PUBKEY").expect("COMMISSION_PUBKEY must be set.");
    let commission_pubkey = match Pubkey::from_str(&commission_env) {
        Ok(pk) => {
            pk
        },
        Err(_) => {
            println!("Invalid COMMISSION_PUBKEY");
            return Ok(())
        }
    };

    let app_database = Arc::new(AppDatabase::new(database_url));
    let app_rr_database = Arc::new(AppRRDatabase::new(database_rr_url));

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

    info!(target: "server_log", "creating managed proof boost token accounts");
    let boost_mints = vec![
        Pubkey::from_str("oreoU2P8bN6jkk3jbaiVxYnG1dCXcYxwhwyK9jSybcp").unwrap(),
        Pubkey::from_str("DrSS5RM7zUd9qjUEdDaf31vnDUSbCrMto6mjqTrHFifN").unwrap(),
        Pubkey::from_str("meUwDp23AaxhiNKaQCyJ2EAF2T4oe1gSkEkGXSRVdZb").unwrap()
    ];

    for boost_mint in boost_mints {
        // create the managed proof token account
        let managed_proof_boost_token_addr = get_associated_token_address(&managed_proof.0, &boost_mint);
        match rpc_client
            .get_token_account_balance(&managed_proof_boost_token_addr)
            .await
        {
            Ok(_) => {
                info!(target: "server_log", "Managed proof boost token account {} already created.", boost_mint.to_string());
            }
            Err(_) => {
                info!(target: "server_log", "Creating managed proof boost token account {}", boost_mint.to_string());
                let ix = create_associated_token_account(
                    &wallet.pubkey(),
                    &managed_proof.0,
                    &boost_mint,
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
                        info!(target: "server_log", "Successfully created managed proof boost token account {}", boost_mint.to_string());
                    }
                    Err(e) => {
                        error!(target: "server_log", "Failed to send and confirm tx.\nE: {:?}", e);
                        panic!("Failed to create managed proof boost token account {}", boost_mint.to_string());
                    }
                }
            }
        }

        let boost_acc = boost_pda(boost_mint);
        let pool_boost_stake_acc = stake_pda(managed_proof.0, boost_acc.0);
        match rpc_client.get_account(&pool_boost_stake_acc.0).await {
            Ok(_) => {
                info!(target: "server_log", "Managed proof boost stake account for {} already exists", boost_mint.to_string());
            }
            Err(_e) => {
                error!(target: "server_log", "Failed to get managed proof boost stake account for {}", boost_mint.to_string());
                info!(target: "server_log", "Creating managed proof boost stake account for {}", boost_mint.to_string());
                let ix = ore_miner_delegation::instruction::open_managed_proof_boost(wallet.pubkey(), boost_mint);

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
                        info!(target: "server_log", "Successfully created managed proof boost stake account for {}", boost_mint.to_string());
                    }
                    Err(e) => {
                        error!(target: "server_log", "Failed to send and confirm tx.\nE: {:?}", e);
                        panic!("Failed to create managed proof boost stake account for {}", boost_mint.to_string());
                    }
                }
            }
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


    info!(target: "server_log", "Validating commissions receiver is in db");
    let commission_miner_id;
    match app_database.get_miner_by_pubkey_str(commission_pubkey.to_string()).await {
        Ok(miner) => {
            info!(target: "server_log", "Found commissions receiver in db.");
            commission_miner_id = miner.id;
        }
        Err(_) => {
            info!(target: "server_log", "Failed to get commissions receiver account from database.");
            info!(target: "server_log", "Inserting Commissions receiver account...");

            let res = app_database.signup_user_transaction(
                commission_pubkey.to_string(),
                wallet.pubkey().to_string(),
            ).await;

            match res {
                Ok(_) => {
                    info!(target: "server_log", "Successfully inserted Commissions receiver account...");
                    if let Ok(m) = app_database.get_miner_by_pubkey_str(commission_pubkey.to_string()).await {
                        commission_miner_id = m.id;
                    } else {
                        panic!("Failed to get commission miner id")
                    }
                },
                Err(_) => {
                    panic!("Failed to insert comissions receiver account")
                }
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
        pool_id: db_pool.id,
        stats_enabled: args.stats,
        signup_fee: args.signup_fee,
        commissions_pubkey: commission_pubkey.to_string(),
        commissions_miner_id: commission_miner_id,
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
    let ws_url = rpc_ws_url.clone();
    // Establish webocket connection for tracking pool proof changes.
    tokio::spawn(async move {
        proof_tracking_system(
            ws_url,
            app_wallet.miner_wallet.clone(),
            app_proof,
            app_last_challenge
        ).await;
    });

    let app_wallet = wallet_extension.clone();
    let app_app_database = app_database.clone();
    let app_rpc_client = rpc_client.clone();
    tokio::spawn(async move {
        delegate_boost_tracking_system(
            rpc_ws_url,
            app_wallet.miner_wallet.pubkey().clone(),
            app_rpc_client,
            app_app_database,
        )
        .await;
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
        .route("/v2/claim-stake-rewards", post(post_claim_stake_rewards_v2))
        // v3 permissionless claim to staker wallet
        .route("/v3/claim-stake-rewards", post(post_claim_stake_rewards_v3)) 
        .route("/stake", post(post_stake))
        .route("/stake-boost", post(post_stake_boost))
        .route("/v2/stake-boost", post(post_stake_boost_v2))
        .route("/v2/migrate-boost", post(post_migrate_boost_v2))
        .route("/unstake", post(post_unstake))
        .route("/unstake-boost", post(post_unstake_boost))
        .route("/v2/unstake-boost", post(post_unstake_boost_v2))
        .route("/active-miners", get(get_connected_miners))
        .route("/timestamp", get(get_timestamp))
        .route("/miner/balance", get(get_miner_balance))
        .route("/v2/miner/balance", get(get_miner_balance_v2))
        .route("/miner/stake", get(get_miner_stake))
        .route("/miner/boost/stake", get(get_miner_boost_stake))
        .route("/v2/miner/boost/stake", get(get_miner_boost_stake_v2))
        .route("/v2/miner/boost/stake-accounts", get(get_miner_boost_stake_accounts_v2))
        .route("/stake-multiplier", get(get_stake_multiplier))
        .route("/boost-multiplier", get(get_boost_multiplier))
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
    let latest_blockhash = rpc_client.get_latest_blockhash_with_commitment(CommitmentConfig { commitment: CommitmentLevel::Finalized }).await.unwrap();

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
    Extension(wallet): Extension<Arc<WalletExtension>>,
    _body: String,
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
        let res = app_database.signup_user_transaction(
            user_pubkey.to_string(),
            wallet.miner_wallet.pubkey().to_string(),
        ).await;

        match res {
            Ok(_) => {
                return Response::builder()
                    .status(StatusCode::OK)
                    .header("Content-Type", "text/text")
                    .body("SUCCESS".to_string())
                    .unwrap();
            },
            Err(_) => {
                error!(target: "server_log", "Failed to add miner to database");
                return Response::builder()
                    .status(StatusCode::INTERNAL_SERVER_ERROR)
                    .body("Failed to add user to database".to_string())
                    .unwrap();
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
}

async fn post_signup_v2(
    query_params: Query<SignupParamsV2>,
    Extension(app_database): Extension<Arc<AppDatabase>>,
    Extension(wallet): Extension<Arc<WalletExtension>>,
    _body: String,
) -> impl IntoResponse {
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

        let res = app_database.signup_user_transaction(
            miner_pubkey.to_string(),
            wallet.miner_wallet.pubkey().to_string(),
        ).await;

        match res {
            Ok(_) => {
                return Response::builder()
                    .status(StatusCode::OK)
                    .header("Content-Type", "text/text")
                    .body("SUCCESS".to_string())
                    .unwrap();
            },
            Err(_) => {
                error!(target: "server_log", "Failed to add miner to database");
                return Response::builder()
                    .status(StatusCode::INTERNAL_SERVER_ERROR)
                    .body("Failed to add user to database".to_string())
                    .unwrap();
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

#[derive(Deserialize)]
struct PubkeyMintParam {
    pubkey: String,
    mint: String,
}

async fn get_miner_balance_v2(
    query_params: Query<PubkeyMintParam>,
    Extension(rpc_client): Extension<Arc<RpcClient>>,
) -> impl IntoResponse {
    let mint = match Pubkey::from_str(&query_params.mint) {
        Ok(pk) => {
            pk
        },
        Err(_) => {
            error!(target: "server_log", "get_miner_balance_v2 - Failed to parse mint");
            return Response::builder()
                .status(StatusCode::BAD_REQUEST)
                .body("Invalid Mint".to_string())
                .unwrap();
        }
    };
    if let Ok(user_pubkey) = Pubkey::from_str(&query_params.pubkey) {
        let miner_token_account = get_associated_token_address(&user_pubkey, &mint);
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



async fn get_miner_boost_stake(
    query_params: Query<PubkeyMintParam>,
    Extension(rpc_client): Extension<Arc<RpcClient>>,
    Extension(wallet): Extension<Arc<WalletExtension>>,
) -> impl IntoResponse {
    let mint = match Pubkey::from_str(&query_params.mint) {
        Ok(pk) => {
            pk
        }
        Err(_) => {
            return Err("Invalid mint".to_string());
        }
    };
    if let Ok(user_pubkey) = Pubkey::from_str(&query_params.pubkey) {
        if let Ok(account) =
            get_delegated_boost_account(&rpc_client, user_pubkey, wallet.miner_wallet.pubkey(), mint)
                .await
        {
            let decimals = 10f64.powf(ORE_TOKEN_DECIMALS as f64);
            let dec_amount = (account.amount as f64).div(decimals);
            return Ok(dec_amount.to_string());
        } else {
            return Err("Failed to get delgated boost account balance".to_string());
        }
    } else {
        return Err("Invalid pubkey".to_string());
    }
}

async fn get_miner_boost_stake_v2(
    query_params: Query<PubkeyMintParam>,
    Extension(rpc_client): Extension<Arc<RpcClient>>,
    Extension(wallet): Extension<Arc<WalletExtension>>,
) -> impl IntoResponse {
    let mint = match Pubkey::from_str(&query_params.mint) {
        Ok(pk) => {
            pk
        }
        Err(_) => {
            return Err("Invalid mint".to_string());
        }
    };
    if let Ok(user_pubkey) = Pubkey::from_str(&query_params.pubkey) {
        if let Ok(account) =
            get_delegated_boost_account_v2(&rpc_client, user_pubkey, wallet.miner_wallet.pubkey(), mint)
                .await
        {
            let decimals = 10f64.powf(ORE_TOKEN_DECIMALS as f64);
            let dec_amount = (account.amount as f64).div(decimals);
            return Ok(dec_amount.to_string());
        } else {
            return Err("Failed to get delgated boost account v2 balance".to_string());
        }
    } else {
        return Err("Invalid pubkey".to_string());
    }
}

async fn get_miner_boost_stake_accounts_v2(
    query_params: Query<PubkeyParam>,
    Extension(app_database): Extension<Arc<AppDatabase>>,
    Extension(app_config): Extension<Arc<Config>>,
) -> impl IntoResponse {
    if let Ok(user_pubkey) = Pubkey::from_str(&query_params.pubkey) {
        let pool_id = app_config.pool_id;
        if let Ok(result) = app_database.get_stake_accounts_for_staker(pool_id, user_pubkey.to_string()).await {
            return Ok(Json(result));

        } else {
            return Err("Failed to get delgated boost accounts v2 from db".to_string());
        }
    } else {
        return Err("Invalid pubkey".to_string());
    }
}

async fn get_stake_multiplier(
    Extension(app_config): Extension<Arc<Config>>,
) -> impl IntoResponse {
    if app_config.stats_enabled {
        let multiplier = 1.0;
        return Ok(Json(multiplier));
    } else {
        return Err("Stats not enabled for this server.".to_string());
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub struct BoostMultiplierData {
    boost_mint: String,
    staked_balance: f64,
    total_stake_balance: f64,
    multiplier: u64,
}

async fn get_boost_multiplier(
    Extension(rpc_client): Extension<Arc<RpcClient>>,
    Extension(app_config): Extension<Arc<Config>>,
) -> impl IntoResponse {
    if app_config.stats_enabled {
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

        return Ok(Json(boost_multiplier_datas));
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

        if queue.contains_key(&(miner_pubkey, None)) {
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
            writer.insert((miner_pubkey, None), ClaimsQueueItem{
                receiver_pubkey: miner_pubkey,
                amount,
                mint: None,
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

                if queue.contains_key(&(miner_pubkey, None)) {
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
                    writer.insert((miner_pubkey, None), ClaimsQueueItem{
                        receiver_pubkey,
                        amount,
                        mint: None,
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
struct ClaimStakeRewardsParamsV2 {
    timestamp: u64,
    mint: String,
    receiver_pubkey: String,
    amount: u64,
}

async fn post_claim_stake_rewards_v2(
    TypedHeader(auth_header): TypedHeader<axum_extra::headers::Authorization<Basic>>,
    Extension(app_database): Extension<Arc<AppDatabase>>,
    Extension(claims_queue): Extension<Arc<ClaimsQueue>>,
    Extension(rpc_client): Extension<Arc<RpcClient>>,
    query_params: Query<ClaimStakeRewardsParamsV2>,
) -> impl IntoResponse {
    let msg_timestamp = query_params.timestamp;

    let staker_pubkey_str = auth_header.username();
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

    let mint_pubkey = match Pubkey::from_str(&query_params.mint) {
        Ok(pubkey) => {
            pubkey
        },
        Err(_) => {
            return Err((StatusCode::BAD_REQUEST, "Invalid mint pubkey provided.".to_string()))
        }
    };

    let boost_mints = vec![
        Pubkey::from_str("oreoU2P8bN6jkk3jbaiVxYnG1dCXcYxwhwyK9jSybcp").unwrap(),
        Pubkey::from_str("DrSS5RM7zUd9qjUEdDaf31vnDUSbCrMto6mjqTrHFifN").unwrap(),
        Pubkey::from_str("meUwDp23AaxhiNKaQCyJ2EAF2T4oe1gSkEkGXSRVdZb").unwrap()
    ];

    if !boost_mints.contains(&mint_pubkey) {
        return Err((StatusCode::BAD_REQUEST, "Invalid mint provided.".to_string()))
    }

    if let Ok(staker_pubkey) = Pubkey::from_str(staker_pubkey_str) {
        if let Ok(signature) = Signature::from_str(signed_msg) {
            let amount = query_params.amount;
            let mut signed_msg = vec![];
            signed_msg.extend(msg_timestamp.to_le_bytes());
            signed_msg.extend(mint_pubkey.to_bytes());
            signed_msg.extend(receiver_pubkey.to_bytes());
            signed_msg.extend(amount.to_le_bytes());

            if signature.verify(&staker_pubkey.to_bytes(), &signed_msg) {
                let reader = claims_queue.queue.read().await;
                let queue = reader.clone();
                drop(reader);

                if queue.contains_key(&(staker_pubkey, Some(mint_pubkey))) {
                    return Err((StatusCode::TOO_MANY_REQUESTS, "QUEUED".to_string()));
                }

                let amount = query_params.amount;

                // 0.00500000000
                let ore_mint = get_ore_mint();
                let receiver_token_account = get_associated_token_address(&receiver_pubkey, &ore_mint);
                let mut is_creating_ata = true;
                if let Ok(response) = rpc_client
                    .get_token_account_balance(&receiver_token_account)
                    .await
                {
                    if let Some(_amount) = response.ui_amount {
                        info!(target: "server_log", "staker claim beneficiary has valid token account.");
                        is_creating_ata = false;
                    }
                }
                if amount < 5_000_000 {
                    return Err((StatusCode::BAD_REQUEST, "claim minimum is 0.00005000000".to_string()));
                }
                if is_creating_ata && amount < 500_000_000 {
                    return Err((StatusCode::BAD_REQUEST, "claim minimum is 0.005".to_string()));
                }

                if let Ok(staker_rewards) = app_database
                    .get_staker_rewards(staker_pubkey.to_string(), mint_pubkey.to_string())
                    .await
                {
                    if amount > staker_rewards.rewards_balance {
                        return Err((StatusCode::BAD_REQUEST, "claim amount exceeds staker rewards balance.".to_string()));
                    }

                    let mut writer = claims_queue.queue.write().await;
                    writer.insert((staker_pubkey, Some(mint_pubkey)), ClaimsQueueItem{
                        receiver_pubkey,
                        amount,
                        mint: Some(mint_pubkey),
                    });
                    drop(writer);
                    return Ok((StatusCode::OK, "SUCCESS"));
                } else {
                    return Err((StatusCode::INTERNAL_SERVER_ERROR, "failed to get staker account from database".to_string()));
                }
            } else {
                return Err((StatusCode::UNAUTHORIZED, "Sig verification failed".to_string()));
            }
        } else {
            return Err((StatusCode::UNAUTHORIZED, "Invalid signature".to_string()));
        }
    } else {
        error!(target: "server_log", "Claim stake rewards with invalid pubkey");
        return Err((StatusCode::BAD_REQUEST, "Invalid Pubkey".to_string()));
    }
}

#[derive(Deserialize)]
struct ClaimStakeRewardsParamsV3 {
    pubkey: String,
    mint: String,
    amount: u64,
}

async fn post_claim_stake_rewards_v3(
    query_params: Query<ClaimStakeRewardsParamsV3>,
    Extension(app_database): Extension<Arc<AppDatabase>>,
    Extension(claims_queue): Extension<Arc<ClaimsQueue>>,
) -> impl IntoResponse {
    let mint_pubkey = match Pubkey::from_str(&query_params.mint) {
        Ok(pk) => pk,
        Err(_) => {
            return Response::builder()
                .status(StatusCode::BAD_REQUEST)
                .body("invalid mint pubkey".to_string())
                .unwrap();
        }
    };
    if let Ok(staker_pubkey) = Pubkey::from_str(&query_params.pubkey) {
        let reader = claims_queue.queue.read().await;
        let queue = reader.clone();
        drop(reader);

        if queue.contains_key(&((staker_pubkey, Some(mint_pubkey)))) {
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

        if let Ok(stake_account) = app_database
            .get_staker_rewards(staker_pubkey.to_string(), mint_pubkey.to_string())
            .await
        {
            if amount > stake_account.rewards_balance {
                return Response::builder()
                    .status(StatusCode::BAD_REQUEST)
                    .body("claim amount exceeds staker rewards balance".to_string())
                    .unwrap();
            }

            let mut writer = claims_queue.queue.write().await;
            writer.insert((staker_pubkey, Some(mint_pubkey)), ClaimsQueueItem{
                receiver_pubkey: staker_pubkey,
                amount,
                mint: Some(mint_pubkey),
            });
            drop(writer);
            return Response::builder()
                .status(StatusCode::OK)
                .body("SUCCESS".to_string())
                .unwrap();
        } else {
            return Response::builder()
                .status(StatusCode::INTERNAL_SERVER_ERROR)
                .body("failed to get staker account from database".to_string())
                .unwrap();
        }
    } else {
        error!(target: "server_log", "Claim staker rewards with invalid pubkey");
        return Response::builder()
            .status(StatusCode::BAD_REQUEST)
            .body("Invalid Pubkey".to_string())
            .unwrap();
    }
}

async fn post_stake() -> impl IntoResponse {
    return Response::builder()
        .status(StatusCode::BAD_REQUEST)
        .body("Legacy staking no longer supported".to_string())
        .unwrap();
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
struct StakeBoostParams {
    pubkey: String,
    mint: String,
    amount: u64,
}

async fn post_stake_boost() -> impl IntoResponse {
    return Response::builder()
        .status(StatusCode::BAD_REQUEST)
        .body("V1 Boost no longer supported. Update client to use V2 Boost".to_string())
        .unwrap();
}

async fn post_stake_boost_v2(
    query_params: Query<StakeBoostParams>,
    Extension(rpc_client): Extension<Arc<RpcClient>>,
    Extension(wallet): Extension<Arc<WalletExtension>>,
    body: String,
) -> impl IntoResponse {
    if let Ok(user_pubkey) = Pubkey::from_str(&query_params.pubkey) {

        let mint = match Pubkey::from_str(&query_params.mint) {
            Ok(pk) => {
                pk
            },
            Err(_) => {
                error!(target: "server_log", "Failed to parse mint: {}", query_params.mint);
                return Response::builder()
                    .status(StatusCode::BAD_REQUEST)
                    .body("Invalid Mint".to_string())
                    .unwrap();
            }
        };

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
            get_delegated_boost_account_v2(&rpc_client, user_pubkey, wallet.miner_wallet.pubkey(), mint)
                .await
        {
            let init_ix = ore_miner_delegation::instruction::init_delegate_boost_v2(
                user_pubkey,
                wallet.miner_wallet.pubkey(),
                wallet.fee_wallet.pubkey(),
                mint,
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
                let mut attempts = 0;
                loop {
                    match rpc_client.send_transaction_with_config(&tx, rpc_config).await {
                        Ok(sig) => {
                            signature = sig;
                            break;
                        },
                        Err(e) => {
                            if attempts > 10 {
                                return Response::builder()
                                    .status(StatusCode::INTERNAL_SERVER_ERROR)
                                    .body(format!("Failed to send transaction: {:?}", e))
                                    .unwrap();
                            }
                            attempts += 1;
                            error!(target: "server_log", "Failed to send init delegate boost account. Error: {:?}.\nretrying in 2 seconds...", e);
                            tokio::time::sleep(Duration::from_millis(2000)).await;
                        }
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
                        info!(target: "server_log", "Successfully created delegate boost account v2 for: {} of mint: {}", user_pubkey.to_string(), mint.to_string());
                    }
                    Err(e) => {
                        error!(target: "server_log", "ERROR: {:?}", e);
                        return Response::builder()
                            .status(StatusCode::INTERNAL_SERVER_ERROR)
                            .body("Failed to init delegate boost account.".to_string())
                            .unwrap();
                    }
                }
            } else {
                error!(target: "server_log", "Failed to confirm transaction for init delegate boost v2.");
            }
        }

        let base_ix = ore_miner_delegation::instruction::delegate_boost_v2(
            user_pubkey,
            wallet.miner_wallet.pubkey(),
            mint,
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
            info!(target: "server_log", "Valid boost tx, submitting.");

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
                    info!(target: "server_log", "Miner {} successfully delegated boost v2 of {} for {}.\nSig: {}", user_pubkey.to_string(), mint.to_string(), amount_dec, sig.to_string());
                    return Response::builder()
                        .status(StatusCode::OK)
                        .body("SUCCESS".to_string())
                        .unwrap();
                }
                Err(e) => {
                    error!(target: "server_log", "{} stake transaction failed...", user_pubkey.to_string());
                    error!(target: "server_log", "Boost v2 stake tx Error: {:?}", e);
                    return Response::builder()
                        .status(StatusCode::INTERNAL_SERVER_ERROR)
                        .body("Failed to send tx".to_string())
                        .unwrap();
                }
            }
        }
    } else {
        error!(target: "server_log", "stake boost v2 with invalid pubkey");
        return Response::builder()
            .status(StatusCode::BAD_REQUEST)
            .body("Invalid Pubkey".to_string())
            .unwrap();
    }
}

#[derive(Deserialize)]
struct MigrateBoostParams {
    pubkey: String,
    mint: String,
    init: bool,
}


async fn post_migrate_boost_v2(
    query_params: Query<MigrateBoostParams>,
    Extension(rpc_client): Extension<Arc<RpcClient>>,
    Extension(wallet): Extension<Arc<WalletExtension>>,
    body: String,
) -> impl IntoResponse {
    if let Ok(user_pubkey) = Pubkey::from_str(&query_params.pubkey) {
        let mint = match Pubkey::from_str(&query_params.mint) {
            Ok(pk) => {
                pk
            },
            Err(_) => {
                error!(target: "server_log", "Failed to parse mint: {}", query_params.mint);
                return Response::builder()
                    .status(StatusCode::BAD_REQUEST)
                    .body("Invalid Mint".to_string())
                    .unwrap();
            }
        };

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

        if query_params.init {
            match get_delegated_boost_account_v2(&rpc_client, user_pubkey, wallet.miner_wallet.pubkey(), mint).await {
                Ok(_) => {
                    return Response::builder()
                        .status(StatusCode::BAD_REQUEST)
                        .body("Account already initialized.".to_string())
                        .unwrap();
                },
                Err(_) => {
                    // Account does not already exist
                    let ixs = tx.message.instructions.clone();

                    // There should be two instructions
                    if ixs.len() > 2 {
                        error!(target: "server_log", "Too many instructions");
                        return Response::builder()
                            .status(StatusCode::BAD_REQUEST)
                            .body("Invalid Tx".to_string())
                            .unwrap();
                    }

                    // First instruction is to init
                    let server_ix = ore_miner_delegation::instruction::init_delegate_boost_v2(
                        user_pubkey,
                        wallet.miner_wallet.pubkey(),
                        wallet.fee_wallet.pubkey(),
                        mint,
                    );
                    let mut accts = Vec::new();
                    for account_index in ixs[0].accounts.clone() {
                        accts.push(tx.key(0, account_index.into()));
                    }

                    if ixs[0].data.ne(&server_ix.data) {
                        error!(target: "server_log", "data missmatch");
                        return Response::builder()
                            .status(StatusCode::BAD_REQUEST)
                            .body("Invalid Tx".to_string())
                            .unwrap();
                    }

                    // Second instruction is to migrate to v2
                    let server_ix = ore_miner_delegation::instruction::migrate_boost_to_v2(
                        user_pubkey,
                        wallet.miner_wallet.pubkey(),
                        mint,
                    );
                    let mut accts = Vec::new();
                    for account_index in ixs[1].accounts.clone() {
                        accts.push(tx.key(0, account_index.into()));
                    }

                    if ixs[1].data.ne(&server_ix.data) {
                        error!(target: "server_log", "data missmatch");
                        return Response::builder()
                            .status(StatusCode::BAD_REQUEST)
                            .body("Invalid Tx".to_string())
                            .unwrap();
                    }
                    info!(target: "server_log", "Valid boost migration tx, submitting.");

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
                            info!(target: "server_log", "Miner {} successfully migrated to boost v2 of {}.\nSig: {}", user_pubkey.to_string(), mint.to_string(), sig.to_string());
                            return Response::builder()
                                .status(StatusCode::OK)
                                .body("SUCCESS".to_string())
                                .unwrap();
                        }
                        Err(e) => {
                            error!(target: "server_log", "{} Boost v2 migration transaction failed...", user_pubkey.to_string());
                            error!(target: "server_log", "Boost v2 migration tx Error: {:?}", e);
                            return Response::builder()
                                .status(StatusCode::INTERNAL_SERVER_ERROR)
                                .body("Failed to send tx".to_string())
                                .unwrap();
                        }
                    }
                }
            }
        } else {
            match get_delegated_boost_account_v2(&rpc_client, user_pubkey, wallet.miner_wallet.pubkey(), mint).await {
                Ok(_) => {
                    // Account already exist
                    let ixs = tx.message.instructions.clone();

                    // There should be one instructions
                    if ixs.len() > 1 {
                        error!(target: "server_log", "Too many instructions");
                        return Response::builder()
                            .status(StatusCode::BAD_REQUEST)
                            .body("Invalid Tx".to_string())
                            .unwrap();
                    }

                    let base_ix = ore_miner_delegation::instruction::migrate_boost_to_v2(
                        user_pubkey,
                        wallet.miner_wallet.pubkey(),
                        mint,
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
                        info!(target: "server_log", "Valid boost migrate tx, submitting.");

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
                                info!(target: "server_log", "Miner {} successfully migrated to boost v2 of {}.\nSig: {}", user_pubkey.to_string(), mint.to_string(), sig.to_string());
                                return Response::builder()
                                    .status(StatusCode::OK)
                                    .body("SUCCESS".to_string())
                                    .unwrap();
                            }
                            Err(e) => {
                                error!(target: "server_log", "{} Boost v1 migration transaction failed...", user_pubkey.to_string());
                                error!(target: "server_log", "Boost v2 migration tx Error: {:?}", e);
                                return Response::builder()
                                    .status(StatusCode::INTERNAL_SERVER_ERROR)
                                    .body("Failed to send tx".to_string())
                                    .unwrap();
                            }
                        }
                    }
                },
                Err(_) => {
                    return Response::builder()
                        .status(StatusCode::BAD_REQUEST)
                        .body("Account not initialized.".to_string())
                        .unwrap();
                }
            }
        }
    } else {
        error!(target: "server_log", "stake boost v2 with invalid pubkey");
        return Response::builder()
            .status(StatusCode::BAD_REQUEST)
            .body("Invalid Pubkey".to_string())
            .unwrap();
    }
}

#[derive(Deserialize)]
struct UnstakeBoostParams {
    pubkey: String,
    mint: String,
    amount: u64,
}

async fn post_unstake_boost(
    query_params: Query<UnstakeBoostParams>,
    Extension(rpc_client): Extension<Arc<RpcClient>>,
    Extension(wallet): Extension<Arc<WalletExtension>>,
    body: String,
) -> impl IntoResponse {
    if let Ok(user_pubkey) = Pubkey::from_str(&query_params.pubkey) {

        let mint = match Pubkey::from_str(&query_params.mint) {
            Ok(pk) => {
                pk
            },
            Err(_) => {
                error!(target: "server_log", "Failed to parse mint: {}", query_params.mint);
                return Response::builder()
                    .status(StatusCode::BAD_REQUEST)
                    .body("Invalid Mint".to_string())
                    .unwrap();
            }
        };

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
            get_delegated_boost_account(&rpc_client, user_pubkey, wallet.miner_wallet.pubkey(), mint)
                .await
        {
                error!(target: "server_log", "unstake-boost error: invalid delegate boost account for user: {}", user_pubkey.to_string());
                return Response::builder()
                    .status(StatusCode::INTERNAL_SERVER_ERROR)
                    .body("Failed to unstake boost".to_string())
                    .unwrap();
        }

        let base_ix = ore_miner_delegation::instruction::undelegate_boost(
            user_pubkey,
            wallet.miner_wallet.pubkey(),
            mint,
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
            info!(target: "server_log", "Valid undelegate boost tx, submitting.");

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
                    info!(target: "server_log", "Miner {} successfully undelegated boost of {} for {}.\nSig: {}", user_pubkey.to_string(), mint.to_string(), amount_dec, sig.to_string());
                    return Response::builder()
                        .status(StatusCode::OK)
                        .body("SUCCESS".to_string())
                        .unwrap();
                }
                Err(e) => {
                    error!(target: "server_log", "{} undelegate boost transaction failed...", user_pubkey.to_string());
                    error!(target: "server_log", "Undelegate boost Tx Error: {:?}", e);
                    return Response::builder()
                        .status(StatusCode::INTERNAL_SERVER_ERROR)
                        .body("Failed to send tx".to_string())
                        .unwrap();
                }
            }
        }
    } else {
        error!(target: "server_log", "stake boost with invalid pubkey");
        return Response::builder()
            .status(StatusCode::BAD_REQUEST)
            .body("Invalid Pubkey".to_string())
            .unwrap();
    }
}

async fn post_unstake_boost_v2(
    query_params: Query<UnstakeBoostParams>,
    Extension(rpc_client): Extension<Arc<RpcClient>>,
    Extension(wallet): Extension<Arc<WalletExtension>>,
    body: String,
) -> impl IntoResponse {
    if let Ok(user_pubkey) = Pubkey::from_str(&query_params.pubkey) {

        let mint = match Pubkey::from_str(&query_params.mint) {
            Ok(pk) => {
                pk
            },
            Err(_) => {
                error!(target: "server_log", "Failed to parse mint: {}", query_params.mint);
                return Response::builder()
                    .status(StatusCode::BAD_REQUEST)
                    .body("Invalid Mint".to_string())
                    .unwrap();
            }
        };

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
            get_delegated_boost_account_v2(&rpc_client, user_pubkey, wallet.miner_wallet.pubkey(), mint)
                .await
        {
                error!(target: "server_log", "unstake-boost error: invalid delegate boost account for user: {}", user_pubkey.to_string());
                return Response::builder()
                    .status(StatusCode::INTERNAL_SERVER_ERROR)
                    .body("Failed to unstake boost".to_string())
                    .unwrap();
        }

        let base_ix = ore_miner_delegation::instruction::undelegate_boost_v2(
            user_pubkey,
            wallet.miner_wallet.pubkey(),
            mint,
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
            info!(target: "server_log", "Valid undelegate boost tx, submitting.");

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
                    info!(target: "server_log", "Miner {} successfully undelegated boost v2 of {} for {}.\nSig: {}", user_pubkey.to_string(), mint.to_string(), amount_dec, sig.to_string());
                    return Response::builder()
                        .status(StatusCode::OK)
                        .body("SUCCESS".to_string())
                        .unwrap();
                }
                Err(e) => {
                    error!(target: "server_log", "{} undelegate boost v2 transaction failed...", user_pubkey.to_string());
                    error!(target: "server_log", "Undelegate boost v2 Tx Error: {:?}", e);
                    return Response::builder()
                        .status(StatusCode::INTERNAL_SERVER_ERROR)
                        .body("Failed to send tx".to_string())
                        .unwrap();
                }
            }
        }
    } else {
        error!(target: "server_log", "unstake boost v2 with invalid pubkey");
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

async fn update_delegate_boost_stake_accounts(
    mining_pubkey: Pubkey,
    app_database: &Arc<AppDatabase>,
    rpc_client: &Arc<RpcClient>
) {
    let managed_proof_authority_pda = managed_proof_pda(mining_pubkey);
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
                tracing::error!(target: "server_logs", "Failed to get program_accounts. Error: {:?}", e);
                return;
            }

    };

    tracing::info!(target: "server_log", "Found {} program accounts", program_accounts.len());

    let mut delegated_boosts_to_update = vec![];
    for program_account in program_accounts.iter() {
        if let Ok(delegate_boost_acct) = DelegatedBoost::try_from_bytes(&program_account.1.data) {
            let updated_stake_account = UpdateStakeAccount {
                stake_pda: program_account.0.to_string(),
                staked_balance: delegate_boost_acct.amount,
            };
            delegated_boosts_to_update.push(updated_stake_account);
        }
    }

    tracing::info!(target: "server_log", "Found {} delegated_boosts.", delegated_boosts_to_update.len());
    let instant = Instant::now();
    let batch_size = 200;
    tracing::info!(target: "server_log", "Updating stake accounts.");
    if delegated_boosts_to_update.len() > 0 {
        for (i, batch) in delegated_boosts_to_update.chunks(batch_size).enumerate() {
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
}



