use std::{collections::{HashMap, HashSet}, net::SocketAddr, ops::{ControlFlow, Div, Mul}, path::Path, str::FromStr, sync::Arc, time::{Duration, SystemTime, UNIX_EPOCH}};

use axum::{extract::{ws::{Message, WebSocket}, ConnectInfo, Query, State, WebSocketUpgrade}, http::StatusCode, response::IntoResponse, routing::get, Extension, Router};
use axum_extra::{headers::authorization::Basic, TypedHeader};
use clap::Parser;
use drillx::Solution;
use futures::{stream::SplitSink, SinkExt, StreamExt};
use ore_api::{consts::BUS_COUNT, state::Proof};
use ore_utils::{get_auth_ix, get_cutoff, get_mine_ix, get_proof, get_register_ix, ORE_TOKEN_DECIMALS};
use rand::Rng;
use serde::Deserialize;
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::{commitment_config::CommitmentConfig, compute_budget::ComputeBudgetInstruction, native_token::LAMPORTS_PER_SOL, pubkey::Pubkey, signature::{read_keypair_file, Signature}, signer::Signer, transaction::Transaction};
use tokio::{io::AsyncReadExt, sync::{mpsc::{UnboundedReceiver, UnboundedSender}, Mutex, RwLock}};
use tower_http::trace::{DefaultMakeSpan, TraceLayer};
use tracing::{error, info};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};


const MIN_DIFF: u32 = 8;
const MIN_HASHPOWER: u64 = 5;


struct AppState {
    sockets: HashMap<SocketAddr, (Pubkey, Mutex<SplitSink<WebSocket, Message>>)>
}

pub struct MessageInternalMineSuccess {
    difficulty: u32,
    total_balance: f64,
    rewards: f64,
    total_hashpower: u64,
    submissions: HashMap<Pubkey, (u32, u64)>
}

#[derive(Debug)]
pub enum ClientMessage {
    Ready(SocketAddr),
    Mining(SocketAddr),
    BestSolution(SocketAddr, Solution, Pubkey)
}

pub struct EpochHashes {
    best_hash: BestHash,
    submissions: HashMap<Pubkey, (u32, u64)>,
}

pub struct BestHash {
    solution: Option<Solution>,
    difficulty: u32,
}

pub struct Config {
    password: String,
    whitelist: Option<HashSet<Pubkey>>
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
    let password = std::env::var("PASSWORD").expect("PASSWORD must be set.");

    let whitelist = if let Some(whitelist) = args.whitelist {
        let file = Path::new(&whitelist);
        if file.exists() {
            // load file
            let mut pubkeys = HashSet::new();
            if let Ok(mut file) = tokio::fs::File::open(file).await {
                let mut file_contents = String::new();
                file.read_to_string(&mut file_contents).await.ok().expect("Failed to read whitelist file");
                drop(file);

                for (i, line) in file_contents.lines().enumerate() {
                    if let Ok(pubkey) = Pubkey::from_str(line) {
                        pubkeys.insert(pubkey);
                    } else {
                        let err = format!("Failed to create pubkey from line {} with value: {}", i, line);
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

    let wallet = read_keypair_file(wallet_path).expect("Failed to load keypair from file: {wallet_path_str}");
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
            .get_latest_blockhash_with_commitment(rpc_client.commitment()).await {
            let mut tx = Transaction::new_with_payer(&[ix], Some(&wallet.pubkey()));

            tx.sign(&[&wallet], hash);

            let result = rpc_client
                .send_and_confirm_transaction_with_spinner_and_commitment(
                    &tx, rpc_client.commitment()
                ).await;

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

    let config = Arc::new(Mutex::new(Config {
        password,
        whitelist
    }));

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

    let shared_state = Arc::new(RwLock::new(AppState {
        sockets: HashMap::new(),
    }));
    let ready_clients = Arc::new(Mutex::new(HashSet::new()));

    let (client_message_sender, client_message_receiver) = tokio::sync::mpsc::unbounded_channel::<ClientMessage>();

    // Handle client messages
    let app_shared_state = shared_state.clone();
    let app_ready_clients = ready_clients.clone();
    let app_proof = proof_ext.clone();
    let app_epoch_hashes = epoch_hashes.clone();
    tokio::spawn(async move {
        client_message_handler_system(client_message_receiver, &app_shared_state, app_ready_clients, app_proof, app_epoch_hashes).await;
    });

    // Handle ready clients
    let app_shared_state = shared_state.clone();
    let app_proof = proof_ext.clone();
    let app_epoch_hashes = epoch_hashes.clone();
    let app_nonce = nonce_ext.clone();
    tokio::spawn(async move {
        loop {

            let mut clients = Vec::new();
            {
                let ready_clients_lock = ready_clients.lock().await;
                for ready_client in ready_clients_lock.iter() {
                    clients.push(ready_client.clone());
                }
            };

            let proof = {
                app_proof.lock().await.clone()
            };

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
                            let _ = sender.1.lock().await.send(Message::Binary(bin_data.to_vec())).await;
                            let _ = ready_clients.lock().await.remove(&client);
                        }
                    }
                }
            }

            tokio::time::sleep(Duration::from_secs(5)).await;
        }
    });

    let (mine_success_sender, mut mine_success_receiver) = tokio::sync::mpsc::unbounded_channel::<MessageInternalMineSuccess>();

    let rpc_client = Arc::new(rpc_client);
    let app_proof = proof_ext.clone();
    let app_epoch_hashes = epoch_hashes.clone();
    let app_wallet = wallet_extension.clone();
    let app_nonce = nonce_ext.clone();
    let app_prio_fee = priority_fee.clone();
    tokio::spawn(async move {
        loop {
            let proof = {
                app_proof.lock().await.clone()
            };

            let cutoff = get_cutoff(proof, 0);
            if cutoff <= 0 {
                // process solutions
                let solution = {
                    app_epoch_hashes.read().await.best_hash.solution.clone()
                };
                if let Some(solution) = solution {
                    let signer = app_wallet.clone();
                    let mut ixs = vec![];
                    // TODO: set cu's
                    let prio_fee = {
                        app_prio_fee.lock().await.clone()
                    };

                    let cu_limit_ix = ComputeBudgetInstruction::set_compute_unit_limit(480000);
                    ixs.push(cu_limit_ix);

                    let prio_fee_ix = ComputeBudgetInstruction::set_compute_unit_price(prio_fee);
                    ixs.push(prio_fee_ix);

                    let noop_ix = get_auth_ix(signer.pubkey());
                    ixs.push(noop_ix);

                    // TODO: choose the highest balance bus
                    let bus = rand::thread_rng().gen_range(0..BUS_COUNT);
                    let difficulty = solution.to_hash().difficulty();

                    let ix_mine = get_mine_ix(signer.pubkey(), solution, bus);
                    ixs.push(ix_mine);
                    info!("Starting mine submission attempts with difficulty {}.", difficulty);
                    if let Ok((hash, _slot)) = rpc_client.get_latest_blockhash_with_commitment(rpc_client.commitment()).await {
                        let mut tx = Transaction::new_with_payer(&ixs, Some(&signer.pubkey()));

                        tx.sign(&[&signer], hash);
                        
                        for i in 0..3 {
                            info!("Sending signed tx...");
                            info!("attempt: {}", i + 1);
                            let sig = rpc_client.send_and_confirm_transaction(&tx).await;
                            if let Ok(sig) = sig {
                                // success
                                info!("Success!!");
                                info!("Sig: {}", sig);
                                // update proof
                                loop {
                                    if let Ok(loaded_proof) = get_proof(&rpc_client, signer.pubkey()).await {
                                        if proof != loaded_proof {
                                            info!("Got new proof.");
                                            let balance = (loaded_proof.balance as f64) / 10f64.powf(ORE_TOKEN_DECIMALS as f64);
                                            info!("New balance: {}", balance);
                                            let rewards = loaded_proof.balance - proof.balance;
                                            let rewards = (rewards as f64) / 10f64.powf(ORE_TOKEN_DECIMALS as f64);
                                            info!("Earned: {} ORE", rewards);

                                            let submissions = {
                                                app_epoch_hashes.read().await.submissions.clone()
                                            };

                                            let mut total_hashpower: u64 = 0;
                                            for submission in submissions.iter() {
                                                total_hashpower += submission.1.1
                                            }

                                            let _ = mine_success_sender.send(MessageInternalMineSuccess {
                                                difficulty,
                                                total_balance: balance,
                                                rewards,
                                                total_hashpower,
                                                submissions,
                                            });

                                            {
                                                let mut mut_proof = app_proof.lock().await;
                                                *mut_proof = loaded_proof;
                                                break;
                                            }
                                        }
                                    } else {
                                        tokio::time::sleep(Duration::from_millis(500)).await;
                                    }
                                }
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
                                break;
                            } else {
                                // sent error
                                if i >= 2 {
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
                                    break;
                                }
                            }
                            tokio::time::sleep(Duration::from_millis(500)).await;
                        }
                    } else {
                        error!("Failed to get latest blockhash. retrying...");
                        tokio::time::sleep(Duration::from_millis(1000)).await;
                    }
                }
            } else {
                tokio::time::sleep(Duration::from_secs(cutoff as u64)).await;
            };
        }
    });


    let app_shared_state = shared_state.clone();
    tokio::spawn(async move {
        loop {
            while let Some(msg) = mine_success_receiver.recv().await {
                {
                    let shared_state = app_shared_state.read().await;
                    for (_socket_addr, socket_sender) in shared_state.sockets.iter() {
                        let pubkey = socket_sender.0;

                        if let Some((supplied_diff, pubkey_hashpower)) = msg.submissions.get(&pubkey) {
                            let hashpower_percent = (*pubkey_hashpower as f64).div(msg.total_hashpower as f64);

                            // TODO: handle overflow/underflow and float imprecision issues
                            let decimals = 10f64.powf(ORE_TOKEN_DECIMALS as f64);
                            let earned_rewards = hashpower_percent.mul(msg.rewards).mul(decimals).floor().div(decimals);
                            let message = format!(
                                "Submitted Difficulty: {}\nPool Earned: {} ORE.\nPool Balance: {}\nMiner Earned: {} ORE for difficulty: {}",
                                msg.difficulty,
                                msg.rewards,
                                msg.total_balance,
                                earned_rewards,
                                supplied_diff
                            );
                            if let Ok(_) = socket_sender.1.lock().await.send(Message::Text(message)).await {
                            } else {
                                println!("Failed to send client text");
                            }
                        }

                    }
                }
            }
        }
    });

    let client_channel = client_message_sender.clone();
    let app_shared_state = shared_state.clone();
    let app = Router::new()
        .route("/", get(ws_handler))
        .with_state(app_shared_state)
        .layer(Extension(config))
        .layer(Extension(wallet_extension))
        .layer(Extension(client_channel))
        // Logging
        .layer(
            TraceLayer::new_for_http()
                .make_span_with(DefaultMakeSpan::default().include_headers(true))
        );


    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000")
        .await
        .unwrap();

    tracing::debug!("listening on {}", listener.local_addr().unwrap());

    let app_shared_state = shared_state.clone();
    tokio::spawn(async move {
        ping_check_system(&app_shared_state).await;
    });
    
    axum::serve(
        listener,
        app.into_make_service_with_connect_info::<SocketAddr>()
    ).await
    .unwrap();

    Ok(())
}

#[derive(Deserialize)]
struct WsQueryParams {
    timestamp: u64
}

async fn ws_handler(
    ws: WebSocketUpgrade,
    TypedHeader(auth_header): TypedHeader<axum_extra::headers::Authorization<Basic>>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    State(app_state): State<Arc<RwLock<AppState>>>,
    Extension(app_config): Extension<Arc<Mutex<Config>>>,
    Extension(client_channel): Extension<UnboundedSender<ClientMessage>>,
    query_params: Query<WsQueryParams>
) -> impl IntoResponse {

    let msg_timestamp = query_params.timestamp;

    let pubkey = auth_header.username();
    let signed_msg = auth_header.password();

    // TODO: Store pubkey data in db


    let now = SystemTime::now().duration_since(UNIX_EPOCH).expect("Time went backwards").as_secs();

    // Signed authentication message is only valid for 5 seconds
    if (now - query_params.timestamp) >= 5 {
        return Err((StatusCode::UNAUTHORIZED, "Timestamp too old."));
    }

    // verify client
    if let Ok(pubkey) = Pubkey::from_str(pubkey) {
        if let Some(whitelist) = &app_config.lock().await.whitelist {
            if whitelist.contains(&pubkey) {
                if let Ok(signature) = Signature::from_str(signed_msg) {
                    let ts_msg = msg_timestamp.to_le_bytes();
                    
                    if signature.verify(&pubkey.to_bytes(), &ts_msg) {
                        println!("Client: {addr} connected with pubkey {pubkey}.");
                        return Ok(ws.on_upgrade(move |socket| handle_socket(socket, addr, pubkey, app_state, client_channel)));
                    } else {
                        return Err((StatusCode::UNAUTHORIZED, "Sig verification failed"));
                    }
                } else {
                    return Err((StatusCode::UNAUTHORIZED, "Invalid signature"));
                }
            } else {
                return Err((StatusCode::UNAUTHORIZED, "pubkey is not authorized to mine"));
            }
        } else {
            if let Ok(signature) = Signature::from_str(signed_msg) {
                let ts_msg = msg_timestamp.to_le_bytes();
                
                if signature.verify(&pubkey.to_bytes(), &ts_msg) {
                    println!("Client: {addr} connected with pubkey {pubkey}.");
                    return Ok(ws.on_upgrade(move |socket| handle_socket(socket, addr, pubkey, app_state, client_channel)));
                } else {
                    return Err((StatusCode::UNAUTHORIZED, "Sig verification failed"));
                }
            } else {
                return Err((StatusCode::UNAUTHORIZED, "Invalid signature"));
            }
        }
    } else {
        return Err((StatusCode::UNAUTHORIZED, "Invalid pubkey"));
    }

}

async fn handle_socket(mut socket: WebSocket, who: SocketAddr, who_pubkey: Pubkey, rw_app_state: Arc<RwLock<AppState>>, client_channel: UnboundedSender<ClientMessage>) {
    if socket.send(axum::extract::ws::Message::Ping(vec![1, 2, 3])).await.is_ok() {
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
        app_state.sockets.insert(who, (who_pubkey, Mutex::new(sender)));
    }
    drop(app_state);

    let _ = tokio::spawn(async move {
        while let Some(Ok(msg)) = receiver.next().await {
            if process_message(msg, who, client_channel.clone()).is_break() {
                break;
            }
        }
    }).await;

    let mut app_state = rw_app_state.write().await;
    app_state.sockets.remove(&who);
    drop(app_state);

    info!("Client: {} disconnected!", who_pubkey.to_string());
}

fn process_message(msg: Message, who: SocketAddr, client_channel: UnboundedSender<ClientMessage>) -> ControlFlow<(), ()> {
    match msg {
        Message::Text(t) => {
            println!(">>> {who} sent str: {t:?}");
        },
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

                    let now = SystemTime::now().duration_since(UNIX_EPOCH).expect("Time went backwards").as_secs();


                    let time_since = now - ts;
                    if time_since > 5 {
                        error!("Client tried to ready up with expired signed message");
                        return ControlFlow::Break(());
                    }

                    let msg = ClientMessage::Ready(who);
                    let _ = client_channel.send(msg);
                },
                1 => {
                    let msg = ClientMessage::Mining(who);
                    let _ = client_channel.send(msg);
                },
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

                },
                _ => {
                    println!(">>> {} sent an invalid message", who);
                }
            }

        },
        Message::Close(c) => {
            if let Some(cf) = c {
                println!(
                    ">>> {} sent close with code {} and reason `{}`",
                    who, cf.code, cf.reason
                );
            } else {
                println!(">>> {who} somehow sent close message without CloseFrame");
            }
            return ControlFlow::Break(())
        },
        Message::Pong(_v) => {
            //println!(">>> {who} sent pong with {v:?}");
        },
        Message::Ping(_v) => {
            //println!(">>> {who} sent ping with {v:?}");
        },
    }

    ControlFlow::Continue(())
}

async fn client_message_handler_system(
    mut receiver_channel: UnboundedReceiver<ClientMessage>,
    shared_state: &Arc<RwLock<AppState>>,
    ready_clients: Arc<Mutex<HashSet<SocketAddr>>>,
    proof: Arc<Mutex<Proof>>,
    epoch_hashes: Arc<RwLock<EpochHashes>>
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

                        if let Ok(_) = sender.1.lock().await.send(Message::Text(String::from("Client successfully added."))).await {
                        } else {
                            println!("Failed notify client they were readied up!");
                        }
                    }
                }
            },
            ClientMessage::Mining(addr) => {
                println!("Client {} has started mining!", addr.to_string());
            },
            ClientMessage::BestSolution(_addr, solution, pubkey) => {
                let pubkey_str = pubkey.to_string();
                let challenge = {
                    let proof = proof.lock().await;
                    proof.challenge
                };

                if solution.is_valid(&challenge) {
                    let diff = solution.to_hash().difficulty();
                    println!("{} found diff: {}", pubkey_str, diff);
                    if diff >= MIN_DIFF {
                        // calculate rewards
                        let hashpower = MIN_HASHPOWER * 2u64.pow(diff - MIN_DIFF);
                        {
                            let mut epoch_hashes = epoch_hashes.write().await;
                            epoch_hashes.submissions.insert(pubkey, (diff, hashpower));
                            if diff > epoch_hashes.best_hash.difficulty {
                                epoch_hashes.best_hash.difficulty = diff;
                                epoch_hashes.best_hash.solution = Some(solution);
                            }
                        }

                    } else {
                        println!("Diff to low, skipping");
                    }
                } else {
                    println!("{} returned an invalid solution!", pubkey);
                }
            }
        }
    }
}

async fn ping_check_system(
    shared_state: &Arc<RwLock<AppState>>,
) {
    loop {
        // send ping to all sockets
        let mut failed_sockets = Vec::new();
        let app_state = shared_state.read().await;
        // I don't like doing all this work while holding this lock...
        for (who, socket) in app_state.sockets.iter() {
            if socket.1.lock().await.send(Message::Ping(vec![1, 2, 3])).await.is_ok() {
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
