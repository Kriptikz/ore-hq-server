use std::{collections::HashMap, net::SocketAddr, ops::ControlFlow, path::Path, sync::Arc, time::Duration};

use axum::{extract::{ws::{Message, WebSocket}, ConnectInfo, State, WebSocketUpgrade}, response::IntoResponse, routing::get, Extension, Router};
use futures::{stream::SplitSink, SinkExt, StreamExt};
use ore_utils::{get_proof, get_register_ix};
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::{commitment_config::CommitmentConfig, native_token::LAMPORTS_PER_SOL, signature::read_keypair_file, signer::Signer, transaction::Transaction};
use tokio::sync::{mpsc::UnboundedSender, Mutex};
use tower_http::trace::{DefaultMakeSpan, TraceLayer};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

struct AppState {
    sockets: HashMap<SocketAddr, SplitSink<WebSocket, Message>>
}

#[derive(Debug)]
pub enum ClientMessage {
    Ready(SocketAddr),
    Mining(SocketAddr),
    BestSolution(SocketAddr)
}

mod ore_utils;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    dotenv::dotenv().ok();
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

    let wallet_extension = Arc::new(wallet);
    let _proof = Arc::new(Mutex::new(proof));
    let _nonce = Arc::new(Mutex::new(0u64));

    let (client_message_sender, mut client_message_receiver) = tokio::sync::mpsc::unbounded_channel::<ClientMessage>();
    let client_channel = client_message_sender.clone();

    tokio::spawn(async move {
        while let Some(client_message) = client_message_receiver.recv().await {
            match client_message {
                ClientMessage::Ready(addr) => {
                    println!("Client {} is ready!", addr.to_string());
                },
                ClientMessage::Mining(addr) => {
                    println!("Client {} has started mining!", addr.to_string());
                },
                ClientMessage::BestSolution(addr) => {
                    println!("Client {} found a solution.", addr);
                }
            }
        }
    });

    let shared_state = Arc::new(Mutex::new(AppState {
        sockets: HashMap::new(),
    }));

    let app_shared_state = shared_state.clone();
    let app = Router::new()
        .route("/", get(ws_handler))
        .with_state(app_shared_state)
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
        loop {
            // send ping to all sockets
            {
                let mut failed_sockets = Vec::new();
                let mut app_state = app_shared_state.lock().await;
                for (who, socket) in app_state.sockets.iter_mut() {
                    if socket.send(Message::Ping(vec![1, 2, 3])).await.is_ok() {
                        println!("Pinged: {who}...");
                    } else {
                        failed_sockets.push(who.clone());
                    }
                }

                // remove any sockets where ping failed
                for address in failed_sockets {
                     app_state.sockets.remove(&address);
                }
            }

            // sleep for 10 seconds
            tokio::time::sleep(Duration::from_secs(10)).await;
        }
    });
    
    axum::serve(
        listener,
        app.into_make_service_with_connect_info::<SocketAddr>()
    ).await
    .unwrap();

    Ok(())
}

async fn ws_handler(
    ws: WebSocketUpgrade,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    State(app_state): State<Arc<Mutex<AppState>>>,
    Extension(client_channel): Extension<UnboundedSender<ClientMessage>>
) -> impl IntoResponse {

    println!("Client: {addr} connected.");


    ws.on_upgrade(move |socket| handle_socket(socket, addr, app_state, client_channel))
}

async fn handle_socket(mut socket: WebSocket, who: SocketAddr, app_state: Arc<Mutex<AppState>>, client_channel: UnboundedSender<ClientMessage>) {
    if socket.send(axum::extract::ws::Message::Ping(vec![1, 2, 3])).await.is_ok() {
        println!("Pinged {who}...");
    } else {
        println!("could not ping {who}");

        // if we can't ping we can't do anything, return to close the connection
        return;
    }

    let (sender, mut receiver) = socket.split();
    {
        let mut app_state = app_state.lock().await;
        if app_state.sockets.contains_key(&who) {
            println!("Socket addr: {who} already has an active connection");
        } else {
            app_state.sockets.insert(who, sender);
        }
    }

    tokio::spawn(async move {
        while let Some(Ok(msg)) = receiver.next().await {
            if process_message(msg, who, client_channel.clone()).is_break() {
                break;
            }
        }
    }).await;

    println!("Websocket context {who} destroyed");
}

fn process_message(msg: Message, who: SocketAddr, client_channel: UnboundedSender<ClientMessage>) -> ControlFlow<(), ()> {
    match msg {
        Message::Text(t) => {
            println!(">>> {who} sent str: {t:?}");
        },
        Message::Binary(d) => {
            // first 8 bytes are message type
            if d.len() > 1 {
                println!(">>> {} send too many bytes!", who);
            } else {
                let message_type = d[0];
                match message_type {
                    0 => {
                        let msg = ClientMessage::Ready(who);
                        let _ = client_channel.send(msg);
                    },
                    1 => {
                        let msg = ClientMessage::Mining(who);
                        let _ = client_channel.send(msg);
                    },
                    2 => {
                        let msg = ClientMessage::BestSolution(who);
                        let _ = client_channel.send(msg);
                    },
                    _ => {
                        println!(">>> {} sent an invalid message", who);
                    }
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
        Message::Pong(v) => {
            println!(">>> {who} sent pong with {v:?}");
        },
        Message::Ping(v) => {
            println!(">>> {who} sent ping with {v:?}");
        },
    }

    ControlFlow::Continue(())
}
