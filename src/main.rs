use std::{collections::HashMap, net::SocketAddr, ops::ControlFlow, sync::Arc, time::Duration};

use axum::{extract::{ws::{Message, WebSocket}, ConnectInfo, State, WebSocketUpgrade}, response::IntoResponse, routing::get, Router};
use futures::{stream::SplitSink, SinkExt, StreamExt};
use tokio::sync::Mutex;
use tower_http::trace::{DefaultMakeSpan, TraceLayer};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

struct AppState {
    sockets: HashMap<SocketAddr, SplitSink<WebSocket, Message>>
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {

    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "ore_hq_server=debug,tower_http=debug".into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    let shared_state = Arc::new(Mutex::new(AppState {
        sockets: HashMap::new(),
    }));

    let app_shared_state = shared_state.clone();
    let app = Router::new()
        .route("/", get(ws_handler))
        .with_state(app_shared_state)
        // Logging
        .layer(
            TraceLayer::new_for_http()
                .make_span_with(DefaultMakeSpan::default().include_headers(true))
        );


    let listener = tokio::net::TcpListener::bind("127.0.0.1:3000")
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
) -> impl IntoResponse {

    println!("Client: {addr} connected.");


    ws.on_upgrade(move |socket| handle_socket(socket, addr, app_state))
}

async fn handle_socket(mut socket: WebSocket, who: SocketAddr, app_state: Arc<Mutex<AppState>>) {
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

    let _recv_task = tokio::spawn(async move {
        let mut count = 0;
        while let Some(Ok(msg)) = receiver.next().await {
            count += 1;
            if process_message(msg, who).is_break() {
                break;
            }
        }

        count
    }).await;

    println!("Websocket context {who} destroyed");
}

fn process_message(msg: Message, who: SocketAddr) -> ControlFlow<(), ()> {
    match msg {
        Message::Text(t) => {
            println!(">>> {who} sent str: {t:?}");
        },
        Message::Binary(d) => {
            println!(">>> {} sent {} bytes: {:?}", who, d.len(), d);
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
