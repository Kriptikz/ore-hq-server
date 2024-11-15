use std::{
    collections::{HashMap, HashSet},
    net::SocketAddr,
    ops::Range,
    sync::Arc,
};


use ore_api::state::Proof;
use solana_sdk::pubkey::Pubkey;
use tokio::{
    sync::{mpsc::UnboundedReceiver, Mutex, RwLock},
    time::Instant,
};

use crate::{
    AppState, ClientMessage, EpochHashes, LastPong, SubmissionWindow,
};

use super::client_submissions_handler::{client_submissions_handler, ClientBestSolution};

pub async fn client_message_handler_system(
    mut receiver_channel: UnboundedReceiver<ClientMessage>,
    ready_clients: Arc<Mutex<HashSet<SocketAddr>>>,
    proof: Arc<Mutex<Proof>>,
    epoch_hashes: Arc<RwLock<EpochHashes>>,
    client_nonce_ranges: Arc<RwLock<HashMap<Pubkey, Vec<Range<u64>>>>>,
    app_state: Arc<RwLock<AppState>>,
    app_pongs: Arc<RwLock<LastPong>>,
    app_submission_window: Arc<RwLock<SubmissionWindow>>,
) {
    let (s, r) = tokio::sync::mpsc::unbounded_channel::<ClientBestSolution>();

    let app_proof = proof.clone();
    let app_epoch_hashes = epoch_hashes.clone();
    let app_client_nonce_ranges = client_nonce_ranges.clone();
    let app_app_state = app_state.clone();
    let app_app_submission_window = app_submission_window.clone();
    tokio::spawn(async move {
        client_submissions_handler(
            r,
            app_proof,
            app_epoch_hashes,
            app_client_nonce_ranges,
            app_app_state,
            app_app_submission_window
        ).await;
    });

    while let Some(client_message) = receiver_channel.recv().await {
        match client_message {
            ClientMessage::Pong(addr) => {
                let mut writer = app_pongs.write().await;
                writer.pongs.insert(addr, Instant::now());
                drop(writer);
            }
            ClientMessage::Ready(addr) => {
                let ready_clients = ready_clients.clone();
                let mut lock = ready_clients.lock().await;
                lock.insert(addr);
                drop(lock);
            }
            ClientMessage::Mining(addr) => {
                tracing::info!(target: "server_log", "Client {} has started mining!", addr.to_string());
            }
            ClientMessage::BestSolution(addr, solution, pubkey) => {
                let _ = s.send(ClientBestSolution {
                    data: (addr, solution, pubkey)
                });
            }
        }
    }
}
