use std::{
    collections::{HashMap, HashSet},
    net::SocketAddr,
    ops::Range,
    sync::Arc,
    time::Duration,
};

use axum::extract::ws::Message;
use futures::SinkExt;
use ore_api::state::Proof;
use solana_sdk::pubkey::Pubkey;
use tokio::sync::{Mutex, RwLock};

use crate::{message::ServerMessageStartMining, ore_utils::get_cutoff, AppState, EpochHashes, SubmissionWindow};

const NONCE_RANGE_SIZE: u64 = 40_000_000;

pub async fn handle_ready_clients_system(
    app_state: Arc<RwLock<AppState>>,
    app_proof: Arc<Mutex<Proof>>,
    app_epoch_hashes: Arc<RwLock<EpochHashes>>,
    ready_clients: Arc<Mutex<HashSet<SocketAddr>>>,
    app_nonce: Arc<Mutex<u64>>,
    app_client_nonce_ranges: Arc<RwLock<HashMap<Pubkey, Vec<Range<u64>>>>>,
    app_submission_window: Arc<RwLock<SubmissionWindow>>,
) {
    tracing::info!(target: "server_log", "handle ready clients system started!");
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

                let cutoff = get_cutoff(latest_proof, 4);
                let mut should_mine = true;

                // only distribute challenge if 10 seconds or more is left
                // or if there is no best_hash yet
                let cutoff = if cutoff < 10 {
                    let solution = app_epoch_hashes.read().await.best_hash.solution;
                    if solution.is_some() {
                        should_mine = false;
                    }
                    0
                } else {
                    cutoff
                };

                let reader = app_submission_window.read().await;
                let is_window_closed = reader.closed;
                drop(reader);

                if should_mine && !is_window_closed {
                    let r_clients_len = clients.len();
                    //tracing::info!(target: "server_log", "Handling {} ready clients.", r_clients_len);
                    let lock = app_proof.lock().await;
                    let latest_proof = lock.clone();
                    drop(lock);
                    let challenge = latest_proof.challenge;

                    // tracing::info!(target: "submission_log", "Giving clients challenge: {}", BASE64_STANDARD.encode(challenge));
                    // tracing::info!(target: "submission_log", "With cutoff: {}", cutoff);
                    let shared_state = app_state.read().await;
                    let sockets = shared_state.sockets.clone();
                    drop(shared_state);
                    for client in clients {
                        let app_client_nonce_ranges = app_client_nonce_ranges.clone();
                        if let Some(sender) = sockets.get(&client) {
                            let nonce_range = {
                                let mut nonce = app_nonce.lock().await;
                                let start = *nonce;
                                *nonce += NONCE_RANGE_SIZE;
                                drop(nonce);
                                // max hashes possible in 60s for a single client
                                //
                                let nonce_end = start + NONCE_RANGE_SIZE - 1;
                                let end = nonce_end;
                                start..end
                            };

                            let start_mining_message = ServerMessageStartMining::new(
                                challenge,
                                cutoff,
                                nonce_range.start,
                                nonce_range.end,
                            );
                            let sender = sender.clone();
                            tokio::spawn(async move {
                                let _ = sender
                                    .socket
                                    .lock()
                                    .await
                                    .send(Message::Binary(start_mining_message.to_message_binary()))
                                    .await;
                                let reader = app_client_nonce_ranges.read().await;
                                let current_nonce_ranges =
                                    if let Some(val) = reader.get(&sender.pubkey) {
                                        Some(val.clone())
                                    } else {
                                        None
                                    };
                                drop(reader);

                                if let Some(nonce_ranges) = current_nonce_ranges {
                                    let mut new_nonce_ranges = nonce_ranges.to_vec();
                                    new_nonce_ranges.push(nonce_range);

                                    app_client_nonce_ranges
                                        .write()
                                        .await
                                        .insert(sender.pubkey, new_nonce_ranges);
                                } else {
                                    let new_nonce_ranges = vec![nonce_range];
                                    app_client_nonce_ranges
                                        .write()
                                        .await
                                        .insert(sender.pubkey, new_nonce_ranges);
                                }
                            });
                        }
                        // remove ready client from list
                        let _ = ready_clients.lock().await.remove(&client);
                    }
                    //tracing::info!(target: "server_log", "Handled {} ready clients.", r_clients_len);
                }
            }
        } else {
            tracing::info!(target: "server_log", "Mining is paused");
            tokio::time::sleep(Duration::from_secs(30)).await;
        }

        tokio::time::sleep(Duration::from_millis(400)).await;
    }
}
