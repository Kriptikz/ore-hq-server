use std::{
    collections::{HashMap, HashSet},
    net::SocketAddr,
    ops::Range,
    sync::Arc,
};

use drillx::Solution;
use uuid::Uuid;

use axum::extract::ws::Message;
use futures::SinkExt;
use ore_api::state::Proof;
use solana_sdk::pubkey::Pubkey;
use tokio::sync::{mpsc::UnboundedReceiver, Mutex, RwLock};

use crate::{
    AppState, EpochHashes, InternalMessageSubmission, SubmissionWindow, MAX_CALCULATED_HASHPOWER, MIN_DIFF, MIN_HASHPOWER
};

pub struct ClientBestSolution {
    pub data: (SocketAddr, Solution, Pubkey),
}

pub async fn client_submissions_handler(
    mut receiver_channel: UnboundedReceiver<ClientBestSolution>,
    proof: Arc<Mutex<Proof>>,
    epoch_hashes: Arc<RwLock<EpochHashes>>,
    client_nonce_ranges: Arc<RwLock<HashMap<Pubkey, Vec<Range<u64>>>>>,
    app_state: Arc<RwLock<AppState>>,
    app_submission_window: Arc<RwLock<SubmissionWindow>>,
) {
    while let Some(msg) = receiver_channel.recv().await {
        let (addr, solution, pubkey) = msg.data;
        let diff = solution.to_hash().difficulty();
        if diff >= MIN_DIFF {
            let reader = app_submission_window.read().await;
            let submission_windows_closed = reader.closed;
            drop(reader);

            if submission_windows_closed {
                //tracing::error!(target: "server_log", "{} submitted after submission window was closed!", pubkey);

                let reader = app_state.read().await;
                if let Some(app_client_socket) = reader.sockets.get(&addr) {
                    let msg = format!("Late submission. Please make sure your hash time is under 60 seconds.");
                    let _ = app_client_socket
                        .socket
                        .lock()
                        .await
                        .send(Message::Text(msg))
                        .await;
                } else {
                    //tracing::error!(target: "server_log", "Failed to get client socket for addr: {}", addr);
                    continue;
                }
                drop(reader);
                continue;
            }

            let reader = client_nonce_ranges.read().await;
            let nonce_ranges: Vec<Range<u64>> = {
                if let Some(nr) = reader.get(&pubkey) {
                    nr.clone()
                } else {
                    //tracing::error!(target: "server_log", "Client nonce range not set!");
                    continue;
                }
            };
            drop(reader);

            let nonce = u64::from_le_bytes(solution.n);

            let mut in_range = false;

            for nonce_range in nonce_ranges.iter() {
                if nonce_range.contains(&nonce) {
                    in_range = true;
                    break;
                }
            }

            if !in_range {
                //tracing::error!(target: "server_log", "Client submitted nonce out of assigned range");
                continue;
            }

            let reader = app_state.read().await;
            let miner_id;
            if let Some(app_client_socket) = reader.sockets.get(&addr) {
                miner_id = app_client_socket.miner_id;
            } else {
                //tracing::error!(target: "server_log", "Failed to get client socket for addr: {}", addr);
                continue;
            }
            drop(reader);

            let lock = proof.lock().await;
            let challenge = lock.challenge;
            drop(lock);

            if solution.is_valid(&challenge) {
                let submission_uuid = Uuid::new_v4();
                //tracing::info!(target: "submission_log", "{} - {} found diff: {}", submission_uuid, pubkey_str, diff);
                // calculate rewards
                let mut hashpower = MIN_HASHPOWER * 2u64.pow(diff - MIN_DIFF);
                if hashpower > MAX_CALCULATED_HASHPOWER {
                    hashpower = MAX_CALCULATED_HASHPOWER;
                }
                {
                    let reader = epoch_hashes.read().await;
                    let subs = reader.submissions.clone();
                    drop(reader);

                    if let Some(old_sub) = subs.get(&pubkey) {
                        if diff > old_sub.supplied_diff {
                            let mut epoch_hashes = epoch_hashes.write().await;
                            epoch_hashes.submissions.insert(
                                pubkey,
                                InternalMessageSubmission {
                                    miner_id,
                                    supplied_nonce: nonce,
                                    supplied_diff: diff,
                                    hashpower,
                                },
                            );
                            if diff > epoch_hashes.best_hash.difficulty {
                                tracing::info!(target: "server_log", "{} - New best diff: {}", submission_uuid, diff);
                                //tracing::info!(target: "submission_log", "{} - New best diff: {}", submission_uuid, diff);
                                epoch_hashes.best_hash.difficulty = diff;
                                epoch_hashes.best_hash.solution = Some(solution);
                            }
                            drop(epoch_hashes);
                        }
                    } else {
                        //tracing::info!(target: "submission_log", "{} - Adding {} submission diff: {} to epoch_hashes submissions.", submission_uuid, pubkey_str, diff);
                        let mut epoch_hashes = epoch_hashes.write().await;
                        epoch_hashes.submissions.insert(
                            pubkey,
                            InternalMessageSubmission {
                                miner_id,
                                supplied_nonce: nonce,
                                supplied_diff: diff,
                                hashpower,
                            },
                        );
                        if diff > epoch_hashes.best_hash.difficulty {
                            tracing::info!(target: "server_log", "{} - New best diff: {}", submission_uuid, diff);
                            //tracing::info!(target: "submission_log", "{} - New best diff: {}", submission_uuid, diff);
                            epoch_hashes.best_hash.difficulty = diff;
                            epoch_hashes.best_hash.solution = Some(solution);
                        }
                        drop(epoch_hashes);
                        //tracing::info!(target: "submission_log", "{} - Added {} submission diff: {} to epoch_hashes submissions.", submission_uuid, pubkey_str, diff);
                    }
                }
            } else {
                tracing::error!(target: "server_log", "{} returned an invalid solution!", pubkey);

                let reader = app_state.read().await;
                if let Some(app_client_socket) = reader.sockets.get(&addr) {
                    let _ = app_client_socket.socket.lock().await.send(Message::Text("Invalid solution. If this keeps happening, please contact support.".to_string())).await;
                } else {
                    //tracing::error!(target: "server_log", "Failed to get client socket for addr: {}", addr);
                    continue;
                }
                drop(reader);
            }
        } else {
            tracing::error!(target: "server_log", "Diff to low, skipping");
        }
    }
}
