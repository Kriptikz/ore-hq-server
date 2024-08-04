use std::time::{Instant, SystemTime, UNIX_EPOCH};

use crossbeam_channel::{Receiver, Sender};
use drillx::{equix, Hash, Solution};
use ore_api::{
    ID as ORE_ID,
    instruction,
    state::{Proof, Treasury},
    consts::{BUS_ADDRESSES, CONFIG_ADDRESS, EPOCH_DURATION, MINT_ADDRESS, PROOF,
    TOKEN_DECIMALS, TREASURY_ADDRESS }
};
pub use ore_utils::AccountDeserialize;
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::{
    account::ReadableAccount, clock::Clock, instruction::Instruction, pubkey::Pubkey, sysvar,
};
use spl_associated_token_account::get_associated_token_address;
use tracing::error;

pub const ORE_TOKEN_DECIMALS: u8 = TOKEN_DECIMALS;

#[derive(Debug)]
pub enum MiningDataChannelMessage {
    Stop,
}

pub fn get_auth_ix(signer: Pubkey, ) -> Instruction {
    let proof = proof_pubkey(signer);

    instruction::auth(proof)
}

pub fn get_mine_ix(signer: Pubkey, solution: Solution, bus: usize) -> Instruction {
    instruction::mine(signer, signer, BUS_ADDRESSES[bus], solution)
}

pub fn get_register_ix(signer: Pubkey) -> Instruction {
    instruction::open(signer, signer, signer)
}

pub fn get_reset_ix(signer: Pubkey) -> Instruction {
    instruction::reset(signer)
}

pub fn get_claim_ix(signer: Pubkey, beneficiary: Pubkey, claim_amount: u64) -> Instruction {
    instruction::claim(signer, beneficiary, claim_amount)
}

pub fn get_stake_ix(signer: Pubkey, sender: Pubkey, stake_amount: u64) -> Instruction {
    instruction::stake(signer, sender, stake_amount)
}

pub fn get_ore_mint() -> Pubkey {
    MINT_ADDRESS
}

pub fn get_ore_epoch_duration() -> i64 {
    EPOCH_DURATION
}

pub fn get_ore_decimals() -> u8 {
    TOKEN_DECIMALS
}

pub async fn get_proof_and_treasury_with_busses(
    client: &RpcClient,
    authority: Pubkey,
) -> (
    Result<Proof, ()>,
    Result<Treasury, ()>,
    Result<ore_api::state::Config, ()>,
    Result<Vec<Result<ore_api::state::Bus, ()>>, ()>,
) {
    let account_pubkeys = vec![
        TREASURY_ADDRESS,
        proof_pubkey(authority),
        CONFIG_ADDRESS,
        BUS_ADDRESSES[0],
        BUS_ADDRESSES[1],
        BUS_ADDRESSES[2],
        BUS_ADDRESSES[3],
        BUS_ADDRESSES[4],
        BUS_ADDRESSES[5],
        BUS_ADDRESSES[6],
        BUS_ADDRESSES[7],
    ];
    let datas = client.get_multiple_accounts(&account_pubkeys).await;
    if let Ok(datas) = datas {
        let treasury = if let Some(data) = &datas[0] {
            Ok(*Treasury::try_from_bytes(data.data()).expect("Failed to parse treasury account"))
        } else {
            Err(())
        };

        let proof = if let Some(data) = &datas[1] {
            Ok(*Proof::try_from_bytes(data.data()).expect("Failed to parse treasury account"))
        } else {
            Err(())
        };

        let treasury_config = if let Some(data) = &datas[2] {
            Ok(*ore_api::state::Config::try_from_bytes(data.data())
                .expect("Failed to parse config account"))
        } else {
            Err(())
        };
        let bus_1 = if let Some(data) = &datas[3] {
            Ok(*ore_api::state::Bus::try_from_bytes(data.data())
                .expect("Failed to parse bus1 account"))
        } else {
            Err(())
        };
        let bus_2 = if let Some(data) = &datas[4] {
            Ok(*ore_api::state::Bus::try_from_bytes(data.data())
                .expect("Failed to parse bus2 account"))
        } else {
            Err(())
        };
        let bus_3 = if let Some(data) = &datas[5] {
            Ok(*ore_api::state::Bus::try_from_bytes(data.data())
                .expect("Failed to parse bus3 account"))
        } else {
            Err(())
        };
        let bus_4 = if let Some(data) = &datas[6] {
            Ok(*ore_api::state::Bus::try_from_bytes(data.data())
                .expect("Failed to parse bus4 account"))
        } else {
            Err(())
        };
        let bus_5 = if let Some(data) = &datas[7] {
            Ok(*ore_api::state::Bus::try_from_bytes(data.data())
                .expect("Failed to parse bus5 account"))
        } else {
            Err(())
        };
        let bus_6 = if let Some(data) = &datas[8] {
            Ok(*ore_api::state::Bus::try_from_bytes(data.data())
                .expect("Failed to parse bus6 account"))
        } else {
            Err(())
        };
        let bus_7 = if let Some(data) = &datas[9] {
            Ok(*ore_api::state::Bus::try_from_bytes(data.data())
                .expect("Failed to parse bus7 account"))
        } else {
            Err(())
        };
        let bus_8 = if let Some(data) = &datas[10] {
            Ok(*ore_api::state::Bus::try_from_bytes(data.data())
                .expect("Failed to parse bus1 account"))
        } else {
            Err(())
        };

        (proof, treasury, treasury_config, Ok(vec![bus_1, bus_2, bus_3, bus_4, bus_5, bus_6, bus_7, bus_8]))
    } else {
        (Err(()), Err(()), Err(()), Err(()))
    }
}

pub async fn get_treasury(client: &RpcClient) -> Result<Treasury, ()> {
    let data = client.get_account_data(&TREASURY_ADDRESS).await;
    if let Ok(data) = data {
        Ok(*Treasury::try_from_bytes(&data).expect("Failed to parse treasury account"))
    } else {
        Err(())
    }
}

pub async fn get_proof(client: &RpcClient, authority: Pubkey) -> Result<Proof, String> {
    let proof_address = proof_pubkey(authority);
    let data = client.get_account_data(&proof_address).await;
    match data {
        Ok(data) => {
            let proof = Proof::try_from_bytes(&data);
            if let Ok(proof) = proof {
                return Ok(*proof)
            } else {
                return Err("Failed to parse proof account".to_string())
            }
        }
        Err(_) => return Err("Failed to get proof account".to_string()),
    }
}

pub fn proof_pubkey(authority: Pubkey) -> Pubkey {
    Pubkey::find_program_address(&[PROOF, authority.as_ref()], &ORE_ID).0
}

pub fn treasury_tokens_pubkey() -> Pubkey {
    get_associated_token_address(&TREASURY_ADDRESS, &MINT_ADDRESS)
}

pub async fn get_clock_account(client: &RpcClient) -> Result<Clock, ()> {
    if let Ok(data) = client.get_account_data(&sysvar::clock::ID).await {
        if let Ok(data) = bincode::deserialize::<Clock>(&data) {
            Ok(data)
        } else {
            Err(())
        }
    } else {
        Err(())
    }
}

pub fn get_cutoff(proof: Proof, buffer_time: u64) -> i64 {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Failed to get time")
        .as_secs() as i64;
    proof
        .last_hash_at
        .saturating_add(60)
        .saturating_sub(buffer_time as i64)
        .saturating_sub(now)
}

pub fn find_hash_par(proof: Proof, cutoff_time: u64, threads: u64, min_difficulty: u32, mining_messages_reciever: Receiver<MiningDataChannelMessage>, mining_messages_sender: Sender<MiningDataChannelMessage>) -> (Solution, u32, Hash, u64) {
    let handles = (0..threads)
        .map(|i| {
            std::thread::spawn({
                let proof = proof.clone();
                let message_receiver = mining_messages_reciever.clone();
                let message_sender = mining_messages_sender.clone();
                let mut memory = equix::SolverMemory::new();
                move || {
                    let timer = Instant::now();
                    let first_nonce = u64::MAX.saturating_div(threads).saturating_mul(i);
                    let mut nonce = first_nonce;
                    let mut best_nonce = nonce;
                    let mut best_difficulty = 0;
                    let mut best_hash = Hash::default();
                    let mut total_hashes: u64 = 0;
                    loop {
                        // Create hash
                        if let Ok(hash) = drillx::hash_with_memory(
                            &mut memory,
                            &proof.challenge,
                            &nonce.to_le_bytes(),
                        ) {
                            total_hashes += 1;
                            let difficulty = hash.difficulty();
                            if difficulty.gt(&best_difficulty) {
                                    best_nonce = nonce;
                                    best_difficulty = difficulty;
                                    best_hash = hash;
                            }
                        }



                        if let Ok(message) = message_receiver.try_recv() {
                            match message {
                                MiningDataChannelMessage::Stop => {
                                    // messages are only received by one receiver. 
                                    // try to send another message for any remaining receivers.
                                    let _ = message_sender.try_send(MiningDataChannelMessage::Stop);
                                    break;
                                }
                            }
                        }

                        // Exit if time has elapsed
                        if nonce % 100 == 0 {
                            if timer.elapsed().as_secs().ge(&cutoff_time) {
                                if best_difficulty.gt(&min_difficulty) {
                                    // Mine until min difficulty has been met
                                    // Stop all other threads since time has elapsed and the minimum difficulty has been found
                                    let _ = message_sender.try_send(MiningDataChannelMessage::Stop);
                                    break;
                                }
                            } 
                        }

                        // Increment nonce
                        nonce += 1;
                    }
                    
                    // Return the best nonce
                    Some((best_nonce, best_difficulty, best_hash, total_hashes))
                }
            })
        })
        .collect::<Vec<_>>();

    // Join handles and return best nonce
    let mut best_nonce: u64 = 0;
    let mut best_difficulty = 0;
    let mut best_hash = Hash::default();
    let mut total_nonces_checked = 0;
    for h in handles {
        if let Ok(Some((nonce, difficulty, hash, nonces_checked))) = h.join() {
            total_nonces_checked += nonces_checked;
            if difficulty > best_difficulty {
                best_difficulty = difficulty;
                best_nonce = nonce;
                best_hash = hash;
            }
        } else {
            error!("Failed to join a thread handle!");
        }
    }
    (Solution::new(best_hash.d, best_nonce.to_le_bytes()), best_difficulty, best_hash, total_nonces_checked)
}
