use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::{commitment_config::{CommitmentConfig, CommitmentLevel}, compute_budget::ComputeBudgetInstruction, signature::Keypair, signer::Signer, transaction::Transaction};
use spl_associated_token_account::get_associated_token_address;
use tracing::{error, info};

pub async fn migrate(rpc_client: &RpcClient, wallet: &Keypair, original_proof_balance: u64, ore_token_account_balance: u64) -> Result<(), String> {

    let miner_ore_token_account_addr = get_associated_token_address(&wallet.pubkey(), &ore_api::consts::MINT_ADDRESS);

    // Claim from original ore proof
    let mut ixs = Vec::new();
    let prio_fee_ix = ComputeBudgetInstruction::set_compute_unit_price(20_000);
    ixs.push(prio_fee_ix);
    let claim_ix = ore_api::instruction::claim(wallet.pubkey(), miner_ore_token_account_addr, original_proof_balance);

    ixs.push(claim_ix);
    let mut tx = Transaction::new_with_payer(&ixs, Some(&wallet.pubkey()));

    let blockhash = rpc_client
        .get_latest_blockhash()
        .await
        .expect("should get latest blockhash");

    tx.sign(&[&wallet], blockhash);

    info!("Claiming from original proof.");
    match rpc_client.send_and_confirm_transaction_with_spinner_and_commitment(&tx, CommitmentConfig {
        commitment: CommitmentLevel::Confirmed
    }).await {
        Ok(_) => {
            info!("Successfully claimed from original proof.");
        },
        Err(e) => {
            error!("Failed to send and confirm tx.\n E: {:?}", e);
            return Err("Failed to claim from original proof".to_string());
        }
    }

    // Delegate stake to new proof
    let mut ixs = Vec::new();
    let prio_fee_ix = ComputeBudgetInstruction::set_compute_unit_price(20_000);
    ixs.push(prio_fee_ix);
    let stake_ix = crate::ore_utils::get_stake_ix(wallet.pubkey(), wallet.pubkey(), original_proof_balance.saturating_add(ore_token_account_balance));

    ixs.push(stake_ix);
    let mut tx = Transaction::new_with_payer(&ixs, Some(&wallet.pubkey()));

    let blockhash = rpc_client
        .get_latest_blockhash()
        .await
        .expect("should get latest blockhash");

    tx.sign(&[&wallet], blockhash);

    info!("Staking to new proof.");
    match rpc_client.send_and_confirm_transaction_with_spinner_and_commitment(&tx, CommitmentConfig {
        commitment: CommitmentLevel::Confirmed
    }).await {
        Ok(_) => {
            info!("Successfully staked to new proof.");
        },
        Err(e) => {
            error!("Failed to send and confirm tx.\n E: {:?}", e);
            return Err("Failed to stake to new proof".to_string());
        }
    }

    Ok(())
}
