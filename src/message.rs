pub struct ServerMessageStartMining {
    challenge: [u8; 32],
    cutoff: i64,
    nonce_range_start: u64,
    nonce_range_end: u64,
}

impl ServerMessageStartMining {
    pub fn new(
        challenge: [u8; 32],
        cutoff: i64,
        nonce_range_start: u64,
        nonce_range_end: u64,
    ) -> Self {
        ServerMessageStartMining {
            challenge,
            cutoff,
            nonce_range_start,
            nonce_range_end
        }
    }

    pub fn to_message_binary(&self) -> Vec<u8> {
        let mut bin_data = Vec::new();
        bin_data.push(0u8);
        bin_data.extend_from_slice(&self.challenge);
        bin_data.extend_from_slice(&self.cutoff.to_le_bytes());
        bin_data.extend_from_slice(&self.nonce_range_start.to_le_bytes());
        bin_data.extend_from_slice(&self.nonce_range_end.to_le_bytes());

        bin_data
    }
}

pub struct ServerMessagePoolSubmissionResult {
    difficulty: u32,
    total_balance: f64,
    total_rewards: f64,
    top_stake: f64,
    multiplier: f64,
    active_miners: u32,
    challenge: [u8; 32],
    best_nonce: u64,
    miner_supplied_difficulty: u32,
    miner_earned_rewards: f64,
    miner_percentage: f64
}

impl ServerMessagePoolSubmissionResult {
    pub fn new(
        difficulty: u32,
        total_balance: f64,
        total_rewards: f64,
        top_stake: f64,
        multiplier: f64,
        active_miners: u32,
        challenge: [u8; 32],
        best_nonce: u64,
        miner_supplied_difficulty: u32,
        miner_earned_rewards: f64,
        miner_percentage: f64,
    ) -> Self {
        ServerMessagePoolSubmissionResult {
            difficulty,
            total_balance,
            total_rewards,
            top_stake,
            multiplier,
            active_miners,
            challenge,
            best_nonce,
            miner_supplied_difficulty,
            miner_earned_rewards,
            miner_percentage
        }
    }

    pub fn to_message_binary(&self) -> Vec<u8> {
        let mut bin_data = Vec::new();
        bin_data.push(1u8);
        bin_data.extend_from_slice(&self.difficulty.to_le_bytes());
        bin_data.extend_from_slice(&self.total_balance.to_le_bytes());
        bin_data.extend_from_slice(&self.total_rewards.to_le_bytes());
        bin_data.extend_from_slice(&self.top_stake.to_le_bytes());
        bin_data.extend_from_slice(&self.multiplier.to_le_bytes());
        bin_data.extend_from_slice(&self.active_miners.to_le_bytes());
        bin_data.extend_from_slice(&self.challenge);
        bin_data.extend_from_slice(&self.best_nonce.to_le_bytes());
        bin_data.extend_from_slice(&self.miner_supplied_difficulty.to_le_bytes());
        bin_data.extend_from_slice(&self.miner_earned_rewards.to_le_bytes());
        bin_data.extend_from_slice(&self.miner_percentage.to_le_bytes());

        bin_data
    }
}
