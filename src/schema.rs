// @generated automatically by Diesel CLI.

diesel::table! {
    challenges (id) {
        id -> Integer,
        pool_id -> Integer,
        submission_id -> Nullable<Integer>,
        #[max_length = 32]
        challenge -> Binary,
        rewards_earned -> Nullable<Unsigned<Bigint>>,
        created_at -> Timestamp,
        updated_at -> Timestamp,
    }
}

diesel::table! {
    claims (id) {
        id -> Integer,
        miner_id -> Integer,
        pool_id -> Integer,
        txn_id -> Integer,
        amount -> Unsigned<Bigint>,
        created_at -> Timestamp,
        updated_at -> Timestamp,
    }
}

diesel::table! {
    earnings (id) {
        id -> Bigint,
        miner_id -> Integer,
        pool_id -> Integer,
        challenge_id -> Integer,
        amount -> Unsigned<Bigint>,
        created_at -> Timestamp,
        updated_at -> Timestamp,
        created_at_day -> Nullable<Date>,
    }
}

diesel::table! {
    miners (id) {
        id -> Integer,
        #[max_length = 44]
        pubkey -> Varchar,
        enabled -> Bool,
        created_at -> Timestamp,
        updated_at -> Timestamp,
    }
}

diesel::table! {
    pools (id) {
        id -> Integer,
        #[max_length = 44]
        proof_pubkey -> Varchar,
        #[max_length = 44]
        authority_pubkey -> Varchar,
        total_rewards -> Unsigned<Bigint>,
        claimed_rewards -> Unsigned<Bigint>,
        created_at -> Timestamp,
        updated_at -> Timestamp,
    }
}

diesel::table! {
    rewards (id) {
        id -> Integer,
        miner_id -> Integer,
        pool_id -> Integer,
        balance -> Unsigned<Bigint>,
        created_at -> Timestamp,
        updated_at -> Timestamp,
    }
}

diesel::table! {
    stake_accounts (id) {
        id -> Integer,
        pool_id -> Integer,
        #[max_length = 44]
        mint_pubkey -> Varchar,
        #[max_length = 44]
        staker_pubkey -> Varchar,
        #[max_length = 44]
        stake_pda -> Varchar,
        rewards_balance -> Unsigned<Bigint>,
        staked_balance -> Unsigned<Bigint>,
        total_rewards_earned -> Unsigned<Bigint>,
        created_at -> Timestamp,
        updated_at -> Timestamp,
    }
}

diesel::table! {
    submissions (id) {
        id -> Bigint,
        miner_id -> Integer,
        challenge_id -> Integer,
        difficulty -> Tinyint,
        nonce -> Unsigned<Bigint>,
        created_at -> Timestamp,
        updated_at -> Timestamp,
        #[max_length = 16]
        digest -> Nullable<Binary>,
    }
}

diesel::table! {
    submissions_2 (id) {
        id -> Integer,
        miner_id -> Integer,
        challenge_id -> Integer,
        difficulty -> Tinyint,
        nonce -> Unsigned<Bigint>,
        created_at -> Timestamp,
        updated_at -> Timestamp,
    }
}

diesel::table! {
    txns (id) {
        id -> Integer,
        #[max_length = 15]
        txn_type -> Varchar,
        #[max_length = 200]
        signature -> Varchar,
        priority_fee -> Unsigned<Integer>,
        created_at -> Timestamp,
        updated_at -> Timestamp,
    }
}

diesel::allow_tables_to_appear_in_same_query!(
    challenges,
    claims,
    earnings,
    miners,
    pools,
    rewards,
    stake_accounts,
    submissions,
    submissions_2,
    txns,
);
