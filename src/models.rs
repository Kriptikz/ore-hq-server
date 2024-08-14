use diesel::prelude::*;
use serde::{Serialize, Deserialize};
use chrono::NaiveDateTime;

#[derive(Debug, Serialize, Deserialize, Queryable, Selectable, QueryableByName)]
#[diesel(table_name = crate::schema::challenges)]
#[diesel(check_for_backend(diesel::mysql::Mysql))]
pub struct Challenge {
    pub id: i32,
    pub pool_id: i32,
    pub submission_id: Option<i32>,
    pub challenge: Vec<u8>,
    pub rewards_earned: Option<u64>,
}

#[derive(Debug, Serialize, Deserialize, Queryable, Selectable, QueryableByName)]
#[diesel(table_name = crate::schema::challenges)]
#[diesel(check_for_backend(diesel::mysql::Mysql))]
pub struct InsertChallenge {
    pub pool_id: i32,
    pub challenge: Vec<u8>,
    pub rewards_earned: Option<u64>,
}

#[derive(Debug, Serialize, Deserialize, Queryable, Selectable, QueryableByName)]
#[diesel(table_name = crate::schema::challenges)]
#[diesel(check_for_backend(diesel::mysql::Mysql))]
pub struct UpdateChallengeRewards {
    pub rewards_earned: Option<u64>,
}

#[derive(Debug, Serialize, Deserialize, Queryable, Selectable, QueryableByName)]
#[diesel(table_name = crate::schema::claims)]
#[diesel(check_for_backend(diesel::mysql::Mysql))]
pub struct Claim {
    pub miner_id: i32,
    pub pool_id: i32,
    pub txn_id: i32,
    pub amount: u64,
}

#[derive(Debug, Serialize, Deserialize, Queryable, Selectable, QueryableByName)]
#[diesel(table_name = crate::schema::claims)]
#[diesel(check_for_backend(diesel::mysql::Mysql))]
pub struct InsertClaim {
    pub miner_id: i32,
    pub pool_id: i32,
    pub txn_id: i32,
    pub amount: u64,
}

#[derive(Debug, Serialize, Deserialize, Queryable, Selectable, QueryableByName)]
#[diesel(table_name = crate::schema::miners)]
#[diesel(check_for_backend(diesel::mysql::Mysql))]
pub struct Miner {
    pub id: i32,
    pub pubkey: String,
    pub enabled: bool
}

#[derive(Debug, Serialize, Deserialize, Queryable, Selectable, QueryableByName)]
#[diesel(table_name = crate::schema::pools)]
#[diesel(check_for_backend(diesel::mysql::Mysql))]
pub struct Pool {
    pub id: i32,
    pub proof_pubkey: String,
    pub authority_pubkey: String,
    pub total_rewards: u64,
    pub claimed_rewards: u64,
}

#[derive(Debug, Serialize, Deserialize, Queryable, Selectable, QueryableByName)]
#[diesel(table_name = crate::schema::submissions)]
#[diesel(check_for_backend(diesel::mysql::Mysql))]
pub struct Submission {
    pub id: i32,
    pub miner_id: i32,
    pub challenge_id: i32,
    pub nonce: u64,
    pub difficulty: i8,
    pub created_at: NaiveDateTime,
}

#[derive(Debug, Serialize, Deserialize, Queryable, Selectable, QueryableByName)]
#[diesel(table_name = crate::schema::submissions)]
#[diesel(check_for_backend(diesel::mysql::Mysql))]
pub struct InsertSubmission {
    pub miner_id: i32,
    pub challenge_id: i32,
    pub nonce: u64,
    pub difficulty: i8,
}


#[derive(Debug, Serialize, Deserialize, Queryable, Selectable, QueryableByName)]
#[diesel(table_name = crate::schema::submissions)]
#[diesel(check_for_backend(diesel::mysql::Mysql))]
pub struct SubmissionWithId {
    pub id: i32,
}

#[derive(Debug, Serialize, Deserialize, Queryable, Selectable, QueryableByName)]
#[diesel(table_name = crate::schema::txns)]
#[diesel(check_for_backend(diesel::mysql::Mysql))]
pub struct Txn {
    pub id: i32,
    pub txn_type: String,
    pub signature: String,
    pub priority_fee: u32,
}

#[derive(Debug, Serialize, Deserialize, Queryable, Selectable, QueryableByName)]
#[diesel(table_name = crate::schema::txns)]
#[diesel(check_for_backend(diesel::mysql::Mysql))]
pub struct InsertTxn {
    pub txn_type: String,
    pub signature: String,
    pub priority_fee: u32,
}

#[derive(Debug, Serialize, Deserialize, Queryable, Selectable, QueryableByName)]
#[diesel(table_name = crate::schema::rewards)]
#[diesel(check_for_backend(diesel::mysql::Mysql))]
pub struct InsertReward {
    pub miner_id: i32,
    pub pool_id: i32,
}

#[derive(Debug, Serialize, Deserialize, Queryable, Selectable, QueryableByName)]
#[diesel(table_name = crate::schema::rewards)]
#[diesel(check_for_backend(diesel::mysql::Mysql))]
pub struct Reward {
    pub balance: u64,
}


