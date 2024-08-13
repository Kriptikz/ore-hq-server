use diesel::{sql_types::{BigInt, Binary, Bool, Integer, Nullable, Text, TinyInt, Unsigned}, MysqlConnection, RunQueryDsl};
use deadpool_diesel::mysql::{Manager, Pool};

use crate::{models, InsertReward, Miner, SubmissionWithId};

#[derive(Debug)]
pub enum AppDatabaseError {
    FailedToGetConnectionFromPool,
    FailedToUpdateEntity,
    EntityDoesNotExist,
    FailedToInsertNewEntity,
}

pub struct AppDatabase {
    connection_pool: Pool,
}

impl AppDatabase {
    pub fn new(url: String) -> Self {
        let manager = Manager::new(
            url,
            deadpool_diesel::Runtime::Tokio1,
        );

        let pool = Pool::builder(manager).build().unwrap();

        AppDatabase {
            connection_pool: pool,
        }
    }

    pub async fn get_challenge_by_challenge(&self, challenge: Vec<u8>) -> Result<models::Challenge, AppDatabaseError> {
        if let Ok(db_conn) = self.connection_pool.get().await {
            let res = db_conn.interact(move |conn: &mut MysqlConnection| {
                diesel::sql_query("SELECT id, pool_id, submission_id, challenge, rewards_earned FROM challenges WHERE challenges.challenge = ?")
                .bind::<Binary, _>(challenge)
                .get_result::<models::Challenge>(conn)
            }).await;

            match res {
                Ok(Ok(challenge)) => {
                    return Ok(challenge)
                },
                _ => {
                    return Err(AppDatabaseError::EntityDoesNotExist);
                }
            }
        } else {
            return Err(AppDatabaseError::FailedToGetConnectionFromPool);
        };

    }

    pub async fn add_new_reward(&self, reward: InsertReward) -> Result<(), AppDatabaseError> {
        if let Ok(db_conn) = self.connection_pool.get().await {
            let res = db_conn.interact(move |conn: &mut MysqlConnection| {
                diesel::sql_query("INSERT INTO rewards (miner_id, pool_id) VALUES (?, ?)")
                .bind::<Integer, _>(reward.miner_id)
                .bind::<Integer, _>(reward.pool_id)
                .execute(conn)
            }).await;

            if res.is_ok() {
                return Ok(());
            } else {
                return Err(AppDatabaseError::FailedToInsertNewEntity);
            }
        } else {
            return Err(AppDatabaseError::FailedToGetConnectionFromPool);
        };
    }

    pub async fn add_new_submission(&self, submission: models::InsertSubmission) -> Result<(), AppDatabaseError> {
        if let Ok(db_conn) = self.connection_pool.get().await {
            let res = db_conn.interact(move |conn: &mut MysqlConnection| {
                diesel::sql_query("INSERT INTO submissions (miner_id, challenge_id, nonce, difficulty) VALUES (?, ?, ?, ?)")
                .bind::<Integer, _>(submission.miner_id)
                .bind::<Integer, _>(submission.challenge_id)
                .bind::<Unsigned<BigInt>, _>(submission.nonce)
                .bind::<TinyInt, _>(submission.difficulty)
                .execute(conn)
            }).await;

            if res.is_ok() {
                return Ok(());
            } else {
                return Err(AppDatabaseError::FailedToInsertNewEntity);
            }
        } else {
            return Err(AppDatabaseError::FailedToGetConnectionFromPool);
        };

    }

    pub async fn get_submission_id_with_nonce(&self, nonce: u64) -> Result<i32, AppDatabaseError> {
        if let Ok(db_conn) = self.connection_pool.get().await {
            let res = db_conn.interact(move |conn: &mut MysqlConnection| {
                diesel::sql_query("SELECT id FROM submissions WHERE submissions.nonce = ?")
                .bind::<Unsigned<BigInt>, _>(nonce)
                .get_result::<SubmissionWithId>(conn)
            }).await;

            match res {
                Ok(Ok(submission)) => {
                    return Ok(submission.id)
                },
                _ => {
                    return Err(AppDatabaseError::EntityDoesNotExist);
                }
            }
        } else {
            return Err(AppDatabaseError::FailedToGetConnectionFromPool);
        };
    }

    pub async fn update_challenge_rewards(&self, challenge: Vec<u8>, submission_id: i32, rewards: u64) -> Result<(), AppDatabaseError> {
        if let Ok(db_conn) = self.connection_pool.get().await {
            let res = db_conn.interact(move |conn: &mut MysqlConnection| {
                diesel::sql_query("UPDATE challenges SET rewards_earned = ?, submission_id = ? WHERE challenge = ?")
                .bind::<Nullable<Unsigned<BigInt>>, _>(Some(rewards))
                .bind::<Nullable<Integer>, _>(submission_id)
                .bind::<Binary, _>(challenge)
                .execute(conn)
            }).await;

            if res.is_ok() {
                return Ok(());
            } else {
                return Err(AppDatabaseError::FailedToUpdateEntity);
            }
        } else {
            return Err(AppDatabaseError::FailedToGetConnectionFromPool);
        };

    }

    pub async fn add_new_challenge(&self, challenge: models::InsertChallenge) -> Result<(), AppDatabaseError> {
        if let Ok(db_conn) = self.connection_pool.get().await {
            let res = db_conn.interact(move |conn: &mut MysqlConnection| {
                diesel::sql_query("INSERT INTO challenges (pool_id, challenge, rewards_earned) VALUES (?, ?, ?)")
                .bind::<Integer, _>(challenge.pool_id)
                .bind::<Binary, _>(challenge.challenge)
                .bind::<Nullable<Unsigned<BigInt>>, _>(challenge.rewards_earned)
                .execute(conn)
            }).await;

            if res.is_ok() {
                return Ok(());
            } else {
                return Err(AppDatabaseError::FailedToInsertNewEntity);
            }
        } else {
            return Err(AppDatabaseError::FailedToGetConnectionFromPool);
        };

    }

    pub async fn get_pool_by_authority_pubkey(&self, pool_pubkey: String) -> Result<models::Pool, AppDatabaseError> {
        if let Ok(db_conn) = self.connection_pool.get().await {
            let res = db_conn.interact(move |conn: &mut MysqlConnection| {
                diesel::sql_query("SELECT id, proof_pubkey, authority_pubkey, total_rewards, claimed_rewards FROM pools WHERE pools.authority_pubkey = ?")
                .bind::<Text, _>(pool_pubkey)
                .get_result::<models::Pool>(conn)
            }).await;

            match res {
                Ok(Ok(pool)) => {
                    return Ok(pool)
                },
                _ => {
                    return Err(AppDatabaseError::EntityDoesNotExist);
                }
            }
        } else {
            return Err(AppDatabaseError::FailedToGetConnectionFromPool);
        };

    }

    pub async fn add_new_pool(&self, authority_pubkey: String, proof_pubkey: String) -> Result<(), AppDatabaseError> {
        if let Ok(db_conn) = self.connection_pool.get().await {
            let res = db_conn.interact(move |conn: &mut MysqlConnection| {
                diesel::sql_query("INSERT INTO pools (authority_pubkey, proof_pubkey) VALUES (?, ?)")
                .bind::<Text, _>(authority_pubkey)
                .bind::<Text, _>(proof_pubkey)
                .execute(conn)
            }).await;

            if res.is_ok() {
                return Ok(());
            } else {
                return Err(AppDatabaseError::FailedToInsertNewEntity);
            }
        } else {
            return Err(AppDatabaseError::FailedToGetConnectionFromPool);
        };

    }

    pub async fn add_new_miner(&self, miner_pubkey: String, is_enabled: bool) -> Result<(), AppDatabaseError> {
        if let Ok(db_conn) = self.connection_pool.get().await {
            let res = db_conn.interact(move |conn: &mut MysqlConnection| {
                diesel::sql_query("INSERT INTO miners (pubkey, enabled) VALUES (?, ?)")
                .bind::<Text, _>(miner_pubkey)
                .bind::<Bool, _>(is_enabled)
                .execute(conn)
            }).await;

            if res.is_ok() {
                return Ok(());
            } else {
                return Err(AppDatabaseError::FailedToInsertNewEntity);
            }
        } else {
            return Err(AppDatabaseError::FailedToGetConnectionFromPool);
        };

    }

    pub async fn get_miner_by_pubkey_str(&self, miner_pubkey: String) -> Result<Miner, AppDatabaseError> {
        if let Ok(db_conn) = self.connection_pool.get().await {
            let res = db_conn.interact(move |conn: &mut MysqlConnection| {
                diesel::sql_query("SELECT id, pubkey, enabled FROM miners WHERE miners.pubkey = ?")
                .bind::<Text, _>(miner_pubkey)
                .get_result::<Miner>(conn)
            }).await;

            match res {
                Ok(Ok(miner)) => {
                    return Ok(miner)
                },
                _ => {
                    return Err(AppDatabaseError::EntityDoesNotExist);
                }
            }
        } else {
            return Err(AppDatabaseError::FailedToGetConnectionFromPool);
        };

    }
}
