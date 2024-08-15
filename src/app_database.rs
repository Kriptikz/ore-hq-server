use diesel::{sql_types::{BigInt, Binary, Bool, Integer, Nullable, Text, TinyInt, Unsigned}, MysqlConnection, RunQueryDsl};
use deadpool_diesel::mysql::{Manager, Pool};
use tracing::{error, info};

use crate::{models, InsertReward, Miner, Submission, SubmissionForSolution, SubmissionWithId};

#[derive(Debug)]
pub enum AppDatabaseError {
    FailedToGetConnectionFromPool,
    FailedToUpdateEntity,
    EntityDoesNotExist,
    FailedToInsertNewEntity,
    InteractionFailed,
    QueryFailed,
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

    pub async fn get_latest_challenge(&self) -> Result<models::Challenge, AppDatabaseError> {
        if let Ok(db_conn) = self.connection_pool.get().await {
            let res = db_conn.interact(move |conn: &mut MysqlConnection| {
                diesel::sql_query("SELECT id, pool_id, submission_id, challenge, rewards_earned FROM challenges ORDER BY id DESC")
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

    pub async fn get_challenge_by_challenge(&self, challenge: Vec<u8>) -> Result<models::Challenge, AppDatabaseError> {
        if let Ok(db_conn) = self.connection_pool.get().await {
            let res = db_conn.interact(move |conn: &mut MysqlConnection| {
                diesel::sql_query("SELECT id, pool_id, submission_id, challenge, rewards_earned FROM challenges WHERE challenges.challenge = ? ORDER BY id DESC")
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

    pub async fn get_miner_rewards(&self, miner_pubkey: String) -> Result<models::Reward, AppDatabaseError> {
        if let Ok(db_conn) = self.connection_pool.get().await {
            let res = db_conn.interact(move |conn: &mut MysqlConnection| {
                diesel::sql_query("SELECT r.balance FROM miners m JOIN rewards r ON m.id = r.miner_id WHERE m.pubkey = ?")
                .bind::<Text, _>(miner_pubkey)
                .get_result::<models::Reward>(conn)
            }).await;

            match res {
                Ok(Ok(reward)) => {
                    return Ok(reward)
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

    pub async fn update_miner_reward(&self, miner_id: i32, rewards_to_add: u64) -> Result<(), AppDatabaseError> {
        if let Ok(db_conn) = self.connection_pool.get().await {
            let res = db_conn.interact(move |conn: &mut MysqlConnection| {
                diesel::sql_query("UPDATE rewards SET balance = balance + ? WHERE miner_id = ?")
                .bind::<Unsigned<BigInt>, _>(rewards_to_add)
                .bind::<Integer, _>(miner_id)
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

    pub async fn decrease_miner_reward(&self, miner_id: i32, rewards_to_decrease: u64) -> Result<(), AppDatabaseError> {
        if let Ok(db_conn) = self.connection_pool.get().await {
            let res = db_conn.interact(move |conn: &mut MysqlConnection| {
                diesel::sql_query("UPDATE rewards SET balance = balance - ? WHERE miner_id = ?")
                .bind::<Unsigned<BigInt>, _>(rewards_to_decrease)
                .bind::<Integer, _>(miner_id)
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


    pub async fn add_new_submission(&self, submission: models::InsertSubmission) -> Result<(), AppDatabaseError> {
        if let Ok(db_conn) = self.connection_pool.get().await {
            let res = db_conn.interact(move |conn: &mut MysqlConnection| {
                diesel::sql_query("INSERT INTO submissions (miner_id, challenge_id, digest, nonce, difficulty) VALUES (?, ?, ?, ?, ?)")
                .bind::<Integer, _>(submission.miner_id)
                .bind::<Integer, _>(submission.challenge_id)
                .bind::<Nullable<Binary>, _>(submission.digest)
                .bind::<Unsigned<BigInt>, _>(submission.nonce)
                .bind::<TinyInt, _>(submission.difficulty)
                .execute(conn)
            }).await;
            match res {
                Ok(interaction) => {
                    match interaction {
                        Ok(_query) => {
                            info!("Successfully added new submission");
                            return Ok(());
                        },
                        Err(e) => {
                            error!("{:?}", e);
                            return Err(AppDatabaseError::QueryFailed);
                        }
                    }
                },
                Err(e) => {
                    error!("{:?}", e);
                    return Err(AppDatabaseError::InteractionFailed);
                }
            }
        } else {
            return Err(AppDatabaseError::FailedToGetConnectionFromPool);
        };

    }

    pub async fn get_all_submission_for_challenge(&self, challenge: Vec<u8>) -> Result<Vec<Submission>, AppDatabaseError> {
        if let Ok(db_conn) = self.connection_pool.get().await {
            let res = db_conn.interact(move |conn: &mut MysqlConnection| {
                diesel::sql_query("SELECT s.id, s.miner_id, s.challenge_id, s.digest, s.nonce, s.difficulty FROM submissions s JOIN challenges c ON c.id = s.challenge_id WHERE c.challenge = ? ORDER BY s.difficulty DESC")
                .bind::<Binary, _>(challenge)
                .get_results::<Submission>(conn)
            }).await;

            match res {
                Ok(Ok(submissions)) => {
                    return Ok(submissions)
                },
                _ => {
                    return Err(AppDatabaseError::EntityDoesNotExist);
                }
            }
        } else {
            return Err(AppDatabaseError::FailedToGetConnectionFromPool);
        };
    }

    pub async fn get_best_submission_for_challenge(&self, challenge: Vec<u8>) -> Result<SubmissionForSolution, AppDatabaseError> {
        if let Ok(db_conn) = self.connection_pool.get().await {
            let res = db_conn.interact(move |conn: &mut MysqlConnection| {
                diesel::sql_query("SELECT s.id, s.digest, s.nonce, s.difficulty FROM submissions s JOIN challenges c ON c.id = s.challenge_id WHERE c.challenge = ? AND s.challenge_id IS NOT NULL ORDER BY s.difficulty DESC")
                .bind::<Binary, _>(challenge)
                .get_result::<SubmissionForSolution>(conn)
            }).await;

            match res {
                Ok(interaction) => {
                    match interaction {
                        Ok(query) => {
                            return Ok(query);
                        },
                        Err(e) => {
                            error!("{:?}", e);
                            return Err(AppDatabaseError::QueryFailed);
                        }
                    }
                },
                Err(e) => {
                    error!("{:?}", e);
                    return Err(AppDatabaseError::InteractionFailed);
                }
            }
        } else {
            return Err(AppDatabaseError::FailedToGetConnectionFromPool);
        };
    }
    
    pub async fn get_submission_id_with_challenge_id(&self, challenge: Vec<u8>) -> Result<i32, AppDatabaseError> {
        if let Ok(db_conn) = self.connection_pool.get().await {
            let res = db_conn.interact(move |conn: &mut MysqlConnection| {
                diesel::sql_query("SELECT s.id FROM submissions s JOIN challenges c ON c.id = s.challenge_id WHERE c.challenge = ?")
                .bind::<Binary, _>(challenge)
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

    pub async fn update_pool_rewards(&self, pool_authority_pubkey: String, earned_rewards: u64) -> Result<(), AppDatabaseError> {
        if let Ok(db_conn) = self.connection_pool.get().await {
            let res = db_conn.interact(move |conn: &mut MysqlConnection| {
                diesel::sql_query("UPDATE pools SET total_rewards = total_rewards + ? WHERE authority_pubkey = ?")
                .bind::<Unsigned<BigInt>, _>(earned_rewards)
                .bind::<Text, _>(pool_authority_pubkey)
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

    pub async fn update_pool_claimed(&self, pool_authority_pubkey: String, claimed_rewards: u64) -> Result<(), AppDatabaseError> {
        if let Ok(db_conn) = self.connection_pool.get().await {
            let res = db_conn.interact(move |conn: &mut MysqlConnection| {
                diesel::sql_query("UPDATE pools SET claimed_rewards = claimed_rewards + ? WHERE authority_pubkey = ?")
                .bind::<Unsigned<BigInt>, _>(claimed_rewards)
                .bind::<Text, _>(pool_authority_pubkey)
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

    pub async fn add_new_claim(&self, claim: models::InsertClaim) -> Result<i32, AppDatabaseError> {
        if let Ok(db_conn) = self.connection_pool.get().await {
            let res = db_conn.interact(move |conn: &mut MysqlConnection| {
                diesel::sql_query("INSERT INTO claims (miner_id, pool_id, txn_id, amount) VALUES (?, ?, ?, ?)")
                .bind::<Integer, _>(claim.miner_id)
                .bind::<Integer, _>(claim.pool_id)
                .bind::<Integer, _>(claim.txn_id)
                .bind::<Unsigned<BigInt>, _>(claim.amount)
                .execute(conn)
            }).await;

            if res.is_ok() {
                return Ok(res.unwrap().unwrap() as i32);
            } else {
                return Err(AppDatabaseError::FailedToInsertNewEntity);
            }
        } else {
            return Err(AppDatabaseError::FailedToGetConnectionFromPool);
        };

    }

    pub async fn add_new_txn(&self, txn: models::InsertTxn) -> Result<(), AppDatabaseError> {
        if let Ok(db_conn) = self.connection_pool.get().await {
            let res = db_conn.interact(move |conn: &mut MysqlConnection| {
                diesel::sql_query("INSERT INTO txns (txn_type, signature, priority_fee) VALUES (?, ?, ?)")
                .bind::<Text, _>(txn.txn_type)
                .bind::<Text, _>(txn.signature)
                .bind::<Unsigned<Integer>, _>(txn.priority_fee)
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

    pub async fn get_txn_by_sig(&self, sig: String) -> Result<models::TxnId, AppDatabaseError> {
        if let Ok(db_conn) = self.connection_pool.get().await {
            let res = db_conn.interact(move |conn: &mut MysqlConnection| {
                diesel::sql_query("SELECT id FROM txns WHERE signature = ?")
                .bind::<Text, _>(sig)
                .get_result::<models::TxnId>(conn)
            }).await;

            match res {
                Ok(Ok(txn)) => {
                    return Ok(txn)
                },
                _ => {
                    return Err(AppDatabaseError::EntityDoesNotExist);
                }
            }
        } else {
            return Err(AppDatabaseError::FailedToGetConnectionFromPool);
        };

    }

    pub async fn add_new_earning(&self, earning: models::InsertEarning) -> Result<(), AppDatabaseError> {
        if let Ok(db_conn) = self.connection_pool.get().await {
            let res = db_conn.interact(move |conn: &mut MysqlConnection| {
                diesel::sql_query("INSERT INTO earnings (miner_id, pool_id, challenge_id, amount) VALUES (?, ?, ?, ?)")
                .bind::<Integer, _>(earning.miner_id)
                .bind::<Integer, _>(earning.pool_id)
                .bind::<Integer, _>(earning.challenge_id)
                .bind::<Unsigned<BigInt>, _>(earning.amount)
                .execute(conn)
            }).await;

            match res {
                Ok(interaction) => {
                    match interaction {
                        Ok(_query) => {
                            return Ok(());
                        },
                        Err(e) => {
                            error!("{:?}", e);
                            return Err(AppDatabaseError::QueryFailed);
                        }
                    }
                },
                Err(e) => {
                    error!("{:?}", e);
                    return Err(AppDatabaseError::InteractionFailed);
                }
            }
        } else {
            return Err(AppDatabaseError::FailedToGetConnectionFromPool);
        };
    }

    pub async fn get_miner_earning(&self, challenge_id: i32, miner_id: i32, pool_id: i32) -> Result<models::EarningAmount, AppDatabaseError> {
        if let Ok(db_conn) = self.connection_pool.get().await {
            let res = db_conn.interact(move |conn: &mut MysqlConnection| {
                diesel::sql_query("SELECT e.amount FROM earnings e WHERE e.miner_id = ? AND e.challenge_id = ? AND e.pool_id = ?")
                .bind::<Integer, _>(miner_id)
                .bind::<Integer, _>(challenge_id)
                .bind::<Integer, _>(pool_id)
                .get_result::<models::EarningAmount>(conn)
            }).await;

            match res {
                Ok(interaction) => {
                    match interaction {
                        Ok(query) => {
                            return Ok(query);
                        },
                        Err(e) => {
                            error!("{:?}", e);
                            return Err(AppDatabaseError::QueryFailed);
                        }
                    }
                },
                Err(e) => {
                    error!("{:?}", e);
                    return Err(AppDatabaseError::InteractionFailed);
                }
            }
        } else {
            return Err(AppDatabaseError::FailedToGetConnectionFromPool);
        };

    }
}
