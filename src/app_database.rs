use deadpool_diesel::mysql::{Manager, Pool};
use diesel::{
    connection::SimpleConnection,
    insert_into,
    sql_types::{BigInt, Binary, Bool, Integer, Nullable, Text, Unsigned},
    MysqlConnection, RunQueryDsl,
};
use tokio::time::Instant;
use tracing::{error, info};

use crate::{models, InsertReward, Miner, SubmissionWithId};

#[derive(Debug)]
pub enum AppDatabaseError {
    FailedToGetConnectionFromPool,
    FailedToUpdateRow,
    FailedToInsertRow,
    InteractionFailed,
    QueryFailed,
}

pub struct AppDatabase {
    connection_pool: Pool,
}

impl AppDatabase {
    pub fn new(url: String) -> Self {
        let manager = Manager::new(url, deadpool_diesel::Runtime::Tokio1);

        let pool = Pool::builder(manager).build().unwrap();

        AppDatabase {
            connection_pool: pool,
        }
    }

    pub async fn get_challenge_by_challenge(
        &self,
        challenge: Vec<u8>,
    ) -> Result<models::Challenge, AppDatabaseError> {
        if let Ok(db_conn) = self.connection_pool.get().await {
            let res = db_conn.interact(move |conn: &mut MysqlConnection| {
                diesel::sql_query("SELECT id, pool_id, submission_id, challenge, rewards_earned FROM challenges WHERE challenges.challenge = ?")
                .bind::<Binary, _>(challenge)
                .get_result::<models::Challenge>(conn)
            }).await;

            match res {
                Ok(interaction) => match interaction {
                    Ok(query) => {
                        return Ok(query);
                    }
                    Err(e) => {
                        error!(target: "server_log", "{:?}", e);
                        return Err(AppDatabaseError::QueryFailed);
                    }
                },
                Err(e) => {
                    error!(target: "server_log", "{:?}", e);
                    return Err(AppDatabaseError::InteractionFailed);
                }
            }
        } else {
            return Err(AppDatabaseError::FailedToGetConnectionFromPool);
        };
    }

    pub async fn get_miner_rewards(
        &self,
        miner_pubkey: String,
    ) -> Result<models::Reward, AppDatabaseError> {
        if let Ok(db_conn) = self.connection_pool.get().await {
            let res = db_conn.interact(move |conn: &mut MysqlConnection| {
                diesel::sql_query("SELECT r.balance, r.miner_id FROM miners m JOIN rewards r ON m.id = r.miner_id WHERE m.pubkey = ?")
                .bind::<Text, _>(miner_pubkey)
                .get_result::<models::Reward>(conn)
            }).await;

            match res {
                Ok(interaction) => match interaction {
                    Ok(query) => {
                        return Ok(query);
                    }
                    Err(e) => {
                        error!(target: "server_log", "{:?}", e);
                        return Err(AppDatabaseError::QueryFailed);
                    }
                },
                Err(e) => {
                    error!(target: "server_log", "{:?}", e);
                    return Err(AppDatabaseError::InteractionFailed);
                }
            }
        } else {
            return Err(AppDatabaseError::FailedToGetConnectionFromPool);
        };
    }

    pub async fn add_new_reward(&self, reward: InsertReward) -> Result<(), AppDatabaseError> {
        if let Ok(db_conn) = self.connection_pool.get().await {
            let res = db_conn
                .interact(move |conn: &mut MysqlConnection| {
                    diesel::sql_query("INSERT INTO rewards (miner_id, pool_id) VALUES (?, ?)")
                        .bind::<Integer, _>(reward.miner_id)
                        .bind::<Integer, _>(reward.pool_id)
                        .execute(conn)
                })
                .await;

            match res {
                Ok(interaction) => match interaction {
                    Ok(_query) => {
                        return Ok(());
                    }
                    Err(e) => {
                        error!(target: "server_log", "{:?}", e);
                        return Err(AppDatabaseError::QueryFailed);
                    }
                },
                Err(e) => {
                    error!(target: "server_log", "{:?}", e);
                    return Err(AppDatabaseError::InteractionFailed);
                }
            }
        } else {
            return Err(AppDatabaseError::FailedToGetConnectionFromPool);
        };
    }

    pub async fn update_rewards(
        &self,
        rewards: Vec<models::UpdateReward>,
    ) -> Result<(), AppDatabaseError> {
        let id = uuid::Uuid::new_v4();
        let instant = Instant::now();
        tracing::info!(target: "server_log", "{} - Getting db pool connection.", id);
        if let Ok(db_conn) = self.connection_pool.get().await {
            tracing::info!(target: "server_log", "{} - Got db pool connection in {}ms.", id, instant.elapsed().as_millis());
            let res = db_conn
                .interact(move |conn: &mut MysqlConnection| {
                    let query = diesel::sql_query(
                        "UPDATE rewards SET balance = balance + CASE miner_id ".to_string() +
                        &rewards
                            .iter()
                            .map(|r| format!("WHEN {} THEN {}", r.miner_id, r.balance))
                            .collect::<Vec<_>>()
                            .join(" ") +
                        " END WHERE miner_id IN (" +
                        &rewards
                            .iter()
                            .map(|r| r.miner_id.to_string())
                            .collect::<Vec<_>>()
                            .join(",") +
                        ")"
                    );
                    query.execute(conn)
                })
                .await;

            match res {
                Ok(interaction) => match interaction {
                    Ok(_query) => {
                        return Ok(());
                    }
                    Err(e) => {
                        error!(target: "server_log", "update rewards query error: {:?}", e);
                        return Err(AppDatabaseError::QueryFailed);
                    }
                },
                Err(e) => {
                    error!(target: "server_log", "update rewards interaction error: {:?}", e);
                    return Err(AppDatabaseError::InteractionFailed);
                }
            }
        } else {
            return Err(AppDatabaseError::FailedToGetConnectionFromPool);
        };
    }

    pub async fn decrease_miner_reward(
        &self,
        miner_id: i32,
        rewards_to_decrease: u64,
    ) -> Result<(), AppDatabaseError> {
        if let Ok(db_conn) = self.connection_pool.get().await {
            let res = db_conn
                .interact(move |conn: &mut MysqlConnection| {
                    diesel::sql_query("UPDATE rewards SET balance = balance - ? WHERE miner_id = ?")
                        .bind::<Unsigned<BigInt>, _>(rewards_to_decrease)
                        .bind::<Integer, _>(miner_id)
                        .execute(conn)
                })
                .await;

            match res {
                Ok(interaction) => match interaction {
                    Ok(_query) => {
                        return Ok(());
                    }
                    Err(e) => {
                        error!(target: "server_log", "{:?}", e);
                        return Err(AppDatabaseError::QueryFailed);
                    }
                },
                Err(e) => {
                    error!(target: "server_log", "{:?}", e);
                    return Err(AppDatabaseError::InteractionFailed);
                }
            }
        } else {
            return Err(AppDatabaseError::FailedToGetConnectionFromPool);
        };
    }

    pub async fn get_submission_id_with_nonce(&self, nonce: u64) -> Result<i64, AppDatabaseError> {
        if let Ok(db_conn) = self.connection_pool.get().await {
            let res = db_conn
                .interact(move |conn: &mut MysqlConnection| {
                    diesel::sql_query(
                        "SELECT id FROM submissions WHERE submissions.nonce = ? ORDER BY id DESC",
                    )
                    .bind::<Unsigned<BigInt>, _>(nonce)
                    .get_result::<SubmissionWithId>(conn)
                })
                .await;

            match res {
                Ok(interaction) => match interaction {
                    Ok(query) => {
                        return Ok(query.id);
                    }
                    Err(e) => {
                        error!(target: "server_log", "{:?}", e);
                        return Err(AppDatabaseError::QueryFailed);
                    }
                },
                Err(e) => {
                    error!(target: "server_log", "{:?}", e);
                    return Err(AppDatabaseError::InteractionFailed);
                }
            }
        } else {
            return Err(AppDatabaseError::FailedToGetConnectionFromPool);
        };
    }

    pub async fn update_challenge_rewards(
        &self,
        challenge: Vec<u8>,
        submission_id: i64,
        rewards: u64,
    ) -> Result<(), AppDatabaseError> {
        if let Ok(db_conn) = self.connection_pool.get().await {
            let res = db_conn.interact(move |conn: &mut MysqlConnection| {
                diesel::sql_query("UPDATE challenges SET rewards_earned = ?, submission_id = ? WHERE challenge = ?")
                .bind::<Nullable<Unsigned<BigInt>>, _>(Some(rewards))
                .bind::<Nullable<BigInt>, _>(submission_id)
                .bind::<Binary, _>(challenge)
                .execute(conn)
            }).await;

            match res {
                Ok(interaction) => match interaction {
                    Ok(query) => {
                        if query != 1 {
                            return Err(AppDatabaseError::FailedToUpdateRow);
                        }
                        info!(target: "server_log", "Updated challenge rewards!");
                        return Ok(());
                    }
                    Err(e) => {
                        error!(target: "server_log", "{:?}", e);
                        return Err(AppDatabaseError::QueryFailed);
                    }
                },
                Err(e) => {
                    error!(target: "server_log", "{:?}", e);
                    return Err(AppDatabaseError::InteractionFailed);
                }
            }
        } else {
            return Err(AppDatabaseError::FailedToGetConnectionFromPool);
        };
    }

    pub async fn add_new_challenge(
        &self,
        challenge: models::InsertChallenge,
    ) -> Result<(), AppDatabaseError> {
        if let Ok(db_conn) = self.connection_pool.get().await {
            let res = db_conn.interact(move |conn: &mut MysqlConnection| {
                diesel::sql_query("INSERT INTO challenges (pool_id, challenge, rewards_earned) VALUES (?, ?, ?)")
                .bind::<Integer, _>(challenge.pool_id)
                .bind::<Binary, _>(challenge.challenge)
                .bind::<Nullable<Unsigned<BigInt>>, _>(challenge.rewards_earned)
                .execute(conn)
            }).await;

            match res {
                Ok(interaction) => match interaction {
                    Ok(query) => {
                        if query != 1 {
                            return Err(AppDatabaseError::FailedToInsertRow);
                        }
                        return Ok(());
                    }
                    Err(e) => {
                        error!(target: "server_log", "{:?}", e);
                        return Err(AppDatabaseError::QueryFailed);
                    }
                },
                Err(e) => {
                    error!(target: "server_log", "{:?}", e);
                    return Err(AppDatabaseError::InteractionFailed);
                }
            }
        } else {
            return Err(AppDatabaseError::FailedToGetConnectionFromPool);
        };
    }

    pub async fn get_pool_by_authority_pubkey(
        &self,
        pool_pubkey: String,
    ) -> Result<models::Pool, AppDatabaseError> {
        if let Ok(db_conn) = self.connection_pool.get().await {
            let res = db_conn.interact(move |conn: &mut MysqlConnection| {
                diesel::sql_query("SELECT id, proof_pubkey, authority_pubkey, total_rewards, claimed_rewards FROM pools WHERE pools.authority_pubkey = ?")
                .bind::<Text, _>(pool_pubkey)
                .get_result::<models::Pool>(conn)
            }).await;

            match res {
                Ok(interaction) => match interaction {
                    Ok(query) => {
                        return Ok(query);
                    }
                    Err(e) => {
                        error!(target: "server_log", "{:?}", e);
                        return Err(AppDatabaseError::QueryFailed);
                    }
                },
                Err(e) => {
                    error!(target: "server_log", "{:?}", e);
                    return Err(AppDatabaseError::InteractionFailed);
                }
            }
        } else {
            return Err(AppDatabaseError::FailedToGetConnectionFromPool);
        };
    }

    pub async fn add_new_pool(
        &self,
        authority_pubkey: String,
        proof_pubkey: String,
    ) -> Result<(), AppDatabaseError> {
        if let Ok(db_conn) = self.connection_pool.get().await {
            let res = db_conn
                .interact(move |conn: &mut MysqlConnection| {
                    diesel::sql_query(
                        "INSERT INTO pools (authority_pubkey, proof_pubkey) VALUES (?, ?)",
                    )
                    .bind::<Text, _>(authority_pubkey)
                    .bind::<Text, _>(proof_pubkey)
                    .execute(conn)
                })
                .await;

            match res {
                Ok(interaction) => match interaction {
                    Ok(query) => {
                        if query != 1 {
                            return Err(AppDatabaseError::FailedToInsertRow);
                        }
                        return Ok(());
                    }
                    Err(e) => {
                        error!(target: "server_log", "{:?}", e);
                        return Err(AppDatabaseError::QueryFailed);
                    }
                },
                Err(e) => {
                    error!(target: "server_log", "{:?}", e);
                    return Err(AppDatabaseError::InteractionFailed);
                }
            }
        } else {
            return Err(AppDatabaseError::FailedToGetConnectionFromPool);
        };
    }

    pub async fn update_pool_rewards(
        &self,
        pool_authority_pubkey: String,
        earned_rewards: u64,
    ) -> Result<(), AppDatabaseError> {
        if let Ok(db_conn) = self.connection_pool.get().await {
            let res = db_conn.interact(move |conn: &mut MysqlConnection| {
                diesel::sql_query("UPDATE pools SET total_rewards = total_rewards + ? WHERE authority_pubkey = ?")
                .bind::<Unsigned<BigInt>, _>(earned_rewards)
                .bind::<Text, _>(pool_authority_pubkey)
                .execute(conn)
            }).await;

            match res {
                Ok(interaction) => match interaction {
                    Ok(query) => {
                        if query != 1 {
                            return Err(AppDatabaseError::FailedToUpdateRow);
                        }
                        info!(target: "server_log", "Successfully updated pool rewards");
                        return Ok(());
                    }
                    Err(e) => {
                        error!(target: "server_log", "{:?}", e);
                        return Err(AppDatabaseError::QueryFailed);
                    }
                },
                Err(e) => {
                    error!(target: "server_log", "{:?}", e);
                    return Err(AppDatabaseError::InteractionFailed);
                }
            }
        } else {
            return Err(AppDatabaseError::FailedToGetConnectionFromPool);
        };
    }

    pub async fn update_pool_claimed(
        &self,
        pool_authority_pubkey: String,
        claimed_rewards: u64,
    ) -> Result<(), AppDatabaseError> {
        if let Ok(db_conn) = self.connection_pool.get().await {
            let res = db_conn.interact(move |conn: &mut MysqlConnection| {
                diesel::sql_query("UPDATE pools SET claimed_rewards = claimed_rewards + ? WHERE authority_pubkey = ?")
                .bind::<Unsigned<BigInt>, _>(claimed_rewards)
                .bind::<Text, _>(pool_authority_pubkey)
                .execute(conn)
            }).await;

            match res {
                Ok(interaction) => match interaction {
                    Ok(query) => {
                        if query != 1 {
                            return Err(AppDatabaseError::FailedToUpdateRow);
                        }
                        return Ok(());
                    }
                    Err(e) => {
                        error!(target: "server_log", "{:?}", e);
                        return Err(AppDatabaseError::QueryFailed);
                    }
                },
                Err(e) => {
                    error!(target: "server_log", "{:?}", e);
                    return Err(AppDatabaseError::InteractionFailed);
                }
            }
        } else {
            return Err(AppDatabaseError::FailedToGetConnectionFromPool);
        };
    }

    pub async fn add_new_miner(
        &self,
        miner_pubkey: String,
        is_enabled: bool,
    ) -> Result<(), AppDatabaseError> {
        if let Ok(db_conn) = self.connection_pool.get().await {
            let res = db_conn
                .interact(move |conn: &mut MysqlConnection| {
                    diesel::sql_query("INSERT INTO miners (pubkey, enabled) VALUES (?, ?)")
                        .bind::<Text, _>(miner_pubkey)
                        .bind::<Bool, _>(is_enabled)
                        .execute(conn)
                })
                .await;

            match res {
                Ok(interaction) => match interaction {
                    Ok(_query) => {
                        return Ok(());
                    }
                    Err(e) => {
                        error!(target: "server_log", "{:?}", e);
                        return Err(AppDatabaseError::QueryFailed);
                    }
                },
                Err(e) => {
                    error!(target: "server_log", "{:?}", e);
                    return Err(AppDatabaseError::InteractionFailed);
                }
            }
        } else {
            return Err(AppDatabaseError::FailedToGetConnectionFromPool);
        };
    }

    pub async fn get_miner_by_pubkey_str(
        &self,
        miner_pubkey: String,
    ) -> Result<Miner, AppDatabaseError> {
        if let Ok(db_conn) = self.connection_pool.get().await {
            let res = db_conn
                .interact(move |conn: &mut MysqlConnection| {
                    diesel::sql_query(
                        "SELECT id, pubkey, enabled FROM miners WHERE miners.pubkey = ?",
                    )
                    .bind::<Text, _>(miner_pubkey)
                    .get_result::<Miner>(conn)
                })
                .await;
            match res {
                Ok(interaction) => match interaction {
                    Ok(query) => {
                        return Ok(query);
                    }
                    Err(e) => {
                        error!(target: "server_log", "{:?}", e);
                        return Err(AppDatabaseError::QueryFailed);
                    }
                },
                Err(e) => {
                    error!(target: "server_log", "{:?}", e);
                    return Err(AppDatabaseError::InteractionFailed);
                }
            }
        } else {
            return Err(AppDatabaseError::FailedToGetConnectionFromPool);
        };
    }

    pub async fn add_new_claim(&self, claim: models::InsertClaim) -> Result<(), AppDatabaseError> {
        if let Ok(db_conn) = self.connection_pool.get().await {
            let res = db_conn.interact(move |conn: &mut MysqlConnection| {
                diesel::sql_query("INSERT INTO claims (miner_id, pool_id, txn_id, amount) VALUES (?, ?, ?, ?)")
                .bind::<Integer, _>(claim.miner_id)
                .bind::<Integer, _>(claim.pool_id)
                .bind::<Integer, _>(claim.txn_id)
                .bind::<Unsigned<BigInt>, _>(claim.amount)
                .execute(conn)
            }).await;

            match res {
                Ok(interaction) => match interaction {
                    Ok(_query) => {
                        return Ok(());
                    }
                    Err(e) => {
                        error!(target: "server_log", "{:?}", e);
                        return Err(AppDatabaseError::QueryFailed);
                    }
                },
                Err(e) => {
                    error!(target: "server_log", "{:?}", e);
                    return Err(AppDatabaseError::InteractionFailed);
                }
            }
        } else {
            return Err(AppDatabaseError::FailedToGetConnectionFromPool);
        };
    }

    pub async fn get_last_claim(
        &self,
        miner_id: i32,
    ) -> Result<models::LastClaim, AppDatabaseError> {
        if let Ok(db_conn) = self.connection_pool.get().await {
            let res = db_conn
                .interact(move |conn: &mut MysqlConnection| {
                    diesel::sql_query(
                        "SELECT created_at FROM claims WHERE miner_id = ? ORDER BY id DESC",
                    )
                    .bind::<Integer, _>(miner_id)
                    .get_result::<models::LastClaim>(conn)
                })
                .await;

            match res {
                Ok(interaction) => match interaction {
                    Ok(query) => {
                        return Ok(query);
                    }
                    Err(e) => {
                        error!(target: "server_log", "{:?}", e);
                        return Err(AppDatabaseError::QueryFailed);
                    }
                },
                Err(e) => {
                    error!(target: "server_log", "{:?}", e);
                    return Err(AppDatabaseError::InteractionFailed);
                }
            }
        } else {
            return Err(AppDatabaseError::FailedToGetConnectionFromPool);
        };
    }

    pub async fn add_new_txn(&self, txn: models::InsertTxn) -> Result<(), AppDatabaseError> {
        if let Ok(db_conn) = self.connection_pool.get().await {
            let res = db_conn
                .interact(move |conn: &mut MysqlConnection| {
                    diesel::sql_query(
                        "INSERT INTO txns (txn_type, signature, priority_fee) VALUES (?, ?, ?)",
                    )
                    .bind::<Text, _>(txn.txn_type)
                    .bind::<Text, _>(txn.signature)
                    .bind::<Unsigned<Integer>, _>(txn.priority_fee)
                    .execute(conn)
                })
                .await;

            match res {
                Ok(interaction) => match interaction {
                    Ok(_query) => {
                        return Ok(());
                    }
                    Err(e) => {
                        error!(target: "server_log", "{:?}", e);
                        return Err(AppDatabaseError::QueryFailed);
                    }
                },
                Err(e) => {
                    error!(target: "server_log", "{:?}", e);
                    return Err(AppDatabaseError::InteractionFailed);
                }
            }
        } else {
            return Err(AppDatabaseError::FailedToGetConnectionFromPool);
        };
    }

    pub async fn get_txn_by_sig(&self, sig: String) -> Result<models::TxnId, AppDatabaseError> {
        if let Ok(db_conn) = self.connection_pool.get().await {
            let res = db_conn
                .interact(move |conn: &mut MysqlConnection| {
                    diesel::sql_query("SELECT id FROM txns WHERE signature = ?")
                        .bind::<Text, _>(sig)
                        .get_result::<models::TxnId>(conn)
                })
                .await;

            match res {
                Ok(interaction) => match interaction {
                    Ok(query) => {
                        return Ok(query);
                    }
                    Err(e) => {
                        error!(target: "server_log", "{:?}", e);
                        return Err(AppDatabaseError::QueryFailed);
                    }
                },
                Err(e) => {
                    error!(target: "server_log", "{:?}", e);
                    return Err(AppDatabaseError::InteractionFailed);
                }
            }
        } else {
            return Err(AppDatabaseError::FailedToGetConnectionFromPool);
        };
    }

    // pub async fn add_new_earning(
    //     &self,
    //     earning: models::InsertEarning,
    // ) -> Result<(), AppDatabaseError> {
    //     if let Ok(db_conn) = self.connection_pool.get().await {
    //         let res = db_conn.interact(move |conn: &mut MysqlConnection| {
    //             diesel::sql_query("INSERT INTO earnings (miner_id, pool_id, challenge_id, amount) VALUES (?, ?, ?, ?)")
    //             .bind::<Integer, _>(earning.miner_id)
    //             .bind::<Integer, _>(earning.pool_id)
    //             .bind::<Integer, _>(earning.challenge_id)
    //             .bind::<Unsigned<BigInt>, _>(earning.amount)
    //             .execute(conn)
    //         }).await;

    //         match res {
    //             Ok(interaction) => match interaction {
    //                 Ok(_query) => {
    //                     return Ok(());
    //                 }
    //                 Err(e) => {
    //                     error!(target: "server_log", "{:?}", e);
    //                     return Err(AppDatabaseError::QueryFailed);
    //                 }
    //             },
    //             Err(e) => {
    //                 error!(target: "server_log", "{:?}", e);
    //                 return Err(AppDatabaseError::InteractionFailed);
    //             }
    //         }
    //     } else {
    //         return Err(AppDatabaseError::FailedToGetConnectionFromPool);
    //     };
    // }

    pub async fn add_new_earnings_batch(
        &self,
        earnings: Vec<models::InsertEarning>,
    ) -> Result<(), AppDatabaseError> {
        if let Ok(db_conn) = self.connection_pool.get().await {
            let res = db_conn
                .interact(move |conn: &mut MysqlConnection| {
                    insert_into(crate::schema::earnings::dsl::earnings)
                        .values(&earnings)
                        .execute(conn)
                })
                .await;

            match res {
                Ok(interaction) => match interaction {
                    Ok(query) => {
                        info!(target: "server_log", "Earnings inserted: {}", query);
                        if query == 0 {
                            return Err(AppDatabaseError::FailedToInsertRow);
                        }
                        return Ok(());
                    }
                    Err(e) => {
                        error!(target: "server_log", "{:?}", e);
                        return Err(AppDatabaseError::QueryFailed);
                    }
                },
                Err(e) => {
                    error!(target: "server_log", "{:?}", e);
                    return Err(AppDatabaseError::InteractionFailed);
                }
            }
        } else {
            return Err(AppDatabaseError::FailedToGetConnectionFromPool);
        };
    }

    pub async fn add_new_submissions_batch(
        &self,
        submissions: Vec<models::InsertSubmission>,
    ) -> Result<(), AppDatabaseError> {
        if let Ok(db_conn) = self.connection_pool.get().await {
            let res = db_conn
                .interact(move |conn: &mut MysqlConnection| {
                    insert_into(crate::schema::submissions::dsl::submissions)
                        .values(&submissions)
                        .execute(conn)
                })
                .await;

            match res {
                Ok(interaction) => match interaction {
                    Ok(query) => {
                        info!(target: "server_log", "Submissions inserted: {}", query);
                        if query == 0 {
                            return Err(AppDatabaseError::FailedToInsertRow);
                        }
                        return Ok(());
                    }
                    Err(e) => {
                        error!(target: "server_log", "{:?}", e);
                        return Err(AppDatabaseError::QueryFailed);
                    }
                },
                Err(e) => {
                    error!(target: "server_log", "{:?}", e);
                    return Err(AppDatabaseError::InteractionFailed);
                }
            }
        } else {
            return Err(AppDatabaseError::FailedToGetConnectionFromPool);
        };
    }
}
