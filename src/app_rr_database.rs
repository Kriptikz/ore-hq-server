use deadpool_diesel::{
    mysql::{Manager, Pool},
};
use diesel::{
    connection::SimpleConnection,
    insert_into,
    sql_types::{BigInt, Binary, Bool, Integer, Nullable, Text, TinyInt, Unsigned},
    MysqlConnection, RunQueryDsl,
};
use tracing::{error, info};

use crate::{app_database::AppDatabaseError, models, InsertReward, Miner, Submission, SubmissionWithId};

pub struct AppRRDatabase {
    connection_pool: Pool,
}

impl AppRRDatabase {
    pub fn new(url: String) -> Self {
        let manager = Manager::new(url, deadpool_diesel::Runtime::Tokio1);

        let pool = Pool::builder(manager).build().unwrap();

        AppRRDatabase {
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
                        error!("{:?}", e);
                        return Err(AppDatabaseError::QueryFailed);
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
                        error!("{:?}", e);
                        return Err(AppDatabaseError::QueryFailed);
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

    pub async fn get_last_claim(&self, miner_id: i32) -> Result<models::LastClaim, AppDatabaseError> {
        if let Ok(db_conn) = self.connection_pool.get().await {
            let res = db_conn
                .interact(move |conn: &mut MysqlConnection| {
                    diesel::sql_query("SELECT created_at FROM claims WHERE miner_id = ? ORDER BY id DESC")
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
                        error!("{:?}", e);
                        return Err(AppDatabaseError::QueryFailed);
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

    pub async fn get_miner_submissions(&self, pubkey: String) -> Result<Vec<Submission>, AppDatabaseError> {
        if let Ok(db_conn) = self.connection_pool.get().await {
            let res = db_conn
                .interact(move |conn: &mut MysqlConnection| {
                    diesel::sql_query("SELECT s.* FROM submissions s JOIN miners m ON s.miner_id = m.id WHERE m.pubkey = ? ORDER BY s.created_at DESC LIMIT 100")
                        .bind::<Text, _>(pubkey)
                        .load::<Submission>(conn)
                })
                .await;

            match res {
                Ok(interaction) => match interaction {
                    Ok(query) => {
                        return Ok(query);
                    }
                    Err(e) => {
                        error!("{:?}", e);
                        return Err(AppDatabaseError::QueryFailed);
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
