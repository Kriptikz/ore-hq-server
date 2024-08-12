use diesel::{sql_types::{Bool, Text}, MysqlConnection, RunQueryDsl};
use deadpool_diesel::mysql::{Manager, Pool};

use crate::{models, Miner};

pub enum AppDatabaseError {
    FailedToGetConnectionFromPool,
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

    pub async fn get_pool_by_authority_pubkey(&self, pool_pubkey: String) -> Result<models::Pool, AppDatabaseError> {
        if let Ok(db_conn) = self.connection_pool.get().await {
            let res = db_conn.interact(move |conn: &mut MysqlConnection| {
                diesel::sql_query("SELECT proof_pubkey, authority_pubkey, total_rewards, claimed_rewards FROM pools WHERE pools.authority_pubkey = ?")
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
                diesel::sql_query("SELECT pubkey, enabled FROM miners WHERE miners.pubkey = ?")
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
