use diesel::{sql_types::{Bool, Text}, MysqlConnection, RunQueryDsl};
use deadpool_diesel::mysql::{Manager, Pool};

use crate::Miner;

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
