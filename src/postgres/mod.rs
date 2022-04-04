mod metadata_store;

use crate::JobSchedulerError;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio_postgres::{Client, NoTls};
use tracing::error;

#[derive(Clone)]
pub enum PostgresStore {
    Created(String),
    Inited(Arc<RwLock<Client>>),
}

impl Default for PostgresStore {
    fn default() -> Self {
        let url = std::env::var("POSTGRES_URL")
            .map(Some)
            .unwrap_or_default()
            .unwrap_or_else(|| {
                let db_host =
                    std::env::var("POSTGRES_HOST").unwrap_or_else(|_| "localhost".to_string());
                let dbname =
                    std::env::var("POSTGRES_DB").unwrap_or_else(|_| "postgres".to_string());
                let username =
                    std::env::var("POSTGRES_USERNAME").unwrap_or_else(|_| "postgres".to_string());
                let password = std::env::var("POSTGRES_PASSWORD")
                    .map(Some)
                    .unwrap_or_default();
                "".to_string()
                    + "host="
                    + &*db_host
                    + " dbname="
                    + &*dbname
                    + " username="
                    + &*username
                    + &*match password {
                        Some(password) => " password=".to_string() + &*password,
                        None => "".to_string(),
                    }
            });
        Self::Created(url)
    }
}

impl PostgresStore {
    pub fn init(
        self,
    ) -> Pin<Box<dyn Future<Output = Result<PostgresStore, JobSchedulerError>> + Send>> {
        Box::pin(async move {
            match self {
                PostgresStore::Created(url) => {
                    #[cfg(feature = "postgres-openssl")]
                    let tls = postgres_openssl::TlsConnector;
                    #[cfg(feature = "postgres-native-tls")]
                    let tls = postgres_native_tls::TlsConnector;
                    #[cfg(not(any(
                        feature = "postgres-native-tls",
                        feature = "postgres-openssl"
                    )))]
                    let tls = NoTls;
                    let connect = tokio_postgres::connect(&*url, tls).await;
                    if let Err(e) = connect {
                        error!("Error connecting to postgres {:?}", e);
                        return Err(JobSchedulerError::CantInit);
                    }
                    let (client, connection) = connect.unwrap();
                    tokio::spawn(async move {
                        if let Err(e) = connection.await {
                            error!("Error with Postgres Connection {:?}", e);
                        }
                    });
                    Ok(PostgresStore::Inited(Arc::new(RwLock::new(client))))
                }
                PostgresStore::Inited(client) => Ok(PostgresStore::Inited(client)),
            }
        })
    }
}
