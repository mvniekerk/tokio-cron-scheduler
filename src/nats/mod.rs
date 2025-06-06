mod metadata_store;
mod notification_store;

use async_nats::jetstream::kv::{Config, Store};
use async_nats::jetstream::Context as JetStream;
use async_nats::ConnectOptions;
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::JobSchedulerError;
pub use metadata_store::NatsMetadataStore;
pub use notification_store::NatsNotificationStore;

pub fn sanitize_nats_key(key: &str) -> String {
    key.replace('#', ".")
        .replace(':', ".")
        .replace('/', ".")
        .replace('=', "_")
}

pub fn sanitize_nats_bucket(bucket: &str) -> String {
    sanitize_nats_key(bucket).replace('.', "-")
}

#[derive(Clone)]
pub struct NatsStore {
    pub context: Arc<RwLock<JetStream>>,
    pub inited: bool,
    pub bucket_name: String,
    pub bucket: Arc<RwLock<Store>>,
}

impl NatsStore {
    pub async fn default() -> Self {
        let nats_host =
            std::env::var("NATS_HOST").unwrap_or_else(|_| "nats://localhost".to_string());
        let nats_app = std::env::var("NATS_APP").unwrap_or_else(|_| "Unknown Nats app".to_string());
        let connection = {
            let username = std::env::var("NATS_USERNAME");
            let password = std::env::var("NATS_PASSWORD");
            match (username, password) {
                (Ok(username), Ok(password)) => {
                    let mut options = ConnectOptions::new()
                        .user_and_password(username, password)
                        .name(&*nats_app);
                    if std::path::Path::new("/etc/runtime-certs/").exists() {
                        options = options
                            .add_root_certificates("/etc/runtime-certs/ca.crt".into())
                            .add_client_certificate(
                                "/etc/runtime-certs/tls.crt".into(),
                                "/etc/runtime-certs/tls.key".into(),
                            )
                    }
                    options.connect(&*nats_host).await
                }
                _ => async_nats::connect(&*nats_host).await,
            }
        }
        .unwrap();
        let bucket_name =
            std::env::var("NATS_BUCKET_NAME").unwrap_or_else(|_| "tokiocron".to_string());
        let bucket_name = sanitize_nats_bucket(&bucket_name);
        let bucket_description = std::env::var("NATS_BUCKET_DESCRIPTION")
            .unwrap_or_else(|_| "Tokio Cron Scheduler".to_string());
        let context = async_nats::jetstream::new(connection);
        let bucket = context
            .create_key_value(Config {
                bucket: bucket_name.clone(),
                description: bucket_description,
                history: 1,
                ..Default::default()
            })
            .await
            .unwrap();
        let context = Arc::new(RwLock::new(context));
        let bucket = Arc::new(RwLock::new(bucket));
        Self {
            context,
            inited: true,
            bucket_name,
            bucket,
        }
    }
}

impl NatsStore {
    /// Create a new builder
    pub fn new_builder() -> NatsStoreBuilder {
        NatsStoreBuilder::default()
    }
}

#[derive(Default)]
pub struct NatsStoreBuilder {
    pub username: Option<String>,
    pub password: Option<String>,
    pub host: Option<String>,
    pub app_name: Option<String>,
    pub bucket: Option<String>,
    pub bucket_description: Option<String>,
}

impl NatsStoreBuilder {
    pub fn username(mut self, username: String) -> Self {
        self.username = Some(username);
        self
    }

    pub fn password(mut self, password: String) -> Self {
        self.password = Some(password);
        self
    }

    pub fn host(mut self, host: String) -> Self {
        self.host = Some(host);
        self
    }

    pub fn app_name(mut self, app_name: String) -> Self {
        self.app_name = Some(app_name);
        self
    }

    pub fn bucket(mut self, bucket: String) -> Self {
        self.bucket = Some(bucket);
        self
    }

    pub fn bucket_description(mut self, bucket_description: String) -> Self {
        self.bucket_description = Some(bucket_description);
        self
    }

    /// Build a NatsStore
    pub async fn build(self) -> Result<NatsStore, JobSchedulerError> {
        let NatsStoreBuilder {
            username,
            password,
            host,
            app_name,
            bucket,
            bucket_description,
        } = self;
        let host = host.ok_or_else(|| JobSchedulerError::BuilderNeedsField("host".to_string()))?;
        let bucket =
            bucket.ok_or_else(|| JobSchedulerError::BuilderNeedsField("bucket".to_string()))?;
        let bucket_name = sanitize_nats_bucket(&*bucket);

        let connection = {
            let options = {
                let mut options = match (username, password) {
                    (Some(username), Some(password)) => {
                        Ok(ConnectOptions::new().user_and_password(username, password))
                    }
                    (None, None) => Ok(ConnectOptions::new()),
                    _ => Err(JobSchedulerError::BuilderNeedsField(
                        "username and password both be set".to_string(),
                    )),
                }?;
                if std::path::Path::new("/etc/runtime-certs/").exists() {
                    options = options
                        .add_root_certificates("/etc/runtime-certs/ca.crt".into())
                        .add_client_certificate(
                            "/etc/runtime-certs/tls.crt".into(),
                            "/etc/runtime-certs/tls.key".into(),
                        )
                }
                if let Some(app_name) = app_name {
                    options = options.name(&*app_name);
                }
                options
            };
            options.connect(&*host).await
        }
        .map_err(|e| JobSchedulerError::NatsCouldNotConnect(e.to_string()))?;

        let mut context = async_nats::jetstream::new(connection);
        let mut bucket_config = Config {
            bucket: bucket_name.clone(),
            history: 1,
            ..Default::default()
        };
        if let Some(description) = bucket_description {
            bucket_config = Config {
                description,
                ..bucket_config
            };
        }
        let bucket = context
            .create_key_value(bucket_config)
            .await
            .map_err(|e| JobSchedulerError::NatsCouldNotCreateKvStore(e.to_string()))?;
        let context = Arc::new(RwLock::new(context));
        let bucket = Arc::new(RwLock::new(bucket));
        Ok(NatsStore {
            context,
            inited: true,
            bucket_name,
            bucket,
        })
    }
}
