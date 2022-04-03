mod metadata_store;
mod notification_store;

use nats::jetstream::JetStream;
use nats::kv::{Config, Store};
use std::sync::Arc;
use tokio::sync::RwLock;

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

impl Default for NatsStore {
    fn default() -> Self {
        let nats_host =
            std::env::var("NATS_HOST").unwrap_or_else(|_| "nats://localhost".to_string());
        let nats_app = std::env::var("NATS_APP").unwrap_or_else(|_| "Unknown Nats app".to_string());
        let connection = {
            let username = std::env::var("NATS_USERNAME");
            let password = std::env::var("NATS_PASSWORD");
            match (username, password) {
                (Ok(username), Ok(password)) => {
                    let mut options =
                        nats::Options::with_user_pass(&*username, &*password).with_name(&*nats_app);
                    if std::path::Path::new("/etc/runtime-certs/").exists() {
                        options = options
                            .add_root_certificate("/etc/runtime-certs/ca.crt")
                            .client_cert("/etc/runtime-certs/tls.crt", "/etc/runtime-certs/tls.key")
                    }
                    options.connect(&*nats_host)
                }
                _ => nats::connect(&*nats_host),
            }
        }
        .unwrap();
        let bucket_name =
            std::env::var("NATS_BUCKET_NAME").unwrap_or_else(|_| "tokiocron".to_string());
        let bucket_name = sanitize_nats_bucket(&bucket_name);
        let bucket_description = std::env::var("NATS_BUCKET_DESCRIPTION")
            .unwrap_or_else(|_| "Tokio Cron Scheduler".to_string());
        let context = nats::jetstream::new(connection);
        let bucket = context
            .create_key_value(&Config {
                bucket: bucket_name.clone(),
                description: bucket_description,
                history: 1,
                ..Default::default()
            })
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
