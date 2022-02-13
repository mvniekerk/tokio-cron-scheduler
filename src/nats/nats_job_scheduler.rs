
use std::sync::{Arc, RwLock};
use nats::jetstream::JetStream;

#[derive(Default, Clone)]
pub struct NatsJobScheduler {
    pub context: Arc<RwLock<JetStream>>,
}