
use std::sync::{Arc, RwLock};
use nats::jetstream::JetStream;

#[derive(Clone)]
pub struct NatsJobScheduler {
    pub context: Arc<RwLock<JetStream>>,
}

impl Default for NatsJobScheduler {
    fn default() -> Self {
        todo!()
    }
}