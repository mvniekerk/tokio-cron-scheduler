use nats::jetstream::JetStream;
use std::sync::{Arc, RwLock};

#[derive(Clone)]
pub struct NatsJobScheduler {
    pub context: Arc<RwLock<JetStream>>,
}

impl Default for NatsJobScheduler {
    fn default() -> Self {
        todo!()
    }
}
