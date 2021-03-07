use saffron::Cron;
use uuid::Uuid;
use chrono::{DateTime, Utc};
use crate::job_scheduler::{JobsScheduleLocked, JobScheduler};
use std::sync::{Arc, RwLock};

pub type JobToRun = Box<dyn (FnMut(&Uuid, &mut JobScheduler) -> ()) + Send + Sync>;
pub type JobLocked = Arc<RwLock<Job>>;

pub struct Job {
    pub schedule: Cron,
    pub run: JobToRun,
    pub last_tick: Option<DateTime<Utc>>,
    pub job_id: Uuid,
    pub count: u32
}

impl Job {
}
