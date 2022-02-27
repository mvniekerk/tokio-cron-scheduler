mod error;
mod job;
mod job_data;
mod job_scheduler;
mod job_store;
#[cfg(feature = "nats_scheduler")]
mod nats;
mod simple;
mod cron_job;
mod non_cron_job;

#[cfg(feature = "nats_scheduler")]
pub use crate::nats::NatsJobScheduler;
pub use error::JobSchedulerError;
pub use job::JobLocked as Job;
pub use job::JobNotification;
pub use job::JobToRun;
pub use job::OnJobNotification;
pub use job_scheduler::JobSchedulerType;
pub use job_scheduler::JobsSchedulerLocked as JobScheduler;

use crate::job_data::Uuid as JobUuid;
use uuid::Uuid;

impl From<Uuid> for JobUuid {
    fn from(uuid: Uuid) -> Self {
        let uuid = uuid.as_u128();
        let id1 = (uuid >> 64) as u64;
        let id2 = (uuid & 0xFFFF_FFFF_FFFF_FFFF) as u64;
        JobUuid { id1, id2 }
    }
}

impl From<JobUuid> for Uuid {
    fn from(uuid: JobUuid) -> Self {
        let id = ((uuid.id1 as u128) << 64) + (uuid.id2 as u128);
        Uuid::from_u128(id)
    }
}
