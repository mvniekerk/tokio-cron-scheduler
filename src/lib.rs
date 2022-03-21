mod context;
mod error;
mod job;
mod job_scheduler;
mod job_store;
#[cfg(feature = "nats_scheduler")]
mod nats;
mod scheduler;
mod simple;
mod store;

#[cfg(feature = "nats_scheduler")]
pub use crate::nats::NatsJobScheduler;
use chrono::{DateTime, Utc};
use cron::Schedule;
pub use error::JobSchedulerError;
pub use job::job_data::JobState as JobNotification;
pub use job::JobLocked as Job;
pub use job::JobToRun;
pub use job::OnJobNotification;
pub use job_scheduler::JobSchedulerType;
pub use job_scheduler::JobsSchedulerLocked as JobScheduler;
use std::ops::Add;
use std::str::FromStr;
use std::time::{Duration, SystemTime};

use job::job_data::{JobAndNextTick, JobStoredData, Uuid as JobUuid};
use uuid::Uuid;

impl From<Uuid> for JobUuid {
    fn from(uuid: Uuid) -> Self {
        let uuid = uuid.as_u128();
        let id1 = (uuid >> 64) as u64;
        let id2 = (uuid & 0xFFFF_FFFF_FFFF_FFFF) as u64;
        JobUuid { id1, id2 }
    }
}

impl From<&Uuid> for JobUuid {
    fn from(uuid: &Uuid) -> Self {
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

impl From<&JobUuid> for Uuid {
    fn from(uuid: &JobUuid) -> Self {
        let id = ((uuid.id1 as u128) << 64) + (uuid.id2 as u128);
        Uuid::from_u128(id)
    }
}

impl JobAndNextTick {
    pub fn utc(lt: u64) -> DateTime<Utc> {
        let dt = SystemTime::UNIX_EPOCH.add(Duration::from_secs(lt));
        let dt: DateTime<Utc> = DateTime::from(dt);
        dt
    }

    fn next_tick_utc(&self) -> Option<DateTime<Utc>> {
        match self.next_tick {
            0 => None,
            val => Some(JobAndNextTick::utc(val)),
        }
    }

    fn last_tick_utc(&self) -> Option<DateTime<Utc>> {
        self.last_tick.map(JobAndNextTick::utc)
    }
}

impl JobStoredData {
    pub fn schedule(&self) -> Option<Schedule> {
        self.job
            .as_ref()
            .and_then(|j| match j {
                job::job_data::job_stored_data::Job::CronJob(cj) => Some(&*cj.schedule),
                _ => None,
            })
            .and_then(|s| Schedule::from_str(s).ok())
    }

    pub fn next_tick_utc(&self) -> Option<DateTime<Utc>> {
        match self.next_tick {
            0 => None,
            val => Some(JobAndNextTick::utc(val)),
        }
    }

    pub fn last_tick_utc(&self) -> Option<DateTime<Utc>> {
        self.last_tick.map(JobAndNextTick::utc)
    }

    pub fn repeated_every(&self) -> Option<u64> {
        self.job.as_ref().and_then(|jt| match jt {
            job::job_data::job_stored_data::Job::CronJob(_) => None,
            job::job_data::job_stored_data::Job::NonCronJob(ncj) => Some(ncj.repeated_every),
        })
    }

    pub fn set_next_tick(&mut self, tick: Option<DateTime<Utc>>) {
        self.next_tick = match tick {
            Some(t) => t.timestamp() as u64,
            None => 0,
        }
    }

    pub fn set_last_tick(&mut self, tick: Option<DateTime<Utc>>) {
        self.last_tick = tick.map(|t| t.timestamp() as u64);
    }
}
