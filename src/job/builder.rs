#[cfg(not(feature = "has_bytes"))]
use crate::job::job_data::{JobType, Uuid};
#[cfg(feature = "has_bytes")]
use crate::job::job_data_prost::{JobType, Uuid};
use crate::{JobSchedulerError, JobToRun, JobToRunAsync};
use chrono::TimeZone;
use core::time::Duration;
use cron::Schedule;
use std::convert::TryInto;
use std::time::Instant;

pub struct JobBuilder<T: TimeZone> {
    pub job_id: Option<Uuid>,
    pub timezone: Option<T>,
    pub job_type: Option<JobType>,
    pub schedule: Option<String>,
    pub run: Option<Box<JobToRun>>,
    pub run_async: Option<Box<JobToRunAsync>>,
    pub duration: Option<Duration>,
    pub repeating: Option<bool>,
    pub instant: Option<Instant>,
}

impl<T: TimeZone> JobBuilder<T> {
    pub fn new() -> Self {
        Self {
            job_id: None,
            timezone: None,
            job_type: None,
            schedule: None,
            run: None,
            run_async: None,
            duration: None,
            repeating: None,
            instant: None,
        }
    }

    pub fn with_timezone<U: TimeZone>(self, timezone: U) -> JobBuilder<U> {
        JobBuilder {
            timezone: Some(timezone),
            job_id: self.job_id,
            job_type: self.job_type,
            schedule: self.schedule,
            run: self.run,
            run_async: self.run_async,
            duration: self.duration,
            repeating: self.repeating,
            instant: self.instant,
        }
    }

    pub fn with_job_id(self, job_id: Uuid) -> Self {
        Self {
            job_id: Some(job_id),
            ..self
        }
    }

    pub fn with_job_type(self, job_type: JobType) -> Self {
        Self {
            job_type: Some(job_type),
            ..self
        }
    }

    pub fn with_schedule<U, E>(self, schedule: U) -> Result<Self, JobSchedulerError>
    where
        U: TryInto<Schedule, Error = E>,
        E: std::error::Error + 'static,
    {
        let schedule: Schedule = schedule
            .try_into()
            .map_err(|_| JobSchedulerError::ParseSchedule)?;
        Ok(Self {
            schedule: Some(schedule.to_string()),
            ..self
        })
    }

    pub fn with_run_sync(self, job: Box<JobToRun>) -> Self {
        Self {
            run: Some(Box::new(job)),
            ..self
        }
    }

    pub fn with_run_async(self, job: Box<JobToRunAsync>) -> Self {
        Self {
            run_async: Some(Box::new(job)),
            ..self
        }
    }

    pub fn every_seconds(self, seconds: u64) -> Self {
        Self {
            duration: Some(Duration::from_secs(seconds)),
            repeating: Some(true),
            ..self
        }
    }

    pub fn after_seconds(self, seconds: u64) -> Self {
        Self {
            duration: Some(Duration::from_secs(seconds)),
            repeating: Some(false),
            ..self
        }
    }

    pub fn at_instant(self, instant: Instant) -> Self {
        Self {
            instant: Some(instant),
            ..self
        }
    }
}
