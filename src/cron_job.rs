use crate::job::{Job, JobToRunAsync};
use crate::job_data::{JobState, JobStoredData, JobType};
use crate::job_store::JobStoreLocked;
use crate::{JobScheduler, JobSchedulerError, JobToRun};
use chrono::{DateTime, Utc};
use cron::Schedule;
use std::ops::Add;
use std::str::FromStr;
use std::time::{Duration, SystemTime};
use tokio::sync::oneshot::Receiver;
use uuid::Uuid;

pub struct CronJob {
    pub data: JobStoredData,
    pub run: Box<JobToRun>,
    pub run_async: Box<JobToRunAsync>,
    pub async_job: bool,
}

impl Job for CronJob {
    fn is_cron_job(&self) -> bool {
        true
    }

    fn schedule(&self) -> Option<Schedule> {
        self.data
            .job
            .as_ref()
            .and_then(|j| match j {
                crate::job_data::job_stored_data::Job::CronJob(cj) => Some(&*cj.schedule),
                _ => None,
            })
            .and_then(|s| Schedule::from_str(s).ok())
    }

    fn repeated_every(&self) -> Option<u64> {
        None
    }

    fn last_tick(&self) -> Option<DateTime<Utc>> {
        self.data
            .last_tick
            .map(|lt| SystemTime::UNIX_EPOCH.add(Duration::from_secs(lt)))
            .map(DateTime::from)
    }

    fn set_last_tick(&mut self, tick: Option<DateTime<Utc>>) {
        self.data.last_tick = tick.map(|t| t.timestamp() as u64);
    }

    fn next_tick(&self) -> Option<DateTime<Utc>> {
        if self.data.next_tick == 0 {
            None
        } else {
            Some(self.data.next_tick)
                .map(|lt| SystemTime::UNIX_EPOCH.add(Duration::from_secs(lt)))
                .map(DateTime::from)
        }
    }

    fn set_next_tick(&mut self, tick: Option<DateTime<Utc>>) {
        self.data.next_tick = match tick {
            Some(t) => t.timestamp() as u64,
            None => 0,
        }
    }

    fn set_count(&mut self, count: u32) {
        self.data.count = count;
    }

    fn count(&self) -> u32 {
        self.data.count
    }

    fn increment_count(&mut self) {
        self.data.count = if self.data.count + 1 < u32::MAX {
            self.data.count + 1
        } else {
            0
        }; // Overflow check
    }

    fn job_id(&self) -> Uuid {
        self.data.id.as_ref().cloned().map(|e| e.into()).unwrap()
    }

    fn run(&mut self, jobs: JobScheduler) -> Receiver<bool> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let job_id = self.job_id();

        if !self.async_job {
            (self.run)(job_id, jobs);
            if let Err(e) = tx.send(true) {
                eprintln!("Error notifying done {:?}", e);
            }
        } else {
            let future = (self.run_async)(job_id, jobs);
            tokio::task::spawn(async move {
                future.await;
                if let Err(e) = tx.send(true) {
                    eprintln!("Error notifying done {:?}", e);
                }
            });
        }
        rx
    }

    fn job_type(&self) -> JobType {
        JobType::Cron
    }

    fn ran(&self) -> bool {
        self.data.ran
    }

    fn set_ran(&mut self, ran: bool) {
        self.data.ran = ran;
    }

    fn stop(&self) -> bool {
        self.data.stopped
    }

    fn set_stopped(&mut self) {
        self.data.stopped = true;
    }

    fn set_started(&mut self) {
        self.data.stopped = false;
    }

    fn job_data_from_job_store(
        &mut self,
        job_store: JobStoreLocked,
    ) -> Result<Option<JobStoredData>, JobSchedulerError> {
        let mut job_store = job_store;
        let jd = job_store.get_job_data(&self.job_id())?;
        Ok(jd)
    }

    fn job_data_from_job(&mut self) -> Result<Option<JobStoredData>, JobSchedulerError> {
        Ok(Some(self.data.clone()))
    }

    fn set_job_data(&mut self, job_data: JobStoredData) -> Result<(), JobSchedulerError> {
        self.data = job_data;
        Ok(())
    }
}
