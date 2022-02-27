use std::collections::HashMap;
use std::ops::Add;
use std::str::FromStr;
use std::time::{Duration, SystemTime};
use chrono::{DateTime, Utc};
use cron::Schedule;
use tokio::task::JoinHandle;
use uuid::Uuid;
use crate::job::{Job, JobToRunAsync};
use crate::{JobNotification, JobScheduler, JobSchedulerError, JobToRun, OnJobNotification};
use crate::job_data::{JobData, JobType};
use crate::job_store::JobStoreLocked;

pub struct CronJob {
    pub data: JobData,
    pub run: Box<JobToRun>,
    pub run_async: Box<JobToRunAsync>,
    pub job_id: Uuid,
    pub async_job: bool,
    pub on_start: HashMap<Uuid, Box<OnJobNotification>>,
    pub on_stop: HashMap<Uuid, Box<OnJobNotification>>,
    pub on_removed: HashMap<Uuid, Box<OnJobNotification>>,
}

impl Job for CronJob {
    fn is_cron_job(&self) -> bool {
        true
    }

    fn schedule(&self) -> Option<Schedule> {
        self.data.job.as_ref().map(|j| match j {
            crate::job_data::job_data::Job::CronJob(cj) => Some(&*cj.schedule),
            _ => None
        })
            .flatten()
            .map(|s| Schedule::from_str(s).ok())
            .flatten()
    }

    fn last_tick(&self) -> Option<DateTime<Utc>> {
        self.data.last_tick
            .map(|lt| SystemTime::UNIX_EPOCH.add(Duration::from_secs(lt)))
            .map(DateTime::from)
    }

    fn set_last_tick(&mut self, tick: Option<DateTime<Utc>>) {
        self.data.last_tick = tick.map(|t| t.timestamp() as u64);
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

    fn run(&mut self, jobs: JobScheduler) {
        let job_id = self.job_id();
        for (uuid, on_start) in self.on_start.iter_mut() {
            let future = (on_start)(job_id.clone(), *uuid, JobNotification::Started);
            tokio::task::spawn(async move {
                future.await;
            });
        }

        let on_stops = self
            .on_stop
            .iter_mut()
            .map(|(uuid, on_stopped)| (on_stopped)(job_id, *uuid, JobNotification::Stopped))
            .collect::<Vec<_>>();
        if !self.async_job {
            (self.run)(job_id, jobs);
            for on_stop in on_stops {
                tokio::task::spawn(async move {
                    on_stop.await;
                });
            }
        } else {
            let future = (self.run_async)(job_id, jobs);
            tokio::task::spawn(async move {
                future.await;
                for on_stop in on_stops {
                    tokio::task::spawn(async move {
                        on_stop.await;
                    });
                }
            });
        }
    }

    fn job_type(&self) -> &JobType {
        &JobType::Cron
    }

    fn ran(&self) -> bool {
        self.data.ran
    }

    fn set_ran(&mut self, ran: bool) {
        self.data.ran = ran;
    }

    fn set_join_handle(&mut self, _handle: Option<JoinHandle<()>>) {}

    fn abort_join_handle(&mut self) {}

    fn stop(&self) -> bool {
        self.data.stopped
    }

    fn set_stopped(&mut self) {
        self.data.stopped = true;
    }

    fn on_start_notification_add(&mut self, on_start: Box<OnJobNotification>) -> Uuid {
        let uuid = Uuid::new_v4();
        self.on_start.insert(uuid, on_start);
        uuid
    }

    fn on_start_notification_remove(&mut self, id: Uuid) -> bool {
        self.on_start.remove(&id).is_some()
    }

    fn on_stop_notification_add(&mut self, on_stop: Box<OnJobNotification>) -> Uuid {
        let uuid = Uuid::new_v4();
        self.on_stop.insert(uuid, on_stop);
        uuid
    }

    fn on_stop_notification_remove(&mut self, id: Uuid) -> bool {
        self.on_stop.remove(&id).is_some()
    }

    fn on_removed_notification_add(&mut self, on_removed: Box<OnJobNotification>) -> Uuid {
        let uuid = Uuid::new_v4();
        self.on_removed.insert(uuid, on_removed);
        uuid
    }

    fn on_removed_notification_remove(&mut self, id: Uuid) -> bool {
        self.on_removed.remove(&id).is_some()
    }

    fn notify_on_removal(&mut self) {
        let job_id = self.job_id;
        self.on_removed
            .iter_mut()
            .map(|(uuid, on_removed)| (on_removed)(job_id, *uuid, JobNotification::Removed))
            .for_each(|v| {
                tokio::task::spawn(async move {
                    v.await;
                });
            });
    }

    fn job_data_from_job_store(&mut self, job_store: JobStoreLocked) -> Result<Option<JobData>, JobSchedulerError> {
        let mut job_store = job_store.clone();
        let jd = job_store.get_job_data(&self.job_id)?;
        Ok(jd)
    }

    fn job_data_from_job(&mut self) -> Result<Option<JobData>, JobSchedulerError> {
        Ok(Some(self.data.clone()))
    }
}
