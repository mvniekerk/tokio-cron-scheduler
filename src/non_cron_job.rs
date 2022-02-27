use std::collections::HashMap;
use chrono::{DateTime, Utc};
use cron::Schedule;
use tokio::task::JoinHandle;
use uuid::Uuid;
use crate::job::{Job, JobToRunAsync};
use crate::job_data::JobType;
use crate::{JobNotification, JobScheduler, JobToRun, OnJobNotification};

pub struct NonCronJob {
    pub run: Box<JobToRun>,
    pub run_async: Box<JobToRunAsync>,
    pub last_tick: Option<DateTime<Utc>>,
    pub job_id: Uuid,
    pub join_handle: Option<JoinHandle<()>>,
    pub ran: bool,
    pub count: u32,
    pub job_type: JobType,
    pub stopped: bool,
    pub async_job: bool,
    pub on_start: HashMap<Uuid, Box<OnJobNotification>>,
    pub on_stop: HashMap<Uuid, Box<OnJobNotification>>,
    pub on_removed: HashMap<Uuid, Box<OnJobNotification>>,
}

impl Job for NonCronJob {
    fn is_cron_job(&self) -> bool {
        false
    }

    fn schedule(&self) -> Option<&Schedule> {
        None
    }

    fn last_tick(&self) -> Option<&DateTime<Utc>> {
        self.last_tick.as_ref()
    }

    fn set_last_tick(&mut self, tick: Option<DateTime<Utc>>) {
        self.last_tick = tick;
    }

    fn set_count(&mut self, count: u32) {
        self.count = count;
    }

    fn count(&self) -> u32 {
        self.count
    }

    fn increment_count(&mut self) {
        self.count = if self.count + 1 < u32::MAX {
            self.count + 1
        } else {
            0
        }; // Overflow check
    }

    fn job_id(&self) -> Uuid {
        self.job_id
    }

    fn run(&mut self, jobs: JobScheduler) {
        if !self.async_job {
            (self.run)(self.job_id, jobs);
        } else {
            let future = (self.run_async)(self.job_id, jobs);
            tokio::task::spawn(async move {
                future.await;
            });
        }
    }

    fn job_type(&self) -> &JobType {
        &self.job_type
    }

    fn ran(&self) -> bool {
        self.ran
    }

    fn set_ran(&mut self, ran: bool) {
        self.ran = ran;
    }

    fn set_join_handle(&mut self, handle: Option<JoinHandle<()>>) {
        self.join_handle = handle;
    }

    fn abort_join_handle(&mut self) {
        let mut s: Option<JoinHandle<()>> = None;
        std::mem::swap(&mut self.join_handle, &mut s);
        if let Some(jh) = s {
            self.set_join_handle(None);
            jh.abort();
            drop(jh);
        }
    }

    fn stop(&self) -> bool {
        self.stopped
    }

    fn set_stopped(&mut self) {
        self.stopped = true;
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
}