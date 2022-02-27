use std::collections::HashMap;
use chrono::{DateTime, Utc};
use cron::Schedule;
use tokio::task::JoinHandle;
use uuid::Uuid;
use crate::job::{Job, JobToRunAsync};
use crate::{JobNotification, JobScheduler, JobToRun, OnJobNotification};
use crate::job_data::JobType;

pub struct CronJob {
    pub schedule: Schedule,
    pub run: Box<JobToRun>,
    pub run_async: Box<JobToRunAsync>,
    pub last_tick: Option<DateTime<Utc>>,
    pub job_id: Uuid,
    pub count: u32,
    pub ran: bool,
    pub stopped: bool,
    pub async_job: bool,
    pub on_start: HashMap<Uuid, Box<OnJobNotification>>,
    pub on_stop: HashMap<Uuid, Box<OnJobNotification>>,
    pub on_removed: HashMap<Uuid, Box<OnJobNotification>>,
}

impl Job for CronJob {
    fn is_cron_job(&self) -> bool {
        true
    }

    fn schedule(&self) -> Option<&Schedule> {
        Some(&self.schedule)
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
        for (uuid, on_start) in self.on_start.iter_mut() {
            let future = (on_start)(self.job_id, *uuid, JobNotification::Started);
            tokio::task::spawn(async move {
                future.await;
            });
        }

        let job_id = self.job_id;
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
        self.ran
    }

    fn set_ran(&mut self, ran: bool) {
        self.ran = ran;
    }

    fn set_join_handle(&mut self, _handle: Option<JoinHandle<()>>) {}

    fn abort_join_handle(&mut self) {}

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
