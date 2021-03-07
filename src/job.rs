use saffron::Cron;
use uuid::Uuid;
use chrono::{DateTime, Utc};
use crate::job_scheduler::{JobsScheduleLocked, JobScheduler};
use std::sync::{Arc, RwLock};

pub type JobToRun = Box<dyn (FnMut(Uuid, JobsScheduleLocked) -> ()) + Send + Sync>;
pub struct JobLocked(pub(crate) Arc<RwLock<Job>>);

pub struct Job {
    pub schedule: Cron,
    pub run: JobToRun,
    pub last_tick: Option<DateTime<Utc>>,
    pub job_id: Uuid,
    pub count: u32
}

impl JobLocked {
    pub fn tick(&mut self) -> bool {
        let now = Utc::now();
        {
            let mut s = self.0.write();
            s.map(|mut s| {
                if s.last_tick.is_none() {
                    s.last_tick = Some(now);
                    return false;
                }
                let last_tick = s.last_tick.unwrap();
                s.last_tick = Some(now.clone());
                s.count = if s.count + 1 < u32::MAX { s.count + 1} else { 0 }; // Overflow check
                s.schedule.next_after(last_tick).map(|na| {
                    let now_to_next = now.cmp(&na);
                    let last_to_next = last_tick.cmp(&na);

                    match now_to_next {
                        std::cmp::Ordering::Greater => {
                            match last_to_next {
                                std::cmp::Ordering::Less => true,
                                _ => false
                            }
                        },
                        _ => false
                    }
                }).unwrap_or(false)
            }).unwrap_or(false)
        }
    }
}
