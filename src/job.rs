use uuid::Uuid;
use chrono::{DateTime, Utc};
use crate::job_scheduler::JobsSchedulerLocked;
use std::sync::{Arc, RwLock};
use cron::Schedule;
use std::str::FromStr;

pub type JobToRun = dyn FnMut(Uuid, JobsSchedulerLocked) -> () + Send + Sync;
pub struct JobLocked(pub(crate) Arc<RwLock<Job>>);

pub struct Job {
    pub schedule: Schedule,
    pub run: Box<JobToRun>,
    pub last_tick: Option<DateTime<Utc>>,
    pub job_id: Uuid,
    pub count: u32
}

impl JobLocked {
    pub fn new<T>(schedule: &str, run: T) -> Result<Self, Box<dyn std::error::Error>>
    where
        T: 'static,
        T: FnMut(Uuid, JobsSchedulerLocked) -> () + Send + Sync {
        let schedule: Schedule = Schedule::from_str(schedule)?;
        Ok(Self {
            0: Arc::new(RwLock::new(Job {
                schedule,
                run: Box::new(run),
                last_tick: None,
                job_id: Uuid::new_v4(),
                count: 0
            }))
        })
    }

    pub fn tick(&mut self) -> bool {
        let now = Utc::now();
        {
            let s = self.0.write();
            s.map(|mut s| {
                if s.last_tick.is_none() {
                    s.last_tick = Some(now);
                    return false;
                }
                let last_tick = s.last_tick.unwrap();
                s.last_tick = Some(now.clone());
                s.count = if s.count + 1 < u32::MAX { s.count + 1} else { 0 }; // Overflow check
                s.schedule.after(&last_tick).take(1).map(|na| {
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
                }).into_iter().find(|_| true).unwrap_or(false)
            }).unwrap_or(false)
        }
    }
}
