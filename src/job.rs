use crate::job_scheduler::JobsSchedulerLocked;
use chrono::{DateTime, Utc};
use cron::Schedule;
use std::str::FromStr;
use std::sync::{Arc, RwLock};
use uuid::Uuid;

pub type JobToRun = dyn FnMut(Uuid, JobsSchedulerLocked) + Send + Sync;

///
/// A schedulable Job
pub struct JobLocked(pub(crate) Arc<RwLock<Job>>);

pub struct Job {
    pub schedule: Schedule,
    pub run: Box<JobToRun>,
    pub last_tick: Option<DateTime<Utc>>,
    pub job_id: Uuid,
    pub count: u32,
}

impl JobLocked {

    /// Create a new job.
    ///
    /// ```rust,ignore
    /// // Run at second 0 of the 15th minute of the 6th, 8th, and 10th hour
    /// // of any day in March and June that is a Friday of the year 2017.
    /// let s: Schedule = "0 15 6,8,10 * Mar,Jun Fri 2017".into().unwrap();
    /// Job::new(s, || println!("I have a complex schedule...") );
    /// ```
    pub fn new<T>(schedule: &str, run: T) -> Result<Self, Box<dyn std::error::Error>>
    where
        T: 'static,
        T: FnMut(Uuid, JobsSchedulerLocked) + Send + Sync,
    {
        let schedule: Schedule = Schedule::from_str(schedule)?;
        Ok(Self {
            0: Arc::new(RwLock::new(Job {
                schedule,
                run: Box::new(run),
                last_tick: None,
                job_id: Uuid::new_v4(),
                count: 0,
            })),
        })
    }

    ///
    /// The `tick` method returns a true if there was an invocation needed after it was last called
    /// This method will also change the last tick on itself
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
                s.last_tick = Some(now);
                s.count = if s.count + 1 < u32::MAX {
                    s.count + 1
                } else {
                    0
                }; // Overflow check
                s.schedule
                    .after(&last_tick)
                    .take(1)
                    .map(|na| {
                        let now_to_next = now.cmp(&na);
                        let last_to_next = last_tick.cmp(&na);

                        matches!(now_to_next, std::cmp::Ordering::Greater) &&
                            matches!(last_to_next, std::cmp::Ordering::Less)
                    })
                    .into_iter()
                    .find(|_| true)
                    .unwrap_or(false)
            })
            .unwrap_or(false)
        }
    }
}
