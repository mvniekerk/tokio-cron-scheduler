use crate::job_scheduler::JobsSchedulerLocked;
use chrono::{DateTime, Utc};
use cron::Schedule;
use std::str::FromStr;
use std::sync::{Arc, RwLock};
use uuid::Uuid;
use crate::JobScheduler;

pub type JobToRun = dyn FnMut(Uuid, JobsSchedulerLocked) + Send + Sync;

///
/// A schedulable Job
pub struct JobLocked(pub(crate) Arc<RwLock<Box<dyn Job + Send + Sync>>>);

pub trait Job {
    fn is_cron_job(&self) -> bool;
    fn schedule(&self) -> Option<&Schedule>;
    fn last_tick(&self) -> Option<&DateTime<Utc>>;
    fn set_last_tick(&mut self, tick: Option<DateTime<Utc>>);
    fn set_count(&mut self, count: u32);
    fn count(&self) -> u32;
    fn increment_count(&mut self);
    fn job_id(&self) -> Uuid;
    fn run(&mut self, jobs: JobScheduler);
}

struct CronJob {
    pub schedule: Schedule,
    pub run: Box<JobToRun>,
    pub last_tick: Option<DateTime<Utc>>,
    pub job_id: Uuid,
    pub count: u32,
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
        (self.run)(self.job_id, jobs)
    }
}

impl JobLocked {

    /// Create a new cron job.
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
            0: Arc::new(RwLock::new(Box::new(CronJob {
                schedule,
                run: Box::new(run),
                last_tick: None,
                job_id: Uuid::new_v4(),
                count: 0,
            }))),
        })
    }

   /// Create a new cron job.
   ///
   /// ```rust,ignore
   /// // Run at second 0 of the 15th minute of the 6th, 8th, and 10th hour
   /// // of any day in March and June that is a Friday of the year 2017.
   /// let s: Schedule = "0 15 6,8,10 * Mar,Jun Fri 2017".into().unwrap();
   /// Job::new(s, || println!("I have a complex schedule...") );
   /// ```
    pub fn new_cron_job<T>(schedule: &str, run: T) -> Result<Self, Box<dyn std::error::Error>>
    where
        T: 'static,
        T: FnMut(Uuid, JobsSchedulerLocked) + Send + Sync,
    {
        JobLocked::new(schedule, run)
    }

    ///
    /// The `tick` method returns a true if there was an invocation needed after it was last called
    /// This method will also change the last tick on itself
    pub fn tick(&mut self) -> bool {
        let now = Utc::now();
        {
            let s = self.0.write();
            s.map(|mut s| {
                if s.is_cron_job() {
                    if s.last_tick().is_none() {
                        s.set_last_tick(Some(now));
                        return false;
                    }
                    let last_tick = s.last_tick().unwrap().clone();
                    s.set_last_tick(Some(now));
                    s.increment_count();
                    s.schedule().unwrap()
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
                } else {
                    false
                }
            })
            .unwrap_or(false)
        }
    }

    ///
    /// Get the GUID for the job
    ///
    pub fn guid(&self) -> Uuid {
        let r = self.0.read().unwrap();
        r.job_id()
    }
}
