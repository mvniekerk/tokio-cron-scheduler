use crate::job_scheduler::JobsSchedulerLocked;
use chrono::{DateTime, Utc};
use cron::Schedule;
use std::str::FromStr;
use std::sync::{Arc, RwLock};
use uuid::Uuid;
use crate::JobScheduler;
use std::time::Duration;
use tokio::task::JoinHandle;

pub type JobToRun = dyn FnMut(Uuid, JobsSchedulerLocked) + Send + Sync;

///
/// A schedulable Job
pub struct JobLocked(pub(crate) Arc<RwLock<Box<dyn Job + Send + Sync>>>);

pub enum JobType {
    CronJob,
    OneShot,
    Repeated
}

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
    fn job_type(&self) -> &JobType;
    fn ran(&self) -> bool;
    fn set_ran(&mut self, ran: bool);
    fn set_join_handle(&mut self, handle: Option<JoinHandle<()>>);
    fn abort_join_handle(&mut self);
    fn stop(&self) -> bool;
    fn set_stopped(&mut self);
}

struct CronJob {
    pub schedule: Schedule,
    pub run: Box<JobToRun>,
    pub last_tick: Option<DateTime<Utc>>,
    pub job_id: Uuid,
    pub count: u32,
    pub ran: bool,
    pub stopped: bool,
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

    fn job_type(&self) -> &JobType {
        &JobType::CronJob
    }

    fn ran(&self) -> bool {
        self.ran
    }

    fn set_ran(&mut self, ran: bool) {
        self.ran = ran;
    }

    fn set_join_handle(&mut self, _handle: Option<JoinHandle<()>>) {}

    fn abort_join_handle(&mut self) {}

    fn set_stopped(&mut self) {
        self.stopped = true;
    }

    fn stop(&self) -> bool {
        self.stopped
    }
}

struct NonCronJob {
    pub run: Box<JobToRun>,
    pub last_tick: Option<DateTime<Utc>>,
    pub job_id: Uuid,
    pub join_handle: Option<JoinHandle<()>>,
    pub ran: bool,
    pub count: u32,
    pub job_type: JobType,
    pub stopped: bool
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
        (self.run)(self.job_id, jobs)
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

    fn set_stopped(&mut self) {
        self.stopped = true;
    }

    fn stop(&self) -> bool {
        self.stopped
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
                ran: false,
                stopped: false
            }))),
        })
    }

   /// Create a new cron job.
   ///
   /// ```rust,ignore
   /// // Run at second 0 of the 15th minute of the 6th, 8th, and 10th hour
   /// // of any day in March and June that is a Friday of the year 2017.
   /// let s: Schedule = "0 15 6,8,10 * Mar,Jun Fri 2017".into().unwrap();
   /// Job::new_cron_job(s, || println!("I have a complex schedule...") );
   /// ```
    pub fn new_cron_job<T>(schedule: &str, run: T) -> Result<Self, Box<dyn std::error::Error>>
    where
        T: 'static,
        T: FnMut(Uuid, JobsSchedulerLocked) + Send + Sync,
    {
        JobLocked::new(schedule, run)
    }

    /// Create a new one shot job.
    ///
    /// This is checked if it is running only after 500ms in 500ms intervals.
    /// ```rust,ignore
    /// // Run after 10 seconds
    /// Job::new_on_shot(std::time::Duration::from_seconds(10), || println!("I run once after 10 seconds") );
    /// ```
    pub fn new_one_shot<T>(duration: Duration, run: T) -> Result<Self, Box<dyn std::error::Error>>
    where
        T: 'static,
        T: FnMut(Uuid, JobsSchedulerLocked) + Send + Sync,
    {
        let job = NonCronJob {
            run: Box::new(run),
            last_tick: None,
            job_id: Uuid::new_v4(),
            join_handle: None,
            ran: false,
            count: 0,
            job_type: JobType::OneShot,
            stopped: false
        };

        let job: Arc<RwLock<Box<dyn Job + Send + Sync + 'static>>> = Arc::new(RwLock::new(Box::new(job)));
        let job_for_trigger = job.clone();

        let jh = tokio::spawn(async move {
            tokio::time::sleep(duration).await;
            {
                let j = job_for_trigger.read().unwrap();
                if j.stop() {
                    return;
                }
            }
            {
                let mut j = job_for_trigger.write().unwrap();
                j.set_last_tick(Some(Utc::now()));
            }
        });

        {
            let mut j = job.write().unwrap();
            j.set_join_handle(Some(jh));
        }

        Ok(Self {
            0: job
        })
    }

    /// Create a new one shot job that runs at an instant
    ///
    /// ```rust,ignore
    /// // Run after 20 seconds
    /// let instant = std::time::Instant::now().checked_add(std::time::Duration::from_secs(20));
    /// Job::new_one_shot_at_instant(instant, || println!("I run once after 20 seconds") );
    /// ```
    pub fn new_one_shot_at_instant<T>(instant: std::time::Instant, run: T) -> Result<Self, Box<dyn std::error::Error>>
    where
        T: 'static,
        T: FnMut(Uuid, JobsSchedulerLocked) + Send + Sync,
    {
        let job = NonCronJob {
            run: Box::new(run),
            last_tick: None,
            job_id: Uuid::new_v4(),
            join_handle: None,
            ran: false,
            count: 0,
            job_type: JobType::OneShot,
            stopped: false
        };

        let job: Arc<RwLock<Box<dyn Job + Send + Sync + 'static>>> = Arc::new(RwLock::new(Box::new(job)));
        let job_for_trigger = job.clone();

        let jh = tokio::spawn(async move {
            tokio::time::sleep_until(tokio::time::Instant::from(instant)).await;
            {
                let j = job_for_trigger.read().unwrap();
                if j.stop() {
                    return;
                }
            }
            {
                let mut j = job_for_trigger.write().unwrap();
                j.set_last_tick(Some(Utc::now()));
            }
        });

        {
            let mut j = job.write().unwrap();
            j.set_join_handle(Some(jh));
        }

        Ok(Self {
            0: job
        })
    }

    /// Create a new one shot job.
    ///
    /// This is checked if it is running only after 500ms in 500ms intervals.
    /// ```rust,ignore
    /// // Repeats 10 seconds
    /// Job::new_repeated(std::time::Duration::from_seconds(10), || println!("I run once after 10 seconds") );
    /// ```
    pub fn new_repeated<T>(duration: Duration, run: T) -> Result<Self, Box<dyn std::error::Error>>
        where
            T: 'static,
            T: FnMut(Uuid, JobsSchedulerLocked) + Send + Sync,
    {
        let job = NonCronJob {
            run: Box::new(run),
            last_tick: None,
            job_id: Uuid::new_v4(),
            join_handle: None,
            ran: false,
            count: 0,
            job_type: JobType::Repeated,
            stopped: false
        };

        let job: Arc<RwLock<Box<dyn Job + Send + Sync + 'static>>> = Arc::new(RwLock::new(Box::new(job)));
        let job_for_trigger = job.clone();

        let jh = tokio::spawn(async move {
            let mut interval = tokio::time::interval(duration);
            loop {
                interval.tick().await;
                {
                    let j = job_for_trigger.read().unwrap();
                    if j.stop() {
                        return;
                    }
                }
                {
                    let mut j = job_for_trigger.write().unwrap();
                    j.set_last_tick(Some(Utc::now()));
                }
            }
        });

        {
            let mut j = job.write().unwrap();
            j.set_join_handle(Some(jh));
        }

        Ok(Self {
            0: job
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
                match s.job_type() {
                    JobType::CronJob => {
                        if s.last_tick().is_none() {
                            s.set_last_tick(Some(now));
                            return false;
                        }
                        let last_tick = s.last_tick().unwrap().clone();
                        s.set_last_tick(Some(now));
                        s.increment_count();
                        let must_run = s.schedule().unwrap()
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
                            .unwrap_or(false);

                        if !s.ran() && must_run {
                            s.set_ran(true);
                        }
                        must_run
                    },
                    JobType::OneShot => {
                        if s.last_tick().is_some() {
                            s.set_last_tick(None);
                            s.set_ran(true);
                            true
                        } else {
                            false
                        }
                    },
                    JobType::Repeated => {
                        if s.last_tick().is_some() {
                            s.set_last_tick(None);
                            s.set_ran(true);
                            true
                        } else {
                            false
                        }
                    }
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
