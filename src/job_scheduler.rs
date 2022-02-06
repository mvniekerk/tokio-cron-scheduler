use crate::error::JobSchedulerError;
use crate::job::JobLocked;
use crate::simple_job_scheduler::SimpleJobScheduler;
use std::sync::{Arc, RwLock};
use tokio::task::JoinHandle;
use uuid::Uuid;

pub trait JobScheduler {
    /// Add a job to the `JobScheduler`
    fn add(&mut self, job: JobLocked) -> Result<(), Box<dyn std::error::Error + '_>>;

    /// Remove a job from the `JobScheduler`
    fn remove(&mut self, to_be_removed: &Uuid) -> Result<(), Box<dyn std::error::Error + '_>>;

    /// The `tick` method increments time for the JobScheduler and executes
    /// any pending jobs.
    fn tick(
        &mut self,
        scheduler: JobsSchedulerLocked,
    ) -> Result<(), Box<dyn std::error::Error + '_>>;

    /// The `time_till_next_job` method returns the duration till the next job
    /// is supposed to run. This can be used to sleep until then without waking
    /// up at a fixed interval.
    fn time_till_next_job(&self) -> Result<std::time::Duration, Box<dyn std::error::Error + '_>>;
}

/// The scheduler type trait. Example implementation is `SimpleJobScheduler`
pub trait JobSchedulerLockedType: JobScheduler + Send + Sync {}

/// The JobScheduler contains and executes the scheduled jobs.
pub struct JobsSchedulerLocked(Arc<RwLock<Box<dyn JobSchedulerLockedType>>>);

impl Clone for JobsSchedulerLocked {
    fn clone(&self) -> Self {
        JobsSchedulerLocked(self.0.clone())
    }
}

impl Default for JobsSchedulerLocked {
    fn default() -> Self {
        Self::new()
    }
}

impl JobsSchedulerLocked {
    /// Create a new `JobSchedulerLocked` using the `SimpleJobScheduler` as scheduler
    pub fn new() -> Self {
        JobsSchedulerLocked(Arc::new(RwLock::new(Box::new(
            SimpleJobScheduler::default(),
        ))))
    }

    /// Create a new `JobsSchedulerLocked` using a custom scheduler
    pub fn new_with_scheduler(scheduler: Box<dyn JobSchedulerLockedType>) -> Self {
        JobsSchedulerLocked(Arc::new(RwLock::new(scheduler)))
    }

    /// Add a job to the `JobScheduler`
    ///
    /// ```rust,ignore
    /// use tokio_cron_scheduler::{Job, JobScheduler, JobToRun};
    /// let mut sched = JobScheduler::new();
    /// sched.add(Job::new("1/10 * * * * *".parse().unwrap(), || {
    ///     println!("I get executed every 10 seconds!");
    /// }));
    /// ```
    pub fn add(&mut self, job: JobLocked) -> Result<(), JobSchedulerError> {
        {
            let mut self_w = self.0.write().map_err(|_e| JobSchedulerError::CantAdd)?;
            if self_w.add(job).is_err() {
                return Err(JobSchedulerError::CantAdd);
            }
        }
        Ok(())
    }

    /// Remove a job from the `JobScheduler`
    ///
    /// ```rust,ignore
    /// use tokio_cron_scheduler::{Job, JobScheduler, JobToRun};
    /// let mut sched = JobScheduler::new();
    /// let job_id = sched.add(Job::new("1/10 * * * * *".parse().unwrap(), || {
    ///     println!("I get executed every 10 seconds!");
    /// }));
    /// sched.remove(job_id);
    /// ```
    pub fn remove(&mut self, to_be_removed: &Uuid) -> Result<(), JobSchedulerError> {
        let mut ws = self.0.write().map_err(|_| JobSchedulerError::CantRemove)?;
        if ws.remove(to_be_removed).is_err() {
            return Err(JobSchedulerError::CantRemove);
        }
        Ok(())
    }

    /// The `tick` method increments time for the JobScheduler and executes
    /// any pending jobs. It is recommended to sleep for at least 500
    /// milliseconds between invocations of this method.
    /// This is kept public if you're running this yourself. It is better to
    /// call the `start` method if you want all of this automated for you.
    ///
    /// ```rust,ignore
    /// loop {
    ///     sched.tick();
    ///     std::thread::sleep(Duration::from_millis(500));
    /// }
    /// ```
    pub fn tick(&mut self) -> Result<(), JobSchedulerError> {
        let mut ws = self.0.write().map_err(|_| JobSchedulerError::TickError)?;
        if ws.tick(self.clone()).is_err() {
            return Err(JobSchedulerError::TickError);
        }
        Ok(())
    }

    /// The `start` spawns a Tokio task where it loops. Every 500ms it
    /// runs the tick method to increment any
    /// any pending jobs.
    ///
    /// ```rust,ignore
    /// if let Err(e) = sched.start().await {
    ///         eprintln!("Error on scheduler {:?}", e);
    ///     }
    /// ```
    pub fn start(&self) -> JoinHandle<()> {
        let jl: JobsSchedulerLocked = self.clone();
        let jh: JoinHandle<()> = tokio::spawn(async move {
            loop {
                tokio::time::sleep(core::time::Duration::from_millis(500)).await;
                let mut jsl = jl.clone();
                let tick = jsl.tick();
                if let Err(e) = tick {
                    eprintln!("Error on job scheduler tick {:?}", e);
                    break;
                }
            }
        });
        jh
    }

    /// The `time_till_next_job` method returns the duration till the next job
    /// is supposed to run. This can be used to sleep until then without waking
    /// up at a fixed interval.AsMut
    ///
    /// ```rust, ignore
    /// loop {
    ///     sched.tick();
    ///     std::thread::sleep(sched.time_till_next_job());
    /// }
    /// ```
    pub fn time_till_next_job(&self) -> Result<std::time::Duration, JobSchedulerError> {
        let r = self
            .0
            .read()
            .map_err(|_| JobSchedulerError::CantGetTimeUntil)?;
        let l = r
            .time_till_next_job()
            .map_err(|_| JobSchedulerError::CantGetTimeUntil)?;
        Ok(l)
    }
}
