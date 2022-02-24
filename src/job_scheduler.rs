use crate::error::JobSchedulerError;
use crate::job::JobLocked;
use crate::job_store::JobStoreLocked;
use crate::simple::SimpleJobScheduler;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, RwLock};
#[cfg(feature = "signal")]
use tokio::signal::unix::SignalKind;
use tokio::task::JoinHandle;
use uuid::Uuid;

pub type ShutdownNotification =
    dyn FnMut() -> Pin<Box<dyn Future<Output = ()> + Send + Sync>> + Send + Sync;

pub trait JobSchedulerWithoutSync {
    /// Add a job to the `JobScheduler`
    fn add(&mut self, job: JobLocked) -> Result<(), JobSchedulerError>;

    /// Remove a job from the `JobScheduler`
    fn remove(&mut self, to_be_removed: &Uuid) -> Result<(), JobSchedulerError>;

    /// The `tick` method increments time for the JobScheduler and executes
    /// any pending jobs.
    fn tick(&mut self, scheduler: JobsSchedulerLocked) -> Result<(), JobSchedulerError>;

    /// The `time_till_next_job` method returns the duration till the next job
    /// is supposed to run. This can be used to sleep until then without waking
    /// up at a fixed interval.
    fn time_till_next_job(&mut self) -> Result<std::time::Duration, JobSchedulerError>;

    ///
    /// Shuts the scheduler down
    fn shutdown(&mut self) -> Result<(), JobSchedulerError>;

    ///
    /// Code that is run after the shutdown was run
    fn set_shutdown_handler(
        &mut self,
        job: Box<ShutdownNotification>,
    ) -> Result<(), JobSchedulerError>;

    ///
    /// Remove the shutdown handler
    fn remove_shutdown_handler(&mut self) -> Result<(), JobSchedulerError>;

    ///
    /// Start the scheduler
    fn start(&self, scheduler: JobsSchedulerLocked) -> Result<JoinHandle<()>, JobSchedulerError>;

    ///
    /// Set the job store
    fn set_job_store(&mut self, job_store: JobStoreLocked) -> Result<(), JobSchedulerError>;

    ///
    /// Get the job store
    fn get_job_store(&self) -> Result<JobStoreLocked, JobSchedulerError>;
}

/// The scheduler type trait. Example implementation is `SimpleJobScheduler`
pub trait JobSchedulerType: JobSchedulerWithoutSync + Send + Sync {}

/// The JobScheduler contains and executes the scheduled jobs.
pub struct JobsSchedulerLocked(Arc<RwLock<Box<dyn JobSchedulerType>>>);

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

    /// Create a new `JobsSchedulerLocked` using a custom scheduler. The custom scheduler should
    /// implement the trait `JobSchedulerType`
    pub fn new_with_scheduler(scheduler: Box<dyn JobSchedulerType>) -> Self {
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
            self_w.add(job)?;
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
        {
            let mut ws = self.0.write().map_err(|_| JobSchedulerError::CantRemove)?;
            ws.remove(to_be_removed)
                .map_err(|_| JobSchedulerError::CantRemove)?;
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
    pub fn start(&self) -> Result<JoinHandle<()>, JobSchedulerError> {
        let jl: JobsSchedulerLocked = self.clone();
        let r = self
            .0
            .read()
            .map_err(|_| JobSchedulerError::StartScheduler)?;
        let jh = r.start(jl)?;
        Ok(jh)
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
        let mut w = self
            .0
            .write()
            .map_err(|_| JobSchedulerError::CantGetTimeUntil)?;
        let l = w
            .time_till_next_job()
            .map_err(|_| JobSchedulerError::CantGetTimeUntil)?;
        Ok(l)
    }

    ///
    /// Shut the scheduler down
    pub fn shutdown(&mut self) -> Result<(), JobSchedulerError> {
        let mut w = self.0.write().map_err(|_| JobSchedulerError::Shutdown)?;
        w.shutdown().map_err(|_| JobSchedulerError::Shutdown)?;
        Ok(())
    }

    ///
    /// Wait for a signal to shut the runtime down with
    #[cfg(feature = "signal")]
    pub fn shutdown_on_signal(&self, signal: SignalKind) {
        let mut l = self.clone();
        tokio::spawn(async move {
            if let Some(_k) = tokio::signal::unix::signal(signal)
                .expect("Can't wait for signal")
                .recv()
                .await
            {
                l.shutdown().expect("Problem shutting down");
            }
        });
    }

    ///
    /// Wait for a signal to shut the runtime down with
    #[cfg(feature = "signal")]
    pub fn shutdown_on_ctrl_c(&self) {
        let mut l = self.clone();
        tokio::spawn(async move {
            tokio::signal::ctrl_c()
                .await
                .expect("Could not await ctrl-c");

            if let Err(err) = l.shutdown() {
                eprintln!("{:?}", err);
            }
        });
    }

    ///
    /// Code that is run after the shutdown was run
    pub fn set_shutdown_handler(
        &mut self,
        job: Box<ShutdownNotification>,
    ) -> Result<(), JobSchedulerError> {
        let mut w = self
            .0
            .write()
            .map_err(|_| JobSchedulerError::AddShutdownNotifier)?;
        w.set_shutdown_handler(job)?;
        Ok(())
    }

    ///
    /// Remove the shutdown handler
    pub fn remove_shutdown_handler(&mut self) -> Result<(), JobSchedulerError> {
        let mut w = self
            .0
            .write()
            .map_err(|_| JobSchedulerError::RemoveShutdownNotifier)?;
        w.remove_shutdown_handler()?;
        Ok(())
    }
}
