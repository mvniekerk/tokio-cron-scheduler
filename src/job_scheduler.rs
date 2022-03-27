use crate::context;
use crate::context::Context;
use crate::error::JobSchedulerError;
use crate::job::to_code::{JobCode, NotificationCode};
use crate::job::{JobCreator, JobDeleter, JobLocked, JobRunner};
use crate::notification::{NotificationCreator, NotificationDeleter, NotificationRunner};
use crate::scheduler::Scheduler;
use crate::simple::{
    SimpleJobCode, SimpleMetadataStore, SimpleNotificationCode, SimpleNotificationStore,
};
use crate::store::{InitStore, MetaDataStorage, NotificationStore};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
#[cfg(feature = "signal")]
use tokio::signal::unix::SignalKind;
use tokio::sync::RwLock;
use tokio::task::JoinHandle;
use uuid::Uuid;

pub type ShutdownNotification =
    dyn FnMut() -> Pin<Box<dyn Future<Output = ()> + Send + Sync>> + Send + Sync;

/// The JobScheduler contains and executes the scheduled jobs.
pub struct JobsSchedulerLocked {
    pub context: Arc<Context>,
    pub inited: bool,
    pub job_creator: Arc<RwLock<JobCreator>>,
    pub job_deleter: Arc<RwLock<JobDeleter>>,
    pub job_runner: Arc<RwLock<JobRunner>>,
    pub notification_creator: Arc<RwLock<NotificationCreator>>,
    pub notification_deleter: Arc<RwLock<NotificationDeleter>>,
    pub notification_runner: Arc<RwLock<NotificationRunner>>,
    pub scheduler: Arc<RwLock<Scheduler>>,
    pub shutdown_notifier: Option<Box<ShutdownNotification>>,
}

impl Clone for JobsSchedulerLocked {
    fn clone(&self) -> Self {
        JobsSchedulerLocked {
            context: self.context.clone(),
            inited: self.inited,
            job_creator: self.job_creator.clone(),
            job_deleter: self.job_deleter.clone(),
            job_runner: self.job_runner.clone(),
            notification_creator: self.notification_creator.clone(),
            notification_deleter: self.notification_deleter.clone(),
            notification_runner: self.notification_runner.clone(),
            scheduler: self.scheduler.clone(),
            shutdown_notifier: None,
        }
    }
}

impl Default for JobsSchedulerLocked {
    fn default() -> Self {
        Self::new().unwrap()
    }
}

impl JobsSchedulerLocked {
    async fn init_context(
        metadata_storage: Arc<RwLock<Box<dyn MetaDataStorage + Send + Sync>>>,
        notification_storage: Arc<RwLock<Box<dyn NotificationStore + Send + Sync>>>,
        job_code: Arc<RwLock<Box<dyn JobCode + Send + Sync>>>,
        notify_code: Arc<RwLock<Box<dyn NotificationCode + Send + Sync>>>,
    ) -> Result<Arc<Context>, JobSchedulerError> {
        {
            let mut metadata_storage = metadata_storage.write().await;
            metadata_storage.init().await?;
        }
        {
            let mut notification_storage = notification_storage.write().await;
            notification_storage.init().await?;
        }
        let context = Context::new(
            metadata_storage,
            notification_storage,
            job_code.clone(),
            notify_code.clone(),
        );
        {
            let mut job_code = job_code.write().await;
            job_code.init(&context).await?;
        }
        {
            let mut notification_code = notify_code.write().await;
            notification_code.init(&context).await?;
        }
        Ok(Arc::new(context))
    }

    async fn init_actors(mut self) -> Result<(), JobSchedulerError> {
        let for_job_runner = self.clone();
        let Self {
            context,
            job_creator,
            job_deleter,
            job_runner,
            notification_creator,
            notification_deleter,
            notification_runner,
            scheduler,
            ..
        } = self;

        {
            let mut job_creator = job_creator.write().await;
            job_creator.init(&context).await?;
        }

        {
            let mut job_deleter = job_deleter.write().await;
            job_deleter.init(&context).await?;
        }

        {
            let mut notification_creator = notification_creator.write().await;
            notification_creator.init(&context).await?;
        }

        {
            let mut notification_deleter = notification_deleter.write().await;
            notification_deleter.init(&context).await?;
        }

        {
            let mut notification_runner = notification_runner.write().await;
            notification_runner.init(&context).await?;
        }

        {
            let mut runner = job_runner.write().await;
            runner.init(&context, for_job_runner).await?;
        }

        {
            let mut scheduler = scheduler.write().await;
            scheduler.init(&context);
        }

        Ok(())
    }

    ///
    /// Initialize the actors
    pub fn init(&mut self) -> Result<(), JobSchedulerError> {
        let mut init = self.clone();

        let (scheduler_init_tx, scheduler_init_rx) = std::sync::mpsc::channel();

        tokio::spawn(async move {
            let init = init.init_actors().await;
            if let Err(e) = scheduler_init_tx.send(init) {
                eprintln!("Error sending error {:?}", e);
            }
        });

        scheduler_init_rx
            .recv()
            .map_err(|_| JobSchedulerError::CantInit)??;
        self.inited = true;
        Ok(())
    }

    ///
    /// Create a new `MetaDataStorage` and `NotificationStore` using the `SimpleMetadataStore`, `SimpleNotificationStore`,
    /// `SimpleJobCode` and `SimpleNotificationCode` implementation
    pub fn new() -> Result<Self, JobSchedulerError> {
        let metadata_storage = SimpleMetadataStore::default();
        let metadata_storage: Arc<RwLock<Box<dyn MetaDataStorage + Send + Sync>>> =
            Arc::new(RwLock::new(Box::new(metadata_storage)));

        let notification_storage = SimpleNotificationStore::default();
        let notification_storage: Arc<RwLock<Box<dyn NotificationStore + Send + Sync>>> =
            Arc::new(RwLock::new(Box::new(notification_storage)));

        let job_code = SimpleJobCode::default();
        let job_code: Arc<RwLock<Box<dyn JobCode + Send + Sync>>> =
            Arc::new(RwLock::new(Box::new(job_code)));

        let notification_code = SimpleNotificationCode::default();
        let notification_code: Arc<RwLock<Box<dyn NotificationCode + Send + Sync>>> =
            Arc::new(RwLock::new(Box::new(notification_code)));

        let (storage_init_tx, storage_init_rx) = std::sync::mpsc::channel();

        tokio::spawn(async move {
            let context = JobsSchedulerLocked::init_context(
                metadata_storage,
                notification_storage,
                job_code,
                notification_code,
            )
            .await;
            if let Err(e) = storage_init_tx.send(context) {
                eprintln!("Error sending init success {:?}", e);
            }
        });

        let context = storage_init_rx
            .recv()
            .map_err(|_| JobSchedulerError::CantInit)??;

        let val = JobsSchedulerLocked {
            context,
            inited: false,
            ..Default::default()
        };

        Ok(val)
    }

    ///
    /// Create a new `JobsSchedulerLocked` using custom metadata and notification runners, job and notification
    /// code providers
    pub fn new_with_storage_and_code(
        metadata_storage: Box<dyn MetaDataStorage + Send + Sync>,
        notification_storage: Box<dyn NotificationStore + Send + Sync>,
        job_code: Box<dyn JobCode + Send + Sync>,
        notification_code: Box<dyn NotificationCode + Send + Sync>,
    ) -> Result<Self, JobSchedulerError> {
        let metadata_storage = Arc::new(RwLock::new(metadata_storage));
        let notification_storage = Arc::new(RwLock::new(notification_storage));
        let job_code = Arc::new(RwLock::new(job_code));
        let notification_code = Arc::new(RwLock::new(notification_code));

        let (storage_init_tx, storage_init_rx) = std::sync::mpsc::channel();

        tokio::spawn(async move {
            let context = JobsSchedulerLocked::init_context(
                metadata_storage,
                notification_storage,
                job_code,
                notification_code,
            )
            .await;
            if let Err(e) = storage_init_tx.send(context) {
                eprintln!("Error sending init success {:?}", e);
            }
        });

        let context = storage_init_rx
            .recv()
            .map_err(|_| JobSchedulerError::CantInit)??;

        let val = JobsSchedulerLocked {
            context,
            inited: false,
            ..Default::default()
        };

        Ok(val)
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
        if !self.inited {
            self.init()?;
        }

        let context = self.context.clone();
        JobCreator::add(&context, job)?;

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
        if !self.inited {
            self.init()?;
        }

        let context = self.context.clone();
        JobDeleter::remove(&context, to_be_removed)
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
        if !self.inited {
            self.init()?;
        }

        let scheduler = self.scheduler.clone();
        let (tx, rx) = std::sync::mpsc::channel();
        tokio::spawn(async move {
            let ret = scheduler.write().await;
            let ret = ret.tick();
            if let Err(e) = tx.send(ret) {
                eprintln!("Error sending tick result {:?}", e);
            }
        });
        let rx = rx.recv();
        match rx {
            Ok(ret) => ret,
            Err(e) => {
                eprintln!("Error receiving tick result {:?}", e);
                Err(JobSchedulerError::TickError)
            }
        }
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
    pub fn start(&mut self) -> Result<(), JobSchedulerError> {
        if !self.inited {
            self.init()?;
        }
        let scheduler = self.scheduler.clone();
        let (tx, rx) = std::sync::mpsc::channel();
        tokio::spawn(async move {
            let mut scheduler = scheduler.write().await;
            let started = scheduler.start();
            if let Err(e) = tx.send(started) {
                eprintln!("Error sending start result {:?}", e);
            }
        });

        let ret = rx.recv();
        match ret {
            Ok(ret) => ret,
            Err(e) => {
                eprintln!("Error receiving start result {:?}", e);
                Err(JobSchedulerError::StartScheduler)
            }
        }
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
    pub fn time_till_next_job(&mut self) -> Result<Option<std::time::Duration>, JobSchedulerError> {
        if !self.inited {
            self.init()?;
        }
        let metadata = self.context.metadata_storage.clone();
        let (tx, rx) = std::sync::mpsc::channel();
        tokio::spawn(async move {
            let mut metadata = metadata.write().await;
            let time = metadata.time_till_next_job().await;
            if let Err(e) = tx.send(time) {
                eprintln!("Error sending result of time till next job {:?}", e);
            }
        });
        let ret = rx.recv();
        match ret {
            Ok(ret) => ret,
            Err(e) => {
                eprintln!("Error getting return of time till next job {:?}", e);
                Err(JobSchedulerError::CantGetTimeUntil)
            }
        }
    }

    ///
    /// Shut the scheduler down
    pub fn shutdown(&mut self) -> Result<(), JobSchedulerError> {
        let mut notify = None;
        std::mem::swap(&mut self.shutdown_notifier, &mut notify);

        let scheduler = self.scheduler.clone();
        tokio::spawn(async move {
            let mut scheduler = scheduler.write().await;
            scheduler.shutdown().await;
        });
        if let Some(mut notify) = notify {
            tokio::spawn(async move {
                let val = notify();
                val.await;
            });
        }
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
    pub fn set_shutdown_handler(&mut self, job: Box<ShutdownNotification>) {
        self.shutdown_notifier = Some(job);
    }

    ///
    /// Remove the shutdown handler
    pub fn remove_shutdown_handler(&mut self) {
        self.shutdown_notifier = None;
    }

    ///
    /// Get the context
    pub fn context(&self) -> Arc<Context> {
        self.context.clone()
    }
}
