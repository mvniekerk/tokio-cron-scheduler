use crate::job_scheduler::JobsSchedulerLocked;
use crate::JobScheduler;
use chrono::{DateTime, Utc};
use cron::Schedule;
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::str::FromStr;
use std::sync::{Arc, RwLock};
use std::time::Duration;
use tokio::task::JoinHandle;
use uuid::Uuid;

pub type JobToRun = dyn FnMut(Uuid, JobsSchedulerLocked) + Send + Sync;
pub type JobToRunAsync = dyn FnMut(Uuid, JobsSchedulerLocked) -> Pin<Box<dyn Future<Output = ()> + Send + Sync>>
    + Send
    + Sync;

pub enum JobNotification {
    Started,
    Stopped,
    Removed,
}

pub type OnJobNotification = dyn FnMut(Uuid, Uuid, JobNotification) -> Pin<Box<dyn Future<Output = ()> + Send + Sync>>
    + Send
    + Sync;

fn nop(_uuid: Uuid, _jobs: JobsSchedulerLocked) {
    // Do nothing
}

fn nop_async(
    _uuid: Uuid,
    _jobs: JobsSchedulerLocked,
) -> Pin<Box<dyn Future<Output = ()> + Send + Sync>> {
    Box::pin(async move {})
}

///
/// A schedulable Job
#[derive(Clone)]
pub struct JobLocked(pub(crate) Arc<RwLock<Box<dyn Job + Send + Sync>>>);

pub enum JobType {
    CronJob,
    OneShot,
    Repeated,
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
    fn on_start_notification_add(&mut self, on_start: Box<OnJobNotification>) -> Uuid;
    fn on_start_notification_remove(&mut self, id: Uuid) -> bool;
    fn on_stop_notification_add(&mut self, on_stop: Box<OnJobNotification>) -> Uuid;
    fn on_stop_notification_remove(&mut self, id: Uuid) -> bool;
    fn on_removed_notification_add(&mut self, on_removed: Box<OnJobNotification>) -> Uuid;
    fn on_removed_notification_remove(&mut self, id: Uuid) -> bool;
    fn notify_on_removal(&mut self);
}

struct CronJob {
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

struct NonCronJob {
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

impl JobLocked {
    /// Create a new cron job.
    ///
    /// ```rust,ignore
    /// let mut sched = JobScheduler::new();
    /// // Run at second 0 of the 15th minute of the 6th, 8th, and 10th hour
    /// // of any day in March and June that is a Friday of the year 2017.
    /// let job = Job::new("0 15 6,8,10 * Mar,Jun Fri 2017", |_uuid, _lock| {
    ///             println!("{:?} Hi I ran", chrono::Utc::now());
    ///         });
    /// sched.add(job)
    /// tokio::spawn(sched.start());
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
                run_async: Box::new(nop_async),
                last_tick: None,
                job_id: Uuid::new_v4(),
                count: 0,
                ran: false,
                stopped: false,
                async_job: false,
                on_stop: HashMap::new(),
                on_start: HashMap::new(),
                on_removed: HashMap::new(),
            }))),
        })
    }

    /// Create a new async cron job.
    ///
    /// ```rust,ignore
    /// let mut sched = JobScheduler::new();
    /// // Run at second 0 of the 15th minute of the 6th, 8th, and 10th hour
    /// // of any day in March and June that is a Friday of the year 2017.
    /// let job = Job::new("0 15 6,8,10 * Mar,Jun Fri 2017", |_uuid, _lock| Box::pin( async move {
    ///             println!("{:?} Hi I ran", chrono::Utc::now());
    ///         }));
    /// sched.add(job)
    /// tokio::spawn(sched.start());
    /// ```
    pub fn new_async<T>(schedule: &str, run: T) -> Result<Self, Box<dyn std::error::Error>>
    where
        T: 'static,
        T: FnMut(Uuid, JobsSchedulerLocked) -> Pin<Box<dyn Future<Output = ()> + Send + Sync>>
            + Send
            + Sync,
    {
        let schedule: Schedule = Schedule::from_str(schedule)?;
        Ok(Self {
            0: Arc::new(RwLock::new(Box::new(CronJob {
                schedule,
                run: Box::new(nop),
                run_async: Box::new(run),
                last_tick: None,
                job_id: Uuid::new_v4(),
                count: 0,
                ran: false,
                stopped: false,
                async_job: true,
                on_stop: HashMap::new(),
                on_start: HashMap::new(),
                on_removed: HashMap::new(),
            }))),
        })
    }

    /// Create a new cron job.
    ///
    /// ```rust,ignore
    /// let mut sched = JobScheduler::new();
    /// // Run at second 0 of the 15th minute of the 6th, 8th, and 10th hour
    /// // of any day in March and June that is a Friday of the year 2017.
    /// let job = Job::new_cron_job("0 15 6,8,10 * Mar,Jun Fri 2017", |_uuid, _lock| {
    ///             println!("{:?} Hi I ran", chrono::Utc::now());
    ///         });
    /// sched.add(job)
    /// tokio::spawn(sched.start());
    /// ```
    pub fn new_cron_job<T>(schedule: &str, run: T) -> Result<Self, Box<dyn std::error::Error>>
    where
        T: 'static,
        T: FnMut(Uuid, JobsSchedulerLocked) + Send + Sync,
    {
        JobLocked::new(schedule, run)
    }

    /// Create a new async cron job.
    ///
    /// ```rust,ignore
    /// let mut sched = JobScheduler::new();
    /// // Run at second 0 of the 15th minute of the 6th, 8th, and 10th hour
    /// // of any day in March and June that is a Friday of the year 2017.
    /// let job = Job::new("0 15 6,8,10 * Mar,Jun Fri 2017", |_uuid, _lock| Box::pin( async move {
    ///             println!("{:?} Hi I ran", chrono::Utc::now());
    ///         }));
    /// sched.add(job)
    /// tokio::spawn(sched.start());
    /// ```
    pub fn new_cron_job_async<T>(schedule: &str, run: T) -> Result<Self, Box<dyn std::error::Error>>
    where
        T: 'static,
        T: FnMut(Uuid, JobsSchedulerLocked) -> Pin<Box<dyn Future<Output = ()> + Send + Sync>>
            + Send
            + Sync,
    {
        JobLocked::new_async(schedule, run)
    }

    fn make_one_shot_job(
        duration: Duration,
        run: Box<JobToRun>,
        run_async: Box<JobToRunAsync>,
        async_job: bool,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let job = NonCronJob {
            run,
            run_async,
            last_tick: None,
            job_id: Uuid::new_v4(),
            join_handle: None,
            ran: false,
            count: 0,
            job_type: JobType::OneShot,
            stopped: false,
            async_job,
            on_stop: HashMap::new(),
            on_start: HashMap::new(),
            on_removed: HashMap::new(),
        };

        let job: Arc<RwLock<Box<dyn Job + Send + Sync + 'static>>> =
            Arc::new(RwLock::new(Box::new(job)));
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

        Ok(Self { 0: job })
    }

    /// Create a new one shot job.
    ///
    /// This is checked if it is running only after 500ms in 500ms intervals.
    /// ```rust,ignore
    /// let mut sched = JobScheduler::new();
    /// let job = Job::new_one_shot(Duration::from_secs(18), |_uuid, _l| {
    ///            println!("{:?} I'm only run once", chrono::Utc::now());
    ///        }
    /// sched.add(job)
    /// tokio::spawn(sched.start());
    /// ```
    pub fn new_one_shot<T>(duration: Duration, run: T) -> Result<Self, Box<dyn std::error::Error>>
    where
        T: 'static,
        T: FnMut(Uuid, JobsSchedulerLocked) + Send + Sync,
    {
        JobLocked::make_one_shot_job(duration, Box::new(run), Box::new(nop_async), false)
    }

    /// Create a new async one shot job.
    ///
    /// This is checked if it is running only after 500ms in 500ms intervals.
    /// ```rust,ignore
    /// let mut sched = JobScheduler::new();
    /// let job = Job::new_one_shot(Duration::from_secs(18), |_uuid, _l| Box::pin(async move {
    ///            println!("{:?} I'm only run once", chrono::Utc::now());
    ///        }));
    /// sched.add(job)
    /// tokio::spawn(sched.start());
    /// ```
    pub fn new_one_shot_async<T>(
        duration: Duration,
        run: T,
    ) -> Result<Self, Box<dyn std::error::Error>>
    where
        T: 'static,
        T: FnMut(Uuid, JobsSchedulerLocked) -> Pin<Box<dyn Future<Output = ()> + Send + Sync>>
            + Send
            + Sync,
    {
        JobLocked::make_one_shot_job(duration, Box::new(nop), Box::new(run), true)
    }

    fn make_new_one_shot_at_an_instant(
        instant: std::time::Instant,
        run: Box<JobToRun>,
        run_async: Box<JobToRunAsync>,
        async_job: bool,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let job = NonCronJob {
            run,
            run_async,
            last_tick: None,
            job_id: Uuid::new_v4(),
            join_handle: None,
            ran: false,
            count: 0,
            job_type: JobType::OneShot,
            stopped: false,
            async_job,
            on_stop: HashMap::new(),
            on_start: HashMap::new(),
            on_removed: HashMap::new(),
        };

        let job: Arc<RwLock<Box<dyn Job + Send + Sync + 'static>>> =
            Arc::new(RwLock::new(Box::new(job)));
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

        Ok(Self { 0: job })
    }

    /// Create a new one shot job that runs at an instant
    ///
    /// ```rust,ignore
    /// // Run after 20 seconds
    /// let mut sched = JobScheduler::new();
    /// let instant = std::time::Instant::now().checked_add(std::time::Duration::from_secs(20));
    /// let job = Job::new_one_shot_at_instant(instant, |_uuid, _lock| println!("I run once after 20 seconds") );
    /// sched.add(job)
    /// tokio::spawn(sched.start());
    /// ```
    pub fn new_one_shot_at_instant<T>(
        instant: std::time::Instant,
        run: T,
    ) -> Result<Self, Box<dyn std::error::Error>>
    where
        T: 'static,
        T: FnMut(Uuid, JobsSchedulerLocked) + Send + Sync,
    {
        JobLocked::make_new_one_shot_at_an_instant(
            instant,
            Box::new(run),
            Box::new(nop_async),
            false,
        )
    }

    /// Create a new async one shot job that runs at an instant
    ///
    /// ```rust,ignore
    /// // Run after 20 seconds
    /// let mut sched = JobScheduler::new();
    /// let instant = std::time::Instant::now().checked_add(std::time::Duration::from_secs(20));
    /// let job = Job::new_one_shot_at_instant(instant, |_uuid, _lock| Box::pin(async move {println!("I run once after 20 seconds")}) );
    /// sched.add(job)
    /// tokio::spawn(sched.start());
    /// ```
    pub fn new_one_shot_at_instant_async<T>(
        instant: std::time::Instant,
        run: T,
    ) -> Result<Self, Box<dyn std::error::Error>>
    where
        T: 'static,
        T: FnMut(Uuid, JobsSchedulerLocked) -> Pin<Box<dyn Future<Output = ()> + Send + Sync>>
            + Send
            + Sync,
    {
        JobLocked::make_new_one_shot_at_an_instant(instant, Box::new(nop), Box::new(run), true)
    }

    fn make_new_repeated(
        duration: Duration,
        run: Box<JobToRun>,
        run_async: Box<JobToRunAsync>,
        async_job: bool,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let job = NonCronJob {
            run,
            run_async,
            last_tick: None,
            job_id: Uuid::new_v4(),
            join_handle: None,
            ran: false,
            count: 0,
            job_type: JobType::Repeated,
            stopped: false,
            async_job,
            on_stop: HashMap::new(),
            on_start: HashMap::new(),
            on_removed: HashMap::new(),
        };

        let job: Arc<RwLock<Box<dyn Job + Send + Sync + 'static>>> =
            Arc::new(RwLock::new(Box::new(job)));
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

        Ok(Self { 0: job })
    }

    /// Create a new repeated job.
    ///
    /// This is checked if it is running only after 500ms in 500ms intervals.
    /// ```rust,ignore
    /// let mut sched = JobScheduler::new();
    /// let job = Job::new_repeated(Duration::from_secs(8), |_uuid, _lock| {
    ///     println!("{:?} I'm repeated every 8 seconds", chrono::Utc::now());
    /// }
    /// sched.add(job)
    /// tokio::spawn(sched.start());
    /// ```
    pub fn new_repeated<T>(duration: Duration, run: T) -> Result<Self, Box<dyn std::error::Error>>
    where
        T: 'static,
        T: FnMut(Uuid, JobsSchedulerLocked) + Send + Sync,
    {
        JobLocked::make_new_repeated(duration, Box::new(run), Box::new(nop_async), false)
    }

    /// Create a new async repeated job.
    ///
    /// This is checked if it is running only after 500ms in 500ms intervals.
    /// ```rust,ignore
    /// let mut sched = JobScheduler::new();
    /// let job = Job::new_repeated(Duration::from_secs(8), |_uuid, _lock| Box::pin(async move {
    ///     println!("{:?} I'm repeated every 8 seconds", chrono::Utc::now());
    /// }));
    /// sched.add(job)
    /// tokio::spawn(sched.start());
    /// ```
    pub fn new_repeated_async<T>(
        duration: Duration,
        run: T,
    ) -> Result<Self, Box<dyn std::error::Error>>
    where
        T: 'static,
        T: FnMut(Uuid, JobsSchedulerLocked) -> Pin<Box<dyn Future<Output = ()> + Send + Sync>>
            + Send
            + Sync,
    {
        JobLocked::make_new_repeated(duration, Box::new(nop), Box::new(run), true)
    }

    ///
    /// The `tick` method returns a true if there was an invocation needed after it was last called
    /// This method will also change the last tick on itself
    pub fn tick(&mut self) -> bool {
        let now = Utc::now();
        {
            let s = self.0.write();
            s.map(|mut s| match s.job_type() {
                JobType::CronJob => {
                    if s.last_tick().is_none() {
                        s.set_last_tick(Some(now));
                        return false;
                    }
                    let last_tick = *s.last_tick().unwrap();
                    s.set_last_tick(Some(now));
                    s.increment_count();
                    let must_run = s
                        .schedule()
                        .unwrap()
                        .after(&last_tick)
                        .take(1)
                        .map(|na| {
                            let now_to_next = now.cmp(&na);
                            let last_to_next = last_tick.cmp(&na);

                            matches!(now_to_next, std::cmp::Ordering::Greater)
                                && matches!(last_to_next, std::cmp::Ordering::Less)
                        })
                        .into_iter()
                        .find(|_| true)
                        .unwrap_or(false);

                    if !s.ran() && must_run {
                        s.set_ran(true);
                    }
                    must_run
                }
                JobType::OneShot => {
                    if s.last_tick().is_some() {
                        s.set_last_tick(None);
                        s.set_ran(true);
                        true
                    } else {
                        false
                    }
                }
                JobType::Repeated => {
                    if s.last_tick().is_some() {
                        s.set_last_tick(None);
                        s.set_ran(true);
                        true
                    } else {
                        false
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

    ///
    /// Run something when the task is started. Returns a UUID as handle for this notification. This
    /// UUID needs to be used when you want to remove the notification handle using `on_start_notification_remove`.
    pub fn on_start_notification_add(&mut self, on_start: Box<OnJobNotification>) -> Uuid {
        let mut w = self.0.write().unwrap();
        w.on_start_notification_add(on_start)
    }

    ///
    /// Remove the notification when the task was started. Uses the same UUID that was returned by
    /// `on_start_notification_add`
    pub fn on_start_notification_remove(&mut self, id: Uuid) -> bool {
        let mut w = self.0.write().unwrap();
        w.on_start_notification_remove(id)
    }

    ///
    /// Run something when the task is stopped. Returns a UUID as handle for this notification. This
    /// UUID needs to be used when you want to remove the notification handle using `on_stop_notification_remove`.
    pub fn on_stop_notification_add(&mut self, on_stop: Box<OnJobNotification>) -> Uuid {
        let mut w = self.0.write().unwrap();
        w.on_stop_notification_add(on_stop)
    }

    ///
    /// Remove the notification when the task was stopped. Uses the same UUID that was returned by
    /// `on_stop_notification_add`
    pub fn on_stop_notification_remove(&mut self, id: Uuid) -> bool {
        let mut w = self.0.write().unwrap();
        w.on_stop_notification_remove(id)
    }

    ///
    /// Run something when the task was removed. Returns a UUID as handle for this notification. This
    /// UUID needs to be used when you want to remove the notification handle using `on_removed_notification_remove`.
    pub fn on_removed_notification_add(&mut self, on_removed: Box<OnJobNotification>) -> Uuid {
        let mut w = self.0.write().unwrap();
        w.on_stop_notification_add(on_removed)
    }

    ///
    /// Remove the notification when the task was removed. Uses the same UUID that was returned by
    /// `on_removed_notification_add`
    pub fn on_removed_notification_remove(&mut self, id: Uuid) -> bool {
        let mut w = self.0.write().unwrap();
        w.on_removed_notification_remove(id)
    }
}
