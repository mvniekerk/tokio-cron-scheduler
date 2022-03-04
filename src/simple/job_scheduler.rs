use crate::job::JobLocked;
use crate::job_data::{JobState, JobType};
use crate::job_scheduler::{
    JobSchedulerType, JobSchedulerWithoutSync, JobsSchedulerLocked, ShutdownNotification,
};
use crate::job_store::JobStoreLocked;
use crate::JobSchedulerError;
use chrono::Utc;
use std::sync::{Arc, RwLock};
use std::time::Duration;
use tokio::task::JoinHandle;
use uuid::Uuid;

#[derive(Default, Clone)]
pub struct SimpleJobScheduler {
    job_store: JobStoreLocked,
    shutdown_handler: Option<Arc<RwLock<Box<ShutdownNotification>>>>,
}

unsafe impl Send for SimpleJobScheduler {}
unsafe impl Sync for SimpleJobScheduler {}

impl JobSchedulerWithoutSync for SimpleJobScheduler {
    fn add(&mut self, job: JobLocked) -> Result<(), JobSchedulerError> {
        self.job_store.add(job)?;
        Ok(())
    }

    fn remove(&mut self, to_be_removed: &Uuid) -> Result<(), JobSchedulerError> {
        self.job_store.remove(to_be_removed)?;
        Ok(())
    }

    fn tick(&mut self, scheduler: JobsSchedulerLocked) -> Result<(), JobSchedulerError> {
        let guids = self.job_store.list_job_guids()?;
        for guid in guids {
            let mut jl = {
                let job = self.job_store.get_job(&guid);
                match job {
                    Ok(Some(job)) => job,
                    _ => continue,
                }
            };

            let tick = jl.tick();
            if matches!(tick, Err(JobSchedulerError::NoNextTick)) {
                let mut js = self.job_store.clone();
                tokio::spawn(async move {
                    let guid = guid;
                    if let Err(e) = js.remove(&guid) {
                        eprintln!("Error removing {:?} {:?}", guid, e);
                    }
                });
                continue;
            }
            if tick.is_err() {
                eprintln!("Error running tick on {:?}", guid);
                continue;
            }
            if matches!(tick, Ok(false)) {
                continue;
            }

            let mut js = self.job_store.clone();
            let jd = {
                let mut w = jl.0.write().unwrap();
                w.job_data_from_job()?
            };
            if let Some(jd) = jd {
                tokio::spawn(async move {
                    if let Err(e) = js.update_job_data(jd) {
                        eprintln!("Error updating job data {:?}", e);
                    }
                });
            }

            let ref_for_later = jl.0.clone();
            let jobs = scheduler.clone();
            tokio::spawn(async move {
                let e = ref_for_later.write();
                if let Ok(mut w) = e {
                    let jt = w.job_type();

                    let job_id = w.job_id();
                    if matches!(jt, JobType::OneShot) {
                        let mut jobs = jobs.clone();
                        let job_id = job_id;
                        tokio::spawn(async move {
                            if let Err(e) = jobs.remove(&job_id) {
                                eprintln!("Error removing job {:?}", e);
                            }
                        });
                    }
                    match jobs.get_job_store() {
                        Ok(mut job_store) => {
                            if let Err(err) =
                                job_store.notify_on_job_state(&job_id, JobState::Started)
                            {
                                eprintln!("Error notifying on job started {:?}", err);
                            }
                            let rx = w.run(jobs);
                            tokio::spawn(async move {
                                if let Err(e) = rx.recv() {
                                    eprintln!("Error waiting for task to finish {:?}", e);
                                }
                                if let Err(err) =
                                    job_store.notify_on_job_state(&job_id, JobState::Done)
                                {
                                    eprintln!("Error notifying on job started {:?}", err);
                                }
                            });
                        }
                        Err(e) => {
                            eprintln!("Error getting job store {:?}", e);
                        }
                    };
                }
            });
        }

        Ok(())
    }

    fn time_till_next_job(&mut self) -> Result<Duration, JobSchedulerError> {
        let guids = self.job_store.list_job_guids()?;
        if guids.is_empty() {
            // Take a guess if there are no jobs.
            return Ok(std::time::Duration::from_millis(500));
        }
        let now = Utc::now();
        let min = guids
            .iter()
            .flat_map(|g| self.job_store.get_job(g))
            .flatten()
            .filter_map(|j| {
                let diff = {
                    j.0.read().ok().and_then(|j| {
                        j.schedule().and_then(|s| {
                            s.upcoming(Utc)
                                .take(1)
                                .find(|_| true)
                                .map(|next| next - now)
                        })
                    })
                };
                diff
            })
            .min();

        let m = min
            .unwrap_or_else(chrono::Duration::zero)
            .to_std()
            .unwrap_or_else(|_| std::time::Duration::new(0, 0));
        Ok(m)
    }

    fn shutdown(&mut self) -> Result<(), JobSchedulerError> {
        let guids = self.job_store.list_job_guids()?;
        for guid in guids {
            self.remove(&guid)?;
        }
        if let Some(e) = self.shutdown_handler.clone() {
            let fut = {
                e.write()
                    .map(|mut w| (w)())
                    .map_err(|_| JobSchedulerError::ShutdownNotifier)
            }?;
            tokio::task::spawn(async move {
                fut.await;
            });
        }
        Ok(())
    }

    ///
    /// Code that is run after the shutdown was run
    fn set_shutdown_handler(
        &mut self,
        job: Box<ShutdownNotification>,
    ) -> Result<(), JobSchedulerError> {
        self.shutdown_handler = Some(Arc::new(RwLock::new(job)));
        Ok(())
    }

    ///
    /// Remove the shutdown handler
    fn remove_shutdown_handler(&mut self) -> Result<(), JobSchedulerError> {
        self.shutdown_handler = None;
        Ok(())
    }

    /// Start the simple job scheduler
    fn start(&self, scheduler: JobsSchedulerLocked) -> Result<JoinHandle<()>, JobSchedulerError> {
        let jh: JoinHandle<()> = tokio::spawn(async move {
            loop {
                tokio::time::sleep(core::time::Duration::from_millis(500)).await;
                let mut jsl = scheduler.clone();
                let tick = jsl.tick();
                if let Err(e) = tick {
                    eprintln!("Error on job scheduler tick {:?}", e);
                    break;
                }
            }
        });
        Ok(jh)
    }

    ///
    /// Set the job store for this scheduler
    fn set_job_store(&mut self, job_store: JobStoreLocked) -> Result<(), JobSchedulerError> {
        self.job_store = job_store;
        Ok(())
    }

    ///
    /// Get the job store in this scheduler
    fn get_job_store(&self) -> Result<JobStoreLocked, JobSchedulerError> {
        Ok(self.job_store.clone())
    }
}
impl JobSchedulerType for SimpleJobScheduler {}
