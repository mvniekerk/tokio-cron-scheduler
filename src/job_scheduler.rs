use crate::job::{Job, JobToRun, JobLocked};
use std::sync::{Arc, RwLock, PoisonError, RwLockWriteGuard};
use uuid::Uuid;
use saffron::Cron;
use chrono::Utc;
use simple_error::SimpleError;
use tokio::task::JoinHandle;

pub type JobsScheduleLocked = Arc<RwLock<JobScheduler>>;

#[derive(Default)]
pub struct JobScheduler {
    jobs: RwLock<Vec<JobLocked>>
}

unsafe impl Send for JobScheduler {}

impl JobScheduler {
    pub fn new() -> JobsScheduleLocked {
        let r = JobScheduler { ..Default::default() };
        Arc::new(RwLock::new(r))
    }

    pub fn add(&self, run: JobToRun, schedule: String) -> Result<Uuid, Box<dyn std::error::Error + '_>> {
        let schedule: Cron = schedule.parse().map_err(|e| SimpleError::new(format!("{:?}", e)))?;
        let job_id = {
            let mut w = self.jobs.write()?;

            let job_id = Uuid::new_v4();

            let job = Job {
                schedule,
                run,
                last_tick: None,
                job_id: job_id.clone(),
                count: 0
            };
            w.push(JobLocked(Arc::new(RwLock::new(job))));
            job_id
        };

        Ok(job_id)
    }

    pub fn remove(&self, to_be_removed: &Uuid) -> Result<(), Box<dyn std::error::Error + '_>> {
        {
            let mut w = self.jobs.write()?;
            w.retain(|f| !{
                if let Ok(f) = f.0.read() {
                    f.job_id.eq(&to_be_removed)
                } else {
                    false
                }
            });
        }
        Ok(())
    }

    pub fn tick(&mut self, l: JobsScheduleLocked) -> Result<(), Box<dyn std::error::Error + '_>> {

        let w = self.jobs.write().map(|mut w| {
            for mut jl in w.iter_mut() {
                if jl.tick() {
                    let ref_for_later = jl.0.clone();
                    let jobs = l.clone();
                    tokio::spawn(async move {
                        let e = ref_for_later.write();
                        match e {
                            Ok(mut w) => {
                                let uuid = w.job_id.clone();
                                (w.run)(uuid, jobs);
                            }
                            _ => { }
                        }
                    });
                }
            }
            ()
        })?;
        Ok(())
    }

    pub fn start(jl: JobsScheduleLocked) -> JoinHandle<()> {
        let jh: JoinHandle<()> = tokio::spawn(async move {
            loop {
                tokio::time::sleep(core::time::Duration::from_millis(500)).await;
                let jl = jl.clone();
                {
                    let mut l = jl.write().unwrap();
                    l.tick(jl.clone());
                }
            }
        });
        jh
    }

    pub fn time_till_next_job(&self) -> Result<std::time::Duration, Box<dyn std::error::Error + '_>> {
        let jobs = self.jobs.read()?;
        if jobs.is_empty() {
            // Take a guess if there are no jobs.
            return Ok(std::time::Duration::from_millis(500));
        }
        let now = Utc::now();
        let min = jobs.iter().map(|j| {
            let diff = {
                j.0.read().ok()
                    .and_then(|j|
                        j.schedule.next_after(now)
                            .map(|next| next - now)
                    )
            };
            diff
        })
            .filter(|d| d.is_some())
            .map(|d| d.unwrap())
            .min();

        let m = min.unwrap_or(chrono::Duration::zero()).to_std().unwrap_or(std::time::Duration::new(0, 0));
        Ok(m)
    }
}
