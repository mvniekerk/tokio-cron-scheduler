use crate::job::JobLocked;
use std::sync::{Arc, RwLock};
use uuid::Uuid;
use chrono::Utc;
use tokio::task::JoinHandle;

pub struct JobsSchedulerLocked(Arc<RwLock<JobScheduler>>);

impl Clone for JobsSchedulerLocked {
    fn clone(&self) -> Self {
        JobsSchedulerLocked(self.0.clone())
    }
}

#[derive(Default)]
pub struct JobScheduler {
    jobs: Vec<JobLocked>
}

unsafe impl Send for JobScheduler {}

impl JobsSchedulerLocked {
    pub fn new() -> JobsSchedulerLocked {
        let r = JobScheduler { ..Default::default() };
        JobsSchedulerLocked(Arc::new(RwLock::new(r)))
    }

    pub fn add(&mut self, job: JobLocked) -> Result<(), Box<dyn std::error::Error + '_>> {
        {
            let mut self_w = self.0.write()?;
            self_w.jobs.push(job);
        }
        Ok(())
    }

    pub fn remove(&mut self, to_be_removed: &Uuid) -> Result<(), Box<dyn std::error::Error + '_>> {
        {
            let mut ws = self.0.write()?;
            ws.jobs.retain(|f| !{
                if let Ok(f) = f.0.read() {
                    f.job_id.eq(&to_be_removed)
                } else {
                    false
                }
            });
        }
        Ok(())
    }

    pub fn tick(&mut self) -> Result<(), Box<dyn std::error::Error + '_>> {
        let l = self.clone();
        {
            let mut ws = self.0.write()?;
            for jl in ws.jobs.iter_mut() {
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
        }

        Ok(())
    }

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

    pub fn time_till_next_job(&self) -> Result<std::time::Duration, Box<dyn std::error::Error + '_>> {
        let r = self.0.read()?;
        if r.jobs.is_empty() {
            // Take a guess if there are no jobs.
            return Ok(std::time::Duration::from_millis(500));
        }
        let now = Utc::now();
        let min = r.jobs.iter().map(|j| {
            let diff = {
                j.0.read().ok()
                    .and_then(|j|
                        j.schedule.upcoming(Utc).take(1).find(|_| true).map(|next| next - now)
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
