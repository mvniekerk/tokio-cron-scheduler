use std::error::Error;
use uuid::Uuid;
use crate::job::{JobLocked, JobType};
use crate::job_scheduler::{JobScheduler, JobSchedulerLockedType, JobsSchedulerLocked};
use std::time::Duration;
use chrono::Utc;

#[derive(Default, Clone)]
pub struct SimpleJobScheduler {
    jobs: Vec<JobLocked>,
}

unsafe impl Send for SimpleJobScheduler {}
unsafe impl Sync for SimpleJobScheduler {}

impl JobScheduler for SimpleJobScheduler {
    fn add(&mut self, job: JobLocked) -> Result<(), Box<dyn Error + '_>> {
        self.jobs.push(job);
        Ok(())
    }

    fn remove(&mut self, to_be_removed: &Uuid) -> Result<(), Box<dyn Error + '_>> {
        {
            let mut removed: Vec<JobLocked> = vec![];
            self.jobs.retain(|f| !{
                let not_to_be_removed = if let Ok(f) = f.0.read() {
                    f.job_id().eq(to_be_removed)
                } else {
                    false
                };
                if !not_to_be_removed {
                    let f = f.0.clone();
                    removed.push(JobLocked(f))
                }
                not_to_be_removed
            });
            for job in removed {
                let mut job_r = job.0.write().unwrap();
                job_r.set_stopped();
                let job_type = job_r.job_type();
                if matches!(job_type, JobType::OneShot) || matches!(job_type, JobType::Repeated) {
                    job_r.abort_join_handle();
                }
            }
        }
        Ok(())
    }

    fn tick(&mut self, scheduler: JobsSchedulerLocked) -> Result<(), Box<dyn Error + '_>> {
        for jl in self.jobs.iter_mut() {
            if jl.tick() {
                let ref_for_later = jl.0.clone();
                let jobs = scheduler.clone();
                tokio::spawn(async move {
                    let e = ref_for_later.write();
                    if let Ok(mut w) = e {
                        let jt = w.job_type();
                        if matches!(jt, JobType::OneShot) {
                            let mut jobs = jobs.clone();
                            let job_id = w.job_id();
                            tokio::spawn(async move {
                                if let Err(e) = jobs.remove(&job_id) {
                                    eprintln!("Error removing job {:?}", e);
                                }
                            });
                        }
                        w.run(jobs);
                    }
                });
            }
        }

        Ok(())
    }

    fn time_till_next_job(&self) -> Result<Duration, Box<dyn Error + '_>> {
        if self.jobs.is_empty() {
            // Take a guess if there are no jobs.
            return Ok(std::time::Duration::from_millis(500));
        }
        let now = Utc::now();
        let min = self
            .jobs
            .iter()
            .map(|j| {
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
            .flatten()
            .min();

        let m = min
            .unwrap_or_else(chrono::Duration::zero)
            .to_std()
            .unwrap_or_else(|_| std::time::Duration::new(0, 0));
        Ok(m)
    }
}
impl JobSchedulerLockedType for SimpleJobScheduler {}