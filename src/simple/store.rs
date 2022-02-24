use crate::job::JobLocked;
use crate::job_data::JobType;
use std::collections::HashMap;

use crate::job_store::JobStore;
use crate::JobSchedulerError;
use uuid::Uuid;

pub struct SimpleJobStore {
    pub jobs: HashMap<Uuid, JobLocked>,
}

impl JobStore for SimpleJobStore {
    fn add(&mut self, job: JobLocked) -> Result<(), JobSchedulerError> {
        let job_for_insert = job.clone();
        let id = job.0.read().unwrap();
        let id = id.job_id();
        self.jobs.insert(id, job_for_insert);
        Ok(())
    }

    fn remove(&mut self, to_be_removed: &Uuid) -> Result<(), JobSchedulerError> {
        let mut removed: Vec<JobLocked> = vec![];
        self.jobs.retain(|_id, f| !{
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
            let mut job_w = job.0.write().unwrap();
            job_w.set_stopped();
            let job_type = job_w.job_type();
            if matches!(job_type, JobType::OneShot) || matches!(job_type, JobType::Repeated) {
                job_w.abort_join_handle();
            }
            job_w.notify_on_removal();
        }
        Ok(())
    }

    fn list_job_guids(&mut self) -> Result<Vec<Uuid>, JobSchedulerError> {
        Ok(self.jobs.keys().cloned().collect::<Vec<_>>())
    }

    fn get_job(&mut self, job: &Uuid) -> Result<Option<JobLocked>, JobSchedulerError> {
        Ok(self.jobs.get(job).cloned())
    }
}
