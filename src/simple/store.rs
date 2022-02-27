use crate::job::JobLocked;
use crate::job_data::{JobState, JobStoredData, JobType};
use std::collections::{HashMap, HashSet};

use crate::job_store::JobStore;
use crate::{JobSchedulerError, OnJobNotification};
use uuid::Uuid;

#[derive(Default)]
pub struct SimpleJobStore {
    pub jobs: HashMap<Uuid, JobLocked>,
    pub notification_guids: HashMap<JobState, HashMap<Uuid, HashSet<Uuid>>>,
    pub job_notification_guids: HashMap<Uuid, Box<OnJobNotification>>,
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
            let job_id = to_be_removed;
            if let Err(e) = self.notify_on_job_state(job_id, JobState::Removed) {
                eprintln!("Error notifying on job state {:?}", e);
            }
            let job_notification_uuids = self
                .notification_guids
                .iter_mut()
                .filter_map(|(_js, v)| v.get(job_id))
                .flat_map(|hs| hs.iter().cloned());

            for job_notification_uuid in job_notification_uuids {
                self.job_notification_guids.remove(&job_notification_uuid);
            }
            for (_js, v) in self.notification_guids.iter_mut() {
                v.remove(job_id);
            }
        }
        Ok(())
    }

    fn list_job_guids(&mut self) -> Result<Vec<Uuid>, JobSchedulerError> {
        Ok(self.jobs.keys().cloned().collect::<Vec<_>>())
    }

    fn get_job(&mut self, job: &Uuid) -> Result<Option<JobLocked>, JobSchedulerError> {
        Ok(self.jobs.get(job).cloned())
    }

    fn get_job_data(&mut self, job: &Uuid) -> Result<Option<JobStoredData>, JobSchedulerError> {
        let jd = {
            self.jobs.get(job).and_then(|j| {
                let mut j =
                    j.0.write()
                        .map_err(|_| JobSchedulerError::GetJobData)
                        .ok()?;
                j.job_data_from_job().ok()?
            })
        };
        Ok(jd)
    }

    fn add_notification(
        &mut self,
        job: &Uuid,
        notification_guid: &Uuid,
        on_notification: Box<OnJobNotification>,
        notifications: Vec<JobState>,
    ) -> Result<(), JobSchedulerError> {
        self.job_notification_guids
            .insert(*notification_guid, on_notification);
        for js in notifications {
            self.notification_guids
                .entry(js)
                .or_insert_with(HashMap::new);
            let states = self.notification_guids.get_mut(&js).unwrap();
            states.entry(*job).or_insert_with(HashSet::new);
            let job_notifications_for_state = states.get_mut(job).unwrap();
            job_notifications_for_state.insert(*notification_guid);
        }
        Ok(())
    }

    fn remove_notification(&mut self, notification_guid: &Uuid) -> Result<(), JobSchedulerError> {
        self.job_notification_guids.remove(notification_guid);
        for (_js, states) in self.notification_guids.iter_mut() {
            for (_job_id, notifications) in states.iter_mut() {
                notifications.remove(notification_guid);
            }
        }
        Ok(())
    }

    fn remove_notification_for_job_state(
        &mut self,
        notification_guid: &Uuid,
        js: JobState,
    ) -> Result<bool, JobSchedulerError> {
        let mut ret = false;
        if let Some(v) = self.notification_guids.get_mut(&js) {
            for (_job_id, n) in v.iter_mut() {
                ret |= n.remove(notification_guid);
            }
        }
        let found_once = self.notification_guids.iter_mut().any(|(_js, states)| {
            states
                .iter_mut()
                .any(|(_job_id, notifications)| notifications.contains(notification_guid))
        });
        if !found_once {
            self.job_notification_guids.remove(notification_guid);
        }
        Ok(ret)
    }

    fn notify_on_job_state(
        &mut self,
        job_id: &Uuid,
        js: JobState,
    ) -> Result<(), JobSchedulerError> {
        let uuids = self.notification_guids.get(&js).and_then(|c| c.get(job_id));
        if let Some(hs) = uuids {
            for uuid in hs {
                let job = self.job_notification_guids.get_mut(uuid);
                if let Some(job) = job {
                    let fut = (job)(*job_id, *uuid, js);
                    tokio::spawn(async move {
                        fut.await;
                    });
                }
            }
        }
        Ok(())
    }
}
