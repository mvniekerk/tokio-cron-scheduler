use crate::job::JobLocked;
use crate::job_data::{JobState, JobStoredData};
use std::collections::{HashMap, HashSet};
use std::sync::mpsc::{channel, Receiver, Sender};
use std::sync::{Arc, RwLock};
use tokio::task::JoinHandle;

use crate::job_store::JobStore;
use crate::{JobSchedulerError, OnJobNotification};
use uuid::Uuid;

pub struct SimpleJobStore {
    pub jobs: Arc<RwLock<HashMap<Uuid, JobLocked>>>,
    pub notification_guids: HashMap<JobState, HashMap<Uuid, HashSet<Uuid>>>,
    pub job_notification_guids: HashMap<Uuid, Box<OnJobNotification>>,
    pub job_handlers: HashMap<Uuid, Option<JoinHandle<()>>>,
    pub tx_add_job: Option<Sender<AddJobMessage>>,
    pub tx_remove_job: Option<Sender<RemoveJobMessage>>,
}

impl Default for SimpleJobStore {
    fn default() -> Self {
        let mut ret = SimpleJobStore {
            jobs: Arc::new(RwLock::new(HashMap::new())),
            notification_guids: HashMap::new(),
            job_notification_guids: HashMap::new(),
            job_handlers: HashMap::new(),
            tx_add_job: None,
            tx_remove_job: None,
        };
        ret.init().unwrap();
        ret
    }
}

pub struct AddJobMessage {
    pub job: JobLocked,
    pub resp: Sender<Result<(), JobSchedulerError>>,
}

pub struct RemoveJobMessage {
    pub job: Uuid,
    pub resp: Sender<Result<(), JobSchedulerError>>,
}

async fn listen_for_additions(
    jobs: Arc<RwLock<HashMap<Uuid, JobLocked>>>,
    rx_add: Receiver<AddJobMessage>,
) {
    while let Ok(job) = rx_add.recv() {
        let AddJobMessage { job, resp } = job;
        let w = jobs.write();
        if w.is_err() {
            eprintln!("Could not add job");
            if let Err(e) = resp.send(Err(JobSchedulerError::CantAdd)) {
                eprintln!("Could not send error {:?}", e);
            }
            continue;
        }
        let mut w = w.unwrap();
        let id = {
            let r = job.0.read();
            if r.is_err() {
                eprintln!("Could not read id for job");
                if let Err(e) = resp.send(Err(JobSchedulerError::CantAdd)) {
                    eprintln!("Could not send error {:?}", e);
                }
                continue;
            }
            let r = r.unwrap();
            r.job_id()
        };
        w.insert(id, job);
        if let Err(e) = resp.send(Ok(())) {
            eprintln!("Could not send result {:?}", e);
        }
    }
}

async fn listen_for_removals(
    jobs: Arc<RwLock<HashMap<Uuid, JobLocked>>>,
    rx_remove: Receiver<RemoveJobMessage>,
) {
    while let Ok(job) = rx_remove.recv() {
        let RemoveJobMessage { job, resp } = job;
        let w = jobs.write();
        if w.is_err() {
            eprintln!("Could not remove job");
            if let Err(e) = resp.send(Err(JobSchedulerError::CantRemove)) {
                eprintln!("Could not send error {:?}", e);
            }
            continue;
        }
        let mut w = w.unwrap();
        w.retain(|_id, f| !{
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
    }
}

impl JobStore for SimpleJobStore {
    fn init(&mut self) -> Result<(), JobSchedulerError> {
        let (tx_add, rx_add) = channel();
        self.tx_add_job = Some(tx_add);
        let jobs = self.jobs.clone();
        println!("Init on simple job store");
        tokio::task::spawn(listen_for_additions(jobs, rx_add));
        Ok(())
    }

    fn add(&mut self, job: JobLocked) -> Result<(), JobSchedulerError> {
        match self.tx_add_job.as_ref() {
            None => Err(JobSchedulerError::CantAdd),
            Some(e) => {
                let tx_add = e.clone();
                let (resp, rx_resp) = channel();
                tx_add
                    .send(AddJobMessage { job, resp })
                    .map_err(|_| JobSchedulerError::CantAdd)?;
                match rx_resp.recv() {
                    Ok(resp) => resp,
                    Err(_) => Err(JobSchedulerError::CantAdd),
                }
            }
        }
    }

    fn remove(&mut self, to_be_removed: &Uuid) -> Result<(), JobSchedulerError> {
        let mut removed: Vec<JobLocked> = vec![];
        let j = self.jobs.clone();
        let mut w = j.write().map_err(|_| JobSchedulerError::CantRemove)?;
        w.retain(|_id, f| !{
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
        let r = self
            .jobs
            .read()
            .map_err(|_| JobSchedulerError::CantListGuids)?;
        let keys = r.keys().cloned().collect::<Vec<_>>();
        Ok(keys)
    }

    fn get_job(&mut self, job: &Uuid) -> Result<Option<JobLocked>, JobSchedulerError> {
        let job = {
            let r = self
                .jobs
                .read()
                .map_err(|_| JobSchedulerError::GetJobData)?;
            r.get(job).cloned()
        };
        Ok(job)
    }

    fn get_job_data(&mut self, job: &Uuid) -> Result<Option<JobStoredData>, JobSchedulerError> {
        let jd = {
            let r = self
                .jobs
                .read()
                .map_err(|_| JobSchedulerError::GetJobData)?;
            r.get(job).and_then(|j| {
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

    fn update_job_data(&mut self, job_data: JobStoredData) -> Result<(), JobSchedulerError> {
        let job_id: uuid::Uuid = job_data
            .id
            .as_ref()
            .cloned()
            .ok_or(JobSchedulerError::UpdateJobData)?
            .into();
        {
            let mut w = self
                .jobs
                .write()
                .map_err(|_| JobSchedulerError::UpdateJobData)?;
            let job = w.get_mut(&job_id).ok_or(JobSchedulerError::UpdateJobData)?;
            job.set_job_data(job_data)?
        }
        Ok(())
    }
}
