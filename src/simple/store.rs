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
    pub notification_guids: Arc<RwLock<HashMap<JobState, HashMap<Uuid, HashSet<Uuid>>>>>,
    pub job_notification_guids: Arc<RwLock<HashMap<Uuid, Box<OnJobNotification>>>>,
    pub job_handlers: HashMap<Uuid, Option<JoinHandle<()>>>,
    pub tx_add_job: Option<Sender<AddJobMessage>>,
    pub tx_remove_job: Option<Sender<RemoveJobMessage>>,
    pub tx_notify_on_job_state: Option<Sender<NotifyOnJobState>>,
}

impl Default for SimpleJobStore {
    fn default() -> Self {
        let mut ret = SimpleJobStore {
            jobs: Arc::new(RwLock::new(HashMap::new())),
            notification_guids: Arc::new(RwLock::new(HashMap::new())),
            job_notification_guids: Arc::new(RwLock::new(HashMap::new())),
            job_handlers: HashMap::new(),
            tx_add_job: None,
            tx_remove_job: None,
            tx_notify_on_job_state: None,
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

pub struct NotifyOnJobState {
    pub job: Uuid,
    pub state: JobState,
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
    notification_guids: Arc<RwLock<HashMap<JobState, HashMap<Uuid, HashSet<Uuid>>>>>,
    job_notification_guids: Arc<RwLock<HashMap<Uuid, Box<OnJobNotification>>>>,
    rx_remove: Receiver<RemoveJobMessage>,
    tx_notify: Sender<NotifyOnJobState>,
) {
    while let Ok(RemoveJobMessage {
        job: to_be_removed,
        resp,
    }) = rx_remove.recv()
    {
        let mut removed: Vec<JobLocked> = vec![];

        {
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
                    f.job_id().eq(&to_be_removed)
                } else {
                    false
                };
                if !not_to_be_removed {
                    let f = f.0.clone();
                    removed.push(JobLocked(f))
                }
                not_to_be_removed
            });
        }

        for job in removed {
            let mut job_w = job.0.write().unwrap();
            job_w.set_stopped();
            let job_id = to_be_removed;

            let (resp, rx_resp) = channel();
            let notify = NotifyOnJobState {
                job: job_id,
                state: JobState::Removed,
                resp,
            };

            // TODO This statement could have used .flatten(), but this is unstable at the moment
            let response_from_notification = tx_notify
                .send(notify)
                .map_err(|_| JobSchedulerError::NotifyOnStateError)
                .and_then(|_| {
                    rx_resp
                        .recv()
                        .map_err(|_| JobSchedulerError::NotifyOnStateError)
                });
            match response_from_notification {
                Err(e) => eprintln!("Error notifying on job state {:?}", e),
                Ok(Err(e)) => eprintln!("Error notifying on job state {:?}", e),
                _ => {}
            }

            let job_notification_uuids = {
                let w = notification_guids
                    .write()
                    .map_err(|_| JobSchedulerError::CantRemove);
                if let Err(e) = w {
                    eprintln!("Error unlocking notification guids {:?}", e);
                    if let Err(e) = resp.send(Err(e)) {
                        eprintln!("Could not send failure of removal {:?}", e);
                    }
                    continue;
                }
                let mut w = w.unwrap();
                w.iter_mut()
                    .filter_map(|(_js, v)| v.get(&job_id))
                    .flat_map(|hs| hs.iter().cloned())
                    .collect::<Vec<_>>()
            };

            {
                let w = job_notification_guids.write();
                if let Err(e) = w {
                    eprintln!("Could not lock job notification guids {:?}", e);
                    if let Err(e) = resp.send(Err(JobSchedulerError::CantRemove)) {
                        eprintln!("Could not send result of removal error {:?}", e);
                    }
                    continue;
                }
                let mut w = w.unwrap();
                for job_notification_uuid in job_notification_uuids {
                    w.remove(&job_notification_uuid);
                }
            }
            {
                let w = notification_guids.write();
                if let Err(e) = w {
                    eprintln!("Error unlocking notification guids {:?}", e);
                    if let Err(e) = resp.send(Err(JobSchedulerError::CantRemove)) {
                        eprintln!("Could not send result of removal error {:?}", e);
                    }
                    continue;
                }
                let mut w = w.unwrap();
                for (_js, v) in w.iter_mut() {
                    v.remove(&job_id);
                }
            }
        }
    }
}

async fn listen_for_notifications(
    notification_guids: Arc<RwLock<HashMap<JobState, HashMap<Uuid, HashSet<Uuid>>>>>,
    job_notification_guids: Arc<RwLock<HashMap<Uuid, Box<OnJobNotification>>>>,
    rx_notify: Receiver<NotifyOnJobState>,
) {
    while let Ok(NotifyOnJobState { job, state, resp }) = rx_notify.recv() {
        let uuids = {
            let r = notification_guids
                .write()
                .map_err(|_| JobSchedulerError::NotifyOnStateError);
            if let Err(e) = r {
                if let Err(e) = resp.send(Err(e)) {
                    eprintln!("Error sending error response {:?}", e);
                }
                continue;
            }
            let r = r.unwrap();
            r.get(&js).and_then(|c| c.get(job_id)).cloned()
        };
        if let Some(hs) = uuids {
            let w = job_notification_guids
                .write()
                .map_err(|_| JobSchedulerError::NotifyOnStateError);
            if let Err(e) = w {
                if let Err(e) = resp.send(Err(e)) {
                    eprintln!("Error sending error response {:?}", e);
                }
                continue;
            }
            let mut w = w.unwrap();
            for uuid in hs {
                let job = w.get_mut(&uuid);
                if let Some(job) = job {
                    let fut = (job)(*job_id, *uuid, js);
                    tokio::spawn(async move {
                        fut.await;
                    });
                }
            }
        }
        if let Err(e) = resp.send(Ok(())) {
            eprintln!("Error sending result of response {:?}", e);
        }
    }
}

impl JobStore for SimpleJobStore {
    fn init(&mut self) -> Result<(), JobSchedulerError> {
        println!("Init on simple job store");
        let (tx_add, rx_add) = channel();
        self.tx_add_job = Some(tx_add);

        let (tx_remove, rx_remove) = channel();
        self.tx_remove_job = Some(tx_remove);

        let (tx_notify, rx_notify) = channel();
        self.tx_notify_on_job_state = Some(tx_notify.clone());

        let jobs = self.jobs.clone();
        tokio::task::spawn(listen_for_additions(jobs.clone(), rx_add));
        tokio::task::spawn(listen_for_removals(
            jobs.clone(),
            self.notification_guids.clone(),
            self.job_notification_guids.clone(),
            rx_remove,
            tx_notify,
        ));
        tokio::task::spawn(listen_for_notifications(
            self.notification_guids.clone(),
            self.job_notification_guids.clone(),
            rx_notify,
        ));

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
        {
            let mut w = self
                .job_notification_guids
                .write()
                .map_err(|_| JobSchedulerError::CantAdd)?;
            w.insert(*notification_guid, on_notification);
        }

        {
            let mut w = self
                .notification_guids
                .write()
                .map_err(|_| JobSchedulerError::CantAdd)?;
            for js in notifications {
                w.entry(js).or_insert_with(HashMap::new);
                let states = w.get_mut(&js).unwrap();
                states.entry(*job).or_insert_with(HashSet::new);
                let job_notifications_for_state = states.get_mut(job).unwrap();
                job_notifications_for_state.insert(*notification_guid);
            }
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
        let found_once = {
            let mut w = self
                .notification_guids
                .write()
                .map_err(|_| JobSchedulerError::CantRemove)?;
            if let Some(v) = w.get_mut(&js) {
                for (_job_id, n) in v.iter_mut() {
                    ret |= n.remove(notification_guid);
                }
            }
            w.iter_mut().any(|(_js, states)| {
                states
                    .iter_mut()
                    .any(|(_job_id, notifications)| notifications.contains(notification_guid))
            })
        };
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
        let uuids = {
            let r = self
                .notification_guids
                .write()
                .map_err(|_| JobSchedulerError::NotifyOnStateError)?;
            r.get(&js).and_then(|c| c.get(job_id)).cloned()
        };
        if let Some(hs) = uuids {
            let mut w = self
                .job_notification_guids
                .write()
                .map_err(|_| JobSchedulerError::NotifyOnStateError)?;
            for uuid in hs {
                let job = w.get_mut(&uuid);
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
