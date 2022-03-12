use crate::job::JobLocked;
use crate::job_data::{JobState, JobStoredData};
use std::collections::{HashMap, HashSet};
use std::sync::mpsc::{channel, Receiver, Sender};
use std::sync::{Arc, RwLock};
use tokio::task::JoinHandle;

use crate::job_store::JobStore;
use crate::{JobSchedulerError, OnJobNotification};
use uuid::Uuid;

type LockedNotificationGuids = Arc<RwLock<HashMap<JobState, HashMap<Uuid, HashSet<Uuid>>>>>;
type LockedJobNotificationGuids = Arc<RwLock<HashMap<Uuid, Box<OnJobNotification>>>>;

pub struct SimpleJobStore {
    pub jobs: Arc<RwLock<HashMap<Uuid, JobLocked>>>,
    pub notification_guids: LockedNotificationGuids,
    pub job_notification_guids: LockedJobNotificationGuids,
    pub job_handlers: HashMap<Uuid, Option<JoinHandle<()>>>,
    pub tx_add_job: Option<Sender<Message<JobLocked, ()>>>,
    pub tx_remove_job: Option<Sender<Message<Uuid, ()>>>,
    pub tx_notify_on_job_state: Option<Sender<Message<NotifyOnJobState, ()>>>,
    pub tx_add_notification: Option<Sender<Message<AddJobNotification, ()>>>,
    pub tx_remove_notification: Option<Sender<Message<RemoveJobNotification, bool>>>,
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
            tx_add_notification: None,
            tx_remove_notification: None,
        };
        ret.init().unwrap();
        ret
    }
}

pub struct Message<T, RET> {
    pub data: T,
    pub resp: Sender<Result<RET, JobSchedulerError>>,
}

pub struct NotifyOnJobState {
    pub job: Uuid,
    pub state: JobState,
}

pub struct AddJobNotification {
    job: Uuid,
    notification_guid: Uuid,
    on_notification: Box<OnJobNotification>,
    notifications: Vec<JobState>,
}

pub struct RemoveJobNotification {
    notification_guid: Uuid,
    // Empty array is for all the states
    states: Vec<JobState>,
}

async fn listen_for_additions(
    jobs: Arc<RwLock<HashMap<Uuid, JobLocked>>>,
    rx_add: Receiver<Message<JobLocked, ()>>,
) {
    while let Ok(job) = rx_add.recv() {
        let Message { data: job, resp } = job;
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
    notification_guids: LockedNotificationGuids,
    job_notification_guids: LockedJobNotificationGuids,
    rx_remove: Receiver<Message<Uuid, ()>>,
    tx_notify: Sender<Message<NotifyOnJobState, ()>>,
) {
    while let Ok(Message {
        data: to_be_removed,
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

            let (tx_resp_notify, rx_resp) = channel();
            let msg = Message {
                data: NotifyOnJobState {
                    job: job_id,
                    state: JobState::Removed,
                },
                resp: tx_resp_notify,
            };

            // TODO This statement could have used .flatten(), but this is unstable at the moment
            let response_from_notification = tx_notify
                .send(msg)
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
    notification_guids: LockedNotificationGuids,
    job_notification_guids: Arc<RwLock<HashMap<Uuid, Box<OnJobNotification>>>>,
    rx_notify: Receiver<Message<NotifyOnJobState, ()>>,
) {
    while let Ok(Message { data, resp }) = rx_notify.recv() {
        let NotifyOnJobState {
            job: job_id,
            state: js,
        } = data;
        let uuids = {
            let notification_guids = notification_guids
                .write()
                .map_err(|_| JobSchedulerError::NotifyOnStateError);
            if let Err(e) = notification_guids {
                if let Err(e) = resp.send(Err(e)) {
                    eprintln!("Error sending error response {:?}", e);
                }
                continue;
            }
            let notification_guids = notification_guids.unwrap();
            notification_guids
                .get(&js)
                .and_then(|c| c.get(&job_id))
                .cloned()
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
                    let fut = (job)(job_id, uuid, js);
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

async fn listen_for_notification_additions(
    notification_guids: LockedNotificationGuids,
    job_notification_guids: LockedJobNotificationGuids,
    rx_notify: Receiver<Message<AddJobNotification, ()>>,
) {
    while let Ok(Message { data, resp }) = rx_notify.recv() {
        let AddJobNotification {
            job,
            notification_guid,
            on_notification,
            notifications,
        } = data;

        {
            let w = job_notification_guids.write();
            if let Err(e) = w {
                eprintln!("Could not unlock job notification guids {:?}", e);
                if let Err(e) = resp.send(Err(JobSchedulerError::CantAdd)) {
                    eprintln!("Could not send failure notification {:?}", e);
                }
                continue;
            }
            let mut w = w.unwrap();
            w.insert(notification_guid, on_notification);
        }

        {
            let w = notification_guids.write();
            if let Err(e) = w {
                eprintln!("Could not unlock notification guids {:?}", e);
                if let Err(e) = resp.send(Err(JobSchedulerError::CantAdd)) {
                    eprintln!("Could not send failure notification {:?}", e);
                }
                continue;
            }
            let mut w = w.unwrap();
            for js in notifications {
                w.entry(js).or_insert_with(HashMap::new);
                let states = w.get_mut(&js).unwrap();
                states.entry(job).or_insert_with(HashSet::new);
                let job_notifications_for_state = states.get_mut(&job).unwrap();
                job_notifications_for_state.insert(notification_guid);
            }
        }
        if let Err(e) = resp.send(Ok(())) {
            eprintln!("Error sending success of notification addition {:?}", e);
        }
    }
}

async fn listen_for_notification_removals(
    notification_guids: LockedNotificationGuids,
    job_notification_guids: LockedJobNotificationGuids,
    rx_notify: Receiver<Message<RemoveJobNotification, bool>>,
) {
    while let Ok(Message { data, resp }) = rx_notify.recv() {
        let RemoveJobNotification {
            notification_guid,
            states,
        } = data;
        let mut ret = false;
        // Need to remove all
        if states.is_empty() {
            {
                let job_notifications = job_notification_guids.write();
                if let Err(e) = job_notifications {
                    eprintln!("Could not unlock job notification guids {:?}", e);
                    if let Err(e) = resp.send(Err(JobSchedulerError::CantRemove)) {
                        eprintln!("Could not send notification removal error {:?}", e);
                    }
                    continue;
                }
                let mut job_notifications = job_notifications.unwrap();
                job_notifications.remove(&notification_guid);
            }
            {
                let notification_guids = notification_guids.write();
                if let Err(e) = notification_guids {
                    eprintln!("Could not unlock notification guids {:?}", e);
                    if let Err(e) = resp.send(Err(JobSchedulerError::CantRemove)) {
                        eprintln!("Could not send notification removal error {:?}", e);
                    }
                    continue;
                }
                let mut notification_guids = notification_guids.unwrap();
                for (_js, states) in notification_guids.iter_mut() {
                    for (_job_id, notifications) in states.iter_mut() {
                        ret |= notifications.remove(&notification_guid);
                    }
                }
            }
        } else {
            let notification_guids = notification_guids.write();
            let job_notification_guids = job_notification_guids.write();

            let (mut notification_guids, mut job_notification_guids) =
                match (notification_guids, job_notification_guids) {
                    (Ok(notification_guids), Ok(job_notification_guids)) => {
                        (notification_guids, job_notification_guids)
                    }
                    (_, _) => {
                        eprintln!("Error unlocking notification or job notification guids");
                        if let Err(e) = resp.send(Err(JobSchedulerError::CantRemove)) {
                            eprintln!("Could not send unlock error {:?}", e);
                        }
                        continue;
                    }
                };
            for js in states {
                let found_once = {
                    if let Some(v) = notification_guids.get_mut(&js) {
                        for (_job_id, n) in v.iter_mut() {
                            ret |= n.remove(&notification_guid);
                        }
                    }
                    notification_guids.iter_mut().any(|(_js, states)| {
                        states.iter_mut().any(|(_job_id, notifications)| {
                            notifications.contains(&notification_guid)
                        })
                    })
                };
                if !found_once {
                    job_notification_guids.remove(&notification_guid);
                }
            }
        }
        if let Err(e) = resp.send(Ok(ret)) {
            eprintln!("Error sending success of removal {:?}", e);
        }
    }
}

fn send_to_tx_channel<T, RET>(
    tx: Option<&Sender<Message<T, RET>>>,
    data: T,
    error: JobSchedulerError,
) -> Result<RET, JobSchedulerError> {
    let tx = tx.ok_or(error)?;
    let (resp, rx_result) = channel();
    let msg = Message { data, resp };
    tx.send(msg).map_err(|_| error)?;
    let msg = rx_result.recv();
    match msg {
        Ok(Ok(ret)) => Ok(ret),
        Ok(Err(e)) => Err(e),
        Err(_) => Err(error),
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

        let (tx_add_notify, rx_add_notify) = channel();
        self.tx_add_notification = Some(tx_add_notify);

        let (tx_remove_notify, rx_remove_notify) = channel();
        self.tx_remove_notification = Some(tx_remove_notify);

        let jobs = self.jobs.clone();
        tokio::task::spawn(listen_for_additions(jobs.clone(), rx_add));
        tokio::task::spawn(listen_for_removals(
            jobs,
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
        tokio::task::spawn(listen_for_notification_additions(
            self.notification_guids.clone(),
            self.job_notification_guids.clone(),
            rx_add_notify,
        ));
        tokio::task::spawn(listen_for_notification_removals(
            self.notification_guids.clone(),
            self.job_notification_guids.clone(),
            rx_remove_notify,
        ));

        Ok(())
    }

    fn add(&mut self, job: JobLocked) -> Result<(), JobSchedulerError> {
        send_to_tx_channel(self.tx_add_job.as_ref(), job, JobSchedulerError::CantAdd)
    }

    fn remove(&mut self, to_be_removed: &Uuid) -> Result<(), JobSchedulerError> {
        send_to_tx_channel(
            self.tx_remove_job.as_ref(),
            *to_be_removed,
            JobSchedulerError::CantRemove,
        )
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
        send_to_tx_channel(
            self.tx_add_notification.as_ref(),
            AddJobNotification {
                job: *job,
                notification_guid: *notification_guid,
                on_notification,
                notifications,
            },
            JobSchedulerError::CantAdd,
        )
    }

    fn remove_notification(&mut self, notification_guid: &Uuid) -> Result<(), JobSchedulerError> {
        send_to_tx_channel(
            self.tx_remove_notification.as_ref(),
            RemoveJobNotification {
                notification_guid: *notification_guid,
                states: vec![],
            },
            JobSchedulerError::CantRemove,
        )
        .map(|_| ())
    }

    fn remove_notification_for_job_state(
        &mut self,
        notification_guid: &Uuid,
        js: JobState,
    ) -> Result<bool, JobSchedulerError> {
        send_to_tx_channel(
            self.tx_remove_notification.as_ref(),
            RemoveJobNotification {
                notification_guid: *notification_guid,
                states: vec![js],
            },
            JobSchedulerError::CantRemove,
        )
    }

    fn notify_on_job_state(
        &mut self,
        job_id: &Uuid,
        js: JobState,
    ) -> Result<(), JobSchedulerError> {
        send_to_tx_channel(
            self.tx_notify_on_job_state.as_ref(),
            NotifyOnJobState {
                state: js,
                job: *job_id,
            },
            JobSchedulerError::NotifyOnStateError,
        )
        .map(|_| ())
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
