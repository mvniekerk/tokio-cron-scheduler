use crate::job::job_data::{JobState, JobStoredData};
use crate::job::JobLocked;
use std::collections::HashMap;
use std::sync::mpsc::{channel, Receiver, Sender};
use std::sync::{Arc, RwLock};
use tokio::task::JoinHandle;

use crate::{JobSchedulerError, OnJobNotification};
use uuid::Uuid;

type LockedJobNotificationGuids = Arc<RwLock<HashMap<Uuid, Box<OnJobNotification>>>>;

pub struct SimpleJobStore {
    pub jobs: Arc<RwLock<HashMap<Uuid, JobLocked>>>,
    pub job_notification_guids: LockedJobNotificationGuids,
    pub job_handlers: HashMap<Uuid, Option<JoinHandle<()>>>,
    pub tx_add_job: Option<Sender<Message<JobLocked, ()>>>,
    pub tx_remove_job: Option<Sender<Message<Uuid, ()>>>,
    pub tx_notify_on_job_state: Option<Sender<Message<NotifyOnJobState, ()>>>,
    pub tx_add_notification: Option<Sender<Message<AddJobNotification, JobStoredData>>>,
    pub tx_remove_notification: Option<Sender<Message<RemoveJobNotification, bool>>>,
    pub inited: bool,
}

impl Default for SimpleJobStore {
    fn default() -> Self {
        SimpleJobStore {
            jobs: Arc::new(RwLock::new(HashMap::new())),
            job_notification_guids: Arc::new(RwLock::new(HashMap::new())),
            job_handlers: HashMap::new(),
            tx_add_job: None,
            tx_remove_job: None,
            tx_notify_on_job_state: None,
            tx_add_notification: None,
            tx_remove_notification: None,
            inited: false,
        }
    }
}

pub struct Message<T, RET> {
    pub data: T,
    pub resp: Sender<Result<RET, JobSchedulerError>>,
}

pub struct NotifyOnJobState {
    pub job: Uuid,
    pub state: JobState,
    pub notification_ids: Vec<Uuid>,
}

pub struct AddJobNotification {
    notification_guid: Uuid,
    on_notification: Box<OnJobNotification>,
    notifications: Vec<JobState>,
    job_data: JobStoredData,
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

async fn listen_for_notifications(
    job_notification_guids: Arc<RwLock<HashMap<Uuid, Box<OnJobNotification>>>>,
    rx_notify: Receiver<Message<NotifyOnJobState, ()>>,
) {
    'next_notification: while let Ok(Message { data, resp }) = rx_notify.recv() {
        let NotifyOnJobState {
            job: job_id,
            state: js,
            notification_ids,
        } = data;
        for uuid in notification_ids {
            let uuid: Uuid = uuid;
            let job_notification_guids = job_notification_guids.write();
            if let Err(e) = job_notification_guids {
                eprintln!("Error unlocking job notifications {:?}", e);
                if let Err(e) = resp.send(Err(JobSchedulerError::NotifyOnStateError)) {
                    eprintln!("Error sending error response {:?}", e);
                }
                continue 'next_notification;
            }
            let mut job_notification_guids = job_notification_guids.unwrap();
            let job = job_notification_guids.get_mut(&uuid);
            if let Some(job) = job {
                let fut = (job)(job_id, uuid, js);
                tokio::spawn(async move {
                    fut.await;
                });
            }
        }
        if let Err(e) = resp.send(Ok(())) {
            eprintln!("Error sending result of response {:?}", e);
        }
    }
}
