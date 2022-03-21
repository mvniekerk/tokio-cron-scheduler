use crate::job::JobLocked;
use crate::job_data::{JobState, JobStoredData};
use std::collections::HashMap;
use std::sync::mpsc::{channel, Receiver, Sender};
use std::sync::{Arc, RwLock};
use tokio::task::JoinHandle;

use crate::job_store::JobStore;
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
        let mut ret = SimpleJobStore {
            jobs: Arc::new(RwLock::new(HashMap::new())),
            job_notification_guids: Arc::new(RwLock::new(HashMap::new())),
            job_handlers: HashMap::new(),
            tx_add_job: None,
            tx_remove_job: None,
            tx_notify_on_job_state: None,
            tx_add_notification: None,
            tx_remove_notification: None,
            inited: false,
        };
        if let Err(e) = ret.init() {
            eprintln!("Error initing store {:?}", e);
        }
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

async fn listen_for_removals(
    jobs: Arc<RwLock<HashMap<Uuid, JobLocked>>>,
    rx_remove: Receiver<Message<Uuid, ()>>,
    tx_notify: Sender<Message<NotifyOnJobState, ()>>,
    tx_remove_notify: Sender<Message<RemoveJobNotification, bool>>,
) {
    // 'next_message: while let Ok(Message {
    //     data: to_be_removed,
    //     resp,
    // }) = rx_remove.recv()
    // {
    //     let job_data = get_job_data(jobs.clone(), &to_be_removed, resp.clone());
    //     let job_data = match job_data {
    //         Ok(job_data) => job_data,
    //         _ => {
    //             eprintln!("Could not fetch job data for {:?}", to_be_removed);
    //             continue 'next_message;
    //         }
    //     };
    //
    //     let mut removed: Vec<JobLocked> = vec![];
    //
    //     // Notify on removal before notifications are removed
    //     let response_from_notification = send_to_tx_channel(
    //         Some(tx_notify.clone()).as_ref(),
    //         NotifyOnJobState {
    //             state: JobState::Removed,
    //             job: to_be_removed,
    //             notification_ids: job_data
    //                 .on_removed
    //                 .iter()
    //                 .map(|i| i.into())
    //                 .collect::<Vec<_>>(),
    //         },
    //         JobSchedulerError::NotifyOnStateError,
    //     );
    //
    //     if let Err(e) = response_from_notification {
    //         eprintln!("Error notifying job state {:?}", e);
    //     }
    //
    //     // Remove notifications
    //     let js_and_state = vec![
    //         (JobState::Removed, job_data.on_removed),
    //         (JobState::Done, job_data.on_done),
    //         (JobState::Scheduled, job_data.on_scheduled),
    //         (JobState::Started, job_data.on_started),
    //         (JobState::Stop, job_data.on_stop),
    //     ];
    //     let js_and_state = js_and_state
    //         .iter()
    //         .flat_map(|(js, l)| l.iter().map(move |u| (js, u)));
    //
    //     'notification_removal: for (js, notification_guid) in js_and_state {
    //         let notification_guid: Uuid = notification_guid.into();
    //         let ret = send_to_tx_channel(
    //             Some(tx_remove_notify.clone()).as_ref(),
    //             RemoveJobNotification {
    //                 states: vec![*js],
    //                 notification_guid,
    //             },
    //             JobSchedulerError::CantRemove,
    //         );
    //         if let Err(e) = ret {
    //             eprintln!(
    //                 "Could not remove notification {:?} for {:?}, continuing {:?}",
    //                 notification_guid, to_be_removed, e
    //             );
    //             continue 'notification_removal;
    //         }
    //     }
    //
    //     // Remove job from storage
    //     {
    //         let w = jobs.write();
    //         if let Err(e) = w {
    //             eprintln!("Could not remove job {:?}", e);
    //             if let Err(e) = resp.send(Err(JobSchedulerError::CantRemove)) {
    //                 eprintln!("Could not send error {:?}", e);
    //             }
    //             continue;
    //         }
    //         let mut w = w.unwrap();
    //         w.retain(|_id, f| {
    //             let job_id = f.guid();
    //             let the_same = job_id.eq(&to_be_removed);
    //             if the_same {
    //                 let f = f.0.clone();
    //                 removed.push(JobLocked(f))
    //             }
    //             !the_same
    //         });
    //     }
    //
    //     for mut job in removed {
    //         if let Err(e) = job.set_stop(true) {
    //             eprintln!("Error setting job to be stopped {:?}, continuing!!", e);
    //         }
    //     }
    //     if let Err(e) = resp.send(Ok(())) {
    //         eprintln!("Error sending success response {:?}", e);
    //     }
    // }
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

fn get_job_data(
    jobs: Arc<RwLock<HashMap<Uuid, JobLocked>>>,
    job: &Uuid,
    resp: Sender<Result<(), JobSchedulerError>>,
) -> Result<JobStoredData, JobSchedulerError> {
    let jobs = jobs.write();
    let send_failure = || {
        if let Err(e) = resp.send(Err(JobSchedulerError::FetchJob)) {
            eprintln!("Could not return error on lock {:?}", e);
        }
        Err(JobSchedulerError::FetchJob)
    };
    if let Err(e) = jobs {
        eprintln!("Could not get lock on jobs hash {:?}", e);
        return send_failure();
    }
    let mut jobs = jobs.unwrap();
    let job_locked = jobs.get_mut(job);
    if job_locked.is_none() {
        eprintln!("Job is not found {:?}", job);
        return send_failure();
    }
    let job_locked = job_locked.unwrap();
    let job_locked = job_locked.0.write();
    if let Err(e) = job_locked {
        eprintln!("Could not get write lock on data {:?}", e);
        return send_failure();
    }
    let mut job_locked = job_locked.unwrap();
    let job_data = job_locked.job_data_from_job();
    match job_data {
        Ok(Some(job_data)) => Ok(job_data),
        _ => {
            eprintln!("Could not get job data");
            send_failure()
        }
    }
}

fn add_notification_to_job_data(
    mut job_data: JobStoredData,
    notification_id: &Uuid,
    jobs_states: Vec<JobState>,
) -> Result<JobStoredData, JobSchedulerError> {
    // let notification_uuid: crate::job_data::Uuid = notification_id.into();
    // for js in jobs_states {
    //     let mut list = match js {
    //         JobState::Stop => job_data.on_stop.clone(),
    //         JobState::Scheduled => job_data.on_scheduled.clone(),
    //         JobState::Started => job_data.on_started.clone(),
    //         JobState::Done => job_data.on_done.clone(),
    //         JobState::Removed => job_data.on_removed.clone(),
    //     };
    //
    //     if list.contains(&notification_uuid) {
    //         eprintln!(
    //             "Already has {:?} notification for {:?}",
    //             js, notification_id
    //         );
    //     } else {
    //         list.push(notification_uuid.clone());
    //     }
    //     match js {
    //         JobState::Stop => {
    //             job_data.on_stop = list;
    //         }
    //         JobState::Scheduled => {
    //             job_data.on_scheduled = list;
    //         }
    //         JobState::Started => {
    //             job_data.on_started = list;
    //         }
    //         JobState::Done => {
    //             job_data.on_done = list;
    //         }
    //         JobState::Removed => job_data.on_removed = list,
    //     }
    // }
    // Ok(job_data)
    Err(JobSchedulerError::FetchJob)
}

async fn listen_for_notification_additions(
    job_notification_guids: LockedJobNotificationGuids,
    rx_notify: Receiver<Message<AddJobNotification, JobStoredData>>,
) {
    'next_message: while let Ok(Message { data, resp }) = rx_notify.recv() {
        let AddJobNotification {
            notification_guid,
            on_notification,
            notifications,
            job_data,
        } = data;

        let send_failure = || {
            if let Err(e) = resp.send(Err(JobSchedulerError::CantAdd)) {
                eprintln!("Could not send failure notification {:?}", e);
            }
        };

        {
            let w = job_notification_guids.write();
            if let Err(e) = w {
                eprintln!("Could not unlock job notification guids {:?}", e);
                send_failure();
                continue 'next_message;
            }
            let mut w = w.unwrap();
            w.insert(notification_guid, on_notification);
        }

        match add_notification_to_job_data(job_data, &notification_guid, notifications) {
            Ok(job_data) => {
                if let Err(e) = resp.send(Ok(job_data)) {
                    eprintln!("Could not send success of notification addition {:?}", e);
                }
            }
            Err(e) => {
                eprintln!("Could not add job notification {:?}", e);
            }
        }
    }
}

async fn listen_for_notification_removals(
    jobs: Arc<RwLock<HashMap<Uuid, JobLocked>>>,
    job_notification_guids: LockedJobNotificationGuids,
    rx_notify: Receiver<Message<RemoveJobNotification, bool>>,
) {
    // while let Ok(Message { data, resp }) = rx_notify.recv() {
    //     let RemoveJobNotification {
    //         notification_guid,
    //         states,
    //     } = data;
    //     let mut ret = false;
    //     let send_failure = || {
    //         if let Err(e) = resp.send(Err(JobSchedulerError::CantRemove)) {
    //             eprintln!("Could not send notification on removal error {:?}", e);
    //         }
    //     };
    //
    //     let notification_id: crate::job_data::Uuid = notification_guid.into();
    //
    //     // Need to remove all
    //     if states.is_empty() {
    //         {
    //             let job_notifications = job_notification_guids.write();
    //             if let Err(e) = job_notifications {
    //                 eprintln!("Could not unlock job notification guids {:?}", e);
    //                 send_failure();
    //                 continue;
    //             }
    //             let mut job_notifications = job_notifications.unwrap();
    //             job_notifications.remove(&notification_guid);
    //         }
    //         {
    //             let jobs = jobs.write();
    //             if let Err(e) = jobs {
    //                 eprintln!("Error unlocking jobs {:?}", e);
    //                 send_failure();
    //                 continue;
    //             }
    //             let mut jobs = jobs.unwrap();
    //             'next_job_to_search: for (job_id, job) in jobs.iter_mut() {
    //                 let job_w = job.0.write();
    //                 if let Err(e) = job_w {
    //                     eprintln!("Error unlocking job {:?} {:?}, continuing... !!", job_id, e);
    //                     continue 'next_job_to_search;
    //                 }
    //                 let mut job_w = job_w.unwrap();
    //                 let mut job_data = match job_w.job_data_from_job() {
    //                     Ok(Some(job_data)) => job_data,
    //                     _ => {
    //                         eprintln!("Could not get job data from job {:?}", job_id);
    //                         continue 'next_job_to_search;
    //                     }
    //                 };
    //
    //                 job_data.on_done.retain(|u| {
    //                     let must_retain = !u.eq(&notification_id);
    //                     ret |= !must_retain;
    //                     must_retain
    //                 });
    //                 job_data.on_removed.retain(|u| {
    //                     let must_retain = !u.eq(&notification_id);
    //                     ret |= !must_retain;
    //                     must_retain
    //                 });
    //                 job_data.on_started.retain(|u| {
    //                     let must_retain = !u.eq(&notification_id);
    //                     ret |= !must_retain;
    //                     must_retain
    //                 });
    //                 job_data.on_scheduled.retain(|u| {
    //                     let must_retain = !u.eq(&notification_id);
    //                     ret |= !must_retain;
    //                     must_retain
    //                 });
    //                 job_data.on_stop.retain(|u| {
    //                     let must_retain = !u.eq(&notification_id);
    //                     ret |= !must_retain;
    //                     must_retain
    //                 });
    //                 if let Err(e) = job_w.set_job_data(job_data) {
    //                     eprintln!("Could not set job data of {:?} {:?}", job_id, e);
    //                 }
    //             }
    //         }
    //     } else {
    //         let job_notification_guids = job_notification_guids.write();
    //         let mut found_once = false;
    //         {
    //             let jobs = jobs.write();
    //             if let Err(e) = jobs {
    //                 eprintln!("Could not lock jobs {:?}", e);
    //                 send_failure();
    //                 continue;
    //             }
    //             let mut jobs = jobs.unwrap();
    //             'next_job: for (job_id, job_locked) in jobs.iter_mut() {
    //                 let job_data = job_locked.job_data();
    //                 if let Err(e) = job_data {
    //                     eprintln!("Could not get job data {:?}", e);
    //                     continue 'next_job;
    //                 }
    //                 let mut job_data = job_data.unwrap();
    //
    //                 let mut lists = vec![
    //                     (JobState::Removed, &mut job_data.on_removed),
    //                     (JobState::Stop, &mut job_data.on_stop),
    //                     (JobState::Started, &mut job_data.on_started),
    //                     (JobState::Done, &mut job_data.on_done),
    //                     (JobState::Scheduled, &mut job_data.on_scheduled),
    //                 ];
    //
    //                 for (js, list) in lists.iter_mut() {
    //                     if states.contains(js) {
    //                         list.retain(|u| {
    //                             let must_retain = !u.eq(&notification_id);
    //                             ret |= !must_retain;
    //                             must_retain
    //                         });
    //                     } else {
    //                         found_once |= list.contains(&notification_id);
    //                     }
    //                 }
    //                 if let Err(e) = job_locked.set_job_data(job_data) {
    //                     eprintln!("Could not set job data for {:?} {:?}", job_id, e)
    //                 }
    //             }
    //         }
    //         if !found_once {
    //             if let Err(e) = job_notification_guids {
    //                 eprintln!("Could not get lock on job notification guids {:?}", e);
    //                 continue;
    //             }
    //             let mut job_notification_guids = job_notification_guids.unwrap();
    //             job_notification_guids.remove(&notification_guid);
    //         }
    //     }
    //     if let Err(e) = resp.send(Ok(ret)) {
    //         eprintln!("Error sending success of removal {:?}", e);
    //     }
    // }
}

fn send_to_tx_channel<T, RET>(
    tx: Option<&Sender<Message<T, RET>>>,
    data: T,
    error: JobSchedulerError,
) -> Result<RET, JobSchedulerError> {
    // if tx.is_none() {
    //     eprintln!("Sender is not set {:?}", error);
    //     return Err(error);
    // }
    // let tx = tx.unwrap();
    // let (resp, rx_result) = channel();
    // let msg = Message { data, resp };
    // tx.send(msg).map_err(|e| {
    //     eprintln!("Error sending to channel {:?}", e);
    //     error
    // })?;
    // let msg = rx_result.recv();
    // match msg {
    //     Ok(Ok(ret)) => Ok(ret),
    //     Ok(Err(e)) => Err(e),
    //     Err(_) => Err(error),
    // }
    Err(JobSchedulerError::NotifyOnStateError)
}

impl JobStore for SimpleJobStore {
    fn init(&mut self) -> Result<(), JobSchedulerError> {
        // let (tx_add, rx_add) = channel();
        // self.tx_add_job = Some(tx_add);
        //
        // let (tx_remove, rx_remove) = channel();
        // self.tx_remove_job = Some(tx_remove);
        //
        // let (tx_notify, rx_notify) = channel();
        // self.tx_notify_on_job_state = Some(tx_notify.clone());
        //
        // let (tx_add_notify, rx_add_notify) = channel();
        // self.tx_add_notification = Some(tx_add_notify);
        //
        // let (tx_remove_notify, rx_remove_notify) = channel();
        // self.tx_remove_notification = Some(tx_remove_notify.clone());
        //
        // let jobs = self.jobs.clone();
        // tokio::task::spawn(listen_for_additions(jobs.clone(), rx_add));
        // tokio::task::spawn(listen_for_removals(
        //     jobs,
        //     rx_remove,
        //     tx_notify,
        //     tx_remove_notify,
        // ));
        // tokio::task::spawn(listen_for_notifications(
        //     self.job_notification_guids.clone(),
        //     rx_notify,
        // ));
        // tokio::task::spawn(listen_for_notification_additions(
        //     self.job_notification_guids.clone(),
        //     rx_add_notify,
        // ));
        // tokio::task::spawn(listen_for_notification_removals(
        //     self.jobs.clone(),
        //     self.job_notification_guids.clone(),
        //     rx_remove_notify,
        // ));

        self.inited = true;

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
        self.get_job_data(job)
            .and_then(|j| j.ok_or(JobSchedulerError::FetchJob))
            .and_then(|job_data| {
                send_to_tx_channel(
                    self.tx_add_notification.as_ref(),
                    AddJobNotification {
                        notification_guid: *notification_guid,
                        on_notification,
                        notifications,
                        job_data,
                    },
                    JobSchedulerError::CantAdd,
                )
            })
            .and_then(|job_data| self.update_job_data(job_data))
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
        notification_ids: Vec<Uuid>,
    ) -> Result<(), JobSchedulerError> {
        send_to_tx_channel(
            self.tx_notify_on_job_state.as_ref(),
            NotifyOnJobState {
                state: js,
                job: *job_id,
                notification_ids,
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

    fn has_job(&mut self, job_id: &Uuid) -> Result<bool, JobSchedulerError> {
        let r = self.jobs.read();
        if let Err(e) = r {
            eprintln!("Could not get read lock on jobs {:?} {:?}", job_id, e);
            return Err(JobSchedulerError::FetchJob);
        }
        let r = r.unwrap();
        Ok(r.contains_key(job_id))
    }

    fn inited(&mut self) -> Result<bool, JobSchedulerError> {
        Ok(self.inited)
    }
}
