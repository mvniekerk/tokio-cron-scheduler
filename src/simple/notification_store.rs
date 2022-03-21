use crate::job::job_data::{JobIdAndNotification, JobState, NotificationData};
use crate::store::{DataStore, InitStore, NotificationStore};
use crate::JobSchedulerError;
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::{RwLock, RwLockReadGuard};
use uuid::Uuid;

pub struct SimpleNotificationStore {
    pub data: Arc<RwLock<HashMap<Uuid, HashMap<Uuid, NotificationData>>>>,
    pub notification_vs_job: Arc<RwLock<HashMap<Uuid, Uuid>>>,
    pub inited: bool,
}

impl InitStore for SimpleNotificationStore {
    fn init(&mut self) -> Box<dyn Future<Output = Result<(), JobSchedulerError>>> {
        self.inited = true;
        Box::new(std::future::ready(Ok(())))
    }

    fn inited(&mut self) -> Box<dyn Future<Output = Result<bool, JobSchedulerError>>> {
        let val = self.inited;
        Box::new(std::future::ready(Ok(val)))
    }
}

impl DataStore<NotificationData> for SimpleNotificationStore {
    fn get(
        &mut self,
        id: Uuid,
    ) -> Pin<Box<dyn Future<Output = Result<Option<NotificationData>, JobSchedulerError>> + Send>>
    {
        let data = self.data.clone();
        let job = self.notification_vs_job.clone();
        Box::pin(async move {
            let job = job.read().await;
            let job = job.get(&id);
            match job {
                Some(job) => {
                    let val = data.read().await;
                    let val = val.get(job);
                    match val {
                        Some(job) => {
                            let val = job.get(&id).cloned();
                            Ok(val)
                        }
                        None => Err(JobSchedulerError::GetJobData),
                    }
                }
                None => Err(JobSchedulerError::GetJobData),
            }
        })
    }

    fn add_or_update(
        &mut self,
        data: NotificationData,
    ) -> Pin<Box<dyn Future<Output = Result<(), JobSchedulerError>> + Send>> {
        let jobs = self.notification_vs_job.clone();
        let notifications = self.data.clone();
        Box::pin(async move {
            let id = data.job_id.as_ref();
            match id {
                Some(val) => {
                    let JobIdAndNotification {
                        job_id,
                        notification_id,
                    } = val;
                    match (job_id, notification_id) {
                        (Some(job_id), Some(notification_id)) => {
                            let job_id: Uuid = job_id.into();
                            let notification_id: Uuid = notification_id.into();

                            let mut jobs = jobs.write().await;
                            jobs.insert(notification_id.clone(), job_id.clone());

                            let mut notifications = notifications.write().await;
                            if !notifications.contains_key(&job_id) {
                                notifications.insert(job_id.clone(), HashMap::new());
                            }
                            let job = notifications.get_mut(&job_id);
                            if let Some(mut job) = job {
                                job.insert(notification_id, data);
                            }

                            Ok(())
                        }
                        _ => Err(JobSchedulerError::UpdateJobData),
                    }
                }
                None => Err(JobSchedulerError::UpdateJobData),
            }
        })
    }

    fn delete(
        &mut self,
        guid: Uuid,
    ) -> Pin<Box<dyn Future<Output = Result<(), JobSchedulerError>> + Send>> {
        let jobs = self.notification_vs_job.clone();
        let notifications = self.data.clone();
        Box::pin(async move {
            let job_id = {
                let r = jobs.read().await;
                r.get(&guid).cloned()
            };
            let mut jobs = jobs.write().await;
            match job_id {
                Some(job_id) => {
                    jobs.remove(&guid);
                    let mut notifications = notifications.write().await;
                    let job = notifications.get_mut(&job_id);
                    match job {
                        Some(job) => {
                            job.remove(&guid);
                            if job.is_empty() {
                                notifications.remove(&job_id);
                            }
                            Ok(())
                        }
                        None => Err(JobSchedulerError::CantRemove),
                    }
                }
                None => Err(JobSchedulerError::CantRemove),
            }
        })
    }
}

impl NotificationStore for SimpleNotificationStore {
    fn list_notification_guids_for_job_and_state(
        &mut self,
        job_id: Uuid,
        state: JobState,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<Uuid>, JobSchedulerError>> + Send>> {
        let state: i32 = state.into();
        let notifications = self.data.clone();
        Box::pin(async move {
            let notifications = notifications.read().await;
            let job = notifications.get(&job_id);
            match job {
                Some(job) => Ok(job
                    .iter()
                    .filter_map(|(k, v)| {
                        if v.job_states.contains(&state) {
                            Some(k.clone())
                        } else {
                            None
                        }
                    })
                    .collect::<Vec<_>>()),
                None => Ok(vec![]),
            }
        })
    }

    fn list_notification_guids_for_job_id(
        &mut self,
        job_id: Uuid,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<Uuid>, JobSchedulerError>> + Send>> {
        let notifications = self.data.clone();
        Box::pin(async move {
            let notifications = notifications.read().await;
            let job = notifications.get(&job_id);
            match job {
                Some(job) => Ok(job.iter().map(|(k, _v)| k.clone()).collect::<Vec<_>>()),
                None => Ok(vec![]),
            }
        })
    }

    fn delete_notification_for_state(
        &mut self,
        notification_id: Uuid,
        state: JobState,
    ) -> Box<dyn Future<Output = Result<(), JobSchedulerError>>> {
        let state: i32 = state.into();

        let jobs = self.notification_vs_job.clone();
        let notifications = self.data.clone();
        Box::new(async move {
            let job_id = {
                let r = jobs.read().await;
                r.get(&notification_id).cloned()
            };
            let mut jobs = jobs.write().await;
            match job_id {
                Some(job_id) => {
                    let mut notifications = notifications.write().await;
                    let job = notifications.get_mut(&job_id);
                    match job {
                        Some(mut job) => {
                            if job.contains_key(&notification_id) {
                                let mut notification = job.get_mut(&notification_id).unwrap();
                                notification.job_states.retain(|v| *v != state);
                                if notification.job_states.is_empty() {
                                    job.remove(&notification_id);
                                    jobs.remove(&notification_id);
                                }
                            }
                            if job.is_empty() {
                                notifications.remove(&job_id);
                            }
                            Ok(())
                        }
                        None => Err(JobSchedulerError::CantRemove),
                    }
                }
                None => Err(JobSchedulerError::CantRemove),
            }
        })
    }

    fn delete_for_job(
        &mut self,
        job_id: Uuid,
    ) -> Box<dyn Future<Output = Result<(), JobSchedulerError>>> {
        let jobs = self.notification_vs_job.clone();
        let notifications = self.data.clone();
        Box::new(async move {
            let mut jobs = jobs.write().await;

            jobs.retain(|k, v| *v != job_id);

            let mut notifications = notifications.write().await;
            notifications.remove(&job_id);
            Ok(())
        })
    }
}
