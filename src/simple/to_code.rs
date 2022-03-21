use crate::context::Context;
use crate::job::job_data::{JobIdAndNotification, JobState, NotificationData};
use crate::job::to_code::{JobCode, NotificationCode, ToCode};
use crate::job::{JobLocked, JobToRunAsync};
use crate::{JobSchedulerError, OnJobNotification};
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::broadcast::Receiver;
use tokio::sync::RwLock;
use uuid::Uuid;

pub struct SimpleJobCode {
    pub job_code: Arc<RwLock<HashMap<Uuid, Arc<RwLock<Box<JobToRunAsync>>>>>>,
}

impl Default for SimpleJobCode {
    fn default() -> Self {
        SimpleJobCode {
            job_code: Arc::new(RwLock::new(HashMap::new())),
        }
    }
}

impl SimpleJobCode {
    async fn listen_for_additions(
        data: Arc<RwLock<HashMap<Uuid, Arc<RwLock<Box<JobToRunAsync>>>>>>,
        mut rx: Receiver<(Uuid, Arc<RwLock<Box<JobToRunAsync>>>)>,
    ) {
        loop {
            let val = rx.recv().await;
            if let Err(e) = val {
                eprintln!("Error receiving {:?}", e);
                break;
            }
            let (uuid, val) = val.unwrap();
            let mut w = data.write().await;
            w.insert(uuid, val);
        }
    }

    async fn listen_for_removals(
        data: Arc<RwLock<HashMap<Uuid, Arc<RwLock<Box<JobToRunAsync>>>>>>,
        mut rx: Receiver<Uuid>,
    ) {
        loop {
            let val = rx.recv().await;
            if let Err(e) = val {
                eprintln!("Error receiving job removal {:?}", e);
                break;
            }
            let uuid = val.unwrap();
            let mut w = data.write().await;
            w.remove(&uuid);
        }
    }
}

impl ToCode<Box<JobToRunAsync>> for SimpleJobCode {
    fn init(
        &mut self,
        context: &Context,
    ) -> Pin<Box<dyn Future<Output = Result<(), JobSchedulerError>> + Send>> {
        let data = self.job_code.clone();
        let job_create = context.job_create_tx.subscribe();
        let job_deleted = context.job_deleted_tx.subscribe();

        Box::pin(async move {
            tokio::spawn(SimpleJobCode::listen_for_additions(
                data.clone(),
                job_create,
            ));
            tokio::spawn(SimpleJobCode::listen_for_removals(data, job_deleted));
            Ok(())
        })
    }

    fn get(
        &mut self,
        uuid: Uuid,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<Option<Arc<RwLock<Box<JobToRunAsync>>>>, JobSchedulerError>>
                + Send,
        >,
    > {
        let data = self.job_code.clone();
        Box::pin(async move {
            let r = data.read().await;
            Ok(r.get(&uuid).cloned())
        })
    }
}

impl JobCode for SimpleJobCode {}

pub struct SimpleNotificationCode {
    pub data: Arc<RwLock<HashMap<Uuid, Arc<RwLock<Box<OnJobNotification>>>>>>,
}

impl Default for SimpleNotificationCode {
    fn default() -> Self {
        Self {
            data: Arc::new(RwLock::new(HashMap::new())),
        }
    }
}

impl SimpleNotificationCode {
    async fn listen_for_additions(
        data: Arc<RwLock<HashMap<Uuid, Arc<RwLock<Box<OnJobNotification>>>>>>,
        mut rx: Receiver<(NotificationData, Arc<RwLock<Box<OnJobNotification>>>)>,
    ) {
        loop {
            let val = rx.recv().await;
            if let Err(e) = val {
                eprintln!("Error receiving {:?}", e);
                break;
            }
            let (uuid, val) = val.unwrap();
            let uuid: Uuid = {
                match uuid {
                    NotificationData {
                        job_id:
                            Some(JobIdAndNotification {
                                job_id: Some(job_id),
                                ..
                            }),
                        ..
                    } => job_id.into(),
                    _ => continue,
                }
            };
            let mut w = data.write().await;
            w.insert(uuid, val);
        }
    }

    // TODO check for elsewhere
    async fn listen_for_removals(
        data: Arc<RwLock<HashMap<Uuid, Arc<RwLock<Box<OnJobNotification>>>>>>,
        mut rx: Receiver<(Uuid, Option<Vec<JobState>>)>,
    ) {
        loop {
            let val = rx.recv().await;
            if let Err(e) = val {
                eprintln!("Error receiving job removal {:?}", e);
                break;
            }
            let (uuid, _) = val.unwrap();
            let mut w = data.write().await;
            w.remove(&uuid);
        }
    }
}

impl ToCode<Box<OnJobNotification>> for SimpleNotificationCode {
    fn init(
        &mut self,
        context: &Context,
    ) -> Pin<Box<dyn Future<Output = Result<(), JobSchedulerError>> + Send>> {
        let data = self.data.clone();
        let rx_create = context.notify_create_tx.subscribe();
        let rx_delete = context.notify_deleted_tx.subscribe();

        Box::pin(async move {
            tokio::spawn(SimpleNotificationCode::listen_for_additions(
                data.clone(),
                rx_create,
            ));
            tokio::spawn(SimpleNotificationCode::listen_for_removals(data, rx_delete));
            Ok(())
        })
    }

    fn get(
        &mut self,
        uuid: Uuid,
    ) -> Pin<
        Box<
            dyn Future<
                    Output = Result<Option<Arc<RwLock<Box<OnJobNotification>>>>, JobSchedulerError>,
                > + Send,
        >,
    > {
        let data = self.data.clone();
        Box::pin(async move {
            let r = data.read().await;
            Ok(r.get(&uuid).cloned())
        })
    }
}

impl NotificationCode for SimpleNotificationCode {}
