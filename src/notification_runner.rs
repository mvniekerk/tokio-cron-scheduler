use crate::context::Context;
use crate::job::job_data::JobState;
use crate::job::to_code::NotificationCode;
use crate::store::NotificationStore;
use crate::JobSchedulerError;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::broadcast::{Receiver, Sender};
use tokio::sync::RwLock;
use uuid::Uuid;

pub struct NotificationRunner {
    pub notify_code: Arc<RwLock<Box<dyn NotificationCode + Send + Sync>>>,
    pub storage: Arc<RwLock<Box<dyn NotificationStore + Send + Sync>>>,
}

impl NotificationRunner {
    pub fn new(
        notify_code: Arc<RwLock<Box<dyn NotificationCode + Send + Sync>>>,
        storage: Arc<RwLock<Box<dyn NotificationStore + Send + Sync>>>,
    ) -> Self {
        Self {
            notify_code,
            storage,
        }
    }

    async fn listen_for_activations(
        code: Arc<RwLock<Box<dyn NotificationCode + Send + Sync>>>,
        mut rx: Receiver<(Uuid, JobState)>,
        storage: Arc<RwLock<Box<dyn NotificationStore + Send + Sync>>>,
    ) {
        loop {
            let val = rx.recv().await;
            if let Err(e) = val {
                eprintln!("Error receiving value {:?}", e);
                break;
            }
            let (job_id, state) = val.unwrap();
            let mut storage = storage.write().await;
            let notifications = storage
                .list_notification_guids_for_job_and_state(job_id.clone(), state)
                .await;
            if let Err(e) = notifications {
                eprintln!(
                    "Error getting the list of notifications guids for job {:?} and state {:?}",
                    job_id, state
                );
                continue;
            }
            let notifications = notifications.unwrap();
            let mut code = code.write().await;
            for notification_id in notifications {
                let code = code.get(notification_id.clone()).await;
                match code {
                    Ok(Some(code)) => {
                        let code = code.clone();
                        let job_id = job_id.clone();
                        let state = state.clone();
                        tokio::spawn(async move {
                            let mut code = code.write().await;
                            (code)(job_id, notification_id, state).await;
                        });
                    }
                    _ => {
                        eprintln!("Could not get notification code for {:?}", notification_id);
                        continue;
                    }
                }
            }
        }
    }

    pub fn init(
        &mut self,
        context: &Context,
    ) -> Pin<Box<dyn Future<Output = Result<(), JobSchedulerError>>>> {
        let code = self.notify_code.clone();
        let rx = context.notify_tx.subscribe();
        let storage = self.storage.clone();

        Box::pin(async move {
            tokio::spawn(NotificationRunner::listen_for_activations(
                code, rx, storage,
            ));
            Ok(())
        })
    }
}
