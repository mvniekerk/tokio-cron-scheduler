use crate::context::Context;
use crate::job::job_data::JobState;
use crate::job::{JobId, NotificationId};
use crate::store::NotificationStore;
use crate::JobSchedulerError;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::broadcast::{Receiver, Sender};
use tokio::sync::RwLock;
use uuid::Uuid;

#[derive(Default)]
pub struct NotificationDeleter {}

impl NotificationDeleter {
    async fn listen_to_job_removals(
        storage: Arc<RwLock<Box<dyn NotificationStore + Send + Sync>>>,
        mut rx_job_delete: Receiver<JobId>,
        mut tx_notification_deleted: Sender<(NotificationId, Option<Vec<JobState>>)>,
    ) {
        loop {
            let val = rx_job_delete.recv().await;
            if let Err(e) = val {
                eprintln!("Error receiving delete jobs {:?}", e);
                break;
            }
            let job_id = val.unwrap();
            let mut storage = storage.write().await;
            let guids = storage
                .list_notification_guids_for_job_id(job_id.clone())
                .await;
            if let Err(e) = guids {
                eprintln!("Error with getting guids for job id {:?}", e);
                continue;
            }
            let guids = guids.unwrap();
            // TODO first check for removal callback
            for notification_id in guids {
                if let Err(e) = storage.delete(notification_id.clone()).await {
                    eprintln!("Error deleting notification {:?}", e);
                    continue;
                }
                if let Err(e) = tx_notification_deleted.send((notification_id, None)) {
                    eprintln!("Error sending deletion {:?}", e);
                    continue;
                }
            }
        }
    }

    async fn listen_for_notification_removals(
        storage: Arc<RwLock<Box<dyn NotificationStore + Send + Sync>>>,
        mut rx: Receiver<(NotificationId, Option<Vec<JobState>>)>,
        mut tx_deleted: Sender<(NotificationId, Option<Vec<JobState>>)>,
    ) {
        loop {
            let val = rx.recv().await;
            if let Err(e) = val {
                eprintln!("Error receiving notification removals {:?}", e);
                break;
            }
            let (uuid, states) = val.unwrap();

            {
                let mut storage = storage.write().await;
                if let Some(states) = states {
                    for state in states {
                        let delete = storage
                            .delete_notification_for_state(uuid.clone(), state)
                            .await;
                        if let Err(e) = delete {
                            eprintln!("Error deleting notification for state {:?}", e);
                            continue;
                        }
                        if let Err(e) = tx_deleted.send((uuid, Some(vec![state]))) {
                            eprintln!("Error sending notification deleted state {:?}", e);
                        }
                    }
                } else {
                    let w = storage.delete(uuid).await;
                    if let Err(e) = w {
                        eprintln!("Error deleting notification for all states {:?}", e);
                        continue;
                    }
                    if let Err(e) = tx_deleted.send((uuid, None)) {
                        eprintln!("Error sending {:?}", e);
                    }
                }
            }
        }
    }

    pub fn init(
        &mut self,
        context: &Context,
    ) -> Pin<Box<dyn Future<Output = Result<(), JobSchedulerError>> + Send>> {
        let rx_job_delete = context.job_delete_tx.subscribe();
        let rx_notification_delete = context.notify_delete_tx.subscribe();
        let tx_notification_deleted = context.notify_deleted_tx.clone();
        let storage = context.notification_storage.clone();

        Box::pin(async move {
            tokio::spawn(NotificationDeleter::listen_to_job_removals(
                storage.clone(),
                rx_job_delete,
                tx_notification_deleted.clone(),
            ));
            tokio::spawn(NotificationDeleter::listen_for_notification_removals(
                storage,
                rx_notification_delete,
                tx_notification_deleted,
            ));
            Ok(())
        })
    }
}
