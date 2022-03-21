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

    pub fn init(
        &mut self,
        context: &Context,
        storage: Arc<RwLock<Box<dyn NotificationStore + Send + Sync>>>,
    ) -> Pin<Box<dyn Future<Output = Result<(), JobSchedulerError>>>> {
        let rx_job_delete = context.job_delete_tx.subscribe();
        let tx_notification_delete = context.notify_deleted_tx.clone();

        Box::pin(async move {
            tokio::spawn(NotificationDeleter::listen_to_job_removals(
                storage,
                rx_job_delete,
                tx_notification_delete,
            ));
            Ok(())
        })
    }
}
