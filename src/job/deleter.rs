use crate::context::Context;
use crate::store::MetaDataStorage;
use crate::JobSchedulerError;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::broadcast::{Receiver, Sender};
use tokio::sync::RwLock;
use uuid::Uuid;

#[derive(Default)]
pub struct JobDeleter {}

impl JobDeleter {
    async fn listen_to_removals(
        storage: Arc<RwLock<Box<dyn MetaDataStorage + Send + Sync>>>,
        mut rx: Receiver<Uuid>,
        mut tx_deleted: Sender<Uuid>,
    ) {
        loop {
            let val = rx.recv().await;
            if let Err(e) = val {
                eprintln!("Error receiving value {:?}", e);
                break;
            }
            let uuid = val.unwrap();
            {
                let mut storage = storage.write().await;
                let delete = storage.delete(uuid.clone()).await;
                if let Err(e) = delete {
                    eprintln!("Error deleting {:?}", e);
                    continue;
                }
            }
            if let Err(e) = tx_deleted.send(uuid) {
                eprintln!("Error sending error {:?}", e);
            }
        }
    }

    pub fn init(
        &mut self,
        context: &Context,
    ) -> Pin<Box<dyn Future<Output = Result<(), JobSchedulerError>> + Send + Sync>> {
        let rx = context.job_delete_tx.subscribe();
        let tx_deleted = context.job_deleted_tx.clone();
        let storage = context.metadata_storage.clone();

        Box::pin(async move {
            tokio::spawn(JobDeleter::listen_to_removals(storage, rx, tx_deleted));
            Ok(())
        })
    }
}
