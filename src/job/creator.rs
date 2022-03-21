use crate::context::Context;
use crate::job::JobToRunAsync;
use crate::store::MetaDataStorage;
use crate::{JobSchedulerError, JobStoredData};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::broadcast::{Receiver, Sender};
use tokio::sync::RwLock;
use uuid::Uuid;

pub struct JobCreator {}

impl JobCreator {
    async fn listen_to_additions(
        storage: Arc<RwLock<Box<dyn MetaDataStorage + Send + Sync>>>,
        mut rx: Receiver<(JobStoredData, Arc<RwLock<Box<JobToRunAsync>>>)>,
        mut tx_created: Sender<Uuid>,
    ) {
        loop {
            let val = rx.recv().await;
            if let Err(e) = val {
                eprintln!("Error receiving {:?}", e);
                break;
            }
            let (data, _) = val.unwrap();
            let uuid: Uuid = match data.id.as_ref().map(|b| b.into()) {
                Some(uuid) => uuid,
                None => continue,
            };
            {
                let mut storage = storage.write().await;
                let saved = storage.add_or_update(data).await;
                if let Err(e) = saved {
                    eprintln!("Error saving job metadata {:?}", e);
                    continue;
                }
            }
            if let Err(e) = tx_created.send(uuid) {
                eprintln!("Error sending created job {:?}", e);
            }
        }
    }

    pub fn init(
        &mut self,
        storage: Arc<RwLock<Box<dyn MetaDataStorage + Send + Sync>>>,
        context: &Context,
    ) -> Pin<Box<dyn Future<Output = Result<(), JobSchedulerError>>>> {
        let rx = context.job_create_tx.subscribe();
        let tx_created = context.job_created_tx.clone();

        Box::pin(async move {
            tokio::spawn(JobCreator::listen_to_additions(storage, rx, tx_created));
            Ok(())
        })
    }
}
