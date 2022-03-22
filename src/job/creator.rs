use crate::context::Context;
use crate::job::{JobLocked, JobToRunAsync};
use crate::store::MetaDataStorage;
use crate::{JobSchedulerError, JobStoredData};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::broadcast::{Receiver, Sender};
use tokio::sync::RwLock;
use uuid::Uuid;

#[derive(Default)]
pub struct JobCreator {}

impl JobCreator {
    async fn listen_to_additions(
        storage: Arc<RwLock<Box<dyn MetaDataStorage + Send + Sync>>>,
        mut rx: Receiver<(JobStoredData, Arc<RwLock<Box<JobToRunAsync>>>)>,
        mut tx_created: Sender<Result<Uuid, (JobSchedulerError, Option<Uuid>)>>,
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
                None => {
                    if let Err(e) = tx_created.send(Err((JobSchedulerError::CantAdd, None))) {
                        eprintln!("Error sending creation error {:?}", e);
                    }
                    continue;
                }
            };
            {
                let mut storage = storage.write().await;
                let saved = storage.add_or_update(data).await;
                if let Err(e) = saved {
                    eprintln!("Error saving job metadata {:?}", e);
                    if let Err(e) = tx_created.send(Err((e, Some(uuid)))) {
                        eprintln!("Could not send failure {:?}", e);
                    }
                    continue;
                }
            }
            if let Err(e) = tx_created.send(Ok(uuid)) {
                eprintln!("Error sending created job {:?}", e);
            }
        }
    }

    pub fn init(
        &self,
        context: &Context,
    ) -> Pin<Box<dyn Future<Output = Result<(), JobSchedulerError>>>> {
        let rx = context.job_create_tx.subscribe();
        let tx_created = context.job_created_tx.clone();
        let storage = context.metadata_storage.clone();

        Box::pin(async move {
            tokio::spawn(JobCreator::listen_to_additions(storage, rx, tx_created));
            Ok(())
        })
    }

    pub fn add(context: &Context, mut job: JobLocked) -> Result<Uuid, JobSchedulerError> {
        let tx = context.job_create_tx.clone();
        let mut rx = context.job_created_tx.subscribe();

        let (done_tx, done_rx) = std::sync::mpsc::channel();

        tokio::spawn(async move {
            let data = job.job_data();
            let uuid = job.guid();

            if let Err(e) = data {
                eprintln!("Error getting job data {:?}", e);
                if let Err(e) = done_tx.send(Err(e)) {
                    eprintln!("Could not notify of error {:?}", e);
                }
                return;
            }
            let data = data.unwrap();
            let job = move |job_id, job_scheduler| {
                let job = job.clone();
                Box::pin(async move {
                    let w = job.0.write();
                    if let Err(e) = w {
                        eprintln!("Error getting job {:?}", e);
                    }
                    let mut w = w.unwrap();
                    let job_done = w.run(job_scheduler);
                    let job_done = job_done.await;
                    match job_done {
                        Err(e) => {
                            eprintln!("Error running job {:?} {:?}", job_id, e);
                        }
                        Ok(val) => {
                            if !val {
                                eprintln!("Error running job {:?}", job_id);
                            }
                        }
                    }
                })
            };
            let job = Arc::new(RwLock::new(Box::new(job)));

            if let Err(e) = tx.send((data, job)) {
                eprintln!("Error sending new job");
                if let Err(e) = done_tx.send(Err(JobSchedulerError::CantAdd)) {
                    eprintln!("Error sending failure of adding job {:?}", e);
                }
                return;
            }

            while let Ok(val) = rx.recv().await {
                match val {
                    Ok(ret_uuid) => {
                        if ret_uuid == uuid {
                            if let Err(e) = done_tx.send(Ok(uuid)) {
                                eprintln!("Could not send successful addition {:?}", e);
                            }
                            break;
                        }
                    }
                    Err((e, Some(ret_uuid))) => {
                        if ret_uuid == uuid {
                            if let Err(e) = done_tx.send(Err(e)) {
                                eprintln!("Could not send failure {:?}", e);
                            }
                        }
                    }
                    _ => {}
                }
            }
        });

        let uuid = done_rx.recv().map_err(|e| {
            eprintln!("Could not receive done from add {:?}", e);
            JobSchedulerError::CantAdd
        })??;
        Ok(uuid)
    }
}
