use crate::context::Context;
use crate::store::MetaDataStorage;
use chrono::Utc;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::broadcast::Sender;
use tokio::sync::RwLock;
use uuid::Uuid;

pub struct Scheduler {
    pub job_activation_tx: Sender<Uuid>,
    pub job_delete_tx: Sender<Uuid>,
    pub notify_tx: Sender<Uuid>,
    pub shutdown: Arc<RwLock<bool>>,
    pub metadata_store: Arc<RwLock<Box<dyn MetaDataStorage + Send + Sync>>>,
}

impl Scheduler {
    fn new(
        context: &Context,
        metadata_store: Arc<RwLock<Box<dyn MetaDataStorage + Send + Sync>>>,
    ) -> Self {
        let job_activation_tx = context.job_activation_tx.clone();
        let notify_tx = context.notify_tx.clone();
        let job_delete_tx = context.job_delete_tx.clone();

        Self {
            job_activation_tx,
            notify_tx,
            shutdown: Arc::new(RwLock::new(false)),
            metadata_store,
            job_delete_tx,
        }
    }

    fn start(&self) {
        let job_activation_tx = self.job_activation_tx.clone();
        let job_delete_tx = self.job_delete_tx.clone();
        let notify_tx = self.notify_tx.clone();
        let shutdown = self.shutdown.clone();
        let store = self.metadata_store.clone();

        tokio::spawn(async move {
            'next_tick: loop {
                let shutdown = {
                    let r = shutdown.read().await;
                    *r
                };
                if shutdown {
                    break 'next_tick;
                }
                tokio::time::sleep(Duration::from_millis(500)).await;
                let now = Utc::now();
                let next_clicks = {
                    let mut w = store.write().await;
                    w.list_next_ticks().await
                };
                if let Err(e) = next_clicks {
                    continue 'next_tick;
                }
                let next_clicks = next_clicks.unwrap();
                let to_be_deleted = next_clicks.iter().filter_map(|v| {
                    if v.id.is_none() {
                        return None;
                    }
                    if v.next_tick == 0 {
                        let id: Uuid = v.id.as_ref().unwrap().into();
                        Some(id)
                    } else {
                        None
                    }
                });
                for uuid in to_be_deleted {
                    let tx = job_delete_tx.clone();
                    tokio::spawn(async move {
                        if let Err(e) = tx.send(uuid) {
                            eprintln!("Error sending deletion {:?}", e);
                        }
                    });
                }

                // let next_clicks = next_clicks
                //     .iter()
                //     .filter_map(|j| j.)
            }
        });
    }

    async fn shutdown(&mut self) {
        let mut w = self.shutdown.write().await;
        *w = true;
    }
}
