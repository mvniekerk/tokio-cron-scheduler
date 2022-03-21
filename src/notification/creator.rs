use crate::context::Context;
use crate::job::job_data::NotificationData;
use crate::store::NotificationStore;
use crate::{JobSchedulerError, OnJobNotification};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::broadcast::Receiver;
use tokio::sync::RwLock;
use uuid::Uuid;

#[derive(Default)]
pub struct NotificationCreator {}

impl NotificationCreator {
    async fn listen_for_additions(
        storage: Arc<RwLock<Box<dyn NotificationStore + Send + Sync>>>,
        mut rx: Receiver<(NotificationData, Arc<RwLock<Box<OnJobNotification>>>)>,
    ) {
        loop {
            let val = rx.recv().await;
            if let Err(e) = val {
                eprintln!("Error receiving {:?}", e);
                break;
            }
            let (data, _) = val.unwrap();
            if data.job_id.is_none() {
                eprintln!("Empty job id {:?}", data);
                continue;
            }
            let notification_id = data
                .job_id
                .as_ref()
                .and_then(|j| j.notification_id.as_ref());
            if notification_id.is_none() {
                eprintln!("Empty job id or notification id {:?}", data);
                continue;
            }
            let notification_id: Uuid = notification_id.unwrap().into();

            let mut storage = storage.write().await;
            let val = storage.get(notification_id.clone()).await;
            let val = match val {
                Ok(Some(mut val)) => {
                    for state in data.job_states {
                        if !val.job_states.contains(&state) {
                            val.job_states.push(state);
                        }
                    }
                    val
                }
                _ => data,
            };

            let val = storage.add_or_update(val).await;
            if let Err(e) = val {
                eprintln!("Error adding or updating {:?}", e);
            }
        }
    }

    pub fn init(
        &mut self,
        storage: Arc<RwLock<Box<dyn NotificationStore + Send + Sync>>>,
        context: &Context,
    ) -> Pin<Box<dyn Future<Output = Result<(), JobSchedulerError>>>> {
        let rx = context.notify_create_tx.subscribe();
        Box::pin(async move {
            tokio::spawn(NotificationCreator::listen_for_additions(storage, rx));
            Ok(())
        })
    }
}
