use crate::context::Context;
use crate::job::job_data::{JobState, NotificationData};
use crate::store::NotificationStore;
use crate::{JobSchedulerError, OnJobNotification};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::broadcast::{Receiver, Sender};
use tokio::sync::RwLock;
use uuid::Uuid;

#[derive(Default)]
pub struct NotificationCreator {}

impl NotificationCreator {
    async fn listen_for_additions(
        storage: Arc<RwLock<Box<dyn NotificationStore + Send + Sync>>>,
        mut rx: Receiver<(NotificationData, Arc<RwLock<Box<OnJobNotification>>>)>,
        tx_created: Sender<Result<Uuid, (JobSchedulerError, Option<Uuid>)>>,
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
            let val = storage.get(notification_id).await;
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
                if let Err(e) = tx_created.send(Err((e, Some(notification_id)))) {
                    eprintln!("Error sending adding or updating error {:?}", e);
                }
                continue;
            }

            if let Err(e) = tx_created.send(Ok(notification_id)) {
                eprintln!("Error sending {:?}", e);
            }
        }
    }

    pub fn init(
        &mut self,
        context: &Context,
    ) -> Pin<Box<dyn Future<Output = Result<(), JobSchedulerError>> + Send>> {
        let rx = context.notify_create_tx.subscribe();
        let tx_created = context.notify_created_tx.clone();
        let storage = context.notification_storage.clone();

        Box::pin(async move {
            tokio::spawn(NotificationCreator::listen_for_additions(
                storage, rx, tx_created,
            ));
            Ok(())
        })
    }

    pub fn add(
        context: &Context,
        run: Box<OnJobNotification>,
        job_states: Vec<JobState>,
        job_id: &Uuid,
    ) -> Result<Uuid, JobSchedulerError> {
        let notification_id = Uuid::new_v4();
        let data = NotificationData {
            job_id: Some(crate::job::job_data::JobIdAndNotification {
                job_id: Some(job_id.into()),
                notification_id: Some(notification_id.into()),
            }),
            job_states: job_states.iter().map(|i| *i as i32).collect::<Vec<_>>(),
            extra: vec![],
        };
        let create_tx = context.notify_create_tx.clone();
        let mut created_rx = context.notify_created_tx.subscribe();
        let (tx, rx) = std::sync::mpsc::channel();

        tokio::spawn(async move {
            tokio::spawn(async move {
                // TODO can maybe not use RwLock
                if let Err(_e) = create_tx.send((data, Arc::new(RwLock::new(run)))) {
                    eprintln!("Error sending notification data");
                }
            });

            'receiving_additions: loop {
                let created = created_rx.recv().await;
                match created {
                    Ok(e) => match e {
                        Ok(uuid) => {
                            if uuid == notification_id {
                                if let Err(e) = tx.send(Ok(uuid)) {
                                    eprintln!(
                                        "Error sending notification addition success {:?}",
                                        e
                                    );
                                }
                                break 'receiving_additions;
                            }
                        }
                        Err((e, Some(uuid))) => {
                            if uuid == notification_id {
                                if let Err(e) = tx.send(Err(e)) {
                                    eprintln!(
                                        "Error sending notification addition failure {:?}",
                                        e
                                    );
                                }
                                break 'receiving_additions;
                            }
                        }
                        _ => {}
                    },
                    Err(e) => {
                        eprintln!("Error receiving from created {:?}", e);
                    }
                }
            }
        });

        let rx = rx.recv();
        match rx {
            Ok(ret) => ret,
            Err(e) => {
                eprintln!("Error receiving status from notification addition {:?}", e);
                Err(JobSchedulerError::CantAdd)
            }
        }
    }
}
