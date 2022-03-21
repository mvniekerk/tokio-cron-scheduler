use crate::job::job_data::{JobState, NotificationData};
use crate::job::JobLocked;
use crate::OnJobNotification;
use std::sync::{Arc, RwLock};
use tokio::sync::broadcast::{Receiver, Sender};
use uuid::Uuid;

pub struct Context {
    pub job_activation_tx: Sender<Uuid>,
    pub job_activation_rx: Receiver<Uuid>,

    pub notify_tx: Sender<(Uuid, JobState)>,
    pub notify_rx: Receiver<(Uuid, JobState)>,

    pub job_create_tx: Sender<JobLocked>,
    pub job_create_rx: Receiver<JobLocked>,

    pub job_created_tx: Sender<Uuid>,
    pub job_created_rx: Receiver<Uuid>,

    pub job_delete_tx: Sender<Uuid>,
    pub job_delete_rx: Receiver<Uuid>,

    pub job_deleted_tx: Sender<Uuid>,
    pub job_deleted_rx: Receiver<Uuid>,

    pub notify_create_tx: Sender<(NotificationData, Box<Arc<RwLock<OnJobNotification>>>)>,
    pub notify_create_rx: Receiver<(NotificationData, Box<Arc<RwLock<OnJobNotification>>>)>,

    pub notify_created_tx: Sender<Uuid>,
    pub notify_created_rx: Receiver<Uuid>,

    pub notify_delete_tx: Sender<(Uuid, Option<Vec<JobState>>)>,
    pub notify_delete_rx: Receiver<(Uuid, Option<Vec<JobState>>)>,

    pub notify_deleted_tx: Sender<(Uuid, Option<Vec<JobState>>)>,
    pub notify_deleted_rx: Receiver<(Uuid, Option<Vec<JobState>>)>,
}

impl Default for Context {
    fn default() -> Self {
        let (job_activation_tx, job_activation_rx) = tokio::sync::broadcast::channel(200);
        let (notify_tx, notify_rx) = tokio::sync::broadcast::channel(200);
        let (job_create_tx, job_create_rx) = tokio::sync::broadcast::channel(200);
        let (job_created_tx, job_created_rx) = tokio::sync::broadcast::channel(200);
        let (job_delete_tx, job_delete_rx) = tokio::sync::broadcast::channel(200);
        let (job_deleted_tx, job_deleted_rx) = tokio::sync::broadcast::channel(200);
        let (notify_create_tx, notify_create_rx) = tokio::sync::broadcast::channel(200);
        let (notify_created_tx, notify_created_rx) = tokio::sync::broadcast::channel(200);
        let (notify_delete_tx, notify_delete_rx) = tokio::sync::broadcast::channel(200);
        let (notify_deleted_tx, notify_deleted_rx) = tokio::sync::broadcast::channel(200);

        Self {
            job_activation_tx,
            job_activation_rx,
            notify_tx,
            notify_rx,
            job_create_tx,
            job_create_rx,
            job_created_tx,
            job_created_rx,
            job_delete_tx,
            job_delete_rx,
            job_deleted_tx,
            job_deleted_rx,
            notify_create_tx,
            notify_create_rx,
            notify_created_tx,
            notify_created_rx,
            notify_delete_tx,
            notify_delete_rx,
            notify_deleted_tx,
            notify_deleted_rx,
        }
    }
}
