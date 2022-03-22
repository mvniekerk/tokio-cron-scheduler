use crate::job::job_data::{JobState, NotificationData};
use crate::job::to_code::{JobCode, NotificationCode};
use crate::job::{JobLocked, JobToRunAsync};
use crate::store::{MetaDataStorage, NotificationStore};
use crate::{JobSchedulerError, JobStoredData, OnJobNotification};
use std::sync::Arc;
use tokio::sync::broadcast::{Receiver, Sender};
use tokio::sync::RwLock;
use uuid::Uuid;

pub struct Context {
    pub job_activation_tx: Sender<Uuid>,
    pub job_activation_rx: Receiver<Uuid>,

    pub notify_tx: Sender<(Uuid, JobState)>,
    pub notify_rx: Receiver<(Uuid, JobState)>,

    pub job_create_tx: Sender<(JobStoredData, Arc<RwLock<Box<JobToRunAsync>>>)>,
    pub job_create_rx: Receiver<(JobStoredData, Arc<RwLock<Box<JobToRunAsync>>>)>,

    pub job_created_tx: Sender<Result<Uuid, (JobSchedulerError, Option<Uuid>)>>,
    pub job_created_rx: Receiver<Result<Uuid, (JobSchedulerError, Option<Uuid>)>>,

    pub job_delete_tx: Sender<Uuid>,
    pub job_delete_rx: Receiver<Uuid>,

    pub job_deleted_tx: Sender<Uuid>,
    pub job_deleted_rx: Receiver<Uuid>,

    pub notify_create_tx: Sender<(NotificationData, Arc<RwLock<Box<OnJobNotification>>>)>,
    pub notify_create_rx: Receiver<(NotificationData, Arc<RwLock<Box<OnJobNotification>>>)>,

    pub notify_created_tx: Sender<Uuid>,
    pub notify_created_rx: Receiver<Uuid>,

    pub notify_delete_tx: Sender<(Uuid, Option<Vec<JobState>>)>,
    pub notify_delete_rx: Receiver<(Uuid, Option<Vec<JobState>>)>,

    pub notify_deleted_tx: Sender<(Uuid, Option<Vec<JobState>>)>,
    pub notify_deleted_rx: Receiver<(Uuid, Option<Vec<JobState>>)>,
    // TODO need to add when notification was deleted and there's no more references to it
    pub metadata_storage: Arc<RwLock<Box<dyn MetaDataStorage + Send + Sync>>>,
    pub notification_storage: Arc<RwLock<Box<dyn NotificationStore + Send + Sync>>>,
    pub job_code: Arc<RwLock<Box<dyn JobCode + Send + Sync>>>,
    pub notification_code: Arc<RwLock<Box<dyn NotificationCode + Send + Sync>>>,
}

impl Context {
    pub fn new(
        metadata_storage: Arc<RwLock<Box<dyn MetaDataStorage + Send + Sync>>>,
        notification_storage: Arc<RwLock<Box<dyn NotificationStore + Send + Sync>>>,
        job_code: Arc<RwLock<Box<dyn JobCode + Send + Sync>>>,
        notification_code: Arc<RwLock<Box<dyn NotificationCode + Send + Sync>>>,
    ) -> Self {
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
            metadata_storage,
            notification_storage,
            job_code,
            notification_code,
        }
    }
}
