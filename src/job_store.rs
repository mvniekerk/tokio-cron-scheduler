use std::sync::{Arc, RwLock};
use crate::{JobSchedulerError, OnJobNotification};
use crate::job::JobLocked;
use uuid::Uuid;
use crate::simple::SimpleJobStore;

pub trait OnJobStart {
    fn list_on_start(&self, job_store: JobStoreLocked) -> Result<Vec<Box<OnJobNotification>>, JobSchedulerError>;
    fn on_start_notification_add(&mut self, on_start: Box<OnJobNotification>, job_store: JobStoreLocked) -> Uuid;
    fn on_start_notification_remove(&mut self, id: Uuid, job_store: JobStoreLocked) -> bool;
}

pub trait OnJobStop {
    fn list_on_stop(&self, job_store: JobStoreLocked) -> Result<Vec<Box<OnJobNotification>>, JobSchedulerError>;
    fn on_stop_notification_add(&mut self, on_stop: Box<OnJobNotification>, job_store: JobStoreLocked) -> Uuid;
    fn on_stop_notification_remove(&mut self, id: Uuid, job_store: JobStoreLocked) -> bool;
}

pub trait OnJobRemove {
    fn list_on_remove(&self, job_store: JobStoreLocked) -> Result<Vec<Box<OnJobNotification>>, JobSchedulerError>;
    fn on_remove_notification_add(&mut self, on_stop: Box<OnJobNotification>, job_store: JobStoreLocked) -> Uuid;
    fn on_remove_notification_remove(&mut self, id: Uuid, job_store: JobStoreLocked) -> bool;
}

pub trait JobStore {
    fn add(&mut self, job: JobLocked) -> Result<(), JobSchedulerError>;
    fn remove(&mut self, job: &Uuid) -> Result<(), JobSchedulerError>;
}

#[derive(Clone)]
pub struct JobStoreLocked(Arc<RwLock<Box<dyn JobStore>>>);

impl Default for JobStoreLocked {
    fn default() -> Self {
        JobStoreLocked(Arc::new(RwLock::new(Box::new(SimpleJobStore { jobs: vec![] }))))
    }
}

impl JobStoreLocked {
    pub fn add(&mut self, job: JobLocked) -> Result<(), JobSchedulerError> {
        {
            let mut w = self.0.write().map_err(|_| JobSchedulerError::CantAdd)?;
            w.add(job)?;
        }
        Ok(())
    }

    pub fn remove(&mut self, job: &Uuid) -> Result<(), JobSchedulerError> {
        {
            let mut w = self.0.write().map_err(|_| JobSchedulerError::CantRemove)?;
            w.remove(job)?;
        }
        Ok(())
    }
}