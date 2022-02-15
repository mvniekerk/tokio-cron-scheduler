use std::sync::{Arc, RwLock};
use crate::{JobSchedulerError, OnJobNotification};
use crate::job_data::Uuid;

pub trait OnJobStart {
    fn list_on_start(&mut self, job_store: JobStoreLocked ) -> Result<Vec<Box<OnJobNotification>>, JobSchedulerError>;
    fn on_start_notification_add(&mut self, on_start: Box<OnJobNotification>) -> Uuid;
    fn on_start_notification_remove(&mut self, id: Uuid) -> bool;
}

pub trait OnJobStop {
    fn list_on_stop(&mut self, job_store: JobStoreLocked ) -> Result<Vec<Box<OnJobNotification>>, JobSchedulerError>;
    fn on_stop_notification_add(&mut self, on_stop: Box<OnJobNotification>) -> Uuid;
    fn on_stop_notification_remove(&mut self, id: Uuid) -> bool;
}

pub trait OnJobRemove {
    fn list_on_remove(&mut self, job_store: JobStoreLocked ) -> Result<Vec<Box<OnJobNotification>>, JobSchedulerError>;
    fn on_remove_notification_add(&mut self, on_stop: Box<OnJobNotification>) -> Uuid;
    fn on_remove_notification_remove(&mut self, id: Uuid) -> bool;
}

pub trait JobStore {

}

pub type JobStoreLocked = Arc<RwLock<Box<dyn JobStore>>>;