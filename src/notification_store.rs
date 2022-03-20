use crate::job_data::{JobState, NotificationData};
use crate::{JobSchedulerError, OnJobNotification};
use std::future::Future;
use std::pin::Pin;
use uuid::Uuid;

pub trait NotificationStore {
    fn list_notification_guids_for_job_and_state(
        &mut self,
        job: Uuid,
        state: JobState,
    ) -> Box<dyn Future<Output = Result<Vec<Uuid>, JobSchedulerError>>>;
    fn notification(
        &mut self,
        notification_id: Uuid,
    ) -> Box<dyn Future<Output = Result<Vec<Uuid>, JobSchedulerError>>>;
    fn add_or_update_notification(
        &mut self,
        notification_data: NotificationData,
    ) -> Box<dyn Future<Output = Result<(), JobSchedulerError>>>;
    fn delete_notification_for_state(
        &mut self,
        notification_id: Uuid,
        state: JobState,
    ) -> Box<dyn Future<Output = Result<(), JobSchedulerError>>>;
}

pub trait NotificationRunnableCodeGet {
    fn get(
        &mut self,
        notification_id: Uuid,
    ) -> Box<dyn Future<Output = Result<Box<OnJobNotification>, JobSchedulerError>>>;
    fn notify_on_add(
        &mut self,
        job_id: Uuid,
        notification_id: Uuid,
    ) -> Box<dyn Future<Output = Result<(), JobSchedulerError>>>;
}
