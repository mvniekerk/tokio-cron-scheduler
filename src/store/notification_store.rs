use crate::job_data::{JobState, NotificationData};
use crate::store::{CodeGet, DataStore, InitStore};
use crate::{JobSchedulerError, OnJobNotification};
use std::future::Future;
use uuid::Uuid;

pub trait NotificationStore: DataStore<NotificationData> + InitStore {
    fn list_notification_guids_for_job_and_state(
        &mut self,
        job: Uuid,
        state: JobState,
    ) -> Box<dyn Future<Output = Result<Vec<Uuid>, JobSchedulerError>>>;
    fn delete_notification_for_state(
        &mut self,
        notification_id: Uuid,
        state: JobState,
    ) -> Box<dyn Future<Output = Result<(), JobSchedulerError>>>;
    fn delete_for_job(
        &mut self,
        job_id: Uuid,
    ) -> Box<dyn Future<Output = Result<(), JobSchedulerError>>>;
}

pub trait NotificationRunnableCodeGet: CodeGet<Box<OnJobNotification>> {}
