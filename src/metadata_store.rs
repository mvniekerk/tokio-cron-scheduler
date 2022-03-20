use crate::job::JobToRunAsync;
use crate::job_data::{JobAndNextTick, JobStoredData};
use crate::JobSchedulerError;
use chrono::{DateTime, Utc};
use std::future::Future;
use std::pin::Pin;
use uuid::Uuid;

pub trait MetaDataStorage {
    fn list_next_ticks(
        &mut self,
    ) -> Box<dyn Future<Output = Result<Vec<JobAndNextTick>, JobSchedulerError>>>;
    fn metadata_for_guid(
        &mut self,
    ) -> Box<dyn Future<Output = Result<JobStoredData, JobSchedulerError>>>;
    fn add_or_update_metadata(
        &mut self,
        data: JobStoredData,
    ) -> Box<dyn Future<Output = Result<(), JobSchedulerError>>>;
    fn delete_metadata(&mut self, guid: Uuid)
        -> dyn Future<Output = Result<(), JobSchedulerError>>;
    fn set_next_tick(
        &mut self,
        guid: Uuid,
        next_tick: DateTime<Utc>,
    ) -> Box<dyn Future<Output = Result<(), JobSchedulerError>>>;
}

pub trait JobCodeGet {
    fn get(
        &mut self,
        id: Uuid,
    ) -> Box<dyn Future<Output = Result<Pin<Box<JobToRunAsync>>, JobSchedulerError>>>;
    fn notify_on_add(
        &mut self,
        id: Uuid,
    ) -> Box<dyn Future<Output = Result<(), JobSchedulerError>>>;
    fn notify_on_delete(
        &mut self,
        id: Uuid,
    ) -> Box<dyn Future<Output = Result<(), JobSchedulerError>>>;
}
