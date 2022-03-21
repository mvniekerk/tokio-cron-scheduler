use crate::job::JobToRunAsync;
use crate::job_data::{JobAndNextTick, JobStoredData};
use crate::store::{CodeGet, DataStore, InitStore};
use crate::JobSchedulerError;
use chrono::{DateTime, Utc};
use std::future::Future;
use uuid::Uuid;

pub trait MetaDataStorage: DataStore<JobStoredData> + InitStore {
    fn list_next_ticks(
        &mut self,
    ) -> Box<dyn Future<Output = Result<Vec<JobAndNextTick>, JobSchedulerError>>>;
    fn set_next_tick(
        &mut self,
        guid: Uuid,
        next_tick: DateTime<Utc>,
    ) -> Box<dyn Future<Output = Result<(), JobSchedulerError>>>;
}

pub trait JobCodeGet: CodeGet<Box<JobToRunAsync>> {}
