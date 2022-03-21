use crate::job_data::{JobAndNextTick, JobStoredData};
use crate::store::{DataStore, InitStore, MetaDataStorage};
use crate::JobSchedulerError;
use chrono::{DateTime, Utc};
use std::collections::HashMap;
use std::future::Future;
use std::sync::{Arc, RwLock};
use uuid::Uuid;

pub struct SimpleMetadataStore {
    pub data: Arc<RwLock<HashMap<Uuid, JobStoredData>>>,
    pub inited: bool,
}

impl DataStore<JobStoredData> for SimpleMetadataStore {
    fn get(
        &mut self,
        id: Uuid,
    ) -> Box<dyn Future<Output = Result<Option<JobStoredData>, JobSchedulerError>>> {
        let data = self.data.clone();
        Box::new(async move {
            let r = data.write();
            if let Err(e) = r {
                Err(JobSchedulerError::GetJobData)
            } else {
                let r = r.unwrap();
                let val = r.get(&id).cloned();
                Ok(val)
            }
        })
    }

    fn add_or_update(
        &mut self,
        data: JobStoredData,
    ) -> Box<dyn Future<Output = Result<(), JobSchedulerError>>> {
        let id: Uuid = data.id.as_ref().unwrap().into();
        let job_data = self.data.clone();
        Box::new(async move {
            let w = job_data.write();
            if let Err(e) = w {
                Err(JobSchedulerError::CantAdd)
            } else {
                let mut w = w.unwrap();
                w.insert(id, data);
                Ok(())
            }
        })
    }

    fn delete(&mut self, guid: Uuid) -> Box<dyn Future<Output = Result<(), JobSchedulerError>>> {
        let job_data = self.data.clone();
        Box::new(async move {
            let w = job_data.write();
            if let Err(e) = w {
                Err(JobSchedulerError::CantRemove)
            } else {
                let mut w = w.unwrap();
                w.remove(&guid);
                Ok(())
            }
        })
    }
}

impl InitStore for SimpleMetadataStore {
    fn init(&mut self) -> Box<dyn Future<Output = Result<(), JobSchedulerError>>> {
        Box::new(async move { Err(JobSchedulerError::CantInit) })
    }

    fn inited(&mut self) -> Box<dyn Future<Output = Result<bool, JobSchedulerError>>> {
        let val = self.inited;
        Box::new(std::future::ready(Ok(val)))
    }
}

impl MetaDataStorage for SimpleMetadataStore {
    fn list_next_ticks(
        &mut self,
    ) -> Box<dyn Future<Output = Result<Vec<JobAndNextTick>, JobSchedulerError>>> {
        let data = self.data.clone();
        Box::new(async move {
            let r = data.read();
            r.map(|r| {
                r.iter()
                    .map(|(_, v)| (v.id.clone(), v.next_tick))
                    .map(|(id, next_tick)| JobAndNextTick { id, next_tick })
                    .collect::<Vec<_>>()
            })
            .map_err(|e| JobSchedulerError::TickError)
        })
    }

    fn set_next_tick(
        &mut self,
        guid: Uuid,
        next_tick: DateTime<Utc>,
    ) -> Box<dyn Future<Output = Result<(), JobSchedulerError>>> {
        let data = self.data.clone();
        Box::new(async move {
            let w = data.write();
            if let Err(e) = w {
                Err(JobSchedulerError::UpdateJobData)
            } else {
                let mut w = w.unwrap();
                let val = w.get_mut(&guid);
                match val {
                    Some(mut val) => {
                        val.next_tick = next_tick.timestamp() as u64;
                        Ok(())
                    }
                    None => Err(JobSchedulerError::UpdateJobData),
                }
            }
        })
    }
}
