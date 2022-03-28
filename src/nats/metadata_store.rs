use crate::job::job_data::ListOfUuids;
use crate::nats::{sanitize_nats_bucket, sanitize_nats_key};
use crate::store::{DataStore, InitStore, MetaDataStorage};
use crate::{JobAndNextTick, JobSchedulerError, JobStoredData};
use chrono::{DateTime, Utc};
use nats::jetstream::JetStream;
use nats::kv::{Config, Store};
use prost::Message;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use uuid::Uuid;

const LIST_NAME: &str = "TCS_JOB_LIST";

///
/// A Nats KV store backed metadata store
#[derive(Clone)]
pub struct NatsMetadataStore {
    pub context: Arc<RwLock<JetStream>>,
    pub inited: bool,
    pub bucket_name: String,
    pub bucket: Arc<RwLock<Store>>,
}

impl Default for NatsMetadataStore {
    fn default() -> Self {
        let nats_host =
            std::env::var("NATS_HOST").unwrap_or_else(|_| "nats://localhost".to_string());
        let nats_app = std::env::var("NATS_APP").unwrap_or_else(|_| "Unknown Nats app".to_string());
        let connection = {
            let username = std::env::var("NATS_USERNAME");
            let password = std::env::var("NATS_PASSWORD");
            match (username, password) {
                (Ok(username), Ok(password)) => {
                    let mut options =
                        nats::Options::with_user_pass(&*username, &*password).with_name(&*nats_app);
                    if std::path::Path::new("/etc/runtime-certs/").exists() {
                        options = options
                            .add_root_certificate("/etc/runtime-certs/ca.crt")
                            .client_cert("/etc/runtime-certs/tls.crt", "/etc/runtime-certs/tls.key")
                    }
                    options.connect(&*nats_host)
                }
                _ => nats::connect(&*nats_host),
            }
        }
        .unwrap();
        let bucket_name =
            std::env::var("NATS_BUCKET_NAME").unwrap_or_else(|_| "tokiocron".to_string());
        let bucket_name = sanitize_nats_bucket(&bucket_name);
        let bucket_description = std::env::var("NATS_BUCKET_DESCRIPTION")
            .unwrap_or_else(|_| "Tokio Cron Scheduler".to_string());
        let context = nats::jetstream::new(connection);
        let bucket = context
            .create_key_value(&Config {
                bucket: bucket_name.clone(),
                description: bucket_description,
                history: 1,
                ..Default::default()
            })
            .unwrap();
        let context = Arc::new(RwLock::new(context));
        let bucket = Arc::new(RwLock::new(bucket));
        Self {
            context,
            inited: true,
            bucket_name,
            bucket,
        }
    }
}

impl DataStore<JobStoredData> for NatsMetadataStore {
    fn get(
        &mut self,
        id: Uuid,
    ) -> Pin<Box<dyn Future<Output = Result<Option<JobStoredData>, JobSchedulerError>> + Send>>
    {
        let bucket = self.bucket.clone();
        Box::pin(async move {
            let r = bucket.read().await;
            let id = sanitize_nats_key(&*id.to_string());
            r.get(&*id)
                .map_err(|e| {
                    eprintln!("Error getting data {:?}", e);
                    JobSchedulerError::GetJobData
                })
                .map(|v| v.and_then(|v| JobStoredData::decode(v.as_slice()).ok()))
        })
    }

    fn add_or_update(
        &mut self,
        data: JobStoredData,
    ) -> Pin<Box<dyn Future<Output = Result<(), JobSchedulerError>> + Send>> {
        let bucket = self.bucket.clone();
        let uuid: Uuid = data.id.as_ref().unwrap().into();
        let get = self.get(uuid);
        Box::pin(async move {
            let bucket = bucket.read().await;
            let bytes = data.encode_length_delimited_to_vec();
            let prev = get.await;
            let uuid = sanitize_nats_key(&*uuid.to_string());
            let done = match prev {
                Ok(Some(_)) => bucket.put(&*uuid, bytes),
                Ok(None) => bucket.create(&*uuid, bytes),
                Err(e) => {
                    eprintln!("Error getting existing value {:?}, assuming does not exist and hope for the best", e);
                    bucket.create(&*uuid, bytes)
                }
            };
            // TODO add to list
            done.map(|_| ()).map_err(|e| {
                eprintln!("Error adding or updating value {:?}", e);
                JobSchedulerError::CantAdd
            })
        })
    }

    fn delete(
        &mut self,
        guid: Uuid,
    ) -> Pin<Box<dyn Future<Output = Result<(), JobSchedulerError>> + Send>> {
        let bucket = self.bucket.clone();
        Box::pin(async move {
            let bucket = bucket.read().await;
            let guid = sanitize_nats_key(&*guid.to_string());
            // TODO remove from list
            bucket.delete(&*guid).map(|_| ()).map_err(|e| {
                eprintln!("Error removing job data {:?}", e);
                JobSchedulerError::CantRemove
            })
        })
    }
}

impl InitStore for NatsMetadataStore {
    fn init(&mut self) -> Pin<Box<dyn Future<Output = Result<(), JobSchedulerError>> + Send>> {
        Box::pin(async move {
            // Nop
            // That being said. Would've been better to do the connection startup here.
            Ok(())
        })
    }

    fn inited(&mut self) -> Pin<Box<dyn Future<Output = Result<bool, JobSchedulerError>>>> {
        let inited = self.inited;
        Box::pin(async move { Ok(inited) })
    }
}

impl MetaDataStorage for NatsMetadataStore {
    fn list_next_ticks(
        &mut self,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<JobAndNextTick>, JobSchedulerError>> + Send>> {
        let list_guids = self.list_guids();
        let bucket = self.bucket.clone();
        Box::pin(async move {
            let list = list_guids.await;
            if let Err(e) = list {
                eprintln!("Error getting list of guids {:?}", e);
            }
            let list = list.unwrap();
            let bucket = bucket.read().await;
            let list = list
                .uuids
                .iter()
                .map(|uuid| {
                    let uuid: Uuid = uuid.into();
                    uuid
                })
                .flat_map(|uuid| bucket.get(&*sanitize_nats_key(&*uuid.to_string())))
                .flatten()
                .flat_map(|buf| JobStoredData::decode(buf.as_slice()))
                .map(|jd| JobAndNextTick {
                    id: jd.id,
                    job_type: jd.job_type,
                    next_tick: jd.next_tick,
                    last_tick: jd.last_tick,
                })
                .collect::<Vec<_>>();
            Ok(list)
        })
    }

    fn set_next_and_last_tick(
        &mut self,
        guid: Uuid,
        next_tick: Option<DateTime<Utc>>,
        last_tick: Option<DateTime<Utc>>,
    ) -> Pin<Box<dyn Future<Output = Result<(), JobSchedulerError>> + Send>> {
        let get = self.get(guid);
        let bucket = self.bucket.clone();
        Box::pin(async move {
            let get = get.await;
            match get {
                Ok(Some(mut val)) => {
                    val.next_tick = match next_tick {
                        Some(next_tick) => next_tick.timestamp(),
                        None => 0,
                    } as u64;
                    val.last_tick = last_tick.map(|lt| lt.timestamp() as u64);
                    let bytes = val.encode_length_delimited_to_vec();
                    let bucket = bucket.read().await;
                    bucket
                        .put(&*sanitize_nats_key(&*guid.to_string()), bytes)
                        .map(|_| ())
                        .map_err(|e| {
                            eprintln!("Error updating value {:?}", e);
                            JobSchedulerError::UpdateJobData
                        })
                }
                Ok(None) => {
                    eprintln!("Could not get value to update");
                    Err(JobSchedulerError::UpdateJobData)
                }
                Err(e) => {
                    eprintln!("Could not get value to update {:?}", e);
                    Err(JobSchedulerError::UpdateJobData)
                }
            }
        })
    }

    fn time_till_next_job(
        &mut self,
    ) -> Pin<Box<dyn Future<Output = Result<Option<Duration>, JobSchedulerError>> + Send>> {
        let list = self.list_guids();
        let bucket = self.bucket.clone();
        Box::pin(async move {
            let list = list.await;
            if let Err(e) = list {
                eprintln!("Could not get list of guids {:?}", e);
                return Err(JobSchedulerError::CantGetTimeUntil);
            }
            let list = list.unwrap();
            let bucket = bucket.read().await;
            let now = Utc::now();
            let now = now.timestamp() as u64;
            let ret = list
                .uuids
                .iter()
                .map(|uuid| {
                    let uuid: Uuid = uuid.into();
                    uuid
                })
                .flat_map(|uuid| bucket.get(&*sanitize_nats_key(&*uuid.to_string())))
                .flatten()
                .flat_map(|b| JobStoredData::decode(b.as_slice()))
                .filter_map(|jd| match jd.next_tick {
                    0 => None,
                    i => {
                        if i > now {
                            Some(i)
                        } else {
                            None
                        }
                    }
                })
                .min()
                .map(|t| t - now)
                .map(std::time::Duration::from_secs);
            Ok(ret)
        })
    }
}

impl NatsMetadataStore {
    fn list_guids(
        &self,
    ) -> Pin<Box<dyn Future<Output = Result<ListOfUuids, JobSchedulerError>> + Send>> {
        let bucket = self.bucket.clone();
        Box::pin(async move {
            let r = bucket.read().await;
            let list = r.get(&*sanitize_nats_key(LIST_NAME));
            match list {
                Ok(Some(list)) => {
                    let list = list.as_slice();
                    ListOfUuids::decode(list).map_err(|e| {
                        eprintln!("Error decoding list value {:?}", e);
                        JobSchedulerError::CantListGuids
                    })
                }
                Ok(None) => Ok(ListOfUuids::default()),
                Err(e) => {
                    eprintln!("Error getting list of guids {:?}", e);
                    Err(JobSchedulerError::CantListGuids)
                }
            }
        })
    }
}
