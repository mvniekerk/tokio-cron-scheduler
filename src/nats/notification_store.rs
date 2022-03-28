use crate::job::job_data::{JobState, NotificationData};
use crate::nats::{sanitize_nats_key, NatsStore};
use crate::store::{DataStore, InitStore, NotificationStore};
use crate::{JobSchedulerError, JobUuid, ListOfUuids};
use nats::kv::Store;
use prost::Message;
use std::future::Future;
use std::pin::Pin;
use tokio::sync::RwLockReadGuard;
use uuid::Uuid;

const LIST_NAME: &str = "TCS_NOTIFICATION_LIST";

#[derive(Clone, Default)]
pub struct NatsNotificationStore {
    pub store: NatsStore,
}

impl DataStore<NotificationData> for NatsNotificationStore {
    fn get(
        &mut self,
        id: Uuid,
    ) -> Pin<Box<dyn Future<Output = Result<Option<NotificationData>, JobSchedulerError>> + Send>>
    {
        let bucket = self.store.bucket.clone();
        Box::pin(async move {
            let r = bucket.read().await;
            let id = sanitize_nats_key(&*id.to_string());
            r.get(&*id)
                .map_err(|e| {
                    eprintln!("Error getting data {:?}", e);
                    JobSchedulerError::GetJobData
                })
                .map(|v| v.and_then(|v| NotificationData::decode(v.as_slice()).ok()))
        })
    }

    fn add_or_update(
        &mut self,
        data: NotificationData,
    ) -> Pin<Box<dyn Future<Output = Result<(), JobSchedulerError>> + Send>> {
        let bucket = self.store.bucket.clone();
        let uuid: Uuid = data
            .job_id
            .as_ref()
            .and_then(|j| j.notification_id.as_ref())
            .unwrap()
            .into();
        let get = self.get(uuid);
        let add_to_list = self.add_to_list_of_guids(uuid);
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
            let added = add_to_list.await;
            match (done, added) {
                (Ok(_), Ok(_)) => Ok(()),
                _ => Err(JobSchedulerError::CantAdd),
            }
        })
    }

    fn delete(
        &mut self,
        guid: Uuid,
    ) -> Pin<Box<dyn Future<Output = Result<(), JobSchedulerError>> + Send>> {
        let bucket = self.store.bucket.clone();
        let removed_from_list = self.remove_from_list(guid);
        Box::pin(async move {
            let bucket = bucket.read().await;
            let guid = sanitize_nats_key(&*guid.to_string());

            let deleted = bucket.delete(&*guid);
            let removed_from_list = removed_from_list.await;

            match (deleted, removed_from_list) {
                (Ok(_), Ok(_)) => Ok(()),
                _ => Err(JobSchedulerError::CantRemove),
            }
        })
    }
}

impl InitStore for NatsNotificationStore {
    fn init(&mut self) -> Pin<Box<dyn Future<Output = Result<(), JobSchedulerError>> + Send>> {
        Box::pin(async move {
            // Nop
            // That being said. Would've been better to do the connection startup here.
            Ok(())
        })
    }

    fn inited(&mut self) -> Pin<Box<dyn Future<Output = Result<bool, JobSchedulerError>>>> {
        let inited = self.store.inited;
        Box::pin(async move { Ok(inited) })
    }
}

impl NotificationStore for NatsNotificationStore {
    fn list_notification_guids_for_job_and_state(
        &mut self,
        job: Uuid,
        state: JobState,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<Uuid>, JobSchedulerError>> + Send>> {
        todo!()
    }

    fn list_notification_guids_for_job_id(
        &mut self,
        job_id: Uuid,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<Uuid>, JobSchedulerError>> + Send>> {
        todo!()
    }

    fn delete_notification_for_state(
        &mut self,
        notification_id: Uuid,
        state: JobState,
    ) -> Pin<Box<dyn Future<Output = Result<bool, JobSchedulerError>> + Send>> {
        todo!()
    }

    fn delete_for_job(
        &mut self,
        job_id: Uuid,
    ) -> Box<dyn Future<Output = Result<(), JobSchedulerError>>> {
        todo!()
    }
}

impl NatsNotificationStore {
    fn list_guids(
        &self,
    ) -> Pin<Box<dyn Future<Output = Result<ListOfUuids, JobSchedulerError>> + Send>> {
        let bucket = self.store.bucket.clone();
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

    fn add_to_list_of_guids(
        &self,
        uuid: Uuid,
    ) -> Pin<Box<dyn Future<Output = Result<(), JobSchedulerError>> + Send>> {
        let list = self.list_guids();
        let bucket = self.store.bucket.clone();
        Box::pin(async move {
            let list = list.await;
            if let Err(e) = list {
                eprintln!("Could not get list of guids {:?}", e);
                return Err(JobSchedulerError::ErrorLoadingGuidList);
            }
            let mut list = list.unwrap();
            let exists = list.uuid_in_list(uuid);
            if exists {
                return Ok(());
            }
            let uuid: JobUuid = uuid.into();
            list.uuids.push(uuid);

            let bucket = bucket.read().await;
            NatsNotificationStore::update_list(bucket, list)
        })
    }

    fn update_list(
        bucket: RwLockReadGuard<Store>,
        list: ListOfUuids,
    ) -> Result<(), JobSchedulerError> {
        let has_list_already = bucket
            .get(&*sanitize_nats_key(LIST_NAME))
            .ok()
            .flatten()
            .is_some();
        if has_list_already {
            bucket.put(
                &*sanitize_nats_key(LIST_NAME),
                list.encode_length_delimited_to_vec(),
            )
        } else {
            bucket.create(
                &*sanitize_nats_key(LIST_NAME),
                list.encode_length_delimited_to_vec(),
            )
        }
        .map(|_| ())
        .map_err(|e| {
            eprintln!("Error saving list of guids {:?}", e);
            JobSchedulerError::CantAdd
        })
    }

    fn remove_from_list(
        &self,
        uuid: Uuid,
    ) -> Pin<Box<dyn Future<Output = Result<(), JobSchedulerError>> + Send>> {
        let list = self.list_guids();
        let bucket = self.store.bucket.clone();
        Box::pin(async move {
            let list = list.await;
            if let Err(e) = list {
                eprintln!("Could not get list of guids {:?}", e);
                return Err(JobSchedulerError::ErrorLoadingGuidList);
            }
            let mut list = list.unwrap();
            let exists = list.uuid_in_list(uuid);
            if !exists {
                return Ok(());
            }
            list.uuids.retain(|v| {
                let v: Uuid = v.into();
                v != uuid
            });
            let bucket = bucket.read().await;
            NatsNotificationStore::update_list(bucket, list)
        })
    }
}
