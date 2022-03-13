use crate::job::JobLocked;
use crate::job_data::{JobState, JobStoredData};
use crate::simple::SimpleJobStore;
use crate::{JobSchedulerError, OnJobNotification};
use std::sync::{Arc, RwLock};
use uuid::Uuid;

pub trait OnJobStart {
    fn list_on_start(
        &self,
        job_store: JobStoreLocked,
    ) -> Result<Vec<Box<OnJobNotification>>, JobSchedulerError>;
    fn on_start_notification_add(
        &mut self,
        on_start: Box<OnJobNotification>,
        job_store: JobStoreLocked,
    ) -> Uuid;
    fn on_start_notification_remove(&mut self, id: Uuid, job_store: JobStoreLocked) -> bool;
}

pub trait OnJobStop {
    fn list_on_stop(
        &self,
        job_store: JobStoreLocked,
    ) -> Result<Vec<Box<OnJobNotification>>, JobSchedulerError>;
    fn on_stop_notification_add(
        &mut self,
        on_stop: Box<OnJobNotification>,
        job_store: JobStoreLocked,
    ) -> Uuid;
    fn on_stop_notification_remove(&mut self, id: Uuid, job_store: JobStoreLocked) -> bool;
}

pub trait OnJobRemove {
    fn list_on_remove(
        &self,
        job_store: JobStoreLocked,
    ) -> Result<Vec<Box<OnJobNotification>>, JobSchedulerError>;
    fn on_remove_notification_add(
        &mut self,
        on_stop: Box<OnJobNotification>,
        job_store: JobStoreLocked,
    ) -> Uuid;
    fn on_remove_notification_remove(&mut self, id: Uuid, job_store: JobStoreLocked) -> bool;
}

pub trait JobStore {
    fn init(&mut self) -> Result<(), JobSchedulerError>;
    fn add(&mut self, job: JobLocked) -> Result<(), JobSchedulerError>;
    fn remove(&mut self, job: &Uuid) -> Result<(), JobSchedulerError>;
    fn list_job_guids(&mut self) -> Result<Vec<Uuid>, JobSchedulerError>;
    fn get_job(&mut self, job: &Uuid) -> Result<Option<JobLocked>, JobSchedulerError>;
    fn get_job_data(&mut self, job: &Uuid) -> Result<Option<JobStoredData>, JobSchedulerError>;
    fn add_notification(
        &mut self,
        job: &Uuid,
        notification_guid: &Uuid,
        on_notification: Box<OnJobNotification>,
        notifications: Vec<JobState>,
    ) -> Result<(), JobSchedulerError>;
    fn remove_notification(&mut self, notification_guid: &Uuid) -> Result<(), JobSchedulerError>;
    fn remove_notification_for_job_state(
        &mut self,
        notification_guid: &Uuid,
        js: JobState,
    ) -> Result<bool, JobSchedulerError>;
    fn notify_on_job_state(
        &mut self,
        job_id: &Uuid,
        js: JobState,
        notification_ids: Vec<Uuid>,
    ) -> Result<(), JobSchedulerError>;
    fn update_job_data(&mut self, job_data: JobStoredData) -> Result<(), JobSchedulerError>;
    fn has_job(&mut self, job_id: &Uuid) -> Result<bool, JobSchedulerError>;
    fn inited(&mut self) -> Result<bool, JobSchedulerError>;
}

#[derive(Clone)]
pub struct JobStoreLocked(Arc<RwLock<Box<dyn JobStore>>>);

unsafe impl Send for JobStoreLocked {}
unsafe impl Sync for JobStoreLocked {}

impl Default for JobStoreLocked {
    fn default() -> Self {
        JobStoreLocked(Arc::new(RwLock::new(Box::new(SimpleJobStore {
            ..Default::default()
        }))))
    }
}

impl JobStoreLocked {
    pub fn init(&mut self) -> Result<(), JobSchedulerError> {
        {
            let mut w = self.0.write().map_err(|_| JobSchedulerError::CantInit)?;
            w.init()?;
        }
        Ok(())
    }

    pub fn inited(&mut self) -> Result<bool, JobSchedulerError> {
        let mut w = self.0.write().map_err(|_| JobSchedulerError::CantInit)?;
        w.inited()
    }

    pub fn add(&mut self, mut job: JobLocked) -> Result<(), JobSchedulerError> {
        {
            let mut w = self.0.write().map_err(|_| JobSchedulerError::CantAdd)?;
            job.set_stop(false)?;
            w.add(job)?;
        }
        Ok(())
    }

    pub fn add_no_start(&mut self, job: JobLocked) -> Result<(), JobSchedulerError> {
        {
            let mut w = self.0.write().map_err(|_| JobSchedulerError::CantAdd)?;
            w.add(job)?;
        }
        Ok(())
    }

    pub fn remove(&mut self, job: &Uuid) -> Result<(), JobSchedulerError> {
        {
            let w = self.0.write();
            if let Err(e) = w {
                eprintln!("Error locking job store for removal of {:?} {:?}", job, e);
                return Err(JobSchedulerError::CantRemove);
            }
            let mut w = w.unwrap();
            w.remove(job)?;
        }
        Ok(())
    }

    pub fn list_job_guids(&mut self) -> Result<Vec<Uuid>, JobSchedulerError> {
        let guids = {
            let mut w = self
                .0
                .write()
                .map_err(|_| JobSchedulerError::ErrorLoadingGuidList)?;
            w.list_job_guids()?
        };
        Ok(guids)
    }

    pub fn get_job(&mut self, guid: &Uuid) -> Result<Option<JobLocked>, JobSchedulerError> {
        let job = {
            let mut w = self
                .0
                .write()
                .map_err(|_| JobSchedulerError::ErrorLoadingJob)?;
            w.get_job(guid)?
        };
        Ok(job)
    }

    pub fn get_job_data(
        &mut self,
        guid: &Uuid,
    ) -> Result<Option<JobStoredData>, JobSchedulerError> {
        let job = {
            let mut w = self.0.write().map_err(|_| JobSchedulerError::GetJobData)?;
            w.get_job_data(guid)?
        };
        Ok(job)
    }

    pub fn add_notification(
        &mut self,
        job: &Uuid,
        notification_guid: &Uuid,
        on_notification: Box<OnJobNotification>,
        notifications: Vec<JobState>,
    ) -> Result<(), JobSchedulerError> {
        {
            let mut w = self.0.write().map_err(|_| JobSchedulerError::CantAdd)?;
            w.add_notification(job, notification_guid, on_notification, notifications)?;
        }
        Ok(())
    }

    pub fn remove_notification_for_job_state(
        &mut self,
        notification_guid: &Uuid,
        js: JobState,
    ) -> Result<bool, JobSchedulerError> {
        let ret = {
            let mut w = self.0.write().map_err(|_| JobSchedulerError::CantRemove)?;
            w.remove_notification_for_job_state(notification_guid, js)?
        };
        Ok(ret)
    }

    pub fn notify_on_job_state(
        &mut self,
        job_id: &Uuid,
        js: JobState,
        notification_ids: Vec<Uuid>,
    ) -> Result<(), JobSchedulerError> {
        let mut w = self.0.write().map_err(|_| JobSchedulerError::GetJobStore)?;
        w.notify_on_job_state(job_id, js, notification_ids)?;
        Ok(())
    }

    pub fn update_job_data(&mut self, job_data: JobStoredData) -> Result<(), JobSchedulerError> {
        let mut w = self
            .0
            .write()
            .map_err(|_| JobSchedulerError::UpdateJobData)?;
        w.update_job_data(job_data)?;
        Ok(())
    }

    pub fn has_job(&mut self, job_id: &Uuid) -> Result<bool, JobSchedulerError> {
        let mut w = self.0.write().map_err(|_| JobSchedulerError::FetchJob)?;
        w.has_job(job_id)
    }
}
