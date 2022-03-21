use crate::context::Context;
use crate::job::JobToRunAsync;
use crate::{JobSchedulerError, OnJobNotification};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::RwLock;
use uuid::Uuid;

pub trait ToCode<T>
where
    T: Send,
{
    fn init(
        &mut self,
        context: &Context,
    ) -> Pin<Box<dyn Future<Output = Result<(), JobSchedulerError>> + Send>>;

    fn get(
        &mut self,
        uuid: Uuid,
    ) -> Pin<Box<dyn Future<Output = Result<Option<Arc<RwLock<T>>>, JobSchedulerError>> + Send>>;
}

pub trait JobCode: ToCode<Box<JobToRunAsync>> + Send {}

pub trait NotificationCode: ToCode<Box<OnJobNotification>> {}
