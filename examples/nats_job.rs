use crate::shared::run_example;
use std::time::Duration;
use tokio_cron_scheduler::{
    Job, JobScheduler, NatsMetadataStore, NatsNotificationStore, SimpleJobCode,
    SimpleNotificationCode,
};

mod shared;

#[tokio::main]
async fn main() {
    let metadata_storage = Box::new(NatsMetadataStore::default());
    let notification_storage = Box::new(NatsNotificationStore::default());

    let simple_job_code = Box::new(SimpleJobCode::default());
    let simple_notification_code = Box::new(SimpleNotificationCode::default());

    let sched = JobScheduler::new_with_storage_and_code(
        metadata_storage,
        notification_storage,
        simple_job_code,
        simple_notification_code,
    )
    .unwrap();

    run_example(sched).await;
}
