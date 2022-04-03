use crate::shared::run_example;
use std::time::Duration;
use tokio_cron_scheduler::{Job, JobScheduler};

mod shared;

#[tokio::main]
async fn main() {
    let sched = JobScheduler::new();
    let sched = sched.unwrap();
    run_example(sched).await;
}
