use crate::lib::run_example;
use std::time::Duration;
use tokio_cron_scheduler::{Job, JobScheduler};

mod lib;

#[tokio::main]
async fn main() {
    let sched = JobScheduler::new();
    let sched = sched.unwrap();
    run_example(sched).await;
}
