use crate::lib::run_example;
use std::time::Duration;
use tokio_cron_scheduler::{Job, JobScheduler};
use tracing::{info, Level};
use tracing_subscriber::FmtSubscriber;

mod lib;

#[tokio::main]
async fn main() {
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::TRACE)
        .finish();
    tracing::subscriber::set_global_default(subscriber).expect("Setting default subscriber failed");
    let sched = JobScheduler::new().await;
    let sched = sched.unwrap();
    run_example(sched).await;
}
