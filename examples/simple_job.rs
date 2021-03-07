use tokio_cron_scheduler::{JobScheduler, JobToRun, Job};

#[tokio::main]
async fn main() {
    let mut sched = JobScheduler::new();

    sched.add(Job::new("1/10 * * * * *", |_uuid, _l| {
        println!("I run every 10 seconds");
    }).unwrap());

    sched.add(Job::new("1/30 * * * * *", |_uuid, _l| {
        println!("I run every 30 seconds");
    }).unwrap());

    if let Err(e) = sched.start().await {
        eprintln!("Error on scheduler {:?}", e);
    }
}
