use tokio_cron_scheduler::{JobScheduler, JobToRun, Job};

#[tokio::main]
async fn main() {
    let mut sched = JobScheduler::new();

    sched.add(Job::new("* * * * *".to_string(), |uuid, l| {
        println!("I run every minute");
    }).unwrap());

    sched.start().await;
}
