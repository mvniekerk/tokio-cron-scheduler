use tokio_cron_scheduler::{Job, JobScheduler};
use std::time::Duration;

#[tokio::main]
async fn main() {
    let mut sched = JobScheduler::new();

    let five_s_job = Job::new("1/5 * * * * *", |_uuid, _l| {
        println!("{:?} I run every 5 seconds", chrono::Utc::now());
    })
        .unwrap();
    let five_s_job_guid = five_s_job.guid();
    sched.add(five_s_job);

    sched.add(
        Job::new("1/30 * * * * *", |_uuid, _l| {
            println!("{:?} I run every 30 seconds", chrono::Utc::now());
        })
        .unwrap(),
    );

    sched.add(
        Job::new_one_shot(Duration::from_secs(18), |_uuid, _l| {
            println!("{:?} I'm only run once", chrono::Utc::now());
        }).unwrap()
    );

    sched.add(
        Job::new_repeated(Duration::from_secs(8), |_uuid, _l| {
            println!("{:?} I'm repeated every 8 seconds", chrono::Utc::now());
        }).unwrap()
    );

    tokio::spawn(sched.start());
    tokio::time::sleep(Duration::from_secs(30)).await;

    println!("{:?} Remove 5 sec job", chrono::Utc::now());
    sched.remove(&five_s_job_guid);

    tokio::time::sleep(Duration::from_secs(40)).await;

    println!("{:?} Goodbye.", chrono::Utc::now())
}
