use std::time::Duration;
use tokio_cron_scheduler::{Job, JobScheduler};

#[tokio::main]
async fn main() {
    let mut sched = JobScheduler::new();

    let five_s_job = Job::new("1/5 * * * * *", |_uuid, _l| {
        println!("{:?} I run every 5 seconds", chrono::Utc::now());
    })
    .unwrap();
    let five_s_job_guid = five_s_job.guid();
    sched.add(five_s_job);

    let four_s_job_async = Job::new_async("1/4 * * * * *", |_uuid, _l| {
        Box::pin(async move {
            println!("{:?} I run async every 4 seconds", chrono::Utc::now());
        })
    })
    .unwrap();
    let four_s_job_guid = four_s_job_async.guid();
    sched.add(four_s_job_async);

    sched.add(
        Job::new("1/30 * * * * *", |_uuid, _l| {
            println!("{:?} I run every 30 seconds", chrono::Utc::now());
        })
        .unwrap(),
    );

    sched.add(
        Job::new_one_shot(Duration::from_secs(18), |_uuid, _l| {
            println!("{:?} I'm only run once", chrono::Utc::now());
        })
        .unwrap(),
    );

    sched.add(
        Job::new_one_shot_async(Duration::from_secs(16), |_uuid, _l| {
            Box::pin(async move {
                println!("{:?} I'm only run once async", chrono::Utc::now());
            })
        })
        .unwrap(),
    );

    let jj = Job::new_repeated(Duration::from_secs(8), |_uuid, _l| {
        println!("{:?} I'm repeated every 8 seconds", chrono::Utc::now());
    })
    .unwrap();
    let jj_guid = jj.guid();
    sched.add(jj);

    let jja = Job::new_repeated_async(Duration::from_secs(7), |_uuid, _l| {
        Box::pin(async move {
            println!(
                "{:?} I'm repeated async every 7 seconds",
                chrono::Utc::now()
            );
        })
    })
    .unwrap();
    let jja_guid = jja.guid();
    sched.add(jja);

    tokio::spawn(sched.start());
    tokio::time::sleep(Duration::from_secs(30)).await;

    println!("{:?} Remove 4, 5, 7 and 8 sec jobs", chrono::Utc::now());
    sched.remove(&five_s_job_guid);
    sched.remove(&four_s_job_guid);
    sched.remove(&jj_guid);
    sched.remove(&jja_guid);

    tokio::time::sleep(Duration::from_secs(40)).await;

    println!("{:?} Goodbye.", chrono::Utc::now())
}
