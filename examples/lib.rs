use anyhow::Result;
use std::time::Duration;
use tokio_cron_scheduler::{Job, JobScheduler};
use tracing::{error, info};

pub async fn run_example(mut sched: JobScheduler) -> Result<()> {
    #[cfg(feature = "signal")]
    sched.shutdown_on_ctrl_c();

    sched.set_shutdown_handler(Box::new(|| {
        Box::pin(async move {
            info!("Shut down done");
        })
    }));

    let mut five_s_job = Job::new("1/5 * * * * *", |uuid, _l| {
        info!(
            "{:?} I run every 5 seconds id {:?}",
            chrono::Utc::now(),
            uuid
        );
    })
    .unwrap();

    // Adding a job notification without it being added to the scheduler will automatically add it to
    // the job store, but with stopped marking
    five_s_job.on_removed_notification_add(
        &sched,
        Box::new(|job_id, notification_id, type_of_notification| {
            Box::pin(async move {
                info!(
                    "5s Job {:?} was removed, notification {:?} ran ({:?})",
                    job_id, notification_id, type_of_notification
                );
            })
        }),
    )?;
    let five_s_job_guid = five_s_job.guid();
    sched.add(five_s_job)?;

    let mut four_s_job_async = Job::new_async("1/4 * * * * *", |uuid, _l| {
        Box::pin(async move {
            info!("I run async every 4 seconds id {:?}", uuid);
        })
    })
    .unwrap();
    let four_s_job_async_clone = four_s_job_async.clone();
    let js = sched.clone();
    info!("4s job id {:?}", four_s_job_async.guid());
    four_s_job_async.on_start_notification_add(&sched, Box::new(move |job_id, notification_id, type_of_notification| {
        let four_s_job_async_clone = four_s_job_async_clone.clone();
        let js = js.clone();
        Box::pin(async move {
            info!("4s Job {:?} ran on start notification {:?} ({:?})", job_id, notification_id, type_of_notification);
            info!("This should only run once since we're going to remove this notification immediately.");
            info!("Removed? {:?}", four_s_job_async_clone.on_start_notification_remove(&js, &notification_id));
        })
    }))?;

    four_s_job_async.on_done_notification_add(
        &sched,
        Box::new(|job_id, notification_id, type_of_notification| {
            Box::pin(async move {
                info!(
                    "4s Job {:?} completed and ran notification {:?} ({:?})",
                    job_id, notification_id, type_of_notification
                );
            })
        }),
    )?;

    let four_s_job_guid = four_s_job_async.guid();
    sched.add(four_s_job_async)?;

    sched.add(
        Job::new("1/30 * * * * *", |uuid, _l| {
            info!("I run every 30 seconds id {:?}", uuid);
        })
        .unwrap(),
    )?;

    info!(
        "Sched one shot for {:?}",
        chrono::Utc::now()
            .checked_add_signed(time::Duration::seconds(10))
            .unwrap()
    );
    sched.add(
        Job::new_one_shot(Duration::from_secs(10), |_uuid, _l| {
            info!("I'm only run once");
        })
        .unwrap(),
    )?;

    info!(
        "Sched one shot async for {:?}",
        chrono::Utc::now()
            .checked_add_signed(time::Duration::seconds(16))
            .unwrap()
    );
    sched.add(
        Job::new_one_shot_async(Duration::from_secs(16), |_uuid, _l| {
            Box::pin(async move {
                info!("I'm only run once async");
            })
        })
        .unwrap(),
    )?;

    let jj = Job::new_repeated(Duration::from_secs(8), |_uuid, _l| {
        info!("I'm repeated every 8 seconds");
    })
    .unwrap();
    let jj_guid = jj.guid();
    sched.add(jj)?;

    let jja = Job::new_repeated_async(Duration::from_secs(7), |_uuid, _l| {
        Box::pin(async move {
            info!("I'm repeated async every 7 seconds");
        })
    })
    .unwrap();
    let jja_guid = jja.guid();
    sched.add(jja)?;

    let start = sched.start();
    if start.is_err() {
        error!("Error starting scheduler");
        return Ok(());
    }
    tokio::time::sleep(Duration::from_secs(20)).await;

    info!("Remove 4, 5, 7 and 8 sec jobs");
    sched.remove(&five_s_job_guid)?;
    sched.remove(&four_s_job_guid)?;
    sched.remove(&jj_guid)?;
    sched.remove(&jja_guid)?;

    tokio::time::sleep(Duration::from_secs(40)).await;

    info!("Goodbye.");
    sched.shutdown().await?;
    Ok(())
}

fn main() {
    eprintln!("Should not be run on its own.");
}
