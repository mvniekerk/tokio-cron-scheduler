mod job_scheduler;
mod job;

pub use job_scheduler::JobsSchedulerLocked as JobScheduler;
pub use job::JobLocked as Job;

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
