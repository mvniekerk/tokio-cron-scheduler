use std::error::Error;
use std::fmt::{Display, Formatter};

#[derive(Debug, Clone)]
pub enum JobSchedulerError {
    CantRemove,
    CantAdd,
    CantInit,
    TickError,
    CantGetTimeUntil,
    Shutdown,
    ShutdownNotifier,
    AddShutdownNotifier,
    RemoveShutdownNotifier,
    FetchJob,
    SaveJob,
    StartScheduler,
    ErrorLoadingGuidList,
    ErrorLoadingJob,
    CouldNotGetTimeUntilNextTick,
    GetJobData,
    GetJobStore,
    JobTick,
    UpdateJobData,
    NoNextTick,
    CantListGuids,
    CantListNextTicks,
    NotifyOnStateError,
    ParseSchedule,
    #[cfg(feature = "nats_storage")]
    BuilderNeedsField(String),
    #[cfg(feature = "nats_storage")]
    NatsCouldNotConnect(String),
    #[cfg(feature = "nats_storage")]
    NatsCouldNotCreateKvStore(String),
}

impl Display for JobSchedulerError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self)
    }
}

impl Error for JobSchedulerError {}
