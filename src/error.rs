#[derive(Debug)]
pub enum JobSchedulerError {
    CantRemove,
    CantAdd,
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
}
