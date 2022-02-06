#[derive(Debug)]
pub enum JobSchedulerError {
    CantRemove,
    CantAdd,
    TickError,
    CantGetTimeUntil,
}
