#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct CronJob {
    pub schedule: String,
}
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct NonCronJob {
    pub repeating: bool,
    pub repeated_every: u64,
}
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct Uuid {
    pub id1: u64,
    pub id2: u64,
}
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct JobStoredData {
    pub id: ::core::option::Option<Uuid>,
    pub last_updated: ::core::option::Option<u64>,
    pub last_tick: ::core::option::Option<u64>,
    pub next_tick: u64,
    pub job_type: i32,
    pub count: u32,
    pub extra: Vec<u8>,
    pub ran: bool,
    pub stopped: bool,
    pub job: ::core::option::Option<job_stored_data::Job>,
    pub time_offset_seconds: i32,
}

/// Nested message and enum types in `JobStoredData`.
pub mod job_stored_data {
    #[derive(Clone, PartialEq, Eq, Hash, Debug)]
    #[repr(i32)]
    pub enum Job {
        CronJob(super::CronJob),
        NonCronJob(super::NonCronJob),
    }
}
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct JobIdAndNotification {
    pub job_id: ::core::option::Option<Uuid>,
    pub notification_id: ::core::option::Option<Uuid>,
}
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct NotificationData {
    pub job_id: ::core::option::Option<JobIdAndNotification>,
    pub job_states: Vec<i32>,
    pub extra: Vec<u8>,
}
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct NotificationIdAndState {
    pub notification_id: ::core::option::Option<Uuid>,
    pub job_state: i32,
}
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct JobAndNextTick {
    pub id: ::core::option::Option<Uuid>,
    pub job_type: i32,
    pub next_tick: u64,
    pub last_tick: ::core::option::Option<u64>,
}
#[derive(Clone, PartialEq, Debug)]
pub struct ListOfUuids {
    pub uuids: Vec<Uuid>,
}
#[derive(Clone, PartialEq, Debug)]
pub struct JobAndNotifications {
    pub job_id: ::core::option::Option<Uuid>,
    pub notification_ids: Vec<Uuid>,
}
#[derive(Clone, PartialEq, Debug)]
pub struct ListOfJobsAndNotifications {
    pub job_and_notifications: Vec<JobAndNotifications>,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, FromPrimitive, ToPrimitive)]
#[repr(i32)]
pub enum JobState {
    Stop = 0,
    Scheduled = 1,
    Started = 2,
    Done = 3,
    Removed = 4,
}
impl JobState {
    /// String value of the enum field names used in the ProtoBuf definition.
    ///
    /// The values are not transformed in any way and thus are considered stable
    /// (if the ProtoBuf definition does not change) and safe for programmatic use.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            Self::Stop => "Stop",
            Self::Scheduled => "Scheduled",
            Self::Started => "Started",
            Self::Done => "Done",
            Self::Removed => "Removed",
        }
    }
    /// Creates an enum from field names used in the ProtoBuf definition.
    pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
        match value {
            "Stop" => Some(Self::Stop),
            "Scheduled" => Some(Self::Scheduled),
            "Started" => Some(Self::Started),
            "Done" => Some(Self::Done),
            "Removed" => Some(Self::Removed),
            _ => None,
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, FromPrimitive, ToPrimitive)]
#[repr(i32)]
pub enum JobType {
    Cron = 0,
    Repeated = 1,
    OneShot = 2,
}

impl JobState {
    pub fn from_i32(x: i32) -> Option<Self> {
        match x {
            0 => Some(Self::Stop),
            1 => Some(Self::Scheduled),
            2 => Some(Self::Started),
            3 => Some(Self::Done),
            4 => Some(Self::Removed),
            _ => None,
        }
    }
}

impl JobType {
    /// String value of the enum field names used in the ProtoBuf definition.
    ///
    /// The values are not transformed in any way and thus are considered stable
    /// (if the ProtoBuf definition does not change) and safe for programmatic use.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            Self::Cron => "Cron",
            Self::Repeated => "Repeated",
            Self::OneShot => "OneShot",
        }
    }
    /// Creates an enum from field names used in the ProtoBuf definition.
    pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
        match value {
            "Cron" => Some(Self::Cron),
            "Repeated" => Some(Self::Repeated),
            "OneShot" => Some(Self::OneShot),
            _ => None,
        }
    }
    pub fn from_i32(x: i32) -> Option<Self> {
        match x {
            0 => Some(Self::Cron),
            1 => Some(Self::Repeated),
            2 => Some(Self::OneShot),
            _ => None,
        }
    }
}

impl From<JobState> for i32 {
    fn from(val: JobState) -> Self {
        val as i32
    }
}

impl From<JobType> for i32 {
    fn from(val: JobType) -> Self {
        val as i32
    }
}

impl JobStoredData {
    pub fn job_type(&self) -> JobType {
        JobType::from_i32(self.job_type).unwrap()
    }
}
