use chrono::{Utc, Offset};
use croner::Cron;

#[derive(Clone, PartialEq, Debug)]
pub struct CronJob {
    pub schedule: String,
}
#[derive(Clone, PartialEq, Debug)]
pub struct NonCronJob {
    pub repeating: bool,
    pub repeated_every: u64,
}
#[derive(Clone, PartialEq, Debug)]
pub struct Uuid {
    pub id1: u64,
    pub id2: u64,
}
#[derive(Clone, PartialEq, Debug)]
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
    /// Default is UTC
    pub timezone: chrono_tz::Tz,
    pub schedule: String,
}

/// Nested message and enum types in `JobStoredData`.
pub mod job_stored_data {
    #[derive(Clone, PartialEq, Debug)]
    #[repr(i32)]
    pub enum Job {
        CronJob(super::CronJob),
        NonCronJob(super::NonCronJob),
    }
}
#[derive(Clone, PartialEq, Debug)]
pub struct JobIdAndNotification {
    pub job_id: ::core::option::Option<Uuid>,
    pub notification_id: ::core::option::Option<Uuid>,
}
#[derive(Clone, PartialEq, Debug)]
pub struct NotificationData {
    pub job_id: ::core::option::Option<JobIdAndNotification>,
    pub job_states: Vec<i32>,
    pub extra: Vec<u8>,
}
#[derive(Clone, PartialEq, Debug)]
pub struct NotificationIdAndState {
    pub notification_id: ::core::option::Option<Uuid>,
    pub job_state: i32,
}
#[derive(Clone, PartialEq, Debug)]
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

    pub fn time_offset_seconds(&self) -> i32 {
        // Use the job's scheduled time (next_tick) to calculate the offset
        // This is crucial for DST handling - we need the offset for when the job will actually run
        let scheduled_time_utc = self.next_tick_utc().unwrap_or_else(Utc::now);
        
        // Convert the scheduled time to the job's timezone
        let scheduled_time_local = scheduled_time_utc.with_timezone(&self.timezone);
        
        // Calculate the total offset from UTC for that specific time
        // This will be different during DST transitions
        scheduled_time_local.offset().fix().local_minus_utc()
    }
    
    pub fn set_timezone(&mut self, timezone: chrono_tz::Tz) {
        self.timezone = timezone;
        let schedule = Cron::new(&self.schedule)
            .with_seconds_required()
            .with_dom_and_dow()
            .parse()
            .unwrap();
        
        // Get current time in the target timezone
        let now = Utc::now().with_timezone(&timezone);
        
        // Calculate next tick in the target timezone
        let next_tick = schedule
            .iter_from(now)
            .next()
            .map(|t| {
                // Convert the timezone-specific time to UTC timestamp
                // This ensures proper comparison with Utc::now() later
                t.timestamp() as u64
            })
            .unwrap_or(0);

        // Log the next tick time for debugging
        println!("Next tick for job {}", next_tick);
            
        self.next_tick = next_tick;
    }
}
