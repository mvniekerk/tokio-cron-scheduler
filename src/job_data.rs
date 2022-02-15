#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CronJob {
    #[prost(string, tag="1")]
    pub schedule: ::prost::alloc::string::String,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct NonCronJob {
    #[prost(bool, tag="1")]
    pub repeating: bool,
    #[prost(uint64, tag="2")]
    pub repeated_every: u64,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Uuid {
    #[prost(uint64, tag="1")]
    pub id1: u64,
    #[prost(uint64, tag="2")]
    pub id2: u64,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Job {
    #[prost(message, optional, tag="1")]
    pub id: ::core::option::Option<Uuid>,
    #[prost(uint64, optional, tag="2")]
    pub last_updated: ::core::option::Option<u64>,
    #[prost(uint64, optional, tag="3")]
    pub last_tick: ::core::option::Option<u64>,
    #[prost(uint64, tag="4")]
    pub next_tick: u64,
    #[prost(enumeration="JobType", tag="5")]
    pub job_type: i32,
    #[prost(uint32, tag="8")]
    pub count: u32,
    #[prost(message, repeated, tag="9")]
    pub on_start: ::prost::alloc::vec::Vec<Uuid>,
    #[prost(message, repeated, tag="10")]
    pub on_stop: ::prost::alloc::vec::Vec<Uuid>,
    #[prost(message, repeated, tag="11")]
    pub on_remove: ::prost::alloc::vec::Vec<Uuid>,
    #[prost(oneof="job::Job", tags="6, 7")]
    pub job: ::core::option::Option<job::Job>,
}
/// Nested message and enum types in `Job`.
pub mod job {
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Job {
        #[prost(message, tag="6")]
        CronJob(super::CronJob),
        #[prost(message, tag="7")]
        NonCronJob(super::NonCronJob),
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum JobState {
    Stop = 0,
    Scheduled = 1,
    Started = 2,
    Done = 3,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum JobType {
    Cron = 0,
    Repeated = 1,
    OneShot = 2,
}
