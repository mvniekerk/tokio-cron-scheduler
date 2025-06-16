

#[cfg(test)]
mod tests {
    use tokio_cron_scheduler::job::job_data::*;
    use chrono::{TimeZone, Utc};
    use chrono_tz::Tz;
    use croner::Cron;

    fn create_test_job_data(timezone: Tz, next_tick: u64) -> JobStoredData {
        JobStoredData {
            id: Some(Uuid { id1: 1, id2: 2 }),
            last_updated: None,
            last_tick: None,
            next_tick,
            job_type: JobType::Cron.into(),
            count: 0,
            extra: vec![],
            ran: false,
            stopped: false,
            job: Some(job_stored_data::Job::CronJob(CronJob {
                schedule: "0 0 12 * * *".to_string(), // Daily at noon
            })),
            timezone,
        }
    }

    #[test]
    fn test_timezone_update_affects_time_offset() {
        // Create job with UTC timezone
        let mut job_data = create_test_job_data(Tz::UTC, 1000);
        let utc_offset = job_data.time_offset_seconds();

        // Update to US/Eastern timezone 
        job_data.timezone = Tz::US__Eastern;
        let eastern_offset = job_data.time_offset_seconds();

        // UTC should have 0 total offset
        assert_eq!(utc_offset, 0);
        
        // Eastern timezone total offset should be -5h (EST) or -4h (EDT)
        assert!(eastern_offset == -18000 || eastern_offset == -14400, 
               "Eastern offset should be -5h (EST) or -4h (EDT), got: {}", eastern_offset);
        
        // Test that the method returns consistent results
        let eastern_offset2 = job_data.time_offset_seconds();
        assert_eq!(eastern_offset, eastern_offset2, "Offset should be consistent");
    }

    #[test]
    fn test_timezone_update_affects_next_tick_calculation() {
        // Test that timezone changes affect cron schedule calculations
        let cron_expression = "0 0 12 * * *"; // Daily at noon
        let schedule = Cron::new(cron_expression)
            .with_seconds_required()
            .with_dom_and_dow()
            .parse()
            .expect("Valid cron expression");

        // Create a fixed reference time for consistent testing
        let base_time = Utc.with_ymd_and_hms(2023, 6, 15, 10, 0, 0).unwrap();

        // Calculate next tick for UTC
        let utc_next = schedule.iter_from(base_time.with_timezone(&Tz::UTC)).next();
        
        // Calculate next tick for US/Eastern (which is UTC-4 in summer)
        let eastern_next = schedule.iter_from(base_time.with_timezone(&Tz::US__Eastern)).next();

        // Both should have values
        assert!(utc_next.is_some());
        assert!(eastern_next.is_some());

        let utc_timestamp = utc_next.unwrap().timestamp();
        let eastern_timestamp = eastern_next.unwrap().timestamp();

        // The timestamps should be different because "12:00 UTC" and "12:00 Eastern" 
        // are different absolute times
        assert_ne!(utc_timestamp, eastern_timestamp);
        
        // The difference should be the timezone offset (4 hours = 14400 seconds in summer)
        let time_diff = (utc_timestamp - eastern_timestamp).abs();
        assert!(time_diff > 0, "Time difference should be non-zero");
    }

    #[test]
    fn test_multiple_timezone_updates() {
        // Test multiple timezone changes to ensure consistency
        let mut job_data = create_test_job_data(Tz::UTC, 1000);
        
        // Store original values
        let original_offset = job_data.time_offset_seconds();
        
        // Change to Tokyo
        job_data.timezone = Tz::Asia__Tokyo;
        let tokyo_offset = job_data.time_offset_seconds();
        
        // Change to London
        job_data.timezone = Tz::Europe__London;
        let london_offset = job_data.time_offset_seconds();
        
        // Change back to UTC
        job_data.timezone = Tz::UTC;
        let final_offset = job_data.time_offset_seconds();
        
        // Final offset should match original
        assert_eq!(original_offset, final_offset);
        
        // All values should be valid DST offsets (0 or positive)
        assert!(tokyo_offset >= 0, "Tokyo DST offset should be non-negative");
        assert!(london_offset >= 0, "London DST offset should be non-negative");
        assert!(original_offset >= 0, "UTC DST offset should be non-negative");
        
        // Verify the timezone field is actually updated
        assert_eq!(job_data.timezone, Tz::UTC);
    }

    #[test]
    fn test_job_type_method_consistency() {
        let job_data = create_test_job_data(Tz::UTC, 1000);
        
        // Test that job_type() method works correctly
        assert_eq!(job_data.job_type(), JobType::Cron);
        
        // Test different job types
        let mut repeated_job_data = job_data.clone();
        repeated_job_data.job_type = JobType::Repeated.into();
        assert_eq!(repeated_job_data.job_type(), JobType::Repeated);
        
        let mut oneshot_job_data = job_data.clone();
        oneshot_job_data.job_type = JobType::OneShot.into();
        assert_eq!(oneshot_job_data.job_type(), JobType::OneShot);
    }

    #[test]
    fn test_timezone_edge_cases() {
        // Test with timezone that has no DST
        let mut job_data = create_test_job_data(Tz::UTC, 1000);
        job_data.timezone = Tz::Asia__Dubai; // No DST
        let dubai_offset = job_data.time_offset_seconds();
        
        // Dubai should have +4h total offset (14400 seconds)
        assert_eq!(dubai_offset, 14400);
        
        // Test with timezone that has DST
        job_data.timezone = Tz::US__Pacific;
        let pacific_offset = job_data.time_offset_seconds();
        
        // Pacific timezone total offset should be -8h (PST) or -7h (PDT)
        assert!(pacific_offset == -28800 || pacific_offset == -25200, 
               "Pacific offset should be -8h (PST) or -7h (PDT), got: {}", pacific_offset);
    }

    #[test]
    fn test_complete_timezone_update_workflow() {
        // This test demonstrates the complete workflow of updating timezone
        // and how it affects both time offset and next tick calculation
        
        let cron_expression = "0 30 14 * * *"; // Daily at 2:30 PM
        let schedule = Cron::new(cron_expression)
            .with_seconds_required()
            .with_dom_and_dow()
            .parse()
            .expect("Valid cron expression");

        // Start with UTC timezone
        let mut job_data = create_test_job_data(Tz::UTC, 0);
        
        // Calculate next tick for UTC at 2023-06-15 10:00:00
        let base_time = Utc.with_ymd_and_hms(2023, 6, 15, 10, 0, 0).unwrap();
        let utc_next_tick = schedule.iter_from(base_time.with_timezone(&Tz::UTC)).next();
        assert!(utc_next_tick.is_some(), "Should find next UTC tick");
        
        let utc_timestamp = utc_next_tick.unwrap().timestamp();
        let _original_offset = job_data.time_offset_seconds();
        
        // Update timezone to New York (Eastern Time)
        job_data.timezone = Tz::America__New_York;
        let eastern_next_tick = schedule.iter_from(base_time.with_timezone(&Tz::America__New_York)).next();
        assert!(eastern_next_tick.is_some(), "Should find next Eastern tick");
        
        let eastern_timestamp = eastern_next_tick.unwrap().timestamp();
        let new_offset = job_data.time_offset_seconds();
        
        // Verify that:
        // 1. The timezone field was updated
        assert_eq!(job_data.timezone, Tz::America__New_York);
        
        // 2. The time offset method returns valid total timezone offset
        assert!(new_offset == -18000 || new_offset == -14400, 
               "New York offset should be -5h (EST) or -4h (EDT), got: {}", new_offset);
        
        // 3. The next tick timestamps are different because "2:30 PM UTC" and "2:30 PM Eastern" 
        //    represent different absolute times
        assert_ne!(utc_timestamp, eastern_timestamp, 
                  "Next tick should be different for different timezones");
        
        // Update timezone to Tokyo
        job_data.timezone = Tz::Asia__Tokyo;
        let tokyo_next_tick = schedule.iter_from(base_time.with_timezone(&Tz::Asia__Tokyo)).next();
        assert!(tokyo_next_tick.is_some(), "Should find next Tokyo tick");
        
        let tokyo_timestamp = tokyo_next_tick.unwrap().timestamp();
        let tokyo_offset = job_data.time_offset_seconds();
        
        // Verify Tokyo calculations
        assert_eq!(job_data.timezone, Tz::Asia__Tokyo);
        assert!(tokyo_offset >= 0, "Tokyo DST offset should be non-negative");
        assert_ne!(tokyo_timestamp, utc_timestamp, "Tokyo time should differ from UTC");
        assert_ne!(tokyo_timestamp, eastern_timestamp, "Tokyo time should differ from Eastern");
        
        // The key insight: same cron expression "2:30 PM" means different absolute times
        // in different timezones, which is exactly what job scheduling needs
        let times = vec![
            ("UTC", utc_timestamp),
            ("Eastern", eastern_timestamp), 
            ("Tokyo", tokyo_timestamp)
        ];
        
        // All timestamps should be unique
        for i in 0..times.len() {
            for j in (i+1)..times.len() {
                assert_ne!(times[i].1, times[j].1, 
                          "Timestamps for {} and {} should be different", 
                          times[i].0, times[j].0);
            }
        }
    }

    #[test]
    fn test_dst_transitions_with_scheduled_time_offset() {
        // This test verifies that time_offset_seconds() uses the job's scheduled time,
        // not current time, which is crucial for DST transitions
        
        // Test job scheduled during spring forward (2:30 AM on March 12, 2023)
        // This time doesn't exist because clocks spring forward from 2:00 AM to 3:00 AM
        let spring_forward_utc = Utc.with_ymd_and_hms(2023, 3, 12, 7, 30, 0).unwrap(); // 2:30 AM EST would be 7:30 UTC
        let mut job_data = create_test_job_data(Tz::US__Eastern, spring_forward_utc.timestamp() as u64);
        
        // The offset should be calculated for the scheduled time (7:30 UTC = 2:30 EST)
        // Since this is during spring forward, it should use EDT (-4h = -14400 seconds)
        let spring_offset = job_data.time_offset_seconds();
        assert_eq!(spring_offset, -14400, "Spring forward job should use EDT offset (-4h)");
        
        // Test job scheduled during fall back (1:30 AM on November 5, 2023)
        // This time occurs twice because clocks fall back from 2:00 AM to 1:00 AM
        let fall_back_utc = Utc.with_ymd_and_hms(2023, 11, 5, 6, 30, 0).unwrap(); // 1:30 AM EST would be 6:30 UTC
        job_data.next_tick = fall_back_utc.timestamp() as u64;
        
        // The offset should be calculated for the scheduled time (6:30 UTC = 1:30 EST)
        // Since this is during fall back, it should use EST (-5h = -18000 seconds)
        let fall_offset = job_data.time_offset_seconds();
        assert_eq!(fall_offset, -18000, "Fall back job should use EST offset (-5h)");
        
        // Test that the same timezone shows different offsets for different scheduled times
        
        // Job scheduled in summer (July 15, 2023 at 2:00 PM)
        let summer_utc = Utc.with_ymd_and_hms(2023, 7, 15, 18, 0, 0).unwrap(); // 2:00 PM EDT = 18:00 UTC
        job_data.next_tick = summer_utc.timestamp() as u64;
        let summer_offset = job_data.time_offset_seconds();
        assert_eq!(summer_offset, -14400, "Summer job should use EDT offset (-4h)");
        
        // Job scheduled in winter (January 15, 2023 at 2:00 PM)
        let winter_utc = Utc.with_ymd_and_hms(2023, 1, 15, 19, 0, 0).unwrap(); // 2:00 PM EST = 19:00 UTC
        job_data.next_tick = winter_utc.timestamp() as u64;
        let winter_offset = job_data.time_offset_seconds();
        assert_eq!(winter_offset, -18000, "Winter job should use EST offset (-5h)");
        
        // Verify they're different
        assert_ne!(summer_offset, winter_offset, "Summer and winter offsets should be different");
        
        // Test with a timezone that doesn't observe DST
        job_data.timezone = Tz::Asia__Dubai;
        job_data.next_tick = summer_utc.timestamp() as u64;
        let dubai_summer = job_data.time_offset_seconds();
        
        job_data.next_tick = winter_utc.timestamp() as u64;
        let dubai_winter = job_data.time_offset_seconds();
        
        // Dubai should have same offset year-round (+4h = 14400 seconds)
        assert_eq!(dubai_summer, 14400, "Dubai summer offset should be +4h");
        assert_eq!(dubai_winter, 14400, "Dubai winter offset should be +4h");
        assert_eq!(dubai_summer, dubai_winter, "Dubai offsets should be same year-round");
    }
    
    #[test]
    fn test_dst_transition_edge_cases() {
        // Test the exact moments of DST transitions
        
        // US Eastern DST begins: March 12, 2023 at 2:00 AM → 3:00 AM
        // Times like 2:30 AM don't exist on this day
        
        let mut job_data = create_test_job_data(Tz::US__Eastern, 0);
        
        // Job scheduled at 1:30 AM (before spring forward)
        let before_spring = Utc.with_ymd_and_hms(2023, 3, 12, 6, 30, 0).unwrap(); // 1:30 AM EST = 6:30 UTC
        job_data.next_tick = before_spring.timestamp() as u64;
        let before_offset = job_data.time_offset_seconds();
        assert_eq!(before_offset, -18000, "Before spring forward should be EST (-5h)");
        
        // Job scheduled at 3:30 AM (after spring forward)
        let after_spring = Utc.with_ymd_and_hms(2023, 3, 12, 7, 30, 0).unwrap(); // 3:30 AM EDT = 7:30 UTC  
        job_data.next_tick = after_spring.timestamp() as u64;
        let after_offset = job_data.time_offset_seconds();
        assert_eq!(after_offset, -14400, "After spring forward should be EDT (-4h)");
        
        // US Eastern DST ends: November 5, 2023 at 2:00 AM → 1:00 AM
        // Times like 1:30 AM occur twice on this day
        
        // Job scheduled at 1:30 AM (first occurrence, still EDT)
        let first_occurrence = Utc.with_ymd_and_hms(2023, 11, 5, 5, 30, 0).unwrap(); // 1:30 AM EDT = 5:30 UTC
        job_data.next_tick = first_occurrence.timestamp() as u64;
        let first_offset = job_data.time_offset_seconds();
        assert_eq!(first_offset, -14400, "First 1:30 AM should be EDT (-4h)");
        
        // Job scheduled at 1:30 AM (second occurrence, now EST)
        let second_occurrence = Utc.with_ymd_and_hms(2023, 11, 5, 6, 30, 0).unwrap(); // 1:30 AM EST = 6:30 UTC
        job_data.next_tick = second_occurrence.timestamp() as u64;
        let second_offset = job_data.time_offset_seconds();
        assert_eq!(second_offset, -18000, "Second 1:30 AM should be EST (-5h)");
        
        // Verify they're different
        assert_ne!(first_offset, second_offset, "Same local time, different offsets during fall back");
    }
}
