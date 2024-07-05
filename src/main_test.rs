#[cfg(test)]
mod tests {
    use std::time::Duration;
    use std::{
        fs,
        ops::Add,
        time::{SystemTime, UNIX_EPOCH},
    };

    use dance_of_bytes::read_from_file;

    use crate::{open_file_read_write, SStStorage};
    #[test]
    fn test_write() {
        // Create a temporary file for testing
        let temp_file_path = "temp_test_file.txt";
        let file = open_file_read_write(temp_file_path).expect("Failed to create temp file");
        let mut sst_storage = SStStorage::new(file);

        let key = vec![1, 2, 3];
        let value = vec![4, 5, 6];
        let timestamp = Some(10);
        let (lower_bound, upper_bound) = generate_timestamp_range(10);

        // Debug print
        println!("Generated timestamp: {:?}", timestamp);
        // Call the write method and validate the result
        let result = sst_storage.write(&key, &value, false, timestamp);
        assert!(result.is_ok());

        let records = read_from_file(&temp_file_path).unwrap();

        // Validate that the key and value were written correctly
        // println!("Value of records[0]..key and &[key..] are {:?} {:?}", records[0].key, &key[..]);
        assert_eq!(records[0].key, &key[..]);
        assert_eq!(records[0].value, &value[..]);
        // Debug print
        let read_timestamp = records[0].timestamp.unwrap();
        // println!("Read timestamp: {}", read_timestamp);
        // println!("Expected range: {} to {}", lower_bound, upper_bound);
        assert!(
            read_timestamp >= lower_bound && read_timestamp <= upper_bound,
            "Timestamp {} is not within expected range {} to {}",
            read_timestamp,
            lower_bound,
            upper_bound
        );

        // Clean up the temporary file
        fs::remove_file(temp_file_path).expect("Failed to remove temp file");
    }

    const SECONDS_IN_MINS: u64 = 60;

    fn generate_timestamp_range(minutes: u64) -> (u64, u64) {
        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
        let lower_bound = now.as_secs();
        let upper_bound = now
            .add(Duration::from_secs(minutes * SECONDS_IN_MINS))
            .as_secs();
        (lower_bound, upper_bound)
    }
}
