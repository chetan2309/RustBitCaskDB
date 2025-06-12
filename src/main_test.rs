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

        // Call the write method and validate the result
        let result = sst_storage.write(&key, &value, false, timestamp);
        assert!(result.is_ok());

        let records = read_from_file(&temp_file_path).unwrap();

        // Validate that the key and value were written correctly
        assert_eq!(records[0].key, &key[..]);
        assert_eq!(records[0].value, &value[..]);
        // Debug print
        let read_timestamp = records[0].timestamp.unwrap();
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

    #[test]
    fn test_insert_key_and_read_existing_key() {
        // Create a temporary file for testing
        let temp_file_path = "temp_test_file.txt";
        let file = open_file_read_write(temp_file_path).expect("Failed to create temp file");
        let mut sst_storage = SStStorage::new(file);

        let key = b"my_key".to_vec();
        let value = b"my_value".to_vec();
        
        // Writing a known kv pair to the file
        sst_storage.write(&key, &value, false, Some(0)).unwrap();
        
        // Reading the kv pair from the file
        let read_value = sst_storage.read(&key).unwrap();
        assert_eq!(read_value, Some(value));

        // cleanup
        fs::remove_file(temp_file_path).expect("Failed to remove temp file");
    }

    #[test]
    fn test_insert_key_and_read_non_existing_key() {
        // Create a temporary file for testing
        let temp_file_path = "temp_test_file.txt";
        let file = open_file_read_write(temp_file_path).expect("Failed to create temp file");
        let mut sst_storage = SStStorage::new(file);

        let key = b"my_key".to_vec();
        let value = b"my_value".to_vec();

        // Writing a known kv pair to the file
        sst_storage.write(&key, &value, false, Some(0)).unwrap();

        // Reading the kv pair from the file that does not exist
        let non_existent_key = b"non_existent_key".to_vec();
        let read_value = sst_storage.read(&non_existent_key ).unwrap();
        assert_eq!(read_value, None);
        assert_ne!(read_value, Some(value));

        // cleanup
        fs::remove_file(temp_file_path).expect("Failed to remove temp file");
    }

    #[test]
    fn test_update_existing_key() {
        // Create a temporary file for testing
        let temp_file_path = "temp_test_file.txt";
        let file = open_file_read_write(temp_file_path).expect("Failed to create temp file");
        let mut sst_storage = SStStorage::new(file);

        // Insert a known kv pair to the file
        let key = b"my_key".to_vec();
        let value = b"my_value".to_vec();
        let timestamp = Some(0);
        sst_storage.write(&key, &value, false, timestamp).unwrap();
        
        // Update the kv pair
        let updated_value = b"updated_value".to_vec();
        let updated_timestamp = Some(1);
        sst_storage.write(&key, &updated_value, false, updated_timestamp).unwrap();

        // Reading the kv pair from the file
        let read_value = sst_storage.read(&key).unwrap();
        assert_eq!(read_value, Some(updated_value));

        // cleanup
        fs::remove_file(temp_file_path).expect("Failed to remove temp file");
    }

    #[test]
    fn test_delete_existing_key() {
        // Create a temporary file for testing
        let temp_file_path = "temp_test_file.txt";
        let file = open_file_read_write(temp_file_path).expect("Failed to create temp file");
        let mut sst_storage = SStStorage::new(file);

        // Insert a known kv pair to the file
        let key = b"my_key".to_vec();
        let value = b"my_value".to_vec();
        let timestamp = Some(0);
        sst_storage.write(&key, &value, false, timestamp).unwrap();

        // Delete the kv pair
        sst_storage.delete_key(&key).unwrap();

        // Reading the kv pair from the file
        let read_value = sst_storage.read(&key).unwrap();
        // assert_eq!(read_value, None);

        assert_ne!(read_value, Some(value));
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
