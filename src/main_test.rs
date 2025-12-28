#[cfg(test)]
mod tests {
    use std::fs;

    use dance_of_bytes::read_from_file;

    use crate::{open_file_read_write, SStStorage};
    #[test]
    fn test_write() {
        // Create a temporary file for testing
        let temp_file_path = "temp_test_file_write.txt";
        // Clean up any existing file from previous test runs
        let _ = fs::remove_file(temp_file_path);
        let file = open_file_read_write(temp_file_path).expect("Failed to create temp file");
        let mut sst_storage = SStStorage::new(file);

        let key = vec![1, 2, 3];
        let value = vec![4, 5, 6];
        let timestamp = Some(1234567890u64);

        // Call the write method and validate the result
        let result = sst_storage.write(&key, &value, false, timestamp);
        assert!(result.is_ok());

        let records = read_from_file(&temp_file_path).unwrap();

        // Validate that the key and value were written correctly
        assert_eq!(records[0].key, &key[..]);
        assert_eq!(records[0].value, &value[..]);
        // Verify the exact timestamp is stored and retrieved correctly
        let read_timestamp = records[0].timestamp.unwrap();
        assert_eq!(
            read_timestamp, 1234567890,
            "Timestamp should match the value that was written"
        );

        // Clean up the temporary file
        fs::remove_file(temp_file_path).expect("Failed to remove temp file");
    }

    #[test]
    fn test_insert_key_and_read_existing_key() {
        // Create a temporary file for testing
        let temp_file_path = "temp_test_file_insert_and_delete.txt";
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
        let temp_file_path = "temp_test_file_insert_delete_non_existing.txt";
        let file = open_file_read_write(temp_file_path).expect("Failed to create temp file");
        let mut sst_storage = SStStorage::new(file);

        let key = b"my_key".to_vec();
        let value = b"my_value".to_vec();

        // Writing a known kv pair to the file
        sst_storage.write(&key, &value, false, Some(0)).unwrap();

        // Reading the kv pair from the file that does not exist
        let non_existent_key = b"non_existent_key".to_vec();
        let read_value = sst_storage.read(&non_existent_key).unwrap();
        assert_eq!(read_value, None);

        // cleanup
        fs::remove_file(temp_file_path).expect("Failed to remove temp file");
    }

    #[test]
    fn test_update_existing_key() {
        // Create a temporary file for testing
        let temp_file_path = "temp_test_file_update_key.txt";
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
        sst_storage
            .write(&key, &updated_value, false, updated_timestamp)
            .unwrap();

        // Reading the kv pair from the file
        let read_value = sst_storage.read(&key).unwrap();
        assert_eq!(read_value, Some(updated_value));

        // cleanup
        fs::remove_file(temp_file_path).expect("Failed to remove temp file");
    }

    #[test]
    fn test_delete_existing_key() {
        // Create a temporary file for testing
        let temp_file_path = "temp_test_file_delete_existing.txt";
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
        assert_eq!(read_value, None);
    }

}
