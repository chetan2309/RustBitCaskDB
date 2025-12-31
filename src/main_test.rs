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
        let mut sst_storage = SStStorage::new(file, 0, std::path::PathBuf::from(temp_file_path));

        let key = b"some_key".to_vec();
        let value = b"some_value".to_vec();
        let timestamp = Some(1234567890u64);

        // Call the write method and validate the result
        let result = sst_storage.write(&key, &value, false, timestamp);
        assert!(result.is_ok());

        let record_value = sst_storage.read(&key).unwrap();

        // Validate that the key and value were written correctly
        assert_eq!(record_value, Some(value));

        // Clean up the temporary file
        fs::remove_file(temp_file_path).expect("Failed to remove temp file");
    }

    #[test]
    fn test_insert_key_and_read_existing_key() {
        // Create a temporary file for testing
        let temp_file_path = "temp_test_file_insert_and_delete.txt";
        let _ = fs::remove_file(temp_file_path);
        let file = open_file_read_write(temp_file_path).expect("Failed to create temp file");
        let mut sst_storage = SStStorage::new(file, 0, std::path::PathBuf::from(temp_file_path));

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
        let _ = fs::remove_file(temp_file_path);
        let file = open_file_read_write(temp_file_path).expect("Failed to create temp file");
        let mut sst_storage = SStStorage::new(file, 0, std::path::PathBuf::from(temp_file_path));

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
        let _ = fs::remove_file(temp_file_path);
        let file = open_file_read_write(temp_file_path).expect("Failed to create temp file");
        let mut sst_storage = SStStorage::new(file, 0, std::path::PathBuf::from(temp_file_path));

        // Insert a known kv pair to the file
        let key = b"my_key".to_vec();
        let value = b"my_value".to_vec();
        let timestamp = Some(0);
        sst_storage.write(&key, &value, false, timestamp).unwrap();

        // Update the kv pair
        let updated_value = b"updated_value".to_vec();
        let updated_timestamp = Some(1);
        sst_storage
            .update(&key, &updated_value, updated_timestamp)
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
        let _ = fs::remove_file(temp_file_path);
        let file = open_file_read_write(temp_file_path).expect("Failed to create temp file");
        let mut sst_storage = SStStorage::new(file, 0, std::path::PathBuf::from(temp_file_path));

        // Insert a known kv pair to the file
        let key = b"my_key".to_vec();
        let value = b"my_value".to_vec();
        let timestamp = Some(0);
        let result = sst_storage.write(&key, &value, false, timestamp);
        assert!(result.is_ok(), "Writing initial key-value pair failed");

        // Delete the kv pair
        let result = sst_storage.delete_key(&key);
        // Should succeed without error (no-op)
        assert!(result.is_ok(), "Deleting an existing key should not error");

        // Reading the kv pair from the file should return None
        let read_value = sst_storage.read(&key).unwrap();
        assert_eq!(read_value, None);

        // Verify tombstone was persisted to disk
        // The file should have 2 records: original write + tombstone
        let records = read_from_file(&temp_file_path).unwrap();
        assert_eq!(records.len(), 2, "Expected 2 records: original + tombstone");
        assert!(records[1].tombstone, "Second record should be a tombstone");
        assert_eq!(records[1].key, key, "Tombstone should have the same key");

        // cleanup
        fs::remove_file(temp_file_path).expect("Failed to remove temp file");
    }

    #[test]
    fn test_delete_non_existing_key() {
        // Create a temporary file for testing
        let temp_file_path = "temp_test_file_delete_non_existing.txt";
        let _ = fs::remove_file(temp_file_path);
        let file = open_file_read_write(temp_file_path).expect("Failed to create temp file");
        let mut sst_storage = SStStorage::new(file, 0, std::path::PathBuf::from(temp_file_path));

        // Try to delete a key that was never inserted
        let non_existent_key = b"ghost_key".to_vec();
        let result = sst_storage.delete_key(&non_existent_key);

        // Should succeed without error (no-op)
        assert!(result.is_ok(), "Deleting non-existent key should not error");

        // Verify no tombstone was written (file should be empty)
        let records = read_from_file(&temp_file_path).unwrap();
        assert_eq!(
            records.len(),
            0,
            "No records should be written for non-existent key delete"
        );

        // cleanup
        fs::remove_file(temp_file_path).expect("Failed to remove temp file");
    }

}
