#[cfg(test)]
mod tests {
    use std::{
        fs,
        time::{SystemTime, UNIX_EPOCH},
    };

    use dance_of_bytes::read_from_file;

    use crate::{open_file_read_write, SStStorage};
    #[test]
    fn test_write() {
        // Create a temporary file for testing
        let temp_file_path = "temp_test_file_write.txt";
        let file = open_file_read_write(temp_file_path).expect("Failed to create temp file");
        let mut sst_storage = SStStorage::new(file);

        let key = vec![1, 2, 3];
        let value = vec![4, 5, 6];
        let timestamp = Some(1000);

        // Call the write method and validate the result
        let result = sst_storage.write(&key, &value, false, timestamp);
        assert!(result.is_ok());

        let records = read_from_file(&temp_file_path).unwrap();

        // Validate that the key and value were written correctly
        assert_eq!(records[0].key, &key[..]);
        assert_eq!(records[0].value, &value[..]);
        // Validate timestamp exists (the dance_of_bytes library manages timestamps)
        assert!(records[0].timestamp.is_some());
        // Validate tombstone is false
        assert_eq!(records[0].tombstone, false);

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
        let read_value = sst_storage.read(&non_existent_key ).unwrap();
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



    #[test]
    fn test_load_db_from_disk_empty_file() {
        // Test loading from an empty database file
        let temp_file_path = "temp_test_file_load_empty.txt";
        let file = open_file_read_write(temp_file_path).expect("Failed to create temp file");
        let mut sst_storage = SStStorage::new(file);

        // Load from empty file should succeed
        let result = sst_storage.load_db_from_disk();
        assert!(result.is_ok());

        // Index should be empty
        assert_eq!(sst_storage.index.len(), 0);

        // Clean up
        fs::remove_file(temp_file_path).expect("Failed to remove temp file");
    }

    #[test]
    fn test_load_db_from_disk_with_data() {
        // Test loading from a database file with existing data
        let temp_file_path = "temp_test_file_load_with_data.txt";
        let file = open_file_read_write(temp_file_path).expect("Failed to create temp file");
        let mut sst_storage = SStStorage::new(file);

        // Write some test data
        let key1 = b"key1".to_vec();
        let value1 = b"value1".to_vec();
        let key2 = b"key2".to_vec();
        let value2 = b"value2".to_vec();
        
        sst_storage.write(&key1, &value1, false, Some(1000)).unwrap();
        sst_storage.write(&key2, &value2, false, Some(2000)).unwrap();

        // Create a new storage instance and load from disk
        let file2 = open_file_read_write(temp_file_path).expect("Failed to open temp file");
        let mut sst_storage2 = SStStorage::new(file2);
        let result = sst_storage2.load_db_from_disk();
        assert!(result.is_ok());

        // Verify data was loaded correctly
        assert_eq!(sst_storage2.index.len(), 2);
        assert!(sst_storage2.index.contains_key(&key1));
        assert!(sst_storage2.index.contains_key(&key2));

        // Verify we can read the data
        let read_value1 = sst_storage2.read(&key1).unwrap();
        let read_value2 = sst_storage2.read(&key2).unwrap();
        assert_eq!(read_value1, Some(value1));
        assert_eq!(read_value2, Some(value2));

        // Clean up
        fs::remove_file(temp_file_path).expect("Failed to remove temp file");
    }

    #[test]
    fn test_load_db_from_disk_with_tombstones() {
        // Test loading from a database file with tombstone records
        let temp_file_path = "temp_test_file_load_with_tombstones.txt";
        let file = open_file_read_write(temp_file_path).expect("Failed to create temp file");
        let mut sst_storage = SStStorage::new(file);

        // Write some data and then delete it
        let key = b"deleted_key".to_vec();
        let value = b"deleted_value".to_vec();
        
        sst_storage.write(&key, &value, false, Some(1000)).unwrap();
        sst_storage.delete_key(&key).unwrap();

        // Create a new storage instance and load from disk
        let file2 = open_file_read_write(temp_file_path).expect("Failed to open temp file");
        let mut sst_storage2 = SStStorage::new(file2);
        let result = sst_storage2.load_db_from_disk();
        assert!(result.is_ok());

        // The deleted key should not be in the index
        assert!(!sst_storage2.index.contains_key(&key));
        
        // Reading the key should return None
        let read_value = sst_storage2.read(&key).unwrap();
        assert_eq!(read_value, None);

        // Clean up
        fs::remove_file(temp_file_path).expect("Failed to remove temp file");
    }

    #[test]
    fn test_delete_non_existing_key() {
        // Test deleting a key that doesn't exist
        let temp_file_path = "temp_test_file_delete_non_existing.txt";
        let file = open_file_read_write(temp_file_path).expect("Failed to create temp file");
        let mut sst_storage = SStStorage::new(file);

        let non_existent_key = b"non_existent".to_vec();
        
        // Deleting a non-existent key should succeed (no-op)
        let result = sst_storage.delete_key(&non_existent_key);
        assert!(result.is_ok());

        // Index should still be empty
        assert_eq!(sst_storage.index.len(), 0);

        // Clean up
        fs::remove_file(temp_file_path).expect("Failed to remove temp file");
    }

    #[test]
    fn test_update_non_existing_key() {
        // Test updating a key that doesn't exist
        let temp_file_path = "temp_test_file_update_non_existing.txt";
        let file = open_file_read_write(temp_file_path).expect("Failed to create temp file");
        let mut sst_storage = SStStorage::new(file);

        let non_existent_key = b"non_existent".to_vec();
        let new_value = b"new_value".to_vec();
        
        // Updating a non-existent key should succeed but not add it to index
        let result = sst_storage.update(&non_existent_key, &new_value, false, Some(1000));
        assert!(result.is_ok());

        // Index should still be empty since key doesn't exist
        assert_eq!(sst_storage.index.len(), 0);

        // Reading the key should return None
        let read_value = sst_storage.read(&non_existent_key).unwrap();
        assert_eq!(read_value, None);

        // Clean up
        fs::remove_file(temp_file_path).expect("Failed to remove temp file");
    }

    #[test]
    fn test_write_with_none_timestamp() {
        // Test writing with None timestamp
        let temp_file_path = "temp_test_file_none_timestamp.txt";
        let file = open_file_read_write(temp_file_path).expect("Failed to create temp file");
        let mut sst_storage = SStStorage::new(file);

        let key = b"test_key".to_vec();
        let value = b"test_value".to_vec();
        
        // Write with None timestamp should work
        let result = sst_storage.write(&key, &value, false, None);
        assert!(result.is_ok());

        // Should be able to read the value
        let read_value = sst_storage.read(&key).unwrap();
        assert_eq!(read_value, Some(value));

        // Clean up
        fs::remove_file(temp_file_path).expect("Failed to remove temp file");
    }

    #[test]
    fn test_multiple_updates_same_key() {
        // Test multiple updates to the same key
        let temp_file_path = "temp_test_file_multiple_updates.txt";
        let file = open_file_read_write(temp_file_path).expect("Failed to create temp file");
        let mut sst_storage = SStStorage::new(file);

        let key = b"update_key".to_vec();
        let value1 = b"value1".to_vec();
        let value2 = b"value2".to_vec();
        let value3 = b"value3".to_vec();
        
        // Write initial value
        sst_storage.write(&key, &value1, false, Some(1000)).unwrap();
        
        // Update multiple times
        sst_storage.write(&key, &value2, false, Some(2000)).unwrap();
        sst_storage.write(&key, &value3, false, Some(3000)).unwrap();

        // Should read the latest value
        let read_value = sst_storage.read(&key).unwrap();
        assert_eq!(read_value, Some(value3));

        // Index should only have one entry for this key
        assert_eq!(sst_storage.index.len(), 1);
        assert!(sst_storage.index.contains_key(&key));

        // Clean up
        fs::remove_file(temp_file_path).expect("Failed to remove temp file");
    }

    #[test]
    fn test_cleanup_expired_keys() {
        // Test the cleanup_expired_keys functionality
        let temp_file_path = "temp_test_file_cleanup.txt";
        let file = open_file_read_write(temp_file_path).expect("Failed to create temp file");
        let mut sst_storage = SStStorage::new(file);

        let current_time = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
        let past_time = current_time - 3600; // 1 hour ago
        let future_time = current_time + 3600; // 1 hour from now

        let expired_key = b"expired_key".to_vec();
        let valid_key = b"valid_key".to_vec();
        let no_timestamp_key = b"no_timestamp_key".to_vec();
        
        // Insert keys with different timestamps
        sst_storage.write(&expired_key, b"expired_value", false, Some(past_time)).unwrap();
        sst_storage.write(&valid_key, b"valid_value", false, Some(future_time)).unwrap();
        sst_storage.write(&no_timestamp_key, b"no_timestamp_value", false, None).unwrap();

        // All keys should be present initially
        assert_eq!(sst_storage.index.len(), 3);

        // Run cleanup
        let result = sst_storage.cleanup_expired_keys();
        assert!(result.is_ok());

        // Only valid_key and no_timestamp_key should remain
        assert_eq!(sst_storage.index.len(), 2);
        assert!(!sst_storage.index.contains_key(&expired_key));
        assert!(sst_storage.index.contains_key(&valid_key));
        assert!(sst_storage.index.contains_key(&no_timestamp_key));

        // Clean up
        fs::remove_file(temp_file_path).expect("Failed to remove temp file");
    }

    #[test]
    fn test_large_key_value_pairs() {
        // Test with larger key-value pairs
        let temp_file_path = "temp_test_file_large_kv.txt";
        let file = open_file_read_write(temp_file_path).expect("Failed to create temp file");
        let mut sst_storage = SStStorage::new(file);

        // Create large key and value (but within u8 limits)
        let large_key = vec![b'k'; 255]; // Max size for u8 length
        let large_value = vec![b'v'; 255]; // Max size for u8 length
        
        // Write large key-value pair
        let result = sst_storage.write(&large_key, &large_value, false, Some(1000));
        assert!(result.is_ok());

        // Should be able to read it back
        let read_value = sst_storage.read(&large_key).unwrap();
        assert_eq!(read_value, Some(large_value));

        // Clean up
        fs::remove_file(temp_file_path).expect("Failed to remove temp file");
    }

    #[test]
    fn test_generate_timestamp_one_hour_in_future() {
        use crate::generate_timestamp_one_hour_in_future;
        
        let current_time = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
        let future_timestamp = generate_timestamp_one_hour_in_future();
        
        // The generated timestamp should be greater than current time
        assert!(future_timestamp > current_time);
        
        // Should be approximately 2 minutes in the future (as per the implementation)
        let expected_future = current_time + 120; // 2 minutes
        let tolerance = 5; // 5 seconds tolerance
        
        assert!(future_timestamp >= expected_future - tolerance);
        assert!(future_timestamp <= expected_future + tolerance);
    }

    #[test]
    fn test_open_file_read_write_new_file() {
        use crate::open_file_read_write;
        
        let temp_file_path = "temp_test_open_new_file.txt";
        
        // Remove file if it exists
        let _ = fs::remove_file(temp_file_path);
        
        // Open file should create it
        let result = open_file_read_write(temp_file_path);
        assert!(result.is_ok());
        
        let file = result.unwrap();
        drop(file); // Close the file
        
        // File should now exist
        assert!(std::path::Path::new(temp_file_path).exists());
        
        // Clean up
        fs::remove_file(temp_file_path).expect("Failed to remove temp file");
    }

    #[test]
    fn test_open_file_read_write_existing_file() {
        use crate::open_file_read_write;
        
        let temp_file_path = "temp_test_open_existing_file.txt";
        
        // Create file first
        std::fs::write(temp_file_path, "existing content").expect("Failed to create test file");
        
        // Open existing file should succeed
        let result = open_file_read_write(temp_file_path);
        assert!(result.is_ok());
        
        let file = result.unwrap();
        drop(file); // Close the file
        
        // Clean up
        fs::remove_file(temp_file_path).expect("Failed to remove temp file");
    }

    #[test]
    fn test_file_io_trait_implementation() {
        use crate::FileIO;
        use std::io::SeekFrom;
        
        let temp_file_path = "temp_test_file_io_trait.txt";
        let mut file = open_file_read_write(temp_file_path).expect("Failed to create temp file");
        
        // Test write using FileIO trait
        let test_data = b"Hello, World!";
        let write_result = FileIO::write(&mut file, test_data);
        assert!(write_result.is_ok());
        
        // Test seek using FileIO trait
        let seek_result = FileIO::seek_from(&mut file, SeekFrom::Start(0));
        assert!(seek_result.is_ok());
        assert_eq!(seek_result.unwrap(), 0);
        
        // Test read using FileIO trait
        let mut buffer = vec![0u8; test_data.len()];
        let read_result = FileIO::read(&mut file, &mut buffer);
        assert!(read_result.is_ok());
        assert_eq!(buffer, test_data);
        
        // Clean up
        drop(file);
        fs::remove_file(temp_file_path).expect("Failed to remove temp file");
    }

    #[test]
    fn test_sst_storage_new() {
        let temp_file_path = "temp_test_sst_storage_new.txt";
        let file = open_file_read_write(temp_file_path).expect("Failed to create temp file");
        let sst_storage = SStStorage::new(file);
        
        // New storage should have empty index
        assert_eq!(sst_storage.index.len(), 0);
        
        // Clean up
        fs::remove_file(temp_file_path).expect("Failed to remove temp file");
    }

    #[test]
    fn test_insert_key_method() {
        let temp_file_path = "temp_test_insert_key_method.txt";
        let file = open_file_read_write(temp_file_path).expect("Failed to create temp file");
        let mut sst_storage = SStStorage::new(file);
        
        let key = b"test_key".to_vec();
        let value_info = (100, 50, false, Some(1234567890));
        
        // Test insert_key method
        sst_storage.insert_key(key.clone(), value_info);
        
        // Key should be in index
        assert_eq!(sst_storage.index.len(), 1);
        assert!(sst_storage.index.contains_key(&key));
        assert_eq!(sst_storage.index.get(&key), Some(&value_info));
        
        // Clean up
        fs::remove_file(temp_file_path).expect("Failed to remove temp file");
    }
}
