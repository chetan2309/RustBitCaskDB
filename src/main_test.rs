#[cfg(test)]
mod tests {
    use std::{collections::HashMap, fs, path::PathBuf};

    use crate::{FileStorage, KeyDir, dance_of_bytes::read_from_file as read_from_file_dance_of_bytes, delete_key, load_db_from_disk, read_from_file, update_key_value, write_to_file};
    #[test]
    fn test_write() {
        // Create a temporary file for testing
        let temp_file_dir = "temp_test_file_write";
        // Clean up any existing file from previous test runs
        let _ = fs::remove_dir_all(temp_file_dir);
        let _ = fs::create_dir(temp_file_dir);
        let temp_file = PathBuf::from(temp_file_dir);
        let mut file_storage = FileStorage::open(&temp_file).unwrap();

        let key = b"some_key";
        let value = b"some_value";
        let timestamp = Some(1234567890u64);

        let mut key_dir = KeyDir {
            index: HashMap::new(),
        };
        
        // Call the write method and validate the result
        let result = write_to_file(key, value, false, timestamp, &mut key_dir, &mut file_storage);
        assert!(result.is_ok());

        let record_value = read_from_file(key, &mut key_dir, &mut file_storage).unwrap();

        // Validate that the key and value were written correctly
        assert_eq!(record_value, Some(value.to_vec()));

        // Clean up the temporary file
        fs::remove_dir_all(temp_file_dir).expect("Failed to remove temp file");
    }

    #[test]
    fn test_insert_key_and_read_existing_key() {
        // Create a temporary file for testing
        let temp_file_dir = "temp_test_insert_key_and_read_existing_key";
        // Clean up any existing file from previous test runs
        let _ = fs::remove_dir_all(temp_file_dir);
        let _ = fs::create_dir(temp_file_dir);
        let temp_file = PathBuf::from(temp_file_dir);
        let mut file_storage = FileStorage::open(&temp_file).unwrap();
        let timestamp = Some(1234567890u64);

        let key = b"my_key";
        let value = b"my_value";

        let mut key_dir = KeyDir {
            index: HashMap::new(),
        };

        // Writing a known kv pair to the file
        write_to_file(key, value, false, timestamp, &mut key_dir, &mut file_storage).unwrap();

        // Reading the kv pair from the file
        let record_value = read_from_file(key, &mut key_dir, &mut file_storage).unwrap();
        assert_eq!(record_value, Some(value.to_vec()));

        // cleanup
        // Clean up the temporary file
        fs::remove_dir_all(temp_file_dir).expect("Failed to remove temp file");
    }

    #[test]
    fn test_insert_key_and_read_non_existing_key() {
        // Create a temporary file for testing
        let temp_file_dir = "temp_test_insert_key_and_read_non_existing_key";
        // Clean up any existing file from previous test runs
        let _ = fs::remove_dir_all(temp_file_dir);
        let _ = fs::create_dir(temp_file_dir);
        let temp_file = PathBuf::from(temp_file_dir);
        let mut file_storage = FileStorage::open(&temp_file).unwrap();
        let timestamp = Some(1234567890u64);

        let key = b"my_key";
        let value = b"my_value";

        let mut key_dir = KeyDir {
            index: HashMap::new(),
        };

        // Writing a known kv pair to the file
        write_to_file(key, value, false, timestamp, &mut key_dir, &mut file_storage).unwrap();

        // Reading the kv pair from the file
        let non_existent_key = b"non_existent_key";
        let record_value = read_from_file(non_existent_key, &mut key_dir, &mut file_storage).unwrap();
        assert_eq!(record_value, None);

        // Clean up the temporary file
        fs::remove_dir_all(temp_file_dir).expect("Failed to remove temp file");
    }

    #[test]
    fn test_update_existing_key() {
        // Create a temporary file for testing
        let temp_file_dir = "temp_test_update_existing_key";
        // Clean up any existing file from previous test runs
        let _ = fs::remove_dir_all(temp_file_dir);
        let _ = fs::create_dir(temp_file_dir);
        let temp_file = PathBuf::from(temp_file_dir);
        let mut file_storage = FileStorage::open(&temp_file).unwrap();
        let timestamp = Some(1234567890u64);

        let key = b"my_key";
        let value = b"my_value";

        let mut key_dir = KeyDir {
            index: HashMap::new(),
        };

        // Writing a known kv pair to the file
        write_to_file(key, value, false, timestamp, &mut key_dir, &mut file_storage).unwrap();


        // Update the kv pair
        let updated_value = b"updated_value";
        let updated_timestamp = Some(1);
        update_key_value(key, updated_value, false, updated_timestamp, &mut key_dir, &mut file_storage).unwrap();

        // Reading the kv pair from the file
        let record_value = read_from_file(key, &mut key_dir, &mut file_storage).unwrap();
        assert_eq!(record_value, Some(updated_value.to_vec()));

        // Clean up the temporary file
        fs::remove_dir_all(temp_file_dir).expect("Failed to remove temp file");
    }

    #[test]
    fn test_delete_existing_key() {
        // Create a temporary file for testing
        let temp_file_dir = "temp_test_delete_existing_key";
        // Clean up any existing file from previous test runs
        let _ = fs::remove_dir_all(temp_file_dir);
        let _ = fs::create_dir(temp_file_dir);
        let temp_file = PathBuf::from(temp_file_dir);
        let mut file_storage = FileStorage::open(&temp_file).unwrap();

        let key = b"my_key";
        let value = b"my_value";
        let timestamp = Some(0);

        let mut key_dir = KeyDir {
            index: HashMap::new(),
        };

        let result = write_to_file(key, value, false, timestamp, &mut key_dir, &mut file_storage);
        assert!(result.is_ok(), "Writing initial key-value pair failed");

        // Delete the kv pair
        let result = delete_key(key, &mut key_dir, &mut file_storage);
        // Should succeed without error (no-op)
        assert!(result.is_ok(), "Deleting an existing key should not error");

        // Reading the kv pair from the file should return None
        let read_value = read_from_file(key, &mut key_dir, &mut file_storage).unwrap();
        assert_eq!(read_value, None);

        // Verify tombstone was persisted to disk
        // The file should have 2 records: original write + tombstone
        // Tempfile to pass will be full string path of the file created above
        let log_path = temp_file.join("0.log");
        let records = read_from_file_dance_of_bytes(log_path.to_str().unwrap()).unwrap();
        assert_eq!(records.len(), 2, "There should be 2 records in the file (write + tombstone)");
        let tombstone_record = &records[1]; // The second record should be the tombstone
        assert!( tombstone_record.tombstone, "The second record should be a tombstone");
        fs::remove_dir_all(temp_file_dir).expect("Failed to remove temp file");

    }

    #[test]
    fn test_delete_non_existing_key() {
        // Create a temporary file for testing
        let temp_file_dir = "temp_test_delete_non_existing_key";
        // Clean up any existing file from previous test runs
        let _ = fs::remove_dir_all(temp_file_dir);
        let _ = fs::create_dir(temp_file_dir);
        let temp_file = PathBuf::from(temp_file_dir);
        let mut file_storage = FileStorage::open(&temp_file).unwrap();

        let key = b"my_key";

        let mut key_dir = KeyDir {
            index: HashMap::new(),
        };

        // Delete the non-existing key
        let result = delete_key(key, &mut key_dir, &mut file_storage);
        // Should succeed without error (no-op)
        assert!(result.is_ok(), "Deleting a non-existing key should not error");

        // Clean up the temporary file
        fs::remove_dir_all(temp_file_dir).expect("Failed to remove temp file");
    }

    #[test]
    fn test_merge_then_restart_keeps_latest_value() {
        // Create a temporary file for testing
        let temp_file_dir = "temp_test_merge_then_restart_keeps_latest_value";
        // Clean up any existing file from previous test runs
        let _ = fs::remove_dir_all(temp_file_dir);
        let _ = fs::create_dir(temp_file_dir);
        let temp_file = PathBuf::from(temp_file_dir);

        let key = b"my_key";
        let value = b"old_value";
        let timestamp = Some(1);
        
        // --- session 1: write old value, rotate, merge, then write new value ---
        {
            let mut file_storage = FileStorage::open(&temp_file).unwrap();
            let mut key_dir = KeyDir {
                index: HashMap::new(),
            };

            // Writing first value, small timestamp
            write_to_file(key, value, false, timestamp, &mut key_dir, &mut file_storage).unwrap();

            // close the active file so merge has something to read
            file_storage.rotate_log_file().unwrap();

            // merge the closed file, apply returned update to the keydir
            let updates = file_storage.merge(&key_dir.index).unwrap();
            for (key, entry) in updates {
                key_dir.index.insert(key, entry);
            }
            
            // newer value, larger timestamp -> goes to the active file
            write_to_file(key, b"new_value", false, Some(2), &mut key_dir, &mut file_storage).unwrap();
        }
        // --- fake restart: fresh keydir + storage, rebuild from disk ---
        let mut file_storage = FileStorage::open(&temp_file).unwrap();
        let mut key_dir = KeyDir { index: HashMap::new() };
        load_db_from_disk(&mut key_dir, &mut file_storage).unwrap();

        // --- the value must be the newest one, not the resurrected old one ---
        let value = read_from_file(key, &key_dir, &mut file_storage).unwrap();
        assert_eq!(value, Some(b"new_value".to_vec()));

        fs::remove_dir_all(temp_file_dir).expect("Failed to remove temp file");
        
    }

    #[test]
    fn test_tomsbtone_stays_deleted() {
        // Create a temporary file for testing
        let temp_file_dir = "temp_test_tombstone_stays_deleted";
        // Clean up any existing file from previous test runs
        let _ = fs::remove_dir_all(temp_file_dir);
        let _ = fs::create_dir(temp_file_dir);
        let temp_file = PathBuf::from(temp_file_dir);

        let key = b"my_key";
        
        // --- session 1: write value, rotate, merge, then delete key ---
        {
            let mut file_storage = FileStorage::open(&temp_file).unwrap();
            let mut key_dir = KeyDir {
                index: HashMap::new(),
            };

            // Writing first value, small timestamp
            write_to_file(key, b"value", false, Some(1), &mut key_dir, &mut file_storage).unwrap();

            // delete the key -> tombstone goes to the active file
            delete_key(key, &mut key_dir, &mut file_storage).unwrap();
        }
        // --- fake restart: fresh keydir + storage, rebuild from disk ---
        let mut file_storage = FileStorage::open(&temp_file).unwrap();
        let mut key_dir = KeyDir { index: HashMap::new() };
        load_db_from_disk(&mut key_dir, &mut file_storage).unwrap();

        // --- the value must be None since the tombstone should stay ---
        let value = read_from_file(key, &key_dir, &mut file_storage).unwrap();
        assert_eq!(value, None);

        fs::remove_dir_all(temp_file_dir).expect("Failed to remove temp file");
    }

}
