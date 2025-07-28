#[cfg(test)]
mod tests {
    use std::{
        fs,
        io::{Seek, Write},
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
        // Call the write method and validate the result
        let result = sst_storage.write(&key, &value, false, Some(0));
        assert!(result.is_ok());

        let records = read_from_file(&temp_file_path).unwrap();

        // Validate that the key and value were written correctly
        assert_eq!(records[0].key, &key[..]);
        assert_eq!(records[0].value, &value[..]);

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
    fn test_read_corrupted_record() {
        // Create a temporary file for testing
        let temp_file_path = "temp_test_file_read_corrupted.txt";
        let file = open_file_read_write(temp_file_path).expect("Failed to create temp file");
        let mut sst_storage = SStStorage::new(file);

        let key = b"my_key".to_vec();
        let value = b"my_value".to_vec();

        // Writing a known kv pair to the file
        sst_storage.write(&key, &value, false, Some(0)).unwrap();

        // Manually corrupt the file
        let mut file_to_corrupt = fs::OpenOptions::new()
            .write(true)
            .open(temp_file_path)
            .unwrap();
        // Corrupt the last byte of the file, which is part of the checksum
        file_to_corrupt.seek(std::io::SeekFrom::End(-1)).unwrap();
        file_to_corrupt.write_all(&[0x00]).unwrap();

        // Try to read the corrupted record
        let result = sst_storage.read(&key);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().kind(), std::io::ErrorKind::InvalidData);

        // cleanup
        fs::remove_file(temp_file_path).expect("Failed to remove temp file");
    }
    #[test]
    fn test_cleanup_expired_keys() {
        // Create a temporary file for testing
        let temp_file_path = "temp_test_file_cleanup_expired.txt";
        let file = open_file_read_write(temp_file_path).expect("Failed to create temp file");
        let mut sst_storage = SStStorage::new(file);

        let key = b"my_key".to_vec();
        let value = b"my_value".to_vec();
        let timestamp = Some(1); // A timestamp in the past

        // Writing a known kv pair to the file
        sst_storage.write(&key, &value, false, timestamp).unwrap();

        // Call the cleanup function
        sst_storage.cleanup_expired_keys().unwrap();

        // Try to read the expired record
        let result = sst_storage.read(&key).unwrap();
        assert_eq!(result, None);

        // cleanup
        fs::remove_file(temp_file_path).expect("Failed to remove temp file");
    }
    #[test]
    fn test_timestamp_issue() {
        println!("Testing timestamp serialization...");

        // Test with the problematic timestamp
        let test_timestamp = Some(1749763021u64);

        let file = std::fs::File::create("test_timestamp.db").unwrap();
        let storage = SStStorage::new(file);

        storage.test_timestamp_serialization(test_timestamp).unwrap();

        // Clean up
        std::fs::remove_file("test_timestamp.db").ok();
    }

    #[test]
    fn test_corruption() {
        let test_file_name = "corruption_test.db";
        // Start with a clean file for a predictable test
        if fs::metadata(test_file_name).is_ok() {
            fs::remove_file(test_file_name).unwrap();
        }

        // --- Step 1: Write a known record ---
        {
            println!("Step 1: Writing a known record to '{}'...", test_file_name);
            let file = open_file_read_write(test_file_name).unwrap();
            let mut sst_storage = SStStorage::new(file);
            let key = b"integrity_check";
            let value = b"this_data_is_good";
            sst_storage.write(key, value, false, None).unwrap();
            println!("Record written successfully.");
        } // `sst_storage` and `file` are dropped here, closing the file.

        // --- Step 2: Manually corrupt the file ---
        {
            println!("Step 2: Corrupting the file by changing one byte...");
            let mut file_to_corrupt = fs::OpenOptions::new().write(true).open(test_file_name).unwrap();

            // Let's corrupt a byte in the middle of the value "this_data_is_good"
            // The value starts after:
            // 1 byte (key_len) + 1 byte (val_len) + 15 bytes (key) = 17 bytes from start
            // Let's change the 'd' in "good" to 'X'. 'd' is at index 10 of the value.
            // So, we seek to offset 17 + 10 = 27
            let corruption_offset = 27;
            file_to_corrupt.seek(std::io::SeekFrom::Start(corruption_offset)).unwrap();
            file_to_corrupt.write_all(&[b'X']).unwrap(); // Corrupt 'd' to 'X'
            println!("File has been corrupted at byte {}!", corruption_offset);
        }

        // --- Step 3 & 4: Attempt to load the corrupted file and observe ---
        println!("Step 3: Attempting to load the corrupted database...");
        let file = open_file_read_write(test_file_name).unwrap();
        let mut sst_storage = SStStorage::new(file);

        // The load_db_from_disk() function will read all records and verify checksums.
        // This call is EXPECTED to fail.
        match sst_storage.load_db_from_disk() {
            Ok(_) => {
                panic!("TEST FAILED: The program loaded the corrupted data without error.");
            }
            Err(e) => {
                if e.to_string().contains("Checksum mismatch") || e.to_string().contains("invalid data") {
                    println!("✅ TEST PASSED: The program correctly detected data corruption!");
                    println!("   Error message was: '{}'", e);
                } else {
                    panic!("TEST FAILED: The program failed, but not with the expected checksum error. Error message was: '{}'", e);
                }
            }
        }

        // Clean up the test file
        fs::remove_file(test_file_name).unwrap();
    }
}
