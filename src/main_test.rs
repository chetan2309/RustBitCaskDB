#[cfg(test)]
mod tests {
    use std::{
        fs::{self},
        io::{Read, Seek, SeekFrom},
    };

    use crate::{open_file_read_only, open_file_read_write, SStStorage};
    #[test]
    fn test_write() {
        // Create a temporary file for testing
        let temp_file_path = "temp_test_file.txt";
        let file = open_file_read_write(temp_file_path).expect("Failed to create temp file");
        let mut sst_storage = SStStorage::new(file);

        let key = vec![1, 2, 3];
        let value = vec![4, 5, 6];

        // Call the write method and validate the result
        let result = sst_storage.write(&key, &value, false, None);
        assert!(result.is_ok());

        // Manually read the content from the temporary file and validate
        let mut file_content = Vec::new();
        let mut file = open_file_read_only(temp_file_path).expect("Failed to open temp file");
        file.seek(SeekFrom::Start(0)).expect("Failed to seek file");
        file.read_to_end(&mut file_content)
            .expect("Failed to read file");

        // Validate that the key and value were written correctly
        assert_eq!(file_content, [&key[..], &value[..]].concat());

        // Clean up the temporary file
        fs::remove_file(temp_file_path).expect("Failed to remove temp file");
    }
}
