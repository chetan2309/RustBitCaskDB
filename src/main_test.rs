#[cfg(test)]
mod tests {
    use std::{
        fs::{self, File, OpenOptions},
        io::{Read, Seek, SeekFrom},
    };

    use crate::{KeyValue, SStStorage};
    #[test]
    fn test_write() {
        // Create a temporary file for testing
        let temp_file_path = "temp_test_file.txt";
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(temp_file_path)
            .expect("Failed to create temp file");
        let mut sst_storage = SStStorage::new(file);

        let key_value = KeyValue {
            key: vec![1, 2, 3],
            value: vec![4, 5, 6],
        };

        // Clone the key and value before passing to the write method
        let key_clone = key_value.key.clone();
        let value_clone = key_value.value.clone();

        // Call the write method and validate the result
        let result = sst_storage.write(
            KeyValue {
                key: key_clone,
                value: value_clone,
            },
            false,
            None,
        );
        assert!(result.is_ok());

        // Manually read the content from the temporary file and validate
        let mut file_content = Vec::new();
        let mut file = File::open(temp_file_path).expect("Failed to open temp file");
        file.seek(SeekFrom::Start(0)).expect("Failed to seek file");
        file.read_to_end(&mut file_content)
            .expect("Failed to read file");

        // Validate that the key and value were written correctly
        assert_eq!(
            file_content,
            [&key_value.key[..], &key_value.value[..]].concat()
        );

        // Clean up the temporary file
        fs::remove_file(temp_file_path).expect("Failed to remove temp file");
    }
}
