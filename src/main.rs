use chrono::{DateTime, Utc};
use dance_of_bytes::{self, KeyValue};
use rand::Rng;
use rust_bit_cask_db::parse_key_value_from_reader;
use rust_bit_cask_db::parse_key_value_from_buffer;
use std::{
    collections::BTreeMap,
    fs::{self, File, OpenOptions},
    io::{self, Error, Read, Seek, SeekFrom, Write},
    time::{Duration, Instant},
};
mod main_test;

struct SStStorage<T: FileIO> {
    index: BTreeMap<Vec<u8>, (u64, u64, bool, Option<u64>)>,
    file: T,
}

trait FileIO {
    fn write(&mut self, buf: &[u8]) -> io::Result<()>;
    fn read(&mut self, buf: &mut [u8]) -> io::Result<()>;
    fn seek_from(&mut self, pos: SeekFrom) -> io::Result<u64>;
}

impl FileIO for File {
    fn write(&mut self, buf: &[u8]) -> io::Result<()> {
        File::write_all(self, buf)
    }

    fn read(&mut self, buf: &mut [u8]) -> io::Result<()> {
        File::read_exact(self, buf)
    }

    fn seek_from(&mut self, pos: SeekFrom) -> io::Result<u64> {
        File::seek(self, pos)
    }
}

impl<T: FileIO> SStStorage<T> {
    fn new(file: T) -> Self {
        SStStorage {
            index: BTreeMap::new(),
            file,
        }
    }

    fn insert_key(&mut self, key: Vec<u8>, value: (u64, u64, bool, Option<u64>)) {
        self.index.insert(key, value);
    }

    fn write(
        &mut self,
        key: &[u8],
        value: &[u8],
        mark_as_deleted: bool,
        timestamp: Option<u64>,
    ) -> Result<(), Error> {
        let kv = KeyValue::new(key, value, timestamp, mark_as_deleted, 0);

        let buffer = kv.to_buffer();
        let offset = self.file.seek_from(SeekFrom::End(0))?;
        let length = buffer.len() as u64;
        self.file.write(&buffer)?;
        // Only update the in-memory index for new or updated keys, not for deletions.
        if !mark_as_deleted {
            self.insert_key(key.to_vec(), (offset, length, mark_as_deleted, timestamp));
        }
        Ok(())
    }

    fn read(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>, Error> {
        if let Some((value_offset, length, is_deleted, _)) = self.index.get(key) {
            // print!("Is key deleted  {:?}", is_deleted);
            if *is_deleted {
                return Ok(None);
            }
            let mut buffer = vec![0; *length as usize];
            self.file.seek_from(io::SeekFrom::Start(*value_offset))?;
            self.file.read(&mut buffer)?;
            let kv = parse_key_value_from_buffer(&buffer)?;
            // print!("Trying to read the key  {:?}", kv.key);
            Ok(Some(kv.value))
        } else {
            // print!("Nothing Nada...");
            Ok(None)
        }
    }

    fn update(
        &mut self,
        key: &[u8],
        updated_value: &[u8],
        mark_as_deleted: bool,
        timestamp: Option<u64>,
    ) -> Result<(), Error> {
        // Key has to be searched in hashmap
        if let Some((_, _, _, _)) = self.index.get(key) {
            println!("Reading: key={:?} ", key);
            let _ = self.write(key, updated_value, mark_as_deleted, timestamp);
        }
        Ok(())
    }

    fn delete_key(&mut self, key: &[u8]) -> Result<(), Error> {
        // First, check if the key exists in the live index.
        if self.index.contains_key(key) {
            // Append a tombstone record to the log. The value for a tombstone is irrelevant,
            // so we use an empty slice `&[]`. Our modified `write` function will handle this
            // without adding the key back to the index.
            self.write(key, &[], true, Some(0))?;

            // Finally, remove the key from the in-memory index to mark it as deleted.
            self.index.remove(key);
        }
        Ok(())
    }

    fn load_db_from_disk(&mut self) -> Result<(), Box<dyn std::error::Error>>
    where 
        T: std::io::Read,
     {
        // Seek to the beginning of the active database file to read all entries.
        let mut current_offset = self.file.seek_from(SeekFrom::Start(0))?;
        let file_size = self.file.seek_from(SeekFrom::End(0))?;
        self.file.seek_from(SeekFrom::Start(0))?; // Seek back to start for reading.

        self.index.clear(); // Rebuilding from scratch.

        while current_offset < file_size {
            let record_start_offset = current_offset;

            // The `parse_key_value_from_reader` will read exactly one entry from the file.
            match parse_key_value_from_reader(&mut self.file) {
                Ok(kv) => {
                    let buffer = kv.to_buffer();
                    let record_len = buffer.len() as u64;

                    if kv.tombstone {
                        // This is a delete marker. The latest entry for a key wins,
                        // so if we see a tombstone, we remove it from our index.
                        self.index.remove(&kv.key);
                    } else {
                        // This is a regular entry. Insert or update the index.
                        self.index.insert(
                            kv.key,
                            (record_start_offset, record_len, false, kv.timestamp),
                        );
                    }
                    current_offset += record_len;
                }
                Err(ref e) if e.kind() == io::ErrorKind::UnexpectedEof => {
                    // We've reached the end of the file, which is expected.
                    break;
                }
                Err(e) => {
                    // An actual error occurred.
                    eprintln!("Error reading log file during startup: {}", e);
                    return Err(Box::new(e));
                }
            }
        }

        // After reading the log, the file cursor must be at the end
        // so that new writes are appended correctly.
        self.file.seek_from(SeekFrom::End(0))?;
        Ok(())
    }

    fn cleanup_expired_keys(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        print!("Performing the clean up process....");
        let current_time = chrono::Utc::now().timestamp();
        self.index
            .retain(|_, (_, _, _, timestamp)| match timestamp {
                Some(ts) => *ts > current_time as u64,
                None => true,
            });
        print!("Ended the clean up process....");
        Ok(())
    }

    /// Lists all active key-value pairs and their timestamps.
    fn list_all(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        println!("\n--- All Key-Value Pairs ---");
        if self.index.is_empty() {
            println!("(No data in the database)");
            return Ok(());
        }

        // Collect the keys and timestamps into a temporary vector to avoid borrow checker errors.
        let items_to_list: Vec<_> = self.index.iter().map(|(key, &(_, _, _, ts))| (key.clone(), ts)).collect();

        println!("---------------------------");
        // Iterate over the independent vector.
        for (key, timestamp_opt) in items_to_list {
            // Get the value for the key.
            let value = self.read(&key)?.unwrap_or_default();
            
            // --- THIS IS THE CORRECTED LOGIC ---
            let formatted_timestamp = if let Some(ts) = timestamp_opt {
                print!("Raw date is {}", ts);
                // Create a timezone-aware DateTime object from the Unix timestamp.
                // This is safer and part of the core chrono API.
                if let Some(dt) = DateTime::from_timestamp(ts as i64, 0) {
                    // Format the DateTime object into a string.
                    dt.format("%Y-%m-%d %H:%M:%S UTC").to_string()
                } else {
                    "Invalid Timestamp".to_string()
                }
            } else {
                "N/A".to_string()
            };

            println!(
                "  Key: {:>15} | Value: {:>15} | Timestamp: {}",
                String::from_utf8_lossy(&key),
                String::from_utf8_lossy(&value),
                formatted_timestamp
            );
        }
        println!("---------------------------\n");
        Ok(())
    }

    // Test serialization roundtrip
    fn test_timestamp_serialization(&self, timestamp: Option<u64>) -> Result<(), Box<dyn std::error::Error>> {
        println!("=== TIMESTAMP SERIALIZATION TEST ===");
        println!("Input timestamp: {:?}", timestamp);
        
        let test_key = b"test_key";
        let test_value = b"test_value";
        
        // Create KeyValue
        let kv = KeyValue::new(test_key, test_value, timestamp, false, 0);
        println!("KeyValue timestamp after creation: {:?}", kv.timestamp);
        
        // Serialize to buffer
        let buffer = kv.to_buffer();
        println!("Buffer created, length: {}", buffer.len());
        
        // Deserialize from buffer
        match parse_key_value_from_buffer(&buffer) {
            Ok(parsed_kv) => {
                println!("Parsed KeyValue timestamp: {:?}", parsed_kv.timestamp);
                
                if kv.timestamp == parsed_kv.timestamp {
                    println!("✅ Serialization roundtrip SUCCESS");
                } else {
                    println!("❌ Serialization roundtrip FAILED");
                    println!("  Original: {:?}", kv.timestamp);
                    println!("  Parsed:   {:?}", parsed_kv.timestamp);
                }
            }
            Err(e) => {
                println!("❌ Failed to parse buffer: {}", e);
            }
        }
        println!("=== END TEST ===\n");
        Ok(())
    }

}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Hello, welcome to DB created on BitCask paper!...................");
    fs::create_dir_all("bitcask/active")?;
    let name = "bitcask/active/database.txt";
    let file = match open_file_read_write(&name) {
        Ok(mut file) => {
            // Print initial offset
            let initial_offset = file.seek(SeekFrom::End(0))?;
            println!("Initial offset after opening: {}", initial_offset);
            file
        }
        Err(err) => return Err(err.into()),
    };
    let mut sst_storage = SStStorage::new(file);
    // Load data from filesystem into BTree Map which acts as an in-memory.
    sst_storage.load_db_from_disk()?;

    let mut last_cleanup_time = Instant::now();

    println!("Completed the loading of index into memory.....");
    loop {
        println!("\nPlease enter your option to proceed. Press 0 to Quit, 1 to Insert, and 2 to Read a Key");
        let mut option = String::new();

        io::stdin()
            .read_line(&mut option)
            .expect("Failed to read option");

        let option: u32 = match option.trim().parse() {
            Ok(num) => num,
            Err(_) => continue,
        };

        let time_since_last_cleanup = last_cleanup_time.elapsed();
        if time_since_last_cleanup >= Duration::from_secs(60) {
            sst_storage.cleanup_expired_keys()?;
            last_cleanup_time = Instant::now();
        }

        match option {
            0 => {
                break;
            }
            1 => {
                println!("Insert key!");
                let mut key = String::new();
                io::stdin().read_line(&mut key).expect(r#"Failed to read"#);
                println!("Insert Value!");
                let mut value = String::new();
                io::stdin().read_line(&mut value)?;
                let _ = &sst_storage.write(
                    key.trim().as_bytes(),
                    value.trim().as_bytes(),
                    false,
                    Some(generate_timestamp_one_hour_in_future()),
                );
            }
            2 => {
                println!("Read key!");
                let mut key = String::new();
                let _ = io::stdin().read_line(&mut key);
                if let Some(value) = sst_storage.read(key.trim().as_bytes())? {
                    println!("Value: {:?}", String::from_utf8_lossy(&value));
                }
            }
            3 => {
                println!("Update an existing key");
                let mut key = String::new();
                let _ = io::stdin().read_line(&mut key);
                println!("Enter the new value for the key");
                let mut new_value = String::new();
                let _ = io::stdin().read_line(&mut new_value);
                let _ = &sst_storage.update(
                    key.trim().as_bytes(),
                    new_value.trim().as_bytes(),
                    false,
                    Some(generate_timestamp_one_hour_in_future()),
                );
            }
            4 => {
                println!("Remove an existing key. Please enter the key");
                let mut key = String::new();
                let _ = io::stdin().read_line(&mut key);

                // Remove the newline character from the input
                let key = key.trim();
                let _ = sst_storage.delete_key(key.as_bytes());
            }
            5 => {
                let mut rng = rand::thread_rng(); // Initialize the random number generator
                let start_write = Instant::now();
                let mut total_write_time = Duration::new(0, 0);
                for _ in 0..1000 {
                    let key_string = rng.gen_range(1..=1000).to_string().to_string();
                    let key = key_string.as_bytes();
                    let value_string = (3 * key[0] as u64).to_string();
                    let value = value_string.as_bytes();
                    let _ = sst_storage.write(
                        key,
                        value,
                        false,
                        Some(generate_timestamp_one_hour_in_future()),
                    );
                }
                let write_time = start_write.elapsed();
                total_write_time += write_time;
                println!("Write time: {:?}", write_time);
            }
            6 => {
                // Reading random keys and displaying their values
                let mut rng = rand::thread_rng();
                let mut total_read_time = Duration::new(0, 0);
                let start_read = Instant::now();
                for _ in 0..1000 {
                    let random_key_string = rng.gen_range(1..=1000).to_string();
                    let random_key = random_key_string.as_bytes();
                    if let Some(value) = sst_storage.read(random_key)? {
                        println!(
                            "Random key: {:?}, Value: {:?}",
                            String::from_utf8_lossy(&random_key),
                            String::from_utf8_lossy(&value)
                        );
                    } else {
                        println!(
                            "Random key not found: {:?}",
                            String::from_utf8_lossy(&random_key)
                        );
                    }
                }
                let read_time = start_read.elapsed();
                total_read_time += read_time;
                println!("Read time: {:?}", read_time);
            }
            7 => {
                let _ = sst_storage.list_all();
            }
            8 => {
                let _ = test_timestamp_issue();
            }
            9 => {
                let _ = test_corruption();
            }
            10_u32..=u32::MAX => todo!(),
        }
    }
    Ok(())
}

fn generate_timestamp_one_hour_in_future() -> u64 {
    let current_time = Utc::now();
    let one_hour_in_future = current_time + chrono::Duration::minutes(2);
    one_hour_in_future.timestamp() as u64
}

fn open_file_read_write(path: &str) -> Result<File, Error> {
    OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .open(path)
}

// Add this to your main function to test
fn test_timestamp_issue() -> Result<(), Box<dyn std::error::Error>> {
    println!("Testing timestamp serialization...");
    
    // Test with the problematic timestamp
    let test_timestamp = Some(1749763021u64);
    
    let file = std::fs::File::create("test_timestamp.db")?;
    let storage = SStStorage::new(file);
    
    storage.test_timestamp_serialization(test_timestamp)?;
    
    // Clean up
    std::fs::remove_file("test_timestamp.db").ok();
    
    Ok(())
}

// Add this function at the end of src/main.rs

fn test_corruption() -> Result<(), Box<dyn std::error::Error>> {
    let test_file_name = "corruption_test.db";
    // Start with a clean file for a predictable test
    if fs::metadata(test_file_name).is_ok() {
        fs::remove_file(test_file_name)?;
    }

    // --- Step 1: Write a known record ---
    {
        println!("Step 1: Writing a known record to '{}'...", test_file_name);
        let file = open_file_read_write(test_file_name)?;
        let mut sst_storage = SStStorage::new(file);
        let key = b"integrity_check";
        let value = b"this_data_is_good";
        sst_storage.write(key, value, false, None)?;
        println!("Record written successfully.");
    } // `sst_storage` and `file` are dropped here, closing the file.

    // --- Step 2: Manually corrupt the file ---
    {
        println!("Step 2: Corrupting the file by changing one byte...");
        let mut file_to_corrupt = OpenOptions::new().write(true).open(test_file_name)?;

        // Let's corrupt a byte in the middle of the value "this_data_is_good"
        // The value starts after:
        // 1 byte (key_len) + 1 byte (val_len) + 15 bytes (key) = 17 bytes from start
        // Let's change the 'd' in "good" to 'X'. 'd' is at index 10 of the value.
        // So, we seek to offset 17 + 10 = 27
        let corruption_offset = 27;
        file_to_corrupt.seek(SeekFrom::Start(corruption_offset))?;
        file_to_corrupt.write_all(&[b'X'])?; // Corrupt 'd' to 'X'
        println!("File has been corrupted at byte {}!", corruption_offset);
    }

    // --- Step 3 & 4: Attempt to load the corrupted file and observe ---
    println!("Step 3: Attempting to load the corrupted database...");
    let file = open_file_read_write(test_file_name)?;
    let mut sst_storage = SStStorage::new(file);

    // The load_db_from_disk() function will read all records and verify checksums.
    // This call is EXPECTED to fail.
    match sst_storage.load_db_from_disk() {
        Ok(_) => {
            eprintln!("❌ TEST FAILED: The program loaded the corrupted data without error.");
        }
        Err(e) => {
            if e.to_string().contains("Checksum mismatch") || e.to_string().contains("invalid data") {
                println!("✅ TEST PASSED: The program correctly detected data corruption!");
                println!("   Error message was: '{}'", e);
            } else {
                eprintln!("❌ TEST FAILED: The program failed, but not with the expected checksum error.");
                eprintln!("   Error message was: '{}'", e);
            }
        }
    }

    // Clean up the test file
    fs::remove_file(test_file_name)?;
    Ok(())
}