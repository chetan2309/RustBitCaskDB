use chrono::{DateTime, Utc};
use dance_of_bytes::{self, KeyValue};
use rand::Rng;
use rust_bit_cask_db::parse_key_value_from_buffer;
use rust_bit_cask_db::parse_key_value_from_reader;
use std::collections::HashMap;
use std::path::Path;
use std::path::PathBuf;
use std::{
    collections::BTreeMap,
    fs::{self, File, OpenOptions},
    io::{self, Error, Read, Seek, SeekFrom, Write},
    time::{Duration, Instant},
};
mod main_test;

struct IndexEntry {
    file_id: u32,
    offset: u64,
    length: u64,
    timestamp: Option<u64>,
}

struct SStStorage<T: Read + Write + Seek> {
    index: BTreeMap<Vec<u8>, IndexEntry>,
    file: T,
    active_file_id: u32,
    file_paths: HashMap<u32, PathBuf>
}

trait Storage: Read + Write + Seek {
    fn open(path: &str) -> Result<Self, Error> where Self: Sized;
}

impl Storage for File {
    fn open(path: &str) -> Result<Self, Error> {
        open_file_read_write(path)
    }
}

impl<T: Storage> SStStorage<T> {
    fn new(file: T, file_id: u32, path: PathBuf, active_file_id: u32) -> Self {
        let mut file_paths = HashMap::new();
        file_paths.insert(file_id, path);
        SStStorage {
            index: BTreeMap::new(),
            file,
            active_file_id,
            file_paths,
        }
    }

    fn open(dir: &Path) -> Result<Self, Error> {
        let paths =  match list_directory_paths(dir) {
            Ok(p) => p,
            Err(e) => {
                eprintln!("Error reading directory: {}", e);
                return Err(e.into());
            }
        };
            
        let mut file_ids_and_name = paths.iter().filter_map(|item| {
            let file_ids: u32 = item.file_stem()?.to_str()?.parse().ok()?;
            Some((file_ids, item.to_str()?.to_string()))       
        }).collect::<Vec<(u32, String)>>();
        file_ids_and_name.sort();
        let (file_id, name) = match file_ids_and_name.last() {
            Some((id, name)) => (*id, name.clone()),
            None => (0, format!("{}/0.log", dir.to_str().unwrap_or("bitcask/active"))),
        };

        let file = T::open(&name)?;
        let mut file_paths = HashMap::new();
        for (id, path) in file_ids_and_name {
            file_paths.insert(id, PathBuf::from(path));
        }

        // Adding this explicit line to add 0.log when it doesn't exist in the HasMap
        file_paths.insert(file_id, PathBuf::from(&name));
        
        let mut storage = SStStorage {
            index: BTreeMap::new(),
            file,
            active_file_id: file_id,
            file_paths,
        };
        storage.load_db_from_disk()?;
        Ok(storage)
    }

    fn rotate_file(&mut self) -> Result<(), Error> {
        self.file.flush()?;
        // Logic to rotate the file when max size is reached
        let updated_active_file_id = self.active_file_id + 1;
        let new_file_name = format!("bitcask/active/{}.log", updated_active_file_id);
        let new_file = T::open(&new_file_name)?;
        self.active_file_id = updated_active_file_id;
        self.file_paths
            .insert(updated_active_file_id, PathBuf::from(new_file_name));
        self.file = new_file;
        Ok(())
    }

    fn insert_key(&mut self, key: Vec<u8>, value: IndexEntry) {
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
        let current_offset = self.file.seek(SeekFrom::End(0))?;
        let length = buffer.len() as u64;
        
        const MAX_FILE_SIZE: u64 = 1024 * 1024;
        if current_offset + length > MAX_FILE_SIZE {
            // Logic to handle file rotation can be added here.
            // For simplicity, we will just print a message.
            println!("Max file size reached. File rotation logic should be implemented.");
            self.rotate_file()?;
        }
        self.file.write(&buffer)?;
        
        // Only update the in-memory index for new or updated keys, not for deletions.
        if !mark_as_deleted {
            self.insert_key(
                key.to_vec(),
                IndexEntry {
                    file_id: self.active_file_id,
                    offset: current_offset,
                    length,
                    timestamp,
                },
            );
        }
        Ok(())
    }

    fn read(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>, Error> {
        if let Some(index_entry) = self.index.get(key) {
            println!("{:?}", self.file_paths);
            let path = self.file_paths.get(&index_entry.file_id)
                .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "File path not found"))?;
            let mut file = File::open(path)?;
            let mut buffer = vec![0; index_entry.length as usize];
            file.seek(io::SeekFrom::Start(index_entry.offset))?;
            file.read(&mut buffer)?;
            let kv = parse_key_value_from_buffer(&buffer)?;
            Ok(Some(kv.value))
        } else {
            Ok(None)
        }
    }

    fn update(
        &mut self,
        key: &[u8],
        updated_value: &[u8],
        timestamp: Option<u64>,
    ) -> Result<(), Error> {
        // Key has to be searched in hashmap
        if self.index.contains_key(key) {
            println!("Reading: key={:?} ", key);
            self.write(key, updated_value, false, timestamp)?;
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

    fn load_db_from_disk(&mut self) -> Result<(), io::Error>
    where
        T: std::io::Read,
    {
        // Seek to the beginning of the active database file to read all entries.
        let mut current_offset = self.file.seek(SeekFrom::Start(0))?;
        let file_size = self.file.seek(SeekFrom::End(0))?;
        self.file.seek(SeekFrom::Start(0))?; // Seek back to start for reading.

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
                            IndexEntry {
                                file_id: self.active_file_id,
                                offset: record_start_offset,
                                length: record_len,
                                timestamp: kv.timestamp,
                            },
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
                    return Err(e);
                }
            }
        }

        // After reading the log, the file cursor must be at the end
        // so that new writes are appended correctly.
        self.file.seek(SeekFrom::End(0))?;
        Ok(())
    }

    fn cleanup_expired_keys(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        print!("Performing the clean up process....");
        let current_time = chrono::Utc::now().timestamp();
        self.index
            .retain(|_, index_entry| match index_entry.timestamp {
                Some(ts) => ts > current_time as u64,
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
        let items_to_list: Vec<_> = self
            .index
            .iter()
            .map(|(key, entry)| (key.clone(), entry.timestamp))
            .collect();

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
    fn test_timestamp_serialization(
        &self,
        timestamp: Option<u64>,
    ) -> Result<(), Box<dyn std::error::Error>> {
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

fn list_directory_paths(path: &Path) -> io::Result<Vec<PathBuf>> {
    let mut all_paths = Vec::new();
    if path.is_dir() {
        for entry in fs::read_dir(path)? {
            let entry = entry?;
            let path = entry.path();
            if path.extension().and_then(|s| s.to_str()) == Some("log") {
                // print!("Found log file: {:?}\n", path);
                all_paths.push(path);
            }
        }
    }
    all_paths.sort();
    Ok(all_paths)
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Hello, welcome to DB created on BitCask paper!...................");
    fs::create_dir_all("bitcask/active")?;

    let mut sst_storage: SStStorage<File> = SStStorage::open(Path::new("bitcask/active"))?;

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
                for _ in 0..50000 {
                    let key_string = rng.gen_range(1..=50000).to_string().to_string();
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
    let storage = SStStorage::new(file, 0, PathBuf::from("test_timestamp.db"), 0);

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
        let mut sst_storage = SStStorage::new(file, 0, PathBuf::from(test_file_name), 0);
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
    let mut sst_storage = SStStorage::new(file, 0, PathBuf::from(test_file_name), 0);

    // The load_db_from_disk() function will read all records and verify checksums.
    // This call is EXPECTED to fail.
    match sst_storage.load_db_from_disk() {
        Ok(_) => {
            eprintln!("❌ TEST FAILED: The program loaded the corrupted data without error.");
        }
        Err(e) => {
            if e.to_string().contains("Checksum mismatch") || e.to_string().contains("invalid data")
            {
                println!("✅ TEST PASSED: The program correctly detected data corruption!");
                println!("   Error message was: '{}'", e);
            } else {
                eprintln!(
                    "❌ TEST FAILED: The program failed, but not with the expected checksum error."
                );
                eprintln!("   Error message was: '{}'", e);
            }
        }
    }

    // Clean up the test file
    fs::remove_file(test_file_name)?;
    Ok(())
}
