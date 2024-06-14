use chrono::prelude::*;
use rand::Rng;
use std::{
    collections::BTreeMap,
    fs::{self, File, OpenOptions},
    io::{self, Error, Read, Seek, SeekFrom, Write},
    time::{Duration, Instant},
};
mod main_test;
struct KeyValue {
    key: Vec<u8>,
    value: Vec<u8>,
}

struct SStStorage {
    index: BTreeMap<Vec<u8>, (u64, u64, bool, Option<i64>)>,
    file: File,
}

impl SStStorage {
    fn new(file: File) -> Self {
        SStStorage {
            index: BTreeMap::new(),
            file,
        }
    }

    fn insert_key(&mut self, key: Vec<u8>, value: (u64, u64, bool, Option<i64>)) {
        self.index.insert(key, value);
    }

    fn write(
        &mut self,
        key_value: KeyValue,
        mark_as_deleted: bool,
        timestamp: Option<i64>,
    ) -> Result<(), Error> {
        let _ = self.file.write_all(&key_value.key);
        let offset = self.file.seek(SeekFrom::End(0))?;
        // let value_offset = file_handler.metadata()?.len();
        let _ = self.file.write_all(&key_value.value);
        let length = key_value.value.len() as u64;
        self.insert_key(key_value.key, (offset, length, mark_as_deleted, timestamp));
        Ok(())
    }

    fn read(&mut self, key: Vec<u8>) -> Result<Option<Vec<u8>>, Error> {
        if let Some((value_offset, length, _, _)) = self.index.get(&key) {
            let mut buffer = vec![0; *length as usize];
            self.file.seek(io::SeekFrom::Start(*value_offset))?;
            self.file.read_exact(&mut buffer)?;
            Ok(Some(buffer))
        } else {
            Ok(None)
        }
    }

    fn update(
        &mut self,
        key: Vec<u8>,
        updated_value: Vec<u8>,
        mark_as_deleted: bool,
        timestamp: Option<i64>,
    ) -> Result<(), Error> {
        println!("Reading: key before if else={:?} ", key);
        // Key has to be searched in hashmap
        if let Some((_, _, _, _)) = self.index.get(&key) {
            println!("Reading: key={:?} ", key);
            let _ = self.write(
                KeyValue {
                    key,
                    value: updated_value,
                },
                mark_as_deleted,
                timestamp,
            );
        }
        Ok(())
    }

    fn delete_key(&mut self, key: Vec<u8>) -> Result<(), Error> {
        if let Some((value, _, _, _)) = self.index.get(&key) {
            // Mark the key as deleted by deleting it from BTreeMap and also adding
            // a value in append log, so that it can be deleted from next reload
            let current_value = value.to_be_bytes().to_vec();
            let _ = self.write(
                KeyValue {
                    key: key.to_vec(),
                    value: current_value,
                },
                true,
                None,
            );
            self.index.remove(&key);
        }
        Ok(())
    }

    fn save_index(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        fs::create_dir_all("bitcask/index")?;
        let name = "bitcask/index/index.bin";
        let mut file = match open_file_read_write(&name) {
            Ok(file) => file,
            Err(err) => return Err(err.into()),
        };
        bincode::serialize_into(&mut file, &self.index)?;
        Ok(())
    }

    fn load_db_from_disk(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let index_file = open_file_read_only("bitcask/index/index.bin")?;
        let as_is_db: BTreeMap<Vec<u8>, (u64, u64, bool, Option<i64>)> =
            bincode::deserialize_from(&index_file)?;
        self.index = as_is_db
            .into_iter()
            .filter(|(_, (_, _, deleted, _))| !deleted)
            .collect();
        Ok(())
    }

    fn cleanup_expired_keys(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        print!("Performing the clean up process....");
        let current_time = chrono::Utc::now().timestamp();
        self.index
            .retain(|_, (_, _, _, timestamp)| match timestamp {
                Some(ts) => *ts > current_time,
                None => true,
            });
        print!("Ended the clean up process....");
        Ok(())
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Hello, welcome to DB created on BitCask paper!...................");
    fs::create_dir_all("bitcask/active")?;
    let name = "bitcask/active/database.txt";
    let file = match open_file_read_write(&name) {
        Ok(file) => file,
        Err(err) => return Err(err.into()),
    };
    let mut sst_storage = SStStorage::new(file);
    // Load data from filesystem into BTree Map which acts as an in-memory.
    sst_storage.load_db_from_disk()?;

    let mut last_cleanup_time = Instant::now();

    println!("Completed the loading of index into memory.....");
    loop {
        println!("Please enter your option to proceed. Press 0 to Quit, 1 to Insert, and 2 to Read a Key");
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
                // On quit we would want to save current Key's and offset mapping from BTreeMap
                // to a file. So that on next start we could read offset's from this file.
                // Although, we can calculate offset's on the fly. However, later when we would
                // have a large no of records, this file will help us not include tombstone entries
                // into our in-memory KV pairs.
                sst_storage.save_index()?;
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
                    KeyValue {
                        key: key.trim().as_bytes().to_vec(),
                        value: value.trim().as_bytes().to_vec(),
                    },
                    false,
                    Some(generate_timestamp_one_hour_in_future()),
                );
            }
            2 => {
                println!("Read key!");
                let mut key = String::new();
                let _ = io::stdin().read_line(&mut key);
                if let Some(value) = sst_storage.read(key.trim().as_bytes().to_vec())? {
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
                    key.trim().as_bytes().to_vec(),
                    new_value.trim().as_bytes().to_vec(),
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
                let _ = sst_storage.delete_key(key.as_bytes().to_vec());
            }
            5 => {
                let mut rng = rand::thread_rng(); // Initialize the random number generator
                let start_write = Instant::now();
                let mut total_write_time = Duration::new(0, 0);
                for _ in 0..1000 {
                    let key = rng.gen_range(1..=1000).to_string().as_bytes().to_vec();
                    let value = (3 * key[0] as u64).to_string().as_bytes().to_vec();
                    let _ = sst_storage.write(
                        KeyValue { key, value },
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
                    let random_key = rng.gen_range(1..=1000).to_string().as_bytes().to_vec();
                    if let Some(value) = sst_storage.read(random_key.clone())? {
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
            7_u32..=u32::MAX => todo!(),
        }
    }
    Ok(())
}

fn generate_timestamp_one_hour_in_future() -> i64 {
    let current_time = Utc::now();
    let one_hour_in_future = current_time + chrono::Duration::minutes(2);
    one_hour_in_future.timestamp()
}

fn open_file_read_only(path: &str) -> Result<File, Error> {
    File::open(path)
}

fn open_file_read_write(path: &str) -> Result<File, Error> {
    OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .open(path)
}
