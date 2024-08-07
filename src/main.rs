use chrono::prelude::*;
use dance_of_bytes::{self, KeyValue};
use rand::Rng;
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
        let kv = KeyValue::new(key, value, timestamp, mark_as_deleted);

        // let key_length = key.len() as u8;
        // self.file.write(&[key_length])?;
        // self.file.write(key)?;
        let buffer = kv.to_buffer();
        let offset = self.file.seek_from(SeekFrom::End(0))?;
        // let value_offset = file_handler.metadata()?.len();
        print!("The value of offset is {:?}", offset);
        // let value_length  = value.len() as u8;
        // self.file.write(&[value_length])?;
        // self.file.write(value)?;
        // let length = value.len() as u64;
        let length = buffer.len() as u64;
        self.file.write(&buffer)?;
        self.insert_key(key.to_vec(), (offset, length, mark_as_deleted, timestamp));
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
        if let Some((value, _, _, _)) = self.index.get(key) {
            // Mark the key as deleted by deleting it from BTreeMap and also adding
            // a value in append log, so that it can be deleted from next reload
            let current_value = value.to_be_bytes();
            let _ = self.write(key, &current_value, true, None);
            self.index.remove(key);
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
        let index_path = "bitcask/index/index.bin";
        if fs::metadata(index_path).is_ok() {
            let index_file = open_file_read_only(index_path)?;
            match bincode::deserialize_from(&index_file) {
                Ok(as_is_db) => {
                    let as_is_db: BTreeMap<Vec<u8>, (u64, u64, bool, Option<u64>)> = as_is_db;
                    self.index = as_is_db
                        .into_iter()
                        .filter(|(_, (_, _, deleted, _))| !deleted)
                        .collect();
                }
                Err(e) => {
                    println!("Warning: Failed to load index file. Error: {}. Starting with an empty index.", e);
                    self.index = BTreeMap::new();
                }
            }
        } else {
            println!("Warning: Index file not found. Starting with an empty index.");
            self.index = BTreeMap::new();
        }
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
            7_u32..=u32::MAX => todo!(),
        }
    }
    Ok(())
}

fn generate_timestamp_one_hour_in_future() -> u64 {
    let current_time = Utc::now();
    let one_hour_in_future = current_time + chrono::Duration::minutes(2);
    one_hour_in_future.timestamp() as u64
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
