use chrono::Utc;
use dance_of_bytes::{self, KeyValue};
use rust_bit_cask_db::parse_key_value_from_buffer;
use rust_bit_cask_db::parse_key_value_from_reader;
use std::path::Path;
use std::path::PathBuf;
use std::{
    collections::BTreeMap,
    collections::HashMap,
    fs::{self, File, OpenOptions},
    io::{self, Error, Read, Seek, SeekFrom, Write},
    time::{Duration, Instant},
};
mod main_test;

struct IndexEntry {
    file_id: u64,
    offset: u64,
    length: u64,
    timestamp: Option<u64>,
}

struct FileStorage<T: Read + Write + Seek> {
    active_file_id: u64,
    file_handles: HashMap<u64, T>,
}

impl FileStorage<File> {
    fn open(dir: &Path) -> io::Result<Self> {
        let mut file_handles = HashMap::new();
        let paths = list_directory_paths(dir)?;
        let mut active_file_id: u64 = 0;
        for path in paths {
            let file = OpenOptions::new().read(true).write(false).open(&path)?;
            let file_id = path
                .file_stem()
                .and_then(|s| s.to_str())
                .unwrap_or_default();
            let int_file_id = file_id.parse::<u64>().unwrap_or(0);
            if int_file_id > active_file_id {
                active_file_id = int_file_id;
            }
            file_handles.insert(int_file_id, file);
        }
        // Reopen highest file id in write mode
        let active_file_path = dir.join(format!("{}.log", active_file_id));
        let active_file = open_file_read_write(&active_file_path)?;
        file_handles.insert(active_file_id, active_file);
        Ok(FileStorage {
            active_file_id: active_file_id,
            file_handles,
        })
    }
}

struct KeyDir {
    index: HashMap<Vec<u8>, IndexEntry>,
}

fn read_from_file<T: Read + Write + Seek>(
    key: &[u8],
    keydir: &KeyDir,
    storage: &mut FileStorage<T>,
) -> Result<Option<Vec<u8>>, Error> {
    if let Some(index_entry) = keydir.index.get(key) {
        if let Some(file_handle) = storage.file_handles.get_mut(&index_entry.file_id) {
            let mut buffer = vec![0; index_entry.length as usize];
            file_handle.seek(io::SeekFrom::Start(index_entry.offset))?;
            file_handle.read_exact(&mut buffer)?;
            let kv = parse_key_value_from_buffer(&buffer)?;
            Ok(Some(kv.value))
        } else {
            Ok(None)
        }
    } else {
        Ok(None)
    }
}

fn load_db_from_disk<T: Read + Write + Seek>(
    keydir: &mut KeyDir,
    storage: &mut FileStorage<T>,
) -> Result<(), Error> {
    // Load the key directory and file storage from disk
    println!("Loading database from disk...");
    for (file_id, file_handle) in &mut storage.file_handles.iter_mut() {
        let mut current_offset = 0;
        loop {
            match parse_key_value_from_reader(file_handle) {
                Ok(keyvalue) => {
                    let record_len = keyvalue.to_buffer().len() as u64;
                    if keyvalue.tombstone {
                        keydir.index.remove(&keyvalue.key);
                    } else {
                        keydir.index.insert(
                            keyvalue.key,
                            IndexEntry {
                                file_id: *file_id,
                                offset: current_offset,
                                length: record_len,
                                timestamp: keyvalue.timestamp,
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
                    return Err(*Box::new(e));
                }
            }
        }
        println!(
            "Loaded keys from file {}: {:?}",
            file_id,
            keydir.index.keys()
        );
    }
    print!("Loaded keys: ");
    for key in keydir.index.keys() {
        print!("{} ", String::from_utf8_lossy(key));
    }
    println!();
    Ok(())
}

fn write_to_file<T: Read + Write + Seek>(
    key: &[u8],
    value: &[u8],
    mark_as_deleted: bool,
    timestamp: Option<u64>,
    keydir: &mut KeyDir,
    storage: &mut FileStorage<T>,
) -> Result<(), Error> {
    let kv = KeyValue::new(key, value, timestamp, mark_as_deleted, 0);
    let buffer = kv.to_buffer();
    let length = buffer.len() as u64;
    if let Some(file_handle) = storage.file_handles.get_mut(&storage.active_file_id) {
        let offset = file_handle.seek(SeekFrom::End(0))?;
        file_handle.write_all(&buffer)?;
        if !mark_as_deleted {
            keydir.index.insert(
                key.to_vec(),
                IndexEntry {
                    file_id: storage.active_file_id,
                    offset,
                    length,
                    timestamp,
                },
            );
        }
        Ok(())
    } else {
        Err(io::Error::new(
            io::ErrorKind::NotFound,
            "Active file handle not found",
        ))
    }
}

fn update_key_value<T: Read + Write + Seek>(
    key: &[u8],
    new_value: &[u8],
    mark_as_deleted: bool,
    timestamp: Option<u64>,
    keydir: &mut KeyDir,
    storage: &mut FileStorage<T>,
) -> Result<(), Error> {
    // Update the key in the key directory by utilizing existing
    // write_to_file function. We also have to update index with updated value
    let _ = write_to_file(key, new_value, mark_as_deleted, timestamp, keydir, storage);
    Ok(())
}

fn delete_key<T: Read + Write + Seek>(
    key: &[u8],
    keydir: &mut KeyDir,
    storage: &mut FileStorage<T>,
) -> Result<(), Error> {
    // First, check if the key exists in the live index.
    // If it does, we will write a tombstone record to the log and remove it from the index.
    if keydir.index.contains_key(key) {
        write_to_file(key, &[], true, Some(0), keydir, storage)?;
        keydir.index.remove(key);
        Ok(())
    } else {
        println!(
            "Key not found for deletion: {:?}",
            String::from_utf8_lossy(key)
        );
        Err(io::Error::new(
            io::ErrorKind::NotFound,
            "Key Not Found in the index",
        ))
    }
}

fn list_directory_paths(path: &Path) -> io::Result<Vec<PathBuf>> {
    let mut all_paths = Vec::new();
    let entries = fs::read_dir(path)?;
    for entry in entries {
        let entry = entry?;
        let path = entry.path();
        if path.extension().and_then(|s| s.to_str()) == Some("log") {
            all_paths.push(path);
        }
    }
    Ok(all_paths)
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Hello, welcome to DB created on BitCask paper!...................");
    let mut key_dir = KeyDir {
        index: HashMap::new(),
    };

    let mut file_storage = FileStorage::open(Path::new("bitcask/active"))?;
    // We have created object of FileStorage now what to do?
    load_db_from_disk(&mut key_dir, &mut file_storage)?;

    // Can we iterate all keys from load_db_from_disk method
    for key in key_dir.index.keys() {
        println!("Loaded key: {}", String::from_utf8_lossy(key));
    }
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
            // We don't know yet if we need this operation, so just commenting it out.
            // sst_storage.cleanup_expired_keys()?;
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
                write_to_file(
                    key.trim().as_bytes(),
                    value.trim().as_bytes(),
                    false,
                    Some(generate_timestamp_one_hour_in_future()),
                    &mut key_dir,
                    &mut file_storage,
                )?;
            }
            2 => {
                println!("Read key!");
                let mut key = String::new();
                let _ = io::stdin().read_line(&mut key);
                if let Some(value) =
                    read_from_file(key.trim().as_bytes(), &key_dir, &mut file_storage)?
                {
                    println!("Value: {:?}", String::from_utf8_lossy(&value));
                } else {
                    println!(
                        "Key not found: {:?}",
                        String::from_utf8_lossy(key.trim().as_bytes())
                    );
                }
            }
            3 => {
                println!("Update an existing key");
                let mut key = String::new();
                let _ = io::stdin().read_line(&mut key);
                println!("Enter the new value for the key");
                let mut value = String::new();
                let _ = io::stdin().read_line(&mut value);
                update_key_value(
                    key.trim().as_bytes(),
                    value.trim().as_bytes(),
                    false,
                    Some(generate_timestamp_one_hour_in_future()),
                    &mut key_dir,
                    &mut file_storage,
                )?;
            }
            4 => {
                println!("Remove an existing key. Please enter the key");
                let mut key = String::new();
                let _ = io::stdin().read_line(&mut key);

                // Remove the newline character from the input
                let key = key.trim();
                delete_key(key.as_bytes(), &mut key_dir, &mut file_storage)?;
            }
            /*
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
            }*/
            4_u32..=u32::MAX => todo!(),
        }
    }
    Ok(())
}

fn generate_timestamp_one_hour_in_future() -> u64 {
    let current_time = Utc::now();
    let one_hour_in_future = current_time + chrono::Duration::minutes(2);
    one_hour_in_future.timestamp() as u64
}

fn open_file_read_write(path: &PathBuf) -> Result<File, Error> {
    OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .open(path)
}
