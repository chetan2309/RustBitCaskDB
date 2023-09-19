use rand::Rng;
use std::{
    collections::BTreeMap,
    fs::{self, File},
    io::{self, Error, Read, Seek, SeekFrom, Write}, time::{Duration, Instant},
};
struct KeyValue {
    key: Vec<u8>,
    value: Vec<u8>,
}

struct SStStorage {
    index: BTreeMap<Vec<u8>, (u64, u64, bool)>,
    file: File
}

impl SStStorage {

    fn new(file: File) -> Self {
        SStStorage { 
            index: BTreeMap::new(), 
            file 
        }
    }


    fn insert_key(&mut self, key: Vec<u8>, value: (u64, u64, bool)) {
        self.index.insert(key, value);
    }

    fn write(
        &mut self,
        key_value: KeyValue,
        mark_as_deleted: bool,
    ) -> Result<(), Error> {
        let _ = self.file.write_all(&key_value.key);
        let offset = self.file.seek(SeekFrom::End(0))?;
        // let value_offset = file_handler.metadata()?.len();
        let _ = self.file.write_all(&key_value.value);
        let length = key_value.value.len() as u64;
        self.insert_key(key_value.key, (offset, length, mark_as_deleted));
        Ok(())
    }

    fn read(
        &mut self,
        key: Vec<u8>,
    ) -> Result<Option<Vec<u8>>, Error> {
        if let Some((value_offset, length, _)) = self.index.get(&key) {
            let mut buffer = vec![0; *length as usize];
            self.file.seek(io::SeekFrom::Start(*value_offset))?;
            self.file.read_exact(&mut buffer)?;
            Ok(Some(buffer))
        } else {
            Ok(None)
        }
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Hello, welcome to DB created on BitCask paper!...................");
    fs::create_dir_all("bitcask/active")?;
    let name = "bitcask/active/database.txt";
    let file = match fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .open(name)
    {
        Ok(file) => file,
        Err(err) => return Err(err.into()),
    };
    let mut sst_storage = SStStorage::new(file);

    if let Ok(index_file) = File::open("bitcask/index/index.bin") {
        let as_is_db: BTreeMap<Vec<u8>, (u64, u64, bool)> = bincode::deserialize_from(&index_file)?;
        for (key, value) in as_is_db {
            if !value.2 {
                sst_storage.index.insert(key, value);
            }
        }
    }
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

        match option {
            0 => {
                // On quit we would want to save current Key's and offset mapping from BTreeMap
                // to a file. So that on next start we could read offset's from this file.
                // Although, we can calculate offset's on the fly. However, later when we would
                // have a large no of records, this file will help us not include tombstone entries
                // into our in-memory KV pairs.
                save_index(&sst_storage)?;
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
                );
            }
            2 => {
                println!("Read key!");
                let mut key = String::new();
                let _ = io::stdin().read_line(&mut key);
                if let Some(value) =
                    sst_storage.read(key.trim().as_bytes().to_vec())?
                {
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

                // Key has to be searched in hashmap
                if let Some((_, _, _)) = sst_storage.index.get(&key.trim().as_bytes().to_vec()) {
                    println!("Reading: key={:?} ", key);
                    let _ = sst_storage.write(
                        KeyValue {
                            key: key.trim().as_bytes().to_vec(),
                            value: new_value.trim().as_bytes().to_vec(),
                        },
                        false,
                    );
                }
            }
            4 => {
                let mut key = String::new();
                let _ = io::stdin().read_line(&mut key);

                // Remove the newline character from the input
                let key = key.trim();

                if let Some((value, _, _)) = sst_storage.index.get(key.as_bytes()) {
                    // Mark the key as deleted by deleting it from BTreeMap and also adding
                    // a value in append log, so that it can be deleted from next reload
                    let current_value = value.to_be_bytes().to_vec();
                    let _ = sst_storage.write(
                        KeyValue {
                            key: key.as_bytes().to_vec(),
                            value: current_value,
                        },
                        true,
                    );
                    sst_storage.index.remove(key.as_bytes());
                }
            }
            5 => {
                let mut rng = rand::thread_rng(); // Initialize the random number generator
                let start_write = Instant::now();
                let mut total_write_time = Duration::new(0, 0);
                for _ in 0..1000 {
                    let key = rng.gen_range(1..=1000).to_string().as_bytes().to_vec();
                    let value = (3 * key[0] as u64).to_string().as_bytes().to_vec();
                    let _ = sst_storage.write(KeyValue { key, value }, false);
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

    fn save_index(sst_storage: &SStStorage) -> Result<(), Box<dyn std::error::Error>> {
        fs::create_dir_all("bitcask/index")?;
        let name = "bitcask/index/index.bin";
        let mut file = match fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(name)
        {
            Ok(file) => file,
            Err(err) => return Err(err.into()),
        };
        let _ = bincode::serialize_into(&mut file, &sst_storage.index);
        Ok(())
    }

    Ok(())
}
