use std::{
    collections::BTreeMap,
    fs,
    io::{self, Error, Read, Seek, Write, SeekFrom},
};

struct KeyValue {
    key: Vec<u8>,
    value: Vec<u8>,
}

struct SStStorage {
    index: BTreeMap<Vec<u8>, (u64, u64)>,
}

impl SStStorage {
    fn insert_key(&mut self, key: Vec<u8>, value: (u64, u64)) {
        self.index.insert(key, value);
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Hello, welcome to DB created on BitCask paper!...................");
    fs::create_dir_all("bitcask/active")?;
    let name = "bitcask/active/database.txt";
    let mut file = match fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .open(name)
    {
        Ok(file) => file,
        Err(err) => return Err(err.into()),
    };
    let mut sst_storage = SStStorage {
        index: BTreeMap::new(),
    };
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
                save_index();
            }
            1 => {
                println!("Insert key!");
                let mut key = String::new();
                io::stdin().read_line(&mut key).expect(r#"Failed to read"#);
                println!("Insert Value!");
                let mut value = String::new();
                io::stdin().read_line(&mut value)?;
                let _ = write(
                    &mut file,
                    &mut sst_storage,
                    KeyValue {
                        key: key.trim().as_bytes().to_vec(),
                        value: value.trim().as_bytes().to_vec(),
                    },
                );
            }
            2 => {
                println!("Read key!");
                let mut key = String::new();
                let _ = io::stdin().read_line(&mut key);
                if let Some(value) =
                    read(&mut file, &mut sst_storage, key.trim().as_bytes().to_vec())?
                {
                    println!("Value: {:?}", String::from_utf8_lossy(&value));
                }
            }
            3_u32..=u32::MAX => todo!(),
        }
    }

    fn write(
        file_handler: &mut fs::File,
        sst_storage: &mut SStStorage,
        key_value: KeyValue,
    ) -> Result<(), Error> {
        let current_offset = file_handler.seek(SeekFrom::End(0))?;
        println!("Reading: offset={:?} to the end of the file", current_offset);
        let _ = file_handler.write_all(&key_value.key);
        let offset = file_handler.seek(SeekFrom::End(0))?;
        // let value_offset = file_handler.metadata()?.len();
        println!("Reading: offset before writing value to the file={:?} to the end of the file", offset);
        let _ = file_handler.write_all(&key_value.value);
        let length = key_value.value.len() as u64;
        sst_storage.insert_key(key_value.key, (offset, length));
        Ok(())
    }

    fn read(
        file_handler: &mut fs::File,
        sst_storage: &mut SStStorage,
        key: Vec<u8>,
    ) -> Result<Option<Vec<u8>>, Error> {
        if let Some((value_offset, length)) = sst_storage.index.get(&key) {
            println!("Reading: key={:?} offset={:?} length={:?}", key, value_offset, length);
            let file_length = file_handler.metadata()?.len();
            println!("Read: {:?}", file_length);
            let mut buffer = vec![0; *length as usize];
            file_handler.seek(io::SeekFrom::Start(*value_offset))?;
            file_handler.read_exact(&mut buffer)?;
            println!("Read: {:?}", buffer);
            Ok(Some(buffer))
        } else {
            Ok(None)
        }
    }

    fn save_index() -> Result<(), Box<dyn std::error::Error>> {
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
        Ok(())
    }
}
