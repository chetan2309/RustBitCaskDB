use std::io::{self, Read};

use dance_of_bytes::KeyValue;

// Improved parse_key_value function
pub fn parse_key_value_from_buffer(buffer: &[u8]) -> io::Result<KeyValue> {
    let mut cursor = std::io::Cursor::new(buffer);
    
    // Read key length (u8)
    let mut key_len_buf = [0u8; 1];
    cursor.read_exact(&mut key_len_buf)?;
    let key_len = key_len_buf[0] as usize;

    // Read value length (u8)
    let mut value_len_buf = [0u8; 1];
    cursor.read_exact(&mut value_len_buf)?;
    let value_len = value_len_buf[0] as usize;

    // Read key
    let mut key = vec![0; key_len];
    cursor.read_exact(&mut key)?;

    // Read value
    let mut value = vec![0; value_len];
    cursor.read_exact(&mut value)?;

    // Read timestamp
    let mut timestamp_buffer = [0u8; 8];
    cursor.read_exact(&mut timestamp_buffer)?;
    let timestamp = Some(u64::from_le_bytes(timestamp_buffer));

    // Read tombstone
    let mut tombstone_buffer = [0u8; 1];
    cursor.read_exact(&mut tombstone_buffer)?;
    let tombstone = tombstone_buffer[0] != 0;

    Ok(KeyValue {
        key,
        value,
        timestamp,
        tombstone,
    })
}