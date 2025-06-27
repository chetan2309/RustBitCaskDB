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

    // Read checksum
    let mut checksum_buffer = [0u8; 4];
    cursor.read_exact(&mut checksum_buffer)?;
    let checksum_from_file = u32::from_le_bytes(checksum_buffer);

    let mut kv = KeyValue {
        key,
        value,
        timestamp,
        tombstone,
        checksum: checksum_from_file
    };
    // Calculate the checksum
    let calculated_checksum = kv.calculate_checksum();
    if calculated_checksum != checksum_from_file {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "Checksum mismatch",
        ));
    }
    println!("Calculated checksum {} checksum_from_file {}", calculated_checksum, checksum_from_file);
    kv.checksum = calculated_checksum;
    Ok(kv)
}

pub fn parse_key_value_from_reader<R: Read>(reader: &mut R) -> io::Result<KeyValue> {
    // Read key length (u8)
    let mut key_len_buf = [0u8; 1];
    reader.read_exact(&mut key_len_buf)?;
    let key_len = key_len_buf[0] as usize;

    // Read value length (u8)
    let mut value_len_buf = [0u8; 1];
    reader.read_exact(&mut value_len_buf)?;
    let value_len = value_len_buf[0] as usize;

    // Read key
    let mut key = vec![0; key_len];
    reader.read_exact(&mut key)?;

    // Read value
    let mut value = vec![0; value_len];
    reader.read_exact(&mut value)?;

    // Read timestamp
    let mut timestamp_buffer = [0u8; 8];
    reader.read_exact(&mut timestamp_buffer)?;
    let timestamp = Some(u64::from_le_bytes(timestamp_buffer));

    // Read tombstone
    let mut tombstone_buffer = [0u8; 1];
    reader.read_exact(&mut tombstone_buffer)?;
    let tombstone = tombstone_buffer[0] != 0;

    // Read checksum
    let mut checksum_buffer = [0u8; 4];
    reader.read_exact(&mut checksum_buffer)?;
    let checksum_from_file = u32::from_le_bytes(checksum_buffer);

    let mut kv = KeyValue {
        key,
        value,
        timestamp,
        tombstone,
        checksum: checksum_from_file
    };
    // Calculate the checksum
    let calculated_checksum = kv.calculate_checksum();
    if calculated_checksum != checksum_from_file {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "Checksum mismatch",
        ));
    }
    kv.checksum = calculated_checksum;
    Ok(kv)
}
