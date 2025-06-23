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

    Ok(KeyValue {
        key,
        value,
        timestamp,
        tombstone,
    })
}
#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;
    use dance_of_bytes::KeyValue;

    #[test]
    fn test_parse_key_value_from_buffer_valid_data() {
        // Create a test KeyValue
        let original_kv = KeyValue {
            key: b"test_key".to_vec(),
            value: b"test_value".to_vec(),
            timestamp: Some(1234567890),
            tombstone: false,
        };
        
        // Convert to buffer
        let buffer = original_kv.to_buffer();
        
        // Parse back from buffer
        let parsed_kv = parse_key_value_from_buffer(&buffer).unwrap();
        
        // Verify all fields match
        assert_eq!(parsed_kv.key, original_kv.key);
        assert_eq!(parsed_kv.value, original_kv.value);
        assert_eq!(parsed_kv.timestamp, original_kv.timestamp);
        assert_eq!(parsed_kv.tombstone, original_kv.tombstone);
    }

    #[test]
    fn test_parse_key_value_from_buffer_with_tombstone() {
        // Create a test KeyValue with tombstone set
        let original_kv = KeyValue {
            key: b"deleted_key".to_vec(),
            value: b"".to_vec(), // Empty value for deleted key
            timestamp: Some(9876543210),
            tombstone: true,
        };
        
        // Convert to buffer
        let buffer = original_kv.to_buffer();
        
        // Parse back from buffer
        let parsed_kv = parse_key_value_from_buffer(&buffer).unwrap();
        
        // Verify all fields match
        assert_eq!(parsed_kv.key, original_kv.key);
        assert_eq!(parsed_kv.value, original_kv.value);
        assert_eq!(parsed_kv.timestamp, original_kv.timestamp);
        assert_eq!(parsed_kv.tombstone, original_kv.tombstone);
    }

    #[test]
    fn test_parse_key_value_from_buffer_empty_key_value() {
        // Create a test KeyValue with empty key and value
        let original_kv = KeyValue {
            key: b"".to_vec(),
            value: b"".to_vec(),
            timestamp: Some(0),
            tombstone: false,
        };
        
        // Convert to buffer
        let buffer = original_kv.to_buffer();
        
        // Parse back from buffer
        let parsed_kv = parse_key_value_from_buffer(&buffer).unwrap();
        
        // Verify all fields match
        assert_eq!(parsed_kv.key, original_kv.key);
        assert_eq!(parsed_kv.value, original_kv.value);
        assert_eq!(parsed_kv.timestamp, original_kv.timestamp);
        assert_eq!(parsed_kv.tombstone, original_kv.tombstone);
    }

    #[test]
    fn test_parse_key_value_from_buffer_large_data() {
        // Create a test KeyValue with larger data
        let large_key = vec![b'k'; 200]; // 200 byte key
        let large_value = vec![b'v'; 255]; // 255 byte value (max for u8 length)
        
        let original_kv = KeyValue {
            key: large_key,
            value: large_value,
            timestamp: Some(u64::MAX),
            tombstone: false,
        };
        
        // Convert to buffer
        let buffer = original_kv.to_buffer();
        
        // Parse back from buffer
        let parsed_kv = parse_key_value_from_buffer(&buffer).unwrap();
        
        // Verify all fields match
        assert_eq!(parsed_kv.key, original_kv.key);
        assert_eq!(parsed_kv.value, original_kv.value);
        assert_eq!(parsed_kv.timestamp, original_kv.timestamp);
        assert_eq!(parsed_kv.tombstone, original_kv.tombstone);
    }

    #[test]
    fn test_parse_key_value_from_buffer_insufficient_data() {
        // Test with buffer that's too short
        let short_buffer = vec![5, 10]; // Only key_len and value_len, missing actual data
        
        let result = parse_key_value_from_buffer(&short_buffer);
        assert!(result.is_err());
        assert_eq!(result.err().unwrap().kind(), std::io::ErrorKind::UnexpectedEof);
    }

    #[test]
    fn test_parse_key_value_from_reader_valid_data() {
        // Create a test KeyValue
        let original_kv = KeyValue {
            key: b"reader_key".to_vec(),
            value: b"reader_value".to_vec(),
            timestamp: Some(5555555555),
            tombstone: false,
        };
        
        // Convert to buffer and create cursor
        let buffer = original_kv.to_buffer();
        let mut cursor = Cursor::new(buffer);
        
        // Parse from reader
        let parsed_kv = parse_key_value_from_reader(&mut cursor).unwrap();
        
        // Verify all fields match
        assert_eq!(parsed_kv.key, original_kv.key);
        assert_eq!(parsed_kv.value, original_kv.value);
        assert_eq!(parsed_kv.timestamp, original_kv.timestamp);
        assert_eq!(parsed_kv.tombstone, original_kv.tombstone);
    }

    #[test]
    fn test_parse_key_value_from_reader_with_tombstone() {
        // Create a test KeyValue with tombstone
        let original_kv = KeyValue {
            key: b"tombstone_key".to_vec(),
            value: b"tombstone_value".to_vec(),
            timestamp: Some(1111111111),
            tombstone: true,
        };
        
        // Convert to buffer and create cursor
        let buffer = original_kv.to_buffer();
        let mut cursor = Cursor::new(buffer);
        
        // Parse from reader
        let parsed_kv = parse_key_value_from_reader(&mut cursor).unwrap();
        
        // Verify all fields match
        assert_eq!(parsed_kv.key, original_kv.key);
        assert_eq!(parsed_kv.value, original_kv.value);
        assert_eq!(parsed_kv.timestamp, original_kv.timestamp);
        assert_eq!(parsed_kv.tombstone, original_kv.tombstone);
    }

    #[test]
    fn test_parse_key_value_from_reader_insufficient_data() {
        // Test with reader that has insufficient data
        let short_buffer = vec![3, 5]; // key_len=3, value_len=5, but no actual key/value data
        let mut cursor = Cursor::new(short_buffer);
        
        let result = parse_key_value_from_reader(&mut cursor);
        assert!(result.is_err());
        assert_eq!(result.err().unwrap().kind(), std::io::ErrorKind::UnexpectedEof);
    }

    #[test]
    fn test_parse_key_value_from_reader_empty_reader() {
        // Test with completely empty reader
        let empty_buffer = vec![];
        let mut cursor = Cursor::new(empty_buffer);
        
        let result = parse_key_value_from_reader(&mut cursor);
        assert!(result.is_err());
        assert_eq!(result.err().unwrap().kind(), std::io::ErrorKind::UnexpectedEof);
    }

    #[test]
    fn test_parse_key_value_roundtrip_consistency() {
        // Test that both parsing functions produce identical results
        let original_kv = KeyValue {
            key: b"consistency_test".to_vec(),
            value: b"both_functions_should_match".to_vec(),
            timestamp: Some(7777777777),
            tombstone: false,
        };
        
        let buffer = original_kv.to_buffer();
        
        // Parse with buffer function
        let parsed_from_buffer = parse_key_value_from_buffer(&buffer).unwrap();
        
        // Parse with reader function
        let mut cursor = Cursor::new(buffer);
        let parsed_from_reader = parse_key_value_from_reader(&mut cursor).unwrap();
        
        // Both should produce identical results
        assert_eq!(parsed_from_buffer.key, parsed_from_reader.key);
        assert_eq!(parsed_from_buffer.value, parsed_from_reader.value);
        assert_eq!(parsed_from_buffer.timestamp, parsed_from_reader.timestamp);
        assert_eq!(parsed_from_buffer.tombstone, parsed_from_reader.tombstone);
        
        // And both should match the original
        assert_eq!(parsed_from_buffer.key, original_kv.key);
        assert_eq!(parsed_from_buffer.value, original_kv.value);
        assert_eq!(parsed_from_buffer.timestamp, original_kv.timestamp);
        assert_eq!(parsed_from_buffer.tombstone, original_kv.tombstone);
    }
}
