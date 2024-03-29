use crate::bloom_filter::BloomFilter;
use crate::compaction::IndexWithSizeInBytes;
use crate::consts::{
    DEFAULT_FALSE_POSITIVE_RATE, SIZE_OF_U32, SIZE_OF_U64, SIZE_OF_U8, WRITE_BUFFER_SIZE,
};
use crate::err::StorageEngineError;
//use crate::memtable::val_option::ValueOption;
use crate::storage_engine::SizeUnit;
use chrono::{DateTime, Utc};
use crossbeam_skiplist::SkipMap;
use rand::distributions::Alphanumeric;
use rand::Rng;
use std::cmp;
use StorageEngineError::*;

use std::{hash::Hash, sync::Arc};

pub type SkipMapKey = Vec<u8>;
pub type ValueOffset = usize;
pub type InsertionTime = u64;
pub type IsDeleted = bool;

#[derive(PartialOrd, PartialEq, Copy, Clone, Debug)]
pub struct Entry<K: Hash + PartialOrd, V> {
    pub key: K,
    pub val_offset: V,
    pub created_at: u64,
    pub is_tombstone: bool,
}
#[derive(Clone, Debug)]
pub struct InMemoryTable<K: Hash + PartialOrd + cmp::Ord> {
    pub index: Arc<SkipMap<K, (usize, u64, bool)>>,
    pub bloom_filter: BloomFilter,
    pub false_positive_rate: f64,
    pub size: usize,
    pub size_unit: SizeUnit,
    pub capacity: usize,
    pub created_at: DateTime<Utc>,
    pub read_only: bool,
}

impl IndexWithSizeInBytes for InMemoryTable<Vec<u8>> {
    fn get_index(&self) -> Arc<SkipMap<SkipMapKey, (ValueOffset, InsertionTime, IsDeleted)>> {
        Arc::clone(&self.index)
    }
    fn size(&self) -> usize {
        self.size
    }
    fn find_biggest_key_from_table(&self) -> Result<Vec<u8>, StorageEngineError> {
        self.find_biggest_key()
    }
}

impl Entry<Vec<u8>, usize> {
    pub fn new(key: Vec<u8>, val_offset: usize, created_at: u64, is_tombstone: bool) -> Self {
        Entry {
            key,
            val_offset,
            created_at,
            is_tombstone,
        }
    }

    pub fn has_expired(&self, ttl: u64) -> bool {
        // Current time
        let current_time = Utc::now();
        let current_timestamp = current_time.timestamp_millis() as u64;
        current_timestamp > (self.created_at + ttl)
    }
}

impl InMemoryTable<Vec<u8>> {
    pub fn new() -> Self {
        Self::with_specified_capacity_and_rate(
            SizeUnit::Bytes,
            WRITE_BUFFER_SIZE,
            DEFAULT_FALSE_POSITIVE_RATE,
        )
    }

    pub fn with_specified_capacity_and_rate(
        size_unit: SizeUnit,
        capacity: usize,
        false_positive_rate: f64,
    ) -> Self {
        assert!(
            false_positive_rate >= 0.0,
            "False positive rate can not be les than or equal to zero"
        );
        assert!(capacity > 0, "Capacity should be greater than 0");

        let capacity_to_bytes = size_unit.to_bytes(capacity);
        let avg_entry_size = 100;
        let max_no_of_entries = capacity_to_bytes / avg_entry_size as usize;
        let bf = BloomFilter::new(false_positive_rate, max_no_of_entries);
        let index = SkipMap::new();
        let now: DateTime<Utc> = Utc::now();
        Self {
            index: Arc::new(index),
            bloom_filter: bf,
            size: 0,
            size_unit: SizeUnit::Bytes,
            capacity: capacity_to_bytes,
            created_at: now,
            false_positive_rate,
            read_only: false,
        }
    }

    pub fn insert(&mut self, entry: &Entry<Vec<u8>, usize>) -> Result<(), StorageEngineError> {
        if !self.bloom_filter.contains(&entry.key) {
            self.bloom_filter.set(&entry.key.clone());
            self.index.insert(
                entry.key.to_owned(),
                (entry.val_offset, entry.created_at, entry.is_tombstone),
            );

            // key length + value offset length + date created length
            // it takes 4 bytes to store a 32 bit integer ande 1 byte for tombstone checker
            let entry_length_byte = entry.key.len() + SIZE_OF_U32 + SIZE_OF_U64 + SIZE_OF_U8;
            self.size += entry_length_byte;
            return Ok(());
        }
        // If the key already exist in the bloom filter then just insert into the entry alone
        self.index.insert(
            entry.key.to_owned(),
            (entry.val_offset, entry.created_at, entry.is_tombstone),
        );
        // key length + value offset length + date created length
        // it takes 4 bytes to store a 32 bit integer since 8 bits makes 1 byte
        let entry_length_byte = entry.key.len() + SIZE_OF_U32 + SIZE_OF_U64 + SIZE_OF_U8;
        self.size += entry_length_byte;
        Ok(())
    }

    pub fn get(&self, key: &Vec<u8>) -> Result<Option<(usize, u64, bool)>, StorageEngineError> {
        if self.bloom_filter.contains(key) {
            if let Some(entry) = self.index.get(key) {
                return Ok(Some(*entry.value())); // returns value offset
            }
        }
        Ok(None)
    }

    pub fn update(&mut self, entry: &Entry<Vec<u8>, usize>) -> Result<(), StorageEngineError> {
        if !self.bloom_filter.contains(&entry.key) {
            return Err(KeyNotFoundInMemTable);
        }
        // If the key already exist in the bloom filter then just insert into the entry alone
        self.index.insert(
            entry.key.to_vec(),
            (entry.val_offset, entry.created_at, entry.is_tombstone),
        );
        Ok(())
    }

    pub fn upsert(&mut self, entry: &Entry<Vec<u8>, usize>) -> Result<(), StorageEngineError> {
        self.insert(&entry)
    }

    pub fn generate_table_id() -> Vec<u8> {
        let rng = rand::thread_rng();
        let id: String = rng
            .sample_iter(&Alphanumeric)
            .take(10)
            .map(char::from)
            .collect();
        id.as_bytes().to_vec()
    }

    pub fn delete(&mut self, entry: &Entry<Vec<u8>, usize>) -> Result<(), StorageEngineError> {
        if !self.bloom_filter.contains(&entry.key) {
            return Err(KeyNotFoundInMemTable);
        }
        let created_at = Utc::now();
        // Insert thumb stone to indicate deletion
        self.index.insert(
            entry.key.to_vec(),
            (
                entry.val_offset,
                created_at.timestamp_millis() as u64,
                entry.is_tombstone,
            ),
        );
        Ok(())
    }

    pub fn is_full(&mut self, key_len: usize) -> bool {
        self.size + key_len + SIZE_OF_U32 + SIZE_OF_U64 + SIZE_OF_U8 >= self.capacity()
    }

    // Find the biggest element in the skip list
    pub fn find_biggest_key(&self) -> Result<Vec<u8>, StorageEngineError> {
        let largest_entry = self.index.iter().next_back();
        match largest_entry {
            Some(e) => return Ok(e.key().to_vec()),
            None => Err(BiggestKeyIndexError),
        }
    }
    pub fn false_positive_rate(&mut self) -> f64 {
        self.false_positive_rate
    }
    pub fn size(&mut self) -> usize {
        self.size
    }

    pub fn get_index(self) -> Arc<SkipMap<Vec<u8>, (usize, u64, bool)>> {
        self.index.clone()
    }

    pub fn get_bloom_filter(&self) -> BloomFilter {
        self.bloom_filter.clone()
    }

    pub fn capacity(&mut self) -> usize {
        self.capacity
    }

    pub fn size_unit(&mut self) -> SizeUnit {
        self.size_unit
    }

    pub fn range() {}

    /// Clears all key-value entries in the MemTable.
    pub fn clear(&mut self) {
        let capacity_to_bytes = self.size_unit.to_bytes(self.capacity);
        let avg_entry_size = 100;
        let max_no_of_entries = capacity_to_bytes / avg_entry_size as usize;

        self.index.clear();
        self.size = 0;
        self.bloom_filter = BloomFilter::new(self.false_positive_rate, max_no_of_entries);
    }
}

// #[cfg(test)]
// mod tests {

//     use super::*;

//     #[test]
//     fn test_new() {
//         let mem_table = InMemoryTable::new();
//         assert_eq!(mem_table.capacity, 1 * 1024);
//         assert_eq!(mem_table.size, 0);
//     }

//     #[test]
//     fn test_insert() {
//         let mut mem_table = InMemoryTable::new();
//         assert_eq!(mem_table.capacity, 1 * 1024);
//         assert_eq!(mem_table.size, 0);
//         let k1 = &vec![1, 2, 3, 4];
//         let k2 = &vec![5, 6, 7, 8];
//         let k3 = &vec![10, 11, 12, 13];

//         let _ = mem_table.insert(k1, 10);
//         assert_eq!(mem_table.size, k1.len() + 4);

//         let prev_size = mem_table.size;
//         let _ = mem_table.insert(k2, 10);
//         assert_eq!(mem_table.size, prev_size + k2.len() + 4);

//         let prev_size = mem_table.size;
//         let _ = mem_table.insert(k3, 10);
//         assert_eq!(mem_table.size, prev_size + k3.len() + 4);
//     }

//     // this tests what happens when multiple keys are written consurrently
//     // NOTE: handling thesame keys written at thesame exact time will be handled at the concurrency level(isolation level)
//     #[test]
//     fn test_concurrent_write() {
//         let mem_table = Arc::new(Mutex::new(InMemoryTable::new()));
//         let mut handlers = Vec::with_capacity(5 as usize);

//         for i in 0..5 {
//             let m = mem_table.clone();
//             let handler = thread::spawn(move || {
//                 m.lock().unwrap().insert(&vec![i], i as u32).unwrap();
//             });
//             handlers.push(handler)
//         }

//         for handler in handlers {
//             handler.join().unwrap();
//         }
//         assert_eq!(mem_table.lock().unwrap().get(&vec![0]).unwrap().unwrap(), 0);
//         assert_eq!(mem_table.lock().unwrap().get(&vec![1]).unwrap().unwrap(), 1);
//         assert_eq!(mem_table.lock().unwrap().get(&vec![2]).unwrap().unwrap(), 2);
//         assert_eq!(mem_table.lock().unwrap().get(&vec![3]).unwrap().unwrap(), 3);
//         assert_eq!(mem_table.lock().unwrap().get(&vec![4]).unwrap().unwrap(), 4);
//     }

//     //test get
//     #[test]
//     fn test_get() {
//         let mut mem_table = InMemoryTable::new();
//         let k1 = &vec![1, 2, 3, 4];
//         let k2 = &vec![5, 6, 7, 8];
//         let k3 = &vec![10, 11, 12, 13];
//         let k4 = &vec![19, 11, 12, 13];
//         let _ = mem_table.insert(k1, 10);
//         let _ = mem_table.insert(k2, 11);
//         let _ = mem_table.insert(k3, 12);

//         assert_eq!(*mem_table.index.get(k1).unwrap().value(), 10);
//         assert_eq!(*mem_table.index.get(k2).unwrap().value(), 11);
//         assert_eq!(*mem_table.index.get(k3).unwrap().value(), 12);

//         assert_eq!(mem_table.bloom_filter.contains(k4), false);
//     }
//     // test latest will be returned
//     #[test]
//     fn test_return_latest_value() {
//         let mut mem_table = InMemoryTable::new();
//         let k = &vec![1, 2, 3, 4];

//         let _ = mem_table.insert(k, 10);
//         let _ = mem_table.insert(k, 11);
//         let _ = mem_table.insert(k, 12);

//         //expect latest value to be returned
//         assert_eq!(mem_table.get(k).unwrap().unwrap(), 12);
//     }

//     //test update
//     #[test]
//     fn test_update() {
//         let mut mem_table = InMemoryTable::new();
//         let k = &vec![1, 2, 3, 4];

//         let _ = mem_table.insert(k, 10);
//         let _ = mem_table.update(k, 11);
//         //expect latest value to be returned
//         assert_eq!(mem_table.get(k).unwrap().unwrap(), 11);

//         let unknown_key = &vec![0, 0, 0, 0];
//         assert!(mem_table.update(unknown_key, 10).is_err());
//     }

//     #[test]
//     fn test_upsert() {
//         let mut mem_table = InMemoryTable::new();
//         let k = &vec![1, 2, 3, 4];

//         let _ = mem_table.insert(k, 10);
//         let _ = mem_table.upsert(k, 11);
//         //expect latest value to be returned
//         assert_eq!(mem_table.get(k).unwrap().unwrap(), 11);

//         let new_key = &vec![5, 6, 7, 8];
//         mem_table.upsert(new_key, 14).unwrap();
//         //expect new key to be inserted if key does not already exist
//         assert_eq!(mem_table.get(new_key).unwrap().unwrap(), 14);
//     }

//     #[test]
//     fn test_delete() {
//         let mut mem_table = InMemoryTable::new();
//         let k = &vec![1, 2, 3, 4];

//         let _ = mem_table.insert(k, 10);
//         //expect latest value to be returned
//         assert_eq!(mem_table.get(k).unwrap().unwrap(), 10);
//         let _ = mem_table.delete(k);
//         assert_eq!(mem_table.get(k).unwrap().unwrap(), THUMB_STONE);
//     }
// }
