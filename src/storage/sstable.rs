use std::path::Path;
use bson::DateTime;
use bson::oid::ObjectId;
use serde::{Deserialize, Serialize};
use anyhow::{anyhow, Result};
use bloom::{BloomFilter, ASMS};
use core::cmp::Ordering;

use crate::storage::record::*;
use crate::storage::conf::*;


/// A handle that stores the location of an SSTable on disk as 
/// well as some metadata.
pub struct SSTableHandle {
    /// The metadata for this SSTable.
    pub meta: SSTableMeta,

    /// The path to this SSTable on disk.
    pub path: String,

    /// A flag indicating whether this SSTable is active
    /// and should be considered for reads.
    pub active: bool,
}

impl SSTableHandle {
    /// Creates a new SSTableHandle.
    pub fn new(meta: SSTableMeta, path: &str) -> Self {
        SSTableHandle {
            meta,
            path: path.to_string(),
            active: true,
        }
    }

    pub fn activate(&mut self) {
        self.active = true;
    }

    pub fn deactivate(&mut self) {
        self.active = false;
    }

    /// Reads the SSTable from disk, from `self.path`.
    pub fn read(&self) -> Result<SSTable> {
        let file = std::fs::File::open(&self.path)?;
        let reader = std::io::BufReader::new(file);
        let sstable = bson::from_reader(reader)?;
        Ok(sstable)
    }

    /// Writes the SSTable to disk.
    /// 
    /// The data is written to `self.path` as a BSON document.
    pub fn write(&self, sstable: &SSTable) -> Result<()> {
        let file = std::fs::File::create(&self.path)?;
        let writer = std::io::BufWriter::new(file);
        let doc = bson::to_document(sstable)?;
        doc.to_writer(writer)?;
        Ok(())
    }

    /// Deletes the SSTable from disk (at `self.path`).
    pub fn delete(&self) -> Result<()> {
        std::fs::remove_file(&self.path)?;
        Ok(())
    }

    /// Returns a bloom filter for this SSTable.
    pub fn get_bloom_filter(&self) -> Result<BloomFilter> {
        let mut bf = BloomFilter::with_rate(
            BLOOM_FILTER_ERROR_RATE, 
            BLOOM_FILTER_SIZE,
        );
        let sstable = self.read()?;
        for record in sstable.records {
            bf.insert(&record.key);
        }
        Ok(bf)
    }
}

/// An SSTable read from disk.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct SSTable {
    /// The metadata for this SSTable.
    pub meta: SSTableMeta,

    /// The records in this SSTable.
    pub records: Vec<Record>,
}

impl SSTable {
    /// Create a new SSTable from a vector of records.
    pub fn new(records: Vec<Record>) -> Result<Self> {
        // Create a new id...
        let id = ObjectId::new();

        // Get the datetime...
        let created_at = id.timestamp();

        // Get the min/max keys and count from the records...
        let min_key = records
            .first()
            .ok_or(anyhow!("records vec was empty"))?
            .key;
        let max_key = records
            .last()
            .ok_or(anyhow!("records vec was empty"))?
            .key;

        // Create the SSTable...
        Ok(SSTable { 
            meta: SSTableMeta {
                table_id: id,
                created_at,
                min_key,
                max_key,
                num_records: records.len(),
            },
            records,
        })
    }

    /// Get the index of the given key in the SSTable. If the key
    /// isn't in the SSTable, returns None.
    /// 
    /// Note that this uses a binary search, so the records must be sorted
    /// -- which they should be, already. This isn't a requirement of the type
    /// but it is a requirement of the system overall. Additionally, there 
    /// could be an issue if there are multiple records with the same key but,
    /// again, that shouldn't happen based on the system's design.
    pub fn get_index(&self, key: &ObjectId) -> Option<usize> {
        self.records
            .binary_search_by(|record| record.key.cmp(key))
            .ok()
    }

    /// Get the record with the given key from the SSTable. If the key
    /// isn't in the SSTable, returns None.
    pub fn get(&self, key: &ObjectId) -> Option<Record> {
        let i = self.get_index(key)?;
        self.records
            .get(i)
            .cloned()
    }

    /// Get all records in the SSTable with keys in the given range (inclusive).
    pub fn get_range(&self, min_key: &ObjectId, max_key: &ObjectId) -> Vec<Record> {
        // Get the starting point...
        let min_i = match self.get_index(min_key) {
            Some(i) => i,
            None => { return vec![]; },
        };
        
        // Create a vector to store the records...
        let mut records = vec![];

        // Iterate over the records from min_i to the end...
        for i in min_i..self.records.len() {
            let record = &self.records[i];
            if record.key > *max_key {
                break;
            }
            records.push(record.clone());
        }

        // Return the records...
        records
    }

    /// Create a new SSTable by merging this SSTable with another SSTable.
    pub fn merge(&self, other: &SSTable) -> Result<SSTable> {
        // Create a vec to store the merged records...
        let mut records = vec![];

        // Check which SSTable is newer...
        let (newer, older) = if self.meta.created_at > other.meta.created_at {
            (self, other)
        } else {
            (other, self)
        };

        // Create indexes to track the position in each SSTable's records...
        let mut i_newer = 0;
        let mut i_older = 0;

        while i_newer < newer.records.len() && i_older < older.records.len() {
            // Get the records at the current indexes...
            let r_newer = &newer.records[i_newer];
            let r_older = &other.records[i_older];

            // Compare the keys...
            match r_newer.key.cmp(&r_older.key) {
                Ordering::Less => {
                    // r_newer.key < r_older.key
                    records.push(r_newer.clone());
                    i_newer += 1;
                },
                Ordering::Greater => {
                    // r_newer.key > r_older.key
                    records.push(r_older.clone());
                    i_older += 1;
                },
                Ordering::Equal => {
                    // r1.key == r2.key
                    // Add the newer and skip the older...
                    records.push(r_newer.clone());
                    i_newer += 1;
                    i_older += 1;
                },
            }
        }

        // Add any remaining records from self...
        while i_newer < newer.records.len() {
            records.push(newer.records[i_newer].clone());
            i_newer += 1;
        }

        // Add any remaining records from other...
        while i_older < older.records.len() {
            records.push(older.records[i_older].clone());
            i_older += 1;
        }

        // Create the SSTable...
        SSTable::new(records)
    }

    /// Returns a handle for this SSTable.
    /// 
    /// If `write` is true, the SSTable will be written to disk before
    /// returning the handle.
    /// 
    /// # Arguments
    /// 
    /// * `parent_path` - The path to the directory where this SSTable is stored.
    /// * `write` - A flag indicating whether the SSTable should be opened
    /// 
    /// # Returns
    /// 
    /// An `SSTableHandle` for working with this SSTable on disk.
    pub fn get_handle(&self, parent_path: &str, write: bool) -> Result<SSTableHandle> {
        // Format the path...
        let tids = self.meta.table_id.to_string();
        let path = Path::new(parent_path);
        let path = path.join(tids);
        let path = path.to_str()
            .ok_or(anyhow!("Failed to create sstable path"))?;

        // Create the handle...
        let handle = SSTableHandle::new(
            self.meta.clone(), 
            path,
        );

        // If we're writing, write the SSTable to disk...
        if write {
            handle.write(self)?;
        }

        // Return the handle...
        Ok(handle)
    }
}

/// Metadata associated with an SSTable on disk.
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Clone)]
pub struct SSTableMeta {
    /// A unique identifier for this SSTable.
    pub table_id: ObjectId,

    /// The time at which this SSTable was created.
    pub created_at: DateTime,

    /// The minimum key in this SSTable.
    pub min_key: ObjectId,

    /// The maximum key in this SSTable.
    pub max_key: ObjectId,

    /// The number of records in this SSTable.
    pub num_records: usize,
}

impl SSTableMeta {
    /// Returns true if the given key is in the range of this SSTable.
    pub fn key_in_range(&self, key: &ObjectId) -> bool {
        self.min_key <= *key && *key <= self.max_key
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use bson::doc;
    use bson::oid::ObjectId;
    use anyhow::Result;

    #[test]
    fn create_and_read_sstable() -> Result<()> {
        // Create an sstable...
        let records = vec![
            Record {
                key: ObjectId::new(),
                value: Value::Data(doc! {
                    "msg": "Hello, World",
                    "num": 42,
                }),
            },
            Record {
                key: ObjectId::new(),
                value: Value::Data(doc! {
                    "msg": "What's up",
                    "num": 0,
                }),
            },
            Record {
                key: ObjectId::new(),
                value: Value::Tombstone,
            },
        ];
        let sstable = SSTable::new(records).unwrap();
        assert_eq!(sstable.records.len(), 3, "Expected 2 records");

        // Create a handle for the sstable...
        let handle = sstable.get_handle("/tmp", false)?;

        // Write the sstable to disk...
        handle.write(&sstable)?;

        // Read in the sstable...
        let sstable2 = handle.read()?;

        // Check that the sstable is the same...
        assert_eq!(sstable, sstable2, "Expected sstables to be equal");

        // Check that the table is stored where expected...
        let path = Path::new("/tmp")
            .join(handle.meta.table_id.to_string());
        assert!(path.exists(), "Expected path to exist");

        // Delete the sstable...
        handle.delete()?;

        // Check that the table is no longer stored where expected...
        assert!(!path.exists(), "Expected path to not exist");

        Ok(())
    }

    #[test]
    fn sstablemeta_key_in_range() {
        // Create three ObjectIds and ensure they're in order...
        let oid1 = ObjectId::new();
        let oid2 = ObjectId::new();
        let oid3 = ObjectId::new();
        assert!(oid1 < oid2, "Expected object ids out of order");
        assert!(oid2 < oid3, "Expected object ids out of order");

        // Create a meta with oid1 and oid3...
        let meta = SSTableMeta {
            table_id: ObjectId::new(),
            created_at: DateTime::now(),
            min_key: oid1,
            max_key: oid3,
            num_records: 0,
        };
        
        // oid 1, 2, and 3 should be in range...
        assert!(meta.key_in_range(&oid1), "Expected oid1 to be in range");
        assert!(meta.key_in_range(&oid2), "Expected oid2 to be in range");
        assert!(meta.key_in_range(&oid3), "Expected oid3 to be in range");

        // Create a meta with oid1 and oid2...
        let meta = SSTableMeta {
            table_id: ObjectId::new(),
            created_at: DateTime::now(),
            min_key: oid1,
            max_key: oid2,
            num_records: 0,
        };
        
        // oid 1 and 2 should be in range, 3 should not...
        assert!(meta.key_in_range(&oid1), "Expected oid1 to be in range");
        assert!(meta.key_in_range(&oid2), "Expected oid2 to be in range");
        assert!(!meta.key_in_range(&oid3), "Expected oid3 to be out of range");

        // Create a meta with oid2 and oid3...
        let meta = SSTableMeta {
            table_id: ObjectId::new(),
            created_at: DateTime::now(),
            min_key: oid2,
            max_key: oid3,
            num_records: 0,
        };
        
        // oid 2 and 3 should be in range, 1 should not...
        assert!(!meta.key_in_range(&oid1), "Expected oid1 to be out of range");
        assert!(meta.key_in_range(&oid2), "Expected oid2 to be in range");
        assert!(meta.key_in_range(&oid3), "Expected oid3 to be in range");
    }
}


