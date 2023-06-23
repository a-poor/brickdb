use std::collections::BTreeMap;
use bson::{Document, DateTime};
use bson::oid::ObjectId;
use serde::{Deserialize, Serialize};
use anyhow::{anyhow, Result};
use bloom::{BloomFilter, Unionable, ASMS};

/// The maximum number of tables per level in the LSM Tree.
/// 
/// Note: This value is fixed temporarily, for simplicity.
/// The goal is to remove this const and make it configurable.
pub const MAX_TABLES_PER_LEVEL: usize = 10;

/// The maximum number of records to store in the memtable 
/// before flushing to disk. This will also be the max size 
/// of a single SSTable in the first (on-disk) level of 
/// the LSM Tree.
/// 
/// Note: This value is fixed temporarily, for simplicity.
/// The goal is to remove this const and make it configurable.
pub const MEMTABLE_MAX_SIZE: usize = 100;

/// The fixed size of the bloom filter.
/// 
/// See also: [BLOOM_FILTER_ERROR_RATE]
/// 
/// Note: This value is fixed temporarily, for simplicity.
/// The goal is to remove this const and make it configurable.
pub const BLOOM_FILTER_SIZE: u32 = 1000;

/// The fixed error rate for level bloom filters.
/// 
/// See also: [BLOOM_FILTER_SIZE]
/// 
/// Note: This value is fixed temporarily, for simplicity.
/// The goal is to remove this const and make it configurable.
pub const BLOOM_FILTER_ERROR_RATE: f32 = 0.01;

/// The name of the metadata file for a level.
/// 
/// Note: This value is fixed for simplicity. This *may* change
/// or become a configurable option in the future.
pub const LEVEL_META_FILE: &str = "_meta.bson";


/// A struct representing an LSM Tree managing both in-memory and on-disk data.
pub struct LSMTree {
    /// The unique identifier for this LSM Tree.
    pub id: ObjectId,

    /// The name of this LSM Tree.
    pub name: String,

    /// The in-memory buffer for this LSM Tree.
    pub memtable: MemTable,

    /// The on-disk levels for this LSM Tree.
    pub levels: Vec<Level>,

    /// The path to the directory where this LSM Tree's data is stored.
    pub path: String,
}

impl LSMTree {
    /// Creates a new LSM Tree with the given name.
    pub fn new(name: String, path: String) -> Self {
        LSMTree {
            id: ObjectId::new(),
            name,
            memtable: MemTable::new(),
            levels: vec![],
            path,
        }
    }

    /// Inserts a new record into the LSM Tree.
    pub fn insert(&mut self, key: ObjectId, value: Value<Document>) {
        self.memtable.records.insert(key, value);
    }

    /// Set a key to a value in the LSM Tree.
    pub fn set(&mut self, key: ObjectId, doc: Document) {
        self.insert(key, Value::Data(doc));
    }

    /// Delete a key from the LSM Tree.
    pub fn del(&mut self, key: ObjectId) {
        self.insert(key, Value::Tombstone);
    }

    /// Get a value from the LSM Tree's in-memory buffer.
    fn get_from_memtable(&self, key: ObjectId) -> Option<Value<Document>> {
        self.memtable.records
            .get(&key)
            .map(|value| value.clone())
    }
    
    /// Get a value from the LSM Tree's on-disk levels.
    fn get_from_disk(&self, _key: ObjectId) -> Option<Value<Document>> {
        unimplemented!();
    }
    
    /// Get a value from the LSM Tree.
    /// 
    /// This will first check the in-memory buffer, then the on-disk levels.
    pub fn get(&self, key: ObjectId) -> Option<Document> {
        if let Some(value) = self.get_from_memtable(key) {
            return match value {
                Value::Data(doc) => Some(doc),
                Value::Tombstone => None,
            };
        }
        match self.get_from_disk(key) {
            Some(value) => match value {
                Value::Data(doc) => Some(doc),
                Value::Tombstone => None,
            },
            None => None,
        }
    }
}

/// The in-memory buffer for an LSM Tree.
/// 
/// This buffer is comprised of a red-black tree of records, sorted by key.
pub struct MemTable {
    pub records: BTreeMap<ObjectId, Value<Document>>,
}

impl MemTable {
    /// Creates a new MemTable.
    pub fn new() -> Self {
        Self::default()
    }

    /// Flushes the contents of the MemTable to disk, returning an SSTable.
    pub fn flush(&mut self) -> Result<SSTable> {
        // Create a vector of records from the BTreeMap...
        let records: Vec<_> = self.records
            .iter()
            .map(|(key, value)| {
                Record {
                    key: *key,
                    value: value.clone(),
                }
            })
            .collect();

        // Get the min/max keys and count from the records...
        let min_key = records
            .first()
            .ok_or(anyhow!("records vec was empty"))?
            .key;
        let max_key = records
            .last()
            .ok_or(anyhow!("records vec was empty"))?
            .key;
        let num_records = records.len();
        let meta = SSTableMeta {
            id: ObjectId::new(),
            created_at: DateTime::now(),
            min_key,
            max_key,
            num_records,
        };

        // Create and return!
        Ok(SSTable {
            meta,
            records,
        })
    }
}

impl Default for MemTable {
    fn default() -> Self {
        MemTable {
            records: BTreeMap::new(),
        }
    }
}

/// An on-disk level in the LSM Tree, comprised of zero or more SSTables.
pub struct Level {
    /// The metadata for this level.
    pub meta: LevelMeta,

    /// The SSTables in this level.
    pub tables: Vec<SSTableHandle>,

    /// A Bloom filter for this level.
    pub bloom_filter: BloomFilter,
}

impl Level {
    /// Create a new LSM Tree Level.
    pub fn new(meta: LevelMeta, tables: Vec<SSTableHandle>) -> Result<Self> {
        let bloom_filter = BloomFilter::with_rate(
            BLOOM_FILTER_ERROR_RATE, 
            BLOOM_FILTER_SIZE,
        );
        Ok(Level {
            meta,
            tables,
            bloom_filter,
        })
    }

    /// Gets the bloom filter from the level's SSTables and returns.
    /// 
    /// Note this **doesn't** change the `self.bloom_filter`.
    pub fn get_bloom_filter(&self) -> Result<BloomFilter> {
        let mut bloom_filter = BloomFilter::with_rate(
            BLOOM_FILTER_ERROR_RATE, 
            BLOOM_FILTER_SIZE,
        );
        for table in &self.tables {
            let table_bloom_filter = table.get_bloom_filter()?;
            bloom_filter.union(&table_bloom_filter);
        }
        Ok(bloom_filter)
    }

    pub fn add_sstable(&mut self, ) -> Result<()> {
        Ok(())
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct LevelMeta {
    /// A unique identifier for this level.
    pub id: ObjectId,

    /// The time at which this level was created.
    pub created_at: DateTime,

    /// The level number (1 is the first on-disk level).
    pub level: usize,

    /// The number of SSTables in this level.
    pub num_tables: usize,
}

/// A handle that stores the location of an SSTable on disk as well as some metadata.
pub struct SSTableHandle {
    /// The metadata for this SSTable.
    pub meta: SSTableMeta,

    /// The path to this SSTable on disk.
    pub path: String,
}

impl SSTableHandle {
    /// Creates a new SSTableHandle.
    pub fn new(meta: SSTableMeta, path: String) -> Self {
        SSTableHandle {
            meta,
            path,
        }
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
#[derive(Debug, Serialize, Deserialize)]
pub struct SSTable {
    /// The metadata for this SSTable.
    pub meta: SSTableMeta,

    /// The records in this SSTable.
    pub records: Vec<Record>,
}

/// Metadata associated with an SSTable on disk.
#[derive(Debug, Serialize, Deserialize)]
pub struct SSTableMeta {
    /// A unique identifier for this SSTable.
    pub id: ObjectId,

    /// The time at which this SSTable was created.
    pub created_at: DateTime,

    /// The minimum key in this SSTable.
    pub min_key: ObjectId,

    /// The maximum key in this SSTable.
    pub max_key: ObjectId,

    /// The number of records in this SSTable.
    pub num_records: usize,
}

/// A record stored in an SSTable.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Record {
    /// The record's unique key.
    pub key: ObjectId,

    /// The record's value.
    pub value: Value<Document>,
}

/// A value stored in an SSTable. Can represent either a true value or a 
/// tombstone (indicating that the record has been deleted).
#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum Value<T> {
    /// A true value.
    Data(T),

    /// A tombstone value indicating that the record has been deleted.
    Tombstone,
}


#[cfg(test)]
mod test {

}
