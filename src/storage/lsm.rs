use std::collections::BTreeMap;
use bson::{Document, DateTime};
use bson::oid::ObjectId;
use serde::{Deserialize, Serialize};

/// The maximum number of tables per level in the LSM Tree.
pub const MAX_TABLES_PER_LEVEL: usize = 10;

/// The maximum number of records to store in the memtable 
/// before flushing to disk. This will also be the max size 
/// of a single SSTable in the first (on-disk) level of 
/// the LSM Tree.
pub const MEMTABLE_MAX_SIZE: usize = 100;


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
}

impl LSMTree {
    /// Creates a new LSM Tree with the given name.
    pub fn new(name: String) -> Self {
        LSMTree {
            id: ObjectId::new(),
            name,
            memtable: MemTable {
                records: BTreeMap::new(),
            },
            levels: vec![],
        }
    }

    /// Inserts a new record into the LSM Tree.
    pub fn insert(&mut self, key: ObjectId, value: Value<Document>) {
        self.memtable.records.insert(key, value);
    }

    pub fn set(&mut self, key: ObjectId, doc: Document) {
        self.insert(key, Value::Data(doc));
    }

    pub fn del(&mut self, key: ObjectId) {
        self.insert(key, Value::Tombstone);
    }

    fn get_from_memtable(&self, key: ObjectId) -> Option<Value<Document>> {
        match self.memtable.records.get(&key) {
            Some(value) => Some(value.clone()),
            None => None,
        }
    }
    
    fn get_from_disk(&self, _key: ObjectId) -> Option<Value<Document>> {
        unimplemented!();
    }
    
    pub fn get(&self, key: ObjectId) -> Option<Document> {
        match self.get_from_memtable(key) {
            Some(value) => match value {
                Value::Data(doc) => Some(doc),
                Value::Tombstone => None,
            },
            None => match self.get_from_disk(key) {
                Some(value) => match value {
                    Value::Data(doc) => Some(doc),
                    Value::Tombstone => None,
                },
                None => None,
            }
        }
    }
}

/// The in-memory buffer for an LSM Tree.
/// 
/// This buffer is comprised of a red-black tree of records, sorted by key.
pub struct MemTable {
    pub records: BTreeMap<ObjectId, Value<Document>>,
}

/// An on-disk level in the LSM Tree, comprised of zero or more SSTables.
pub struct Level {
    pub tables: Vec<SSTableHandle>,
}

pub struct LevelMeta {

}

/// A handle that stores the location of an SSTable on disk as well as some metadata.
pub struct SSTableHandle;

/// In-memory summary data associated with an SSTable on disk.
pub struct SSTableSummary;

/// An SSTable read from disk.
#[derive(Debug, Serialize, Deserialize)]
pub struct SSTable {
    /// The metadata for this SSTable.
    pub meta: SSTableMeta,

    /// The records in this SSTable.
    pub records: Vec<Record>,
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


#[derive(Debug, Serialize, Deserialize)]
pub struct SSTableMeta {
    pub id: ObjectId,
    pub created_at: DateTime,
    pub min_key: String,
    pub max_key: String,
    pub num_records: usize,
}


