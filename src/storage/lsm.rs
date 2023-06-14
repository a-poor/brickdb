use bson::Document;
use serde::{Deserialize, Serialize};

/// A struct representing an LSM Tree managing both in-memory and on-disk data.
pub struct LSMTree {
    /// The unique identifier for this LSM Tree.
    pub id: bson::oid::ObjectId,

    /// The name of this LSM Tree.
    pub name: String,

    /// The configuration for this LSM Tree.
    pub config: LSMTreeConfig,

    /// The in-memory buffer for this LSM Tree.
    pub memtable: MemTable,

    /// The on-disk levels for this LSM Tree.
    pub levels: Vec<Level>,
}

/// Configuration for an LSM Tree.
/// 
/// Note that in this early iteration, the configuration will be very simple.
pub struct LSMTreeConfig {
    /// The maximum number of records to store in the in-memory buffer before
    /// flushing to disk.
    pub memtable_size: usize,

    /// The maximum number of SSTables to store in the first level of the LSMTree.
    /// 
    /// Subsequent levels will have a maximum number of SSTables equal to the
    /// previous level's maximum divided by the `max_sstable_factor`.
    pub max_sstables_level1: usize,

    /// The maximum number of SSTables to store in each level of the LSMTree after
    /// the first level. This number will be divided by previous level's maximum
    /// to determine the maximum number of SSTables in the next level.
    pub max_sstable_factor: usize,
}

/// The in-memory buffer for an LSM Tree.
/// 
/// This buffer is comprised of a red-black tree of records, sorted by key.
pub struct MemTable;

/// An on-disk level in the LSM Tree, comprised of zero or more SSTables.
pub struct Level {
    pub sstables: Vec<SSTableHandle>,
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
#[derive(Debug, Serialize, Deserialize)]
pub struct Record {
    /// The record's unique key.
    pub key: bson::oid::ObjectId,

    /// The record's value.
    pub value: Value<Document>,
}

/// A value stored in an SSTable. Can represent either a true value or a 
/// tombstone (indicating that the record has been deleted).
#[derive(Debug, Serialize, Deserialize)]
pub enum Value<T> {
    /// A true value.
    Data(T),

    /// A tombstone value indicating that the record has been deleted.
    Tombstone,
}


#[derive(Debug, Serialize, Deserialize)]
pub struct SSTableMeta {
    pub id: bson::oid::ObjectId,
    pub created_at: bson::DateTime,
    pub min_key: String,
    pub max_key: String,
    pub num_records: usize,
    // pub summary: SSTableSummary, // NOTE - Is this needed?
}


