use bson::{DateTime};
use bson::oid::ObjectId;
use serde::{Deserialize, Serialize};
use anyhow::Result;
use bloom::{BloomFilter, ASMS};

use crate::storage::record::*;
use crate::storage::conf::*;


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


#[cfg(test)]
mod test {}



