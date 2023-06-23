use bson::DateTime;
use bson::oid::ObjectId;
use serde::{Deserialize, Serialize};
use anyhow::Result;
use bloom::{BloomFilter, Unionable};

use crate::storage::conf::*;
use crate::storage::sstable::*;


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


#[cfg(test)]
mod test {}

