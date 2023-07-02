use std::path::Path;
use bson::DateTime;
use bson::oid::ObjectId;
use serde::{Deserialize, Serialize};
use anyhow::{anyhow, Result};
use bloom::{BloomFilter, Unionable, ASMS};

use crate::storage::conf::*;
use crate::storage::sstable::*;
use crate::storage::record::*;


/// An on-disk level in the LSM Tree, comprised of zero or more SSTables.
pub struct Level {
    /// The metadata for this level.
    pub meta: LevelMeta,

    /// The SSTables in this level.
    pub tables: Vec<SSTableHandle>,

    /// A Bloom filter for this level.
    pub bloom_filter: BloomFilter,

    /// The path to this level's directory on disk.
    pub path: String,

    /// The maximum number of tables allowed in this level
    /// for it to be considered full.
    pub max_tables: usize,
    
    /// The maximum number of records per table in this level.
    /// 
    /// Due to compaction, there may be fewer records in a 
    /// given table in this level.
    pub records_per_table: usize,
}

impl Level {
    /// Create a new LSM Tree Level.
    pub fn new(path: &str, level_number: usize, tables: Vec<SSTableHandle>) -> Result<Self> {
        /// TODO - Use the id to format the path...
        
        let bloom_filter = BloomFilter::with_rate(
            BLOOM_FILTER_ERROR_RATE, 
            BLOOM_FILTER_SIZE,
        );
        Ok(Level {
            meta: LevelMeta {
                id: ObjectId::new(),
                created_at: DateTime::now(),
                level: level_number,
                num_tables: tables.len(),
            },
            tables,
            bloom_filter,
            path: path.to_string(),
            max_tables: MAX_TABLES_PER_LEVEL,
            records_per_table: MEMTABLE_MAX_SIZE * level_number,
        })
    }

    /// Gets the bloom filter from the level's SSTables and returns.
    /// 
    /// Note this *doesn't* change the `self.bloom_filter`.
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

    /// Checks if the level *doesn't* contain the given key.
    /// 
    /// # Arguments
    /// 
    /// * `key` - The key to check for
    /// 
    /// # Returns
    /// 
    /// Returns a `bool` indicating whether the level *doesn't*
    /// contain the given key. If `false`, the level *probably*
    /// contains the key.
    pub fn doesnt_contain(&self, key: &ObjectId) -> bool {
        !self.bloom_filter.contains(key)
    }
    
    fn format_table_path(&self, id: &ObjectId) -> Option<String> {
        Path::new(&self.path)
            .join(format!("{}.bson", id))
            .to_str()
            .map(|s| s.to_string())
    }
 
    /// Adds an SSTable to this level.
    pub fn add_sstable(&mut self, table: &SSTable) -> Result<()> {
        // Get the path to the table...
        let table_path = self.format_table_path(&table.meta.table_id)
            .ok_or(anyhow!("Couldn't format table path"))?;

        // Create the handle...
        let handle = SSTableHandle::new(
            table.meta.clone(), 
            table_path.as_str(),
        );

        // Write the table to disk...
        handle.write(table)?;
        
        // Add the handle...
        self.tables.push(handle);
        Ok(())
    }

    fn format_meta_path(&self) -> Option<String> {
        Path::new(&self.path)
            .join(LEVEL_META_FILE)
            .to_str()
            .map(|s| s.to_string())
    }

    /// Reads the metadata for this level from disk.
    pub fn load_meta(&mut self) -> Result<()> {
        // Get the path to the meta file...
        let p = self.format_meta_path()
            .ok_or(anyhow!("Couldn't format meta path"))?;

        // Read in the metadata...
        let file = std::fs::File::open(p)?;
        let reader = std::io::BufReader::new(file);
        let meta: LevelMeta = bson::from_reader(reader)?;

        // Set the metadata...
        self.meta = meta;
        Ok(())
    }

    /// Writes the metadata for this level to disk.
    pub fn write_meta(&self) -> Result<()> {
        // Get the path to the meta file...
        let p = self.format_meta_path()
            .ok_or(anyhow!("Couldn't format meta path"))?;

        // Write the metadata...
        let file = std::fs::File::create(p)?;
        let writer = std::io::BufWriter::new(file);
        let doc = bson::to_document(&self.meta)?;
        doc.to_writer(writer)?;
        Ok(())
    }

    /// Checks if this level is full based on the number of tables.
    pub fn is_full(&self) -> bool {
        self.tables.len() >= self.max_tables
    }

    /// Gets a record from this level, if it exists.
    /// 
    /// # Arguments
    /// 
    /// * `key` - The key for the record to get.
    /// 
    /// # Returns
    /// 
    /// Returns a `Result` containing either a `Option<Record` or an `Error`.
    /// If the `Result` is `Ok(Some(record))`, then the record was found. If
    /// the `Result` is `Ok(None)`, then the record was not found. If the 
    /// `Result` is `Err`, then an error occurred.
    pub fn get(&self, key: &ObjectId) -> Result<Option<Record>> {
        // Check the bloom filter first...
        if self.doesnt_contain(key) {
            return Ok(None);
        }

        // Then iterate through the SSTables...
        for th in &self.tables {
            // Check if the table is active...
            if !th.active {
                // The table isn't active, skip it...
                continue;
            }

            // Check if the key is in range...
            if !th.meta.key_in_range(key) {
                // The key isn't in range, skip this table...
                continue;
            }

            // Read in the table...
            let sstable = th.read()?;

            // Check if the table contains the key...
            if let Some(record) = sstable.get(key) {
                // Return the record if it exists...
                return Ok(Some(record));
            }
        }
        
        // Key not found...
        Ok(None)
    }

    /// Compacts the tables in this level into a single SSTable.
    /// 
    /// # Returns
    /// 
    /// Returns a reference the new SSTable.
    pub fn compact_tables(&self) -> Result<SSTable> {
        // Create a place to store the merged SSTable...
        let mut res: Option<SSTable> = None;

        // Iterate through the level's sstables...
        for table in self.tables.iter() {
            // Read in the table...
            let sstable = table.read()?;
            if let Some(prev) = res {
                // Merge the table with the accumulated SSTable...
                let m = prev.merge(&sstable)?;
                res = Some(m);
            } else {
                // There is no accumulated SSTable, so just use this one...
                res = Some(sstable);
            }
        }

        // Return the merged SSTable.
        match res {
            Some(table) => Ok(table),
            None => Err(anyhow!("No SSTable found")),
        }
    }
}

/// The metadata for an LSM Tree Level.
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

