use anyhow::{anyhow, Result};
use bloom::{BloomFilter, ASMS};
use bson::oid::ObjectId;
use bson::DateTime;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::path::Path;
use tokio::fs;

use crate::storage::conf::*;
use crate::storage::record::*;
use crate::storage::sstable::*;
use crate::storage::util::*;

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
    ///
    /// # Arguments
    ///
    /// * `parent_path` - The path to the parent directory for this level.
    /// * `level_number` - The level number (1 is the first on-disk level).
    /// * `tables` - The SSTables in this level.
    /// * `to_disk` - Whether to create the directory for this level.
    ///
    /// # Returns
    ///
    /// Returns a `Result` containing either the new level or an `Error`.
    pub async fn new(
        parent_path: &str,
        level_number: usize,
        tables: Vec<SSTableHandle>,
        to_disk: bool,
    ) -> Result<Self> {
        // Create the metadata...
        let meta = LevelMeta::new(
            level_number,
            tables.len(),
            tables.iter().map(|t| t.meta.table_id).collect(),
        );

        // Format the path...
        let path = Path::new(parent_path);
        let path = path.join(meta.id.to_string());
        let path = path
            .to_str()
            .ok_or(anyhow!("Couldn't format level path"))?
            .to_string();

        // Create the bloom filter...
        let bloom_filter = BloomFilter::with_rate(BLOOM_FILTER_ERROR_RATE, BLOOM_FILTER_SIZE);

        // Create the level...
        let level = Level {
            meta,
            tables,
            bloom_filter,
            path: path.clone(),
            max_tables: MAX_TABLES_PER_LEVEL,
            records_per_table: MEMTABLE_MAX_SIZE * level_number,
        };

        if to_disk {
            // Create the directory...
            fs::create_dir_all(&path).await?;

            // Write the metadata to disk...
            level.write_meta().await?;
        }

        // Return the level...
        Ok(level)
    }

    pub async fn load_from_file(parent_path: &str, id: &ObjectId) -> Result<Self> {
        // Get the level's path...
        let path = Path::new(parent_path);
        let path = path.join(id.to_string());
        let path = path
            .to_str()
            .ok_or(anyhow!("Couldn't format level path"))?
            .to_string();

        // CHeck that the path exists and is a directory...
        let path = Path::new(&path);
        if !path.exists() {
            return Err(anyhow!("Level path doesn't exist"));
        }
        if !path.is_dir() {
            return Err(anyhow!("Level path isn't a directory"));
        }

        // Load the metadata...
        let meta_path = path.join(LEVEL_META_FILE);
        let meta = {
            let bytes = read_bson(meta_path).await?;
            let meta: LevelMeta = bson::from_slice(&bytes)?;
            meta
        };
        let level_num = meta.level;

        // Create the level...
        let mut level = Level {
            meta,
            tables: vec![],
            bloom_filter: BloomFilter::with_rate(BLOOM_FILTER_ERROR_RATE, BLOOM_FILTER_SIZE),
            path: path
                .to_str()
                .ok_or(anyhow!("Couldn't format level path"))?
                .to_string(),
            max_tables: MAX_TABLES_PER_LEVEL,
            records_per_table: MEMTABLE_MAX_SIZE * level_num,
        };

        // Load the tables...
        level.reload_handles().await?;

        // Load the bloom filter...
        level.bloom_filter = level.get_bloom_filter().await?;

        Ok(level)
    }

    /// Gets the bloom filter from the level's SSTables and returns.
    ///
    /// Note this *doesn't* change the `self.bloom_filter`.
    pub async fn get_bloom_filter(&self) -> Result<BloomFilter> {
        // Create a new, empty bloom filter...
        let mut bloom_filter = BloomFilter::with_rate(BLOOM_FILTER_ERROR_RATE, BLOOM_FILTER_SIZE);

        // Iterate over the table handles (in reverse order)...
        for table in self.tables.iter().rev() {
            // Read in the table...
            let sstable = table.read().await?;

            // Iterate over the table's records...
            for record in sstable.records.iter() {
                // Insert the record's key into the bloom filter...
                bloom_filter.insert(&record.key);
            }
        }

        // Return it!
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
    pub async fn add_sstable(&mut self, table: &SSTable) -> Result<()> {
        // Get the path to the table...
        let table_path = self
            .format_table_path(&table.meta.table_id)
            .ok_or(anyhow!("Couldn't format table path"))?;

        // Create the handle...
        let handle = SSTableHandle::new(table.meta.clone(), table_path.as_str());

        // Write the table to disk...
        handle.write(table).await?;

        // Add the handle...
        self.tables.push(handle);

        // Update the metadata...
        self.update_table_ids().await?;
        Ok(())
    }

    /// Reads the metadata for this level from disk.
    pub async fn load_meta(&mut self) -> Result<()> {
        // Get the path to the meta file...
        let path =
            format_meta_path(self.path.as_str()).ok_or(anyhow!("Couldn't format meta path"))?;

        // Read in the data and deserialize from BSON...
        let buff = read_bson(path).await?;
        let meta: LevelMeta = bson::from_slice(&buff)?;

        // Set the metadata...
        self.meta = meta;
        Ok(())
    }

    /// Writes the metadata for this level to disk.
    pub async fn write_meta(&self) -> Result<()> {
        // Get the path to the meta file...
        let path = format_meta_path(&self.path).ok_or(anyhow!("Couldn't format meta path"))?;

        // Convert the metadata to a BSON document...
        let doc = bson::to_document(&self.meta)?;

        // Write the data...
        write_bson(path, &doc).await?;

        // Success!
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
    pub async fn get(&self, key: &ObjectId) -> Result<Option<Record>> {
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
            let sstable = th.read().await?;

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
    pub async fn compact_tables(&self) -> Result<CompactResult> {
        // Create a place to store the merged SSTable...
        let mut res: Option<SSTable> = None;

        // Create a vector to store the old table ids...
        let mut old_table_ids = vec![];

        // Iterate through the level's sstables...
        for table in self.tables.iter() {
            // Add the table id to the old table ids...
            old_table_ids.push(table.meta.table_id);

            // Read in the table...
            let sstable = table.read().await?;
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
            Some(new_table) => Ok(CompactResult {
                new_table,
                old_table_ids,
            }),
            None => Err(anyhow!("No SSTable found")),
        }
    }

    /// Clears the given tables from this level.
    ///
    /// Clears the given tables from this level object, removes
    /// cleared tables from disk, and updates this level's metadata.
    ///
    /// # Arguments
    ///
    /// * `ids` - The ids of the tables to be cleared.
    ///
    /// # Returns
    ///
    /// Returns a `Result` containing either `()` if successful or
    /// an `Error` if not.
    pub async fn clear(&mut self, ids: &[ObjectId]) -> Result<()> {
        // Create a vector to store the remaining tables...
        let mut remaining = vec![];

        // Convert the ids to a set...
        let ids: HashSet<_> = ids.iter().collect();

        // Iterate through the tables...
        for table in self.tables.iter() {
            // Check if the table is in the ids...
            if ids.contains(&table.meta.table_id) {
                // The table is in the ids, so delete it...
                table.delete().await?;
            } else {
                // The table isn't in the ids, so keep it...
                remaining.push(table.clone());
            }
        }

        // Set the remaining tables...
        self.tables = remaining;

        // Update the metadata...
        self.update_table_ids().await?;

        // Success!
        Ok(())
    }

    /// Removes all tables from this level.
    ///
    /// Clears tables from this level object, removes cleared
    /// tables from disk, and updates this level's metadata.
    ///
    /// # Returns
    ///
    /// Returns a `Result` containing either `()` if successful or
    /// an `Error` if not.
    pub async fn clear_all(&mut self) -> Result<()> {
        // Get the tables...
        let tables = self.tables.clone();

        // Set the tables to an empty vector...
        self.tables = vec![];

        // Iterate through deleting the old tables...
        for table in tables {
            // Delete the table...
            table.delete().await?;
        }
        Ok(())
    }

    /// Updates the table ids in the level's metadata.
    pub async fn update_table_ids(&mut self) -> Result<()> {
        self.meta.table_ids = self.tables.iter().map(|t| t.meta.table_id).collect();

        // Update the number of tables...
        self.meta.num_tables = self.tables.len();

        // Update the bloom filter...
        self.bloom_filter = self.get_bloom_filter().await?;

        // Update the metadata file on disk...
        self.write_meta().await?;

        // Success!
        Ok(())
    }

    /// Reloads the table handles from disk.
    ///
    /// Uses the table ids in the level's metadata to reload
    /// the table handles from disk.
    pub async fn reload_handles(&mut self) -> Result<()> {
        // Create a vector to store the handles...
        let mut handles = vec![];

        // Create a bloom filter for the level...
        let mut bf = BloomFilter::with_rate(BLOOM_FILTER_ERROR_RATE, BLOOM_FILTER_SIZE);

        // Iterate through the table ids...
        // TODO - Make this parallel?
        for id in self.meta.table_ids.iter() {
            // Get the path to the table...
            let table_path = self
                .format_table_path(id)
                .ok_or(anyhow!("Couldn't format table path"))?;

            // Read in the table...
            let table = {
                let bytes = read_bson(table_path.clone()).await?;
                let table: SSTable = bson::from_slice(&bytes)?;
                table
            };

            // Create the handle...
            let handle = SSTableHandle {
                active: true,
                meta: table.meta,
                path: table_path,
            };

            // Add the table's records to the bloom filter...
            for record in table.records.iter() {
                bf.insert(&record.key);
            }

            // Add the handle to the vector...
            handles.push(handle);
        }

        // Sort the handles by create date (desc)...
        handles.sort_by(|a, b| b.meta.created_at.cmp(&a.meta.created_at));

        // Set the handles...
        self.tables = handles;

        // Set the bloom filter...
        self.bloom_filter = bf;

        // Success!
        Ok(())
    }
}

pub struct CompactResult {
    pub new_table: SSTable,
    pub old_table_ids: Vec<ObjectId>,
}

/// The metadata for an LSM Tree Level.
#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct LevelMeta {
    /// A unique identifier for this level.
    pub id: ObjectId,

    /// The time at which this level was created.
    pub created_at: DateTime,

    /// The level number (1 is the first on-disk level).
    pub level: usize,

    /// The number of SSTables in this level.
    pub num_tables: usize,

    /// The ids of the tables in this level.
    pub table_ids: Vec<ObjectId>,
}

impl LevelMeta {
    /// Creates a new LSM Tree Level Metadata.
    ///
    /// # Arguments
    ///
    /// * `level` - The level number (1 is the first on-disk level).
    /// * `num_tables` - The number of SSTables in this level.
    /// * `table_ids` - The ids of the tables in this level.
    ///
    /// # Returns
    ///
    /// Returns a `Result` containing either the new metadata or an `Error`.
    pub fn new(level: usize, num_tables: usize, table_ids: Vec<ObjectId>) -> Self {
        LevelMeta {
            id: ObjectId::new(),
            created_at: DateTime::now(),
            level,
            num_tables,
            table_ids,
        }
    }
}

fn format_meta_path(path: &str) -> Option<String> {
    Path::new(path)
        .join(LEVEL_META_FILE)
        .to_str()
        .map(|s| s.to_string())
}

#[cfg(test)]
mod test {
    use super::*;
    use anyhow::Result;
    use bson::doc;

    #[tokio::test]
    async fn create_level() -> Result<()> {
        // Create a new level with no tables...
        let level = Level::new("/tmp", 1, vec![], true).await?;

        println!("Created level: {:?}", level.meta.id);

        // Check if the directory exists at (/tmp/<level_id>)...
        let path = Path::new("/tmp").join(level.meta.id.to_string());
        assert!(path.exists());

        println!("path {:?} exists", path);

        // And check if it's a directory...
        assert!(path.is_dir());

        // Now check if the metadata file exists...
        let meta_path = path.join(LEVEL_META_FILE);
        assert!(meta_path.exists());

        // And check if it's a file...
        assert!(meta_path.is_file());

        // Read the metadata back in and compare it to the level's metadata...
        let meta = {
            let bytes = read_bson(meta_path).await?;
            let meta: LevelMeta = bson::from_slice(&bytes)?;
            meta
        };
        assert_eq!(meta, level.meta);

        // (Clean up) Remove the directory...
        fs::remove_dir_all(path).await?;
        Ok(())
    }

    #[tokio::test]
    async fn get_bloom_filter() -> Result<()> {
        // Create a new level with no tables...
        let mut level = Level::new("/tmp", 1, vec![], true).await?;

        // Create an ID to check for...
        let id = ObjectId::new();

        // Create a new SSTable...
        let table = SSTable::new(vec![
            Record {
                key: id.clone(),
                value: Value::Data(doc! {
                    "name": "John",
                }),
            },
            Record::new_data(doc! { "name": "Jane" }),
        ])?;

        // Add the SSTable to the level...
        level.add_sstable(&table).await?;

        // Get the bloom filter...
        let bloom_filter = level.get_bloom_filter().await?;

        // Check if the bloom filter contains the id...
        assert!(bloom_filter.contains(&id));

        // Check if another id is in the bloom filter...
        assert!(!bloom_filter.contains(&ObjectId::new()));

        // (Clean up) Remove the directory...
        fs::remove_dir_all(Path::new("/tmp").join(level.meta.id.to_string())).await?;

        // Success!
        Ok(())
    }

    #[tokio::test]
    async fn doesnt_contain() -> Result<()> {
        // Create a new level with no tables...
        let mut level = Level::new("/tmp", 1, vec![], true).await?;
        println!("level_id = {}", level.meta.id);

        // Create a new key...
        let key = ObjectId::new();
        println!("key = {}", key);

        // Check if the level doesn't contain the key...
        assert!(level.doesnt_contain(&key));

        // Add a table to the level with one record with that key...
        let rec = Record {
            key,
            value: Value::Data(doc! {
                "msg": "world",
            }),
        };
        let table = SSTable::new(vec![rec.clone()])?;
        println!("table_id = {:?}", table.meta.table_id);
        println!("record key = {}", rec.key);

        // Get the table's bloom filter...
        let table_bloom_filter = table.get_bloom_filter()?;

        // Check that the bloom filter _might_ contain the key...
        assert!(table_bloom_filter.contains(&key));

        // Check if the level doesn't contain the key...
        assert!(level.doesnt_contain(&key));

        // Add the table to the level...
        level.add_sstable(&table).await?;

        let table_bloom_filter = table.get_bloom_filter()?;
        assert!(table_bloom_filter.contains(&key));

        // Get the value from the level...
        let val = level.get(&key).await?;
        println!("val = {:?}", val);
        assert!(val.is_some());

        // Check that the Record is a document with the key "msg"...
        let val = val.ok_or(anyhow!("No value found"))?;
        assert_eq!(val.key, key);
        assert_eq!(val.value, Value::Data(doc! { "msg": "world" }));

        // Check that the level doesn't _not_ contain the key now...
        assert!(!level.doesnt_contain(&key));

        // Clean up...
        fs::remove_dir_all(Path::new("/tmp").join(level.meta.id.to_string())).await?;

        // Done!
        Ok(())
    }

    #[tokio::test]
    async fn add_sstable() -> Result<()> {
        // Create a new level with no tables...
        let mut level = Level::new("/tmp", 1, vec![], true).await?;

        // Create a new SSTable...
        let table = SSTable::new(vec![
            Record::new_data(doc! { "name": "John" }),
            Record::new_data(doc! { "name": "Jane" }),
        ])?;

        // Add the SSTable to the level...
        level.add_sstable(&table).await?;

        // Check if the id is in the level's metadata...
        assert!(level.meta.table_ids.contains(&table.meta.table_id));

        // Read the table back in.
        // Format the path...
        let table_path = level
            .format_table_path(&table.meta.table_id)
            .ok_or(anyhow!("Couldn't format table path"))?;

        // Read in the bytes...
        let bytes = std::fs::read(table_path)?;

        // Deserialize the table as an SSTable...
        let table: SSTable = bson::from_slice(&bytes)?;

        // Check if the table is the same as the original...
        assert_eq!(table, table);

        // (Clean up) Remove the directory...
        fs::remove_dir_all(Path::new("/tmp").join(level.meta.id.to_string())).await?;
        Ok(())
    }

    // #[test]
    // fn load_meta() -> Result<()> {
    //     todo!();
    // }

    // #[test]
    // fn write_meta() -> Result<()> {
    //     todo!();
    // }

    // #[test]
    // fn compact_tables() -> Result<()> {
    //     todo!();
    // }

    #[tokio::test]
    async fn is_full() -> Result<()> {
        // Create a new level with no tables...
        let mut level = Level::new("/tmp", 1, vec![], true).await?;

        // Iterate through the max number of tables, adding handles to the level,
        // checking if the level is full after each iteration. It should only be
        // full on the last iteration...
        for i in 0..MAX_TABLES_PER_LEVEL {
            // Create a new SSTable...
            let table = SSTable::new(vec![Record::new_tombstone()])?;

            // Add the SSTable to the level...
            level.add_sstable(&table).await?;

            // Check if the level is full...
            if i == MAX_TABLES_PER_LEVEL - 1 {
                assert!(level.is_full());
            } else {
                assert!(!level.is_full());
            }
        }

        // (Clean up) Remove the directory...
        fs::remove_dir_all(Path::new("/tmp").join(level.meta.id.to_string())).await?;

        // Done!
        Ok(())
    }

    // #[test]
    // fn get() -> Result<()> {
    //     todo!();
    // }

    // #[test]
    // fn compact_tables() -> Result<()> {
    //     todo!();
    // }

    // #[test]
    // fn clear() -> Result<()> {
    //     todo!();
    // }

    // #[test]
    // fn clear_all() -> Result<()> {
    //     todo!();
    // }

    // #[test]
    // fn update_table_ids() -> Result<()> {
    //     todo!();
    // }
}
