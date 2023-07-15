use bson::Document;
use bson::oid::ObjectId;
use anyhow::{anyhow, Result};

use crate::storage::record::*;
use crate::storage::level::*;
use crate::storage::memtable::*;



/// A struct representing an LSM Tree managing both in-memory and on-disk data.
pub struct LSMTree {
    /// The unique identifier for this LSM Tree.
    pub id: ObjectId,

    /// The name of this LSM Tree.
    pub name: String,

    /// The in-memory buffer for this LSM Tree.
    pub memtable: MemTable,

    /// A memtable that is frozen and in the process of being flushed 
    /// to disk. This will keep the data accessible while it is being
    /// flushed but will not allow any new data to be added.
    /// 
    /// If `None`, it isn't in the process of being flushed.
    pub frozen_memtable: Option<MemTable>,

    /// The on-disk levels for this LSM Tree.
    pub levels: Vec<Level>,

    /// The path to the directory where this LSM Tree's data is stored.
    pub path: String,
}

impl LSMTree {
    /// Creates a new LSM Tree with the given name.
    pub fn new(name: &str, path: &str) -> Self {
        LSMTree {
            id: ObjectId::new(),
            name: name.to_string(),
            memtable: MemTable::new(),
            frozen_memtable: None,
            levels: vec![],
            path: path.to_string(),
        }
    }

    /// Load an existing LSM Tree from disk.
    pub fn load() -> Result<Self> {
        todo!();
    }

    /// Set a key to a value in the LSM Tree.
    pub fn set(&mut self, key: &ObjectId, doc: Document) {
        self.memtable.set(key, doc);
    }

    /// Delete a key from the LSM Tree.
    pub fn del(&mut self, key: &ObjectId) {
        self.memtable.del(key);
    }
    
    /// Get a value from the LSM Tree's on-disk levels.
    async fn get_from_disk(&self, key: &ObjectId) -> Result<Option<Record>> {
        // Iterate through the levels...
        for level in self.levels.iter() {
            if let Some(val) = level.get(key).await? {
                return Ok(Some(val));
            }
        }
        Ok(None)
    }
    
    /// Get a value from the LSM Tree.
    /// 
    /// This will first check the in-memory buffer, then the on-disk levels.
    pub async fn get(&self, key: &ObjectId) -> Result<Option<Document>> {
        // First try to get it from the memtable...
        if let Some(value) = self.memtable.get(key) {
            return match value {
                Value::Data(doc) => Ok(Some(doc)),
                Value::Tombstone => Ok(None),
            };
        }

        // Next try to get it from the frozen memtable...
        if let Some(frozen) = &self.frozen_memtable {
            if let Some(value) = frozen.get(key) {
                return match value {
                    Value::Data(doc) => Ok(Some(doc)),
                    Value::Tombstone => Ok(None),
                };
            }
        }

        // Otherwise try to get it from disk...
        match self.get_from_disk(key).await? {
            Some(rec) => match rec.value {
                Value::Data(doc) => Ok(Some(doc)),
                Value::Tombstone => Ok(None),
            },
            None => Ok(None),
        }
    }

    /// Move through the levels of the LSM Tree (including the memtable)
    /// and compact them, if necessary.
    pub async fn compaction_cycle(&mut self) -> Result<()> {
        // Compact the memtable...
        self.compact_memtable(false).await?;

        // Iterate through the levels...
        // Using a while loop as number of levels may change during compaction...
        let mut i = 0;
        while i < self.levels.len() {
            // Get a mutable reference to the level...
            if let Some(level) = self.levels.get_mut(i) {
                // Is the level full?
                if !level.is_full() {
                    // Not full, stop here...
                    return Ok(());
                }

                // Compact the level...
                let n = i + 1; // The level number is 1-indexed...
                self.compact_level(n, false).await?;
            }
            i += 1;
        }
        Ok(())
    }

    /// Compacts the memtable into an SSTable and adds it to the first level.
    /// 
    /// # Arguments
    /// 
    /// * `force` - If `true`, the memtable will be compacted even if it isn't full.
    async fn compact_memtable(&mut self, force: bool) -> Result<()> {
        // Is the memtable full?
        if !force || !self.memtable.is_full() {
            // Not full, stop here...
            return Ok(());
        }

        // Ensure there isn't already a frozen memtable...
        if self.frozen_memtable.is_some() {
            return Err(anyhow!("Memtable already frozen!"));
        }

        // Freeze the memtable...
        self.frozen_memtable = Some(self.memtable.clone()); // TODO - Get rid of clone
        self.memtable = MemTable::new();

        // Flush the frozen memtable to an SSTable...
        let sstable = self.frozen_memtable
            .as_ref()
            .ok_or(anyhow!("Failed to get frozen memtable"))?
            .flush()?;

        // Does a new level need to be created before adding the sstable?
        if self.levels.len() == 0 {
            self.add_level(true).await?;
        }

        // Add the ss-table to the first level...
        // (There should now be at least one level) 
        self.levels[0].add_sstable(&sstable).await?;

        // Remove the frozen memtable...
        self.frozen_memtable = None;
        Ok(())
    }

    /// Compacts the given level into the next level.
    /// 
    /// # Arguments
    /// 
    /// * `n` - The level number (1-indexed).
    /// * `force` - If `true`, the memtable will be compacted even if it isn't full.
    async fn compact_level(&mut self, n: usize, force: bool) -> Result<()> {
        // Validate the level number...
        if n == 0 {
            return Err(anyhow!("Level number must be greater than 0"));
        }
        let level_len = self.levels.len();
        if n > level_len {
            return Err(anyhow!("Level {} not found", n));
        }
        
        // Get the n-th level...
        let i = n - 1; // The level number is 1-indexed...
        
        // Get the sstable...
        // Wrapped in a scope to ensure the mutable borrow of self.levels is dropped
        let CompactResult { new_table, old_table_ids } = {
            let level = self.levels
                .get_mut(i)
                .ok_or(anyhow!("Level {} not found", n))?;
            
            // Is the level full?
            if !force || !level.is_full() {
                // Not full, stop here...
                return Ok(());
            }
        
            // Compact the level...
            level.compact_tables().await?
        };

        // Does a new level need to be created before adding the sstable?
        if level_len == n {
            self.add_level(true).await?;
        }

        // Add the ss-table to the next level...
        // (There should now be at least n levels)
        self.levels[i+1].add_sstable(&new_table).await?;
        
        // Clear the old level...
        self.levels[i].clear(&old_table_ids).await?;
        Ok(())
    }

    /// Adds a new level to the LSM Tree.
    /// 
    /// # Arguments
    /// 
    /// * `to_disk` - If `true`, the level will be stored on disk.
    /// 
    /// # Returns
    /// 
    /// A `Result` containing `Ok(())` if the level was added successfully.
    pub async fn add_level(&mut self, to_disk: bool) -> Result<()> {
        // Create a new level...
        let level = Level::new(
            self.path.as_str(), 
            self.levels.len() + 1, 
            vec![],
            to_disk,
        ).await?;

        // Add the level to the LSM Tree...
        self.levels.push(level);
        Ok(())
    }
}


/// A struct representing the metadata for an LSM Tree.
pub struct LSMTreeMeta {
    /// The unique identifier for this LSM Tree.
    pub id: ObjectId,

    /// The name of this LSM Tree.
    pub name: String,

    /// The path to the directory where this LSM Tree's data is stored.
    pub path: String,
}

#[cfg(test)]
mod test {}

