use bson::Document;
use bson::oid::ObjectId;
use anyhow::Result;

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

    /// Set a key to a value in the LSM Tree.
    pub fn set(&mut self, key: &ObjectId, doc: Document) {
        self.memtable.set(key, doc);
    }

    /// Delete a key from the LSM Tree.
    pub fn del(&mut self, key: &ObjectId) {
        self.memtable.del(key);
    }
    
    /// Get a value from the LSM Tree's on-disk levels.
    fn get_from_disk(&self, key: &ObjectId) -> Result<Option<Record>> {
        // Iterate through the levels...
        for level in self.levels.iter() {
            if let Some(val) = level.get(&key)? {
                return Ok(Some(val));
            }
        }
        Ok(None)
    }
    
    /// Get a value from the LSM Tree.
    /// 
    /// This will first check the in-memory buffer, then the on-disk levels.
    pub fn get(&self, key: &ObjectId) -> Result<Option<Document>> {
        // First try to get it from the memtable...
        if let Some(value) = self.memtable.get(key) {
            return match value {
                Value::Data(doc) => Ok(Some(doc)),
                Value::Tombstone => Ok(None),
            };
        }

        // Otherwise try to get it from disk...
        match self.get_from_disk(key)? {
            Some(rec) => match rec.value {
                Value::Data(doc) => Ok(Some(doc)),
                Value::Tombstone => Ok(None),
            },
            None => Ok(None),
        }
    }
}


#[cfg(test)]
mod test {}

