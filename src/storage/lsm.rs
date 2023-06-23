use bson::Document;
use bson::oid::ObjectId;

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
    pub fn set(&mut self, key: ObjectId, doc: Document) {
        self.memtable.set(key, doc);
    }

    /// Delete a key from the LSM Tree.
    pub fn del(&mut self, key: ObjectId) {
        self.memtable.del(key);
    }
    
    /// Get a value from the LSM Tree's on-disk levels.
    fn get_from_disk(&self, _key: ObjectId) -> Option<Value<Document>> {
        unimplemented!();
    }
    
    /// Get a value from the LSM Tree.
    /// 
    /// This will first check the in-memory buffer, then the on-disk levels.
    pub fn get(&self, key: ObjectId) -> Option<Document> {
        if let Some(value) = self.memtable.get(key) {
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


#[cfg(test)]
mod test {}

