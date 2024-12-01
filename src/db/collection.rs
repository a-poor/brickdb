use std::collections::HashMap;
use anyhow::Result;
use bson::oid::ObjectId;
use bson::Document;
use serde::{Deserialize, Serialize};
use crate::storage::lsm::LSMTree;
use crate::index::bptree::BPTree;

/// Metadata about a collection.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CollectionMeta {
    pub name: String,
}

/// A collection of documents. Equivalent to a table in a relational database.
///
/// Collections are stored in a [super::database::Database].
/// 
/// This is the higher-level API for interacting with a collection.
/// 
/// On disk, a collection has the following structure:
pub struct Collection {
    /// The underlying LSM tree that stores the documents in the collection.
    pub tree: LSMTree,

    /// A map from index id to the fields in the collection.
    pub indexes: HashMap<String, BPTree>,
}

impl Collection {
    pub fn new(name: &str, path: &str) -> Self {
        Collection {
            tree: LSMTree::new(name, path),
            indexes: HashMap::new(),
        }
    }

    pub fn load() -> Result<Self> {
        todo!();
    }

    pub async fn get(&self, key: &ObjectId) -> Result<Option<Document>> {
        self.tree.get(key).await
    }

    pub async fn get_range(&self, _start: &ObjectId, _end: &ObjectId) -> Result<Vec<Document>> {
        // self.tree.get_range(start, end).await
        todo!();
    }

    pub async fn set(&mut self, key: &ObjectId, doc: Document) -> Result<()> {
        self.tree.set(key, doc);
        Ok(())
    }

    pub async fn del(&mut self, key: &ObjectId) -> Result<()> {
        self.tree.del(key);
        Ok(())
    }
}
