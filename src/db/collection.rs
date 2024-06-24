use crate::storage::lsm::LSMTree;
use anyhow::Result;
use bson::oid::ObjectId;
use bson::Document;

pub struct CollectionMeta {
    pub name: String,
}

/// A collection of documents. Equivalent to a table in a relational database.
///
/// Collections are stored in a [super::database::Database].
pub struct Collection {
    pub tree: LSMTree,
}

impl Collection {
    pub fn new(name: &str, path: &str) -> Self {
        Collection {
            tree: LSMTree::new(name, path),
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
