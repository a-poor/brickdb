use bson::Bson;
use bson::oid::ObjectId;
use serde::{Serialize, Deserialize};
use uuid::Uuid;


/// BPTree represents a handle to a B+ tree index.
pub struct BPTree {}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BPTreeMeta {}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiskNode {
    pub id: Uuid,
    pub parent: Option<Uuid>,
    pub node: Node,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Node {
    Internal(InternalNode),
    Leaf(LeafNode),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InternalNode;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LeafNode;

#[cfg(test)]
mod tests {}
