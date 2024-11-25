use std::path;
use anyhow::{anyhow, Context, Result};
use bson::Bson;
use bson::oid::ObjectId;
use serde::{Serialize, Deserialize};
use uuid::Uuid;

/// The name of the metadata file for a B+ tree
/// index in the index directory.
const BPTREE_META_NAME: &str = "_meta.json";

/// BPTree represents a handle to a B+ tree index.
/// 
/// On disk, a BPTree has the following structure:
/// - `.../indexes/<index-uuid>/`: The directory for the index
/// - `.../indexes/<index-uuid>/_meta.json`: The index's metadata file
/// - `.../indexes/<index-uuid>/<node-id>.json`: One or more node files
pub struct BPTree {
    /// Metadata about the B+ tree
    pub meta: BPTreeMeta,

    /// The path to the index directory.
    pub dir_path: String,
}

impl BPTree {
    /// Creates a new B+ tree index.
    pub fn new() -> Self {
        todo!();
    }

    /// Loads a B+ tree index from disk.
    pub fn load(parent_dir_path: String, id: Uuid) -> Result<Self> {
        // Get the path to the index directory
        let sid = id.to_string();
        let idx_dir_path = path::Path::new(&parent_dir_path)
            .join(&sid);
        let dir_path = idx_dir_path.to_string_lossy().into();
        let meta_file_path = idx_dir_path.join(BPTREE_META_NAME);

        // Read the metadata file
        let meta_file_contents = std::fs::read_to_string(meta_file_path)
            .context(format!("Failed to read index ({}) metadata file", &sid))?;

        // Parse as a metadata file
        let meta: BPTreeMeta = serde_json::from_str(&meta_file_contents)
            .context(format!("Failed to parse index ({}) metadata file as json", &sid))?;

        // TODO - Validate the metadata file and confirm nodes exist?
        // ...

        // Create the b+ tree and return
        Ok(Self {
            dir_path,
            meta,
        })
    }

    /// Checks if the `value` is in the index.
    pub fn has(&self, value: Bson) -> Result<bool> {
        self
            .get_one(value)
            .map(|k| k.is_some())
    }

    /// Gets the ID of the first record in the index with the
    /// given `value`.
    pub fn get_one(&self, _value: Bson) -> Result<Option<ObjectId>> {
        todo!();
    }
    
    /// Gets the IDs of all records in the index with the
    /// given `value`.
    pub fn get_all(&self, _value: Bson) -> Result<Vec<ObjectId>> {
        todo!();
    }

    /// Returns all IDs for records where the index key's value
    /// is in the range from `from_value` to `to_value`, inclusive.
    pub fn scan(&self, _from_val: Bson, _to_val: Bson) -> Result<Vec<ObjectId>> {
        todo!();
    }

    /// Writes the tree's metadata to disk.
    fn write_meta(&self) -> Result<()> {
        // Get the path to the meta file
        let p = path::Path::new(&self.dir_path)
            .join(BPTREE_META_NAME);

        // Encode the metadata
        let b = serde_json::to_string(&self.meta)
            .context(format!("Failed to encode index ({}) metadata file as json", &self.meta.id))?;

        // Write it to disk
        std::fs::write(p, b)
            .context(format!("Failed to write index ({}) metadata file", &self.meta.id))
    }

    /// Creates a new node and writes it to disk.
    fn create_node(&self) -> Result<()> {
        todo!();
    }
    
    /// Gets a node with the given `id` from disk.
    fn get_node(&self, id: Uuid) -> Result<()> {
        // Check that a node with the given id exists
        if (&self).meta.node_ids.binary_search(&id).is_ok() {
            // TODO - Create custom error for this
            return Err(anyhow!("The node={} doesn't exist in the index={}", &self.meta.id, &id));
        }

        todo!()
    }

    fn update_node(&self) -> Result<()> {
        todo!();
    }

    /// Deletes a node with the given `id` from disk.
    fn delete_node(&self, _id: Uuid) -> Result<()> {
        // - delete the node
        // - remove it from the metadata (and write)
        todo!();
    }
}

/// BPTreeMeta stores metadata about a B+ tree index.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BPTreeMeta {
    /// The ID of the index.
    pub id: Uuid,

    /// The name of the index.
    pub name: String,

    /// The key being indexed.
    pub key: String,

    /// Does the index contain unique values?
    pub distinct: bool,

    /// The ID of the starting node.
    pub root_node_id: Option<Uuid>,

    /// The IDs of all nodes in the index.
    pub node_ids: Vec<Uuid>,
}

/// `DiskNode` represents a node from the index on disk.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiskNode {
    /// The ID of the node.
    pub id: Uuid,

    /// The ID of the node's parent.
    pub parent: Option<Uuid>,

    /// The content of the node.
    pub node: Node,
}

impl DiskNode {
    /// Creates a new `DiskNode` and writes it to disk.
    pub fn new() -> Self {
        todo!();
    }

    /// Loads a `DiskNode` from disk.
    pub fn load() -> Self {
        todo!();
    }

    /// Updates a `DiskNode` on disk.
    pub fn update() -> Result<()> {
        todo!();
    }

    /// Deletes a `DiskNode` from disk.
    pub fn delete() -> Result<()> {
        todo!();
    }
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
