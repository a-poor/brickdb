use anyhow::{anyhow, Context, Result};
use bson::oid::ObjectId;
use bson::Bson;
use serde::{Deserialize, Serialize};
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
        let idx_dir_path = std::path::Path::new(&parent_dir_path).join(&sid);
        let dir_path = idx_dir_path.to_string_lossy().into();
        let meta_file_path = idx_dir_path.join(BPTREE_META_NAME);

        // Read the metadata file
        let meta_file_contents = std::fs::read_to_string(meta_file_path)
            .context(format!("Failed to read index ({}) metadata file", &sid))?;

        // Parse as a metadata file
        let meta: BPTreeMeta = serde_json::from_str(&meta_file_contents).context(format!(
            "Failed to parse index ({}) metadata file as json",
            &sid
        ))?;

        // TODO - Validate the metadata file and confirm nodes exist?
        // ...

        // Create the b+ tree and return
        Ok(Self { dir_path, meta })
    }

    /// Checks if the `value` is in the index.
    pub fn has(&self, value: Bson) -> Result<bool> {
        self.get_one(value).map(|k| k.is_some())
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
        let p = std::path::Path::new(&self.dir_path)
            .join(BPTREE_META_NAME);

        // Encode the metadata
        let b = serde_json::to_string(&self.meta).context(format!(
            "Failed to encode index ({}) metadata file as json",
            &self.meta.id
        ))?;

        // Write it to disk
        std::fs::write(p, b).context(format!(
            "Failed to write index ({}) metadata file",
            &self.meta.id
        ))
    }

    /// Creates a new node and writes it to disk.
    fn create_node(&mut self, parent: Option<Uuid>, node: Node) -> Result<DiskNode> {
        // Create the node
        // (Note: This also writes it to disk)
        let node = DiskNode::new(&self.dir_path, parent, node)?;

        // If the node is the root (aka no parent), mark that
        // in the metadata
        if parent.is_none() {
            self.meta.root_node_id = Some(node.id);
        }

        // Add the key to the metadata
        let pos = match self.meta.node_ids.binary_search(&node.id) {
            Ok(pos) => pos,
            Err(pos) => pos,
        };
        self.meta.node_ids.insert(pos, node.id);

        // Re-write the metadata
        self.write_meta()?;

        // Return the node
        Ok(node)
    }

    /// Gets a node with the given `id` from disk.
    fn get_node(&self, id: Uuid) -> Result<DiskNode> {
        // Check that a node with the given id exists
        if (&self).meta.node_ids.binary_search(&id).is_ok() {
            // TODO - Create custom error for this
            return Err(anyhow!(
                "The node={} doesn't exist in the index={}",
                &self.meta.id,
                &id
            ));
        }

        DiskNode::load(&self.dir_path, id)
    }

    fn update_node_content(&mut self, id: Uuid, node: Node) -> Result<()> {
        // Does the node exist?
        if (&self).meta.node_ids.binary_search(&id).is_err() {
            // TODO - Create custom error for this
            return Err(anyhow!(
                "The node={} doesn't exist in the index={}",
                &self.meta.id,
                &id
            ));
        }
        todo!();
    }

    /// Deletes a node with the given `id` from disk.
    /// 
    /// Note: This may need to be replaced with something or some things
    /// more case-specific for cases like moving/merging/splitting nodes.
    /// And those things may need to perform multiple operations before 
    /// the disk-updates get flushed (e.g. re-write metadata).
    fn delete_node(&mut self, id: Uuid) -> Result<()> {
        // Delete it from the metadata and write
        match self.meta.node_ids.binary_search(&id) {
            Ok(pos) => {
                // Remove
                self.meta.node_ids.remove(pos);

                // Re-write
                self.write_meta()?;
            },
            Err(_pos) => {},
        };
        Ok(())
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
    pub fn new(dir_name: &str, parent: Option<Uuid>, node: Node) -> Result<Self> {
        let id = Uuid::new_v4();
        let node = DiskNode { id, parent, node };
        node.write(dir_name)
            .context(format!("Failed to write node={} to disk", &id))?;
        Ok(node)
    }

    /// Loads a `DiskNode` from disk.
    pub fn load(dir_name: &str, id: Uuid) -> Result<Self> {
        let p = std::path::Path::new(&dir_name)
            .join(id.to_string());
        let b = std::fs::read(&p)
            .context(format!("Failed to read node={} from disk", &id))?;
        let node: DiskNode = bson::from_slice(&b)
            .context(format!("Failed to parse node={} from json", &id))?;
        Ok(node)
    }

    /// Writes a `DiskNode` to disk.
    pub fn write(&self, dir_name: &str) -> Result<()> {
        let p = self.file_path(&dir_name);
        let b = bson::to_vec(&self)
            .context(format!("Failed to encode node={} as json", &self.id))?;
        std::fs::write(p, b)
            .context(format!("Failed to write node={} to disk", &self.id))?;
        Ok(())
    }

    /// Deletes a `DiskNode` from disk.
    pub fn delete(&self, dir_name: &str) -> Result<()> {
        let p = self.file_path(&dir_name);
        std::fs::remove_file(p).context(format!("Failed to delete node={} from disk", &self.id))?;
        Ok(())
    }

    fn file_path(&self, dir_name: &str) -> String {
        std::path::Path::new(&dir_name)
            .join(self.id.to_string())
            .to_string_lossy()
            .into()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Node {
    Internal(InternalNode),
    Leaf(LeafNode),
}

/// Internal nodes contain pointers from key ranges 
/// to other nodes -- either internal or leaf.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InternalNode;

/// Leaf nodes contain pointers from keys to record IDs.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LeafNode;

#[cfg(test)]
mod tests {}
