use crate::storage::lsm::LSMTree;

/// A collection of documents. Equivalent to a table in a relational database.
/// 
/// Collections are stored in a [super::database::Database].
pub struct Collection {
    pub tree: LSMTree,
}


