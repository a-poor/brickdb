


/// A struct representing an LSM Tree managing both in-memory and on-disk data.
pub struct LSMTree {
    /// The configuration for this LSM Tree.
    pub config: LSMTreeConfig,

    pub memtable: MemTable,

    pub levels: Vec<Level>,
}

/// Configuration for an LSM Tree.
/// 
/// Note that in this early iteration, the configuration will be very simple.
pub struct LSMTreeConfig {
    pub memtable_size: usize,

    pub max_sstables_level1: usize,
}

/// The in-memory buffer for an LSM Tree.
pub struct MemTable;

pub struct Level;

pub struct SSTableMeta;

pub struct SSTable;
