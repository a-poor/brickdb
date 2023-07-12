
/// The maximum number of tables per level in the LSM Tree.
/// 
/// Note: This value is fixed temporarily, for simplicity.
/// The goal is to remove this const and make it configurable.
pub const MAX_TABLES_PER_LEVEL: usize = 10;

/// The maximum number of records to store in the memtable 
/// before flushing to disk. This will also be the max size 
/// of a single SSTable in the first (on-disk) level of 
/// the LSM Tree.
/// 
/// Note: This value is fixed temporarily, for simplicity.
/// The goal is to remove this const and make it configurable.
pub const MEMTABLE_MAX_SIZE: usize = 100;

/// The fixed size of the bloom filter.
/// 
/// See also: [BLOOM_FILTER_ERROR_RATE]
/// 
/// Note: This value is fixed temporarily, for simplicity.
/// The goal is to remove this const and make it configurable.
pub const BLOOM_FILTER_SIZE: u32 = 1000;

/// The fixed error rate for level bloom filters.
/// 
/// See also: [BLOOM_FILTER_SIZE]
/// 
/// Note: This value is fixed temporarily, for simplicity.
/// The goal is to remove this const and make it configurable.
pub const BLOOM_FILTER_ERROR_RATE: f32 = 0.001;

/// The name of the metadata file for a level.
/// 
/// Note: This value is fixed for simplicity. This *may* change
/// or become a configurable option in the future.
pub const LEVEL_META_FILE: &str = "_meta.bson";

