use crate::db::collection::Collection;
use anyhow::Result;
use std::collections::HashMap;

pub struct DBMeta {
    /// The name of the database.
    pub name: String,

    /// The path to the directory where this database's
    /// data is stored.
    pub path: String,
}

/// A representation of a database (a group of [Collection]s).
pub struct Database {
    /// The metadata for this database.
    pub meta: DBMeta,

    /// The collections in this database.
    pub collections: HashMap<String, Collection>,
}

impl Database {
    /// Creates a new database.
    pub fn new(name: &str, path: &str) -> Self {
        Database {
            meta: DBMeta {
                name: name.to_string(),
                path: path.to_string(),
            },
            collections: HashMap::new(),
        }
    }

    /// Load an existing database from disk.
    pub fn load() -> Result<Self> {
        todo!();
    }
}
