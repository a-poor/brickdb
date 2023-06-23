use std::collections::BTreeMap;
use bson::{Document, DateTime};
use bson::oid::ObjectId;
use anyhow::{anyhow, Result};

use crate::storage::record::*;
use crate::storage::sstable::*;


/// The in-memory buffer for an LSM Tree.
/// 
/// This buffer is comprised of a red-black tree of records, sorted by key.
pub struct MemTable {
    pub records: BTreeMap<ObjectId, Value<Document>>,
}

impl MemTable {
    /// Creates a new MemTable.
    pub fn new() -> Self {
        Self::default()
    }

    /// Flushes the contents of the MemTable to disk, returning an SSTable.
    pub fn flush(&mut self) -> Result<SSTable> {
        // Create a vector of records from the BTreeMap...
        let records: Vec<_> = self.records
            .iter()
            .map(|(key, value)| {
                Record {
                    key: *key,
                    value: value.clone(),
                }
            })
            .collect();

        // Get the min/max keys and count from the records...
        let min_key = records
            .first()
            .ok_or(anyhow!("records vec was empty"))?
            .key;
        let max_key = records
            .last()
            .ok_or(anyhow!("records vec was empty"))?
            .key;
        let num_records = records.len();
        let meta = SSTableMeta {
            id: ObjectId::new(),
            created_at: DateTime::now(),
            min_key,
            max_key,
            num_records,
        };

        // Create and return!
        Ok(SSTable {
            meta,
            records,
        })
    }
}

impl Default for MemTable {
    fn default() -> Self {
        MemTable {
            records: BTreeMap::new(),
        }
    }
}


#[cfg(test)]
mod test {

}



