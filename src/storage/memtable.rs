use anyhow::{anyhow, Result};
use bson::oid::ObjectId;
use bson::{DateTime, Document};
use std::collections::BTreeMap;

use crate::storage::conf::*;
use crate::storage::record::*;
use crate::storage::sstable::*;

/// The in-memory buffer for an LSM Tree.
///
/// This buffer is comprised of a red-black tree of records, sorted by key.
#[derive(Default, Debug, Clone)]
pub struct MemTable {
    /// The records in the MemTable.
    pub records: BTreeMap<ObjectId, Value<Document>>,

    /// The maximum number of records allowed in the MemTable.
    pub max_records: usize,
}

impl MemTable {
    /// Creates a new MemTable.
    pub fn new() -> Self {
        Self {
            max_records: MEMTABLE_MAX_SIZE,
            ..Default::default()
        }
    }

    /// Inserts a record into the MemTable.
    pub fn insert(&mut self, key: &ObjectId, value: Value<Document>) {
        self.records.insert(*key, value);
    }

    /// Sets a value in the MemTable.
    pub fn set(&mut self, key: &ObjectId, doc: Document) {
        self.insert(key, Value::Data(doc));
    }

    /// Delete's a value in the MemTable.
    ///
    /// Note that this doesn't remove the key from the MemTable, but instead
    /// sets the value to a tombstone.
    pub fn del(&mut self, key: &ObjectId) {
        self.insert(key, Value::Tombstone);
    }

    /// Gets a value from the MemTable.
    pub fn get(&self, key: &ObjectId) -> Option<Value<Document>> {
        self.records.get(key).cloned()
    }

    /// Flushes the contents of the MemTable to an SSTable.
    pub fn flush(&self) -> Result<SSTable> {
        // Create a vector of records from the BTreeMap...
        let records: Vec<_> = self
            .records
            .iter()
            .map(|(key, value)| Record {
                key: *key,
                value: value.clone(),
            })
            .collect();

        // Get the min/max keys and count from the records...
        let min_key = records.first().ok_or(anyhow!("records vec was empty"))?.key;
        let max_key = records.last().ok_or(anyhow!("records vec was empty"))?.key;
        let num_records = records.len();
        let meta = SSTableMeta {
            table_id: ObjectId::new(),
            created_at: DateTime::now(),
            min_key,
            max_key,
            num_records,
        };

        // Create and return!
        Ok(SSTable { meta, records })
    }

    pub fn clear(&mut self) {
        self.records.clear();
    }

    /// Check the size of the MemTable.
    pub fn size(&self) -> usize {
        self.records.len()
    }

    /// Check if the MemTable is full.
    pub fn is_full(&self) -> bool {
        self.size() >= self.max_records
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use bson::doc;

    #[test]
    fn set_and_get() {
        // Define a key/value pair to add to the memtable...
        let k = ObjectId::new();
        let v = doc! {
            "msg": "Hello, World!",
            "num": 42,
            "skyIsBlue": true,
        };
        let exp = Some(Value::Data(v.clone()));

        // Create an empty memtable...
        let mut mt = MemTable::new();

        // Add it to the memtable...
        mt.set(&k, v);

        // Check that the BTree contains the key...
        assert!(
            mt.records.contains_key(&k),
            "Key doesn't exist in the btree"
        );

        // Get the value back from the memtable...
        let res = mt.get(&k);

        // Check that it matches...
        assert_eq!(res, exp);
    }

    #[test]
    fn set_del_get() {
        // Define a key/value pair to add to the memtable...
        let k = ObjectId::new();
        let v = doc! {
            "msg": "Hello, World!",
            "num": 42,
            "skyIsBlue": true,
        };

        // Create an empty memtable...
        let mut mt = MemTable::new();

        // Add it to the memtable...
        mt.set(&k, v);

        // Check that the BTree contains the key...
        assert!(
            mt.records.contains_key(&k),
            "Key doesn't exist in the btree"
        );

        // Delete the value from the memtable...
        mt.del(&k);

        // Check that the key is still in the btree...
        assert!(
            mt.records.contains_key(&k),
            "Key should still exist in the btree after 'deletion'"
        );

        // Check that the returned value is a tombstone...
        let res = mt.get(&k);
        let exp = Some(Value::<Document>::Tombstone);
        assert_eq!(res, exp, "Expecting a present tombstone");
    }
}
