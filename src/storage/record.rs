use bson::oid::ObjectId;
use bson::{doc, Document};
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;

/// A record stored in an SSTable.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct Record {
    /// The record's unique key.
    pub key: ObjectId,

    /// The record's value.
    pub value: Value<Document>,
}

impl Record {
    /// Creates a new record with the given key and value.
    pub fn new(value: Value<Document>) -> Self {
        let key = ObjectId::new();
        Self { key, value }
    }

    pub fn new_tombstone() -> Self {
        Self::new(Value::Tombstone)
    }

    pub fn new_data(doc: Document) -> Self {
        Self::new(Value::Data(doc))
    }
}

impl Default for Record {
    fn default() -> Self {
        Self::new_data(doc!())
    }
}

impl PartialOrd for Record {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.key.cmp(&other.key))
    }
}

impl Ord for Record {
    fn cmp(&self, other: &Self) -> Ordering {
        self.key.cmp(&other.key)
    }
}

/// A value stored in an SSTable. Can represent either a true value or a
/// tombstone (indicating that the record has been deleted).
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub enum Value<T> {
    /// A true value.
    Data(T),

    /// A tombstone value indicating that the record has been deleted.
    Tombstone,
}

impl Eq for Value<Document> {}

#[cfg(test)]
mod test {
    use super::*;
    use bson::doc;
    use bson::oid::ObjectId;

    #[test]
    fn record_equality_basic() {
        let r1 = Record {
            key: ObjectId::new(),
            value: Value::Data(doc! {
                "msg": "Hello, World",
                "num": 42,
            }),
        };
        let r2 = Record {
            key: ObjectId::new(),
            value: Value::Data(doc! {
                "msg": "What's up",
                "num": 0,
            }),
        };
        assert_ne!(r1, r2, "Different records, shouldn't match");
    }

    #[test]
    fn record_equality_cloned() {
        let r1 = Record {
            key: ObjectId::new(),
            value: Value::Data(doc! {
                "msg": "Hello, World",
                "num": 42,
            }),
        };
        let r2 = Record {
            key: r1.key,
            value: r1.value.clone(),
        };
        assert_eq!(r1, r2, "Records with cloned values, should match");
    }

    #[test]
    fn record_equality_tombstone() {
        let r1 = Record {
            key: ObjectId::new(),
            value: Value::Data(doc! {
                "msg": "What's up",
                "num": 0,
            }),
        };
        let r2 = Record {
            key: r1.key,
            value: Value::Tombstone,
        };
        assert_ne!(r1, r2, "Same key but different values, shouldn't match");

        let r3 = Record {
            key: r1.key,
            value: Value::Tombstone,
        };
        assert_eq!(r2, r3, "Same key and same (tombstone) values, should match");
    }

    #[test]
    fn record_ordering() {
        // Create object IDs and assert they're ordered...
        let oid1 =
            ObjectId::parse_str("649cbc250a24a2522fc95f74").expect("Couldn't parse ObjectId");
        let oid2 =
            ObjectId::parse_str("649cbc31f7ad863f0880dc04").expect("Couldn't parse ObjectId");
        assert!(oid1 < oid2, "oid1 should be less than oid2");

        // Double check the ordering...
        let d1 = oid1.timestamp();
        let d2 = oid2.timestamp();
        assert!(d1 < d2, "d1 should be less than d2");

        // Now create records with those object IDs...
        let r1 = Record {
            key: oid1,
            value: Value::Data(doc! {
                "msg": "Hello, World",
                "num": 42,
            }),
        };
        let r2 = Record {
            key: oid2,
            value: Value::Tombstone,
        };
        assert!(r1 < r2, "r1 should be less than r2");
    }

    #[test]
    fn search_for_record() {
        // Create object IDs and assert they're ordered...
        let r1 = Record {
            key: ObjectId::new(),
            value: Value::Data(doc! {
                "msg": "Hello, World",
                "num": 123,
            }),
        };
        let r2 = Record {
            key: ObjectId::new(),
            value: Value::Data(doc! {
                "msg": "Hello, World",
                "num": 456,
            }),
        };
        let r3 = Record {
            key: ObjectId::new(),
            value: Value::Data(doc! {
                "msg": "Hello, World",
                "num": 789,
            }),
        };

        // Create a vector of records...
        let records = vec![r1.clone(), r2.clone(), r3.clone()];

        // Search for the records...
        let p1 = records.binary_search(&Record {
            key: r1.key,
            value: Value::Tombstone,
        });
        let p2 = records.binary_search(&Record {
            key: r2.key,
            value: Value::Tombstone,
        });
        let p3 = records.binary_search(&Record {
            key: r3.key,
            value: Value::Tombstone,
        });

        // Check that the results are correct...
        assert_eq!(p1, Ok(0), "r1 should be at position 0");
        assert_eq!(p2, Ok(1), "r2 should be at position 1");
        assert_eq!(p3, Ok(2), "r3 should be at position 2");

        // Search for a record that doesn't exist...
        let p4 = records.binary_search(&Record {
            key: ObjectId::new(),
            value: Value::Tombstone,
        });

        // It should return an error showing that the record should
        // be inserted at the end, since the object ID was created
        // last and is therefore the largest...
        assert_eq!(p4, Err(3), "r4 should be at position 2");
    }
}
