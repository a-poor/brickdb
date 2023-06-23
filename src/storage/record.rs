use std::cmp::Ordering;
use bson::Document;
use bson::oid::ObjectId;
use serde::{Deserialize, Serialize};


/// A record stored in an SSTable.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct Record {
    /// The record's unique key.
    pub key: ObjectId,

    /// The record's value.
    pub value: Value<Document>,
}

impl PartialOrd for Record {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.key.cmp(&other.key))
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
            })
        };
        let r2 = Record {
            key: ObjectId::new(),
            value: Value::Data(doc! {
                "msg": "What's up",
                "num": 0,
            })
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
            })
        };
        let r2 = Record {
            key: r1.key.clone(),
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
            })
        };
        let r2 = Record {
            key: r1.key.clone(),
            value: Value::Tombstone,
        };
        assert_ne!(r1, r2, "Same key but different values, shouldn't match");
        
        
        let r3 = Record {
            key: r1.key.clone(),
            value: Value::Tombstone,
        };
        assert_eq!(r2, r3, "Same key and same (tombstone) values, should match");
    }
}


