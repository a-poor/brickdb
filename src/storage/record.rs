use bson::Document;
use bson::oid::ObjectId;
use serde::{Deserialize, Serialize};


/// A record stored in an SSTable.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Record {
    /// The record's unique key.
    pub key: ObjectId,

    /// The record's value.
    pub value: Value<Document>,
}

/// A value stored in an SSTable. Can represent either a true value or a 
/// tombstone (indicating that the record has been deleted).
#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum Value<T> {
    /// A true value.
    Data(T),

    /// A tombstone value indicating that the record has been deleted.
    Tombstone,
}


#[cfg(test)]
mod test {

}


