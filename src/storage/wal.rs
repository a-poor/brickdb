use crate::storage::record::*;
use anyhow::Result;

/// A Write Ahead Log (WAL) that stores database writes
/// to disk for durability.
///
/// The WAL is a log of all database record modificiations. It's used
/// in case of a crash to ensure that all changes are persisted.
#[derive(Default, Debug, Clone)]
pub struct WAL;

impl WAL {
    /// Creates a new instance of the `WAL` struct.
    pub fn new() -> Self {
        todo!();
    }

    /// Loads a WAL connection from disk.
    pub fn load() -> Self {
        todo!();
    }

    /// Writes a record to the WAL.
    pub fn write(&self, _record: &Record) -> Result<()> {
        todo!();
    }

    /// Reads all records from the WAL.
    pub fn read(&self) -> Result<Vec<Record>> {
        todo!();
    }

    pub fn delete(&self) -> Result<Vec<Record>> {
        todo!();
    }
}

#[cfg(test)]
mod tests {}
