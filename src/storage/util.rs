//! Utility functions for the storage module.

use anyhow::Result;


pub trait FileSystem {
    fn open(&self, file_path: &str) -> Result<FileHandle>;
    fn create(&mut self, file_path: &str) -> Result<()>;
    fn write(&mut self, file: &mut FileHandle, data: &[u8]) -> Result<()>;
    fn read(&self, file: &FileHandle, buffer: &mut [u8]) -> Result<usize>;
    fn delete(&mut self, file_path: &str) -> Result<()>;
}

pub struct FileHandle;


#[cfg(test)]
mod test {

}



