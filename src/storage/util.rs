//! Utility functions for the storage module.

use std::path::Path;
use anyhow::Result;
use bson::Document;
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncWriteExt};


/// Write a document to disk.
/// 
/// If the file already exists, it will be overwritten.
/// 
/// # Arguments
/// 
/// * `path` - The path to write the document to.
/// * `doc` - The document to be written.
/// 
/// # Returns
/// 
/// * `Result<()>` - A result indicating whether the operation was successful.
pub async fn write_bson(path: impl AsRef<Path>, doc: &Document) -> Result<()> {
    // Write the document to a buffer...
    let mut buffer: Vec<u8> = vec![];
    doc.to_writer(&mut buffer)?;

    // Write to disk...
    let mut file = File::create(path).await?;
    file.write_all(&buffer).await?;

    // Sync the file...
    // Note: I'm adding this because I *was* getting intermittent errors
    //       during my tests. I'm not sure if this is the right solution
    //       but it seems to work for now.
    file.sync_all().await?;
    
    // Done!
    Ok(())
}


/// Read bson data from disk.
/// 
/// # Arguments
/// 
/// * `path` - The path to the file from which to read the bson data.
/// 
/// # Returns
/// 
/// * `Result<Vec<u8>>` - A result containing the document if the operation was successful.
pub async fn read_bson(path: impl AsRef<Path>) -> Result<Vec<u8>> {
    // Get the file...
    let mut file = File::open(path).await?;
    
    // Read the data in to a buffer...
    let mut buf: Vec<u8> = Vec::new();
    file.read_to_end(&mut buf).await?;

    // Done!
    Ok(buf)
}


#[cfg(test)]
mod tests {
    use super::*;
    use bson::doc;
    use tokio::fs;
    use anyhow::Result;

    #[tokio::test]
    async fn test_write_bson() -> Result<()> {
        // Define setup params...
        let path = "/tmp/test-write.bson";
        let doc = doc! {
            "name": "test",
            "value": 1
        };

        // Write the document...
        write_bson(path, &doc).await?;

        // Check that the file exists...
        let p = std::path::Path::new(path);
        assert!(p.exists());

        // Clean up...
        fs::remove_file(path).await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_read_and_write_bson() -> Result<()> {
        // Define setup params...
        let path = "/tmp/test-write-read.bson";
        let doc = doc! {
            "name": "test",
            "value": 1
        };

        // Write the document...
        write_bson(path, &doc).await?;
        
        // Read the data back in...
        let data = read_bson(path).await?;
        
        // Deserialize the data...
        let doc2 = bson::from_slice(&data)?;

        // Check that the documents are the same...
        assert_eq!(doc, doc2);

        // Clean up...
        fs::remove_file(path).await?;
        Ok(())
    }

}

