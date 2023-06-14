use serde::{Deserialize, Serialize};
use bson::{Document, doc};

#[derive(Debug, Serialize, Deserialize)]
enum Value<T> {
    Tombstone,
    Value(T),
}

#[derive(Debug, Serialize, Deserialize)]
struct Record {
    id: String,
    value: Value<Document>,
}

fn main() {
    // Define two records...
    let record_a = Record {
        id: "a".to_string(),
        value: Value::Value(doc! { "name": "Alice" }),
    };
    let record_b = Record {
        id: "c".to_string(),
        value: Value::Tombstone,
    };
    
    // Convert them to BSON...
    let doc_a = bson::to_document(&record_a).unwrap();
    let doc_b = bson::to_document(&record_b).unwrap();

    // Create two files...
    let mut file_a = std::fs::File::create("a.bson").unwrap();
    let mut file_b = std::fs::File::create("b.bson").unwrap();

    // Write the BSON to the files...
    doc_a.to_writer(&mut file_a).unwrap();
    doc_b.to_writer(&mut file_b).unwrap();

    // Read the BSON from the files...
    let doc_a = bson::Document::from_reader(&mut std::fs::File::open("a.bson").unwrap()).unwrap();
    let doc_b = bson::Document::from_reader(&mut std::fs::File::open("b.bson").unwrap()).unwrap();

    // Convert the BSON to records...
    let record_a: Record = bson::from_document(doc_a).unwrap();
    let record_b: Record = bson::from_document(doc_b).unwrap();

    // Print the records...
    println!("record_a: {:?}", record_a);
    println!("record_b: {:?}", record_b);

    // Delete the files...
    std::fs::remove_file("a.bson").unwrap();
    std::fs::remove_file("b.bson").unwrap();
}
