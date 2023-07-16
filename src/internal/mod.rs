//! Internal gRPC API for communications between databse nodes.

pub mod client;
pub mod server; 

pub mod gen {
    tonic::include_proto!("bdb.internal.v0");
}


