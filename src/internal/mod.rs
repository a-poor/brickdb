//! Internal gRPC API for communications between databse nodes.

pub mod client;
pub mod server; 

pub mod gen {
    tonic::include_proto!("brickdb.internal.v0");
}


