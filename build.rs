fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Build the proto files in the `proto/` dir...
    tonic_build::compile_protos("proto/brickdb/v0/brickdb.proto")?;
    tonic_build::compile_protos("proto/brickdb/internal/v0/internal.proto")?;
    Ok(())
}
