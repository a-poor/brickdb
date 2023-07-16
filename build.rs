fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::compile_protos("proto/bdb/v0/bdb.proto")?;
    tonic_build::compile_protos("proto/bdb/internal/v0/internal.proto")?;
    Ok(())
}