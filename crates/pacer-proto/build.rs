//! Compiles `proto/pacer/v1/peer.proto` with a vendored protoc (no system
//! dependency for contributors or CI).

fn main() -> Result<(), Box<dyn std::error::Error>> {
    std::env::set_var("PROTOC", protoc_bin_vendored::protoc_bin_path()?);
    tonic_build::configure()
        .bytes([".pacer.v1.BlobChunk.data"])
        .compile_protos(&["proto/pacer/v1/peer.proto"], &["proto"])?;
    Ok(())
}
