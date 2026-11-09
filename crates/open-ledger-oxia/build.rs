fn main() -> Result<(), Box<dyn std::error::Error>> {
    let proto = "../../proto/bookie/v1/bookie.proto";
    println!("cargo:rerun-if-changed={proto}");
    tonic_build::configure()
        .build_server(false)
        .build_client(true)
        .compile_protos(&[proto], &["../../proto"])?;
    Ok(())
}
