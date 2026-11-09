fn main() -> Result<(), Box<dyn std::error::Error>> {
    let bookie = "../../proto/bookie/v1/bookie.proto";
    println!("cargo:rerun-if-changed={bookie}");
    tonic_build::configure()
        .build_server(false)
        .build_client(true)
        .compile_protos(&[bookie], &["../../proto"])?;

    let kv = "../../proto/openkv/kv/v1/kv.proto";
    let ds = "../../proto/openkv/kv/v1/ds.proto";
    println!("cargo:rerun-if-changed={kv}");
    println!("cargo:rerun-if-changed={ds}");
    tonic_build::configure()
        .build_server(false)
        .build_client(true)
        .compile_protos(&[kv, ds], &["../../proto/openkv"])?;
    Ok(())
}
