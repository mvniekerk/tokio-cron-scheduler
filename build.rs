use std::env;
use std::error::Error;
use std::fs;
use std::path::Path;

fn main() -> Result<(), Box<dyn Error>> {
    let out_dir = env::var("OUT_DIR").unwrap();
    let manifest_dir = env::var("CARGO_MANIFEST_DIR").unwrap();
    println!("Manifest {:}", manifest_dir);
    println!("Out {:}", out_dir);
    let mut prost_build = prost_build::Config::new();
    prost_build.protoc_arg("--experimental_allow_proto3_optional");
    prost_build.compile_protos(&["./proto/job.proto"], &["./proto/"])?;

    let src = Path::new(&out_dir).join("za.co.agriio.job.rs");
    let dst = Path::new(&manifest_dir).join("src/job/job_data.rs");

    fs::copy(&src, &dst).expect("Could not copy Protobuf file over");

    println!("cargo:rerun-if-changed=proto/job.proto");
    Ok(())
}
