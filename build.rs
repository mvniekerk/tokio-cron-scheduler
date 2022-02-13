use std::env;
use std::error::Error;
use std::path::Path;
use std::process::Command;

fn main() -> Result<(), Box<dyn Error>> {

    let out_dir = env::var("OUT_DIR").unwrap();
    let manifest_dir = env::var("CARGO_MANIFEST_DIR").unwrap();
    println!("Manifest {:}", manifest_dir);
    println!("Out {:}", out_dir);
    prost_build::compile_protos(&["./proto/job.proto"], &["./proto/"])?;
    Command::new("cp")
        .args(&[
            "za.co.agriio.job.rs",
            format!("{:}/src/job_data.rs", manifest_dir).as_str(),
        ])
        .current_dir(&Path::new(&out_dir))
        .status()
        .unwrap();
    println!("cargo:rerun-if-changed=proto/job.proto");
    Ok(())
}
