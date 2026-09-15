use std::env;
use std::io::{Error, ErrorKind, Result};
use std::path::PathBuf;

fn main() -> Result<()> {
    println!("cargo:rerun-if-changed=proto/redaction_plan.proto");
    println!("cargo:rerun-if-changed=proto/compliance/taxonomy.proto");
    println!("cargo:rerun-if-changed=proto/compliance/shape.proto");
    println!("cargo:rerun-if-env-changed=DATABRICKS_VECTOR_REDACTOR_LIB_DIR");

    if env::var_os("CARGO_FEATURE_STATIC_EXECUTOR").is_some() {
        link_static_executor()?;
    }

    let mut config = prost_build::Config::new();
    config.include_file("_bindings.rs");
    config.compile_protos(&["proto/redaction_plan.proto"], &["proto"])?;
    Ok(())
}

fn link_static_executor() -> Result<()> {
    let library_directory = env::var_os("DATABRICKS_VECTOR_REDACTOR_LIB_DIR")
        .map(PathBuf::from)
        .ok_or_else(|| {
            Error::new(
                ErrorKind::NotFound,
                "DATABRICKS_VECTOR_REDACTOR_LIB_DIR must contain libvector_redactor.a",
            )
        })?;
    let library = library_directory.join("libvector_redactor.a");
    if !library.is_file() {
        return Err(Error::new(
            ErrorKind::NotFound,
            format!(
                "redaction executor archive not found at {}",
                library.display()
            ),
        ));
    }
    println!(
        "cargo:rustc-link-search=native={}",
        library_directory.display()
    );
    println!("cargo:rustc-link-lib=static=vector_redactor");
    for system_library in ["dl", "m", "pthread", "rt", "util"] {
        println!("cargo:rustc-link-lib={system_library}");
    }
    Ok(())
}
