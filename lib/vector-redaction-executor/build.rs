use std::io::Result;

fn main() -> Result<()> {
    println!("cargo:rerun-if-changed=proto/redaction_plan.proto");
    println!("cargo:rerun-if-changed=proto/compliance/taxonomy.proto");
    println!("cargo:rerun-if-changed=proto/compliance/shape.proto");
    // `proto` is the include root so `import "compliance/taxonomy.proto"` resolves. `include_file`
    // emits a single `_bindings.rs` wiring the per-package files into a nested module tree, which
    // the cross-package `compliance.DataLabel` reference requires.
    let mut config = prost_build::Config::new();
    config.include_file("_bindings.rs");
    config.compile_protos(&["proto/redaction_plan.proto"], &["proto"])?;
    Ok(())
}
