//! Runs the harness's unit tests: bench targets use `harness = false`, so tests inside them
//! would otherwise never execute.
#![allow(dead_code)]

#[path = "main.rs"]
mod databricks;
