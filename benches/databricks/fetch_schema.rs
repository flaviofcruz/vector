//! `fetch-schema`: saves a Unity Catalog table's GET-table response for `fake-uc` to serve.
//!
//! The schema comes from the workspace through the Databricks CLI rather than a copy in the repo,
//! so the fake answers with the columns the sink will really see. The CLI's stdout is parsed as
//! the response; its stderr is kept apart, so CLI notices cannot end up in the file.

use std::path::PathBuf;
use std::process::Command;

use super::{CmdResult, err, io_at};

#[derive(clap::Args, Clone)]
pub struct FetchSchemaArgs {
    /// Databricks CLI profile from `~/.databrickscfg` for the workspace that owns the table.
    #[arg(long)]
    pub profile: String,
    /// Full table name, `catalog.schema.table`.
    #[arg(long)]
    pub table: String,
    /// File to write the GET-table response to, for `--schema TABLE=PATH` (`check` or `fake-uc`).
    #[arg(long)]
    pub out: PathBuf,
}

pub fn fetch_schema(args: FetchSchemaArgs) -> CmdResult {
    let output = Command::new("databricks")
        .args([
            "tables",
            "get",
            &args.table,
            "-p",
            &args.profile,
            "-o",
            "json",
        ])
        .output()
        .map_err(|e| match e.kind() {
            std::io::ErrorKind::NotFound => err(
                "databricks CLI not found on PATH; install it and retry (the fakes need its \
                 output to know the table's columns)",
            ),
            _ => err(format!("running the databricks CLI: {e}")),
        })?;
    if !output.status.success() {
        return Err(err(format!(
            "`databricks tables get {}` failed ({}):\n{}\n\
             If the token has expired, re-authenticate with:\n  databricks auth login --profile {}",
            args.table,
            output.status,
            String::from_utf8_lossy(&output.stderr).trim_end(),
            args.profile,
        )));
    }
    if !output.stderr.is_empty() {
        eprint!("{}", String::from_utf8_lossy(&output.stderr));
    }

    let body = validate_table_response(&output.stdout)?;
    let columns = body["columns"].as_array().map_or(0, Vec::len);
    let pretty = serde_json::to_vec_pretty(&body)?;
    io_at(
        std::fs::write(&args.out, pretty),
        "writing --out",
        &args.out,
    )?;
    println!(
        "wrote {} ({columns} columns) to {}",
        args.table,
        args.out.display()
    );
    Ok(true)
}

/// Parses a GET-table response and checks the parts `fake-uc` and the sink rely on: a
/// non-empty `columns` array whose entries each carry a `name` and a `type_name`.
pub fn validate_table_response(
    body: &[u8],
) -> Result<serde_json::Value, Box<dyn std::error::Error + Send + Sync>> {
    let value: serde_json::Value = serde_json::from_slice(body).map_err(|e| {
        let text = String::from_utf8_lossy(body);
        err(format!(
            "table response is not JSON ({e}); starts with: {:?}",
            text.chars().take(120).collect::<String>()
        ))
    })?;
    let columns = value
        .get("columns")
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| err("table response has no `columns` array"))?;
    if columns.is_empty() {
        return Err(err("table response has an empty `columns` array"));
    }
    for (index, column) in columns.iter().enumerate() {
        let has = |key: &str| {
            column
                .get(key)
                .and_then(serde_json::Value::as_str)
                .is_some()
        };
        if !has("name") || !has("type_name") {
            return Err(err(format!(
                "column {index} lacks a string `name` or `type_name`"
            )));
        }
    }
    Ok(value)
}

#[cfg(test)]
#[allow(unused_imports, dead_code)] // Bench builds compile this module but never run it.
mod tests {
    use super::*;

    const COLUMN: &str = r#"{"name":"a","type_name":"STRING"}"#;

    #[test]
    fn accepts_a_table_response_with_columns() {
        let body = format!(r#"{{"name":"t","columns":[{COLUMN}]}}"#);
        assert!(validate_table_response(body.as_bytes()).is_ok());
    }

    #[test]
    fn rejects_empty_or_missing_columns() {
        assert!(validate_table_response(br#"{"name":"t","columns":[]}"#).is_err());
        assert!(validate_table_response(br#"{"name":"t"}"#).is_err());
    }

    #[test]
    fn rejects_a_column_without_a_type() {
        let body = br#"{"columns":[{"name":"a"}]}"#;
        assert!(validate_table_response(body).is_err());
    }

    #[test]
    fn reports_non_json_output_with_its_start() {
        let error = validate_table_response(b"Notice: skills\n{}").unwrap_err();
        assert!(error.to_string().contains("not JSON"));
    }
}
