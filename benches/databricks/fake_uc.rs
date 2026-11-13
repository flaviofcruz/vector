//! Fake Unity Catalog: the two endpoints the `databricks_zerobus` sink calls in `oauth` mode to
//! get its Arrow schema, served over plain HTTP on `--listen`.
//!
//! - `POST /oidc/v1/token`: returns the bearer token `databricks-bench-token`, whatever the body.
//! - `GET /api/2.1/unity-catalog/tables/<catalog>.<schema>.<table>`: the `--schema` file given for
//!   that table, with `name`, `catalog_name`, `schema_name` and `full_name` set from the request.
//!   Segments are percent-decoded. A name that is not three dot-separated parts gets 400; a table
//!   that no `--schema` names gets 404, so the sink fails instead of building from another table's
//!   columns.
//! - Anything else gets 404.
//!
//! The sink needs these to build its Arrow schema. It fetches the table the first time it needs a
//! stream, and again only after Zerobus rejects its schema, when `recover_from_schema_rejection`
//! is set. The fake keeps no state and records nothing.

use std::collections::HashMap;
use std::convert::Infallible;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use hyper::service::{make_service_fn, service_fn};
use hyper::{Body, Method, Request, Response, StatusCode};
use serde_json::{Value, json};

use super::fakes::{BoxError, RunningFake, percent_decode, serve_until_signal};
use super::{CmdResult, err, fetch_schema, io_at};

const TOKEN_PATH: &str = "/oidc/v1/token";
const TABLES_PREFIX: &str = "/api/2.1/unity-catalog/tables/";

/// The saved schemas fake-uc serves, by full table name (`<catalog>.<schema>.<table>`).
type Tables = HashMap<String, Value>;

#[derive(clap::Args, Clone)]
pub struct FakeUcArgs {
    #[arg(long, default_value = "127.0.0.1:0")]
    pub listen: SocketAddr,
    /// `TABLE=PATH`, repeatable. TABLE is the full `<catalog>.<schema>.<table>` the sink asks for;
    /// PATH is the `GET table` response `fetch-schema` saved for it. Each table is served its own
    /// file, and a table with no `--schema` gets 404.
    #[arg(
        long = "schema",
        value_name = "TABLE=PATH",
        value_parser = parse_table_schema,
        required = true
    )]
    pub schemas: Vec<TableSchema>,
}

/// One `--schema TABLE=PATH`.
#[derive(Clone, Debug, PartialEq)]
pub struct TableSchema {
    /// `<catalog>.<schema>.<table>`, as the sink names the table.
    pub table: String,
    /// The `GET table` response saved by `fetch-schema`.
    pub path: PathBuf,
}

/// Parses `TABLE=PATH`. TABLE must have three non-empty dot-separated parts and PATH must name an
/// existing file, so both are refused as the argument is parsed.
pub fn parse_table_schema(spec: &str) -> Result<TableSchema, String> {
    let usage =
        || format!("--schema `{spec}`: expected TABLE=PATH, with TABLE as catalog.schema.table");
    let (table, path) = spec.split_once('=').ok_or_else(usage)?;
    let parts: Vec<&str> = table.split('.').collect();
    if parts.len() != 3 || parts.iter().any(|part| part.is_empty()) || path.is_empty() {
        return Err(usage());
    }
    let path = PathBuf::from(path);
    if !path.is_file() {
        return Err(format!("--schema `{spec}`: no file at {}", path.display()));
    }
    Ok(TableSchema {
        table: table.to_owned(),
        path,
    })
}

/// Refuses a TABLE given twice, since each table is served one file.
pub fn check_unique(schemas: &[TableSchema]) -> Result<(), String> {
    for (i, schema) in schemas.iter().enumerate() {
        if schemas[..i].iter().any(|seen| seen.table == schema.table) {
            return Err(format!(
                "--schema {}={}: the table is given more than once; each table takes one file",
                schema.table,
                schema.path.display()
            ));
        }
    }
    Ok(())
}

/// Starts the fake in this process, on the caller's runtime. Reads every `--schema` file up front.
pub fn start(args: FakeUcArgs) -> Result<RunningFake, BoxError> {
    check_unique(&args.schemas).map_err(err)?;
    let mut tables = Tables::new();
    for TableSchema { table, path } in &args.schemas {
        let body = io_at(std::fs::read(path), "reading --schema", path)?;
        let schema = fetch_schema::validate_table_response(&body)
            .map_err(|e| err(format!("--schema {table}={}: {e}", path.display())))?;
        tables.insert(table.clone(), schema);
    }

    let bound = hyper::Server::try_bind(&args.listen)
        .map_err(|e| err(format!("binding --listen {}: {e}", args.listen)))?;
    let tables = Arc::new(tables);
    let make = make_service_fn(move |_| {
        let tables = Arc::clone(&tables);
        async move { Ok::<_, Infallible>(service_fn(move |req| handle(Arc::clone(&tables), req))) }
    });
    let server = bound.serve(make);
    let addr = server.local_addr();
    let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
    let task = tokio::spawn(async move {
        server
            .with_graceful_shutdown(async move {
                let _ = stopped.await;
            })
            .await
            .map_err(|e| -> BoxError { e.into() })
    });
    Ok(RunningFake::new(addr, stop, task))
}

pub async fn serve(args: FakeUcArgs) -> CmdResult {
    serve_until_signal(start(args)?).await
}

async fn handle(tables: Arc<Tables>, req: Request<Body>) -> Result<Response<Body>, Infallible> {
    let path = req.uri().path();
    Ok(match (req.method(), path.strip_prefix(TABLES_PREFIX)) {
        (&Method::POST, _) if path == TOKEN_PATH => json_response(
            StatusCode::OK,
            json!({
                "access_token": "databricks-bench-token",
                "token_type": "Bearer",
                "expires_in": 3600
            }),
        ),
        (&Method::GET, Some(name)) => {
            let (status, body) = table_reply(&tables, name);
            json_response(status, body)
        }
        _ => json_response(StatusCode::NOT_FOUND, json!({ "error_code": "NOT_FOUND" })),
    })
}

/// The status and body for `name` (`<catalog>.<schema>.<table>`, still percent-encoded).
fn table_reply(tables: &Tables, name: &str) -> (StatusCode, Value) {
    let parts: Vec<String> = name.split('.').map(percent_decode).collect();
    let [catalog, schema_name, table] = parts.as_slice() else {
        return (
            StatusCode::BAD_REQUEST,
            json!({ "error_code": "INVALID_PARAMETER_VALUE" }),
        );
    };
    let Some(schema) = tables.get(&parts.join(".")) else {
        return (
            StatusCode::NOT_FOUND,
            json!({ "error_code": "TABLE_DOES_NOT_EXIST" }),
        );
    };
    let mut body = schema.clone();
    body["catalog_name"] = json!(catalog);
    body["schema_name"] = json!(schema_name);
    body["name"] = json!(table);
    body["full_name"] = json!(parts.join("."));
    (StatusCode::OK, body)
}

fn json_response(status: StatusCode, body: Value) -> Response<Body> {
    Response::builder()
        .status(status)
        .header("content-type", "application/json")
        .body(Body::from(body.to_string()))
        .expect("valid response")
}

#[cfg(test)]
#[allow(unused_imports, dead_code)] // Bench builds compile this module but never run it.
mod tests {
    use super::*;

    /// A saved schema whose one column is `column`, so a test can tell which file answered.
    fn saved(column: &str) -> Value {
        json!({
            "name": "saved",
            "catalog_name": "saved",
            "schema_name": "saved",
            "columns": [{ "name": column, "type_name": "STRING" }]
        })
    }

    fn tables() -> Tables {
        HashMap::from([
            ("main.databricks_bench.output".to_owned(), saved("a")),
            ("main.other.output2".to_owned(), saved("b")),
        ])
    }

    #[test]
    fn names_the_table_from_the_request() {
        let (status, body) = table_reply(&tables(), "main.databricks_bench.output");
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["name"], "output");
        assert_eq!(body["catalog_name"], "main");
        assert_eq!(body["schema_name"], "databricks_bench");
        assert_eq!(body["full_name"], "main.databricks_bench.output");
    }

    #[test]
    fn serves_each_table_its_own_schema() {
        let tables = tables();
        let (_, first) = table_reply(&tables, "main.databricks_bench.output");
        let (_, second) = table_reply(&tables, "main.other.output2");
        assert_eq!(first["columns"][0]["name"], "a");
        assert_eq!(second["columns"][0]["name"], "b");
    }

    #[test]
    fn a_table_no_schema_names_gets_404() {
        let (status, body) = table_reply(&tables(), "main.databricks_bench.missing");
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(body["error_code"], "TABLE_DOES_NOT_EXIST");
    }

    #[test]
    fn decodes_each_segment() {
        let tables = HashMap::from([("main.my schema.t#".to_owned(), saved("a"))]);
        let (status, body) = table_reply(&tables, "main.my%20schema.t%23");
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["schema_name"], "my schema");
        assert_eq!(body["name"], "t#");
        assert_eq!(body["full_name"], "main.my schema.t#");
    }

    #[test]
    fn rejects_names_without_three_parts() {
        let (status, _) = table_reply(&tables(), "main.t");
        assert_eq!(status, StatusCode::BAD_REQUEST);
        let (status, _) = table_reply(&tables(), "a.b.c.d");
        assert_eq!(status, StatusCode::BAD_REQUEST);
    }

    #[test]
    fn parses_a_table_and_its_file() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("t.json");
        std::fs::write(&file, b"{}").unwrap();
        let spec = format!("main.databricks_bench.output={}", file.display());
        assert_eq!(
            parse_table_schema(&spec).unwrap(),
            TableSchema {
                table: "main.databricks_bench.output".to_owned(),
                path: file,
            }
        );
    }

    #[test]
    fn refuses_a_malformed_spec_and_names_the_flag() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("t.json");
        std::fs::write(&file, b"{}").unwrap();
        let path = file.display();
        for bad in [
            "main.databricks_bench.output".to_owned(),
            "=/tmp/x".to_owned(),
            format!("main.output={path}"),
            format!("main..output={path}"),
            "main.databricks_bench.output=".to_owned(),
        ] {
            let error = parse_table_schema(&bad).unwrap_err();
            assert!(error.starts_with("--schema `"), "{bad}: {error}");
            assert!(error.contains("TABLE=PATH"), "{bad}: {error}");
        }
    }

    #[test]
    fn refuses_a_missing_file_and_names_it() {
        let dir = tempfile::tempdir().unwrap();
        let missing = dir.path().join("no-such.json");
        let spec = format!("main.databricks_bench.output={}", missing.display());
        let error = parse_table_schema(&spec).unwrap_err();
        assert!(error.starts_with("--schema `"), "{error}");
        assert!(error.contains(&missing.display().to_string()), "{error}");
    }

    #[test]
    fn refuses_a_table_given_twice() {
        let schema = |table: &str| TableSchema {
            table: table.to_owned(),
            path: PathBuf::from("t.json"),
        };
        assert!(check_unique(&[schema("main.s.a"), schema("main.s.b")]).is_ok());
        let error = check_unique(&[schema("main.s.a"), schema("main.s.a")]).unwrap_err();
        assert!(error.starts_with("--schema main.s.a=t.json"), "{error}");
        assert!(error.contains("given more than once"), "{error}");
    }
}
