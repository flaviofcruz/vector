//! Fake Unity Catalog: the two endpoints the `databricks_zerobus` sink calls in `oauth` mode to
//! get its Arrow schema, served over plain HTTP on `--listen`.
//!
//! - `POST /oidc/v1/token`: returns the bearer token `databricks-bench-token`, whatever the body.
//! - `GET /api/2.1/unity-catalog/tables/<catalog>.<schema>.<table>`: the `--schema` file saved by
//!   `fetch-schema`, with `name`, `catalog_name`, `schema_name` and `full_name` set from the
//!   request. Segments are percent-decoded. A name that is not three dot-separated parts gets 400.
//! - Anything else gets 404.
//!
//! The sink needs these to build its Arrow schema. It fetches the table the first time it needs a
//! stream, and again only after Zerobus rejects its schema, when `recover_from_schema_rejection`
//! is set. The fake keeps no state and records nothing.

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

#[derive(clap::Args, Clone)]
pub struct FakeUcArgs {
    #[arg(long, default_value = "127.0.0.1:0")]
    pub listen: SocketAddr,
    /// `GET table` response saved by `fetch-schema`; its columns are served for every table.
    #[arg(long)]
    pub schema: PathBuf,
}

/// Starts the fake in this process, on the caller's runtime. Reads `--schema` up front.
pub fn start(args: FakeUcArgs) -> Result<RunningFake, BoxError> {
    let body = io_at(
        std::fs::read(&args.schema),
        "reading --schema",
        &args.schema,
    )?;
    let schema = fetch_schema::validate_table_response(&body)
        .map_err(|e| err(format!("--schema {}: {e}", args.schema.display())))?;

    let bound = hyper::Server::try_bind(&args.listen)
        .map_err(|e| err(format!("binding --listen {}: {e}", args.listen)))?;
    let schema = Arc::new(schema);
    let make = make_service_fn(move |_| {
        let schema = Arc::clone(&schema);
        async move { Ok::<_, Infallible>(service_fn(move |req| handle(Arc::clone(&schema), req))) }
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

async fn handle(schema: Arc<Value>, req: Request<Body>) -> Result<Response<Body>, Infallible> {
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
        (&Method::GET, Some(name)) => match table_body(&schema, name) {
            Some(body) => json_response(StatusCode::OK, body),
            None => json_response(
                StatusCode::BAD_REQUEST,
                json!({ "error_code": "INVALID_PARAMETER_VALUE" }),
            ),
        },
        _ => json_response(StatusCode::NOT_FOUND, json!({ "error_code": "NOT_FOUND" })),
    })
}

/// The table response for `name` (`<catalog>.<schema>.<table>`, still percent-encoded), or `None`
/// unless it has exactly three segments.
fn table_body(schema: &Value, name: &str) -> Option<Value> {
    let parts: Vec<String> = name.split('.').map(percent_decode).collect();
    let [catalog, schema_name, table] = parts.as_slice() else {
        return None;
    };
    let mut body = schema.clone();
    body["catalog_name"] = json!(catalog);
    body["schema_name"] = json!(schema_name);
    body["name"] = json!(table);
    body["full_name"] = json!(parts.join("."));
    Some(body)
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

    fn schema() -> Value {
        json!({
            "name": "saved",
            "catalog_name": "saved",
            "schema_name": "saved",
            "columns": [{ "name": "a", "type_name": "STRING" }]
        })
    }

    #[test]
    fn names_the_table_from_the_request() {
        let body = table_body(&schema(), "main.databricks_bench.cand-a").unwrap();
        assert_eq!(body["name"], "cand-a");
        assert_eq!(body["catalog_name"], "main");
        assert_eq!(body["schema_name"], "databricks_bench");
        assert_eq!(body["full_name"], "main.databricks_bench.cand-a");
        assert_eq!(body["columns"][0]["name"], "a");
    }

    #[test]
    fn decodes_each_segment() {
        let body = table_body(&schema(), "main.my%20schema.t%23").unwrap();
        assert_eq!(body["schema_name"], "my schema");
        assert_eq!(body["name"], "t#");
        assert_eq!(body["full_name"], "main.my schema.t#");
    }

    #[test]
    fn rejects_names_without_three_parts() {
        assert!(table_body(&schema(), "main.t").is_none());
        assert!(table_body(&schema(), "a.b.c.d").is_none());
    }
}
