//! Opt-in, synthetic large-snapshot memory probe. Run in a fresh test process with
//! `PUID_MEMORY_ROWS` and `PUID_MEMORY_RESULT` set; it exercises cold load and one refresh.
//! Use the same feature set, allocator, row count and build profile for both revisions.
//! RSS includes the local streaming server and runtime; HWM is cumulative since process start.

use std::{convert::Infallible, fmt::Write as _, time::Instant};

use futures::StreamExt;

use super::*;

fn resident_memory() -> serde_json::Value {
    let status = std::fs::read_to_string("/proc/self/status").unwrap();
    let bytes = |field: &str| {
        status
            .lines()
            .find_map(|line| line.strip_prefix(field))
            .unwrap()
            .split_whitespace()
            .next()
            .unwrap()
            .parse::<u64>()
            .unwrap()
            * 1024
    };
    serde_json::json!({
        "rss_bytes": bytes("VmRSS:"),
        "peak_rss_bytes": bytes("VmHWM:"),
    })
}

#[tokio::test]
#[ignore = "large synthetic snapshot; run separately to measure process RSS"]
async fn large_snapshot_cold_load_and_refresh_memory() {
    let count = std::env::var("PUID_MEMORY_ROWS")
        .unwrap_or_else(|_| "1000000".into())
        .parse::<usize>()
        .unwrap();
    assert!((1..=10_000_000).contains(&count));
    let result_path = std::env::var("PUID_MEMORY_RESULT").expect("set the result JSON path");
    // Generate each response in small chunks so the probe itself does not retain a second
    // full body or JSON tree. IDs are synthetic and exceed f64's exact-integer range.
    let uri = crate::test_util::http::spawn_blackhole_http_server(move |_| async move {
        let chunks = futures::stream::iter((0..count).step_by(1000)).map(move |start| {
            let end = (start + 1000).min(count);
            let mut chunk = String::with_capacity(64_000);
            if start == 0 {
                chunk.push('[');
            }
            for row in start..end {
                if row != 0 {
                    chunk.push(',');
                }
                let user_id = 9_007_199_254_740_993_i64 + row as i64;
                let puid = -1 - row as i64;
                write!(chunk, r#"{{"user_id":{user_id},"puid":{puid}}}"#).unwrap();
            }
            if end == count {
                chunk.push(']');
            }
            Ok::<_, Infallible>(chunk)
        });
        Ok::<_, Infallible>(http::Response::new(Body::wrap_stream(chunks)))
    })
    .await;
    let config = HttpConfig {
        url: uri.to_string(),
        schema: HashMap::from([
            ("user_id".into(), "integer".into()),
            ("puid".into(), "integer".into()),
        ]),
        request_timeout_secs: 1200,
        refresh_interval_secs: None,
        require_initial_load: true,
        persist: false,
        limits: super::super::config::LimitsConfig {
            max_pages: 1,
            max_rows: count + 1,
        },
        ..Default::default()
    };
    let before = resident_memory();
    let started = Instant::now();
    let mut table = HttpTable::new(config.clone(), &crate::config::GlobalOptions::default())
        .await
        .unwrap();
    let handle = table.add_index(Case::Sensitive, &["user_id"]).unwrap();
    let cold_load_secs = started.elapsed().as_secs_f64();
    let after_cold_load = resident_memory();

    for _ in 0..8 {
        assert_eq!(
            table.add_index(Case::Sensitive, &["user_id"]).unwrap(),
            handle
        );
    }
    let after_repeated_registration = resident_memory();
    let ctx = FetchContext::new(&config, TimeZone::default(), &ProxyConfig::default()).unwrap();
    let started = Instant::now();
    assert!(refresh_once(&table.shared, &ctx, &config, None).await);
    let refresh_secs = started.elapsed().as_secs_f64();
    let after_refresh = resident_memory();

    assert_eq!(table.shared.data.load().len(), count);
    for row in [0, count / 2, count - 1] {
        let condition = [Condition::Equals {
            field: "user_id",
            value: Value::Integer(9_007_199_254_740_993_i64 + row as i64),
        }];
        let found = table
            .find_table_row(Case::Sensitive, &condition, None, None, Some(handle))
            .unwrap();
        assert_eq!(found["puid"], Value::Integer(-1 - row as i64));
    }
    let missing = [Condition::Equals {
        field: "user_id",
        value: Value::Integer(-1),
    }];
    assert!(
        table
            .find_table_rows(Case::Sensitive, &missing, None, None, Some(handle))
            .unwrap()
            .is_empty()
    );
    std::fs::write(
        result_path,
        serde_json::to_vec_pretty(&serde_json::json!({
            "rows": count,
            "before": before,
            "after_cold_load": after_cold_load,
            "after_repeated_registration": after_repeated_registration,
            "after_refresh": after_refresh,
            "cold_load_secs": cold_load_secs,
            "refresh_secs": refresh_secs,
            "lookup_checks": "PASS",
        }))
        .unwrap(),
    )
    .unwrap();
}
