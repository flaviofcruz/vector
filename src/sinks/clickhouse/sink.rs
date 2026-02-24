//! Implementation of the `clickhouse` sink.

use std::{
    collections::HashMap,
    sync::Mutex,
    task::{Context, Poll},
    time::Instant,
};

use super::{config::Format, request_builder::ClickhouseRequestBuilder};
use crate::{
    internal_events::{ClickhouseBatchFlushed, ClickhouseBatchInterval, ClickhouseInsertCompleted},
    sinks::{prelude::*, util::http::HttpRequest},
};

pub struct ClickhouseSink<S> {
    batch_settings: BatcherSettings,
    service: S,
    database: Template,
    table: Template,
    format: Format,
    request_builder: ClickhouseRequestBuilder,
}

impl<S> ClickhouseSink<S>
where
    S: Service<HttpRequest<PartitionKey>> + Send + 'static,
    S::Future: Send + 'static,
    S::Response: DriverResponse + Send + 'static,
    S::Error: std::fmt::Debug + Into<crate::Error> + Send,
{
    pub const fn new(
        batch_settings: BatcherSettings,
        service: S,
        database: Template,
        table: Template,
        format: Format,
        request_builder: ClickhouseRequestBuilder,
    ) -> Self {
        Self {
            batch_settings,
            service,
            database,
            table,
            format,
            request_builder,
        }
    }

    async fn run_inner(self: Box<Self>, input: BoxStream<'_, Event>) -> Result<(), ()> {
        let batch_settings = self.batch_settings;
        let last_flush_times: Mutex<HashMap<PartitionKey, Instant>> = Mutex::new(HashMap::new());

        input
            .batched_partitioned(
                KeyPartitioner::new(self.database, self.table, self.format),
                || batch_settings.as_byte_size_config(),
            )
            .filter_map(|(key, batch)| async move { key.map(move |k| (k, batch)) })
            .map(|(key, batch)| {
                let event_count = batch.len();
                let in_memory_byte_size: usize = batch.iter().map(|e| e.size_of()).sum();

                emit!(ClickhouseBatchFlushed {
                    event_count,
                    in_memory_byte_size,
                });

                let now = Instant::now();
                {
                    let mut map = last_flush_times.lock().unwrap_or_else(|e| e.into_inner());
                    if let Some(last_time) = map.get(&key) {
                        emit!(ClickhouseBatchInterval {
                            interval: now.duration_since(*last_time),
                        });
                    }
                    map.insert(key.clone(), now);
                }

                (key, batch)
            })
            .request_builder(
                default_request_builder_concurrency_limit(),
                self.request_builder,
            )
            .filter_map(|request| async {
                match request {
                    Err(error) => {
                        emit!(SinkRequestBuildError { error });
                        None
                    }
                    Ok(req) => Some(req),
                }
            })
            .into_driver(InstrumentedClickhouseService {
                inner: self.service,
            })
            .run()
            .await
    }
}

#[async_trait::async_trait]
impl<S> StreamSink<Event> for ClickhouseSink<S>
where
    S: Service<HttpRequest<PartitionKey>> + Send + 'static,
    S::Future: Send + 'static,
    S::Response: DriverResponse + Send + 'static,
    S::Error: std::fmt::Debug + Into<crate::Error> + Send,
{
    async fn run(
        self: Box<Self>,
        input: futures_util::stream::BoxStream<'_, Event>,
    ) -> Result<(), ()> {
        self.run_inner(input).await
    }
}

/// PartitionKey used to partition events by (database, table) pair.
#[derive(Hash, Eq, PartialEq, Clone, Debug)]
pub struct PartitionKey {
    pub database: String,
    pub table: String,
    pub format: Format,
}

/// KeyPartitioner that partitions events by (database, table) pair.
struct KeyPartitioner {
    database: Template,
    table: Template,
    format: Format,
}

impl KeyPartitioner {
    const fn new(database: Template, table: Template, format: Format) -> Self {
        Self {
            database,
            table,
            format,
        }
    }

    fn render(template: &Template, item: &Event, field: &'static str) -> Option<String> {
        template
            .render_string(item)
            .map_err(|error| {
                emit!(TemplateRenderingError {
                    error,
                    field: Some(field),
                    drop_event: true,
                });
            })
            .ok()
    }
}

impl Partitioner for KeyPartitioner {
    type Item = Event;
    type Key = Option<PartitionKey>;

    fn partition(&self, item: &Self::Item) -> Self::Key {
        let database = Self::render(&self.database, item, "database_key")?;
        let table = Self::render(&self.table, item, "table_key")?;
        Some(PartitionKey {
            database,
            table,
            format: self.format,
        })
    }
}

/// Service wrapper that measures insert latency and compressed batch size for
/// each ClickHouse HTTP request.
struct InstrumentedClickhouseService<S> {
    inner: S,
}

impl<S> Service<HttpRequest<PartitionKey>> for InstrumentedClickhouseService<S>
where
    S: Service<HttpRequest<PartitionKey>> + Send + 'static,
    S::Future: Send + 'static,
    S::Response: DriverResponse + Send + 'static,
    S::Error: std::fmt::Debug + Into<crate::Error> + Send,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = BoxFuture<'static, Result<S::Response, S::Error>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, request: HttpRequest<PartitionKey>) -> Self::Future {
        let compressed_byte_size = request.get_metadata().request_encoded_size();
        let start = Instant::now();
        let future = self.inner.call(request);

        Box::pin(async move {
            let result = future.await;
            let latency = start.elapsed();
            let status = match &result {
                Ok(response) => match response.event_status() {
                    EventStatus::Delivered => "success",
                    EventStatus::Errored => "error",
                    EventStatus::Rejected => "rejected",
                    _ => "error",
                },
                Err(_) => "error",
            };
            emit!(ClickhouseInsertCompleted {
                latency,
                compressed_byte_size,
                status,
            });
            result
        })
    }
}
