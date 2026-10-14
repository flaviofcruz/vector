#![deny(missing_docs)]

use std::{
    collections::HashMap,
    time::{Duration, Instant},
};

use bytes::BytesMut;
use futures::{Stream, StreamExt};
use vector_lib::{
    config::LogNamespace,
    lookup::OwnedTargetPath,
    stream::expiration_map::{Emitter, map_with_expiration},
};
use vrl::owned_value_path;

use crate::{
    event,
    event::{Event, LogEvent, Value},
    internal_events::KubernetesMergedLineTooBigError,
    sources::kubernetes_logs::transform_utils::get_message_path,
};

/// The key we use for `file` field.
const FILE_KEY: &str = "file";

const EXPIRATION_TIME: Duration = Duration::from_secs(30);

struct PartialEventMergeState {
    buckets: HashMap<String, Bucket>,
    maybe_max_merged_line_bytes: Option<usize>,
}

impl PartialEventMergeState {
    fn add_event(
        &mut self,
        mut event: LogEvent,
        file: &str,
        message_path: &OwnedTargetPath,
        expiration_time: Duration,
    ) {
        let mut bytes_mut = BytesMut::new();
        if let Some(bucket) = self.buckets.get_mut(file) {
            // don't bother continuing to process new partial events that match existing ones that are already too big
            if bucket.exceeds_max_merged_line_limit {
                return;
            }

            // merging with existing event

            if let (Some(Value::Bytes(prev_value)), Some(Value::Bytes(new_value))) =
                (bucket.event.get_mut(message_path), event.get(message_path))
            {
                bytes_mut.extend_from_slice(prev_value);
                bytes_mut.extend_from_slice(new_value);
                let content_offset = prev_value.len();

                // drop event if it's bigger than max allowed
                if let Some(max_merged_line_bytes) = self.maybe_max_merged_line_bytes
                    && bytes_mut.len() > max_merged_line_bytes
                {
                    bucket.exceeds_max_merged_line_limit = true;
                    // perf impact of clone should be minimal since being here means no further processing of this event will occur
                    emit!(KubernetesMergedLineTooBigError {
                        event: &Value::Bytes(new_value.clone()),
                        configured_limit: max_merged_line_bytes,
                        encountered_size_so_far: bytes_mut.len()
                    });
                }

                *prev_value = bytes_mut.freeze();

                if let Some(accounting) = event.metadata_mut().delivery_path_accounting_mut() {
                    accounting.shift_content(Some(content_offset));
                }

                // Each CRI fragment was counted at READ, before partial merging.
                let count = bucket
                    .event
                    .metadata()
                    .delivery_event_count()
                    .saturating_add(event.metadata().delivery_event_count());
                bucket.event.metadata_mut().merge(event.into_parts().1);
                bucket.event.metadata_mut().set_delivery_event_count(count);
            }
        } else {
            // new event

            let mut exceeds_max_merged_line_limit = false;

            if let Some(Value::Bytes(event_bytes)) = event.get(message_path) {
                bytes_mut.extend_from_slice(event_bytes);
                if let Some(max_merged_line_bytes) = self.maybe_max_merged_line_bytes {
                    exceeds_max_merged_line_limit = bytes_mut.len() > max_merged_line_bytes;

                    if exceeds_max_merged_line_limit {
                        // perf impact of clone should be minimal since being here means no further processing of this event will occur
                        emit!(KubernetesMergedLineTooBigError {
                            event: &Value::Bytes(event_bytes.clone()),
                            configured_limit: max_merged_line_bytes,
                            encountered_size_so_far: bytes_mut.len()
                        });
                    }
                }
            }

            self.buckets.insert(
                file.to_owned(),
                Bucket {
                    event,
                    expiration: Instant::now() + expiration_time,
                    exceeds_max_merged_line_limit,
                },
            );
        }
    }

    fn remove_event(&mut self, file: &str) -> Option<LogEvent> {
        self.buckets
            .remove(file)
            .filter(|bucket| !bucket.exceeds_max_merged_line_limit)
            .map(|bucket| bucket.event)
    }

    fn emit_expired_events(&mut self, emitter: &mut Emitter<LogEvent>) {
        let now = Instant::now();
        self.buckets.retain(|_key, bucket| {
            let expired = now >= bucket.expiration;
            if expired && !bucket.exceeds_max_merged_line_limit {
                emitter.emit(bucket.event.clone());
            }
            !expired
        });
    }

    fn flush_events(&mut self, emitter: &mut Emitter<LogEvent>) {
        for (_, bucket) in self.buckets.drain() {
            if !bucket.exceeds_max_merged_line_limit {
                emitter.emit(bucket.event);
            }
        }
    }
}

struct Bucket {
    event: LogEvent,
    expiration: Instant,
    exceeds_max_merged_line_limit: bool,
}

pub fn merge_partial_events(
    stream: impl Stream<Item = Event> + 'static,
    log_namespace: LogNamespace,
    maybe_max_merged_line_bytes: Option<usize>,
) -> impl Stream<Item = Event> {
    merge_partial_events_with_custom_expiration(
        stream,
        log_namespace,
        EXPIRATION_TIME,
        maybe_max_merged_line_bytes,
    )
}

// internal function that allows customizing the expiration time (for testing)
fn merge_partial_events_with_custom_expiration(
    stream: impl Stream<Item = Event> + 'static,
    log_namespace: LogNamespace,
    expiration_time: Duration,
    maybe_max_merged_line_bytes: Option<usize>,
) -> impl Stream<Item = Event> {
    let partial_flag_path = match log_namespace {
        LogNamespace::Vector => {
            OwnedTargetPath::metadata(owned_value_path!(super::Config::NAME, event::PARTIAL))
        }
        LogNamespace::Legacy => OwnedTargetPath::event(owned_value_path!(event::PARTIAL)),
    };

    let file_path = match log_namespace {
        LogNamespace::Vector => {
            OwnedTargetPath::metadata(owned_value_path!(super::Config::NAME, FILE_KEY))
        }
        LogNamespace::Legacy => OwnedTargetPath::event(owned_value_path!(FILE_KEY)),
    };

    let state = PartialEventMergeState {
        buckets: HashMap::new(),
        maybe_max_merged_line_bytes,
    };

    let message_path = get_message_path(log_namespace);

    map_with_expiration(
        state,
        stream.map(|e| e.into_log()),
        Duration::from_secs(1),
        move |state: &mut PartialEventMergeState,
              event: LogEvent,
              emitter: &mut Emitter<LogEvent>| {
            // called for each event
            let is_partial = event
                .get(&partial_flag_path)
                .and_then(|x| x.as_boolean())
                .unwrap_or(false);

            let file = event
                .get(&file_path)
                .and_then(|x| x.as_str())
                .map(|x| x.to_string())
                .unwrap_or_default();

            state.add_event(event, &file, &message_path, expiration_time);
            if !is_partial && let Some(log_event) = state.remove_event(&file) {
                emitter.emit(log_event);
            }
        },
        |state: &mut PartialEventMergeState, emitter: &mut Emitter<LogEvent>| {
            // check for expired events
            state.emit_expired_events(emitter)
        },
        |state: &mut PartialEventMergeState, emitter: &mut Emitter<LogEvent>| {
            // the source is ending, flush all pending events
            state.flush_events(emitter);
        },
    )
    // LogEvent -> Event
    .map(|e| e.into())
}

#[cfg(test)]
mod test {
    use vector_common::internal_event::vector_event::delivery_path::{
        DeliveryPathAccounting, DeliverySourceLabels,
    };
    use vector_lib::event::LogEvent;
    use vrl::value;

    use super::*;

    fn counted_fragments(namespace: LogNamespace, finish: bool) -> Vec<Event> {
        [2, 3, 5]
            .into_iter()
            .enumerate()
            .map(|(index, count)| {
                let mut event = match namespace {
                    LogNamespace::Legacy => LogEvent::from("part"),
                    LogNamespace::Vector => LogEvent::from(value!("part")),
                };
                let partial_path = match namespace {
                    LogNamespace::Legacy => OwnedTargetPath::event(owned_value_path!("_partial")),
                    LogNamespace::Vector => OwnedTargetPath::metadata(owned_value_path!(
                        super::super::Config::NAME,
                        "_partial"
                    )),
                };
                event.insert(&partial_path, index < 2 || !finish);
                event.metadata_mut().set_delivery_event_count(count);
                let mut accounting = DeliveryPathAccounting::at_read(
                    &DeliverySourceLabels {
                        process_generation_id: "source-process".to_owned(),
                        time_parity: if index < 2 { 0 } else { 3_600_000 },
                        topic: "service-log".to_owned(),
                        delivery_method: "VECTOR_WOODCHUCK_V2_FILE".to_owned(),
                        service_system: "test-service".to_owned(),
                        source_pod_id: "source-pod".to_owned(),
                        workspace_id: None,
                    },
                    ["online".to_owned()],
                );
                accounting.counts[0].count = count as u64;
                accounting.record_content(4);
                event
                    .metadata_mut()
                    .set_delivery_path_accounting(Some(accounting));
                event.into()
            })
            .collect()
    }

    #[tokio::test]
    async fn partial_merge_preserves_delivery_counts_and_source_hours() {
        for namespace in [LogNamespace::Legacy, LogNamespace::Vector] {
            for finish in [true, false] {
                let inputs = counted_fragments(namespace, finish);
                let expected_source = inputs[0]
                    .metadata()
                    .delivery_path_accounting()
                    .unwrap()
                    .counts[0]
                    .source
                    .clone();
                let output: Vec<_> =
                    merge_partial_events(futures::stream::iter(inputs), namespace, None)
                        .collect()
                        .await;
                assert_eq!(output.len(), 1);
                let log = output[0].as_log();
                assert_eq!(
                    log.get(&get_message_path(namespace)),
                    Some(&value!("partpartpart"))
                );
                assert_eq!(log.metadata().delivery_event_count(), 10);
                let counts = &log.metadata().delivery_path_accounting().unwrap().counts;
                assert_eq!(counts.len(), 2);
                assert_eq!(counts[0].source, expected_source);
                assert_eq!(counts[0].count, 5);
                assert_eq!(counts[1].source.time_parity, 3_600_000);
                assert_eq!(counts[1].count, 5);
                assert!(
                    counts
                        .iter()
                        .all(|count| count.path == "online" && !count.selected)
                );
                assert_eq!(
                    counts
                        .iter()
                        .flat_map(|count| &count.content_ranges)
                        .map(|range| (range.start, range.end, range.count))
                        .collect::<Vec<_>>(),
                    vec![(0, 4, 2), (4, 8, 3), (8, 12, 5)]
                );
            }
        }
    }

    #[tokio::test]
    async fn expired_partial_merge_preserves_delivery_counts() {
        let input = futures::stream::iter(counted_fragments(LogNamespace::Legacy, false))
            .chain(futures::stream::pending());
        let output: Vec<_> = merge_partial_events_with_custom_expiration(
            input,
            LogNamespace::Legacy,
            Duration::ZERO,
            None,
        )
        .take(1)
        .collect()
        .await;
        assert_eq!(output[0].metadata().delivery_event_count(), 10);
        assert_eq!(
            output[0]
                .metadata()
                .delivery_path_accounting()
                .unwrap()
                .counts
                .iter()
                .map(|count| count.count)
                .sum::<u64>(),
            10
        );
    }

    #[tokio::test]
    async fn oversized_partial_merge_does_not_produce_delivery_credit() {
        let output: Vec<_> = merge_partial_events(
            futures::stream::iter(counted_fragments(LogNamespace::Legacy, true)),
            LogNamespace::Legacy,
            Some(8),
        )
        .collect()
        .await;
        assert!(output.is_empty());
    }

    #[tokio::test]
    async fn merge_single_event_legacy() {
        let mut e_1 = LogEvent::from("test message 1");
        e_1.insert("foo", 1);

        let input_stream = futures::stream::iter([e_1.into()]);
        let output_stream = merge_partial_events(input_stream, LogNamespace::Legacy, None);

        let output: Vec<Event> = output_stream.collect().await;
        assert_eq!(output.len(), 1);
        assert_eq!(
            output[0].as_log().get(".message"),
            Some(&value!("test message 1"))
        );
    }

    #[tokio::test]
    async fn merge_single_event_legacy_exceeds_max_merged_line_limit() {
        let mut e_1 = LogEvent::from("test message 1");
        e_1.insert("foo", 1);

        let input_stream = futures::stream::iter([e_1.into()]);
        let output_stream = merge_partial_events(input_stream, LogNamespace::Legacy, Some(1));

        let output: Vec<Event> = output_stream.collect().await;
        assert_eq!(output.len(), 0);
    }

    #[tokio::test]
    async fn merge_multiple_events_legacy() {
        let mut e_1 = LogEvent::from("test message 1");
        e_1.insert("foo", 1);
        e_1.insert("_partial", true);

        let mut e_2 = LogEvent::from("test message 2");
        e_2.insert("foo2", 1);

        let input_stream = futures::stream::iter([e_1.into(), e_2.into()]);
        let output_stream = merge_partial_events(input_stream, LogNamespace::Legacy, None);

        let output: Vec<Event> = output_stream.collect().await;
        assert_eq!(output.len(), 1);
        assert_eq!(
            output[0].as_log().get(".message"),
            Some(&value!("test message 1test message 2"))
        );
    }

    #[tokio::test]
    async fn merge_multiple_events_legacy_exceeds_max_merged_line_limit() {
        let mut e_1 = LogEvent::from("test message 1");
        e_1.insert("foo", 1);
        e_1.insert("_partial", true);

        let mut e_2 = LogEvent::from("test message 2");
        e_2.insert("foo2", 1);

        let input_stream = futures::stream::iter([e_1.into(), e_2.into()]);
        // 24 > length of first message but less than the two combined
        let output_stream = merge_partial_events(input_stream, LogNamespace::Legacy, Some(24));

        let output: Vec<Event> = output_stream.collect().await;
        assert_eq!(output.len(), 0);
    }

    #[tokio::test]
    async fn multiple_events_flush_legacy() {
        let mut e_1 = LogEvent::from("test message 1");
        e_1.insert("foo", 1);
        e_1.insert("_partial", true);

        let mut e_2 = LogEvent::from("test message 2");
        e_2.insert("foo2", 1);
        e_1.insert("_partial", true);

        let input_stream = futures::stream::iter([e_1.into(), e_2.into()]);
        let output_stream = merge_partial_events(input_stream, LogNamespace::Legacy, None);

        let output: Vec<Event> = output_stream.collect().await;
        assert_eq!(output.len(), 1);
        assert_eq!(
            output[0].as_log().get(".message"),
            Some(&value!("test message 1test message 2"))
        );
    }

    #[tokio::test]
    async fn multiple_events_flush_legacy_exceeds_max_merged_line_limit() {
        let mut e_1 = LogEvent::from("test message 1");
        e_1.insert("foo", 1);
        e_1.insert("_partial", true);

        let mut e_2 = LogEvent::from("test message 2");
        e_2.insert("foo2", 1);
        e_1.insert("_partial", true);

        let input_stream = futures::stream::iter([e_1.into(), e_2.into()]);
        // 24 > length of first message but less than the two combined
        let output_stream = merge_partial_events(input_stream, LogNamespace::Legacy, Some(24));

        let output: Vec<Event> = output_stream.collect().await;
        assert_eq!(output.len(), 0);
    }

    #[tokio::test]
    async fn multiple_events_expire_legacy() {
        let mut e_1 = LogEvent::from("test message");
        e_1.insert(FILE_KEY, "foo1");
        e_1.insert("_partial", true);

        let mut e_2 = LogEvent::from("test message");
        e_2.insert(FILE_KEY, "foo2");
        e_1.insert("_partial", true);

        // and input stream that never ends
        let input_stream =
            futures::stream::iter([e_1.into(), e_2.into()]).chain(futures::stream::pending());

        let output_stream = merge_partial_events_with_custom_expiration(
            input_stream,
            LogNamespace::Legacy,
            Duration::from_secs(1),
            None,
        );

        let output: Vec<Event> = output_stream.take(2).collect().await;
        assert_eq!(output.len(), 2);
        assert_eq!(
            output[0].as_log().get(".message"),
            Some(&value!("test message"))
        );
        assert_eq!(
            output[1].as_log().get(".message"),
            Some(&value!("test message"))
        );
    }

    #[tokio::test]
    async fn merge_single_event_vector_namespace() {
        let mut e_1 = LogEvent::from(value!("test message 1"));
        e_1.insert(
            vrl::metadata_path!(super::super::Config::NAME, FILE_KEY),
            "foo1",
        );

        let input_stream = futures::stream::iter([e_1.into()]);
        let output_stream = merge_partial_events(input_stream, LogNamespace::Vector, None);

        let output: Vec<Event> = output_stream.collect().await;
        assert_eq!(output.len(), 1);
        assert_eq!(output[0].as_log().get("."), Some(&value!("test message 1")));
        assert_eq!(
            output[0].as_log().get("%kubernetes_logs.file"),
            Some(&value!("foo1"))
        );
    }

    #[tokio::test]
    async fn merge_multiple_events_vector_namespace() {
        let mut e_1 = LogEvent::from(value!("test message 1"));
        e_1.insert(
            vrl::metadata_path!(super::super::Config::NAME, "_partial"),
            true,
        );
        e_1.insert(
            vrl::metadata_path!(super::super::Config::NAME, FILE_KEY),
            "foo1",
        );

        let mut e_2 = LogEvent::from(value!("test message 2"));
        e_2.insert(
            vrl::metadata_path!(super::super::Config::NAME, FILE_KEY),
            "foo1",
        );

        let input_stream = futures::stream::iter([e_1.into(), e_2.into()]);
        let output_stream = merge_partial_events(input_stream, LogNamespace::Vector, None);

        let output: Vec<Event> = output_stream.collect().await;
        assert_eq!(output.len(), 1);
        assert_eq!(
            output[0].as_log().get("."),
            Some(&value!("test message 1test message 2"))
        );
        assert_eq!(
            output[0].as_log().get("%kubernetes_logs.file"),
            Some(&value!("foo1"))
        );
    }
}
