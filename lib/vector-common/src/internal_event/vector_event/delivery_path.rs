use serde::{Deserialize, Serialize};

#[cfg(feature = "byte_size_of")]
use crate::byte_size_of::ByteSizeOf;

/// Labels captured at source READ and preserved through delivery, including across process restarts.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct DeliverySourceLabels {
    pub process_generation_id: String,
    pub time_parity: i64,
    pub topic: String,
    pub delivery_method: String,
    pub service_system: String,
    pub source_pod_id: String,
}

#[cfg(feature = "byte_size_of")]
impl ByteSizeOf for DeliverySourceLabels {
    fn allocated_bytes(&self) -> usize {
        self.process_generation_id.allocated_bytes()
            + self.topic.allocated_bytes()
            + self.delivery_method.allocated_bytes()
            + self.service_system.allocated_bytes()
            + self.source_pod_id.allocated_bytes()
    }
}

/// Source-count units assigned to one configured primary path.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct DeliveryPathCount {
    pub path: String,
    pub source: DeliverySourceLabels,
    pub count: u64,
    /// Only an explicitly selected outbound branch can earn delivery credit.
    pub selected: bool,
}

#[cfg(feature = "byte_size_of")]
impl ByteSizeOf for DeliveryPathCount {
    fn allocated_bytes(&self) -> usize {
        self.path.allocated_bytes() + self.source.allocated_bytes()
    }
}

/// Counts carried by an event; no per-event identity or completion ledger is needed.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct DeliveryPathAccounting {
    pub counts: Vec<DeliveryPathCount>,
}

impl DeliveryPathAccounting {
    /// An empty match set remains explicitly accounted, without falling back to unscoped counters.
    pub fn at_read(source: DeliverySourceLabels, paths: impl IntoIterator<Item = String>) -> Self {
        let mut accounting = Self::default();
        for path in paths {
            // A repeated configuration path is still one requirement for this READ.
            if !accounting.counts.iter().any(|count| count.path == path) {
                accounting.counts.push(DeliveryPathCount {
                    path,
                    source: source.clone(),
                    count: 1,
                    selected: false,
                });
            }
        }
        accounting
    }

    pub fn has_path(&self, path: &str) -> bool {
        self.counts.iter().any(|count| count.path == path)
    }

    /// Select existing requirements for this branch without creating new READs.
    pub fn select_path(&mut self, path: &str) {
        self.select_paths(&[path]);
    }

    /// A shared branch can carry several independently enrolled primary paths.
    pub fn select_paths(&mut self, paths: &[impl AsRef<str>]) {
        let keep = |count: &DeliveryPathCount| paths.iter().any(|path| count.path == path.as_ref());
        self.counts.retain(keep);
        for count in &mut self.counts {
            count.selected = true;
        }
    }

    /// Supplemental copies carry no primary delivery requirements or credit.
    pub fn clear_paths(&mut self) {
        self.counts.clear();
    }

    /// Removes and returns the requirements resolved by an approved policy decision.
    pub fn take_path(&mut self, path: &str) -> Vec<DeliveryPathCount> {
        let mut taken = Vec::new();
        self.counts.retain(|count| {
            if count.path == path {
                taken.push(count.clone());
                false
            } else {
                true
            }
        });
        taken
    }

    pub fn merge(&mut self, other: Self) {
        for count in other.counts {
            if let Some(existing) = self.counts.iter_mut().find(|existing| {
                existing.path == count.path
                    && existing.source == count.source
                    && existing.selected == count.selected
            }) {
                existing.count = existing.count.saturating_add(count.count);
            } else {
                self.counts.push(count);
            }
        }
    }
}

#[cfg(feature = "byte_size_of")]
impl ByteSizeOf for DeliveryPathAccounting {
    fn allocated_bytes(&self) -> usize {
        self.counts.allocated_bytes()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use metrics_util::debugging::{DebugValue, DebuggingRecorder};

    use super::super::delivery_event::DeliveryEventStage;
    use super::*;

    fn source_labels(process: &str) -> DeliverySourceLabels {
        DeliverySourceLabels {
            process_generation_id: process.to_owned(),
            time_parity: 3_600_000,
            topic: "service-log".to_owned(),
            delivery_method: "VECTOR_WOODCHUCK_V2_FILE".to_owned(),
            service_system: "test-service".to_owned(),
            source_pod_id: "source-pod".to_owned(),
        }
    }

    #[test]
    fn branch_copies_select_only_their_existing_requirement() {
        let source = DeliveryPathAccounting::at_read(
            source_labels("old-process"),
            [
                "archive".to_owned(),
                "online".to_owned(),
                "archive".to_owned(),
            ],
        );
        assert_eq!(source.counts.len(), 2);
        assert!(
            source
                .counts
                .iter()
                .all(|count| !count.selected && count.count == 1)
        );
        let mut archive = source.clone();
        archive.select_path("archive");
        assert_eq!(archive.counts.len(), 1);
        assert_eq!(archive.counts[0].path, "archive");
        assert!(archive.counts[0].selected);
        assert_eq!(source.counts.len(), 2);

        let mut unknown = source.clone();
        unknown.select_path("not-required");
        assert!(unknown.counts.is_empty());
        let mut supplemental = source;
        supplemental.clear_paths();
        assert!(supplemental.counts.is_empty());
    }

    #[test]
    fn reduction_sums_counts_without_losing_source_labels() {
        let first = DeliveryPathAccounting::at_read(source_labels("old"), ["archive".to_owned()]);
        let second = DeliveryPathAccounting::at_read(source_labels("new"), ["archive".to_owned()]);
        let mut merged = first.clone();
        merged.merge(first);
        merged.merge(second);
        assert_eq!(merged.counts.len(), 2);
        assert_eq!(merged.counts[0].source.process_generation_id, "old");
        assert_eq!(merged.counts[0].count, 2);
        assert_eq!(merged.counts[1].source.process_generation_id, "new");
        assert_eq!(merged.counts[1].count, 1);
    }

    #[test]
    fn merging_does_not_promote_unselected_requirements_to_delivery_credit() {
        let mut unselected =
            DeliveryPathAccounting::at_read(source_labels("p"), ["archive".to_owned()]);
        let mut selected = unselected.clone();
        selected.select_path("archive");
        unselected.merge(selected);
        assert_eq!(unselected.counts.len(), 2);
        assert_eq!(
            unselected
                .counts
                .iter()
                .filter(|count| count.selected)
                .count(),
            1
        );
        assert_eq!(
            unselected
                .counts
                .iter()
                .map(|count| count.count)
                .sum::<u64>(),
            2
        );
    }

    #[test]
    fn source_labels_use_original_read_topic_resolution_and_defaults() {
        let source = DeliverySourceLabels::at_read(
            "/var/log/SecurityLog.pb.base64",
            &None,
            None,
            Some("pod-123"),
            "file",
            3_600_000,
        );
        assert_eq!(source.topic, "security-log");
        assert_eq!(source.service_system, "unknown");
        assert_eq!(source.source_pod_id, "pod-123");
        assert_eq!(source.delivery_method, "VECTOR_WOODCHUCK_V2_FILE");
        assert_eq!(source.time_parity, 3_600_000);
        assert!(!source.process_generation_id.is_empty());
    }

    #[test]
    fn delivery_requires_selection_and_preserves_the_original_process_identity() {
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        let mut count = DeliveryPathCount {
            path: "archive".to_owned(),
            source: source_labels("original-process"),
            count: 3,
            selected: false,
        };
        metrics::with_local_recorder(&recorder, || {
            count.emit(DeliveryEventStage::Staged);
            count.emit(DeliveryEventStage::Delivered);
        });
        assert!(snapshotter.snapshot().into_vec().is_empty());
        count.selected = true;
        metrics::with_local_recorder(&recorder, || {
            count.emit(DeliveryEventStage::SinkInputAccepted);
            count.emit(DeliveryEventStage::Rejected);
            count.emit(DeliveryEventStage::FileUnwatched);
        });
        assert!(snapshotter.snapshot().into_vec().is_empty());
        metrics::with_local_recorder(&recorder, || count.emit(DeliveryEventStage::Delivered));
        let counters = snapshotter.snapshot().into_vec();
        assert_eq!(counters.len(), 2);
        let mut pods = Vec::new();
        for (key, _, _, value) in &counters {
            let labels = key
                .key()
                .labels()
                .map(|label| (label.key(), label.value()))
                .collect::<HashMap<_, _>>();
            assert_eq!(key.key().name(), "delivery_events_total");
            assert_eq!(*value, DebugValue::Counter(3));
            assert_eq!(
                labels["delivery_event_type"],
                "VECTOR_SINK_UPLOAD_DELIVERED"
            );
            assert_eq!(labels["delivery_path"], "archive");
            assert_eq!(labels["source_process_generation_id"], "original-process");
            assert_ne!(labels["process_generation_id"], "original-process");
            assert_eq!(labels["time_parity"], "3600000");
            assert_eq!(labels["topic"], "service-log");
            assert_eq!(labels["service_system"], "test-service");
            pods.push(labels["source_pod_id"]);
        }
        pods.sort_unstable();
        assert_eq!(pods, ["__all_source_pods__", "source-pod"]);
    }

    #[test]
    fn read_and_intentional_stages_are_separate_counters() {
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        let count = DeliveryPathCount {
            path: "online".to_owned(),
            source: source_labels("p"),
            count: 1,
            selected: false,
        };
        metrics::with_local_recorder(&recorder, || {
            count.emit(DeliveryEventStage::Read);
            count.emit(DeliveryEventStage::IntentionalDrop {
                reason: "policy_exclusion",
            });
            count.emit(DeliveryEventStage::IntentionalDrop { reason: "" });
        });
        let counters = snapshotter.snapshot().into_vec();
        assert_eq!(counters.len(), 4);
        for (key, _, _, value) in counters {
            let labels = key
                .key()
                .labels()
                .map(|label| (label.key(), label.value()))
                .collect::<HashMap<_, _>>();
            assert_eq!(value, DebugValue::Counter(1));
            match labels["delivery_event_type"] {
                "VECTOR_SOURCE_READ" => {
                    assert!(!labels.contains_key("reason"))
                }
                "VECTOR_DELIVERY_INTENTIONAL_DROP" => {
                    assert_eq!(labels["reason"], "policy_exclusion")
                }
                stage => panic!("unexpected accounting stage: {stage}"),
            }
        }
    }
}
