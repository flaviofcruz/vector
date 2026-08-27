//! Log-metadata shape-enforcement policy — the LP write-path port of Scala
//! `LogMetadataShapeEnforcer` (`log-sync/structured-logs/common/src/redaction/
//! LogMetadataShapeEnforcer.scala`). Keep the field -> shape maps below in lockstep with it: LP and
//! the Spark centralization ETL write the same Delta tables, so they must null the same values.
//!
//! This module owns only the *policy* (which `_log_metadata` field may take which `DataShape`s) so it
//! sits next to the shape validators. The tree walk over the metadata object lives in the
//! `enforce_metadata_shape` VRL function (`vector-vrl-functions`), which owns the VRL `Value` types.
//!
//! A value is kept iff it is empty, the literal `"unknown"`, or matches at least one of its shapes
//! (OR semantics, via [`crate::shape::is_allowed_shape`]); otherwise the caller wipes it to null.
//! This mirrors `enforceStringShape` exactly (an absent field stays absent — the caller only touches
//! present string values).

use crate::proto_bindings::compliance::DataShape;
use crate::shape::is_allowed_shape;

/// `service_metadata` string fields. Mirrors `enforceServiceMetadata`. `data_plane_metadata` is a
/// nested message enforced separately via [`DATA_PLANE_METADATA_SHAPES`].
pub const SERVICE_METADATA_SHAPES: &[(&str, &[i32])] = &[
    ("branch_name", &[DataShape::BranchName as i32]),
    ("system_uri", &[DataShape::SystemUri as i32]),
    (
        "shard_name",
        &[
            DataShape::ShardName as i32,
            DataShape::KubeContext as i32,
            DataShape::CloudRegion as i32,
        ],
    ),
    ("request_id", &[DataShape::Uuid as i32, DataShape::Ulid as i32]),
    (
        "traceparent",
        &[DataShape::W3cTraceparent as i32, DataShape::Hex32 as i32],
    ),
    ("trace_id", &[DataShape::Hex32 as i32]),
    ("account_id", &[DataShape::Uuid as i32]),
    ("pod_uid", &[DataShape::Uuid as i32]),
];

/// `service_metadata.data_plane_metadata` string fields. Mirrors `enforceDataPlaneMetadata`.
pub const DATA_PLANE_METADATA_SHAPES: &[(&str, &[i32])] = &[
    ("cluster_id", &[DataShape::DbrClusterId as i32]),
    ("cluster_creator", &[DataShape::ClusterCreator as i32]),
    ("spark_image_label", &[DataShape::DbrImageLabel as i32]),
    (
        "spark_version",
        &[
            DataShape::DbrImageLabel as i32,
            DataShape::VersionNumber as i32,
        ],
    ),
    ("cluster_size_type", &[DataShape::ClusterSizeType as i32]),
    (
        "cluster_node_type",
        &[
            DataShape::CloudInstanceType as i32,
            DataShape::ServerlessNodeTypeId as i32,
        ],
    ),
    (
        "cluster_unity_catalog_mode",
        &[DataShape::ClusterUnityCatalogMode as i32],
    ),
    ("dbsql_channel", &[DataShape::PlatformChannel as i32]),
    ("internal_cluster_id", &[DataShape::InternalClusterId as i32]),
];

/// `log_daemon_metadata` string fields. Mirrors `enforceLogDaemonMetadata`.
pub const LOG_DAEMON_METADATA_SHAPES: &[(&str, &[i32])] = &[
    (
        "shard_name",
        &[DataShape::ShardName as i32, DataShape::KubeContext as i32],
    ),
    (
        "stream_guid",
        &[
            DataShape::Uuid as i32,
            DataShape::LoggingFileId as i32,
            DataShape::Hex32 as i32,
        ],
    ),
    ("plane", &[DataShape::Plane as i32]),
    ("cluster_type", &[DataShape::ClusterType as i32]),
    ("spark_image_label", &[DataShape::DbrImageLabel as i32]),
    ("org_id", &[DataShape::Numeric as i32]),
    ("region", &[DataShape::CloudRegion as i32]),
    (
        "unp_node_id",
        &[
            DataShape::Uuid as i32,
            DataShape::AwsEc2InstanceId as i32,
            DataShape::GcpComputeInstanceId as i32,
            DataShape::GoogleCloudHex32Id as i32,
        ],
    ),
];

/// Whether a field value may be kept: empty, the literal `"unknown"`, or matching at least one of
/// `shapes`. Mirrors `enforceStringShape` (OR over shapes; empty/`unknown` always pass). A caller
/// wipes the field to null when this returns `false`.
pub fn field_value_conforms(shapes: &[i32], value: &str) -> bool {
    value.is_empty() || value == "unknown" || is_allowed_shape(shapes, value)
}

#[cfg(test)]
mod tests {
    use super::*;

    // Matches no shape — the same probe LogMetadataShapeEnforcerSuite uses; every enforced field
    // must wipe it.
    const UNSHAPED_PROBE: &str = "!!metadata-shape-probe (matches no shape)!!";
    // A UUID reused across fields, as in the Scala suite.
    const A_UUID: &str = "01ef1742-bfef-1d54-8e8f-0d79b856b0f3";
    // A full DBR image label from the Scala happy-path case.
    const DBR_IMAGE_LABEL: &str =
        "release__18.2.x-photon-scala2.13__dbr__18.2.2__8020d77__099e4fa__jenkins__sdp-20260524-rc0-fbcacd4__format-4";

    fn shapes_for<'a>(map: &'a [(&str, &[i32])], field: &str) -> &'a [i32] {
        map.iter()
            .find(|(f, _)| *f == field)
            .map(|(_, shapes)| *shapes)
            .unwrap_or_else(|| panic!("field {field} not present in map"))
    }

    fn assert_kept(map: &[(&str, &[i32])], field: &str, value: &str) {
        assert!(
            field_value_conforms(shapes_for(map, field), value),
            "expected field {field} to KEEP {value:?}"
        );
    }

    fn assert_wiped(map: &[(&str, &[i32])], field: &str, value: &str) {
        assert!(
            !field_value_conforms(shapes_for(map, field), value),
            "expected field {field} to WIPE {value:?}"
        );
    }

    #[test]
    fn empty_and_unknown_always_pass() {
        // Every enforced field, across all three sub-structs, keeps the skipped placeholders.
        for map in [
            SERVICE_METADATA_SHAPES,
            DATA_PLANE_METADATA_SHAPES,
            LOG_DAEMON_METADATA_SHAPES,
        ] {
            for (field, _) in map {
                assert_kept(map, field, "");
                assert_kept(map, field, "unknown");
            }
        }
    }

    #[test]
    fn every_enforced_field_wipes_the_unshaped_probe() {
        for map in [
            SERVICE_METADATA_SHAPES,
            DATA_PLANE_METADATA_SHAPES,
            LOG_DAEMON_METADATA_SHAPES,
        ] {
            for (field, _) in map {
                assert_wiped(map, field, UNSHAPED_PROBE);
            }
        }
    }

    #[test]
    fn service_metadata_happy_path_values_kept() {
        assert_kept(SERVICE_METADATA_SHAPES, "branch_name", "master");
        assert_kept(SERVICE_METADATA_SHAPES, "system_uri", "system:notebook");
        assert_kept(SERVICE_METADATA_SHAPES, "shard_name", "oregon-prod");
        // shard_name also accepts a serverless DP shard name and a cloud region (OR set).
        assert_kept(SERVICE_METADATA_SHAPES, "shard_name", "aws-an1-s1-0-7hzac1");
        assert_kept(SERVICE_METADATA_SHAPES, "shard_name", "us-east-1");
        assert_kept(SERVICE_METADATA_SHAPES, "request_id", A_UUID);
        assert_kept(
            SERVICE_METADATA_SHAPES,
            "traceparent",
            "00-fc0296e891538bed49b0b1d6087ce59b-5f4344be67cae67a-00",
        );
        assert_kept(
            SERVICE_METADATA_SHAPES,
            "trace_id",
            "01f0c57fcad41b81aee48604eb996db7",
        );
        assert_kept(SERVICE_METADATA_SHAPES, "account_id", A_UUID);
        assert_kept(SERVICE_METADATA_SHAPES, "pod_uid", A_UUID);
    }

    #[test]
    fn data_plane_metadata_happy_path_values_kept() {
        assert_kept(
            DATA_PLANE_METADATA_SHAPES,
            "cluster_id",
            "0506-133027-8y3ep6tp-v2n",
        );
        assert_kept(DATA_PLANE_METADATA_SHAPES, "cluster_creator", "JobLauncher");
        assert_kept(
            DATA_PLANE_METADATA_SHAPES,
            "spark_image_label",
            DBR_IMAGE_LABEL,
        );
        // spark_version accepts a full image label OR a bare version number (OR set).
        assert_kept(DATA_PLANE_METADATA_SHAPES, "spark_version", DBR_IMAGE_LABEL);
        assert_kept(DATA_PLANE_METADATA_SHAPES, "spark_version", "15.4.0");
        assert_kept(DATA_PLANE_METADATA_SHAPES, "cluster_size_type", "VM_CONTAINER");
        assert_kept(DATA_PLANE_METADATA_SHAPES, "cluster_node_type", "r8gd.8xlarge");
        assert_kept(
            DATA_PLANE_METADATA_SHAPES,
            "cluster_unity_catalog_mode",
            "SINGLE_USER",
        );
        assert_kept(
            DATA_PLANE_METADATA_SHAPES,
            "dbsql_channel",
            "CHANNEL_NAME_CURRENT",
        );
        assert_kept(
            DATA_PLANE_METADATA_SHAPES,
            "internal_cluster_id",
            "qeaj2n--i0508122019-c6vlrq5a",
        );
    }

    #[test]
    fn log_daemon_metadata_happy_path_values_kept() {
        assert_kept(LOG_DAEMON_METADATA_SHAPES, "shard_name", "oregon-prod");
        // stream_guid OR set: UUID, logging-agent file_id (checksum form), and MD5 (32-char hex).
        assert_kept(LOG_DAEMON_METADATA_SHAPES, "stream_guid", A_UUID);
        assert_kept(
            LOG_DAEMON_METADATA_SHAPES,
            "stream_guid",
            "checksum:1580485302",
        );
        assert_kept(
            LOG_DAEMON_METADATA_SHAPES,
            "stream_guid",
            "d41d8cd98f00b204e9800998ecf8427e",
        );
        assert_wiped(LOG_DAEMON_METADATA_SHAPES, "stream_guid", "not-a-guid");
        assert_kept(LOG_DAEMON_METADATA_SHAPES, "plane", "PLANE_DATA");
        assert_kept(LOG_DAEMON_METADATA_SHAPES, "cluster_type", "CLUSTER_TYPE_NEPHOS");
        assert_kept(LOG_DAEMON_METADATA_SHAPES, "spark_image_label", DBR_IMAGE_LABEL);
        assert_kept(LOG_DAEMON_METADATA_SHAPES, "org_id", "12345");
        assert_kept(LOG_DAEMON_METADATA_SHAPES, "region", "us-east-1");
        // unp_node_id OR set: AWS EC2 id, GCP compute id, Google Cloud hex32 id, and UUID.
        assert_kept(LOG_DAEMON_METADATA_SHAPES, "unp_node_id", "i-059d5bb24debad6e7");
        assert_kept(
            LOG_DAEMON_METADATA_SHAPES,
            "unp_node_id",
            "68422481410_us-central1-a_7756660147328626277",
        );
        assert_kept(
            LOG_DAEMON_METADATA_SHAPES,
            "unp_node_id",
            "GoogleCloud-01B77A6D142D4FFEB6DFB2A62C06C599",
        );
        assert_kept(LOG_DAEMON_METADATA_SHAPES, "unp_node_id", A_UUID);
    }

    /// Completeness guard: the enforced field sets must match this list exactly. Mirrors the Scala
    /// suite's reflective completeness test — a field added to or removed from a map without updating
    /// this list (and the Scala side) fails here, catching silent drift between the two write paths.
    #[test]
    fn enforced_field_sets_are_exact() {
        fn fields<'a>(map: &[(&'a str, &'a [i32])]) -> Vec<&'a str> {
            let mut fs: Vec<&str> = map.iter().map(|(f, _)| *f).collect();
            fs.sort_unstable();
            fs
        }
        let mut service = vec![
            "account_id",
            "branch_name",
            "pod_uid",
            "request_id",
            "shard_name",
            "system_uri",
            "trace_id",
            "traceparent",
        ];
        service.sort_unstable();
        assert_eq!(fields(SERVICE_METADATA_SHAPES), service);

        let mut data_plane = vec![
            "cluster_creator",
            "cluster_id",
            "cluster_node_type",
            "cluster_size_type",
            "cluster_unity_catalog_mode",
            "dbsql_channel",
            "internal_cluster_id",
            "spark_image_label",
            "spark_version",
        ];
        data_plane.sort_unstable();
        assert_eq!(fields(DATA_PLANE_METADATA_SHAPES), data_plane);

        let mut daemon = vec![
            "cluster_type",
            "org_id",
            "plane",
            "region",
            "shard_name",
            "spark_image_label",
            "stream_guid",
            "unp_node_id",
        ];
        daemon.sort_unstable();
        assert_eq!(fields(LOG_DAEMON_METADATA_SHAPES), daemon);
    }
}
