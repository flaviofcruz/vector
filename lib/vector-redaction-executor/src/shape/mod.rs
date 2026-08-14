//! Data-shape enforcement validators.
//!
//! Vendored Rust port of the Databricks data-shape validators. Validates string field values
//! against expected data shapes (UUID, hex, numeric, etc.) to determine if they are centralizable
//! for logging and compliance.
//!
//! This module re-exports the key validators consumed by the streaming redaction executor. The
//! underlying implementations are in the patterns, patterns_complex, and enumerated modules.

mod enumerated;
mod patterns;
mod patterns_complex;

// Imported from the proto bindings. The DataShape enum is defined in compliance/shape.proto
// and is available via the proto_bindings module created by prost_build.
use crate::proto_bindings::compliance::DataShape;

/// Returns `true` iff `value` matches `shape`.
///
/// `DATA_SHAPE_UNSPECIFIED` (and any enum value with neither a pattern validator nor an enumerated
/// set) returns `false` — there is no shape to match, so nothing is centralizable through it.
pub fn match_shape(shape: DataShape, value: &str) -> bool {
    let name = shape.as_str_name();
    match shape {
        // ---- Pattern / structural validators (66). ----
        DataShape::Uuid => patterns::uuid(value),
        DataShape::NimbusReplId => patterns::nimbus_repl_id(value),
        DataShape::Hex32 => patterns::hex32(value),
        DataShape::Hex40 => patterns::hex40(value),
        DataShape::Ulid => patterns::ulid(value),
        DataShape::DeploymentName => patterns::deployment_name(value),
        DataShape::PrefixedUuid => patterns::prefixed_uuid(value),
        DataShape::Numeric => patterns::numeric(value),
        DataShape::ChatCompletionResponseId => patterns::chat_completion_response_id(value),
        DataShape::AwsEc2InstanceId => patterns::aws_ec2_instance_id(value),
        DataShape::AwsEbsVolumeId => patterns::aws_ebs_volume_id(value),
        DataShape::AwsEbsSnapshotId => patterns::aws_ebs_snapshot_id(value),
        DataShape::CloudAvailabilityZone => enumerated::cloud_availability_zone(value),
        DataShape::GcpComputeInstanceId => enumerated::gcp_compute_instance_id(value),
        DataShape::GoogleCloudHex32Id => patterns::google_cloud_hex32_id(value),
        DataShape::Iso8601Timestamp => patterns::iso8601_timestamp(value),
        DataShape::CalendarDate => patterns::calendar_date(value),
        DataShape::W3cTraceparent => patterns::w3c_traceparent(value),
        DataShape::TruncatedIp => patterns::truncated_ip(value),
        DataShape::PrivateIpAddress => patterns::private_ip_address(value),
        DataShape::DbrClusterId => patterns::dbr_cluster_id(value),
        DataShape::Ja4Fingerprint => patterns::ja4_fingerprint(value),
        DataShape::IsoCountryCode => patterns::iso_country_code(value),
        DataShape::Boolean => patterns::boolean_string(value),
        DataShape::HttpProtocol => patterns::http_protocol(value),
        DataShape::TlsVersion => patterns::tls_version(value),
        DataShape::DatabricksPrincipalUri => patterns::databricks_principal_uri(value),
        DataShape::TaskRunId => patterns::task_run_id(value),
        DataShape::JiraTicketKey => patterns::jira_ticket_key(value),
        DataShape::PostgresTimestampTz => patterns::postgres_timestamp_tz(value),
        DataShape::InternalClusterId => patterns::internal_cluster_id(value),
        DataShape::Hex8 => patterns::hex8(value),
        DataShape::Hex16 => patterns::hex16(value),
        DataShape::ApprovalId => patterns::approval_id(value),
        DataShape::K8sApiVersion => patterns::k8s_api_version(value),
        DataShape::K8sNodeName => enumerated::k8s_node_name(value),
        DataShape::ClusterPoolName => patterns::cluster_pool_name(value),
        DataShape::ServerlessNodeTypeId => patterns::serverless_node_type_id(value),
        DataShape::NotebooksId => patterns::notebooks_id(value),
        DataShape::LoggingFileId => patterns::logging_file_id(value),
        DataShape::Hex16Pair => patterns::hex16_pair(value),
        DataShape::Hex32Hex16 => patterns::hex32_hex16(value),
        DataShape::VersionNumber => patterns::version_number(value),
        DataShape::SparkThriftUserAgent => patterns::spark_thrift_user_agent(value),
        DataShape::SalesforceAccountId => patterns::salesforce_account_id(value),
        DataShape::BricksterEmail => patterns::brickster_email(value),
        DataShape::GrpcService => patterns_complex::grpc_service(value),
        DataShape::GrpcMethod => patterns_complex::grpc_method(value),
        DataShape::ProtoMessageFqn => patterns_complex::proto_message_fqn(value),
        DataShape::NexusAttributeId => enumerated::nexus_attribute_id(value),
        DataShape::OomReasonPiped => enumerated::oom_reason_piped(value),
        DataShape::Hex64 => patterns::hex64(value),
        DataShape::DbrImageLabel => patterns_complex::dbr_image_label(value),
        DataShape::SparkConfigName => patterns::spark_config_name(value),
        DataShape::BranchName => patterns_complex::branch_name(value),
        DataShape::SystemUri => patterns::system_uri(value),
        DataShape::EstoreNamespace => patterns::estore_namespace(value),
        DataShape::SparkVersion => patterns_complex::spark_version(value),
        DataShape::PlatformChannel => patterns_complex::platform_channel(value),
        DataShape::DbletImageLabel => patterns_complex::dblet_image_label(value),
        DataShape::KaasReleaseVersionName => patterns::kaas_release_version_name(value),
        DataShape::KubeContext => enumerated::kube_context(value),
        DataShape::DatabricksFqdn => patterns_complex::databricks_fqdn(value),
        DataShape::GoogleApiEndpoint => patterns_complex::google_api_endpoint(value),
        DataShape::GenericErrorCode => patterns_complex::generic_error_code(value),
        DataShape::RedactedStackTrace => patterns_complex::redacted_stack_trace(value),
        DataShape::JfrRedactedTypeName => patterns_complex::jfr_redacted_type_name(value),
        DataShape::JfrRedactedThreadName => enumerated::jfr_redacted_thread_name(value),
        DataShape::InfraDataModelUri => enumerated::infra_data_model_uri(value),
        DataShape::SqlState => patterns::sql_state(value),
        DataShape::ReleaseStepName => enumerated::release_step_name(value),
        DataShape::EngineRequestId => patterns_complex::engine_request_id(value),
        DataShape::ExceptionType => patterns_complex::exception_type(value),
        DataShape::EngineRequestOutcome => enumerated::engine_request_outcome(value),
        DataShape::SqlGatewayEndpointId => patterns::sql_gateway_endpoint_id(value),
        DataShape::UuidWithVersion => patterns::uuid_with_version(value),
        DataShape::SparkThriftMetadataOpsParamSelectivity => {
            patterns::spark_thrift_metadata_ops_param_selectivity(value)
        }
        DataShape::SparkThriftResultSchema => patterns_complex::spark_thrift_result_schema(value),
        DataShape::SparkThriftPartitionSizeBuckets => {
            patterns_complex::spark_thrift_partition_size_buckets(value)
        }
        DataShape::PdTileId => patterns::pd_tile_id(value),
        DataShape::BoundedAlphanumeric => patterns::bounded_alphanumeric(value),
        DataShape::DatabricksAccountConsoleUrl => {
            patterns_complex::databricks_account_console_url(value)
        }
        DataShape::AzureSubscriptionPath => patterns_complex::azure_subscription_path(value),
        DataShape::PartnerChangeEventRegistrationId => {
            patterns_complex::partner_change_event_registration_id(value)
        }
        DataShape::DataRoomsEmbeddingPath => patterns_complex::data_rooms_embedding_path(value),
        DataShape::NotifsvcIdempotencyKey => patterns::notifsvc_idempotency_key(value),

        // ---- Everything else: enumerated set from static-shapes.json, or no validator. ----
        // Unspecified never matches; any other enum value falls through to the enumerated-set
        // lookup (returns false when the shape has no set, matching Java's fail-closed dispatch).
        DataShape::Unspecified => false,
        _ => enumerated::enumerated_set_contains(name, value),
    }
}

/// Convenience for the executor: resolve a raw proto enum number to a shape and match.
///
/// An unrecognized enum number (a shape added to the proto but not to this build) returns `false`
/// — fail closed, never keep an unvalidated string.
pub fn match_shape_i32(shape_number: i32, value: &str) -> bool {
    match DataShape::try_from(shape_number) {
        Ok(shape) => match_shape(shape, value),
        Err(_) => false,
    }
}

/// Returns `true` iff `value` matches ANY of `shape_numbers` (union / OR semantics). An empty slice
/// returns `false` — no shape can match, so the field is dropped (the streaming redactor's
/// PassThroughString contract).
pub fn is_allowed_shape(shape_numbers: &[i32], value: &str) -> bool {
    shape_numbers.iter().any(|&n| match_shape_i32(n, value))
}

#[cfg(test)]
mod corpus_test;
