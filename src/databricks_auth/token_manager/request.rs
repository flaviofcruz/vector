use prost_reflect::{DeserializeOptions, DynamicMessage, MessageDescriptor};
use serde_json::json;
use snafu::ResultExt;

use super::config::{LoginServiceAuthConfig, SIGNED_PRINCIPAL_CONTEXT_TYPE};
use super::error::{BuildRequestSnafu, TokenManagerError};

/// Variable the SSP policy binds to the target table's full name.
const TABLE_FULL_NAME_BINDING: &str = "tableFullName";

pub(super) enum PrincipalContextRequestKind {
    ServicePrincipal,
    DpAuthToken {
        allowed_service_principal_resource_path: String,
        metastore_id: String,
    },
}

impl PrincipalContextRequestKind {
    pub(super) fn dp_auth_token(config: &LoginServiceAuthConfig) -> Self {
        Self::DpAuthToken {
            allowed_service_principal_resource_path: config
                .zerobus_ssp_resource_path
                .clone()
                .unwrap_or_else(|| {
                    format!(
                        "workspaces/{}/zerobusDirectWriteApi",
                        config.user.workspace_id
                    )
                }),
            metastore_id: config.metastore_id.clone(),
        }
    }
}

/// Build a `CreatePrincipalContext` request (`databricks.identity.PrincipalContextRequest`) from
/// config + the runtime request `MessageDescriptor`.
///
/// DP auth tokens require the legacy signed-context config. Regular requests return an
/// unsigned context and may also return signed PCv2, controlled by Login's producer mode.
///
/// The request is constructed as a `serde_json::Value` and then deserialized into a
/// `DynamicMessage` through `prost-reflect`'s serde integration. This gives us schema
/// validation for free: if the descriptor doesn't have a field we name, or if a value's
/// type doesn't match the descriptor, `deserialize_with_options` returns a structured
/// error pointing at the bad field.
///
/// Pulled out as a free function so it can be tested against a fixture descriptor
/// pool without an actual `TokenManager`.
pub(super) fn build_request_message(
    config: &LoginServiceAuthConfig,
    table_name: &str,
    request_desc: &MessageDescriptor,
    kind: PrincipalContextRequestKind,
) -> Result<DynamicMessage, TokenManagerError> {
    let operation = match &kind {
        PrincipalContextRequestKind::ServicePrincipal => &config.user.uc_read_operation,
        PrincipalContextRequestKind::DpAuthToken { .. } => &config.user.zerobus_write_operation,
    };

    let mut request = json!({
        "authentication": {
            "workspace": config.user.workspace_id,
            "service": {
                "servicePrincipalType": config.user.service_principal_type,
                "servicePrincipalResource": &config.user.service_principal_resource,
            },
        },
        "authorization": {
            "sspAuthorizationOperationRequest": {
                "operation": operation,
                "bindings": [{ "name": TABLE_FULL_NAME_BINDING, "single": table_name }],
            },
        },
    });
    if let PrincipalContextRequestKind::DpAuthToken {
        allowed_service_principal_resource_path,
        metastore_id,
    } = kind
    {
        request["signedPrincipalContextConfig"] = json!({
            "signedPrincipalContextType": SIGNED_PRINCIPAL_CONTEXT_TYPE,
            "allowedServicePrincipalResourcePath": allowed_service_principal_resource_path,
            "metastoreId": metastore_id,
        });
    }

    let opts = DeserializeOptions::new().deny_unknown_fields(true);
    DynamicMessage::deserialize_with_options(request_desc.clone(), &request, &opts)
        .context(BuildRequestSnafu)
}
