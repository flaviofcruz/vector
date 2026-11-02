use prost_reflect::{DeserializeOptions, DynamicMessage, MessageDescriptor};
use serde_json::json;
use snafu::ResultExt;

use super::config::{LoginServiceAuthConfig, SIGNED_PRINCIPAL_CONTEXT_TYPE};
use super::error::{BuildRequestSnafu, TokenManagerError};

/// Build a `CreatePrincipalContext` request (`databricks.identity.PrincipalContextRequest`) from
/// config + the runtime request `MessageDescriptor`.
///
/// With `signed` set, the request also carries a `SignedPrincipalContextConfig` so Login
/// returns the signed wrapper Zerobus accepts; otherwise Login returns only the unsigned
/// context UC reads.
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
    request_desc: &MessageDescriptor,
    signed: bool,
) -> Result<DynamicMessage, TokenManagerError> {
    let uc_securable_permission: Vec<_> = config
        .user
        .uc_permissions
        .iter()
        .map(|p| {
            json!({
                "privileges": &p.privileges,
                "securableReference": {
                    "type": &p.securable_type,
                    "fullName": &p.full_name,
                },
            })
        })
        .collect();

    let authorization_constraint = if uc_securable_permission.is_empty() {
        json!({})
    } else {
        json!({ "ucPermissionConstraint": { "ucSecurablePermission": uc_securable_permission } })
    };

    let mut request = json!({
        "authentication": {
            "workspace": config.user.workspace_id,
            "service": {
                "servicePrincipalType": config.user.service_principal_type,
                "servicePrincipalResource": &config.user.service_principal_resource,
            },
        },
        "authorization": { "authorizationConstraint": authorization_constraint },
    });
    if signed {
        let ssp_resource_path = config.zerobus_ssp_resource_path.clone().unwrap_or_else(|| {
            format!(
                "workspaces/{}/zerobusDirectWriteApi",
                config.user.workspace_id
            )
        });
        request["signedPrincipalContextConfig"] = json!({
            "signedPrincipalContextType": SIGNED_PRINCIPAL_CONTEXT_TYPE,
            "allowedServicePrincipalResourcePath": ssp_resource_path,
            "metastoreId": &config.metastore_id,
        });
    }

    let opts = DeserializeOptions::new().deny_unknown_fields(true);
    DynamicMessage::deserialize_with_options(request_desc.clone(), &request, &opts)
        .context(BuildRequestSnafu)
}
