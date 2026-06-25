use prost_reflect::{DeserializeOptions, DynamicMessage, MessageDescriptor};
use serde_json::json;
use snafu::ResultExt;

use super::config::LoginServiceAuthConfig;
use super::error::{BuildRequestSnafu, TokenManagerError};

/// Build a `DynamicMessage` for the OAuth bootstrap request from config + the runtime
/// request `MessageDescriptor`.
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

    let request = json!({
        "authentication": {
            "workspace": config.user.workspace_id,
            "service": {
                "servicePrincipalType": config.user.service_principal_type,
                "servicePrincipalResource": &config.user.service_principal_resource,
            },
        },
        "authorization": { "authorizationConstraint": authorization_constraint },
    });

    let opts = DeserializeOptions::new().deny_unknown_fields(true);
    DynamicMessage::deserialize_with_options(request_desc.clone(), &request, &opts)
        .context(BuildRequestSnafu)
}
