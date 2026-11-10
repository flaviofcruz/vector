//! Wire-compatible subset of security/prodam/context/proto/workload_identity_attributes.proto
//! in databricks-eng/universe. OpenSSL authenticates the peer before this parser is called.
use prost::Message;
use x509_parser::{der_parser::der::parse_der_octetstring, prelude::*};

const UWI_OID: &str = "1.3.6.1.4.1.42.113.1";
const SERVERLESS_TYPE: &str = "security.prodam.context.ServerlessWorkloadIdentityAttributes";

/// Policy for authenticated serverless OTLP producers. This does not identify a pod replica.
#[derive(Clone, Debug)]
pub(super) struct UwiIdentityPolicy {
    /// Accepted UWI URI SAN trust domains, matched exactly without wildcards.
    pub trust_domains: Vec<String>,
    /// Exact signed region URI expected on this receiver.
    pub region_uri: String,
    /// Applications permitted to submit telemetry to this receiver.
    pub allowed_apps: Vec<String>,
}

#[derive(Clone, Debug, PartialEq)]
pub(super) struct VerifiedIdentity {
    pub principal: String,
    pub workspace_id: String,
    pub app: String,
    pub app_instance: String,
    pub workload: String,
    pub region_uri: String,
    pub external_workload_uri: String,
    pub workspace_scoped_app_id: String,
}

impl UwiIdentityPolicy {
    pub(super) fn validate(&self) -> Result<(), &'static str> {
        // This initial implementation understands only the serverless platform extension.
        if self.trust_domains.is_empty()
            || self
                .trust_domains
                .iter()
                .any(|domain| domain.trim().is_empty())
            || self.region_uri.is_empty()
            || self.allowed_apps.is_empty()
            || self.allowed_apps.iter().any(String::is_empty)
        {
            return Err("UWI policy requires nonempty trust_domains, region_uri and allowed_apps");
        }
        Ok(())
    }

    pub(super) fn extract(&self, leaf_der: &[u8]) -> Result<VerifiedIdentity, &'static str> {
        self.validate()?;
        let (rest, cert) = X509Certificate::from_der(leaf_der).map_err(|_| "Invalid X509")?;
        if !rest.is_empty() || !cert.validity().is_valid() {
            return Err("Invalid or expired certificate");
        }
        // Reject duplicate extensions, including SANs, rather than choosing an interpretation.
        cert.extensions_map()
            .map_err(|_| "Duplicate X509 extension")?;
        let ext = cert
            .extensions()
            .iter()
            .find(|e| e.oid.to_id_string() == UWI_OID)
            .ok_or("Missing UWI attributes")?;
        // uwicert.MarshalExtension adds an inner OCTET STRING around the protobuf.
        let (rest, octets) = parse_der_octetstring(ext.value).map_err(|_| "Invalid UWI wrapper")?;
        if !rest.is_empty() {
            return Err("Trailing UWI data");
        }
        let attrs = WorkloadIdentityAttributes::decode(
            octets.as_slice().map_err(|_| "Invalid UWI octets")?,
        )
        .map_err(|_| "Invalid UWI protobuf")?;
        if attrs.app.is_empty()
            || attrs.app_instance.is_empty()
            || attrs.workload.is_empty()
            || attrs.region_uri != self.region_uri
            || !self.allowed_apps.contains(&attrs.app)
        {
            return Err("UWI outside receiver policy");
        }
        let san = cert
            .subject_alternative_name()
            .map_err(|_| "Invalid SAN")?
            .ok_or("Missing SAN")?;
        let mut principals = san
            .value
            .general_names
            .iter()
            .filter_map(|name| match name {
                GeneralName::URI(uri) if uri.starts_with("principal:") => Some(*uri),
                _ => None,
            });
        let principal = principals.next().ok_or("Missing UWI SAN")?;
        if principals.next().is_some() {
            return Err("Ambiguous UWI SAN");
        }
        let uri = url::Url::parse(principal).map_err(|_| "Invalid UWI URI")?;
        if uri.scheme() != "principal"
            || !uri
                .host_str()
                .is_some_and(|host| self.trust_domains.iter().any(|domain| domain == host))
            || !uri.username().is_empty()
            || uri.password().is_some()
            || uri.port().is_some()
            || uri.query().is_some()
            || uri.fragment().is_some()
        {
            return Err("Invalid UWI trust domain");
        }
        let segments: Vec<_> = uri.path_segments().ok_or("Invalid UWI path")?.collect();
        if segments
            != [
                "apps",
                &attrs.app,
                "instances",
                &attrs.app_instance,
                "workloads",
                &attrs.workload,
            ]
        {
            return Err("SAN and UWI attributes differ");
        }
        let mut extensions = attrs
            .extensions
            .iter()
            .filter(|e| e.type_url.rsplit_once('/').map(|(_, name)| name) == Some(SERVERLESS_TYPE));
        let serverless = extensions.next().ok_or("Missing serverless attributes")?;
        if extensions.next().is_some() {
            return Err("Duplicate serverless attributes");
        }
        let serverless = ServerlessWorkloadIdentityAttributes::decode(serverless.value.as_slice())
            .map_err(|_| "Invalid serverless attributes")?;
        if serverless.assignment_state != 3 || serverless.workspace_id.is_empty() {
            return Err("Assigned workspace required");
        }
        Ok(VerifiedIdentity {
            principal: principal.into(),
            workspace_id: serverless.workspace_id,
            app: attrs.app,
            app_instance: attrs.app_instance,
            workload: attrs.workload,
            region_uri: attrs.region_uri,
            external_workload_uri: serverless.external_workload_uri,
            workspace_scoped_app_id: serverless.workspace_scoped_app_id,
        })
    }
}

#[derive(Clone, PartialEq, Message)]
struct WorkloadIdentityAttributes {
    #[prost(string, tag = "1")]
    app: String,
    #[prost(string, tag = "2")]
    app_instance: String,
    #[prost(string, tag = "3")]
    workload: String,
    #[prost(string, tag = "4")]
    region_uri: String,
    #[prost(message, repeated, tag = "5")]
    extensions: Vec<prost_types::Any>,
}

#[derive(Clone, PartialEq, Message)]
struct ServerlessWorkloadIdentityAttributes {
    #[prost(int32, tag = "1")]
    network_trust_tier: i32,
    #[prost(int32, tag = "2")]
    assignment_state: i32,
    #[prost(string, tag = "3")]
    workspace_id: String,
    #[prost(string, tag = "4")]
    external_workload_uri: String,
    #[prost(string, tag = "5")]
    workspace_scoped_app_id: String,
}

#[cfg(test)]
pub(super) mod tests {
    use super::*;
    use openssl::{
        asn1::{Asn1Object, Asn1OctetString, Asn1Time},
        bn::BigNum,
        hash::MessageDigest,
        pkey::PKey,
        rsa::Rsa,
        x509::{X509, X509Extension, X509NameBuilder, extension::SubjectAlternativeName},
    };

    const PRINCIPAL: &str = "principal://serverless.databricks.com/apps/test-app/instances/test-instance/workloads/driver";

    fn policy() -> UwiIdentityPolicy {
        UwiIdentityPolicy {
            trust_domains: vec!["serverless.databricks.com".into()],
            region_uri: "region:test".into(),
            allowed_apps: vec!["test-app".into()],
        }
    }

    fn attributes() -> WorkloadIdentityAttributes {
        WorkloadIdentityAttributes {
            app: "test-app".into(),
            app_instance: "test-instance".into(),
            workload: "driver".into(),
            region_uri: "region:test".into(),
            extensions: vec![prost_types::Any {
                type_url: format!("type.googleapis.com/{SERVERLESS_TYPE}"),
                value: ServerlessWorkloadIdentityAttributes {
                    assignment_state: 3,
                    workspace_id: "12345".into(),
                    external_workload_uri: "dbr-cluster:test-cluster".into(),
                    ..Default::default()
                }
                .encode_to_vec(),
            }],
        }
    }

    fn wrap(attrs: &WorkloadIdentityAttributes) -> Vec<u8> {
        let bytes = attrs.encode_to_vec();
        // Test-only DER encoder; production parsing uses x509-parser.
        let mut result = vec![4, 0x82, (bytes.len() >> 8) as u8, bytes.len() as u8];
        // DER requires the shortest length encoding.
        if bytes.len() < 128 {
            result = vec![4, bytes.len() as u8];
        } else if bytes.len() < 256 {
            result = vec![4, 0x81, bytes.len() as u8];
        }
        result.extend(bytes);
        result
    }

    fn certificate(principals: &[&str], extensions: &[Vec<u8>], expired: bool) -> Vec<u8> {
        certificate_and_key(principals, extensions, expired, None, false)
            .1
            .to_der()
            .unwrap()
    }

    fn certificate_and_key(
        principals: &[&str],
        extensions: &[Vec<u8>],
        expired: bool,
        issuer: Option<&(PKey<openssl::pkey::Private>, X509)>,
        ca: bool,
    ) -> (PKey<openssl::pkey::Private>, X509) {
        let key = PKey::from_rsa(Rsa::generate(2048).unwrap()).unwrap();
        let mut name = X509NameBuilder::new().unwrap();
        name.append_entry_by_text("CN", if ca { "test-root" } else { "not-an-identity" })
            .unwrap();
        let name = name.build();
        let mut cert = X509::builder().unwrap();
        cert.set_version(2).unwrap();
        cert.set_serial_number(&BigNum::from_u32(1).unwrap().to_asn1_integer().unwrap())
            .unwrap();
        cert.set_subject_name(&name).unwrap();
        cert.set_issuer_name(issuer.map(|(_, cert)| cert.subject_name()).unwrap_or(&name))
            .unwrap();
        cert.set_pubkey(&key).unwrap();
        cert.set_not_before(&Asn1Time::from_unix(0).unwrap())
            .unwrap();
        let not_after = if expired {
            Asn1Time::from_unix(1).unwrap()
        } else {
            Asn1Time::days_from_now(1).unwrap()
        };
        cert.set_not_after(&not_after).unwrap();
        if ca {
            cert.append_extension(
                openssl::x509::extension::BasicConstraints::new()
                    .critical()
                    .ca()
                    .build()
                    .unwrap(),
            )
            .unwrap();
        }
        if !principals.is_empty() {
            let mut sans = SubjectAlternativeName::new();
            for principal in principals {
                sans.uri(principal);
            }
            let san = sans.build(&cert.x509v3_context(None, None)).unwrap();
            cert.append_extension(san).unwrap();
        }
        for value in extensions {
            cert.append_extension(
                X509Extension::new_from_der(
                    &Asn1Object::from_str(UWI_OID).unwrap(),
                    false,
                    &Asn1OctetString::new_from_bytes(value).unwrap(),
                )
                .unwrap(),
            )
            .unwrap();
        }
        cert.sign(
            issuer.map(|(key, _)| key).unwrap_or(&key),
            MessageDigest::sha256(),
        )
        .unwrap();
        (key, cert.build())
    }

    pub(crate) fn assigned_certificate() -> Vec<u8> {
        certificate(&[PRINCIPAL], &[wrap(&attributes())], false)
    }

    pub(crate) fn assigned_client_identity() -> (Vec<u8>, Vec<u8>, Vec<u8>) {
        let root = certificate_and_key(&[], &[], false, None, true);
        let (key, client) = certificate_and_key(
            &[PRINCIPAL],
            &[wrap(&attributes())],
            false,
            Some(&root),
            false,
        );
        (
            root.1.to_pem().unwrap(),
            client.to_pem().unwrap(),
            key.private_key_to_pem_pkcs8().unwrap(),
        )
    }

    #[test]
    fn decodes_universe_uwicert_extension() {
        // Generated by Universe's TestGenerateServerlessUWICert/MarshalExtension;
        // security/prodam/dbts/testdata/uwi-serverless-cert.pem. Only the signed
        // extension bytes are retained so the certificate fixture cannot expire.
        let extension = hex::decode("0481ac0a127365727665726c6573732d636f6d707574651213746573742d636c75737465722d6162633132331a06647269766572220d6177732f75732d776573742d322a6a0a50747970652e676f6f676c65617069732e636f6d2f73656375726974792e70726f64616d2e636f6e746578742e5365727665726c657373576f726b6c6f61644964656e74697479417474726962757465731216080410031a1031323334353637383930313233343536").unwrap();
        assert_eq!(UWI_OID, "1.3.6.1.4.1.42.113.1");
        let principal = "principal://serverless.databricks.com/apps/serverless-compute/instances/test-cluster-abc123/workloads/driver";
        let policy = UwiIdentityPolicy {
            region_uri: "aws/us-west-2".into(),
            allowed_apps: vec!["serverless-compute".into()],
            ..policy()
        };
        assert_eq!(
            policy
                .extract(&certificate(&[principal], &[extension], false))
                .unwrap(),
            VerifiedIdentity {
                principal: principal.into(),
                workspace_id: "1234567890123456".into(),
                app: "serverless-compute".into(),
                app_instance: "test-cluster-abc123".into(),
                workload: "driver".into(),
                region_uri: "aws/us-west-2".into(),
                external_workload_uri: "".into(),
                workspace_scoped_app_id: "".into(),
            }
        );
    }

    #[test]
    fn incomplete_receiver_policies_are_rejected_before_extraction() {
        let mut empty_region_attrs = attributes();
        empty_region_attrs.region_uri.clear();
        for invalid in [
            UwiIdentityPolicy {
                trust_domains: vec![],
                ..policy()
            },
            UwiIdentityPolicy {
                trust_domains: vec!["serverless.databricks.com".into(), " ".into()],
                ..policy()
            },
            UwiIdentityPolicy {
                region_uri: "".into(),
                ..policy()
            },
            UwiIdentityPolicy {
                allowed_apps: vec!["test-app".into(), "".into()],
                ..policy()
            },
        ] {
            let error = invalid.validate().unwrap_err();
            let attrs = if invalid.region_uri.is_empty() {
                empty_region_attrs.clone()
            } else {
                attributes()
            };
            assert_eq!(
                invalid.extract(&certificate(&[PRINCIPAL], &[wrap(&attrs)], false)),
                Err(error)
            );
        }
    }

    #[test]
    fn serverless_attributes_are_selected_by_type_url() {
        let mut attrs = attributes();
        let decoy = prost_types::Any {
            type_url: "type.googleapis.com/security.prodam.context.OtherAttributes".into(),
            value: ServerlessWorkloadIdentityAttributes {
                assignment_state: 3,
                workspace_id: "999".into(),
                ..Default::default()
            }
            .encode_to_vec(),
        };
        attrs.extensions.insert(0, decoy.clone());
        assert_eq!(
            policy()
                .extract(&certificate(&[PRINCIPAL], &[wrap(&attrs)], false))
                .unwrap()
                .workspace_id,
            "12345"
        );
        attrs.extensions = vec![decoy];
        assert_eq!(
            policy().extract(&certificate(&[PRINCIPAL], &[wrap(&attrs)], false)),
            Err("Missing serverless attributes")
        );
        attrs.extensions = attributes().extensions;
        attrs.extensions[0].value = vec![0xff];
        assert_eq!(
            policy().extract(&certificate(&[PRINCIPAL], &[wrap(&attrs)], false)),
            Err("Invalid serverless attributes")
        );
    }

    #[test]
    fn assigned_workspace_and_workload_are_read_from_signed_attributes() {
        let cert = certificate(&[PRINCIPAL], &[wrap(&attributes())], false);
        let identity = policy().extract(&cert).unwrap();
        assert_eq!(identity.workspace_id, "12345");
        assert_eq!(identity.app_instance, "test-instance");
        assert_eq!(identity.workload, "driver");
        assert_eq!(identity.external_workload_uri, "dbr-cluster:test-cluster");
    }

    #[test]
    fn missing_attributes_or_san_are_rejected() {
        assert!(
            policy()
                .extract(&certificate(&[PRINCIPAL], &[], false))
                .is_err()
        );
        assert!(
            policy()
                .extract(&certificate(&[], &[wrap(&attributes())], false))
                .is_err()
        );
    }

    #[test]
    fn duplicate_identity_extensions_are_rejected() {
        let ext = wrap(&attributes());
        assert!(
            policy()
                .extract(&certificate(&[PRINCIPAL], &[ext.clone(), ext], false))
                .is_err()
        );
        let mut attrs = attributes();
        attrs.extensions.push(attrs.extensions[0].clone());
        assert!(
            policy()
                .extract(&certificate(&[PRINCIPAL], &[wrap(&attrs)], false))
                .is_err()
        );
    }

    #[test]
    fn malformed_or_trailing_extension_data_is_rejected() {
        for ext in [vec![], vec![4, 1, 0xff], {
            let mut ext = wrap(&attributes());
            ext.push(0);
            ext
        }] {
            assert!(
                policy()
                    .extract(&certificate(&[PRINCIPAL], &[ext], false))
                    .is_err()
            );
        }
    }

    #[test]
    fn configured_trust_domains_are_matched_exactly() {
        let policy = UwiIdentityPolicy {
            trust_domains: vec![
                "serverless.databricks.com".into(),
                "alternate.example.com".into(),
            ],
            ..policy()
        };
        for domain in ["serverless.databricks.com", "alternate.example.com"] {
            let principal = PRINCIPAL.replace("serverless.databricks.com", domain);
            let identity = policy
                .extract(&certificate(&[&principal], &[wrap(&attributes())], false))
                .unwrap();
            assert_eq!(identity.principal, principal);
            assert_eq!(identity.workspace_id, "12345");
        }
        for domain in [
            "unlisted.example.com",
            "child.alternate.example.com",
            "alternate.example.com.evil",
        ] {
            let principal = PRINCIPAL.replace("serverless.databricks.com", domain);
            assert_eq!(
                policy.extract(&certificate(&[&principal], &[wrap(&attributes())], false)),
                Err("Invalid UWI trust domain")
            );
        }
    }

    #[test]
    fn san_must_match_attributes_and_receiver_trust_domain() {
        for principal in [
            PRINCIPAL.replace("test-app", "other-app"),
            PRINCIPAL.replace("serverless.databricks.com", "control-plane.databricks.com"),
            format!("{PRINCIPAL}?claim=12345"),
            PRINCIPAL.replace("driver", "driver/child"),
        ] {
            assert!(
                policy()
                    .extract(&certificate(&[&principal], &[wrap(&attributes())], false))
                    .is_err()
            );
        }
        assert!(
            policy()
                .extract(&certificate(
                    &[PRINCIPAL, PRINCIPAL],
                    &[wrap(&attributes())],
                    false
                ))
                .is_err()
        );
    }

    #[test]
    fn unassigned_or_empty_workspace_is_rejected() {
        for (state, workspace) in [
            (0, ""),
            (1, ""),
            (2, ""),
            (3, ""),
            (2, "12345"),
            (99, "12345"),
        ] {
            let mut attrs = attributes();
            attrs.extensions[0].value = ServerlessWorkloadIdentityAttributes {
                assignment_state: state,
                workspace_id: workspace.into(),
                ..Default::default()
            }
            .encode_to_vec();
            assert!(
                policy()
                    .extract(&certificate(&[PRINCIPAL], &[wrap(&attrs)], false))
                    .is_err()
            );
        }
    }

    #[test]
    fn region_and_application_policy_are_enforced() {
        let cert = certificate(&[PRINCIPAL], &[wrap(&attributes())], false);
        let mut other_region = policy();
        other_region.region_uri = "region:other".into();
        let mut other_app = policy();
        other_app.allowed_apps = vec!["other-app".into()];
        assert!(other_region.extract(&cert).is_err());
        assert!(other_app.extract(&cert).is_err());
        assert!(
            UwiIdentityPolicy {
                allowed_apps: vec![],
                ..policy()
            }
            .validate()
            .is_err()
        );
    }

    #[test]
    fn expired_and_non_certificate_data_are_rejected() {
        assert!(
            policy()
                .extract(&certificate(&[PRINCIPAL], &[wrap(&attributes())], true))
                .is_err()
        );
        assert!(policy().extract(b"garbage").is_err());
        let mut cert = certificate(&[PRINCIPAL], &[wrap(&attributes())], false);
        cert.push(0);
        assert!(policy().extract(&cert).is_err());
    }
}
