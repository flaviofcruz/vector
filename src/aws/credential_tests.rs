use std::{
    sync::{Arc, Mutex},
    time::{Duration, SystemTime},
};

use aws_credential_types::provider::ProvideCredentials;
use aws_smithy_async::time::TimeSource;
use aws_smithy_runtime_api::client::{
    http::{
        HttpClient, HttpConnector, HttpConnectorFuture, HttpConnectorSettings, SharedHttpConnector,
    },
    orchestrator::{HttpRequest, HttpResponse},
    runtime_components::RuntimeComponents,
};
use aws_smithy_types::body::SdkBody;
use aws_types::{region::Region, sdk_config::SharedHttpClient};

use super::auth::{AwsAuthentication, assume_role_sdk_config};

// Only transport is replaced: the real SDK discovers credentials, exchanges the
// token, signs AssumeRole, and resolves both destinations without contacting AWS.
#[derive(Clone, Debug, Default)]
struct CredentialHttp {
    requests: Arc<Mutex<Vec<HttpRequest>>>,
    settings: Arc<Mutex<Vec<HttpConnectorSettings>>>,
    clock: CredentialClock,
}

#[derive(Clone, Debug)]
struct CredentialClock(Arc<Mutex<SystemTime>>);

impl Default for CredentialClock {
    fn default() -> Self {
        Self(Arc::new(Mutex::new(SystemTime::now())))
    }
}

impl TimeSource for CredentialClock {
    fn now(&self) -> SystemTime {
        *self.0.lock().unwrap()
    }
}

impl HttpConnector for CredentialHttp {
    fn call(&self, request: HttpRequest) -> HttpConnectorFuture {
        let uri = request.uri();
        let mut response = if uri.starts_with("http://169.254.169.254/") {
            let body = if uri.ends_with("/latest/api/token") {
                "synthetic-imds-token"
            } else if uri.ends_with("/iam/security-credentials/") {
                "synthetic-role"
            } else {
                assert!(
                    uri.ends_with("/iam/security-credentials/synthetic-role"),
                    "{uri}"
                );
                r#"{"Code":"Success","AccessKeyId":"synthetic-access","SecretAccessKey":"synthetic-secret","Token":"synthetic-session","Expiration":"2099-01-01T00:00:00Z"}"#
            };
            HttpResponse::new(200.try_into().unwrap(), SdkBody::from(body))
        } else {
            let body = std::str::from_utf8(request.body().bytes().unwrap()).unwrap();
            let action = body
                .split('&')
                .find_map(|s| s.strip_prefix("Action="))
                .unwrap();
            assert!(matches!(
                action,
                "AssumeRole" | "AssumeRoleWithWebIdentity" | "GetCallerIdentity"
            ));
            let expiration =
                chrono::DateTime::<chrono::Utc>::from(self.clock.now() + Duration::from_secs(3600))
                    .to_rfc3339();
            let xml = if action == "GetCallerIdentity" {
                "<GetCallerIdentityResponse><GetCallerIdentityResult><UserId>test:session</UserId><Account>123456789012</Account><Arn>arn:aws:sts::123456789012:assumed-role/test/session</Arn></GetCallerIdentityResult></GetCallerIdentityResponse>".to_owned()
            } else {
                format!(
                    "<{action}Response><{action}Result><Credentials><AccessKeyId>synthetic-access</AccessKeyId><SecretAccessKey>synthetic-secret</SecretAccessKey><SessionToken>synthetic-session</SessionToken><Expiration>{expiration}</Expiration></Credentials><AssumedRoleUser><Arn>arn:aws:sts::123456789012:assumed-role/test/session</Arn><AssumedRoleId>test:session</AssumedRoleId></AssumedRoleUser></{action}Result></{action}Response>"
                )
            };
            HttpResponse::new(200.try_into().unwrap(), SdkBody::from(xml))
        };
        response
            .headers_mut()
            .insert("x-aws-ec2-metadata-token-ttl-seconds", "21600");
        self.requests.lock().unwrap().push(request);
        HttpConnectorFuture::ready(Ok(response))
    }
}

impl HttpClient for CredentialHttp {
    fn http_connector(
        &self,
        settings: &HttpConnectorSettings,
        _: &RuntimeComponents,
    ) -> SharedHttpConnector {
        self.settings.lock().unwrap().push(settings.clone());
        SharedHttpConnector::new(self.clone())
    }
}

async fn run_credential_case() {
    let mode = std::env::var("VECTOR_CREDENTIAL_TEST_MODE").unwrap();
    let region = std::env::var("VECTOR_CREDENTIAL_TEST_REGION").unwrap();
    let fips: bool = std::env::var("VECTOR_CREDENTIAL_TEST_FIPS")
        .unwrap()
        .parse()
        .unwrap();
    let token_file = std::env::var("VECTOR_CREDENTIAL_TEST_TOKEN_FILE").unwrap();
    let http = CredentialHttp::default();
    if mode == "cached-assume-role-irsa" {
        run_cached_credential_case(&http, &region, fips, &token_file).await;
        return;
    }
    let connector = SharedHttpClient::new(http.clone());
    let auth_region = Region::new(region.clone());
    let partition = if region.starts_with("us-gov-") {
        "aws-us-gov"
    } else {
        "aws"
    };
    let role = format!("arn:{partition}:iam::123456789012:role/test");
    let auth = match mode.as_str() {
        "web-identity" => serde_json::json!({
            "role_arn": role, "web_identity_token_file": token_file,
            "session_name": "credential-test", "region": region,
        }),
        "file-profile" => serde_json::json!({
            "credentials_file": std::env::var("VECTOR_CREDENTIAL_TEST_FILE").unwrap(),
            "profile": "component-profile",
        }),
        "assume-role-irsa" => serde_json::json!({
            "assume_role": role, "session_name": "credential-test",
        }),
        _ => serde_json::json!({
            "imds": { "max_attempts": 2, "connect_timeout_seconds": 7, "read_timeout_seconds": 11 },
        }),
    };
    let auth: AwsAuthentication = serde_json::from_value(auth).unwrap();
    let provider = auth
        .credentials_provider_with_connector(auth_region, || Ok(connector.clone()))
        .await
        .unwrap();
    for token in ["synthetic-token-first", "synthetic-token-rotated"] {
        std::fs::write(&token_file, token).unwrap();
        assert_eq!(
            provider
                .provide_credentials()
                .await
                .unwrap()
                .access_key_id(),
            "synthetic-access"
        );
        let mut requests = http.requests.lock().unwrap();
        if mode == "imds" {
            assert!(
                requests
                    .iter()
                    .all(|r| r.uri().starts_with("http://169.254.169.254/"))
            );
            assert!(requests.iter().any(|r| {
                r.uri()
                    .ends_with("/iam/security-credentials/synthetic-role")
            }));
            assert!(http.settings.lock().unwrap().iter().all(|s| {
                s.connect_timeout() == Some(Duration::from_secs(7))
                    && s.read_timeout() == Some(Duration::from_secs(11))
            }));
        } else if mode == "environment" {
            assert!(requests.is_empty());
        } else {
            let service = if fips && !region.starts_with("us-gov-") {
                "sts-fips"
            } else {
                "sts"
            };
            let expected_uri = format!("https://{service}.{region}.amazonaws.com/");
            let actions = match mode.as_str() {
                "assume-role-irsa" => vec!["AssumeRoleWithWebIdentity", "AssumeRole"],
                "profile-role" | "file-profile" => vec!["AssumeRole"],
                _ => vec!["AssumeRoleWithWebIdentity"],
            };
            assert_eq!(requests.len(), actions.len());
            for (request, action) in requests.iter().zip(actions) {
                assert_eq!(request.uri(), expected_uri);
                let body = std::str::from_utf8(request.body().bytes().unwrap()).unwrap();
                assert!(
                    body.split('&')
                        .any(|part| part == format!("Action={action}"))
                );
                if action == "AssumeRoleWithWebIdentity" {
                    assert!(body.contains(&format!("WebIdentityToken={token}")));
                    assert!(request.headers().get("authorization").is_none());
                } else {
                    assert!(
                        request
                            .headers()
                            .get("authorization")
                            .unwrap()
                            .contains(&format!("/{region}/sts/aws4_request"))
                    );
                }
            }
        }
        requests.clear();
    }
}

// Drive the real service-side identity cache through a hit and an expiry. The
// manual clock avoids waiting an hour or reducing credential lifetime in AWS.
async fn run_cached_credential_case(
    http: &CredentialHttp,
    region: &str,
    fips: bool,
    token_file: &str,
) {
    let connector = SharedHttpClient::new(http.clone());
    let region_value = Region::new(region.to_owned());
    let config = assume_role_sdk_config(&region_value, connector.clone()).await;
    let partition = if region.starts_with("us-gov-") {
        "aws-us-gov"
    } else {
        "aws"
    };
    let auth: AwsAuthentication = serde_json::from_value(serde_json::json!({
        "assume_role": format!("arn:{partition}:iam::123456789012:role/test"),
        "session_name": "credential-refresh-test",
    }))
    .unwrap();
    let role = auth
        .credentials_provider_with_connector(region_value, || Ok(connector.clone()))
        .await
        .unwrap();
    let config = config
        .into_builder()
        .credentials_provider(role)
        .time_source(http.clock.clone())
        .identity_cache(
            AwsAuthentication::default()
                .credentials_cache()
                .await
                .unwrap(),
        )
        .build();
    let client = aws_sdk_sts::Client::new(&config);
    let service = if fips && !region.starts_with("us-gov-") {
        "sts-fips"
    } else {
        "sts"
    };
    let expected_uri = format!("https://{service}.{region}.amazonaws.com/");
    for (step, actions) in [
        (
            0,
            vec![
                "AssumeRoleWithWebIdentity",
                "AssumeRole",
                "GetCallerIdentity",
            ],
        ),
        (1, vec!["GetCallerIdentity"]),
        (
            2,
            vec![
                "AssumeRoleWithWebIdentity",
                "AssumeRole",
                "GetCallerIdentity",
            ],
        ),
    ] {
        let token = if step == 2 {
            "synthetic-token-rotated"
        } else {
            "synthetic-token-first"
        };
        std::fs::write(token_file, token).unwrap();
        if step == 2 {
            *http.clock.0.lock().unwrap() += Duration::from_secs(7200);
        }
        assert_eq!(
            client.get_caller_identity().send().await.unwrap().account(),
            Some("123456789012")
        );
        let mut requests = http.requests.lock().unwrap();
        assert_eq!(requests.len(), actions.len());
        for (request, action) in requests.iter().zip(actions) {
            assert_eq!(request.uri(), expected_uri);
            let body = std::str::from_utf8(request.body().bytes().unwrap()).unwrap();
            assert!(
                body.split('&')
                    .any(|part| part == format!("Action={action}"))
            );
            if action == "AssumeRoleWithWebIdentity" {
                assert!(body.contains(&format!("WebIdentityToken={token}")));
            }
        }
        requests.clear();
    }
}

#[tokio::test]
async fn credential_endpoint_environment_and_profile_precedence() {
    if std::env::var_os("VECTOR_CREDENTIAL_TEST_MODE").is_some() {
        run_credential_case().await;
        return;
    }
    // Child processes isolate AWS configuration from other tests and ensure no
    // developer credentials or mounted identity tokens can enter the chain.
    for region in [
        "us-east-1",
        "us-east-2",
        "us-west-1",
        "us-west-2",
        "ca-central-1",
        "ca-west-1",
        "us-gov-east-1",
        "us-gov-west-1",
    ] {
        for (environment, profile, expected) in [
            (None, None, false),
            (Some("true"), None, true),
            (Some("false"), None, false),
            (None, Some("true"), true),
            (None, Some("false"), false),
            (Some("true"), Some("false"), true),
            (Some("false"), Some("true"), false),
        ] {
            for mode in [
                "irsa",
                "assume-role-irsa",
                "cached-assume-role-irsa",
                "web-identity",
                "profile-role",
                "profile-web-identity",
                "file-profile",
                "environment",
                "imds",
            ] {
                let directory = tempfile::tempdir().unwrap();
                let config = directory.path().join("config");
                let credentials = directory.path().join("credentials");
                let token = directory.path().join("token");
                let partition = if region.starts_with("us-gov-") {
                    "aws-us-gov"
                } else {
                    "aws"
                };
                let role = format!("arn:{partition}:iam::123456789012:role/test");
                let mut contents = "[profile credential-test]\n".to_owned();
                if let Some(profile) = profile {
                    contents.push_str(&format!("use_fips_endpoint = {profile}\n"));
                }
                if mode == "profile-web-identity" {
                    contents.push_str(&format!("role_arn = {role}\nweb_identity_token_file = {}\nrole_session_name = credential-test\n", token.display()));
                }
                std::fs::write(&config, contents).unwrap();
                std::fs::write(&credentials, if mode == "profile-role" {
                    format!("[credential-test]\nrole_arn = {role}\nsource_profile = base\n[base]\naws_access_key_id = synthetic-access\naws_secret_access_key = synthetic-secret\n")
                } else { String::new() }).unwrap();
                // The process-wide policy comes from the environment or selected
                // AWS profile, even when auth selects a separate credential file.
                let explicit_file = directory.path().join("explicit-credentials");
                std::fs::write(&explicit_file, format!("[component-profile]\nuse_fips_endpoint = {}\nrole_arn = {role}\nsource_profile = base\n[base]\naws_access_key_id = synthetic-access\naws_secret_access_key = synthetic-secret\n", !expected)).unwrap();
                let mut child = std::process::Command::new(std::env::current_exe().unwrap());
                for (key, _) in std::env::vars_os() {
                    if key.to_string_lossy().starts_with("AWS_") {
                        child.env_remove(key);
                    }
                }
                child.args(["--exact", "aws::credential_tests::credential_endpoint_environment_and_profile_precedence", "--nocapture"])
                    .env("VECTOR_CREDENTIAL_TEST_MODE", mode)
                    .env("VECTOR_CREDENTIAL_TEST_REGION", region)
                    .env("VECTOR_CREDENTIAL_TEST_FIPS", expected.to_string())
                    .env("VECTOR_CREDENTIAL_TEST_TOKEN_FILE", &token)
                    .env("VECTOR_CREDENTIAL_TEST_FILE", &explicit_file)
                    .env("AWS_PROFILE", "credential-test")
                    .env("AWS_CONFIG_FILE", &config)
                    .env("AWS_SHARED_CREDENTIALS_FILE", &credentials)
                    .env("AWS_REGION", "ap-southeast-2")
                    .env("AWS_EC2_METADATA_DISABLED", if mode == "imds" { "false" } else { "true" });
                if let Some(environment) = environment {
                    child.env("AWS_USE_FIPS_ENDPOINT", environment);
                }
                if matches!(
                    mode,
                    "irsa" | "assume-role-irsa" | "cached-assume-role-irsa"
                ) {
                    child
                        .env("AWS_ROLE_ARN", &role)
                        .env("AWS_WEB_IDENTITY_TOKEN_FILE", &token)
                        .env("AWS_ROLE_SESSION_NAME", "credential-test");
                }
                if mode == "environment" {
                    child
                        .env("AWS_ACCESS_KEY_ID", "synthetic-access")
                        .env("AWS_SECRET_ACCESS_KEY", "synthetic-secret");
                }
                let output = child.output().unwrap();
                assert!(
                    output.status.success(),
                    "region={region} env={environment:?} profile={profile:?} mode={mode}: {} {}",
                    String::from_utf8_lossy(&output.stdout),
                    String::from_utf8_lossy(&output.stderr)
                );
                assert!(String::from_utf8_lossy(&output.stdout).contains("1 passed"));
            }
        }
    }
}
