use super::{
    AwsAuthentication, ClientBuilder, Region, SdkConfig,
    auth::assume_role_sdk_config,
    create_client,
    test_util::{CaptureClient, TEST_SECRET},
};
use crate::{
    common::s3::S3ClientBuilder, config::ProxyConfig,
    sinks::aws_kinesis::streams::KinesisClientBuilder,
};
use aws_config::sts::AssumeRoleProviderBuilder;
use aws_credential_types::{Credentials, provider::ProvideCredentials};
use aws_types::sdk_config::SharedHttpClient;

// Keep Vector's config/auth loading and the actual SDK builders, replacing only
// transport so these requests cannot contact AWS or use real credentials.
struct CapturingBuilder<B> {
    inner: B,
    http: CaptureClient,
}

impl<B: ClientBuilder> ClientBuilder for CapturingBuilder<B> {
    type Client = B::Client;

    fn build(&self, config: &SdkConfig) -> Self::Client {
        self.inner.build(
            &config
                .clone()
                .into_builder()
                .http_client(self.http.clone())
                .build(),
        )
    }
}

fn assert_request(http: &CaptureClient, expected: &str, region: &str, service: &str) {
    let requests = http.requests.lock().unwrap();
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].uri(), expected);
    assert!(
        requests[0]
            .headers()
            .get("authorization")
            .unwrap()
            .contains(&format!("/{region}/{service}/aws4_request"))
    );
}

async fn run_case() {
    let region = std::env::var("VECTOR_ENDPOINT_TEST_REGION").unwrap();
    let use_fips: bool = std::env::var("VECTOR_ENDPOINT_TEST_FIPS")
        .unwrap()
        .parse()
        .unwrap();
    let endpoint_mode = std::env::var("VECTOR_ENDPOINT_TEST_OVERRIDE").unwrap();
    let endpoint = (endpoint_mode == "sink").then(|| "https://sink.example.invalid".to_owned());
    let override_host = match endpoint_mode.as_str() {
        "sink" => Some("sink.example.invalid"),
        "environment" => Some("environment.example.invalid"),
        "profile" => Some("profile.example.invalid"),
        "none" => None,
        _ => unreachable!(),
    };
    let auth = AwsAuthentication::AccessKey {
        access_key_id: "test-access".to_owned().into(),
        secret_access_key: TEST_SECRET.to_owned().into(),
        assume_role: None,
        external_id: None,
        region: None,
        session_name: None,
        session_token: None,
    };
    let domain = if region.starts_with("cn-") {
        "amazonaws.com.cn"
    } else {
        "amazonaws.com"
    };
    let unsupported_fips = use_fips && region.starts_with("cn-");

    let s3_http = CaptureClient::new("");
    let s3 = create_client(
        &CapturingBuilder {
            inner: S3ClientBuilder {
                force_path_style: Some(true),
            },
            http: s3_http.clone(),
        },
        &auth,
        Some(Region::new(region.clone())),
        endpoint.clone(),
        &ProxyConfig::default(),
        None,
        None,
    )
    .await
    .unwrap();
    let result = s3.head_bucket().bucket("test-bucket").send().await;
    if unsupported_fips && override_host.is_none() {
        let error = result.unwrap_err();
        assert!(format!("{error:?}").contains("FIPS"), "{error:?}");
        assert!(s3_http.requests.lock().unwrap().is_empty());
    } else {
        result.unwrap();
        let expected = if let Some(host) = override_host {
            format!("https://{host}/test-bucket/")
        } else if use_fips {
            format!("https://test-bucket.s3-fips.{region}.{domain}/")
        } else {
            format!("https://s3.{region}.{domain}/test-bucket/")
        };
        assert_request(&s3_http, &expected, &region, "s3");
    }

    let kinesis_http = CaptureClient::new(r#"{"StreamNames":[],"HasMoreStreams":false}"#);
    let kinesis = create_client(
        &CapturingBuilder {
            inner: KinesisClientBuilder,
            http: kinesis_http.clone(),
        },
        &auth,
        Some(Region::new(region.clone())),
        endpoint,
        &ProxyConfig::default(),
        None,
        None,
    )
    .await
    .unwrap();
    let result = kinesis.list_streams().send().await;
    result.unwrap();
    let service = if use_fips && !region.starts_with("us-gov-") {
        "kinesis-fips"
    } else {
        "kinesis"
    };
    let expected = if let Some(host) = override_host {
        format!("https://{host}/")
    } else {
        format!("https://{service}.{region}.{domain}/")
    };
    assert_request(&kinesis_http, &expected, &region, "kinesis");

    let sts_http = CaptureClient::new(
        "<AssumeRoleResponse><AssumeRoleResult><Credentials><AccessKeyId>test-temporary</AccessKeyId><SecretAccessKey>test-secret</SecretAccessKey><SessionToken>test-session</SessionToken><Expiration>2099-01-01T00:00:00Z</Expiration></Credentials><AssumedRoleUser><Arn>arn:aws:sts::123456789012:assumed-role/test/session</Arn><AssumedRoleId>test:session</AssumedRoleId></AssumedRoleUser></AssumeRoleResult></AssumeRoleResponse>",
    );
    let config = assume_role_sdk_config(
        &Region::new(region.clone()),
        SharedHttpClient::new(sts_http.clone()),
    )
    .await;
    assert_eq!(config.endpoint_url(), None);
    let provider = AssumeRoleProviderBuilder::new("arn:aws:iam::123456789012:role/test")
        .session_name("endpoint-test")
        .configure(&config)
        .build_from_provider(Credentials::from_keys("test-access", TEST_SECRET, None))
        .await;
    for _ in 0..2 {
        let result = provider.provide_credentials().await;
        assert_eq!(result.unwrap().access_key_id(), "test-temporary");
        let service = if use_fips && !region.starts_with("us-gov-") {
            "sts-fips"
        } else {
            "sts"
        };
        assert_request(
            &sts_http,
            &format!("https://{service}.{region}.{domain}/"),
            &region,
            "sts",
        );
        sts_http.requests.lock().unwrap().clear();
    }
}

#[tokio::test]
async fn fips_endpoint_environment_profile_and_override_precedence() {
    if std::env::var_os("VECTOR_ENDPOINT_TEST_REGION").is_some() {
        run_case().await;
        return;
    }
    // Isolate AWS environment/profile settings without mutating this process's
    // environment or OpenSSL policy, which other tests may be using concurrently.
    for region in ["us-east-1", "us-gov-west-1", "eu-west-1", "cn-north-1"] {
        for (environment, profile, expected) in [
            (None, None, false),
            (Some("true"), Some("false"), true),
            (Some("false"), Some("true"), false),
            (None, Some("true"), true),
            (None, Some("false"), false),
            (Some("true"), None, true),
            (Some("false"), None, false),
        ] {
            for endpoint_mode in ["none", "sink", "environment", "profile"] {
                let directory = tempfile::tempdir().unwrap();
                let credentials = directory.path().join("credentials");
                std::fs::write(&credentials, "").unwrap();
                let config = directory.path().join("config");
                let mut contents = "[profile endpoint-test]\n".to_owned();
                if let Some(profile) = profile {
                    contents.push_str(&format!("use_fips_endpoint = {profile}\n"));
                }
                if endpoint_mode != "none" {
                    contents.push_str("endpoint_url = https://profile.example.invalid\n");
                }
                std::fs::write(&config, contents).unwrap();
                let mut child = std::process::Command::new(std::env::current_exe().unwrap());
                for (name, _) in std::env::vars_os() {
                    if name.to_string_lossy().starts_with("AWS_") {
                        child.env_remove(name);
                    }
                }
                child.args(["--exact", "aws::endpoint_tests::fips_endpoint_environment_profile_and_override_precedence", "--nocapture"])
                    .env("VECTOR_ENDPOINT_TEST_REGION", region)
                    .env("VECTOR_ENDPOINT_TEST_FIPS", expected.to_string())
                    .env("VECTOR_ENDPOINT_TEST_OVERRIDE", endpoint_mode)
                    .env("AWS_CONFIG_FILE", &config)
                    .env("AWS_SHARED_CREDENTIALS_FILE", &credentials)
                    .env("AWS_PROFILE", "endpoint-test")
                    .env("AWS_REGION", "ap-southeast-2")
                    .env("AWS_EC2_METADATA_DISABLED", "true")
                    .env("AWS_ENDPOINT_URL_STS", "https://ignored-sts.example.invalid");
                if let Some(environment) = environment {
                    child.env("AWS_USE_FIPS_ENDPOINT", environment);
                }
                if matches!(endpoint_mode, "sink" | "environment") {
                    child.env("AWS_ENDPOINT_URL", "https://environment.example.invalid");
                }
                let output = child.output().unwrap();
                assert!(
                    output.status.success(),
                    "region={region} env={environment:?} profile={profile:?} override={endpoint_mode}: {} {}",
                    String::from_utf8_lossy(&output.stdout),
                    String::from_utf8_lossy(&output.stderr)
                );
                assert!(String::from_utf8_lossy(&output.stdout).contains("1 passed"));
            }
        }
    }
}
