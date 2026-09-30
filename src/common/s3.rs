use aws_sdk_s3::config;

use crate::aws::ClientBuilder;

pub(crate) struct S3ClientBuilder {
    pub force_path_style: Option<bool>,
}

impl ClientBuilder for S3ClientBuilder {
    type Client = aws_sdk_s3::client::Client;

    fn build(&self, config: &aws_types::SdkConfig) -> Self::Client {
        // The SDK rejects FIPS resolution with a custom endpoint.
        let use_fips = config.endpoint_url().is_none() && config.use_fips().unwrap_or(false);
        // AWS S3 FIPS endpoints require the bucket in the hostname.
        let force_path_style = !use_fips && self.force_path_style.unwrap_or(true);
        let mut builder = config::Builder::from(config).force_path_style(force_path_style);
        builder.set_use_fips(Some(use_fips));
        aws_sdk_s3::client::Client::from_conf(builder.build())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::aws::test_util::CaptureClient;

    #[tokio::test]
    async fn fips_s3_requests_use_regional_virtual_hosted_endpoints() {
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
            for requested in [None, Some(false), Some(true)] {
                let http = CaptureClient::new("");
                let client = S3ClientBuilder {
                    force_path_style: Some(true),
                }
                .build(&http.config(region, requested));
                client
                    .head_bucket()
                    .bucket("test-bucket")
                    .send()
                    .await
                    .unwrap();
                let expected = if requested == Some(true) {
                    format!("https://test-bucket.s3-fips.{region}.amazonaws.com/")
                } else {
                    format!("https://s3.{region}.amazonaws.com/test-bucket/")
                };
                let requests = http.requests.lock().unwrap();
                assert_eq!(requests.len(), 1);
                assert_eq!(requests[0].uri(), expected);
                assert!(
                    requests[0]
                        .headers()
                        .get("authorization")
                        .unwrap()
                        .contains(&format!("/{region}/s3/aws4_request"))
                );
            }
        }
    }

    #[tokio::test]
    async fn fips_s3_preserves_custom_endpoints_and_addressing() {
        for requested in [None, Some(false), Some(true)] {
            for force_path_style in [None, Some(false), Some(true)] {
                for region in ["us-east-1", "cn-north-1"] {
                    let http = CaptureClient::new("");
                    let config = http
                        .config(region, requested)
                        .into_builder()
                        .endpoint_url("https://custom-s3.example.invalid")
                        .build();
                    let client = S3ClientBuilder { force_path_style }.build(&config);
                    client
                        .head_bucket()
                        .bucket("test-bucket")
                        .send()
                        .await
                        .unwrap();
                    let requests = http.requests.lock().unwrap();
                    assert_eq!(requests.len(), 1);
                    assert_eq!(
                        requests[0].uri(),
                        if force_path_style == Some(false) {
                            "https://test-bucket.custom-s3.example.invalid/"
                        } else {
                            "https://custom-s3.example.invalid/test-bucket/"
                        }
                    );
                }
            }
        }
    }

    #[tokio::test]
    async fn fips_s3_addressing_follows_sdk_flag() {
        for requested in [None, Some(false), Some(true)] {
            for force_path_style in [None, Some(false), Some(true)] {
                let http = CaptureClient::new("");
                let client = S3ClientBuilder { force_path_style }
                    .build(&http.config("us-west-2", requested));
                client
                    .head_bucket()
                    .bucket("test-bucket")
                    .send()
                    .await
                    .unwrap();
                let expected = if requested == Some(true) {
                    "https://test-bucket.s3-fips.us-west-2.amazonaws.com/"
                } else if force_path_style == Some(false) {
                    "https://test-bucket.s3.us-west-2.amazonaws.com/"
                } else {
                    "https://s3.us-west-2.amazonaws.com/test-bucket/"
                };
                assert_eq!(http.requests.lock().unwrap()[0].uri(), expected);
            }
        }
    }

    #[tokio::test]
    async fn fips_s3_does_not_fallback_on_endpoint_resolution_failure() {
        for (region, endpoint) in [("cn-north-1", None), ("us-east-1", Some("not a URL"))] {
            let http = CaptureClient::new("");
            let mut config = http.config(region, Some(true)).into_builder();
            config.set_endpoint_url(endpoint.map(str::to_owned));
            let client = S3ClientBuilder {
                force_path_style: Some(true),
            }
            .build(&config.build());
            let error = client
                .head_bucket()
                .bucket("test-bucket")
                .send()
                .await
                .unwrap_err();
            if endpoint.is_none() {
                assert!(
                    format!("{error:?}").contains("Partition does not support FIPS"),
                    "{error:?}"
                );
            }
            assert!(http.requests.lock().unwrap().is_empty());
        }
    }
}
