use crate::aws::ClientBuilder;

pub(crate) struct SqsClientBuilder;

impl ClientBuilder for SqsClientBuilder {
    type Client = aws_sdk_sqs::client::Client;

    fn build(&self, config: &aws_types::SdkConfig) -> Self::Client {
        // The SDK rejects FIPS resolution with a custom endpoint.
        let use_fips = config.endpoint_url().is_none() && config.use_fips().unwrap_or(false);
        let mut builder = aws_sdk_sqs::config::Builder::from(config);
        builder.set_use_fips(Some(use_fips));
        aws_sdk_sqs::client::Client::from_conf(builder.build())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::aws::test_util::CaptureClient;

    #[tokio::test]
    async fn sqs_endpoint_override_takes_precedence_over_fips() {
        for use_fips in [None, Some(false), Some(true)] {
            for endpoint in [
                "https://sqs.ca-central-1.amazonaws.com",
                "https://vpce-sqs.example.invalid",
                "http://localhost:4566",
            ] {
                let http = CaptureClient::new("{}");
                let config = http
                    .config("ca-central-1", use_fips)
                    .into_builder()
                    .endpoint_url(endpoint)
                    .build();
                SqsClientBuilder
                    .build(&config)
                    .receive_message()
                    .queue_url("https://sqs.ca-central-1.amazonaws.com/123456789012/test")
                    .send()
                    .await
                    .unwrap();
                let requests = http.requests.lock().unwrap();
                assert_eq!(requests.len(), 1);
                assert_eq!(requests[0].uri(), format!("{endpoint}/"));
                assert!(
                    requests[0]
                        .headers()
                        .get("authorization")
                        .unwrap()
                        .contains("/ca-central-1/sqs/aws4_request")
                );
            }
        }
    }

    #[tokio::test]
    async fn sqs_without_override_uses_sdk_fips_setting() {
        for region in ["us-east-1", "us-west-2", "us-gov-west-1"] {
            for use_fips in [None, Some(false), Some(true)] {
                let http = CaptureClient::new("{}");
                SqsClientBuilder
                    .build(&http.config(region, use_fips))
                    .receive_message()
                    .queue_url(format!(
                        "https://sqs.{region}.amazonaws.com/123456789012/test"
                    ))
                    .send()
                    .await
                    .unwrap();
                let service = if use_fips == Some(true) && !region.starts_with("us-gov-") {
                    "sqs-fips"
                } else {
                    "sqs"
                };
                assert_eq!(
                    http.requests.lock().unwrap()[0].uri(),
                    format!("https://{service}.{region}.amazonaws.com/")
                );
            }
        }
    }

    #[tokio::test]
    async fn sqs_resolution_failure_does_not_retry_on_standard_endpoint() {
        for (region, endpoint) in [(None, None), (Some("us-east-1"), Some("not a URL"))] {
            let http = CaptureClient::new("{}");
            let mut config = http.config("us-east-1", Some(true)).into_builder();
            config.set_region(region.map(aws_types::region::Region::new));
            config.set_endpoint_url(endpoint.map(str::to_owned));
            let result = SqsClientBuilder
                .build(&config.build())
                .receive_message()
                .queue_url("https://sqs.us-east-1.amazonaws.com/123456789012/test")
                .send()
                .await;
            assert!(result.is_err());
            assert!(http.requests.lock().unwrap().is_empty());
        }
    }
}
