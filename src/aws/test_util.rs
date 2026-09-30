use std::sync::{Arc, Mutex};

use aws_smithy_runtime_api::client::{
    http::{
        HttpClient, HttpConnector, HttpConnectorFuture, HttpConnectorSettings, SharedHttpConnector,
    },
    orchestrator::{HttpRequest, HttpResponse},
    runtime_components::RuntimeComponents,
};
use aws_smithy_types::body::SdkBody;

pub(crate) const TEST_SECRET: &str = "vector-endpoint-test-secret-0000000000000";

// Record SDK requests without contacting AWS or loading real credentials.
#[derive(Clone, Debug)]
pub(crate) struct CaptureClient {
    pub(crate) requests: Arc<Mutex<Vec<HttpRequest>>>,
    response: &'static str,
}

impl CaptureClient {
    pub(crate) fn new(response: &'static str) -> Self {
        Self {
            requests: Arc::default(),
            response,
        }
    }

    pub(crate) fn config(&self, region: &str, use_fips: Option<bool>) -> aws_types::SdkConfig {
        use aws_credential_types::{Credentials, provider::SharedCredentialsProvider};
        use aws_smithy_async::time::SystemTimeSource;
        use aws_smithy_types::retry::RetryConfig;
        use aws_types::{SdkConfig, region::Region};

        let mut builder = SdkConfig::builder()
            .http_client(self.clone())
            .credentials_provider(SharedCredentialsProvider::new(Credentials::from_keys(
                "test-access",
                TEST_SECRET,
                None,
            )))
            .region(Region::new(region.to_owned()))
            .retry_config(RetryConfig::disabled())
            .time_source(SystemTimeSource::new())
            .behavior_version(aws_config::BehaviorVersion::latest());
        builder.set_use_fips(use_fips);
        builder.build()
    }
}

impl HttpConnector for CaptureClient {
    fn call(&self, request: HttpRequest) -> HttpConnectorFuture {
        self.requests.lock().unwrap().push(request);
        let response: HttpResponse = http::Response::builder()
            .status(200)
            .body(SdkBody::from(self.response))
            .unwrap()
            .try_into()
            .unwrap();
        HttpConnectorFuture::ready(Ok(response))
    }
}

impl HttpClient for CaptureClient {
    fn http_connector(
        &self,
        _: &HttpConnectorSettings,
        _: &RuntimeComponents,
    ) -> SharedHttpConnector {
        SharedHttpConnector::new(self.clone())
    }
}
