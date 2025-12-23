use std::task::{Context, Poll};

use futures::{TryFutureExt, future::BoxFuture};
use http::Uri;
use hyper::client::HttpConnector;
use hyper_openssl::HttpsConnector;
use hyper_proxy::ProxyConnector;
use kafka_producer_proxy::proto as proto_kpp;
use kafka_producer_proxy::proto::kafka_producer_proxy_service_client::KafkaProducerProxyServiceClient as KPPClient;
use tonic::{IntoRequest, body::BoxBody};
use tower::Service;
use vector_lib::request_metadata::{GroupedCountByteSize, MetaDescriptive, RequestMetadata};
use vector_lib::stream::DriverResponse;

use crate::{
    Error,
    event::{EventFinalizers, EventStatus, Finalizable},
    sinks::kafka_producer_proxy::KafkaProducerProxySinkError,
    sinks::kafka_producer_proxy::ResponseCodes,
};

#[derive(Clone, Debug)]
pub struct KPPService {
    pub client: KPPClient<HyperSvc>,
}

#[derive(Clone, Default)]
pub struct KPPRequest {
    pub finalizers: EventFinalizers,
    pub metadata: RequestMetadata,
    pub request: proto_kpp::KafkaMessages,
}

impl Finalizable for KPPRequest {
    fn take_finalizers(&mut self) -> EventFinalizers {
        self.finalizers.take_finalizers()
    }
}

impl MetaDescriptive for KPPRequest {
    fn get_metadata(&self) -> &RequestMetadata {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut RequestMetadata {
        &mut self.metadata
    }
}

#[derive(Debug)]
pub struct KPPResponse {
    pub event_byte_size: GroupedCountByteSize,
    pub all_succeeded: bool,
    pub results: Vec<ResponseCodes>,
}

impl DriverResponse for KPPResponse {
    fn event_status(&self) -> EventStatus {
        if self.all_succeeded {
            return EventStatus::Delivered;
        }

        EventStatus::Rejected
    }

    fn events_sent(&self) -> &GroupedCountByteSize {
        &self.event_byte_size
    }
}

impl KPPService {
    pub fn new(
        hyper_client: hyper::Client<ProxyConnector<HttpsConnector<HttpConnector>>, BoxBody>,
        uri: Uri,
    ) -> Self {
        let proto_client = KPPClient::new(HyperSvc {
            uri,
            client: hyper_client,
        });

        Self {
            client: proto_client,
        }
    }
}

impl Service<KPPRequest> for KPPService {
    type Response = KPPResponse;
    type Error = Error;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        // The readiness check is done through the `produce_messages()` call happening inside `call()`.
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, mut batched_request: KPPRequest) -> Self::Future {
        let mut service = self.clone();
        let metadata = std::mem::take(batched_request.metadata_mut());
        let events_byte_size = metadata.into_events_estimated_json_encoded_byte_size();

        let future = async move {
            // Convert the request into a tonic request and make gRPC call
            service
                .client
                .produce_messages(batched_request.request.into_request())
                .map_ok(|response| {
                    // Handling if every response from the server is true
                    let inner_response = response.into_inner();
                    let mut results: Vec<_> = Vec::new();
                    let mut errors: Vec<String> = Vec::new();

                    for (idx, msg) in inner_response.responses.into_iter().enumerate() {
                        if let Some(err_code) = msg.error_code {
                            let result = ResponseCodes::try_from(err_code)
                                .unwrap_or(ResponseCodes::Unknown(err_code));

                            if matches!(result, ResponseCodes::Unknown(_)) {
                                warn!(
                                    "Encountered unknown error code {} with error message: {}",
                                    err_code,
                                    msg.error_msg.unwrap_or_default()
                                );
                            } else {
                                errors.push(format!(
                                    "Index: {}, Response Code: {:?}, Response Msg: {}",
                                    idx,
                                    result,
                                    msg.error_msg.unwrap_or_default()
                                ));
                                results.push(result);
                            }
                        }
                    }

                    let success = inner_response.all_succeed.unwrap_or(false);

                    if !success {
                        warn!("{:?}", errors);
                    }

                    KPPResponse {
                        event_byte_size: events_byte_size,
                        all_succeeded: success,
                        results,
                    }
                })
                .map_err(|source| KafkaProducerProxySinkError::Request { source }.into())
                .await
        };
        Box::pin(future)
    }
}

#[derive(Clone, Debug)]
pub struct HyperSvc {
    uri: Uri,
    client: hyper::Client<ProxyConnector<HttpsConnector<HttpConnector>>, BoxBody>,
}

impl Service<hyper::Request<BoxBody>> for HyperSvc {
    type Response = hyper::Response<hyper::Body>;
    type Error = hyper::Error;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    // Emission of an internal event in case of errors is handled upstream by the caller.
    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    // Emission of internal events for errors and dropped events is handled upstream by the caller.
    fn call(&mut self, mut req: hyper::Request<BoxBody>) -> Self::Future {
        let uri = Uri::builder()
            .scheme(self.uri.scheme().unwrap().clone())
            .authority(self.uri.authority().unwrap().clone())
            .path_and_query(req.uri().path_and_query().unwrap().clone())
            .build()
            .unwrap();

        *req.uri_mut() = uri;

        Box::pin(self.client.request(req))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use hyper::Client;
    use kafka_producer_proxy::proto::kafka_producer_proxy_service_server::KafkaProducerProxyService;
    use kafka_producer_proxy::proto::{KafkaMessage, KafkaMessages, kafka_message, kafka_messages};
    use std::net::SocketAddr;
    use std::str::FromStr;
    use tokio::net::TcpListener;
    use tonic::transport::Server;
    use tonic::{Request, Response, Status};

    #[derive(Clone)]
    pub struct MockKafkaService {
        pub rpc_error: bool,       // Error occurs on the protocol level
        pub total_failure: bool,   // All messages have an error
        pub partial_failure: bool, // Partial Failure Occurs (one of the messages have an error)
    }

    #[async_trait]
    impl KafkaProducerProxyService for MockKafkaService {
        async fn produce_message(
            &self,
            _request: Request<KafkaMessage>,
        ) -> Result<Response<kafka_message::Response>, Status> {
            if self.rpc_error {
                return Err(Status::internal("Response RPC Mock Error"));
            }
            Ok(Response::new(kafka_message::Response {
                error_code: Some(if self.total_failure || self.partial_failure {
                    3
                } else {
                    0
                }),
                error_msg: Some(if self.total_failure || self.partial_failure {
                    "error".to_string()
                } else {
                    "success".to_string()
                }),
            }))
        }

        async fn produce_messages(
            &self,
            request: Request<KafkaMessages>,
        ) -> Result<Response<kafka_messages::Response>, Status> {
            if self.rpc_error {
                return Err(Status::internal("mock error"));
            }

            let messages = request.into_inner().messages;
            let responses = messages
                .iter()
                .enumerate()
                .map(|(idx, _msg)| {
                    let (error_code, error_msg) = match (self.total_failure, self.partial_failure) {
                        (true, _) => (3, "total failure".to_string()),
                        (false, true) => {
                            if idx == 0 {
                                (0, "success".to_string())
                            } else {
                                (3, "partial failure".to_string())
                            }
                        }
                        _ => (0, "success".to_string()),
                    };
                    kafka_message::Response {
                        error_code: Some(error_code),
                        error_msg: Some(error_msg),
                    }
                })
                .collect();

            Ok(Response::new(kafka_messages::Response {
                responses,
                all_succeed: Some(!self.total_failure && !self.partial_failure),
            }))
        }
    }

    /// Function to get an available port.
    /// This function is required since multiple tests are running
    async fn get_available_port() -> u16 {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        addr.port()
    }

    async fn start_mock_server(mock_service: MockKafkaService) -> SocketAddr {
        let port = get_available_port().await;
        let addr: SocketAddr = format!("127.0.0.1:{}", port).parse().unwrap();

        let server = Server::builder()
            .add_service(
                kafka_producer_proxy::proto::kafka_producer_proxy_service_server::KafkaProducerProxyServiceServer::new(mock_service)
            )
            .serve(addr);

        tokio::spawn(async move {
            let _ = server.await;
        });

        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        addr
    }

    pub fn create_svc() -> hyper::Client<ProxyConnector<HttpsConnector<HttpConnector>>, BoxBody> {
        let http_connector = HttpConnector::new();
        let ssl = openssl::ssl::SslConnector::builder(openssl::ssl::SslMethod::tls()).unwrap();
        let https = HttpsConnector::with_connector(http_connector, ssl).unwrap();
        let proxy = ProxyConnector::new(https).unwrap();
        Client::builder().http2_only(true).build(proxy)
    }

    pub fn create_service(addr: SocketAddr) -> KPPService {
        let client = create_svc();
        let uri = Uri::from_str(&format!("http://{}", addr)).unwrap();
        KPPService {
            client: KPPClient::new(HyperSvc {
                uri: uri.clone(),
                client,
            }),
        }
    }

    pub fn create_test_kpp_request() -> KPPRequest {
        KPPRequest {
            finalizers: EventFinalizers::default(),
            metadata: RequestMetadata::default(),
            request: proto_kpp::KafkaMessages {
                messages: vec![
                    proto_kpp::KafkaMessage {
                        topic_name: Some("test-topic".to_string()),
                        key: Some("key1".to_string()),
                        data: Some("test-data1".as_bytes().to_vec()),
                        log_entry: Some("test-log-entry1".as_bytes().to_vec()),
                    },
                    proto_kpp::KafkaMessage {
                        topic_name: Some("test-topic".to_string()),
                        key: Some("key2".to_string()),
                        data: Some("test-data2".as_bytes().to_vec()),
                        log_entry: Some("test-log-entry2".as_bytes().to_vec()),
                    },
                ],
            },
        }
    }

    #[tokio::test]
    async fn test_service_call_success() {
        let mock_service = MockKafkaService {
            rpc_error: false,
            total_failure: false,
            partial_failure: false,
        };
        let addr = start_mock_server(mock_service).await;

        let mut service = create_service(addr);
        let req = create_test_kpp_request();
        let res = service.call(req).await;

        assert!(res.is_ok());

        let kpp_response = res.unwrap();

        assert!(kpp_response.all_succeeded);

        for status in kpp_response.results.iter() {
            assert_eq!(*status, ResponseCodes::Success);
        }

        assert!(matches!(
            kpp_response.event_status(),
            EventStatus::Delivered
        ));
    }

    #[tokio::test]
    async fn test_service_call_error() {
        let mock_service = MockKafkaService {
            rpc_error: true,
            total_failure: false,
            partial_failure: false,
        };
        let addr = start_mock_server(mock_service).await;

        let mut service = create_service(addr);
        let req = create_test_kpp_request();
        let res = service.call(req).await;

        assert!(res.is_err());

        let err = res.unwrap_err();
        let kpp_err = err.downcast_ref::<KafkaProducerProxySinkError>().unwrap();
        match kpp_err {
            KafkaProducerProxySinkError::Request { source } => {
                assert_eq!(source.code(), tonic::Code::Internal);
                assert_eq!(kpp_err.to_string(), format!("Request failed: {}", source));
            }
        }
    }

    #[tokio::test]
    async fn test_service_call_partial_error() {
        let mock_service = MockKafkaService {
            rpc_error: false,
            total_failure: false,
            partial_failure: true,
        };
        let addr = start_mock_server(mock_service).await;

        let mut service = create_service(addr);
        let req = create_test_kpp_request();
        let res = service.call(req).await;

        assert!(res.is_ok()); // RPC Call should succeed

        let kpp_response = res.unwrap();

        assert!(!kpp_response.all_succeeded);

        // Check to ensure that there is failure in error codes and that it is a partial error
        for (idx, status) in kpp_response.results.iter().enumerate() {
            if idx == 0 {
                assert_eq!(*status, ResponseCodes::Success);
            } else {
                assert_eq!(*status, ResponseCodes::KafkaErrorErrorCode);
            }
        }

        assert!(matches!(kpp_response.event_status(), EventStatus::Rejected));
    }

    #[tokio::test]
    async fn test_service_call_total_error() {
        let mock_service = MockKafkaService {
            rpc_error: false,
            total_failure: true,
            partial_failure: false,
        };
        let addr = start_mock_server(mock_service).await;

        let mut service = create_service(addr);
        let req = create_test_kpp_request();
        let res = service.call(req).await;

        assert!(res.is_ok()); // RPC Call should succeed

        let kpp_response = res.unwrap();

        assert!(!kpp_response.all_succeeded);

        // Check to ensure that there is failure in error codes
        for status in kpp_response.results.iter() {
            assert_eq!(*status, ResponseCodes::KafkaErrorErrorCode);
        }

        assert!(matches!(kpp_response.event_status(), EventStatus::Rejected));
    }
}
