// Standard library imports for core functionality
use std::{convert::TryInto, io::ErrorKind, sync::Arc};

// Azure SDK imports for blob and queue operations
use async_compression::tokio::bufread;
use azure_storage::{ConnectionString, StorageCredentials};
use azure_storage_blobs::prelude::*;
use azure_storage_queues::prelude::*;

// Utility and serialization imports
use bytes::Bytes;
use futures::{Stream, TryStreamExt, stream, stream::StreamExt};
use snafu::Snafu;
#[cfg(test)]
use std::num::NonZeroUsize;
use tokio_util::io::StreamReader;
use url::Url;

// Vector-specific imports
use vector_lib::codecs::NewlineDelimitedDecoderConfig;
use vector_lib::codecs::decoding::{
    DeserializerConfig, FramingConfig, NewlineDelimitedDecoderOptions,
};
use vector_lib::config::{LegacyKey, LogNamespace};
use vector_lib::configurable::configurable_component;
use vector_lib::lookup::owned_value_path;
use vrl::value::{Kind, kind::Collection};

// Local imports
use super::util::MultilineConfig;
use crate::codecs::DecodingConfig;
use crate::{
    config::{
        ProxyConfig, SourceAcknowledgementsConfig, SourceConfig, SourceContext, SourceOutput,
    },
    line_agg,
    serde::{bool_or_struct, default_decoding},
    tls::TlsConfig,
};

pub mod pem_certificate_credential;
pub mod queue;

use pem_certificate_credential::PemCertificateCredential;

/// Compression scheme for objects retrieved from Azure Blob Storage.
///
/// This enum defines the supported compression formats for blob content.
/// The source can automatically detect compression based on metadata or
/// use a specific compression format as configured.
#[configurable_component]
#[configurable(metadata(docs::advanced))]
#[derive(Clone, Copy, Debug, Derivative, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
#[derivative(Default)]
pub enum Compression {
    /// Automatically attempt to determine the compression scheme.
    ///
    /// The compression scheme is determined by checking:
    /// 1. The blob's `Content-Encoding` header
    /// 2. The blob's `Content-Type` header
    /// 3. The blob's file extension (e.g., .gz, .zst)
    ///
    /// Falls back to `None` if the compression scheme cannot be determined.
    #[derivative(Default)]
    Auto,

    /// No compression - process the blob content as-is.
    None,

    /// GZIP compression - decompress using GZIP algorithm.
    Gzip,

    /// ZSTD compression - decompress using Zstandard algorithm.
    Zstd,
}

/// Configuration for the Azure Blob Storage source.
///
/// This source collects logs from Azure Blob Storage by monitoring a queue
/// for blob creation notifications. When a new blob is created, it downloads
/// and processes the content according to the configured settings.
#[configurable_component(source("azure_blob", "Collect logs from Azure Blob Storage."))]
#[derive(Clone, Debug, Derivative)]
#[derivative(Default)]
#[serde(deny_unknown_fields)]
pub struct AzureBlobConfig {
    /// The Azure Storage connection string.
    ///
    /// This connection string must provide access to both Blob Storage and Queue Storage.
    /// It should include the account name, access key, and endpoints for both services.
    /// This method of authentication is NOT recommended, and should be used only for local testing.
    /// Please consider using Service Principal based SNI authentication instead.
    #[configurable(metadata(
        docs::examples = "DefaultEndpointsProtocol=https;AccountName=myaccount;AccountKey=mykey;EndpointSuffix=core.windows.net"
    ))]
    pub connection_string: Option<String>,

    /// Client certificate configuration for authentication.
    ///
    /// Use this for authentication with client certificates instead of connection strings.
    /// Cannot be used together with connection_string.
    #[configurable(derived)]
    pub client_certificate: Option<PemCertificateCredential>,

    /// The compression scheme used for decompressing blobs.
    ///
    /// This can be set to a specific compression type or set to `auto` to
    /// automatically detect the compression based on blob metadata.
    #[serde(default)]
    compression: Compression,

    /// Configuration options for Azure Queue Storage.
    ///
    /// These settings control how the source polls the queue for blob
    /// creation notifications and processes the messages.
    queue: queue::Config,

    /// Multiline aggregation configuration.
    ///
    /// When enabled, this allows combining multiple log lines into a single
    /// event based on patterns and timing. Useful for logs that span multiple
    /// lines but represent a single event.
    #[configurable(derived)]
    #[serde(default)]
    multiline: Option<MultilineConfig>,

    /// Configuration for message acknowledgements.
    ///
    /// Controls whether and how the source acknowledges successful processing
    /// of queue messages.
    #[configurable(derived)]
    #[serde(default, deserialize_with = "bool_or_struct")]
    acknowledgements: SourceAcknowledgementsConfig,

    /// TLS configuration options for secure communication.
    ///
    /// Used to configure TLS settings for connections to Azure services.
    #[configurable(derived)]
    #[serde(default)]
    tls_options: Option<TlsConfig>,

    /// The namespace to use for logs.
    ///
    /// This overrides the global log namespace setting. When enabled,
    /// logs are structured with Vector's new namespace format.
    #[configurable(metadata(docs::hidden))]
    #[serde(default)]
    log_namespace: Option<bool>,

    /// Configuration for framing the input data.
    ///
    /// Controls how the source splits the blob content into individual
    /// events. Defaults to newline-delimited framing.
    #[configurable(derived)]
    #[serde(default = "default_framing")]
    #[derivative(Default(value = "default_framing()"))]
    pub framing: FramingConfig,

    /// Configuration for decoding the input data.
    ///
    /// Controls how the source parses the framed data into structured
    /// events. Supports various formats like JSON, logfmt, etc.
    #[configurable(derived)]
    #[serde(default = "default_decoding")]
    #[derivative(Default(value = "default_decoding()"))]
    pub decoding: DeserializerConfig,

    /// Optional ingestion callback configuration.
    ///
    /// When present, the source fires HTTP callbacks to notify an upstream
    /// service after a custom direct-ingest file finishes processing.
    /// Only triggered for messages with `process_custom_message = true`.
    #[configurable(derived)]
    pub ingestion_callback: Option<super::ingestion_callback::IngestionCallbackConfig>,
}

/// Default framing configuration for backward compatibility.
///
/// Uses newline-delimited framing with no maximum line length.
const fn default_framing() -> FramingConfig {
    FramingConfig::NewlineDelimited(NewlineDelimitedDecoderConfig {
        newline_delimited: NewlineDelimitedDecoderOptions { max_length: None },
    })
}

impl_generate_config_from_default!(AzureBlobConfig);

/// Implementation of Vector's SourceConfig trait for Azure Blob Storage.
///
/// This implementation handles:
/// 1. Building the source with the provided configuration
/// 2. Defining the output schema for events
/// 3. Managing acknowledgements
#[async_trait::async_trait]
#[typetag::serde(name = "azure_blob")]
impl SourceConfig for AzureBlobConfig {
    /// Builds the Azure Blob Storage source with the provided configuration.
    ///
    /// # Arguments
    /// * `cx` - Source context containing output sender and shutdown signal
    ///
    /// # Returns
    /// A Result containing the configured source or an error if configuration is invalid
    async fn build(&self, cx: SourceContext) -> crate::Result<super::Source> {
        let log_namespace = cx.log_namespace(self.log_namespace);

        // Convert multiline config if specified
        let multiline_config: Option<line_agg::Config> = self
            .multiline
            .as_ref()
            .map(|config| config.try_into())
            .transpose()?;

        // Create and run the queue ingestor
        Ok(Box::pin(
            self.create_queue_ingestor(multiline_config, log_namespace, &cx.proxy)
                .await?
                .run(cx, self.acknowledgements, log_namespace),
        ))
    }

    /// Defines the output schema for events produced by this source.
    ///
    /// This includes:
    /// - Container, blob, and account metadata
    /// - Timestamp information
    /// - Vector-specific metadata
    /// - Dynamic metadata from blob properties
    ///
    /// # Arguments
    /// * `global_log_namespace` - The global log namespace setting
    ///
    /// # Returns
    /// A vector of source outputs with their schema definitions
    fn outputs(&self, global_log_namespace: LogNamespace) -> Vec<SourceOutput> {
        let log_namespace = global_log_namespace.merge(self.log_namespace);
        let mut schema_definition = self
            .decoding
            .schema_definition(log_namespace)
            // Add container metadata
            .with_source_metadata(
                Self::NAME,
                Some(LegacyKey::Overwrite(owned_value_path!("container"))),
                &owned_value_path!("container"),
                Kind::bytes(),
                None,
            )
            // Add blob metadata
            .with_source_metadata(
                Self::NAME,
                Some(LegacyKey::Overwrite(owned_value_path!("blob"))),
                &owned_value_path!("blob"),
                Kind::bytes(),
                None,
            )
            // Add account metadata
            .with_source_metadata(
                Self::NAME,
                Some(LegacyKey::Overwrite(owned_value_path!("account"))),
                &owned_value_path!("account"),
                Kind::bytes(),
                None,
            )
            // Add timestamp metadata
            .with_source_metadata(
                Self::NAME,
                None,
                &owned_value_path!("timestamp"),
                Kind::timestamp(),
                Some("timestamp"),
            )
            // Add standard Vector metadata
            .with_standard_vector_source_metadata()
            // Add dynamic metadata from blob properties
            .with_source_metadata(
                Self::NAME,
                None,
                &owned_value_path!("metadata"),
                Kind::object(Collection::empty().with_unknown(Kind::bytes())).or_undefined(),
                None,
            );

        // Handle legacy namespace
        if log_namespace == LogNamespace::Legacy {
            schema_definition = schema_definition.unknown_fields(Kind::bytes());
        }

        vec![SourceOutput::new_maybe_logs(
            self.decoding.output_type(),
            schema_definition,
        )]
    }

    /// Indicates whether this source supports acknowledgements.
    ///
    /// Azure Blob Storage source always supports acknowledgements.
    fn can_acknowledge(&self) -> bool {
        true
    }
}

impl AzureBlobConfig {
    /// Creates a new queue ingestor with the provided configuration.
    ///
    /// This function:
    /// 1. Parses the connection string or sets up client certificate authentication.
    /// 2. Creates Azure clients
    /// 3. Builds the decoder
    /// 4. Initializes the ingestor
    ///
    /// # Arguments
    /// * `multiline` - Optional configuration for multiline processing
    /// * `log_namespace` - The log namespace to use
    ///
    /// # Returns
    /// A Result containing the configured ingestor or an error if initialization fails
    async fn create_queue_ingestor(
        &self,
        multiline: Option<line_agg::Config>,
        log_namespace: LogNamespace,
        proxy: &ProxyConfig,
    ) -> crate::Result<queue::Ingestor> {
        let (blob_client, queue_client) = match (&self.connection_string, &self.client_certificate)
        {
            (Some(connection_string), None) => {
                self.create_clients_from_connection_string(connection_string)?
            }
            (None, Some(client_cert_config)) => {
                self.create_clients_from_certificate(client_cert_config)?
            }
            (None, None) => {
                return Err(CreateQueueIngestorError::MissingCredentials.into());
            }
            (Some(_), Some(_)) => {
                // We should error out here, as this is an example of incorrect configuration and
                // the user should only specify one of the two.
                return Err(CreateQueueIngestorError::ConflictingCredentials.into());
            }
        };

        // Build decoder
        let decoder =
            DecodingConfig::new(self.framing.clone(), self.decoding.clone(), log_namespace)
                .build()?;

        // Build callback client if configured
        let callback_client = self
            .ingestion_callback
            .as_ref()
            .map(|cb_config| {
                super::ingestion_callback::IngestionCallbackClient::new(cb_config, proxy)
            })
            .transpose()
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;

        // Create and return ingestor
        let ingestor = queue::Ingestor::new(
            blob_client,
            queue_client,
            self.queue.clone(),
            self.compression,
            multiline,
            decoder,
            callback_client,
        )
        .await?;

        Ok(ingestor)
    }

    /// Creates Azure clients using connection string authentication.
    ///
    /// # Arguments
    /// * `connection_string` - The Azure Storage connection string
    ///
    /// # Returns
    /// A tuple containing (BlobServiceClient, QueueServiceClient) or an error
    fn create_clients_from_connection_string(
        &self,
        connection_string: &str,
    ) -> Result<(BlobServiceClient, QueueServiceClient), CreateQueueIngestorError> {
        // Connection string authentication
        warn!(
            message = "Using Azure connection string for authentication. This method of authentication is NOT recommended, and should be used only for local testing. Please consider using Service Principal based SNI authentication instead."
        );

        // Parse connection string and create credentials
        let conn = ConnectionString::new(connection_string)
            .map_err(|_| CreateQueueIngestorError::CreationFailed)?;
        let creds = conn
            .storage_credentials()
            .map_err(|_| CreateQueueIngestorError::CreationFailed)?;

        // Extract account name from connection string or endpoint
        let account: String = if let Some(name) = conn.account_name {
            name.to_string()
        } else if let Some(endpoint) = conn.blob_endpoint {
            let url = Url::parse(endpoint).map_err(|_| CreateQueueIngestorError::CreationFailed)?;
            let host = url
                .host_str()
                .ok_or(CreateQueueIngestorError::CreationFailed)?;
            host.split('.')
                .next()
                .ok_or(CreateQueueIngestorError::CreationFailed)?
                .to_string()
        } else {
            return Err(CreateQueueIngestorError::CreationFailed);
        };

        // Create Azure clients with connection string
        let blob_client = BlobServiceClient::new(account.clone(), creds.clone());
        let queue_client = QueueServiceClient::new(account, creds);

        Ok((blob_client, queue_client))
    }

    /// Creates Azure clients using client certificate authentication.
    ///
    /// # Arguments
    /// * `client_cert_config` - The client certificate configuration
    ///
    /// # Returns
    /// A tuple containing (BlobServiceClient, QueueServiceClient) or an error
    fn create_clients_from_certificate(
        &self,
        client_cert_config: &PemCertificateCredential,
    ) -> Result<(BlobServiceClient, QueueServiceClient), CreateQueueIngestorError> {
        info!(message = "Using client certificate for Azure authentication.",);

        let client_credential = client_cert_config
            .create_client_certificate_credential()
            .map_err(|e| {
                error!(
                    message = "Failed to create client certificate credential",
                    error = ?e,
                );
                CreateQueueIngestorError::ClientCertificateCreationFailed {
                    source: Box::new(e),
                }
            })?;
        let storage_credentials = StorageCredentials::token_credential(Arc::new(client_credential));

        let account = client_cert_config.storage_account.clone();
        let blob_client = BlobServiceClient::new(account.clone(), storage_credentials.clone());
        let queue_client = QueueServiceClient::new(account, storage_credentials);

        Ok((blob_client, queue_client))
    }
}

/// Decodes a blob's content based on its compression settings.
///
/// This function:
/// 1. Reads the first chunk of data
/// 2. Determines the compression type if auto-detection is enabled
/// 3. Creates an appropriate decoder for the content
///
/// # Arguments
/// * `compression` - The compression scheme to use
/// * `blob_name` - Name of the blob being processed
/// * `content_encoding` - Content-Encoding header from blob metadata
/// * `content_type` - Content-Type header from blob metadata
/// * `body` - Stream of blob content chunks
///
/// # Returns
/// A boxed AsyncRead implementation that handles decompression
async fn blob_object_decoder(
    compression: Compression,
    blob_name: &str,
    content_encoding: Option<&str>,
    content_type: Option<&str>,
    mut body: Box<dyn Stream<Item = Result<Bytes, std::io::Error>> + Send + Unpin>,
) -> Box<dyn tokio::io::AsyncRead + Send + Unpin> {
    // Read first chunk to determine if content exists
    let first = if let Some(first) = body.next().await {
        first
    } else {
        return Box::new(tokio::io::empty());
    };

    // Create buffered reader for the content stream
    let r = tokio::io::BufReader::new(StreamReader::new(
        stream::iter(Some(first))
            .chain(body)
            .map_err(|e| std::io::Error::new(ErrorKind::Other, e)),
    ));

    // Determine compression type if auto-detection is enabled
    let compression = match compression {
        Auto => determine_compression(content_encoding, content_type, blob_name).unwrap_or(None),
        _ => compression,
    };

    // Create appropriate decoder based on compression type
    use Compression::*;
    match compression {
        Auto => unreachable!(), // Handled above
        None => Box::new(r),
        Gzip => Box::new({
            let mut decoder = bufread::GzipDecoder::new(r);
            decoder.multiple_members(true);
            decoder
        }),
        Zstd => Box::new({
            let mut decoder = bufread::ZstdDecoder::new(r);
            decoder.multiple_members(true);
            decoder
        }),
    }
}

/// Determines the compression type of a blob based on its metadata.
///
/// Checks the following in order:
/// 1. Content-Encoding header
/// 2. Content-Type header
/// 3. File extension
///
/// # Arguments
/// * `content_encoding` - Content-Encoding header value
/// * `content_type` - Content-Type header value
/// * `blob_name` - Name of the blob
///
/// # Returns
/// Some(Compression) if type can be determined, None otherwise
fn determine_compression(
    content_encoding: Option<&str>,
    content_type: Option<&str>,
    blob_name: &str,
) -> Option<Compression> {
    content_encoding
        .and_then(content_encoding_to_compression)
        .or_else(|| content_type.and_then(content_type_to_compression))
        .or_else(|| blob_name_to_compression(blob_name))
}

/// Converts a Content-Encoding header value to a Compression type.
///
/// # Arguments
/// * `content_encoding` - The Content-Encoding header value
///
/// # Returns
/// Some(Compression) if the encoding is supported, None otherwise
fn content_encoding_to_compression(content_encoding: &str) -> Option<Compression> {
    match content_encoding {
        "gzip" => Some(Compression::Gzip),
        "zstd" => Some(Compression::Zstd),
        _ => None,
    }
}

/// Converts a Content-Type header value to a Compression type.
///
/// # Arguments
/// * `content_type` - The Content-Type header value
///
/// # Returns
/// Some(Compression) if the type indicates compression, None otherwise
fn content_type_to_compression(content_type: &str) -> Option<Compression> {
    match content_type {
        "application/gzip" | "application/x-gzip" => Some(Compression::Gzip),
        "application/zstd" => Some(Compression::Zstd),
        _ => None,
    }
}

/// Determines compression type from a blob's file extension.
///
/// # Arguments
/// * `blob_name` - The name of the blob
///
/// # Returns
/// Some(Compression) if the extension indicates compression, None otherwise
fn blob_name_to_compression(blob_name: &str) -> Option<Compression> {
    let extension = std::path::Path::new(blob_name)
        .extension()
        .and_then(std::ffi::OsStr::to_str);

    use Compression::*;
    extension.and_then(|extension| match extension {
        "gz" => Some(Gzip),
        "zst" => Some(Zstd),
        _ => Option::None,
    })
}

/// Errors that can occur during queue ingestor creation.
#[derive(Debug, Snafu)]
enum CreateQueueIngestorError {
    /// Failed to create the queue ingestor due to invalid configuration or connection issues.
    #[snafu(display("Failed to create queue ingestor"))]
    CreationFailed,

    /// Failed to create client certificate credential.
    #[snafu(display("Failed to create client certificate credential: {}", source))]
    ClientCertificateCreationFailed {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    /// Neither connection string nor client certificate provided.
    #[snafu(display("Either connection_string or client_certificate must be provided"))]
    MissingCredentials,

    /// Both connection string and client certificate provided.
    #[snafu(display(
        "connection_string and client_certificate cannot be provided at the same time"
    ))]
    ConflictingCredentials,
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_module_works() {
        assert_eq!(Compression::Auto, Compression::Auto);
    }

    /// Tests the compression detection logic with various input combinations.
    ///
    /// Verifies that compression type is correctly determined from:
    /// - Content-Encoding header
    /// - Content-Type header
    /// - File extension
    #[test]
    fn determine_compression() {
        use super::Compression;

        let cases = vec![
            // Test Case 1: Gzip via Content-Encoding
            ("out.log", Some("gzip"), None, Some(Compression::Gzip)),
            // Test Case 2: Gzip via Content-Type
            (
                "out.log",
                None,
                Some("application/gzip"),
                Some(Compression::Gzip),
            ),
            // Test Case 3: Gzip via file extension
            ("out.log.gz", None, None, Some(Compression::Gzip)),
            // Test Case 4: Zstd via Content-Encoding
            ("data.log", Some("zstd"), None, Some(Compression::Zstd)),
            // Test Case 5: Zstd via Content-Type
            (
                "data.log",
                None,
                Some("application/zstd"),
                Some(Compression::Zstd),
            ),
            // Test Case 6: Zstd via file extension
            ("data.log.zst", None, None, Some(Compression::Zstd)),
            // Test Case 7: No compression
            ("out.txt", None, None, None),
            // Test Case 8: Unknown compression type
            ("out.log", Some("unknown"), None, None),
            // Test Case 9: Priority - Content-Encoding wins over Content-Type
            (
                "out.log",
                Some("gzip"),
                Some("application/zstd"),
                Some(Compression::Gzip),
            ),
            // Test Case 10: Priority - Content-Type wins over extension
            (
                "out.log.gz",
                None,
                Some("application/zstd"),
                Some(Compression::Zstd),
            ),
        ];

        for (i, case) in cases.iter().enumerate() {
            let (blob_name, content_encoding, content_type, expected) = case;
            assert_eq!(
                super::determine_compression(*content_encoding, *content_type, blob_name),
                *expected,
                "Test case {} failed: blob_name={:?} content_encoding={:?} content_type={:?}",
                i + 1,
                blob_name,
                content_encoding,
                content_type,
            );
        }
    }

    /// Tests valid configuration parsing
    /// This test only checks if the TOML -> AzureBlobConfig object is happening
    /// as expected, and does not deal with the correctness of the config itself.
    #[test]
    fn test_valid_config_parsing() {
        // Test 1: Test minimal valid config with connection string
        let config: Result<AzureBlobConfig, _> = toml::from_str(
            r#"
            connection_string = "DefaultEndpointsProtocol=https;AccountName=myaccount;AccountKey=mykey;EndpointSuffix=core.windows.net"
            [queue]
            queue_name = "my-queue"
            "#,
        );
        assert!(
            config.is_ok(),
            "Minimal config with connection_string should parse successfully"
        );
        let config = config.unwrap();
        assert_eq!(config.connection_string, Some("DefaultEndpointsProtocol=https;AccountName=myaccount;AccountKey=mykey;EndpointSuffix=core.windows.net".to_string()));
        assert!(config.client_certificate.is_none());
        assert_eq!(config.queue.queue_name, "my-queue");

        // Test 2: Test minimal valid config with client certificate
        let config: Result<AzureBlobConfig, _> = toml::from_str(
            r#"
            [client_certificate]
            tenant_id = "12345678-1234-1234-1234-123456789012"
            client_id = "87654321-4321-4321-4321-210987654321"
            storage_account = "mystorageaccount"
            client_certificate_path = "/path/to/certificate.pem"
            send_certificate_chain = true
            [queue]
            queue_name = "my-queue"
            "#,
        );
        assert!(
            config.is_ok(),
            "Minimal config with client_certificate should parse successfully"
        );
        let config = config.unwrap();
        assert!(config.connection_string.is_none());
        assert!(config.client_certificate.is_some());
        let client_cert = config.client_certificate.unwrap();
        assert_eq!(
            client_cert.tenant_id,
            "12345678-1234-1234-1234-123456789012"
        );
        assert_eq!(
            client_cert.client_id,
            "87654321-4321-4321-4321-210987654321"
        );
        assert_eq!(client_cert.storage_account, "mystorageaccount");
        assert_eq!(
            client_cert.client_certificate_path,
            "/path/to/certificate.pem"
        );
        assert!(client_cert.send_certificate_chain);
        assert_eq!(config.queue.queue_name, "my-queue");

        // Test 3: Test full config with connection string and all options
        let config: Result<AzureBlobConfig, _> = toml::from_str(
            r#"
            connection_string = "DefaultEndpointsProtocol=https;AccountName=myaccount;AccountKey=mykey;EndpointSuffix=core.windows.net"
            compression = "gzip"
            log_namespace = true
            
            [queue]
            queue_name = "my-queue"
            poll_secs = 30
            visibility_timeout_secs = 600
            delete_message = false
            delete_failed_message = false
            client_concurrency = 4
            max_number_of_messages = 5
            
            [multiline]
            start_pattern = "^\\[\\d{4}-\\d{2}-\\d{2}"
            mode = "halt_before"
            condition_pattern = "^\\[\\d{4}-\\d{2}-\\d{2}"
            timeout_ms = 1000
            
            [framing]
            method = "newline_delimited"
            
            [decoding]
            codec = "json"
            "#,
        );
        assert!(config.is_ok(), "Full config should parse successfully");
        let config = config.unwrap();
        assert_eq!(config.compression, Compression::Gzip);
        assert_eq!(config.queue.poll_secs, 30);
        assert_eq!(config.queue.visibility_timeout_secs, 600);
        assert!(!config.queue.delete_message);
        assert!(!config.queue.delete_failed_message);
        assert_eq!(
            config.queue.client_concurrency,
            Some(NonZeroUsize::new(4).unwrap())
        );
        assert_eq!(config.queue.max_number_of_messages, 5);
        assert!(config.multiline.is_some());
        assert_eq!(config.log_namespace, Some(true));
    }

    /// Tests invalid configuration scenarios
    /// Some of the configurations specified here may be invalid
    /// (like having no authentication methods) but they would only be checked for
    /// correctness at runtime (when create_queue_ingestor) is called. These cases
    /// are covered in other cases.
    #[test]
    fn test_invalid_config_parsing() -> Result<(), Box<dyn std::error::Error>> {
        struct TestCase {
            case_name: &'static str,
            config: &'static str,
            expect_error: bool,
            error_message: &'static str,
        }

        let test_cases = vec![
            TestCase {
                case_name: "Config without any auth method should parse but fail at runtime",
                config: r#"
                    [queue]
                    queue_name = "my-queue"
                "#,
                expect_error: false,
                error_message: "Config without any auth method should parse but fail at runtime",
            },
            TestCase {
                case_name: "Config with both auth methods should parse but fail at runtime",
                config: r#"
                    connection_string = "DefaultEndpointsProtocol=https;AccountName=myaccount;AccountKey=mykey;EndpointSuffix=core.windows.net"
                    [client_certificate]
                    tenant_id = "12345678-1234-1234-1234-123456789012"
                    client_id = "87654321-4321-4321-4321-210987654321"
                    storage_account = "mystorageaccount"
                    client_certificate_path = "/path/to/certificate.pem"
                    [queue]
                    queue_name = "my-queue"
                "#,
                expect_error: false,
                error_message: "Config with both auth methods should parse but fail at runtime",
            },
            TestCase {
                case_name: "Config without queue_name should fail",
                config: r#"
                    connection_string = "DefaultEndpointsProtocol=https;AccountName=myaccount;AccountKey=mykey;EndpointSuffix=core.windows.net"
                    [queue]
                "#,
                expect_error: true,
                error_message: "Config without queue_name should fail",
            },
            TestCase {
                case_name: "Config with invalid compression should fail",
                config: r#"
                    connection_string = "DefaultEndpointsProtocol=https;AccountName=myaccount;AccountKey=mykey;EndpointSuffix=core.windows.net"
                    compression = "invalid"
                    [queue]
                    queue_name = "my-queue"
                "#,
                expect_error: true,
                error_message: "Config with invalid compression should fail",
            },
            TestCase {
                case_name: "Config with unknown fields should fail due to deny_unknown_fields",
                config: r#"
                    connection_string = "DefaultEndpointsProtocol=https;AccountName=myaccount;AccountKey=mykey;EndpointSuffix=core.windows.net"
                    unknown_field = "value"
                    [queue]
                    queue_name = "my-queue"
                "#,
                expect_error: true,
                error_message: "Config with unknown fields should fail due to deny_unknown_fields",
            },
            TestCase {
                case_name: "Config without client_certificate_path should fail",
                config: r#"
                    [client_certificate]
                    tenant_id = "12345678-1234-1234-1234-123456789012"
                    client_id = "87654321-4321-4321-4321-210987654321"
                    storage_account = "mystorageaccount"
                    [queue]
                    queue_name = "my-queue"
                "#,
                expect_error: true,
                error_message: "Config without client_certificate_path should fail",
            },
            TestCase {
                case_name: "Config with extraneous fields in [client_certificate]",
                config: r#"
                    [client_certificate]
                    tenant_id = "12345678-1234-1234-1234-123456789012"
                    client_id = "87654321-4321-4321-4321-210987654321"
                    storage_account = "mystorageaccount"
                    client_certificate_path = "/path/to/certificate.pem"
                    unknown_field = "value"
                    [queue]
                    queue_name = "my-queue"
                "#,
                expect_error: true,
                error_message: "Config with extraneous fields in [client_certificate] should fail",
            },
        ];

        for test_case in test_cases.iter() {
            let result = toml::from_str::<AzureBlobConfig>(test_case.config);

            if test_case.expect_error {
                assert!(
                    result.is_err(),
                    "Test case {} failed: {}",
                    test_case.case_name,
                    test_case.error_message
                );
            } else {
                assert!(
                    result.is_ok(),
                    "Test case {} failed: {}",
                    test_case.case_name,
                    test_case.error_message
                );
            }
        }

        Ok(())
    }

    /// Tests connection string parsing
    #[test]
    fn test_connection_string_parsing() {
        // Valid connection string with all endpoints
        let conn_str = "DefaultEndpointsProtocol=https;AccountName=myaccount;AccountKey=mykey;BlobEndpoint=https://myaccount.blob.core.windows.net/;QueueEndpoint=https://myaccount.queue.core.windows.net/";
        let conn = ConnectionString::new(conn_str);
        assert!(conn.is_ok(), "Valid connection string should parse");
        let conn = conn.unwrap();
        assert_eq!(conn.account_name, Some("myaccount"));

        // Invalid connection string
        let conn_str = "InvalidConnectionString";
        let conn = ConnectionString::new(conn_str);
        assert!(
            conn.is_err(),
            "Invalid connection string should fail to parse"
        );

        // Connection string without account name
        let conn_str =
            "DefaultEndpointsProtocol=https;AccountKey=mykey;EndpointSuffix=core.windows.net";
        let conn = ConnectionString::new(conn_str);
        assert!(
            conn.is_ok(),
            "Connection string without account name should still parse"
        );
    }

    /// Tests creating queue ingestor with invalid configuration
    #[tokio::test]
    async fn test_create_queue_ingestor_errors() {
        struct TestCase {
            case_name: &'static str,
            config: AzureBlobConfig,
            expected_error: CreateQueueIngestorError,
            description: &'static str,
        }

        let test_cases = vec![
            TestCase {
                case_name: "Invalid connection string should fail",
                config: AzureBlobConfig {
                    connection_string: Some("InvalidConnectionString".to_string()),
                    client_certificate: None,
                    compression: Compression::Auto,
                    queue: queue::Config {
                        queue_name: "test-queue".to_string(),
                        ..Default::default()
                    },
                    ..Default::default()
                },
                expected_error: CreateQueueIngestorError::CreationFailed,
                description: "Invalid connection string should fail with CreationFailed",
            },
            TestCase {
                case_name: "Connection string without credentials should fail",
                config: AzureBlobConfig {
                    connection_string: Some(
                        "DefaultEndpointsProtocol=https;EndpointSuffix=core.windows.net".to_string(),
                    ),
                    client_certificate: None,
                    compression: Compression::Auto,
                    queue: queue::Config {
                        queue_name: "test-queue".to_string(),
                        ..Default::default()
                    },
                    ..Default::default()
                },
                expected_error: CreateQueueIngestorError::CreationFailed,
                description: "Connection string without credentials should fail with CreationFailed",
            },
            TestCase {
                case_name: "No auth method should fail",
                config: AzureBlobConfig {
                    connection_string: None,
                    client_certificate: None,
                    compression: Compression::Auto,
                    queue: queue::Config {
                        queue_name: "test-queue".to_string(),
                        ..Default::default()
                    },
                    ..Default::default()
                },
                expected_error: CreateQueueIngestorError::MissingCredentials,
                description: "Config without any auth method should fail with MissingCredentials",
            },
            TestCase {
                case_name: "Both auth methods should fail",
                config: AzureBlobConfig {
                    connection_string: Some("DefaultEndpointsProtocol=https;AccountName=myaccount;AccountKey=mykey;EndpointSuffix=core.windows.net".to_string()),
                    client_certificate: Some(PemCertificateCredential::new(
                        "12345678-1234-1234-1234-123456789012".to_string(),
                        "87654321-4321-4321-4321-210987654321".to_string(),
                        "mystorageaccount".to_string(),
                        "/path/to/certificate.pem".to_string(),
                        Some(true),
                    )),
                    compression: Compression::Auto,
                    queue: queue::Config {
                        queue_name: "test-queue".to_string(),
                        ..Default::default()
                    },
                    ..Default::default()
                },
                expected_error: CreateQueueIngestorError::ConflictingCredentials,
                description: "Config with both auth methods should fail with ConflictingCredentials",
            },
            TestCase {
                case_name: "Invalid PEM file path should fail",
                config: AzureBlobConfig {
                    connection_string: None,
                    client_certificate: Some(PemCertificateCredential::new(
                        "12345678-1234-1234-1234-123456789012".to_string(),
                        "87654321-4321-4321-4321-210987654321".to_string(),
                        "mystorageaccount".to_string(),
                        "/nonexistent/path/certificate.pem".to_string(),
                        Some(true),
                    )),
                    compression: Compression::Auto,
                    queue: queue::Config {
                        queue_name: "test-queue".to_string(),
                        ..Default::default()
                    },
                    ..Default::default()
                },
                expected_error: CreateQueueIngestorError::ClientCertificateCreationFailed { source: Box::new(std::io::Error::new(std::io::ErrorKind::NotFound, "dummy")) },
                description: "Client certificate with invalid PEM file path should fail with ClientCertificateCreationFailed",
            },
        ];

        for (i, test_case) in test_cases.iter().enumerate() {
            let result = test_case
                .config
                .create_queue_ingestor(None, LogNamespace::Legacy, &ProxyConfig::default())
                .await;

            assert!(
                result.is_err(),
                "Test case {} should fail: {}",
                i + 1,
                test_case.description
            );

            if let Err(error) = result {
                let downcast_result = error.downcast_ref::<CreateQueueIngestorError>();
                assert!(
                    downcast_result.is_some(),
                    "Test case {} should return CreateQueueIngestorError: {}",
                    i + 1,
                    test_case.case_name
                );

                let actual_error = downcast_result.unwrap();
                match (&test_case.expected_error, actual_error) {
                    (
                        CreateQueueIngestorError::CreationFailed,
                        CreateQueueIngestorError::CreationFailed,
                    ) => {}
                    (
                        CreateQueueIngestorError::MissingCredentials,
                        CreateQueueIngestorError::MissingCredentials,
                    ) => {}
                    (
                        CreateQueueIngestorError::ConflictingCredentials,
                        CreateQueueIngestorError::ConflictingCredentials,
                    ) => {}
                    (
                        CreateQueueIngestorError::ClientCertificateCreationFailed { .. },
                        CreateQueueIngestorError::ClientCertificateCreationFailed { .. },
                    ) => {}
                    _ => {
                        panic!(
                            "Test case {} failed: Expected error type does not match actual error type for case: {}",
                            i + 1,
                            test_case.case_name
                        );
                    }
                }
            }
        }
    }
}
