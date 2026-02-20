# Kafka Producer Proxy

This service defines the protobuf schema for interacting with Databricks' internal Kafka infrastructure.

## Purpose
Databricks' internal Kafka deployment currently exposes a **gRPC-based ingestion endpoint**. However, Vector's [Kafka sink](https://vector.dev/docs/reference/configuration/sinks/kafka/) uses a Kafka-native protocol and does not support the use of a gRPC interface. Therefore, this protobuf schema makes communication between Vector and our internal GRPC-based Kafka endpoint possible, enabling [Woodchuck V2's](go/woodchuck) log delivery architecture.

## Modifications
The protobuf definition has been copied from an [existing protobuf definition](https://sourcegraph.prod.databricks-corp.com/databricks-eng/universe/-/blob/storageplatform/kafka-producer-proxy/api/proto/service.proto). Minor modifications have been made to this definition.

Some of these changes include: 

1. **Removal of defined endpoints in the `produceMessage` and `produceMessages` RPC schemas**: This is to allow for further design considerations before finalizing endpoints.

2. **Removal of the sample test results and messages**: These have been removed until a better/alternative testing strategy has been investigated.

3. **Removal of language specific imports and options**: Options and imports have been removed as we are now using Rust. We have newly added Rust files for configuration.

**Other Additions:** To make the interface compatible with Vector's existing codebase, build-related Rust files were introduced.