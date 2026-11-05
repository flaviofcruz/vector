# Databricks proto descriptor fixtures

`principal_context_service_proto_descriptor.pb` is the `FileDescriptorSet` that universe bakes
into the vector image at `/etc/proto/principal-context/service_proto_descriptor.pb`. The
`databricks_auth` token manager tests load it so they run against Login's real
`PrincipalContextService` schema.

Regenerate from a universe checkout:

```sh
bazel build //common/principal-context/api/proto:service_proto_descriptor
cp bazel-bin/common/principal-context/api/proto/service_proto_descriptor.pb \
  <vector>/tests/data/databricks/principal_context_service_proto_descriptor.pb
```

Generated from universe commit `1db9ffbf40b4afa774a686fb4f8237fa309313b2`.
