# Zerobus authentication

## Encoding workers

Set `encoding_concurrency = 2` on the sink to run Arrow encoding on up to two
blocking workers shared by that sink's requests. Omit it to keep inline encoding.
This lets concurrent requests encode in parallel even when the sink driver polls
them on one task. The request concurrency limit still bounds outstanding requests;
the encoding limit only bounds CPU jobs, including jobs whose callers timed out.

`zerobus_encoding_seconds` measures encoding time and
`zerobus_encoding_wait_seconds` measures permit waiting and blocking-pool scheduling delay.
Increasing encoding workers does not increase the request limit or the number of
Zerobus streams. Size those limits and the memory budget together: concurrent raw
events, Arrow batches, and SDK replay data all consume memory. Delivery still waits
for a Zerobus acknowledgment.

## Authentication settings

With `auth.strategy = "login_service"`, configure the shared TLS identity and trust
under `auth.tls`. Login, Unity Catalog, and Zerobus ingestion all present its
required `crt_file` and `key_file`. The optional `ca_file` supplies the server trust
bundle; otherwise the system trust store is used.

`auth.tls.verify_hostname` defaults to `true` and applies only to Login and Unity
Catalog. It does not apply to Zerobus ingestion because the SDK does not support
disabling hostname verification. For
internal E/W DBNS endpoints whose certificate SANs do not match their DNS names, set it
to `false` and supply the shard-services trust chain through `auth.tls.ca_file`. This
still verifies the certificate chain, validity period, and TLS handshake signatures.

When the ingestion endpoint is reached through a proxy whose certificate has a different
DNS name, set the optional top-level `ingestion_domain_name`, next to
`ingestion_endpoint`, to that certificate name, for
example `regional-dp-apiproxy.service` for the staging regional DP API proxy. The SDK
uses it for ingestion SNI and certificate-name verification, while the configured
`ingestion_endpoint` still selects the destination and HTTP authority. Ingestion
always verifies the certificate chain and server name, independently of
`auth.tls.verify_hostname`. If the override is absent, the SDK verifies the ingestion
endpoint's hostname. Both auth strategies accept the override. It does not change
`login_server_name` or UC's name.

Login connection settings remain under `auth.login_service`. Its `login_server_name`
sets SNI; when hostname verification is enabled, Login checks the hostname in
`login_endpoint`. Set the UC destination at the sink's top-level
`unity_catalog_endpoint`; internal UC uses its URL hostname for both SNI and any
enabled hostname verification. Only the UC descriptor path belongs under
`auth.unity_catalog.proto_descriptor_path`. Move the former Login fields
`tls_crt_file`, `tls_key_file`, and `tls_ca_file` to `auth.tls` as `crt_file`, `key_file`,
and `ca_file`; replace both per-service `verify_hostname` settings with
`auth.tls.verify_hostname`. The old locations are rejected rather than silently ignored.
The OAuth strategy rejects `auth.tls`; its optional ingestion name override uses
the system trust store.

A TLS-only check from a `vector-aggregator-metrics` pod in
`staging-aws-us-west-2-0` confirmed this for the Login, managed-catalog and Shinkansen CP
E/W endpoints: each chain was issued by `Staging Shard Services UWI CA`, negotiated `h2`,
and passed validation with the pod's mounted trust bundle when hostname checks were
disabled. Each failed hostname validation, and the system trust store did not trust the
chain. The VAM bundle at `/databricks-infra/dbts2/creds.cert.pem` contained the required
trust chain; deployments must provide the corresponding bundle for their environment.

The pinned Zerobus SDK does not yet support binary principal-context metadata, so config
validation still rejects `login_service` at startup until the SDK fix is pinned.
SDK 3.x also replaced its custom TLS endpoint hook with a settings API that cannot
disable hostname verification. Direct ingestion listeners with URI-only certificates
cannot be used with a DNS-name override; use an endpoint whose certificate has a
matching DNS name, such as the regional DP API proxy.

Internal UC reads send the unsigned context in `s2s-principal-context-bin`. When Login
also returns signed PCv2, the same request sends `s2s-principal-context-v2-sig-bin` using
the `pctx_v2_` wire envelope. Both come from the same cached bootstrap. Older Login
regions that do not dual-send PCv2 continue using the unsigned context.

Login bootstrap uses two request kinds: a regular service-principal request for UC
(unsigned context plus optional signed PCv2), and a legacy `PCTX_DP_AUTH_TOKEN` request
for Zerobus. Only the latter sends `signedPrincipalContextConfig`, including the required
metastore ID and allowed SSP resource path. That path defaults to
`workspaces/<workspace_id>/zerobusDirectWriteApi` and can be overridden with
`auth.zerobus_ssp_resource_path`; it does not customize the regular PCv2 response.

Both auth strategies require the top-level `unity_catalog_endpoint`. For OAuth, use
the public workspace URL for token requests and UC REST schema reads. For Login, use
the internal managed-catalog HTTPS gRPC URL; its hostname supplies the TLS server name
and its port supplies the connection port. Login uses the descriptor under
`auth.unity_catalog` and the shared `auth.tls` settings for UC requests. Fields from
the other auth strategy are rejected during config loading.
