# Vector patch

This is aws-config 1.8.13, the version already pinned in Vector's Cargo.lock,
from the Databricks crates proxy (Apache-2.0). Upstream archive SHA-256:
`c456581cb3c77fafcc8c67204a70680d40b61112d6da78c77bd31d945b65f1b5`.

The only source change makes `ProviderConfig::with_use_fips` public. Vector
needs to propagate the SDK's environment/profile FIPS setting to credential
providers while retaining its OpenSSL HTTP connector and custom IMDS client.
`ConfigLoader` propagates FIPS internally but cannot accept that IMDS client.

Remove this patch when the SDK provides a public configuration path preserving
both settings. No dependency versions or credential-provider behavior change
inside this crate.
