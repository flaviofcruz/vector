use std::sync::{Arc, Mutex};

use crate::tls::{MaybeTlsSettings, TlsEnableableConfig, tls_connector_builder};

/// Parsed credentials shared with the running config, never with a new generation.
#[derive(Clone, Debug, Default)]
pub(crate) struct TlsConfigSnapshot(Arc<Mutex<Option<MaybeTlsSettings>>>);

impl TlsConfigSnapshot {
    fn load(config: Option<&TlsEnableableConfig>, server: bool) -> crate::Result<MaybeTlsSettings> {
        let settings = MaybeTlsSettings::from_config(config, server)?;
        // Parsing PEM does not check that the certificate matches its private key.
        if server {
            if let MaybeTlsSettings::Tls(tls) = &settings {
                tls.acceptor()?;
            }
        } else {
            tls_connector_builder(&settings)?;
        }
        Ok(settings)
    }

    pub(crate) fn prepare(
        &mut self,
        config: Option<&TlsEnableableConfig>,
        server: bool,
    ) -> crate::Result<()> {
        // Keep the old config's snapshot available if another component rejects
        // this reload, or the publisher replaces a file after validation.
        *self = Self(Arc::new(Mutex::new(Some(Self::load(config, server)?))));
        Ok(())
    }

    pub(crate) fn get_or_load(
        &self,
        config: Option<&TlsEnableableConfig>,
        server: bool,
    ) -> crate::Result<MaybeTlsSettings> {
        let mut snapshot = self.0.lock().expect("TLS snapshot lock poisoned");
        if snapshot.is_none() {
            *snapshot = Some(Self::load(config, server)?);
        }
        Ok(snapshot.as_ref().expect("TLS snapshot initialized").clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tls::{
        TEST_PEM_CA_PATH, TEST_PEM_CLIENT_CRT_PATH, TEST_PEM_CLIENT_KEY_PATH, TEST_PEM_CRT_PATH,
        TEST_PEM_KEY_PATH,
    };

    #[test]
    fn invalid_replacement_retains_last_good_credentials_for_server_and_client() {
        for server in [true, false] {
            for invalid in [
                "missing-key",
                "malformed-key",
                "mismatched-key",
                "malformed-cert",
                "malformed-ca",
            ] {
                let directory = tempfile::tempdir().unwrap();
                let cert = directory.path().join("cert.pem");
                let key = directory.path().join("key.pem");
                let ca = directory.path().join("ca.pem");
                std::fs::copy(TEST_PEM_CRT_PATH, &cert).unwrap();
                std::fs::copy(TEST_PEM_KEY_PATH, &key).unwrap();
                std::fs::copy(TEST_PEM_CA_PATH, &ca).unwrap();
                let mut config = TlsEnableableConfig::test_config();
                config.options.crt_file = Some(cert.clone());
                config.options.key_file = Some(key.clone());
                config.options.ca_file = Some(ca.clone());
                let mut original = TlsConfigSnapshot::default();
                original.prepare(Some(&config), server).unwrap();
                let mut candidate = original.clone();
                match invalid {
                    "missing-key" => std::fs::remove_file(&key).unwrap(),
                    "malformed-key" => std::fs::write(&key, b"incomplete PEM").unwrap(),
                    "mismatched-key" => {
                        std::fs::copy(TEST_PEM_CLIENT_KEY_PATH, &key).unwrap();
                    }
                    "malformed-cert" => std::fs::write(&cert, b"incomplete PEM").unwrap(),
                    "malformed-ca" => std::fs::write(&ca, b"incomplete PEM").unwrap(),
                    _ => unreachable!(),
                }
                assert!(
                    candidate.prepare(Some(&config), server).is_err(),
                    "{invalid}, server={server}"
                );
                directory.close().unwrap();
                for snapshot in [original, candidate] {
                    let settings = snapshot.get_or_load(Some(&config), server).unwrap();
                    if server {
                        settings.tls().unwrap().acceptor().unwrap();
                    } else {
                        tls_connector_builder(&settings).unwrap();
                    }
                }
            }
        }
    }

    #[test]
    fn prepared_identity_does_not_reread_files_between_preflight_and_build() {
        for server in [true, false] {
            let directory = tempfile::tempdir().unwrap();
            let key = directory.path().join("key.pem");
            std::fs::copy(TEST_PEM_KEY_PATH, &key).unwrap();
            let mut config = TlsEnableableConfig::test_config();
            config.options.key_file = Some(key.clone());
            let mut snapshot = TlsConfigSnapshot::default();
            snapshot.prepare(Some(&config), server).unwrap();
            std::fs::copy(TEST_PEM_CLIENT_KEY_PATH, &key).unwrap();
            let settings = snapshot.get_or_load(Some(&config), server).unwrap();
            if server {
                settings.tls().unwrap().acceptor().unwrap();
            } else {
                tls_connector_builder(&settings).unwrap();
            }
        }
    }

    #[test]
    fn next_generation_does_not_overwrite_the_rollback_identity() {
        let directory = tempfile::tempdir().unwrap();
        let cert = directory.path().join("cert.pem");
        let key = directory.path().join("key.pem");
        std::fs::copy(TEST_PEM_CRT_PATH, &cert).unwrap();
        std::fs::copy(TEST_PEM_KEY_PATH, &key).unwrap();
        let mut config = TlsEnableableConfig::test_config();
        config.options.crt_file = Some(cert.clone());
        config.options.key_file = Some(key.clone());
        let mut original = TlsConfigSnapshot::default();
        original.prepare(Some(&config), true).unwrap();
        let mut candidate = original.clone();
        std::fs::copy(TEST_PEM_CLIENT_CRT_PATH, &cert).unwrap();
        std::fs::copy(TEST_PEM_CLIENT_KEY_PATH, &key).unwrap();
        candidate.prepare(Some(&config), true).unwrap();
        directory.close().unwrap();
        for (snapshot, expected) in [
            (original, TEST_PEM_CRT_PATH),
            (candidate, TEST_PEM_CLIENT_CRT_PATH),
        ] {
            let settings = snapshot.get_or_load(Some(&config), true).unwrap();
            let acceptor = settings.tls().unwrap().acceptor().unwrap();
            let expected =
                openssl::x509::X509::from_pem(&std::fs::read(expected).unwrap()).unwrap();
            assert_eq!(
                acceptor.context().certificate().unwrap().to_der().unwrap(),
                expected.to_der().unwrap()
            );
        }
    }

    #[test]
    fn disabled_tls_does_not_load_identity_files() {
        let mut config = TlsEnableableConfig::test_config();
        config.enabled = Some(false);
        config.options.crt_file = Some("/nonexistent/cert.pem".into());
        config.options.key_file = Some("/nonexistent/key.pem".into());
        for server in [true, false] {
            let mut snapshot = TlsConfigSnapshot::default();
            snapshot.prepare(Some(&config), server).unwrap();
            assert!(
                !snapshot
                    .get_or_load(Some(&config), server)
                    .unwrap()
                    .is_tls()
            );
        }
    }
}
