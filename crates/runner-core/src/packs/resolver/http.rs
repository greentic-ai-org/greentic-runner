use std::io::copy;

use anyhow::{bail, Context, Result};
use greentic_config_types::{NetworkConfig, TlsMode};
use reqwest::blocking::Client;
use std::time::Duration;
use tempfile::NamedTempFile;

use super::{FetchResponse, PackResolver};

pub struct HttpResolver {
    scheme: &'static str,
    client: Client,
}

impl HttpResolver {
    pub fn new(scheme: &'static str, network: Option<&NetworkConfig>) -> Result<Self> {
        let mut builder = Client::builder();
        if let Some(cfg) = network {
            if let Some(proxy) = &cfg.proxy_url {
                builder = builder.proxy(reqwest::Proxy::all(proxy)?);
            }
            if let Some(timeout) = cfg.connect_timeout_ms {
                builder = builder.connect_timeout(Duration::from_millis(timeout));
            }
            if let Some(timeout) = cfg.read_timeout_ms {
                builder = builder.timeout(Duration::from_millis(timeout));
            }
            if matches!(cfg.tls_mode, TlsMode::Disabled) {
                bail!("TLS certificate validation cannot be disabled");
            }
        }
        Ok(Self {
            scheme,
            client: builder.build()?,
        })
    }
}

impl PackResolver for HttpResolver {
    fn scheme(&self) -> &'static str {
        self.scheme
    }

    fn fetch(&self, locator: &str) -> Result<FetchResponse> {
        let mut response = self
            .client
            .get(locator)
            .send()
            .with_context(|| format!("failed to download {}", locator))?
            .error_for_status()
            .with_context(|| format!("download failed {}", locator))?;

        let mut temp = NamedTempFile::new().context("failed to allocate temp file for download")?;
        {
            let mut writer = temp.as_file_mut();
            copy(&mut response, &mut writer).context("failed to stream HTTP content")?;
        }
        Ok(FetchResponse::from_temp(temp.into_temp_path()))
    }
}
