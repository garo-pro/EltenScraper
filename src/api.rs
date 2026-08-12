//! Low-level Elten API client: response envelope, shared rate limiting,
//! retry/backoff, and streamed media downloads.
//!
//! Wire format (as used by the Elten 3 desktop client):
//!   * base `https://api.elten.link`, REST under `/api/v1`
//!   * every response is `{"success": <truthy>, "data": {...}}`, or
//!     `{"success": false, "error": {"code": ..., "message": ...}}`
//!   * an authenticated session travels as `name` + `token` query parameters
//!
//! Only GETs are issued while scraping, so a run never mutates server state
//! (marking threads read is a separate explicit PATCH the scraper never sends).

use anyhow::{anyhow, bail, Context, Result};
use futures::StreamExt;
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::io::AsyncWriteExt;
use tokio::sync::Mutex;

use crate::config::Config;
use crate::model::truthy;

/// A shared token bucket that paces every request the process makes, no matter
/// which worker issues it.
#[derive(Debug)]
pub struct RateLimiter {
    interval: Option<Duration>,
    next_slot: Mutex<Instant>,
}

impl RateLimiter {
    pub fn new(requests_per_second: f64) -> Self {
        let interval = if requests_per_second > 0.0 {
            Some(Duration::from_secs_f64(1.0 / requests_per_second))
        } else {
            None
        };
        Self { interval, next_slot: Mutex::new(Instant::now()) }
    }

    pub async fn acquire(&self) {
        let Some(interval) = self.interval else { return };
        let wait = {
            let mut slot = self.next_slot.lock().await;
            let now = Instant::now();
            let scheduled = (*slot).max(now);
            *slot = scheduled + interval;
            scheduled.saturating_duration_since(now)
        };
        if !wait.is_zero() {
            tokio::time::sleep(wait).await;
        }
    }
}

#[derive(Debug, Clone)]
pub struct Session {
    pub name: String,
    pub token: String,
}

/// An error the server reported in the envelope, as opposed to a transport
/// failure. `code` is what distinguishes e.g. a 2FA challenge.
#[derive(Debug)]
pub struct ApiError {
    pub code: String,
    pub message: String,
}

impl std::fmt::Display for ApiError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} ({})", self.message, self.code)
    }
}

impl std::error::Error for ApiError {}

pub struct Client {
    http: reqwest::Client,
    base: String,
    limiter: Arc<RateLimiter>,
    session: Mutex<Option<Session>>,
    max_retries: u32,
    retry_base_ms: u64,
    retry_max_ms: u64,
}

impl Client {
    pub fn new(cfg: &Config) -> Result<Self> {
        Self::with_rate_limit(cfg, cfg.scrape.requests_per_second)
    }

    pub fn with_rate_limit(cfg: &Config, requests_per_second: f64) -> Result<Self> {
        let http = reqwest::Client::builder()
            .user_agent(cfg.network.user_agent.clone())
            .timeout(Duration::from_secs(cfg.network.request_timeout_secs))
            .connect_timeout(Duration::from_secs(cfg.network.connect_timeout_secs))
            .build()
            .context("building HTTP client")?;
        Ok(Self {
            http,
            base: cfg.network.base_url.trim_end_matches('/').to_string(),
            limiter: Arc::new(RateLimiter::new(requests_per_second)),
            session: Mutex::new(None),
            max_retries: cfg.scrape.max_retries,
            retry_base_ms: cfg.scrape.retry_base_ms,
            retry_max_ms: cfg.scrape.retry_max_ms,
        })
    }

    pub async fn set_session(&self, session: Option<Session>) {
        *self.session.lock().await = session;
    }

    pub async fn has_session(&self) -> bool {
        self.session.lock().await.is_some()
    }

    fn url(&self, path: &str) -> String {
        if path.starts_with("http://") || path.starts_with("https://") {
            path.to_string()
        } else if path.starts_with('/') {
            format!("{}{}", self.base, path)
        } else {
            format!("{}/{}", self.base, path)
        }
    }

    async fn auth_params(&self) -> Vec<(String, String)> {
        match self.session.lock().await.as_ref() {
            Some(s) => vec![
                ("name".to_string(), s.name.clone()),
                ("token".to_string(), s.token.clone()),
            ],
            None => Vec::new(),
        }
    }

    /// GET a path and unwrap the envelope, returning the `data` object.
    pub async fn get_data(&self, path: &str, params: &[(String, String)]) -> Result<Value> {
        let value = self.request_json("GET", path, params, &[]).await?;
        unwrap_envelope(value, path)
    }

    /// POST form parameters and unwrap the envelope.
    pub async fn post_data(&self, path: &str, form: &[(String, String)]) -> Result<Value> {
        let value = self.request_json("POST", path, &[], form).await?;
        unwrap_envelope(value, path)
    }

    async fn request_json(
        &self,
        method: &str,
        path: &str,
        params: &[(String, String)],
        form: &[(String, String)],
    ) -> Result<Value> {
        let mut query = self.auth_params().await;
        query.extend_from_slice(params);
        let url = self.url(path);

        let mut attempt = 0u32;
        loop {
            self.limiter.acquire().await;
            let mut req = match method {
                "GET" => self.http.get(&url),
                "POST" => self.http.post(&url),
                other => bail!("unsupported method {other}"),
            };
            if !query.is_empty() {
                req = req.query(&query);
            }
            if !form.is_empty() {
                req = req.form(&form.to_vec());
            }

            let outcome = async {
                let resp = req.send().await?;
                let status = resp.status();
                let body = resp.text().await?;
                Ok::<_, reqwest::Error>((status, body))
            }
            .await;

            match outcome {
                Ok((status, body)) => {
                    if status.is_success() {
                        return serde_json::from_str::<Value>(&body).with_context(|| {
                            format!("invalid JSON from {}", redact(&url))
                        });
                    }
                    // 429/5xx are worth another go; 4xx are not.
                    let retryable =
                        status.as_u16() == 429 || status.is_server_error();
                    if !retryable || attempt >= self.max_retries {
                        bail!(
                            "HTTP {} from {} - {}",
                            status.as_u16(),
                            redact(&url),
                            body.chars().take(200).collect::<String>()
                        );
                    }
                }
                Err(err) => {
                    if attempt >= self.max_retries {
                        return Err(anyhow!(err))
                            .with_context(|| format!("requesting {}", redact(&url)));
                    }
                }
            }

            attempt += 1;
            tokio::time::sleep(self.backoff(attempt)).await;
        }
    }

    /// Exponential backoff with jitter, capped by `retry_max_ms`.
    fn backoff(&self, attempt: u32) -> Duration {
        let exp = self
            .retry_base_ms
            .saturating_mul(1u64 << attempt.min(16).saturating_sub(1).min(16));
        let capped = exp.min(self.retry_max_ms).max(1);
        let jitter = rand::random::<f64>() * 0.3 + 0.85;
        Duration::from_millis(((capped as f64) * jitter) as u64)
    }

    /// Stream a media file to `dest`, returning (bytes, sha256).
    /// `max_bytes` is enforced *while* streaming (0 disables it), so an
    /// unexpectedly huge file cannot fill the disk.
    pub async fn download(&self, url: &str, dest: &Path, max_bytes: u64) -> Result<(u64, String)> {
        let query = self.auth_params().await;
        let full = self.url(url);

        let mut attempt = 0u32;
        loop {
            self.limiter.acquire().await;
            let mut req = self.http.get(&full);
            if !query.is_empty() {
                req = req.query(&query);
            }

            match self.try_download(req, dest, max_bytes).await {
                Ok(result) => return Ok(result),
                Err(err) => {
                    if attempt >= self.max_retries {
                        tokio::fs::remove_file(dest).await.ok();
                        return Err(err).with_context(|| format!("downloading {}", redact(&full)));
                    }
                }
            }
            attempt += 1;
            tokio::time::sleep(self.backoff(attempt)).await;
        }
    }

    async fn try_download(
        &self,
        req: reqwest::RequestBuilder,
        dest: &Path,
        max_bytes: u64,
    ) -> Result<(u64, String)> {
        let resp = req.send().await?;
        if !resp.status().is_success() {
            bail!("HTTP {}", resp.status().as_u16());
        }
        if let Some(len) = resp.content_length() {
            if max_bytes > 0 && len > max_bytes {
                bail!("file is {len} bytes, over the configured size limit");
            }
        }
        if let Some(dir) = dest.parent() {
            tokio::fs::create_dir_all(dir).await.ok();
        }

        // Write to a temp file first so an interrupted run never leaves a
        // truncated file that `skip_existing` would later treat as complete.
        let tmp = dest.with_extension("part");
        let mut file = tokio::fs::File::create(&tmp)
            .await
            .with_context(|| format!("creating {}", tmp.display()))?;
        let mut hasher = Sha256::new();
        let mut total: u64 = 0;
        let mut stream = resp.bytes_stream();

        while let Some(chunk) = stream.next().await {
            let chunk = chunk?;
            total += chunk.len() as u64;
            if max_bytes > 0 && total > max_bytes {
                drop(file);
                tokio::fs::remove_file(&tmp).await.ok();
                bail!("exceeded the configured size limit mid-download");
            }
            hasher.update(&chunk);
            file.write_all(&chunk).await?;
        }
        file.flush().await?;
        drop(file);
        tokio::fs::rename(&tmp, dest)
            .await
            .with_context(|| format!("finalising {}", dest.display()))?;

        Ok((total, format!("{:x}", hasher.finalize())))
    }

    /// Time a request without keeping the body: returns
    /// (time to response headers, time to last byte, bytes read).
    pub async fn timed_fetch(&self, url: &str) -> Result<(Duration, Duration, u64)> {
        let query = self.auth_params().await;
        let full = self.url(url);
        self.limiter.acquire().await;

        let started = Instant::now();
        let mut req = self.http.get(&full);
        if !query.is_empty() {
            req = req.query(&query);
        }
        let resp = req.send().await?;
        let headers_at = started.elapsed();
        if !resp.status().is_success() {
            bail!("HTTP {} from {}", resp.status().as_u16(), redact(&full));
        }
        let mut total: u64 = 0;
        let mut stream = resp.bytes_stream();
        while let Some(chunk) = stream.next().await {
            total += chunk?.len() as u64;
        }
        Ok((headers_at, started.elapsed(), total))
    }
}

/// Pull `data` out of the envelope, turning a reported failure into [`ApiError`].
fn unwrap_envelope(value: Value, path: &str) -> Result<Value> {
    if truthy(value.get("success")) {
        return Ok(value.get("data").cloned().unwrap_or(Value::Object(Default::default())));
    }
    let (code, message) = match value.get("error") {
        Some(Value::Object(_)) => {
            let err = value.get("error").unwrap();
            (
                crate::model::str_of(err, "code"),
                crate::model::str_of(err, "message"),
            )
        }
        Some(other) => (String::new(), other.to_string()),
        None => (String::new(), "request failed".to_string()),
    };
    let code = if code.is_empty() { "api_error".to_string() } else { code };
    let message = if message.is_empty() {
        format!("request to {} failed", redact(path))
    } else {
        message
    };
    Err(ApiError { code, message }.into())
}

/// Keep credentials out of logs and error messages.
pub fn redact(url: &str) -> String {
    let mut out = String::with_capacity(url.len());
    for (i, part) in url.split(|c| c == '?' || c == '&').enumerate() {
        if i > 0 {
            out.push(if i == 1 { '?' } else { '&' });
        }
        let lowered = part.to_ascii_lowercase();
        if ["token=", "password=", "autotoken=", "name="]
            .iter()
            .any(|p| lowered.starts_with(p))
        {
            let key = part.split('=').next().unwrap_or(part);
            out.push_str(key);
            out.push_str("=[REDACTED]");
        } else {
            out.push_str(part);
        }
    }
    out
}
