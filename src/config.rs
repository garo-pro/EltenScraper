//! Configuration file handling. Everything that tunes *how* the scrape runs
//! (speed, parallelism, retries, media) lives here; *what* gets scraped is
//! chosen interactively by the `select` command and stored in [`Selection`].

use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};
use std::path::Path;

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct Config {
    pub network: Network,
    pub auth: Auth,
    pub scrape: Scrape,
    pub media: Media,
    pub storage: Storage,
    pub selection: Selection,
    pub bench: Bench,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct Network {
    pub base_url: String,
    /// Sent as the HTTP User-Agent. Identifies this tool rather than
    /// impersonating the official client.
    pub user_agent: String,
    /// Sent as `version_string` during login. The official client sends its own
    /// version here; the server accepts the generic fallback below.
    pub version_string: String,
    pub os: String,
    pub language: String,
    pub request_timeout_secs: u64,
    pub connect_timeout_secs: u64,
}

impl Default for Network {
    fn default() -> Self {
        Self {
            base_url: "https://api.elten.link".into(),
            user_agent: concat!("EltenScraper/", env!("CARGO_PKG_VERSION")).into(),
            version_string: "ELTEN".into(),
            os: "windows".into(),
            language: "en".into(),
            request_timeout_secs: 60,
            connect_timeout_secs: 15,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct Auth {
    /// The public parts of the forum are readable anonymously. Set this to true
    /// to refuse to scrape unless a session is established (needed to reach
    /// private/hidden groups your account belongs to).
    pub require_auth: bool,
    pub username: String,
    /// Cached session token, written by `login`. Never contains your password.
    pub session_file: String,
    /// Stable random device id, generated once. The server ties device
    /// authorisations to it, so keeping it stable avoids repeat 2FA prompts.
    pub appid_file: String,
}

impl Default for Auth {
    fn default() -> Self {
        Self {
            require_auth: false,
            username: String::new(),
            session_file: "data/session.json".into(),
            appid_file: "data/appid.txt".into(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct Scrape {
    /// How many threads are fetched concurrently.
    pub thread_concurrency: usize,
    /// How many audio files are downloaded concurrently.
    pub audio_concurrency: usize,
    /// Global ceiling across all workers. This is the main politeness dial:
    /// it is a shared token bucket, so raising concurrency alone will not
    /// exceed it. Set to 0 to disable rate limiting entirely.
    pub requests_per_second: f64,
    pub max_retries: u32,
    pub retry_base_ms: u64,
    pub retry_max_ms: u64,
    /// Skip re-fetching threads whose `last_update` has not advanced since the
    /// last run. Turn off (or pass `--full`) to force a complete re-scrape.
    pub skip_unchanged_threads: bool,
    /// Abort the whole run on the first thread that fails, instead of logging
    /// it and carrying on.
    pub stop_on_error: bool,
}

impl Default for Scrape {
    fn default() -> Self {
        Self {
            thread_concurrency: 4,
            audio_concurrency: 3,
            requests_per_second: 5.0,
            max_retries: 4,
            retry_base_ms: 500,
            retry_max_ms: 30_000,
            skip_unchanged_threads: true,
            stop_on_error: false,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct Media {
    pub download_audio: bool,
    /// Off by default: attachments are arbitrary user files and can be large.
    /// Their ids and metadata are recorded either way.
    pub download_attachments: bool,
    pub audio_dir: String,
    pub attachment_dir: String,
    /// Refuse to download any single file larger than this.
    pub max_audio_mb: u64,
    pub max_attachment_mb: u64,
    /// Don't re-download media already present on disk.
    pub skip_existing: bool,
}

impl Default for Media {
    fn default() -> Self {
        Self {
            download_audio: true,
            download_attachments: false,
            audio_dir: "data/audio".into(),
            attachment_dir: "data/attachments".into(),
            max_audio_mb: 64,
            max_attachment_mb: 256,
            skip_existing: true,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct Storage {
    /// One database for the whole network. Threads move between forums, and the
    /// catalog is fetched globally with a single resume point, so splitting per
    /// forum would fragment both. Audio stays on disk, referenced by path.
    pub database: String,
}

impl Default for Storage {
    fn default() -> Self {
        Self { database: "data/elten.db".into() }
    }
}

/// What to scrape. Managed by the `select` command.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct Selection {
    /// "all" | "groups" | "forums" | "exclude"
    pub mode: String,
    pub groups: Vec<i64>,
    pub forums: Vec<i64>,
}

impl Default for Selection {
    fn default() -> Self {
        Self { mode: "all".into(), groups: Vec::new(), forums: Vec::new() }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct Bench {
    /// Threads sampled when measuring text-post latency.
    pub text_samples: usize,
    /// Audio posts sampled when measuring audio latency.
    pub audio_samples: usize,
    /// Concurrency levels to sweep, to find where the server stops scaling.
    pub concurrency_levels: Vec<usize>,
    /// Ignore the configured rate limit while benchmarking, so the numbers
    /// reflect the server rather than our own throttle.
    pub ignore_rate_limit: bool,
    pub report_file: String,
}

impl Default for Bench {
    fn default() -> Self {
        Self {
            text_samples: 25,
            audio_samples: 15,
            concurrency_levels: vec![1, 2, 4, 8],
            ignore_rate_limit: true,
            report_file: "data/bench-report.json".into(),
        }
    }
}

impl Config {
    pub fn load(path: &Path) -> Result<Self> {
        if !path.exists() {
            bail!(
                "no config at {} - run `elten-scraper init` first",
                path.display()
            );
        }
        let text = std::fs::read_to_string(path)
            .with_context(|| format!("reading {}", path.display()))?;
        let cfg: Config = toml::from_str(&text)
            .with_context(|| format!("parsing {}", path.display()))?;
        cfg.validate()?;
        Ok(cfg)
    }

    pub fn save(&self, path: &Path) -> Result<()> {
        if let Some(dir) = path.parent() {
            if !dir.as_os_str().is_empty() {
                std::fs::create_dir_all(dir).ok();
            }
        }
        let text = toml::to_string_pretty(self).context("serialising config")?;
        std::fs::write(path, text).with_context(|| format!("writing {}", path.display()))?;
        Ok(())
    }

    fn validate(&self) -> Result<()> {
        if self.scrape.thread_concurrency == 0 {
            bail!("scrape.thread_concurrency must be at least 1");
        }
        if self.scrape.audio_concurrency == 0 {
            bail!("scrape.audio_concurrency must be at least 1");
        }
        if self.scrape.requests_per_second < 0.0 {
            bail!("scrape.requests_per_second cannot be negative");
        }
        if !matches!(self.selection.mode.as_str(), "all" | "groups" | "forums" | "exclude") {
            bail!(
                "selection.mode must be one of: all, groups, forums, exclude (got {:?})",
                self.selection.mode
            );
        }
        Ok(())
    }
}
