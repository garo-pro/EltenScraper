//! elten-scraper - archive the Elten (EltenLink) forum network.
//!
//! The public forum is readable without credentials, so `scrape` works out of
//! the box; `login` is only needed for private groups your account belongs to,
//! or when `auth.require_auth` is set in the config.

mod api;
mod auth;
mod bench;
mod config;
mod model;
mod scrape;
mod select;
mod store;

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use std::path::PathBuf;
use std::sync::Arc;

use api::Client;
use config::Config;
use store::Store;

#[derive(Parser)]
#[command(name = "elten-scraper", version, about = "Scraper and archiver for the Elten forum network")]
struct Cli {
    /// Path to the configuration file.
    #[arg(long, short, default_value = "elten.toml", global = true)]
    config: PathBuf,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Write a configuration file with default settings.
    Init {
        /// Overwrite an existing config.
        #[arg(long)]
        force: bool,
    },
    /// Choose interactively which groups or forums to scrape.
    Select,
    /// Log in and cache a session token (needed only for private groups).
    Login,
    /// Forget the cached session.
    Logout,
    /// Scrape the selected forums.
    Scrape {
        /// Re-fetch every thread, ignoring what was already scraped.
        #[arg(long)]
        full: bool,
        /// Skip audio downloads for this run.
        #[arg(long)]
        no_audio: bool,
        /// Stop after this many threads (useful for a trial run).
        #[arg(long)]
        limit: Option<usize>,
    },
    /// Measure how long text and audio posts take to arrive.
    Bench {
        /// Override the configured sample counts.
        #[arg(long)]
        samples: Option<usize>,
    },
    /// Show what is currently in the database.
    Status,
    /// Write one forum out as JSON.
    Export {
        /// Numeric forum id (see the `forums` table).
        #[arg(long)]
        forum: i64,
        /// Destination file.
        #[arg(long)]
        out: PathBuf,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Command::Init { force } => {
            if cli.config.exists() && !force {
                println!(
                    "{} already exists. Pass --force to overwrite it.",
                    cli.config.display()
                );
                return Ok(());
            }
            let cfg = Config::default();
            cfg.save(&cli.config)?;
            println!("Wrote default settings to {}.", cli.config.display());
            println!("Next: `elten-scraper select` to choose forums, then `elten-scraper scrape`.");
        }

        Command::Select => {
            let mut cfg = Config::load(&cli.config)?;
            let client = Client::new(&cfg)?;
            if let Some(session) = auth::load_session(&cfg) {
                client.set_session(Some(session)).await;
            }
            select::run(&client, &mut cfg, &cli.config).await?;
        }

        Command::Login => {
            let mut cfg = Config::load(&cli.config)?;
            let client = Client::new(&cfg)?;
            let session = auth::login(&client, &mut cfg, &cli.config).await?;
            println!("Logged in as {}. The session token is cached.", session.name);
        }

        Command::Logout => {
            let cfg = Config::load(&cli.config)?;
            auth::clear_session(&cfg)?;
            println!("Cached session removed.");
        }

        Command::Scrape { full, no_audio, limit } => {
            let cfg = Config::load(&cli.config)?;
            let client = Arc::new(Client::new(&cfg)?);
            auth::establish(&client, &cfg).await?;
            scrape::run(client, &cfg, full, no_audio, limit).await?;
        }

        Command::Bench { samples } => {
            let cfg = Config::load(&cli.config)?;
            bench::run(&cfg, samples).await?;
        }

        Command::Status => {
            let cfg = Config::load(&cli.config)?;
            let path = std::path::Path::new(&cfg.storage.database);
            if !path.exists() {
                println!("No database at {} yet - nothing scraped.", path.display());
                return Ok(());
            }
            let store = Store::open(path)?;
            let stats = store.stats()?;
            println!("Database: {}", path.display());
            println!("  groups            {}", stats.groups);
            println!("  forums            {}", stats.forums);
            println!("  threads known     {}", stats.threads);
            println!("  threads scraped   {}", stats.threads_scraped);
            println!("  posts             {}", stats.posts);
            println!(
                "  audio posts       {} ({} downloaded, {:.1} MB)",
                stats.audio_posts,
                stats.audio_downloaded,
                stats.audio_bytes as f64 / 1_048_576.0
            );
            println!(
                "  attachments       {} ({} downloaded)",
                stats.attachments, stats.attachments_downloaded
            );
            println!("  errors logged     {}", stats.errors);
            if let Some(ident) = store.get_meta("structure_ident")? {
                println!("  catalog fingerprint {ident}");
            }
        }

        Command::Export { forum, out } => {
            let cfg = Config::load(&cli.config)?;
            let store = Store::open(std::path::Path::new(&cfg.storage.database))?;
            let data = store.export_forum(forum)?;
            if let Some(dir) = out.parent() {
                if !dir.as_os_str().is_empty() {
                    std::fs::create_dir_all(dir).ok();
                }
            }
            std::fs::write(&out, serde_json::to_string_pretty(&data)?)
                .with_context(|| format!("writing {}", out.display()))?;
            println!("Exported forum {forum} to {}.", out.display());
        }
    }

    Ok(())
}
