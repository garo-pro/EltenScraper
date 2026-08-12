//! Optional authentication.
//!
//! The public forum reads fine anonymously, so this is only needed to reach
//! private or hidden groups your account belongs to - or when
//! `auth.require_auth` is set, in which case the scrape refuses to start
//! without a session.
//!
//! Login mirrors the desktop client's flow: POST the credentials, and if the
//! server answers `auth.two_factor_required`, satisfy the challenge (SMS or a
//! backup code) against the device id and retry.

use anyhow::{anyhow, bail, Context, Result};
use dialoguer::{theme::ColorfulTheme, Input, Password, Select};
use rand::{distributions::Alphanumeric, Rng};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

use crate::api::{ApiError, Client, Session};
use crate::config::Config;
use crate::model::str_of;

const APPID_LEN: usize = 32;

#[derive(Debug, Serialize, Deserialize)]
struct CachedSession {
    name: String,
    token: String,
    #[serde(default)]
    issued_at: i64,
    #[serde(default)]
    appid: String,
}

fn path_of(value: &str) -> PathBuf {
    PathBuf::from(value)
}

/// A stable per-installation device id. The server ties device authorisations
/// to it, so regenerating it would trigger a fresh 2FA challenge every run.
pub fn device_id(cfg: &Config) -> Result<String> {
    let path = path_of(&cfg.auth.appid_file);
    if let Ok(existing) = std::fs::read_to_string(&path) {
        let trimmed = existing.trim().to_string();
        if !trimmed.is_empty() {
            return Ok(trimmed);
        }
    }
    let generated: String = rand::thread_rng()
        .sample_iter(&Alphanumeric)
        .take(APPID_LEN)
        .map(char::from)
        .collect();
    if let Some(dir) = path.parent() {
        if !dir.as_os_str().is_empty() {
            std::fs::create_dir_all(dir).ok();
        }
    }
    std::fs::write(&path, &generated)
        .with_context(|| format!("writing device id to {}", path.display()))?;
    Ok(generated)
}

pub fn load_session(cfg: &Config) -> Option<Session> {
    let text = std::fs::read_to_string(path_of(&cfg.auth.session_file)).ok()?;
    let cached: CachedSession = serde_json::from_str(&text).ok()?;
    if cached.name.is_empty() || cached.token.is_empty() {
        return None;
    }
    Some(Session { name: cached.name, token: cached.token })
}

fn save_session(cfg: &Config, session: &Session, appid: &str) -> Result<()> {
    let path = path_of(&cfg.auth.session_file);
    if let Some(dir) = path.parent() {
        if !dir.as_os_str().is_empty() {
            std::fs::create_dir_all(dir).ok();
        }
    }
    let cached = CachedSession {
        name: session.name.clone(),
        token: session.token.clone(),
        issued_at: crate::store::now(),
        appid: appid.to_string(),
    };
    std::fs::write(&path, serde_json::to_string_pretty(&cached)?)
        .with_context(|| format!("writing {}", path.display()))?;
    Ok(())
}

pub fn clear_session(cfg: &Config) -> Result<()> {
    let path = path_of(&cfg.auth.session_file);
    if path.exists() {
        std::fs::remove_file(&path)
            .with_context(|| format!("removing {}", path.display()))?;
    }
    Ok(())
}

fn base_params(cfg: &Config, appid: &str, username: &str, authmethod: &str) -> Vec<(String, String)> {
    vec![
        ("name".into(), username.to_string()),
        ("version_string".into(), cfg.network.version_string.to_uppercase()),
        ("version_isdevelopment".into(), "0".into()),
        ("version_islauncher".into(), "0".into()),
        ("appid".into(), appid.to_string()),
        ("lang".into(), cfg.network.language.clone()),
        ("language".into(), cfg.network.language.clone()),
        ("os".into(), cfg.network.os.clone()),
        ("authmethod".into(), authmethod.to_string()),
    ]
}

/// Extract the server's error code, if this failure came from the envelope.
fn api_code(err: &anyhow::Error) -> Option<String> {
    err.downcast_ref::<ApiError>().map(|e| e.code.clone())
}

async fn attempt_login(
    client: &Client,
    cfg: &Config,
    appid: &str,
    username: &str,
    password: &str,
    authmethod: &str,
) -> Result<Session> {
    let mut params = base_params(cfg, appid, username, authmethod);
    params.push(("password".into(), password.to_string()));
    let data = client.post_data("/api/v1/session", &params).await?;
    let token = str_of(&data, "token");
    if token.is_empty() {
        bail!("the server accepted the login but returned no session token");
    }
    let name = {
        let returned = str_of(&data, "name");
        if returned.is_empty() { username.to_string() } else { returned }
    };
    Ok(Session { name, token })
}

/// Interactive login. Prompts for whatever the config does not supply, walks
/// the 2FA challenge if the server asks for one, and caches the token.
pub async fn login(client: &Client, cfg: &mut Config, config_path: &Path) -> Result<Session> {
    let appid = device_id(cfg)?;

    let username = if cfg.auth.username.trim().is_empty() {
        Input::<String>::with_theme(&ColorfulTheme::default())
            .with_prompt("Elten username")
            .interact_text()?
    } else {
        println!("Logging in as {}", cfg.auth.username);
        cfg.auth.username.clone()
    };

    // Prefer the environment so the password never has to be typed in CI, and
    // never gets written to the config file either way.
    let password = match std::env::var("ELTEN_PASSWORD") {
        Ok(value) if !value.is_empty() => {
            println!("Using the password from $ELTEN_PASSWORD.");
            value
        }
        _ => Password::with_theme(&ColorfulTheme::default())
            .with_prompt("Password (not stored)")
            .interact()?,
    };

    let session = match attempt_login(client, cfg, &appid, &username, &password, "list").await {
        Ok(session) => session,
        Err(err) if api_code(&err).as_deref() == Some("auth.two_factor_required") => {
            solve_two_factor(client, cfg, &appid, &username, &password).await?
        }
        Err(err) => return Err(err),
    };

    save_session(cfg, &session, &appid)?;
    if cfg.auth.username != username {
        cfg.auth.username = username;
        cfg.save(config_path)?;
    }
    client.set_session(Some(session.clone())).await;
    Ok(session)
}

async fn solve_two_factor(
    client: &Client,
    cfg: &Config,
    appid: &str,
    username: &str,
    password: &str,
) -> Result<Session> {
    println!("\nThis account has two-factor authentication enabled.");
    let choice = Select::with_theme(&ColorfulTheme::default())
        .with_prompt("How do you want to authenticate?")
        .items(&["Text message (SMS)", "Backup code"])
        .default(0)
        .interact()?;

    if choice == 0 {
        // Asking with authmethod=phone is what makes the server send the SMS;
        // it answers with the same challenge error, which is expected here.
        match attempt_login(client, cfg, appid, username, password, "phone").await {
            Ok(session) => return Ok(session),
            Err(err) if api_code(&err).as_deref() == Some("auth.two_factor_required") => {}
            Err(err) => return Err(err),
        }
        println!("A code has been sent by text message.");
    }

    let prompt = if choice == 0 { "Code from the text message" } else { "Backup code" };
    for remaining in (1..=3).rev() {
        let code: String = Input::<String>::with_theme(&ColorfulTheme::default())
            .with_prompt(prompt)
            .interact_text()?;
        let code = code.trim().to_string();

        let authorize = client
            .post_data(
                "/api/v1/authentication/authorizations",
                &[
                    ("appid".into(), appid.to_string()),
                    ("name".into(), username.to_string()),
                    ("code".into(), code),
                ],
            )
            .await;

        match authorize {
            Ok(_) => return attempt_login(client, cfg, appid, username, password, "list").await,
            Err(err) => {
                if remaining == 1 {
                    return Err(err).context("verification failed too many times");
                }
                println!("  That code was not accepted ({remaining} attempts left).");
            }
        }
    }
    Err(anyhow!("two-factor authentication failed"))
}

/// Attach a cached session if there is one. Returns whether the scrape is
/// running authenticated, and fails when `require_auth` is set but no session
/// is available.
pub async fn establish(client: &Client, cfg: &Config) -> Result<bool> {
    match load_session(cfg) {
        Some(session) => {
            let name = session.name.clone();
            client.set_session(Some(session)).await;
            println!("Using the cached session for {name}.");
            Ok(true)
        }
        None => {
            if cfg.auth.require_auth {
                bail!(
                    "auth.require_auth is set but no session is cached - run `elten-scraper login` first"
                );
            }
            println!("Running anonymously (public forums only).");
            Ok(false)
        }
    }
}
