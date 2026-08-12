//! Scrape orchestration.
//!
//! Two phases, each bounded by its own concurrency setting and both sharing the
//! one global rate limiter:
//!   1. fetch every selected thread's posts and write them
//!   2. download the audio those posts referenced
//!
//! Splitting them means audio work is driven from the database rather than from
//! memory, so an interrupted run picks up exactly where it stopped - including
//! audio belonging to threads scraped on an earlier run.

use anyhow::{Context, Result};
use futures::StreamExt;
use indicatif::{ProgressBar, ProgressStyle};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use crate::api::Client;
use crate::config::Config;
use crate::model::{expand_incremental, str_of, truthy, Post, Structure};
use crate::select::resolve_forums;
use crate::store::{now, AttachmentRecord, AudioJob, DbOp, Store, Writer};

fn progress(len: u64, what: &str) -> ProgressBar {
    let pb = ProgressBar::new(len);
    let style = ProgressStyle::with_template("  {bar:38} {pos}/{len} {msg}")
        .unwrap_or_else(|_| ProgressStyle::default_bar());
    pb.set_style(style);
    pb.set_message(what.to_string());
    pb
}

#[cfg(test)]
mod tests {
    use super::short_key;

    #[test]
    fn short_keys_stay_within_path_limits() {
        // Typical audio key: already short, so it is left recognisable.
        let audio = "bNX25hABNjAuubjddixpz2eM";
        assert_eq!(short_key(audio), audio);

        // Attachment ids can run to 130+ characters.
        let long: String = std::iter::repeat('a').take(140).collect();
        let shortened = short_key(&long);
        assert!(shortened.len() <= 40, "got {} chars", shortened.len());

        // Two long keys sharing a prefix must not collide.
        let a = format!("{}{}", "x".repeat(60), "one");
        let b = format!("{}{}", "x".repeat(60), "two");
        assert_ne!(short_key(&a), short_key(&b));
    }

    #[test]
    fn unsafe_characters_are_replaced() {
        assert_eq!(short_key("a/b\\c:d"), "a_b_c_d");
    }
}

/// Watch for Ctrl-C so a long run can be stopped without losing what is
/// already written.
fn install_cancel() -> Arc<AtomicBool> {
    let cancel = Arc::new(AtomicBool::new(false));
    let flag = cancel.clone();
    tokio::spawn(async move {
        if tokio::signal::ctrl_c().await.is_ok() {
            eprintln!("\nStopping - letting in-flight requests finish...");
            flag.store(true, Ordering::Relaxed);
        }
    });
    cancel
}

/// Fetch the catalog, asking the server for a delta when we already hold a copy.
///
/// The full catalog is ~5 MB and every run needs it, so this matters. Sending
/// the previous `ident` lets the server answer one of three ways:
///   * `unchanged` - nothing moved; rebuild from what we already stored
///   * `incremental` - unchanged rows are sent as `{"ref": id}` stubs to expand
///   * neither - a full catalog, which we take as-is
///
/// Any inconsistency (a stub referring to something we don't hold) falls back
/// to a full fetch rather than risking a catalog with holes in it.
async fn fetch_structure(client: &Client, store: &Store) -> Result<(Structure, &'static str)> {
    let cached_ident = store.get_meta("structure_ident")?.unwrap_or_default();
    if cached_ident.is_empty() {
        let data = client.get_data("/api/v1/forum", &[]).await?;
        return Ok((Structure::from_data(&data), "full"));
    }

    let data = client
        .get_data(
            "/api/v1/forum",
            &[
                ("ident".to_string(), cached_ident.clone()),
                ("increment".to_string(), "1".to_string()),
            ],
        )
        .await?;

    let (groups, forums, threads) = store.cached_structure_rows()?;
    let have_cache = !forums.is_empty() && !threads.is_empty();

    if truthy(data.get("unchanged")) && have_cache {
        let ident = {
            let returned = str_of(&data, "ident");
            if returned.is_empty() { cached_ident } else { returned }
        };
        return Ok((Structure::from_rows(&groups, &forums, &threads, ident), "unchanged"));
    }

    if truthy(data.get("incremental")) && have_cache {
        let rows = |key: &str| -> Vec<serde_json::Value> {
            data.get(key).and_then(|v| v.as_array()).cloned().unwrap_or_default()
        };
        let merged = expand_incremental(&rows("groups"), &groups, "id").and_then(|g| {
            let f = expand_incremental(&rows("forums"), &forums, "forumid")?;
            let t = expand_incremental(&rows("threads"), &threads, "id")?;
            Some((g, f, t))
        });
        if let Some((g, f, t)) = merged {
            let ident = {
                let returned = str_of(&data, "ident");
                if returned.is_empty() { cached_ident } else { returned }
            };
            return Ok((Structure::from_rows(&g, &f, &t, ident), "incremental"));
        }
        // Cache was not usable after all - start clean.
        let full = client.get_data("/api/v1/forum", &[]).await?;
        return Ok((Structure::from_data(&full), "full (cache miss)"));
    }

    Ok((Structure::from_data(&data), "full"))
}

async fn fetch_thread(client: &Client, thread_id: i64) -> Result<Vec<Post>> {
    let data = client
        .get_data(&format!("/api/v1/forum/{thread_id}"), &[])
        .await?;
    let rows = data
        .get("posts")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();
    Ok(rows.iter().map(|r| Post::from_row(r, thread_id)).collect())
}

/// Turn an opaque media key into a filesystem-safe, length-bounded stem.
///
/// Attachment ids run to 130+ characters, which pushes full paths past
/// Windows' 260-character limit and breaks anything that touches the files
/// outside this tool. The full id lives in the database, so the file only needs
/// a short stable name: a readable prefix plus a digest of the whole key to
/// keep it unique.
fn short_key(key: &str) -> String {
    let safe: String = key
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() || c == '-' || c == '_' { c } else { '_' })
        .collect();
    if safe.len() <= 40 {
        return safe;
    }
    let digest = format!("{:x}", Sha256::digest(key.as_bytes()));
    format!("{}-{}", &safe[..24], &digest[..12])
}

/// Shard media over subdirectories so no single directory holds tens of
/// thousands of files.
fn audio_path(cfg: &Config, job: &AudioJob) -> PathBuf {
    let key = if job.key.is_empty() {
        format!("post-{}", job.post_id)
    } else {
        job.key.clone()
    };
    let safe = short_key(&key);
    let ext = match job.content_type.as_str() {
        "audio/mpeg" | "audio/mp3" => "mp3",
        "audio/wav" | "audio/x-wav" => "wav",
        "audio/opus" => "opus",
        _ => "ogg",
    };
    let shard: String = safe.chars().take(2).collect();
    PathBuf::from(&cfg.media.audio_dir)
        .join(if shard.is_empty() { "_".to_string() } else { shard })
        .join(format!("{safe}.{ext}"))
}

/// Resolve one attachment's metadata, then fetch the file itself.
///
/// The info call gives the original filename, which is worth keeping: the id
/// alone says nothing about what the file is.
async fn fetch_attachment(
    client: &Client,
    id: &str,
    dir: &str,
    skip_existing: bool,
    max_bytes: u64,
) -> Result<AttachmentRecord> {
    let info = client
        .get_data(&format!("/api/v1/attachments/{id}"), &[])
        .await?;

    let name = str_of(&info, "name");
    let url = {
        let given = str_of(&info, "url");
        if given.is_empty() {
            format!("/api/v1/attachments/{id}/download")
        } else {
            given
        }
    };

    let safe_id = short_key(id);
    let extension = Path::new(&name)
        .extension()
        .and_then(|e| e.to_str())
        .map(|e| {
            e.chars()
                .filter(|c| c.is_ascii_alphanumeric())
                .take(8)
                .collect::<String>()
        })
        .filter(|e| !e.is_empty());
    let filename = match &extension {
        Some(ext) => format!("{safe_id}.{ext}"),
        None => safe_id.clone(),
    };
    let shard: String = safe_id.chars().take(2).collect();
    let dest = PathBuf::from(dir)
        .join(if shard.is_empty() { "_".to_string() } else { shard })
        .join(filename);

    let mut record = AttachmentRecord {
        id: id.to_string(),
        name,
        size: crate::model::int_of(&info, "size"),
        uploader: str_of(&info, "uploader"),
        uploadtime: crate::model::int_of(&info, "uploadtime"),
        url: url.clone(),
        path: dest.to_string_lossy().to_string(),
        ..Default::default()
    };

    if skip_existing {
        if let Ok(meta) = tokio::fs::metadata(&dest).await {
            if meta.len() > 0 {
                record.bytes = meta.len() as i64;
                return Ok(record);
            }
        }
    }

    let (bytes, sha) = client.download(&url, &dest, max_bytes).await?;
    record.bytes = bytes as i64;
    record.sha = sha;
    Ok(record)
}

pub async fn run(
    client: Arc<Client>,
    cfg: &Config,
    full: bool,
    no_audio: bool,
    limit: Option<usize>,
) -> Result<()> {
    let started = now();
    let cancel = install_cancel();

    let mut store = Store::open(Path::new(&cfg.storage.database))?;

    println!("Fetching the forum catalog...");
    let (structure, how) = fetch_structure(&client, &store).await?;
    println!("  catalog: {how}.");
    store.save_structure(&structure)?;
    if !structure.ident.is_empty() {
        store.set_meta("structure_ident", &structure.ident)?;
    }
    store.set_meta("structure_fetched_at", &now().to_string())?;
    println!(
        "  {} groups, {} forums, {} threads.",
        structure.groups.len(),
        structure.forums.len(),
        structure.threads.len()
    );

    // Work out which threads actually need fetching.
    let forum_ids: std::collections::HashSet<i64> =
        resolve_forums(cfg, &structure).into_iter().collect();
    let watermarks: HashMap<i64, i64> = if full || !cfg.scrape.skip_unchanged_threads {
        HashMap::new()
    } else {
        store.thread_watermarks()?
    };

    let mut targets: Vec<(i64, i64)> = structure
        .threads
        .iter()
        .filter(|t| forum_ids.contains(&t.forum_id))
        .filter(|t| match watermarks.get(&t.id) {
            // Already scraped and nothing new has been posted since.
            Some(seen) => *seen < t.last_update,
            None => true,
        })
        .map(|t| (t.id, t.last_update))
        .collect();
    targets.sort_by(|a, b| b.1.cmp(&a.1));
    // Count before truncating, so the report distinguishes "nothing left to do"
    // from "capped by --limit".
    let outstanding = targets.len();
    if let Some(limit) = limit {
        targets.truncate(limit);
    }

    let selected_threads = structure
        .threads
        .iter()
        .filter(|t| forum_ids.contains(&t.forum_id))
        .count();
    println!(
        "Selection covers {} forums and {} threads; {} need fetching{}{}.",
        forum_ids.len(),
        selected_threads,
        outstanding,
        if full { " (full re-scrape)" } else { "" },
        if targets.len() < outstanding {
            format!(", capped at {} by --limit", targets.len())
        } else {
            String::new()
        }
    );

    let mut threads_done = 0i64;
    let mut posts_written = 0i64;
    let mut failures = 0i64;

    if targets.is_empty() {
        println!("Everything is already up to date.");
    } else {
        let writer = Writer::spawn(store);
        let tx = writer.sender();
        let pb = progress(targets.len() as u64, "threads");

        let results = futures::stream::iter(targets.into_iter().map(|(thread_id, last_update)| {
            let client = client.clone();
            let tx = tx.clone();
            let pb = pb.clone();
            let cancel = cancel.clone();
            let stop_on_error = cfg.scrape.stop_on_error;
            async move {
                if cancel.load(Ordering::Relaxed) {
                    return None;
                }
                let outcome = fetch_thread(&client, thread_id).await;
                pb.inc(1);
                match outcome {
                    Ok(posts) => {
                        let count = posts.len();
                        let _ = tx.send(DbOp::Posts { thread_id, last_update, posts });
                        Some(Ok(count))
                    }
                    Err(err) => {
                        let _ = tx.send(DbOp::Error {
                            scope: "thread".into(),
                            ref_id: thread_id.to_string(),
                            message: format!("{err:#}"),
                        });
                        if stop_on_error {
                            cancel.store(true, Ordering::Relaxed);
                        }
                        Some(Err(err))
                    }
                }
            }
        }))
        .buffer_unordered(cfg.scrape.thread_concurrency)
        .collect::<Vec<_>>()
        .await;

        pb.finish_and_clear();
        drop(tx);
        store = writer.finish().await?;

        for result in results.into_iter().flatten() {
            match result {
                Ok(count) => {
                    threads_done += 1;
                    posts_written += count as i64;
                }
                Err(_) => failures += 1,
            }
        }
        println!("Threads: {threads_done} fetched, {posts_written} posts, {failures} failed.");
    }

    // Phase two: audio.
    let mut audio_done = 0i64;
    if cfg.media.download_audio && !no_audio && !cancel.load(Ordering::Relaxed) {
        let jobs = store.pending_audio()?;
        if jobs.is_empty() {
            println!("No audio left to download.");
        } else {
            println!("Downloading {} audio files...", jobs.len());
            let max_bytes = cfg.media.max_audio_mb.saturating_mul(1024 * 1024);
            let writer = Writer::spawn(store);
            let tx = writer.sender();
            let pb = progress(jobs.len() as u64, "audio");

            let results = futures::stream::iter(jobs.into_iter().map(|job| {
                let client = client.clone();
                let tx = tx.clone();
                let pb = pb.clone();
                let cancel = cancel.clone();
                let dest = audio_path(cfg, &job);
                let skip_existing = cfg.media.skip_existing;
                async move {
                    if cancel.load(Ordering::Relaxed) {
                        return None;
                    }
                    // Already on disk from an interrupted run: adopt it rather
                    // than fetching the bytes again.
                    if skip_existing && dest.exists() {
                        if let Ok(meta) = tokio::fs::metadata(&dest).await {
                            if meta.len() > 0 {
                                let _ = tx.send(DbOp::Audio {
                                    post_id: job.post_id,
                                    path: dest.to_string_lossy().to_string(),
                                    bytes: meta.len() as i64,
                                    sha: String::new(),
                                });
                                pb.inc(1);
                                return Some(Ok(0u64));
                            }
                        }
                    }
                    let outcome = client.download(&job.url, &dest, max_bytes).await;
                    pb.inc(1);
                    match outcome {
                        Ok((bytes, sha)) => {
                            let _ = tx.send(DbOp::Audio {
                                post_id: job.post_id,
                                path: dest.to_string_lossy().to_string(),
                                bytes: bytes as i64,
                                sha,
                            });
                            Some(Ok(bytes))
                        }
                        Err(err) => {
                            let _ = tx.send(DbOp::Error {
                                scope: "audio".into(),
                                ref_id: job.post_id.to_string(),
                                message: format!("{err:#}"),
                            });
                            Some(Err(err))
                        }
                    }
                }
            }))
            .buffer_unordered(cfg.scrape.audio_concurrency)
            .collect::<Vec<_>>()
            .await;

            pb.finish_and_clear();
            drop(tx);
            store = writer.finish().await?;

            let mut bytes_total = 0u64;
            let mut audio_failed = 0i64;
            for result in results.into_iter().flatten() {
                match result {
                    Ok(bytes) => {
                        audio_done += 1;
                        bytes_total += bytes;
                    }
                    Err(_) => audio_failed += 1,
                }
            }
            println!(
                "Audio: {audio_done} stored ({:.1} MB this run), {audio_failed} failed.",
                bytes_total as f64 / 1_048_576.0
            );
        }
    } else if no_audio || !cfg.media.download_audio {
        println!("Audio download is off; URLs are recorded in the database.");
    }

    // Phase three: attachments. Off by default - the ids and metadata are
    // recorded regardless, so switching this on later backfills them.
    let mut attachments_done = 0i64;
    if cfg.media.download_attachments && !cancel.load(Ordering::Relaxed) {
        let jobs = store.pending_attachments()?;
        if jobs.is_empty() {
            println!("No attachments left to download.");
        } else {
            println!("Downloading {} attachments...", jobs.len());
            let max_bytes = cfg.media.max_attachment_mb.saturating_mul(1024 * 1024);
            let writer = Writer::spawn(store);
            let tx = writer.sender();
            let pb = progress(jobs.len() as u64, "attachments");

            let results = futures::stream::iter(jobs.into_iter().map(|(id, _post_id)| {
                let client = client.clone();
                let tx = tx.clone();
                let pb = pb.clone();
                let cancel = cancel.clone();
                let dir = cfg.media.attachment_dir.clone();
                let skip_existing = cfg.media.skip_existing;
                async move {
                    if cancel.load(Ordering::Relaxed) {
                        return None;
                    }
                    let outcome =
                        fetch_attachment(&client, &id, &dir, skip_existing, max_bytes).await;
                    pb.inc(1);
                    match outcome {
                        Ok(record) => {
                            let bytes = record.bytes as u64;
                            let _ = tx.send(DbOp::Attachment(Box::new(record)));
                            Some(Ok(bytes))
                        }
                        Err(err) => {
                            let _ = tx.send(DbOp::Error {
                                scope: "attachment".into(),
                                ref_id: id,
                                message: format!("{err:#}"),
                            });
                            Some(Err(err))
                        }
                    }
                }
            }))
            .buffer_unordered(cfg.scrape.audio_concurrency)
            .collect::<Vec<_>>()
            .await;

            pb.finish_and_clear();
            drop(tx);
            store = writer.finish().await?;

            let mut bytes_total = 0u64;
            let mut failed = 0i64;
            for result in results.into_iter().flatten() {
                match result {
                    Ok(bytes) => {
                        attachments_done += 1;
                        bytes_total += bytes;
                    }
                    Err(_) => failed += 1,
                }
            }
            println!(
                "Attachments: {attachments_done} stored ({:.1} MB this run), {failed} failed.",
                bytes_total as f64 / 1_048_576.0
            );
        }
    }

    let notes = format!(
        "{}; catalog={}; attachments={}",
        if cancel.load(Ordering::Relaxed) { "interrupted" } else { "completed" },
        how,
        attachments_done
    );
    store
        .record_run(started, threads_done, posts_written, audio_done, &notes)
        .context("recording the run")?;

    let stats = store.stats()?;
    println!(
        "\nDatabase now holds {} posts across {} threads ({} scraped), {} audio files ({:.1} MB).",
        stats.posts,
        stats.threads,
        stats.threads_scraped,
        stats.audio_downloaded,
        stats.audio_bytes as f64 / 1_048_576.0
    );
    if stats.attachments > 0 {
        println!(
            "{} attachments referenced, {} downloaded.",
            stats.attachments, stats.attachments_downloaded
        );
    }
    if stats.errors > 0 {
        println!("{} errors logged - see the `errors` table.", stats.errors);
    }
    if cancel.load(Ordering::Relaxed) {
        println!("Run was interrupted; re-run to continue where it stopped.");
    }
    Ok(())
}
