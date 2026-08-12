//! Read-side latency benchmark: how long text posts and audio posts actually
//! take to arrive after the request goes out.
//!
//! Two numbers per sample, because they answer different questions:
//!   * **headers** - time until the server starts responding. Dominated by
//!     round-trip and server work, essentially independent of payload size.
//!   * **complete** - time until the last byte lands. This is what a scrape
//!     actually waits for, and where audio diverges from text.
//!
//! The sweep over concurrency levels shows where extra parallelism stops buying
//! throughput, which is what `scrape.thread_concurrency` and
//! `scrape.audio_concurrency` should be set from.

use anyhow::{bail, Result};
use futures::StreamExt;
use rand::seq::SliceRandom;
use serde::Serialize;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use crate::api::Client;
use crate::config::Config;
use crate::model::{str_of, Structure};
use crate::store::Store;

#[derive(Debug, Clone, Copy)]
struct Sample {
    headers_ms: f64,
    total_ms: f64,
    bytes: u64,
}

#[derive(Debug, Serialize)]
struct Summary {
    kind: String,
    concurrency: usize,
    samples: usize,
    failures: usize,
    headers_p50_ms: f64,
    headers_p90_ms: f64,
    complete_p50_ms: f64,
    complete_p90_ms: f64,
    complete_p99_ms: f64,
    complete_min_ms: f64,
    complete_max_ms: f64,
    complete_mean_ms: f64,
    mean_bytes: u64,
    wall_secs: f64,
    requests_per_sec: f64,
    throughput_mb_s: f64,
}

#[derive(Debug, Serialize)]
struct Report {
    generated_at: String,
    base_url: String,
    authenticated: bool,
    rate_limit_ignored: bool,
    results: Vec<Summary>,
}

fn percentile(sorted: &[f64], p: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let rank = (p / 100.0) * (sorted.len() - 1) as f64;
    let low = rank.floor() as usize;
    let high = rank.ceil() as usize;
    if low == high {
        return sorted[low];
    }
    let weight = rank - low as f64;
    sorted[low] * (1.0 - weight) + sorted[high] * weight
}

fn summarise(
    kind: &str,
    concurrency: usize,
    samples: &[Sample],
    failures: usize,
    wall: Duration,
) -> Summary {
    let mut totals: Vec<f64> = samples.iter().map(|s| s.total_ms).collect();
    let mut headers: Vec<f64> = samples.iter().map(|s| s.headers_ms).collect();
    totals.sort_by(|a, b| a.partial_cmp(b).unwrap());
    headers.sort_by(|a, b| a.partial_cmp(b).unwrap());

    let bytes: u64 = samples.iter().map(|s| s.bytes).sum();
    let wall_secs = wall.as_secs_f64().max(0.000_001);
    let n = samples.len();

    Summary {
        kind: kind.to_string(),
        concurrency,
        samples: n,
        failures,
        headers_p50_ms: percentile(&headers, 50.0),
        headers_p90_ms: percentile(&headers, 90.0),
        complete_p50_ms: percentile(&totals, 50.0),
        complete_p90_ms: percentile(&totals, 90.0),
        complete_p99_ms: percentile(&totals, 99.0),
        complete_min_ms: totals.first().copied().unwrap_or(0.0),
        complete_max_ms: totals.last().copied().unwrap_or(0.0),
        complete_mean_ms: if n == 0 { 0.0 } else { totals.iter().sum::<f64>() / n as f64 },
        mean_bytes: if n == 0 { 0 } else { bytes / n as u64 },
        wall_secs,
        requests_per_sec: n as f64 / wall_secs,
        throughput_mb_s: (bytes as f64 / 1_048_576.0) / wall_secs,
    }
}

/// Open and warm the connection pool before measuring.
///
/// Without this the very first batch pays TCP + TLS setup and reports it as
/// latency. Deliberately uses throwaway URLs, not any that will be measured:
/// fetching a measured URL first would leave it warm in the server's cache.
async fn warmup(client: &Client, urls: &[String]) {
    for url in urls.iter().take(3) {
        let _ = client.timed_fetch(url).await;
    }
}

/// Hand each concurrency level its own untouched slice of URLs.
///
/// Re-using one URL set across levels measures the server's cache rather than
/// the server: the first level pays for a cold fetch and every later level gets
/// a warm one, which is both an unfair comparison and unlike a real scrape,
/// where every thread is fetched exactly once. Wraps around only if the pool is
/// too small, which the caller reports.
fn slice_for(pool: &[String], level_index: usize, n: usize) -> (Vec<String>, bool) {
    if pool.is_empty() || n == 0 {
        return (Vec::new(), false);
    }
    let start = level_index * n;
    if start + n <= pool.len() {
        return (pool[start..start + n].to_vec(), false);
    }
    let wrapped = (0..n).map(|i| pool[(start + i) % pool.len()].clone()).collect();
    (wrapped, true)
}

/// Fire `urls` at the given concurrency and time each one.
async fn measure(client: Arc<Client>, urls: Vec<String>, concurrency: usize) -> (Vec<Sample>, usize, Duration) {
    let started = std::time::Instant::now();
    let results = futures::stream::iter(urls.into_iter().map(|url| {
        let client = client.clone();
        async move {
            client.timed_fetch(&url).await.map(|(headers, total, bytes)| Sample {
                headers_ms: headers.as_secs_f64() * 1000.0,
                total_ms: total.as_secs_f64() * 1000.0,
                bytes,
            })
        }
    }))
    .buffer_unordered(concurrency)
    .collect::<Vec<_>>()
    .await;
    let wall = started.elapsed();

    let mut samples = Vec::new();
    let mut failures = 0;
    for result in results {
        match result {
            Ok(sample) => samples.push(sample),
            Err(_) => failures += 1,
        }
    }
    (samples, failures, wall)
}

/// Look for audio posts by sampling threads, when the database has none yet.
async fn discover_audio(client: &Client, structure: &Structure, needed: usize) -> Result<Vec<String>> {
    let mut found = Vec::new();
    let mut candidates: Vec<i64> = structure.threads.iter().map(|t| t.id).collect();
    candidates.shuffle(&mut rand::thread_rng());

    for thread_id in candidates.into_iter().take(120) {
        if found.len() >= needed {
            break;
        }
        let Ok(data) = client.get_data(&format!("/api/v1/forum/{thread_id}"), &[]).await else {
            continue;
        };
        let Some(posts) = data.get("posts").and_then(|v| v.as_array()) else {
            continue;
        };
        for post in posts {
            if let Some(audio) = post.get("audio") {
                let url = str_of(audio, "url");
                if !url.is_empty() {
                    found.push(url);
                    if found.len() >= needed {
                        break;
                    }
                }
            }
        }
    }
    Ok(found)
}

fn print_table(results: &[Summary]) {
    println!(
        "\n{:<6} {:>5} {:>7} {:>9} {:>9} {:>9} {:>9} {:>10} {:>9}",
        "kind", "conc", "n", "hdr p50", "hdr p90", "all p50", "all p90", "mean size", "req/s"
    );
    println!("{}", "-".repeat(84));
    for r in results {
        println!(
            "{:<6} {:>5} {:>7} {:>8.0}m {:>8.0}m {:>8.0}m {:>8.0}m {:>9} {:>9.2}",
            r.kind,
            r.concurrency,
            r.samples,
            r.headers_p50_ms,
            r.headers_p90_ms,
            r.complete_p50_ms,
            r.complete_p90_ms,
            human_bytes(r.mean_bytes),
            r.requests_per_sec,
        );
    }
}

fn human_bytes(bytes: u64) -> String {
    if bytes >= 1_048_576 {
        format!("{:.1} MB", bytes as f64 / 1_048_576.0)
    } else if bytes >= 1024 {
        format!("{:.1} KB", bytes as f64 / 1024.0)
    } else {
        format!("{bytes} B")
    }
}

pub async fn run(cfg: &Config, samples_override: Option<usize>) -> Result<()> {
    // Benchmarking through our own throttle would just measure the throttle.
    let rate = if cfg.bench.ignore_rate_limit { 0.0 } else { cfg.scrape.requests_per_second };
    let client = Arc::new(Client::with_rate_limit(cfg, rate)?);
    if let Some(session) = crate::auth::load_session(cfg) {
        client.set_session(Some(session)).await;
    }

    let text_n = samples_override.unwrap_or(cfg.bench.text_samples);
    let audio_n = samples_override.unwrap_or(cfg.bench.audio_samples);
    if text_n == 0 && audio_n == 0 {
        bail!("nothing to measure - both sample counts are zero");
    }

    println!("Fetching the catalog to pick samples...");
    let data = client.get_data("/api/v1/forum", &[]).await?;
    let structure = Structure::from_data(&data);
    if structure.threads.is_empty() {
        bail!("the catalog came back empty");
    }

    let levels = if cfg.bench.concurrency_levels.is_empty() {
        vec![1usize]
    } else {
        cfg.bench.concurrency_levels.clone()
    };

    // Text samples: enough distinct threads to give every level its own set,
    // plus three held back purely for warming the connection.
    let mut thread_ids: Vec<i64> = structure.threads.iter().map(|t| t.id).collect();
    thread_ids.shuffle(&mut rand::thread_rng());
    let warm_urls: Vec<String> = thread_ids
        .iter()
        .take(3)
        .map(|id| format!("/api/v1/forum/{id}"))
        .collect();
    let text_pool: Vec<String> = thread_ids
        .iter()
        .skip(3)
        .take(text_n * levels.len())
        .map(|id| format!("/api/v1/forum/{id}"))
        .collect();

    // Audio samples: reuse what the database already knows, else go looking.
    let audio_needed = audio_n * levels.len();
    let db_path = Path::new(&cfg.storage.database);
    let mut audio_pool: Vec<String> = if db_path.exists() {
        Store::open(db_path)
            .and_then(|s| s.sample_audio_urls(audio_needed))
            .unwrap_or_default()
    } else {
        Vec::new()
    };
    if audio_pool.len() < audio_needed {
        println!(
            "Looking for audio posts ({} known, want {})...",
            audio_pool.len(),
            audio_needed
        );
        let found = discover_audio(&client, &structure, audio_needed - audio_pool.len()).await?;
        audio_pool.extend(found);
        audio_pool.sort();
        audio_pool.dedup();
    }
    if audio_pool.is_empty() {
        println!("No audio posts found to measure; reporting text only.");
    }

    println!(
        "\nMeasuring {} text and {} audio fetches per level, at concurrency {:?}{}.",
        text_n,
        audio_n.min(audio_pool.len()),
        levels,
        if cfg.bench.ignore_rate_limit { " (rate limit off)" } else { "" }
    );

    print!("Warming up connections... ");
    warmup(&client, &warm_urls).await;
    println!("done.");

    let mut results = Vec::new();
    let mut reused = false;
    for (index, &level) in levels.iter().enumerate() {
        let (urls, wrapped) = slice_for(&text_pool, index, text_n);
        reused |= wrapped;
        if !urls.is_empty() {
            let (samples, failures, wall) = measure(client.clone(), urls, level).await;
            results.push(summarise("text", level, &samples, failures, wall));
        }
        let (urls, wrapped) = slice_for(&audio_pool, index, audio_n);
        reused |= wrapped;
        if !urls.is_empty() {
            let (samples, failures, wall) = measure(client.clone(), urls, level).await;
            results.push(summarise("audio", level, &samples, failures, wall));
        }
    }
    if reused {
        println!(
            "\nNote: there were not enough distinct URLs to give every level a fresh set, so some \
             were re-used and may have been served from cache."
        );
    }

    print_table(&results);
    interpret(&results);

    let report = Report {
        generated_at: time::OffsetDateTime::now_utc()
            .format(&time::format_description::well_known::Rfc3339)
            .unwrap_or_default(),
        base_url: cfg.network.base_url.clone(),
        authenticated: client.has_session().await,
        rate_limit_ignored: cfg.bench.ignore_rate_limit,
        results,
    };
    let path = Path::new(&cfg.bench.report_file);
    if let Some(dir) = path.parent() {
        if !dir.as_os_str().is_empty() {
            std::fs::create_dir_all(dir).ok();
        }
    }
    std::fs::write(path, serde_json::to_string_pretty(&report)?)?;
    println!("\nFull report written to {}.", path.display());
    Ok(())
}

/// Turn the numbers into the two things the operator actually wants to know:
/// how much slower audio is, and where concurrency stops helping.
fn interpret(results: &[Summary]) {
    let at = |kind: &str, conc: usize| results.iter().find(|r| r.kind == kind && r.concurrency == conc);
    let lowest = results.iter().map(|r| r.concurrency).min().unwrap_or(1);

    println!("\nWhat this means:");
    let text = at("text", lowest);
    let audio = at("audio", lowest);

    if let Some(t) = text {
        // For text the two timings are nearly identical, which is itself the
        // finding: the wait is the server, not the wire.
        println!(
            "  - A thread of text posts arrives in about {:.0} ms, of which {:.0} ms is spent \
             before the first byte. Mean payload {} per thread.",
            t.complete_p50_ms,
            t.headers_p50_ms,
            human_bytes(t.mean_bytes)
        );
    }
    if let Some(a) = audio {
        println!(
            "  - An audio post arrives in about {:.0} ms: {:.0} ms to the first byte, the rest \
             transferring {}.",
            a.complete_p50_ms,
            a.headers_p50_ms,
            human_bytes(a.mean_bytes)
        );
    }

    if let (Some(t), Some(a)) = (text, audio) {
        let size_ratio = if t.mean_bytes > 0 {
            a.mean_bytes as f64 / t.mean_bytes as f64
        } else {
            0.0
        };
        if a.complete_p50_ms >= t.complete_p50_ms {
            let ratio = a.complete_p50_ms / t.complete_p50_ms.max(0.001);
            println!(
                "  - Audio takes {ratio:.1}x as long as text end to end, carrying {size_ratio:.0}x \
                 the bytes; the extra time is transfer, so it scales with file size."
            );
        } else {
            let ratio = t.complete_p50_ms / a.complete_p50_ms.max(0.001);
            println!(
                "  - Text is the slower of the two, by {ratio:.1}x, despite audio carrying about \
                 {size_ratio:.0}x more bytes."
            );
            println!(
                "  - The reason is where the time goes: a thread request spends nearly all of it \
                 waiting for the first byte ({:.0} of {:.0} ms) because the server assembles every \
                 post in the thread, while audio is a static file that starts returning in {:.0} \
                 ms. So text latency tracks thread length, and audio latency tracks file size.",
                t.headers_p50_ms, t.complete_p50_ms, a.headers_p50_ms
            );
        }
    }

    for kind in ["text", "audio"] {
        let mut by_level: Vec<&Summary> = results.iter().filter(|r| r.kind == kind).collect();
        by_level.sort_by_key(|r| r.concurrency);
        if by_level.len() < 2 {
            continue;
        }
        let best = by_level
            .iter()
            .max_by(|a, b| a.requests_per_sec.partial_cmp(&b.requests_per_sec).unwrap())
            .unwrap();
        println!(
            "  - {kind}: throughput peaks at concurrency {} ({:.1} req/s); set the matching \
             concurrency no higher than that.",
            best.concurrency, best.requests_per_sec
        );
    }
    println!(
        "  - These are read latencies. Publishing latency (how long after posting a new message it \
         becomes visible) is a separate, write-side measurement this command does not perform."
    );
}

#[cfg(test)]
mod tests {
    use super::percentile;

    #[test]
    fn percentiles_interpolate() {
        let data = vec![10.0, 20.0, 30.0, 40.0];
        assert_eq!(percentile(&data, 0.0), 10.0);
        assert_eq!(percentile(&data, 100.0), 40.0);
        assert_eq!(percentile(&data, 50.0), 25.0);
        assert!(percentile(&[], 50.0) == 0.0);
    }
}
