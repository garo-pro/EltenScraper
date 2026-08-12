//! SQLite storage.
//!
//! One database for the whole network rather than one per forum: the catalog is
//! fetched globally with a single resume point, threads move between forums, and
//! cross-forum queries (a user's whole post history, network-wide stats) are the
//! interesting ones. Audio blobs stay on disk in a sharded tree and are
//! referenced by path, so the database stays small enough to copy around.

use anyhow::{Context, Result};
use rusqlite::{params, Connection, OptionalExtension};
use std::collections::HashMap;
use std::path::Path;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};

use crate::model::{Post, Structure};

const SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS groups (
    id            INTEGER PRIMARY KEY,
    name          TEXT,
    founder       TEXT,
    description   TEXT,
    lang          TEXT,
    public        INTEGER,
    open          INTEGER,
    hidden        INTEGER,
    recommended   INTEGER,
    parent        INTEGER,
    created       INTEGER,
    forums_count  INTEGER,
    threads_count INTEGER,
    posts_count   INTEGER,
    members       INTEGER,
    audio_limit   INTEGER,
    raw           TEXT,
    updated_at    INTEGER
);

CREATE TABLE IF NOT EXISTS forums (
    id            INTEGER PRIMARY KEY,
    ident         TEXT,
    name          TEXT,
    kind          INTEGER,
    group_id      INTEGER,
    description   TEXT,
    closed        INTEGER,
    threads_count INTEGER,
    posts_count   INTEGER,
    position      INTEGER,
    raw           TEXT,
    updated_at    INTEGER
);

CREATE TABLE IF NOT EXISTS threads (
    id                  INTEGER PRIMARY KEY,
    name                TEXT,
    author              TEXT,
    forum_id            INTEGER,
    pinned              INTEGER,
    closed              INTEGER,
    last_update         INTEGER,
    posts_count         INTEGER,
    raw                 TEXT,
    updated_at          INTEGER,
    scraped_at          INTEGER,
    scraped_last_update INTEGER
);

CREATE TABLE IF NOT EXISTS posts (
    id                 INTEGER PRIMARY KEY,
    thread_id          INTEGER,
    author             TEXT,
    text               TEXT,
    signature          TEXT,
    date               INTEGER,
    format             INTEGER,
    likes              INTEGER,
    edited             INTEGER,
    locked             INTEGER,
    transcription      TEXT,
    polls              TEXT,
    attachments        TEXT,
    audio_key          TEXT,
    audio_url          TEXT,
    audio_content_type TEXT,
    audio_path         TEXT,
    audio_bytes        INTEGER,
    audio_sha256       TEXT,
    raw                TEXT,
    scraped_at         INTEGER
);

CREATE TABLE IF NOT EXISTS attachments (
    id         TEXT PRIMARY KEY,
    post_id    INTEGER,
    name       TEXT,
    size       INTEGER,
    uploader   TEXT,
    uploadtime INTEGER,
    url        TEXT,
    path       TEXT,
    bytes      INTEGER,
    sha256     TEXT,
    fetched_at INTEGER
);

CREATE TABLE IF NOT EXISTS errors (
    id      INTEGER PRIMARY KEY AUTOINCREMENT,
    at      INTEGER,
    scope   TEXT,
    ref_id  TEXT,
    message TEXT
);

CREATE TABLE IF NOT EXISTS runs (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    started_at  INTEGER,
    finished_at INTEGER,
    threads     INTEGER,
    posts       INTEGER,
    audio       INTEGER,
    notes       TEXT
);

CREATE INDEX IF NOT EXISTS idx_posts_thread ON posts(thread_id);
CREATE INDEX IF NOT EXISTS idx_posts_author ON posts(author);
CREATE INDEX IF NOT EXISTS idx_posts_date   ON posts(date);
CREATE INDEX IF NOT EXISTS idx_threads_forum ON threads(forum_id);
CREATE INDEX IF NOT EXISTS idx_forums_group  ON forums(group_id);
CREATE INDEX IF NOT EXISTS idx_attachments_post ON attachments(post_id);
CREATE INDEX IF NOT EXISTS idx_attachments_pending
    ON attachments(id) WHERE path IS NULL OR path = '';
CREATE INDEX IF NOT EXISTS idx_posts_audio_pending
    ON posts(id) WHERE audio_url IS NOT NULL AND audio_url <> '' AND (audio_path IS NULL OR audio_path = '');
"#;

pub fn now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

pub struct Store {
    conn: Connection,
}

#[derive(Debug, Default, Clone)]
pub struct Stats {
    pub groups: i64,
    pub forums: i64,
    pub threads: i64,
    pub threads_scraped: i64,
    pub posts: i64,
    pub audio_posts: i64,
    pub audio_downloaded: i64,
    pub audio_bytes: i64,
    pub attachments: i64,
    pub attachments_downloaded: i64,
    pub errors: i64,
}

/// An audio file still to be fetched.
#[derive(Debug, Clone)]
pub struct AudioJob {
    pub post_id: i64,
    pub key: String,
    pub url: String,
    pub content_type: String,
}

impl Store {
    pub fn open(path: &Path) -> Result<Self> {
        if let Some(dir) = path.parent() {
            if !dir.as_os_str().is_empty() {
                std::fs::create_dir_all(dir)
                    .with_context(|| format!("creating {}", dir.display()))?;
            }
        }
        let conn = Connection::open(path)
            .with_context(|| format!("opening database {}", path.display()))?;
        // WAL keeps the single writer from blocking readers; NORMAL is the
        // right durability trade for a re-runnable scrape.
        conn.execute_batch(
            "PRAGMA journal_mode = WAL;
             PRAGMA synchronous = NORMAL;
             PRAGMA foreign_keys = ON;
             PRAGMA busy_timeout = 10000;",
        )
        .context("configuring database")?;
        conn.execute_batch(SCHEMA).context("creating schema")?;
        Ok(Self { conn })
    }

    pub fn set_meta(&self, key: &str, value: &str) -> Result<()> {
        self.conn.execute(
            "INSERT INTO meta (key, value) VALUES (?1, ?2)
             ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            params![key, value],
        )?;
        Ok(())
    }

    pub fn get_meta(&self, key: &str) -> Result<Option<String>> {
        let value = self
            .conn
            .query_row("SELECT value FROM meta WHERE key = ?1", params![key], |r| {
                r.get::<_, String>(0)
            })
            .optional()?;
        Ok(value)
    }

    /// Write the whole catalog in one transaction. Counters and names change
    /// constantly, so everything but our own scrape bookkeeping is overwritten.
    pub fn save_structure(&mut self, structure: &Structure) -> Result<()> {
        let ts = now();
        let tx = self.conn.transaction()?;
        {
            let mut stmt = tx.prepare(
                "INSERT INTO groups (id, name, founder, description, lang, public, open, hidden,
                    recommended, parent, created, forums_count, threads_count, posts_count,
                    members, audio_limit, raw, updated_at)
                 VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17,?18)
                 ON CONFLICT(id) DO UPDATE SET
                    name=excluded.name, founder=excluded.founder, description=excluded.description,
                    lang=excluded.lang, public=excluded.public, open=excluded.open,
                    hidden=excluded.hidden, recommended=excluded.recommended, parent=excluded.parent,
                    created=excluded.created, forums_count=excluded.forums_count,
                    threads_count=excluded.threads_count, posts_count=excluded.posts_count,
                    members=excluded.members, audio_limit=excluded.audio_limit,
                    raw=excluded.raw, updated_at=excluded.updated_at",
            )?;
            for g in &structure.groups {
                stmt.execute(params![
                    g.id, g.name, g.founder, g.description, g.lang,
                    g.public as i64, g.open as i64, g.hidden as i64, g.recommended as i64,
                    g.parent, g.created, g.forums, g.threads, g.posts, g.members, g.audio_limit,
                    g.raw.to_string(), ts
                ])?;
            }

            let mut stmt = tx.prepare(
                "INSERT INTO forums (id, ident, name, kind, group_id, description, closed,
                    threads_count, posts_count, position, raw, updated_at)
                 VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12)
                 ON CONFLICT(id) DO UPDATE SET
                    ident=excluded.ident, name=excluded.name, kind=excluded.kind,
                    group_id=excluded.group_id, description=excluded.description,
                    closed=excluded.closed, threads_count=excluded.threads_count,
                    posts_count=excluded.posts_count, position=excluded.position,
                    raw=excluded.raw, updated_at=excluded.updated_at",
            )?;
            for f in &structure.forums {
                stmt.execute(params![
                    f.id, f.ident, f.name, f.kind, f.group_id, f.description, f.closed as i64,
                    f.threads, f.posts, f.position, f.raw.to_string(), ts
                ])?;
            }

            // Note the deliberate omission of scraped_at / scraped_last_update
            // from the UPDATE clause: those are our watermarks, not the
            // server's, and must survive a catalog refresh.
            let mut stmt = tx.prepare(
                "INSERT INTO threads (id, name, author, forum_id, pinned, closed, last_update,
                    posts_count, raw, updated_at)
                 VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10)
                 ON CONFLICT(id) DO UPDATE SET
                    name=excluded.name, author=excluded.author, forum_id=excluded.forum_id,
                    pinned=excluded.pinned, closed=excluded.closed,
                    last_update=excluded.last_update, posts_count=excluded.posts_count,
                    raw=excluded.raw, updated_at=excluded.updated_at",
            )?;
            for t in &structure.threads {
                stmt.execute(params![
                    t.id, t.name, t.author, t.forum_id, t.pinned as i64, t.closed as i64,
                    t.last_update, t.posts, t.raw.to_string(), ts
                ])?;
            }
        }
        tx.commit()?;
        Ok(())
    }

    /// thread id -> the `last_update` value we had already scraped.
    pub fn thread_watermarks(&self) -> Result<HashMap<i64, i64>> {
        let mut stmt = self.conn.prepare(
            "SELECT id, COALESCE(scraped_last_update, -1) FROM threads WHERE scraped_at IS NOT NULL",
        )?;
        let rows = stmt.query_map([], |r| Ok((r.get::<_, i64>(0)?, r.get::<_, i64>(1)?)))?;
        let mut map = HashMap::new();
        for row in rows {
            let (id, watermark) = row?;
            map.insert(id, watermark);
        }
        Ok(map)
    }

    fn write_posts(&mut self, thread_id: i64, last_update: i64, posts: &[Post]) -> Result<usize> {
        let ts = now();
        let tx = self.conn.transaction()?;
        {
            // audio_path/bytes/sha256 are intentionally not overwritten here:
            // a re-scrape of the metadata must not forget a file we already
            // pulled down.
            let mut stmt = tx.prepare(
                "INSERT INTO posts (id, thread_id, author, text, signature, date, format, likes,
                    edited, locked, transcription, polls, attachments,
                    audio_key, audio_url, audio_content_type, raw, scraped_at)
                 VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17,?18)
                 ON CONFLICT(id) DO UPDATE SET
                    thread_id=excluded.thread_id, author=excluded.author, text=excluded.text,
                    signature=excluded.signature, date=excluded.date, format=excluded.format,
                    likes=excluded.likes, edited=excluded.edited, locked=excluded.locked,
                    transcription=excluded.transcription, polls=excluded.polls,
                    attachments=excluded.attachments, audio_key=excluded.audio_key,
                    audio_url=excluded.audio_url, audio_content_type=excluded.audio_content_type,
                    raw=excluded.raw, scraped_at=excluded.scraped_at",
            )?;
            for p in posts {
                let (key, url, ctype) = match &p.audio {
                    Some(a) => (a.key.clone(), a.url.clone(), a.content_type.clone()),
                    None => (String::new(), String::new(), String::new()),
                };
                stmt.execute(params![
                    p.id, p.thread_id, p.author, p.text, p.signature, p.date, p.format, p.likes,
                    p.edited as i64, p.locked as i64, p.transcription, p.polls, p.attachments,
                    key, url, ctype, p.raw.to_string(), ts
                ])?;
            }
            // Record every attachment id referenced by these posts, so the
            // download phase can be driven from the database instead of having
            // to re-parse posts. OR IGNORE keeps already-fetched rows intact.
            let mut stmt =
                tx.prepare("INSERT OR IGNORE INTO attachments (id, post_id) VALUES (?1, ?2)")?;
            for p in posts {
                for attachment in p.attachments.split(',') {
                    let attachment = attachment.trim();
                    if !attachment.is_empty() {
                        stmt.execute(params![attachment, p.id])?;
                    }
                }
            }

            tx.execute(
                "UPDATE threads SET scraped_at = ?2, scraped_last_update = ?3 WHERE id = ?1",
                params![thread_id, ts, last_update],
            )?;
        }
        tx.commit()?;
        Ok(posts.len())
    }

    fn write_audio(&mut self, post_id: i64, path: &str, bytes: i64, sha: &str) -> Result<()> {
        self.conn.execute(
            "UPDATE posts SET audio_path = ?2, audio_bytes = ?3, audio_sha256 = ?4 WHERE id = ?1",
            params![post_id, path, bytes, sha],
        )?;
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn write_attachment(
        &mut self,
        id: &str,
        name: &str,
        size: i64,
        uploader: &str,
        uploadtime: i64,
        url: &str,
        path: &str,
        bytes: i64,
        sha: &str,
    ) -> Result<()> {
        self.conn.execute(
            "UPDATE attachments SET name = ?2, size = ?3, uploader = ?4, uploadtime = ?5,
                url = ?6, path = ?7, bytes = ?8, sha256 = ?9, fetched_at = ?10
             WHERE id = ?1",
            params![id, name, size, uploader, uploadtime, url, path, bytes, sha, now()],
        )?;
        Ok(())
    }

    fn write_error(&mut self, scope: &str, ref_id: &str, message: &str) -> Result<()> {
        self.conn.execute(
            "INSERT INTO errors (at, scope, ref_id, message) VALUES (?1,?2,?3,?4)",
            params![now(), scope, ref_id, message],
        )?;
        Ok(())
    }

    /// Audio referenced by scraped posts that we have not stored yet.
    pub fn pending_audio(&self) -> Result<Vec<AudioJob>> {
        let mut stmt = self.conn.prepare(
            "SELECT id, COALESCE(audio_key,''), audio_url, COALESCE(audio_content_type,'')
             FROM posts
             WHERE audio_url IS NOT NULL AND audio_url <> ''
               AND (audio_path IS NULL OR audio_path = '')",
        )?;
        let rows = stmt.query_map([], |r| {
            Ok(AudioJob {
                post_id: r.get(0)?,
                key: r.get(1)?,
                url: r.get(2)?,
                content_type: r.get(3)?,
            })
        })?;
        let mut jobs = Vec::new();
        for row in rows {
            jobs.push(row?);
        }
        Ok(jobs)
    }

    /// Attachments referenced by scraped posts that we have not stored yet.
    pub fn pending_attachments(&self) -> Result<Vec<(String, i64)>> {
        let mut stmt = self.conn.prepare(
            "SELECT id, COALESCE(post_id, 0) FROM attachments WHERE path IS NULL OR path = ''",
        )?;
        let rows = stmt.query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, i64>(1)?)))?;
        let mut out = Vec::new();
        for row in rows {
            out.push(row?);
        }
        Ok(out)
    }

    /// The raw catalog rows we already hold, used to expand an incremental
    /// response. Returned in (groups, forums, threads) order.
    pub fn cached_structure_rows(
        &self,
    ) -> Result<(Vec<serde_json::Value>, Vec<serde_json::Value>, Vec<serde_json::Value>)> {
        let load = |table: &str| -> Result<Vec<serde_json::Value>> {
            let mut stmt = self
                .conn
                .prepare(&format!("SELECT raw FROM {table} WHERE raw IS NOT NULL"))?;
            let rows = stmt.query_map([], |r| r.get::<_, String>(0))?;
            let mut out = Vec::new();
            for row in rows {
                if let Ok(value) = serde_json::from_str::<serde_json::Value>(&row?) {
                    out.push(value);
                }
            }
            Ok(out)
        };
        Ok((load("groups")?, load("forums")?, load("threads")?))
    }

    /// A few audio URLs already known, for the benchmark.
    pub fn sample_audio_urls(&self, limit: usize) -> Result<Vec<String>> {
        let mut stmt = self.conn.prepare(
            "SELECT audio_url FROM posts WHERE audio_url IS NOT NULL AND audio_url <> ''
             ORDER BY RANDOM() LIMIT ?1",
        )?;
        let rows = stmt.query_map(params![limit as i64], |r| r.get::<_, String>(0))?;
        let mut out = Vec::new();
        for row in rows {
            out.push(row?);
        }
        Ok(out)
    }

    pub fn stats(&self) -> Result<Stats> {
        let one = |sql: &str| -> Result<i64> {
            Ok(self.conn.query_row(sql, [], |r| r.get::<_, i64>(0))?)
        };
        Ok(Stats {
            groups: one("SELECT COUNT(*) FROM groups")?,
            forums: one("SELECT COUNT(*) FROM forums")?,
            threads: one("SELECT COUNT(*) FROM threads")?,
            threads_scraped: one("SELECT COUNT(*) FROM threads WHERE scraped_at IS NOT NULL")?,
            posts: one("SELECT COUNT(*) FROM posts")?,
            audio_posts: one(
                "SELECT COUNT(*) FROM posts WHERE audio_url IS NOT NULL AND audio_url <> ''",
            )?,
            audio_downloaded: one(
                "SELECT COUNT(*) FROM posts WHERE audio_path IS NOT NULL AND audio_path <> ''",
            )?,
            audio_bytes: one("SELECT COALESCE(SUM(audio_bytes), 0) FROM posts")?,
            attachments: one("SELECT COUNT(*) FROM attachments")?,
            attachments_downloaded: one(
                "SELECT COUNT(*) FROM attachments WHERE path IS NOT NULL AND path <> ''",
            )?,
            errors: one("SELECT COUNT(*) FROM errors")?,
        })
    }

    pub fn record_run(&self, started: i64, threads: i64, posts: i64, audio: i64, notes: &str) -> Result<()> {
        self.conn.execute(
            "INSERT INTO runs (started_at, finished_at, threads, posts, audio, notes)
             VALUES (?1,?2,?3,?4,?5,?6)",
            params![started, now(), threads, posts, audio, notes],
        )?;
        Ok(())
    }

    /// Dump one forum (with its threads and posts) as JSON.
    pub fn export_forum(&self, forum_id: i64) -> Result<serde_json::Value> {
        let forum = self
            .conn
            .query_row(
                "SELECT raw FROM forums WHERE id = ?1",
                params![forum_id],
                |r| r.get::<_, String>(0),
            )
            .optional()?
            .and_then(|s| serde_json::from_str::<serde_json::Value>(&s).ok())
            .unwrap_or(serde_json::Value::Null);

        let mut stmt = self
            .conn
            .prepare("SELECT id, name, author, last_update FROM threads WHERE forum_id = ?1")?;
        let thread_rows = stmt.query_map(params![forum_id], |r| {
            Ok((
                r.get::<_, i64>(0)?,
                r.get::<_, String>(1)?,
                r.get::<_, String>(2)?,
                r.get::<_, i64>(3)?,
            ))
        })?;

        let mut threads = Vec::new();
        for row in thread_rows {
            let (id, name, author, last_update) = row?;
            let mut pstmt = self.conn.prepare(
                "SELECT id, author, text, transcription, date, likes, audio_url, audio_path
                 FROM posts WHERE thread_id = ?1 ORDER BY id",
            )?;
            let posts = pstmt
                .query_map(params![id], |r| {
                    Ok(serde_json::json!({
                        "id": r.get::<_, i64>(0)?,
                        "author": r.get::<_, String>(1)?,
                        "text": r.get::<_, String>(2)?,
                        "transcription": r.get::<_, String>(3)?,
                        "date": r.get::<_, i64>(4)?,
                        "likes": r.get::<_, i64>(5)?,
                        "audio_url": r.get::<_, Option<String>>(6)?,
                        "audio_path": r.get::<_, Option<String>>(7)?,
                    }))
                })?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            threads.push(serde_json::json!({
                "id": id,
                "name": name,
                "author": author,
                "last_update": last_update,
                "posts": posts,
            }));
        }

        Ok(serde_json::json!({
            "forum_id": forum_id,
            "forum": forum,
            "threads": threads,
        }))
    }
}

/// Messages sent to the writer thread.
pub enum DbOp {
    Posts { thread_id: i64, last_update: i64, posts: Vec<Post> },
    Audio { post_id: i64, path: String, bytes: i64, sha: String },
    Attachment(Box<AttachmentRecord>),
    Error { scope: String, ref_id: String, message: String },
}

#[derive(Debug, Clone, Default)]
pub struct AttachmentRecord {
    pub id: String,
    pub name: String,
    pub size: i64,
    pub uploader: String,
    pub uploadtime: i64,
    pub url: String,
    pub path: String,
    pub bytes: i64,
    pub sha: String,
}

/// Serialises all writes onto one thread. Workers stay async and never contend
/// for the connection; SQLite gets one transaction per message, which for a
/// thread's worth of posts is already good batching.
pub struct Writer {
    tx: UnboundedSender<DbOp>,
    handle: tokio::task::JoinHandle<Result<Store>>,
}

impl Writer {
    pub fn spawn(store: Store) -> Self {
        let (tx, rx) = unbounded_channel::<DbOp>();
        let handle = tokio::task::spawn_blocking(move || writer_loop(store, rx));
        Self { tx, handle }
    }

    pub fn sender(&self) -> UnboundedSender<DbOp> {
        self.tx.clone()
    }

    /// Drop the sending half and wait for the queue to drain, handing the
    /// store back so the caller can read final stats.
    pub async fn finish(self) -> Result<Store> {
        drop(self.tx);
        self.handle.await.context("writer thread panicked")?
    }
}

fn writer_loop(mut store: Store, mut rx: UnboundedReceiver<DbOp>) -> Result<Store> {
    while let Some(op) = rx.blocking_recv() {
        let result = match &op {
            DbOp::Posts { thread_id, last_update, posts } => {
                store.write_posts(*thread_id, *last_update, posts).map(|_| ())
            }
            DbOp::Audio { post_id, path, bytes, sha } => {
                store.write_audio(*post_id, path, *bytes, sha)
            }
            DbOp::Attachment(record) => store.write_attachment(
                &record.id,
                &record.name,
                record.size,
                &record.uploader,
                record.uploadtime,
                &record.url,
                &record.path,
                record.bytes,
                &record.sha,
            ),
            DbOp::Error { scope, ref_id, message } => {
                store.write_error(scope, ref_id, message)
            }
        };
        // A failed write must not kill the writer and strand every worker
        // still queuing behind it; record and continue.
        if let Err(err) = result {
            eprintln!("  ! database write failed: {err:#}");
        }
    }
    Ok(store)
}
