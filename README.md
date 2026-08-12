# elten-scraper

Archives the [Elten](https://elten.link) (EltenLink) forum network — groups, forums, threads, posts, transcriptions, audio and attachments — into a single SQLite database.

## Quick start

```sh
cargo build --release

elten-scraper init      # write elten.toml with defaults
elten-scraper select    # choose interactively what to scrape
elten-scraper scrape    # run it
elten-scraper status    # see what's stored
```

`scrape` is resumable and safe to interrupt: Ctrl-C lets in-flight requests finish, everything already written stays written, and the next run continues from there.

## Commands

Every command accepts `-c, --config <PATH>` (default `elten.toml`), so you can keep several configurations side by side — for example one per forum selection, each with its own database.

| Command | What it does |
| --- | --- |
| `init [--force]` | Write `elten.toml` with defaults. Refuses to overwrite an existing file unless `--force` is given. |
| `select` | Interactively choose which groups or forums to scrape, and save that choice to the config. |
| `login` | Authenticate and cache a session token. Only needed for private groups. |
| `logout` | Delete the cached session token. |
| `scrape [--full] [--no-audio] [--limit N]` | Fetch the selected forums. |
| `bench [--samples N]` | Measure how long text and audio posts take to arrive. `--samples` overrides both configured sample counts. |
| `status` | Print what the database currently holds. |
| `export --forum <ID> --out <FILE>` | Write one forum, with its threads and posts, out as JSON. |

`scrape` flags: `--full` re-fetches every selected thread, ignoring the watermarks that normally skip unchanged ones. `--no-audio` skips the audio phase for this run only, leaving the config untouched. `--limit N` stops after N threads, which is the right way to trial a large selection.

## How re-scraping works

Three mechanisms, in the order they apply.

**The catalog is fetched incrementally.** The full catalog is about 5.2 MB and every run needs it. After the first run the stored fingerprint (`ident`) is sent back with `increment=1`, and the server replies either with "unchanged", or with a delta in which untouched rows are collapsed to `{"ref": id}` stubs that get expanded from the local copy. Measured on this network: **5.19 MB / 2584 ms drops to 0.30 MB / 503 ms, a 94% saving.** If a stub ever refers to a row not held locally, the merge is abandoned and a full catalog is fetched instead, so a partial merge can never produce a catalog with holes in it.

**Unchanged threads are skipped.** Each thread's `last_update` from the catalog is compared against the watermark recorded when it was last scraped, and only threads that actually moved get fetched. A catalog refresh never clobbers those watermarks.

**Changed threads are re-fetched whole.** `GET /api/v1/forum/{id}` returns every post in the thread; the API offers no "posts since X" parameter, and the official client does not use one either. So a single new post re-downloads that entire thread. The waste is bounded by *thread* size, not group size.

What this costs in practice: a run that finds one new post makes two requests — the incremental catalog (~0.3 MB, ~0.5 s) plus the one thread. The average thread is about 24 posts (~40 ms); the largest thread on the network is 3,240 posts (1.41 MB, ~711 ms). **So roughly 0.6 s typical and 1.2 s worst case.** It does *not* walk every post in the group — a group holding 20,000 posts costs exactly the threads that changed, and nothing for the rest.

Deleted posts and threads are kept rather than removed, on the grounds that an archive should not lose what the server has dropped.

## Authentication

The public forum reads fine **without credentials**, so `scrape` works out of the box. Logging in only adds access to private or hidden groups your account belongs to.

```sh
elten-scraper login     # prompts for username/password, handles 2FA
```

Two-factor authentication is supported, by either SMS or backup code. The session token is cached in `data/session.json` and your password is never written to disk; `ELTEN_PASSWORD` is read from the environment if set, so an unattended run needs no prompt. A stable device id is generated once into `data/appid.txt` — keeping it avoids a fresh 2FA challenge on every run.

Set `auth.require_auth = true` to make the scraper refuse to run anonymously, which is what you want when a run that silently skipped private groups would be worse than no run at all.

Scraping issues only GETs, so it never mutates server state — in particular it never marks threads as read.

## Configuration

`elten.toml` controls *how* the scrape runs; `select` manages *what* it covers.

### `[network]`

| Key | Default | Meaning |
| --- | --- | --- |
| `base_url` | `https://api.elten.link` | API root. |
| `user_agent` | `EltenScraper/<version>` | Identifies this tool rather than impersonating the official client. |
| `version_string` | `ELTEN` | Sent as `version_string` at login. |
| `os` | `windows` | Reported at login. |
| `language` | `en` | Reported at login. |
| `request_timeout_secs` | `60` | Whole-request timeout. |
| `connect_timeout_secs` | `15` | Connection timeout. |

### `[auth]`

| Key | Default | Meaning |
| --- | --- | --- |
| `require_auth` | `false` | Refuse to scrape without a session. |
| `username` | *(empty)* | Remembered after the first `login`. |
| `session_file` | `data/session.json` | Where the cached token lives. |
| `appid_file` | `data/appid.txt` | Stable device id. |

### `[scrape]`

| Key | Default | Meaning |
| --- | --- | --- |
| `thread_concurrency` | `4` | Parallel thread fetches. |
| `audio_concurrency` | `3` | Parallel media downloads (audio and attachments). |
| `requests_per_second` | `5.0` | Global ceiling shared by every worker; `0` disables it. The main politeness dial. |
| `max_retries` | `4` | Retry attempts for 429s, 5xx and network errors. |
| `retry_base_ms` | `500` | Backoff base. |
| `retry_max_ms` | `30000` | Backoff ceiling. |
| `skip_unchanged_threads` | `true` | Skip threads with no new posts since the last run. |
| `stop_on_error` | `false` | Abort the whole run on the first failing thread instead of logging it and continuing. |

Concurrency is bounded independently of the rate limit, so raising `thread_concurrency` alone will never exceed `requests_per_second`.

### `[media]`

| Key | Default | Meaning |
| --- | --- | --- |
| `download_audio` | `true` | Fetch the audio files, not just their URLs. |
| `download_attachments` | `false` | Fetch post attachments. Ids and metadata are recorded either way, so switching this on later backfills them. |
| `audio_dir` | `data/audio` | Audio root. |
| `attachment_dir` | `data/attachments` | Attachment root. |
| `max_audio_mb` | `64` | Per-file cap, enforced *while streaming*. |
| `max_attachment_mb` | `256` | Per-file cap, enforced *while streaming*. |
| `skip_existing` | `true` | Adopt files already on disk instead of re-downloading them. |

### `[storage]`, `[selection]` and `[bench]`

`storage.database` (default `data/elten.db`) is the database path. `selection.mode` is one of `all`, `groups`, `forums` or `exclude`, with `selection.groups` and `selection.forums` holding the ids — all managed by `select`, but editable by hand. Under `[bench]`, `text_samples` (25) and `audio_samples` (15) set how many of each are measured *per concurrency level*, `concurrency_levels` (`[1, 2, 4, 8]`) is the sweep, `ignore_rate_limit` (`true`) bypasses the throttle so the numbers describe the server rather than your own config, and `report_file` is where the JSON report goes.

## Storage

One database for the whole network, not one per forum:

- the catalog is fetched globally with a single resume point;
- threads **move between forums**, which per-forum files would turn into orphans and duplicates;
- the interesting queries are cross-cutting — a user's whole history, network-wide statistics.

Tables: `groups`, `forums`, `threads`, `posts` and `attachments`, plus `meta` (the catalog fingerprint), `errors` (per-item failures, so one bad thread never stops a run) and `runs` (one row per invocation). Every catalog and post row also keeps the untouched API response in a `raw` column, so fields this tool does not model yet are still archived — and that same column is what the incremental catalog merge expands against.

Media stays on disk, sharded two characters deep, with only paths, sizes and SHA-256 digests in the database, so the `.db` remains small enough to copy around. Downloads are written to a `.part` file and renamed on completion, so an interrupted transfer is never mistaken for a finished one.

## Benchmark

`elten-scraper bench` measures how long text and audio posts take to arrive.

Each concurrency level gets its **own** set of URLs. Re-using one set would measure the server's cache rather than the server — the first level pays for a cold fetch and every later level gets a warm one — and a real scrape fetches each thread exactly once, so cold is the honest case. Three separate throwaway URLs warm the connection pool first, so TLS setup is not counted as latency.

Typical result from this network:

| | time to first byte | total | payload |
| --- | --- | --- | --- |
| text thread | ~39 ms | ~39 ms | ~8 KB |
| audio post | ~31 ms | ~59 ms | ~900 KB |

The two are limited by different things. For **text**, essentially all the time is spent before the first byte: the server assembles every post in the thread, so latency tracks *thread length*, not payload size. For **audio**, time to first byte is a flat ~31 ms because it is a static file, and everything above that is transfer, so latency tracks *file size*. Audio ends up only about 1.5× slower than text while carrying roughly 100× the bytes.

These are read-side latencies. How long a newly published post takes to become visible is a separate, write-side measurement this command does not perform.

## Scale

The public network is roughly 566 groups, 1,461 forums, 18,000 threads and 430,000 posts. About 10% of posts carry audio averaging ~750 KB, so a complete archive with `download_audio = true` is on the order of **30–40 GB** and many hours at polite rates. Use `select` to narrow the scope, or `--limit` for a trial run.

## Licence, and what it does not cover

The **code** is GPL-3.0-or-later; see `LICENSE`.

The **archive it produces is not covered by that licence, and is not yours to relicense.** A full run stores tens of thousands of posts written by identifiable people, along with their voice recordings. That content belongs to its authors. Some specifics worth keeping in mind:

- Most of these users are in the EU, so **GDPR applies**. Keeping an archive for your own analysis has a reasonable "personal or household purposes" footing; publishing or redistributing it does not, and is a materially different question.
- Voice recordings identify a person in a way a username does not, and this is a community of blind users who recorded them for a forum rather than for republication.
- "Readable anonymously" is not the same as "intended for redistribution". Some groups are `public` but not `open`, and some forums are closed archives.

**This tool is intended for local analysis.** `data/` and `export/` are both excluded by `.gitignore`; keep them out of any repository you publish, and treat the database and audio directory as personal data at rest. If you ever want to share the archive itself rather than the tool, discuss it with Elten's maintainer first.

## Provenance

The wire protocol was learned by reading the official Elten 3 desktop client (`dawidpieper/elten3`, GPLv3): base `https://api.elten.link`, REST under `/api/v1`, a `{"success", "data"}` envelope, sessions carried as `name` and `token` query parameters, and the `ident`/`increment` catalog scheme. No code from that project is reproduced here; this is an independent implementation.
