//! Domain types plus lenient accessors for the API's JSON.
//!
//! The API is loose about scalar types (booleans arrive as `true`, `1` or
//! `"1"`; numbers sometimes as strings), so rather than deriving Deserialize we
//! read fields defensively out of `serde_json::Value` and keep the untouched
//! row alongside for archival.

use serde_json::Value;

pub fn str_of(v: &Value, key: &str) -> String {
    match v.get(key) {
        Some(Value::String(s)) => s.clone(),
        Some(Value::Null) | None => String::new(),
        Some(other) => other.to_string(),
    }
}

pub fn int_of(v: &Value, key: &str) -> i64 {
    match v.get(key) {
        Some(Value::Number(n)) => n.as_i64().or_else(|| n.as_f64().map(|f| f as i64)).unwrap_or(0),
        Some(Value::String(s)) => s.trim().parse::<i64>().unwrap_or(0),
        Some(Value::Bool(b)) => *b as i64,
        _ => 0,
    }
}

pub fn bool_of(v: &Value, key: &str) -> bool {
    match v.get(key) {
        Some(Value::Bool(b)) => *b,
        Some(Value::Number(n)) => n.as_i64().unwrap_or(0) != 0,
        Some(Value::String(s)) => {
            let s = s.trim();
            s == "1" || s.eq_ignore_ascii_case("true")
        }
        _ => false,
    }
}

/// `success` may be `true`, `1` or `"1"`.
pub fn truthy(v: Option<&Value>) -> bool {
    match v {
        Some(Value::Bool(b)) => *b,
        Some(Value::Number(n)) => n.as_i64().unwrap_or(0) != 0,
        Some(Value::String(s)) => s == "1" || s.eq_ignore_ascii_case("true"),
        _ => false,
    }
}

#[derive(Debug, Clone)]
pub struct Group {
    pub id: i64,
    pub name: String,
    pub founder: String,
    pub description: String,
    pub lang: String,
    pub public: bool,
    pub open: bool,
    pub hidden: bool,
    pub recommended: bool,
    pub parent: i64,
    pub created: i64,
    pub forums: i64,
    pub threads: i64,
    pub posts: i64,
    pub members: i64,
    pub audio_limit: i64,
    pub raw: Value,
}

impl Group {
    pub fn from_row(v: &Value) -> Self {
        Self {
            id: int_of(v, "id"),
            name: str_of(v, "name"),
            founder: str_of(v, "founder"),
            description: str_of(v, "description"),
            lang: str_of(v, "lang"),
            public: bool_of(v, "public"),
            open: bool_of(v, "open"),
            hidden: bool_of(v, "hidden"),
            recommended: bool_of(v, "recommended"),
            parent: int_of(v, "parent"),
            created: int_of(v, "created"),
            forums: int_of(v, "forums"),
            threads: int_of(v, "threads"),
            posts: int_of(v, "posts"),
            members: int_of(v, "acmembers"),
            audio_limit: int_of(v, "audiolimit"),
            raw: v.clone(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct Forum {
    /// Numeric id used by the API (`forumid`).
    pub id: i64,
    /// Opaque string identifier, e.g. "PL_ELTEN".
    pub ident: String,
    pub name: String,
    pub kind: i64,
    pub group_id: i64,
    pub description: String,
    pub closed: bool,
    pub threads: i64,
    pub posts: i64,
    pub position: i64,
    pub raw: Value,
}

impl Forum {
    pub fn from_row(v: &Value) -> Self {
        Self {
            id: int_of(v, "forumid"),
            ident: str_of(v, "id"),
            name: str_of(v, "name"),
            kind: int_of(v, "type"),
            group_id: int_of(v, "group_id"),
            description: str_of(v, "description"),
            closed: bool_of(v, "closed"),
            threads: int_of(v, "threads"),
            posts: int_of(v, "posts"),
            position: int_of(v, "position"),
            raw: v.clone(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct ThreadMeta {
    pub id: i64,
    pub name: String,
    pub author: String,
    pub forum_id: i64,
    pub pinned: bool,
    pub closed: bool,
    pub last_update: i64,
    pub posts: i64,
    pub raw: Value,
}

impl ThreadMeta {
    pub fn from_row(v: &Value) -> Self {
        Self {
            id: int_of(v, "id"),
            name: str_of(v, "name"),
            author: str_of(v, "author"),
            forum_id: int_of(v, "forumid"),
            pinned: bool_of(v, "pinned"),
            closed: bool_of(v, "closed"),
            last_update: int_of(v, "last_update"),
            posts: int_of(v, "posts"),
            raw: v.clone(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct AudioRef {
    pub key: String,
    pub url: String,
    pub content_type: String,
}

#[derive(Debug, Clone)]
pub struct Post {
    pub id: i64,
    pub thread_id: i64,
    pub author: String,
    pub text: String,
    pub signature: String,
    pub date: i64,
    pub format: i64,
    pub likes: i64,
    pub edited: bool,
    pub locked: bool,
    pub transcription: String,
    pub polls: String,
    pub attachments: String,
    pub audio: Option<AudioRef>,
    pub raw: Value,
}

impl Post {
    pub fn from_row(v: &Value, thread_id: i64) -> Self {
        let audio = v.get("audio").and_then(|a| {
            let url = str_of(a, "url");
            if url.is_empty() {
                return None;
            }
            Some(AudioRef {
                key: str_of(a, "id"),
                url,
                content_type: str_of(a, "content_type"),
            })
        });
        Self {
            id: int_of(v, "id"),
            thread_id,
            author: str_of(v, "author"),
            text: str_of(v, "text"),
            signature: str_of(v, "signature"),
            date: int_of(v, "date"),
            format: int_of(v, "format"),
            likes: int_of(v, "likes"),
            edited: bool_of(v, "edited"),
            locked: bool_of(v, "locked"),
            transcription: str_of(v, "transcription"),
            polls: str_of(v, "polls"),
            attachments: str_of(v, "attachments"),
            audio,
            raw: v.clone(),
        }
    }
}

/// Ids arrive as either numbers or strings; normalise for lookups.
fn scalar_key(v: &Value) -> String {
    match v {
        Value::String(s) => s.clone(),
        Value::Number(n) => n.to_string(),
        other => other.to_string(),
    }
}

/// In an incremental catalog response, a row consisting of nothing but `ref`
/// means "unchanged since the copy you already hold".
fn reference_id(v: &Value) -> Option<String> {
    let obj = v.as_object()?;
    if obj.len() != 1 {
        return None;
    }
    Some(scalar_key(obj.get("ref")?))
}

/// Rebuild a full row list from an incremental response by substituting cached
/// rows for `ref` stubs.
///
/// Returns `None` if any referenced row is missing locally - the caller must
/// then fall back to a full fetch, because a partial merge would silently drop
/// forums or threads.
pub fn expand_incremental(rows: &[Value], cached: &[Value], id_key: &str) -> Option<Vec<Value>> {
    let mut by_id: std::collections::HashMap<String, &Value> = std::collections::HashMap::new();
    for row in cached {
        if let Some(id) = row.get(id_key) {
            by_id.insert(scalar_key(id), row);
        }
    }
    let mut out = Vec::with_capacity(rows.len());
    for row in rows {
        match reference_id(row) {
            Some(id) => out.push((*by_id.get(&id)?).clone()),
            None => out.push(row.clone()),
        }
    }
    Some(out)
}

/// The whole public catalog, returned by a single `GET /api/v1/forum`.
#[derive(Debug, Clone)]
pub struct Structure {
    pub groups: Vec<Group>,
    pub forums: Vec<Forum>,
    pub threads: Vec<ThreadMeta>,
    /// Server-side fingerprint of the catalog, usable for cheap re-sync.
    pub ident: String,
}

impl Structure {
    pub fn from_data(data: &Value) -> Self {
        let rows = |key: &str| -> Vec<Value> {
            data.get(key)
                .and_then(|v| v.as_array())
                .cloned()
                .unwrap_or_default()
        };
        Self::from_rows(
            &rows("groups"),
            &rows("forums"),
            &rows("threads"),
            str_of(data, "ident"),
        )
    }

    pub fn from_rows(groups: &[Value], forums: &[Value], threads: &[Value], ident: String) -> Self {
        Self {
            groups: groups.iter().map(Group::from_row).collect(),
            forums: forums.iter().map(Forum::from_row).collect(),
            threads: threads.iter().map(ThreadMeta::from_row).collect(),
            ident,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn expands_reference_rows_from_cache() {
        let cached = vec![json!({"id": 1, "name": "kept"}), json!({"id": 2, "name": "old"})];
        let incoming = vec![json!({"ref": 1}), json!({"id": 2, "name": "new"})];
        let merged = expand_incremental(&incoming, &cached, "id").unwrap();
        assert_eq!(merged[0]["name"], "kept");
        assert_eq!(merged[1]["name"], "new");
    }

    #[test]
    fn string_and_numeric_ids_match() {
        let cached = vec![json!({"forumid": 7, "name": "seven"})];
        let merged = expand_incremental(&[json!({"ref": "7"})], &cached, "forumid").unwrap();
        assert_eq!(merged[0]["name"], "seven");
    }

    #[test]
    fn missing_reference_forces_full_refetch() {
        let cached = vec![json!({"id": 1})];
        assert!(expand_incremental(&[json!({"ref": 99})], &cached, "id").is_none());
    }

    #[test]
    fn truthy_accepts_the_servers_several_spellings() {
        assert!(truthy(Some(&json!(true))));
        assert!(truthy(Some(&json!(1))));
        assert!(truthy(Some(&json!("1"))));
        assert!(truthy(Some(&json!("true"))));
        assert!(!truthy(Some(&json!(false))));
        assert!(!truthy(Some(&json!(0))));
        assert!(!truthy(None));
    }
}
