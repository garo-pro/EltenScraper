//! Interactive picker for choosing what to scrape.
//!
//! Deliberately numbered lists with typed answers rather than arrow-key
//! multi-select widgets: Elten's users are blind, so a linear, readable
//! transcript works far better with a screen reader than a redrawing TUI, and
//! it also survives being piped or run over a plain terminal.

use anyhow::{bail, Result};
use dialoguer::{theme::ColorfulTheme, Input};
use std::collections::HashSet;
use std::path::Path;

use crate::api::Client;
use crate::config::Config;
use crate::model::Structure;

/// Print a numbered menu and read one choice back. Returns a 0-based index.
fn choose_one(prompt: &str, options: &[&str]) -> Result<usize> {
    println!("\n{prompt}");
    for (i, option) in options.iter().enumerate() {
        println!("  {}. {}", i + 1, option);
    }
    loop {
        let answer: String = Input::with_theme(&ColorfulTheme::default())
            .with_prompt(format!("Enter a number (1-{})", options.len()))
            .interact_text()?;
        match answer.trim().parse::<usize>() {
            Ok(n) if n >= 1 && n <= options.len() => return Ok(n - 1),
            _ => println!("  Please enter a number between 1 and {}.", options.len()),
        }
    }
}

/// Parse "1,4,7-10" or "all" into 0-based indices.
fn parse_indices(input: &str, max: usize) -> Result<Vec<usize>> {
    let trimmed = input.trim();
    if trimmed.is_empty() {
        return Ok(Vec::new());
    }
    if trimmed.eq_ignore_ascii_case("all") {
        return Ok((0..max).collect());
    }
    let mut chosen = HashSet::new();
    for part in trimmed.split(',') {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }
        if let Some((from, to)) = part.split_once('-') {
            let from: usize = from.trim().parse().map_err(|_| bad(part))?;
            let to: usize = to.trim().parse().map_err(|_| bad(part))?;
            if from == 0 || to == 0 || from > to || to > max {
                bail!("{part} is out of the range 1-{max}");
            }
            for n in from..=to {
                chosen.insert(n - 1);
            }
        } else {
            let n: usize = part.parse().map_err(|_| bad(part))?;
            if n == 0 || n > max {
                bail!("{n} is out of the range 1-{max}");
            }
            chosen.insert(n - 1);
        }
    }
    let mut out: Vec<usize> = chosen.into_iter().collect();
    out.sort_unstable();
    Ok(out)
}

fn bad(part: &str) -> anyhow::Error {
    anyhow::anyhow!("could not read {part:?} as a number or range")
}

/// Ask for a list selection, re-prompting until it parses.
fn ask_indices(prompt: &str, max: usize) -> Result<Vec<usize>> {
    loop {
        let answer: String = Input::with_theme(&ColorfulTheme::default())
            .with_prompt(prompt)
            .allow_empty(true)
            .interact_text()?;
        match parse_indices(&answer, max) {
            Ok(indices) if indices.is_empty() => {
                println!("  Nothing selected - enter numbers such as 1,3,5-8 or 'all'.")
            }
            Ok(indices) => return Ok(indices),
            Err(err) => println!("  {err}"),
        }
    }
}

fn ask_filter(what: &str) -> Result<String> {
    let answer: String = Input::with_theme(&ColorfulTheme::default())
        .with_prompt(format!(
            "Filter {what} by name or language (press Enter for all)"
        ))
        .allow_empty(true)
        .interact_text()?;
    Ok(answer.trim().to_lowercase())
}

pub async fn run(client: &Client, cfg: &mut Config, config_path: &Path) -> Result<()> {
    println!("Fetching the forum catalog...");
    let data = client.get_data("/api/v1/forum", &[]).await?;
    let structure = Structure::from_data(&data);

    let total_posts: i64 = structure.forums.iter().map(|f| f.posts).sum();
    println!(
        "\n{} groups, {} forums, {} threads, about {} posts are visible{}.",
        structure.groups.len(),
        structure.forums.len(),
        structure.threads.len(),
        total_posts,
        if client.has_session().await { " to your account" } else { " anonymously" }
    );

    let mode = choose_one(
        "What should be scraped?",
        &[
            "Everything visible",
            "Whole groups that I pick",
            "Individual forums that I pick",
            "Everything except forums that I pick",
        ],
    )?;

    match mode {
        0 => {
            cfg.selection.mode = "all".into();
            cfg.selection.groups.clear();
            cfg.selection.forums.clear();
        }
        1 => {
            let picked = pick_groups(&structure)?;
            cfg.selection.mode = "groups".into();
            cfg.selection.groups = picked;
            cfg.selection.forums.clear();
        }
        2 => {
            let picked = pick_forums(&structure, "scrape")?;
            cfg.selection.mode = "forums".into();
            cfg.selection.forums = picked;
            cfg.selection.groups.clear();
        }
        _ => {
            let picked = pick_forums(&structure, "skip")?;
            cfg.selection.mode = "exclude".into();
            cfg.selection.forums = picked;
            cfg.selection.groups.clear();
        }
    }

    cfg.save(config_path)?;
    summarise(cfg, &structure);
    println!("\nSaved to {}. Run `elten-scraper scrape` to begin.", config_path.display());
    Ok(())
}

fn pick_groups(structure: &Structure) -> Result<Vec<i64>> {
    let filter = ask_filter("groups")?;
    let mut groups: Vec<_> = structure
        .groups
        .iter()
        .filter(|g| {
            filter.is_empty()
                || g.name.to_lowercase().contains(&filter)
                || g.lang.to_lowercase() == filter
        })
        .collect();
    groups.sort_by(|a, b| b.posts.cmp(&a.posts));

    if groups.is_empty() {
        bail!("no groups matched {filter:?}");
    }

    println!("\n{} matching groups (most active first):", groups.len());
    for (i, g) in groups.iter().enumerate() {
        println!(
            "  {}. {} [{}] - {} forums, {} posts{}",
            i + 1,
            g.name,
            if g.lang.is_empty() { "??" } else { &g.lang },
            g.forums,
            g.posts,
            if g.public { "" } else { " (private)" }
        );
    }

    let picked = ask_indices("Groups to scrape (e.g. 1,3,5-8 or 'all')", groups.len())?;
    Ok(picked.into_iter().map(|i| groups[i].id).collect())
}

fn pick_forums(structure: &Structure, verb: &str) -> Result<Vec<i64>> {
    let filter = ask_filter("forums")?;
    let group_names: std::collections::HashMap<i64, &str> = structure
        .groups
        .iter()
        .map(|g| (g.id, g.name.as_str()))
        .collect();

    let mut forums: Vec<_> = structure
        .forums
        .iter()
        .filter(|f| {
            if filter.is_empty() {
                return true;
            }
            let group = group_names.get(&f.group_id).copied().unwrap_or("");
            f.name.to_lowercase().contains(&filter) || group.to_lowercase().contains(&filter)
        })
        .collect();
    forums.sort_by(|a, b| b.posts.cmp(&a.posts));

    if forums.is_empty() {
        bail!("no forums matched {filter:?}");
    }

    println!("\n{} matching forums (most active first):", forums.len());
    for (i, f) in forums.iter().enumerate() {
        println!(
            "  {}. {} - {} threads, {} posts (group: {})",
            i + 1,
            f.name,
            f.threads,
            f.posts,
            group_names.get(&f.group_id).copied().unwrap_or("unknown")
        );
    }

    let picked = ask_indices(
        &format!("Forums to {verb} (e.g. 1,3,5-8 or 'all')"),
        forums.len(),
    )?;
    Ok(picked.into_iter().map(|i| forums[i].id).collect())
}

/// Which forum ids the current selection resolves to.
pub fn resolve_forums(cfg: &Config, structure: &Structure) -> Vec<i64> {
    match cfg.selection.mode.as_str() {
        "groups" => {
            let wanted: HashSet<i64> = cfg.selection.groups.iter().copied().collect();
            structure
                .forums
                .iter()
                .filter(|f| wanted.contains(&f.group_id))
                .map(|f| f.id)
                .collect()
        }
        "forums" => {
            let wanted: HashSet<i64> = cfg.selection.forums.iter().copied().collect();
            structure
                .forums
                .iter()
                .filter(|f| wanted.contains(&f.id))
                .map(|f| f.id)
                .collect()
        }
        "exclude" => {
            let skip: HashSet<i64> = cfg.selection.forums.iter().copied().collect();
            structure
                .forums
                .iter()
                .filter(|f| !skip.contains(&f.id))
                .map(|f| f.id)
                .collect()
        }
        _ => structure.forums.iter().map(|f| f.id).collect(),
    }
}

fn summarise(cfg: &Config, structure: &Structure) {
    let forums = resolve_forums(cfg, structure);
    let set: HashSet<i64> = forums.iter().copied().collect();
    let posts: i64 = structure
        .forums
        .iter()
        .filter(|f| set.contains(&f.id))
        .map(|f| f.posts)
        .sum();
    let threads = structure
        .threads
        .iter()
        .filter(|t| set.contains(&t.forum_id))
        .count();
    println!(
        "\nSelection: {} forums, {} threads, about {} posts.",
        forums.len(),
        threads,
        posts
    );
}

#[cfg(test)]
mod tests {
    use super::parse_indices;

    #[test]
    fn parses_lists_ranges_and_all() {
        assert_eq!(parse_indices("1,3", 5).unwrap(), vec![0, 2]);
        assert_eq!(parse_indices("2-4", 5).unwrap(), vec![1, 2, 3]);
        assert_eq!(parse_indices("all", 3).unwrap(), vec![0, 1, 2]);
        assert_eq!(parse_indices("", 3).unwrap(), Vec::<usize>::new());
        // Overlapping entries collapse rather than duplicating work.
        assert_eq!(parse_indices("1,1-2", 5).unwrap(), vec![0, 1]);
    }

    #[test]
    fn rejects_out_of_range_and_garbage() {
        assert!(parse_indices("0", 5).is_err());
        assert!(parse_indices("6", 5).is_err());
        assert!(parse_indices("4-2", 5).is_err());
        assert!(parse_indices("x", 5).is_err());
    }
}
