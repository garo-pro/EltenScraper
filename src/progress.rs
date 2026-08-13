//! Progress reporting, written for screen readers first.
//!
//! Elten is a network for blind people, so nearly everyone running this tool is
//! listening to the terminal rather than looking at it. A drawn bar is the wrong
//! shape for that: the glyphs are announced one by one as noise, and because the
//! bar is redrawn in place several times a second the line is re-announced
//! constantly while never saying anything new.
//!
//! So progress is plain numbers on their own lines - `threads: 120/400 (30%),
//! about 4m left` - emitted only when there is something worth hearing: every
//! `step_percent` of the work, or after `interval` has passed with no report (so
//! a slow phase still says it is alive), whichever comes first. Each report is a
//! complete, newline-terminated line, so it stays in the scrollback and can be
//! reviewed with the screen reader's review cursor - which an in-place redraw
//! cannot offer.

use std::io::Write;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use crate::config::Ui;

/// A counter over a known amount of work, shared across worker tasks.
///
/// Cheap to clone through the `Arc` the constructor returns; every clone counts
/// into the same total.
pub struct Progress {
    label: String,
    total: u64,
    started: Instant,
    /// 0 disables percentage-triggered reports.
    step_percent: u64,
    /// Zero disables time-triggered reports.
    interval: Duration,
    silent: bool,
    state: Mutex<State>,
}

struct State {
    done: u64,
    last_percent: u64,
    last_report: Instant,
    reported: bool,
}

impl Progress {
    pub fn new(total: u64, label: &str, ui: &Ui) -> Arc<Self> {
        let now = Instant::now();
        Arc::new(Self {
            label: label.to_string(),
            total,
            started: now,
            step_percent: ui.progress_step_percent as u64,
            interval: Duration::from_secs(ui.progress_interval_secs),
            silent: !ui.progress_enabled() || total == 0,
            state: Mutex::new(State {
                done: 0,
                last_percent: 0,
                last_report: now,
                reported: false,
            }),
        })
    }

    pub fn inc(&self, n: u64) {
        if self.silent {
            return;
        }
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        state.done = (state.done + n).min(self.total);
        let percent = self.percent(state.done);
        let now = Instant::now();

        let stepped = self.step_percent > 0
            && percent >= state.last_percent.saturating_add(self.step_percent);
        let overdue = !self.interval.is_zero()
            && now.duration_since(state.last_report) >= self.interval
            && percent > state.last_percent;
        // The finishing line is left to `finish`, which has the total runtime.
        if (stepped || overdue) && state.done < self.total {
            state.last_percent = percent;
            state.last_report = now;
            state.reported = true;
            let done = state.done;
            drop(state);
            self.report(done, percent, now);
        }
    }

    /// Final line for the phase: what was completed, and how long it took.
    /// Reports a partial count honestly when a run was interrupted.
    pub fn finish(&self) {
        if self.silent {
            return;
        }
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        let done = state.done;
        let reported = state.reported;
        drop(state);
        // Nothing ran long enough to have been reported and nothing happened:
        // stay quiet rather than adding a line saying so.
        if done == 0 && !reported {
            return;
        }
        let elapsed = self.started.elapsed();
        line(format!(
            "  {}: {}/{} ({}%) in {}",
            self.label,
            done,
            self.total,
            self.percent(done),
            duration(elapsed)
        ));
    }

    fn percent(&self, done: u64) -> u64 {
        done.saturating_mul(100).checked_div(self.total).unwrap_or(100)
    }

    fn report(&self, done: u64, percent: u64, now: Instant) {
        let mut text = format!("  {}: {}/{} ({}%)", self.label, done, self.total, percent);
        if let Some(left) = self.remaining(done, now) {
            text.push_str(&format!(", about {} left", duration(left)));
        }
        line(text);
    }

    /// Linear estimate from the rate so far. `None` until enough has finished
    /// for that rate to mean anything - an estimate off by an order of magnitude
    /// is worse than no estimate.
    fn remaining(&self, done: u64, now: Instant) -> Option<Duration> {
        if done < 5 || done >= self.total {
            return None;
        }
        let elapsed = now.duration_since(self.started).as_secs_f64();
        if elapsed < 1.0 {
            return None;
        }
        let left = (self.total - done) as f64 * (elapsed / done as f64);
        Some(Duration::from_secs_f64(left))
    }
}

/// Progress goes to stderr, keeping stdout to the run's actual output.
fn line(text: String) {
    let mut err = std::io::stderr().lock();
    let _ = writeln!(err, "{text}");
    let _ = err.flush();
}

/// Two units at most, largest first: "1h 4m", "3m 12s", "45s".
fn duration(d: Duration) -> String {
    let secs = d.as_secs();
    match (secs / 3600, (secs % 3600) / 60, secs % 60) {
        (0, 0, s) => format!("{s}s"),
        (0, m, s) => format!("{m}m {s}s"),
        (h, m, _) => format!("{h}h {m}m"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ui(step: u8, interval: u64) -> Ui {
        Ui {
            progress: "plain".into(),
            progress_step_percent: step,
            progress_interval_secs: interval,
        }
    }

    #[test]
    fn durations_read_as_words_not_clock_faces() {
        assert_eq!(duration(Duration::from_secs(45)), "45s");
        assert_eq!(duration(Duration::from_secs(192)), "3m 12s");
        assert_eq!(duration(Duration::from_secs(3840)), "1h 4m");
    }

    #[test]
    fn percentages_are_whole_numbers_of_the_total() {
        let pb = Progress::new(400, "threads", &ui(5, 30));
        assert_eq!(pb.percent(0), 0);
        assert_eq!(pb.percent(120), 30);
        assert_eq!(pb.percent(400), 100);
    }

    #[test]
    fn counting_never_runs_past_the_total() {
        let pb = Progress::new(3, "threads", &ui(5, 30));
        for _ in 0..10 {
            pb.inc(1);
        }
        assert_eq!(pb.state.lock().unwrap().done, 3);
    }

    #[test]
    fn a_disabled_progress_reports_nothing() {
        let mut ui = ui(5, 30);
        ui.progress = "none".into();
        let pb = Progress::new(10, "threads", &ui);
        pb.inc(1);
        assert!(pb.silent);
        assert_eq!(pb.state.lock().unwrap().done, 0);
    }

    #[test]
    fn no_estimate_until_the_rate_means_something() {
        let pb = Progress::new(400, "threads", &ui(5, 30));
        let now = Instant::now();
        // Too few samples, and no elapsed time to divide by.
        assert!(pb.remaining(2, now).is_none());
        assert!(pb.remaining(200, now).is_none());
        // Nothing left to wait for.
        assert!(pb.remaining(400, now + Duration::from_secs(10)).is_none());
        // Half done after 10s: roughly another 10s.
        let left = pb.remaining(200, now + Duration::from_secs(10)).unwrap();
        assert!((left.as_secs_f64() - 10.0).abs() < 0.5, "got {left:?}");
    }
}

