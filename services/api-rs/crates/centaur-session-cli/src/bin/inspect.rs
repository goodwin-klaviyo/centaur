use std::{collections::BTreeMap, env, fs, str::FromStr, time::Duration};

use centaur_api_server::{
    client::{CentaurClient, ClientError, SseEvent},
    types::SessionContextResponse,
};
use centaur_session_core::ThreadKey;
use clap::Parser;
use eyre::{Result, WrapErr, bail};
use futures_util::{Stream, StreamExt};
use serde::Serialize;
use serde_json::Value;
use tokio::time::{Instant, timeout, timeout_at};

const DEFAULT_API_URL: &str = "http://127.0.0.1:8080";
const DEFAULT_IDLE_MS: u64 = 1_500;
const DEFAULT_MAX_SECONDS: u64 = 30;
const MAX_EVENTS: usize = 5_000;
const MAX_IDLE_MS: u64 = 30_000;
const MAX_SECONDS: u64 = 300;

#[derive(Debug, Parser)]
#[command(about = "Read and inspect recorded Centaur session activity")]
struct Args {
    /// Session/thread key (or a Slack permalink containing the thread timestamp).
    thread_key: String,

    #[arg(long, env = "CENTAUR_API_URL", default_value = DEFAULT_API_URL)]
    api_url: String,

    /// Return only events for one execution. Also ends when that execution is terminal.
    #[arg(long)]
    execution_id: Option<String>,

    /// Resume after this durable event ID.
    #[arg(long, default_value_t = 0)]
    after_event_id: i64,

    /// Stop after this much stream inactivity (maximum 30 seconds).
    #[arg(long, default_value_t = DEFAULT_IDLE_MS, value_parser = bounded_idle_ms)]
    idle_ms: u64,

    /// Hard stream deadline (maximum 300 seconds).
    #[arg(long, default_value_t = DEFAULT_MAX_SECONDS, value_parser = bounded_max_seconds)]
    max_seconds: u64,

    /// Write the complete JSON export to this path instead of human output.
    #[arg(long)]
    output: Option<String>,

    /// Emit JSON to stdout instead of the human-readable timeline.
    #[arg(long)]
    json: bool,
}

fn bounded_idle_ms(value: &str) -> std::result::Result<u64, String> {
    let parsed = value.parse::<u64>().map_err(|error| error.to_string())?;
    if parsed == 0 || parsed > MAX_IDLE_MS {
        return Err(format!("must be between 1 and {MAX_IDLE_MS} milliseconds"));
    }
    Ok(parsed)
}

fn bounded_max_seconds(value: &str) -> std::result::Result<u64, String> {
    let parsed = value.parse::<u64>().map_err(|error| error.to_string())?;
    if parsed == 0 || parsed > MAX_SECONDS {
        return Err(format!("must be between 1 and {MAX_SECONDS} seconds"));
    }
    Ok(parsed)
}

#[derive(Debug, Serialize)]
struct Inspection {
    session: SessionContextResponse,
    events: Vec<TimelineEvent>,
    executions: BTreeMap<String, String>,
    provenance: &'static str,
    truncated: bool,
    stop_reason: &'static str,
}

#[derive(Debug, Serialize)]
struct TimelineEvent {
    event_id: i64,
    execution_id: Option<String>,
    event_type: String,
    activity: String,
    data: Value,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let api_token = env::var("CENTAUR_API_TOKEN")
        .wrap_err("CENTAUR_API_TOKEN must be set in the environment")?;
    if api_token.trim().is_empty() {
        bail!("CENTAUR_API_TOKEN must not be empty");
    }
    if args.after_event_id < 0 {
        bail!("--after-event-id must be zero or greater");
    }
    let thread_key = parse_thread_key(&args.thread_key)?;
    let client = CentaurClient::with_bearer_token(&args.api_url, &api_token)
        .wrap_err("configure API authentication")?;
    let session = client
        .get_session_context(&thread_key)
        .await
        .wrap_err("read session context (the API enforces session ownership)")?;
    let mut stream = client
        .stream_events_for(
            &thread_key,
            args.after_event_id,
            args.execution_id.as_deref(),
        )
        .await
        .wrap_err("open authorized session event stream")?;

    let (events, executions, truncated, stop_reason) = collect_events(
        &mut stream,
        args.after_event_id,
        args.execution_id.as_deref(),
        Duration::from_millis(args.idle_ms),
        Duration::from_secs(args.max_seconds),
    )
    .await?;

    let inspection = Inspection {
        session,
        events,
        executions,
        provenance: "not exposed by the session read API; unknown",
        truncated,
        stop_reason,
    };
    if let Some(path) = args.output {
        let encoded = serde_json::to_vec_pretty(&inspection)?;
        fs::write(&path, encoded).wrap_err_with(|| format!("write JSON export to {path}"))?;
    } else if args.json {
        println!("{}", serde_json::to_string_pretty(&inspection)?);
    } else {
        render_human(&inspection);
    }
    Ok(())
}

async fn collect_events<S>(
    stream: &mut S,
    after_event_id: i64,
    requested_execution: Option<&str>,
    idle: Duration,
    max_duration: Duration,
) -> Result<(
    Vec<TimelineEvent>,
    BTreeMap<String, String>,
    bool,
    &'static str,
)>
where
    S: Stream<Item = std::result::Result<SseEvent, ClientError>> + Unpin,
{
    let deadline = Instant::now() + max_duration;
    let mut events = Vec::new();
    let mut executions = BTreeMap::new();
    let mut last_id = after_event_id;
    let mut truncated = false;
    let stop_reason = loop {
        if events.len() >= MAX_EVENTS {
            truncated = true;
            break "event_limit";
        }
        let next = timeout_at(deadline, timeout(idle, stream.next())).await;
        let item = match next {
            Err(_) => {
                truncated = true;
                break "max_duration";
            }
            Ok(Err(_)) => break "idle_timeout",
            Ok(Ok(None)) => break "stream_closed",
            Ok(Ok(Some(Err(error)))) => return Err(error).wrap_err("read session event"),
            Ok(Ok(Some(Ok(event)))) => event,
        };
        let Some(record) = timeline_event(item, &mut executions)? else {
            continue;
        };
        if record.event_id <= last_id {
            continue;
        }
        last_id = record.event_id;
        let terminal_for_requested = requested_execution.is_some_and(|wanted| {
            record.execution_id.as_deref() == Some(wanted) && is_terminal(&record.event_type)
        });
        events.push(record);
        if terminal_for_requested {
            break "execution_terminal";
        }
    };
    Ok((events, executions, truncated, stop_reason))
}

fn parse_thread_key(value: &str) -> Result<ThreadKey> {
    if value.starts_with("http://") || value.starts_with("https://") {
        // Slack permalinks end in p<timestamp>; the API namespace includes the Slack channel.
        let url = url::Url::parse(value).wrap_err("parse permalink")?;
        let channel = url
            .path_segments()
            .and_then(|mut p| p.next_back())
            .unwrap_or_default();
        let timestamp = channel.strip_prefix('p').unwrap_or_default();
        if timestamp.len() < 7 || !timestamp.bytes().all(|b| b.is_ascii_digit()) {
            bail!("Slack permalink must end with p<TIMESTAMP> and include the channel in the path");
        }
        let channel_id = url
            .path_segments()
            .and_then(|p| p.rev().nth(1))
            .unwrap_or_default();
        if channel_id.is_empty() {
            bail!("Slack permalink does not identify a channel");
        }
        return ThreadKey::from_str(&format!(
            "slack:{channel_id}:{}.{:0<6}",
            &timestamp[..timestamp.len().saturating_sub(6)],
            &timestamp[timestamp.len().saturating_sub(6)..]
        ))
        .wrap_err("convert Slack permalink to session key");
    }
    ThreadKey::from_str(value).wrap_err("invalid session/thread key")
}

fn timeline_event(
    event: SseEvent,
    executions: &mut BTreeMap<String, String>,
) -> Result<Option<TimelineEvent>> {
    let Ok(event_id) = event.id.parse::<i64>() else {
        return Ok(None);
    };
    let event_type = event.event;
    let raw_data = event.data;
    let data = serde_json::from_str::<Value>(&raw_data).unwrap_or(Value::String(raw_data));
    let execution_id = data
        .get("execution_id")
        .and_then(Value::as_str)
        .map(ToOwned::to_owned);
    let id = execution_id.clone().or_else(|| {
        executions
            .iter()
            .find(|(_, status)| *status == "running")
            .map(|(id, _)| id.clone())
    });
    if let Some(execution_id) = &execution_id {
        match event_type.as_str() {
            "session.execution_started" => {
                executions.insert(execution_id.clone(), "running".into());
            }
            "session.execution_completed" => {
                executions.insert(execution_id.clone(), "completed".into());
            }
            "session.execution_failed" => {
                executions.insert(execution_id.clone(), "failed".into());
            }
            "session.execution_cancelled" => {
                executions.insert(execution_id.clone(), "cancelled".into());
            }
            _ => {}
        }
    }
    let activity = activity_label(&event_type, &data);
    Ok(Some(TimelineEvent {
        event_id,
        execution_id: id,
        event_type,
        activity,
        data,
    }))
}

fn activity_label(event_type: &str, data: &Value) -> String {
    match event_type {
        "session.execution_started" => "execution started".into(),
        "session.execution_completed" => "execution completed".into(),
        "session.execution_failed" => "execution failed".into(),
        "session.execution_cancelled" => "execution cancelled".into(),
        "session.stream_error" => "event stream error".into(),
        "session.output.line" => data
            .get("type")
            .and_then(Value::as_str)
            .map(|kind| format!("harness output: {kind}"))
            .unwrap_or_else(|| "harness output line".into()),
        _ => event_type.into(),
    }
}

fn is_terminal(event_type: &str) -> bool {
    matches!(
        event_type,
        "session.execution_completed" | "session.execution_failed" | "session.execution_cancelled"
    )
}

fn render_human(inspection: &Inspection) {
    println!("Session: {}", inspection.session.thread_key.as_str());
    println!(
        "Platform: {}{}",
        inspection.session.platform,
        inspection
            .session
            .title
            .as_ref()
            .map(|title| format!(" — {title}"))
            .unwrap_or_default()
    );
    println!(
        "Execution status: {}",
        if inspection.executions.is_empty() {
            "unknown".into()
        } else {
            inspection
                .executions
                .iter()
                .map(|(id, status)| format!("{id}={status}"))
                .collect::<Vec<_>>()
                .join(", ")
        }
    );
    println!("Overlay/skill/prompt provenance: {}", inspection.provenance);
    for event in &inspection.events {
        println!(
            "\n#{} {} [{}]",
            event.event_id,
            event.activity,
            event.execution_id.as_deref().unwrap_or("execution unknown")
        );
        println!(
            "{}",
            serde_json::to_string_pretty(&event.data).unwrap_or_else(|_| event.data.to_string())
        );
    }
    println!(
        "\nRead {} event(s); stopped at {}{}.",
        inspection.events.len(),
        inspection.stop_reason,
        if inspection.truncated {
            " (partial result)"
        } else {
            ""
        }
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::stream;

    fn event(id: &str, kind: &str, data: &str) -> SseEvent {
        SseEvent {
            id: id.into(),
            event: kind.into(),
            data: data.into(),
            ..Default::default()
        }
    }

    #[test]
    fn groups_recorded_activity_and_failure_by_execution() {
        let mut statuses = BTreeMap::new();
        let started = timeline_event(
            event(
                "1",
                "session.execution_started",
                r#"{"execution_id":"run-1"}"#,
            ),
            &mut statuses,
        )
        .unwrap()
        .unwrap();
        let output = timeline_event(
            event(
                "2",
                "session.output.line",
                r#"{"type":"tool_call","name":"pytest","command":"pytest -q"}"#,
            ),
            &mut statuses,
        )
        .unwrap()
        .unwrap();
        let failed = timeline_event(
            event(
                "3",
                "session.execution_failed",
                r#"{"execution_id":"run-1","error":"tests failed"}"#,
            ),
            &mut statuses,
        )
        .unwrap()
        .unwrap();
        assert_eq!(started.execution_id.as_deref(), Some("run-1"));
        assert_eq!(output.activity, "harness output: tool_call");
        assert_eq!(output.execution_id.as_deref(), Some("run-1"));
        assert_eq!(failed.data["error"], "tests failed");
        assert_eq!(statuses.get("run-1").map(String::as_str), Some("failed"));
    }

    #[test]
    fn replay_cursor_and_terminal_filter_are_well_defined() {
        let mut statuses = BTreeMap::new();
        let first = timeline_event(
            event(
                "7",
                "session.execution_completed",
                r#"{"execution_id":"run-2"}"#,
            ),
            &mut statuses,
        )
        .unwrap()
        .unwrap();
        assert!(first.event_id > 6);
        assert!(is_terminal(&first.event_type));
        assert!(is_terminal("session.execution_cancelled"));
        assert!(!is_terminal("session.output.line"));
    }

    #[test]
    fn bounds_reject_unbounded_stream_settings() {
        assert!(bounded_idle_ms("30001").is_err());
        assert!(bounded_max_seconds("301").is_err());
        assert_eq!(bounded_max_seconds("30").unwrap(), 30);
    }

    #[test]
    fn slack_permalink_maps_to_the_supported_session_key_shape() {
        let thread_key =
            parse_thread_key("https://slack.example/archives/C123ABC/p1730000000123456").unwrap();
        assert_eq!(thread_key.as_str(), "slack:C123ABC:1730000000.123456");
    }

    #[tokio::test]
    async fn replay_discards_duplicate_ids_and_honors_the_cursor() {
        let mut events = stream::iter(vec![
            Ok(event(
                "7",
                "session.execution_completed",
                r#"{"execution_id":"run-2"}"#,
            )),
            Ok(event(
                "8",
                "session.execution_failed",
                r#"{"execution_id":"run-3"}"#,
            )),
            Ok(event(
                "8",
                "session.execution_failed",
                r#"{"execution_id":"run-3"}"#,
            )),
        ]);
        let (records, _, _, reason) = collect_events(
            &mut events,
            7,
            None,
            Duration::from_millis(100),
            Duration::from_secs(1),
        )
        .await
        .unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].event_id, 8);
        assert_eq!(reason, "stream_closed");
    }

    #[tokio::test]
    async fn stream_completion_is_bounded_by_idle_and_hard_deadlines() {
        let mut idle_stream = stream::pending::<std::result::Result<SseEvent, ClientError>>();
        let (records, _, truncated, reason) = collect_events(
            &mut idle_stream,
            0,
            None,
            Duration::from_millis(5),
            Duration::from_secs(1),
        )
        .await
        .unwrap();
        assert!(records.is_empty());
        assert!(!truncated);
        assert_eq!(reason, "idle_timeout");

        let mut deadline_stream = stream::pending::<std::result::Result<SseEvent, ClientError>>();
        let (records, _, truncated, reason) = collect_events(
            &mut deadline_stream,
            0,
            None,
            Duration::from_secs(1),
            Duration::from_millis(5),
        )
        .await
        .unwrap();
        assert!(records.is_empty());
        assert!(truncated);
        assert_eq!(reason, "max_duration");
    }
}
