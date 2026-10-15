# Inspect a recorded session

`centaur-session-inspect` is a local, read-only view over the durable session
read API. It fetches session context, then replays the authorized SSE event
stream from the requested event cursor. It does not need an OTel collector or
transcript capture.

Configure the API endpoint and a bearer token through the environment. The
identity represented by the token must have `SessionsRead` and must be allowed
to read the requested session. API authorization and principal ownership checks
remain in force.

```sh
export CENTAUR_API_URL=http://127.0.0.1:8080
read -s CENTAUR_API_TOKEN
export CENTAUR_API_TOKEN
cargo run --manifest-path services/api-rs/Cargo.toml -p centaur-session-cli \
  --bin centaur-session-inspect -- slack:CHANNEL_ID:THREAD_TIMESTAMP
```

A Slack permalink such as `https://slack.example/archives/CHANNEL_ID/p1730000000123456`
can be used in place of the thread key. Use `--execution-id ID` to narrow the
stream to one execution. `--after-event-id ID` resumes after a durable event.
The default stops after 1.5 seconds without events, and `--max-seconds` provides
a hard deadline (maximum 300 seconds). Fetches also stop after 5,000 events.

Use `--json` for JSON on stdout, or `--output session.json` to save a JSON
export. `--help` lists the available options.

Events are displayed in durable event-ID order. Output lines contain only what
the harness actually recorded, which can include assistant/tool activity,
commands, results, errors, or test output depending on the harness and run.
The read API does not expose event timestamps or the complete persisted message
and execution tables. An execution status is reported only when a corresponding
start/terminal event is present in the replay; otherwise it is unknown. Overlay,
skill, and prompt provenance are not exposed by this API and are explicitly
reported as unknown. A partial result identifies whether it stopped at the
idle timeout, hard deadline, or event limit.
