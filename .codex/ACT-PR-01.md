# ACT-PR-01 — greentic-runner: Trace Recorder (Phase 0) + CI Artifacts Support

## Goal
Make failures replayable and visible:
- On any failure, emit a `trace.json` with the key execution details.
- Support `--trace-out` to control destination.

## Scope
### Add
- `TraceRecorder` (internal module or shared crate) that records:
  - pack reference (tag/digest/path) + resolved hash
  - flow id + version
  - per node step:
    - node_id
    - component_id + operation
    - input_hash
    - output_hash (if any)
    - state_delta_hash (if any)
    - duration_ms
    - error block (code, message, details)
- `--trace-out <path>` flag
- `GREENTIC_TRACE_OUT=<path>` env override (useful for CI/harness)
- `--trace=off|on|always` (default `on` meaning “emit on failure”) 

### Trace format
Create a stable schema (v1) and include versioning:
- `trace_version: 1`
- `runner_version` and `git_sha` optional

## Implementation details
### Files
- `src/trace/mod.rs`
- `src/trace/model.rs`
- `src/trace/recorder.rs`

### Hashing
- Use a fast stable hash (sha256 is fine; blake3 is faster). Include algorithm field.

### When to flush
- Always keep a rolling buffer of last N successful steps (default 20).
- On error:
  - include buffered steps
  - write trace atomically (write temp then rename)

## Acceptance criteria
- Any failing run writes trace.json.
- Trace contains at least: pack ref, flow id, list of steps, and one error.
- CI can set `GREENTIC_TRACE_OUT=$GITHUB_WORKSPACE/artifacts/trace.json`.

## Test plan
- Unit test trace serialization.
- Integration test with a deliberately failing component to ensure trace is produced.

## Notes
Replay is the next PR.
