# ACT-PR-02 — greentic-runner: `replay` Subcommand for trace.json

## Goal
Turn “hard to reproduce” into “minutes to reproduce” by adding:
- `greentic-runner replay <trace.json>`

## Scope
### Add
- CLI subcommand `replay` that:
  - loads the trace
  - re-runs the same pack + flow
  - optionally stops at the failing step
  - prints step-by-step results and highlights divergences

### Replay modes
- `--until-failure` (default)
- `--step <n>` to run a specific step
- `--compare-hashes` (default on): compare input/output hashes to trace

## Implementation details
### What to capture for true replay
To replay deterministically, ensure trace includes:
- resolved pack digest (or local pack path)
- flow version or full flow snapshot hash
- per step:
  - the exact invocation envelope (preferred)
  - or a pointer to captured inputs (artifact path) if too large

If step inputs are not currently stored, add an option:
- `--trace-capture-inputs=on|off` (default off in PRs, on in nightly)

### Files
- `src/cli/replay.rs`
- updates to trace model to optionally include `invocation_json` or `invocation_path`.

## Acceptance criteria
- Given a trace from a failing CI run, `replay` reproduces failure locally.
- If output differs, runner prints which step diverged and how.

## Test plan
- Add a small fixture pack/flow that fails deterministically.
- Run, collect trace, then replay and assert same failure code.

## Notes
This PR may require small trace model extension; keep backwards compatibility by allowing missing optional fields.
