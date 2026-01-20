# ACT-PR-04 — greentic-runner: Fault Injection Decorators (State + Asset) for Nightly Chaos

## Goal
Support deterministic chaos testing by injecting faults into:
- state-store operations
- asset resolution operations

## Scope
### Add
- Decorator/wrapper around state store client:
  - drop writes
  - delay reads
  - optional stale reads
- Decorator/wrapper around asset resolver:
  - transient failures at a rate (n/m)
  - delay

### Configuration (env)
Consume env vars set by integration tester:
- `GREENTIC_FAIL_DROP_STATE_WRITE=1`
- `GREENTIC_FAIL_DELAY_STATE_READ_MS=250`
- `GREENTIC_FAIL_ASSET_TRANSIENT=1/10`
- `GREENTIC_FAIL_SEED=<seed>`

### Determinism
- Use seed + step index to ensure repeatability.

## Implementation details
- Add `src/fault/mod.rs` with helpers.
- Wrap state-store implementation in runner init.
- Wrap asset resolution path (wherever adaptive-card assets are loaded/resolved).

## Acceptance criteria
- With injection enabled, scenarios fail in predictable ways.
- With the same seed, failures occur at the same steps.

## Test plan
- Add an integration test that runs a scenario with fixed seed and asserts same failure step.

## Notes
Keep injection strictly opt-in; never enabled by default.
