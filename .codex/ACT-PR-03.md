# ACT-PR-03 — greentic-runner: Invocation Envelope Validation (warn→error)

## Goal
Add schema-first guardrails at boundaries:
- Validate invocation envelopes going into components and tools.
- Start in warn mode, later flip to error.

## Scope
### Add
- `--validation=off|warn|error` (default: `warn` in CI nightly, `off` locally initially; choose conservative default)
- A shared envelope schema: `InvocationEnvelope v1`
- Validate:
  - required top-level keys if your contract demands them (or allow partial but validate types)
  - metadata fields (`tenant_id`, `trace_id`, `session.id`) where present

### Diagnostics
- Structured validation issue format:
  - `code` (stable)
  - `path` (json pointer)
  - `message`
- Include issues in trace.json and console output.

## Implementation details
- Create a small crate `greentic-validate` or add internal module (depending on repo boundaries).
- Use a Rust JSON Schema validator compatible with Draft-07 or your chosen schema version.

## Acceptance criteria
- Invalid envelopes are reported consistently.
- In `warn` mode, execution continues but logs issues.
- In `error` mode, execution stops with a stable error code.

## Test plan
- Unit tests with known-good and known-bad envelopes.
- Integration test that sends malformed invocation to `component-adaptive-card` and asserts issues.

## Notes
Start with envelope schema only. Per-component schemas come later.
