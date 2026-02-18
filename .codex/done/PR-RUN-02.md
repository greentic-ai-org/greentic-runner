# PR-RUN-02 — greentic-runner: i18n-aware diagnostics + contract artifact support + CI fixtures

Repo: `greentic-runner`

## Goal
Polish runner reliability and dev experience:
- Localize runner diagnostics using i18n keys where available.
- Allow emitting/consuming `describe.cbor` artifacts for debugging (WASM remains authoritative).
- Add no-network fixtures and CI coverage.

## Decisions locked (2026-02-11)
- Target ABI: **greentic:component@0.6.0** world `component-v0-v6-v0`.
- Contract authority: **WASM `describe()`** is source of truth (operations + inline SchemaIR + config_schema).
- Validation: strict by default (no silent accept). Any escape hatches must be explicit flags.
- Encodings: CBOR everywhere; use canonical CBOR encoding for stable hashing and deterministic artifacts.
- Hashes:
  - `describe_hash = sha256(canonical_cbor(typed_describe))`
  - `schema_hash = sha256(canonical_cbor({input, output, config}))` recomputed from typed SchemaIR values.
- i18n: `component-i18n.i18n-keys()` required for 0.6.0 components; QA specs must reference only known keys.
- Artifact trust model:
  - allow `--describe <path>` / `--contract <path>` for debug/test-only offline flows
  - for real execution, WASM `describe()` remains authoritative
  - artifacts are inspection/debug snapshots, not a bypass for WASM contract truth
- Artifact verification model:
  - default: do not trust artifact for execution paths
  - optional unverified use is allowed only in explicit non-execution flows (e.g., validate-only) and with `--no-verify`
- Diagnostics v1 stable shape:
  - required: `code`, `path`, `severity`, `message_key`, `fallback`, `message`
  - optional: `hint`, `component_id`, `digest`, `operation_id`
- Locale precedence: CLI `--locale` > `GREENTIC_LOCALE` > system locale (if available) > `en`.
- CI scope: default PR CI remains lightweight; heavy WASM execution tests run in a separate opt-in/nightly job.


## Scope
### In-scope
- Diagnostics:
  - represent user-facing messages as `I18nText` (key + fallback)
  - integrate greentic-i18n resolver for CLI/host contexts
- Artifact support:
  - optional `--emit-describe dist/<digest>.describe.cbor` for debugging
  - optional `--print-contract` (stable JSON to stdout)
  - optional `--describe <path>` / `--contract <path>` for offline debug/test flows
- Fixtures:
  - add `tests/fixtures/` containing describe payloads and sample inputs
  - mock runner harness to avoid network and heavy toolchains in default CI
  - separate heavy WASM execution fixtures in optional opt-in/nightly CI job

## Implementation tasks
1) Diagnostics localization
- Extend `Diagnostic` to stable v1 fields:
  - required: `code`, `path`, `severity`, `message_key`, `fallback`, resolved `message`
  - optional: `hint`, `component_id`, `digest`, `operation_id`
- Add locale selection with strict precedence: `--locale` > `GREENTIC_LOCALE` > system locale > `en`.

2) Contract artifact helper
- Add helper to write the typed describe payload as canonical CBOR for inspection.
- Add optional artifact input (`--describe` / `--contract`) for debug/test offline flows.
- Ensure execution paths never trust artifact over WASM `describe()`.
- Allow unverified artifact usage only in explicit non-execution paths and only with `--no-verify`.

3) Tests/CI
- Add fixture tests verifying:
  - invalid inputs produce stable codes
  - localization fallback chain works
- CI default job runs offline tests only (mock-backed + contract bytes + validator tests).
- Add separate opt-in/nightly job for heavy WASM execution fixtures.

## Acceptance criteria
- Diagnostics can be localized deterministically.
- Diagnostics expose the stable v1 field shape for tooling.
- Contract artifacts can be emitted and consumed for debug/test workflows without bypassing WASM authority in execution.
- Fixtures run in CI with no network.
- Default PR CI stays fast/deterministic; heavy WASM fixtures are isolated to optional CI.
- `cargo test` passes.
