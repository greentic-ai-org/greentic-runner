# PR-RUN-01 — greentic-runner: component@0.6.0 execution with contract-driven validation

Repo: `greentic-runner`

## Goal
Make greentic-runner execute **component@0.6.0** reliably:
- Introspect WASM `describe()` (inline SchemaIR) and select operation (`run` default).
- Validate input/config/state/output against SchemaIR.
- Cache contract by digest to avoid repeated introspection.
- Produce structured, deterministic diagnostics on validation failures.

## Decisions locked (2026-02-11)
- Target ABI: **greentic:component@0.6.0** world `component-v0-v6-v0`.
- Contract authority: **WASM `describe()`** is source of truth (operations + inline SchemaIR + config_schema).
- Validation: strict by default (no silent accept). Any escape hatches must be explicit flags.
- Output validation: ON by default; expose `--skip-output-validate` for explicit edge cases.
- `new_state` validation: validate against `config_schema` in v0.6.0 (no separate `state_schema` yet).
- Encodings: CBOR everywhere; use canonical CBOR encoding for stable hashing and deterministic artifacts.
- Hashes:
  - `describe_hash = sha256(canonical_cbor(typed_describe))`
  - `schema_hash = sha256(canonical_cbor({input, output, config}))` recomputed from typed SchemaIR values.
- SchemaIR enforcement:
  - strict mode FAILS if schema includes constraints the runner cannot enforce (including unsupported `regex`/`format`)
  - optional permissive behavior can be added later behind `--permissive-schema`
- Cache policy: true LRU with byte-size cap keyed by `resolved_digest` (target), with temporary fallback to size-capped `HashMap` only if implementation cost blocks initial delivery.
- i18n: `component-i18n.i18n-keys()` required for 0.6.0 components; QA specs must reference only known keys.


## Scope
### In-scope
- Add a 0.6.0 component runner path using wasmtime to call:
  - `component-descriptor.describe()`
  - `component-runtime.run(input, state)`
- SchemaIR validation:
  - input vs op.input.schema
  - state/config vs describe.config_schema
  - output vs op.output.schema (default ON; can be explicitly bypassed with `--skip-output-validate`)
- Contract cache keyed by resolved digest:
  - store typed describe, describe_hash, schema_hashes, i18n keys
  - evict least-recently-used entries by byte budget
- Error model:
  - return `RunnerError` containing `Vec<Diagnostic>` (stable codes and paths)

### Out-of-scope
- Operator / flow orchestration
- Distribution resolution (handled elsewhere; runner accepts a resolved artifact/digest)

## API changes
- Introduce `ResolvedComponent { digest, wasm_bytes, component_id? }` input to runner.
- Add runner options:
  - `operation_id` (default "run")
  - `validate_output: bool` (default true)
  - `strict: bool` (default true)

## Implementation tasks
1) **Contract loader**
- Load wasm component via wasmtime.
- Call `describe()` and decode typed `ComponentDescribe` (greentic-types).
- Compute `describe_hash`.
- Select operation:
  - if `operation_id` provided use it; else choose "run" if present; else error listing ops.
- Verify per-op `schema_hash` by recomputing from typed SchemaIR.

2) **SchemaIR validator**
- Implement (or reuse) validator for strict subset:
  - types/required/additionalProperties/enums/bounds/items
  - regex/format: enforce if supported; in strict mode return hard error for unsupported constraints

3) **Execution flow**
- Decode input CBOR -> `ciborium::Value` (or typed).
- Validate input vs schema.
- Validate state/config vs config_schema.
- Call `run(input, state)` and decode output/new_state.
- Validate output/new_state (output schema + `config_schema` for `new_state` in v0.6.0).

4) **Caching**
- Cache contract by `resolved_digest` using true LRU with byte-size cap (default target 128-512 MB depending on stored artifacts).
- If needed for initial merge, use a temporary size-capped `HashMap` fallback and track LRU as follow-up within the PR scope.
- Invalidate cache only on digest change (digest is immutable).

5) **Tests**
- Unit tests for validator (positive/negative).
- Fixture-based tests:
  - mock component runner interface (or wasm fixture) returning a known describe payload
  - verify runner rejects invalid input with stable diagnostics.

## Acceptance criteria
- Runner can execute a 0.6.0 component with strict validation.
- Output validation is enabled by default and bypassable only via explicit flag.
- Strict mode rejects unsupported schema constraints (no warn-and-ignore behavior).
- Deterministic diagnostics on failure.
- Contract caching reduces repeated describe calls with byte-capped eviction strategy.
- `cargo test` passes.
