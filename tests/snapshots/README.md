# Runner Snapshot Tests

Snapshot tests live under `tests/snapshots/`:

- `inputs/` holds the test inputs used to drive the runner.
- `expected/` stores the golden snapshots (insta `.snap` files).

## Updating snapshots

To update snapshots locally:

```bash
INSTA_UPDATE=always cargo test -p greentic-runner snapshots
```

Alternatively, use the CLI flag to accept updates:

```bash
cargo test -p greentic-runner snapshots -- --accept
```

Snapshots must **not** auto-update in CI.

## Debugging failures

When a snapshot test fails, the harness writes artifacts to:

```
target/snapshot-artifacts/<test>/
```

Each failure directory contains:
- `input.json`
- `actual.json`
- `expected.json`
