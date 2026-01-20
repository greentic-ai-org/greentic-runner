# Pack Resolution Property Tests

This document describes the property-based tests for pack resolution.

## Running the tests

Default (128 cases):

```bash
cargo test -p greentic-runner-host pack_resolution_proptest
```

Override the number of cases:

```bash
PROPTEST_CASES=256 cargo test -p greentic-runner-host pack_resolution_proptest
```

Reproduce a specific seed:

```bash
PROPTEST_SEED=1337 cargo test -p greentic-runner-host pack_resolution_proptest
```

## Regression seeds

Regression seeds live in:

`tests/fixtures/proptest-seeds.txt`

To add a new seed, append the integer seed on its own line. The
`pack_resolution_regression_seeds` test reads this file and runs a
single-case test for each seed.
