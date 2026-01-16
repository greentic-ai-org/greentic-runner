# Component Payload And State In Greentic Runner

This document explains how the runner handles payload templating and persistent state for components.

## Payload Inputs For Components

The runner renders node input configuration using Handlebars before invoking a component. This is the
official way to wire data between nodes.

### Template Context

The render context includes:
- `entry`: the initial flow input payload (immutable, `{}` if none).
- `prev`: the previous node output payload (or `{}` for the first node).
- `node`: map of executed node outputs keyed by node id (example: `node.start`).
- `state`: runner-local execution view (entry/input/nodes with `ok`/`payload`/`meta`).

### Typed Insertion Rule

To preserve JSON types:
- If a scalar is exactly `{{expr}}`, the expression is evaluated and the value is inserted as JSON.
- If the scalar contains other text, it is rendered as a string.

Examples:
```
user_id: {{node.start.user.id}}      # inserts number/bool/object
url: "https://x/{{entry.user_id}}"   # remains string
```

### What Is Not Included

Persistent state is not injected into input JSON. Use the state store interface instead.

## Persistent State Access

Components read/write/delete persistent state through the WIT interface:
`greentic:state/store@1.0.0`.

Operations:
- `state.read(key, ctx)`
- `state.write(key, bytes, ctx)`
- `state.delete(key, ctx)`

### Tenant Context

`ctx` is optional. If omitted, the host fills tenant/env and flow/node/session metadata from the
current execution context. If provided, it can override those fields.

### Capability Gating

The state store is linked only when:
- the component manifest declares state capability, and
- the host has a state store configured, and
- bindings allow it (`state_store.allow`).

If any condition fails, the interface is not linked and calls will fail at instantiation.

## Recommended Patterns

1) Use templating (`entry`/`prev`/`node`) for wiring data between nodes.
2) Use `greentic:state/store@1.0.0` for persistent state.
3) Do not rely on internal snapshot keys or legacy KV interfaces.
