0) Global rule for all repos (tell Codex this every time)

Use this paragraph at the top of every prompt:

Global policy: greentic:component@0.6.0 WIT must have a single source of truth in greentic-interfaces. No other repo should define or vendor package greentic:component@0.6.0 or world component-v0-v6-v0 in its own wit/ directory. Repos may keep tiny repo-specific worlds (e.g. messaging-provider-teams) but must depend on the canonical greentic component WIT via deps/ pointing at greentic-interfaces or via a published crate path, never by copying the WIT file contents.

F) greentic-runner repo prompt (tests use canonical, delete dummy copies if redundant)
You are working in the greentic-runner repository.

Goal
- Remove redundant local copies of canonical `greentic:component@0.6.0` WIT used only for tests (e.g. component-v0-6-dummy) and switch tests to consume canonical WIT from greentic-interfaces.
- Where tests need an exported v0.6 component, use `greentic_interfaces_guest::export_component_v060!` to generate deterministic fixtures.

Work
- Identify test assets under `tests/assets/component-v0-6-dummy/wit/component-v0-6.wit` and similar.
- Replace them with:
  a) a fixture crate that uses greentic-interfaces-guest wrapper macro, OR
  b) WIT deps pointing to greentic-interfaces canonical WIT.
- Add a guard test to prevent adding canonical component WIT files here in the future.

Deliverables
- Runner tests no longer carry local canonical component WIT duplicates.
- Guard added.

Now implement it.