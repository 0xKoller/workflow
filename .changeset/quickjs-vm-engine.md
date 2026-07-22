---
'@workflow/core': minor
'workflow': minor
---

Add an experimental QuickJS WASM VM engine for workflow execution, opt-in via `WORKFLOW_VM=quickjs`. It performs the same full event replay as the default `node:vm` engine, enabling platforms without `node:vm` and laying groundwork for VM-memory snapshotting.
