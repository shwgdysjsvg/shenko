# Native managed-node journey / 原生受管节点旅程

## Purpose and limits / 用途与边界

`shenko-node` is an optional Rust 2024 installation-control adapter. It is not
linked into `bin/shenko`, not an ElectionProvider, not a replacement subject,
and not a model-facing tool. Its only handler is `node.blake3.v1`: <=64KiB bytes
to a deterministic digest, with no external effect. This deliberately makes
unknown computation transferable without pretending arbitrary effects are safe.

这是 C17 首条有限旅程，不是完整 A42/A47/A48/A49。SSH 使用已合入 C07 原生
OpenSSH/receipt API，不冒称 WASM。节点独立目录、身份和进程不等于物理两机容灾。
没有已核验模型 profile 时 capability 返回 `not_configured_or_probed` 和
`smart_node_evidence=false`，不以节点名赋予智能资格。

## Authority / 权限来源

The control CLI requires a private OS account/data directory or its already
authorized strict-host-key SSH channel. `register`, `join`, `create`, `transfer`
and `accept` are trusted installation/controller operations. Do not expose their
JSON directly to chat/model arguments or an unauthenticated network endpoint.
`from_node` must be supplied by that authenticated transport, never a peer's
unverified message. The eventual registry/IPC adapter must derive invocation
and source identity itself; it is not implemented here.

`Identity.origin_proof` references an **already verified** earlier self supplied
by provisioning. It does not discover trust or prove age by itself. Prepare
checks that this identity equals the actual controller store. First adoption
archives a virgin independent identity; an already joined/evolved branch is
rejected for explicit merge, never silently erased. No competing subject loop
or daemon is started. Provider settings, private keys, model weights and secret
bytes are absent from the node synchronization schema.

## Build and inspect / 构建与确认实际工件

Use a clean product commit and the shared build budget in the managed container.
For a standalone caller, the equivalent native command is

```sh
SHENKO_SOURCE_REVISION=$(git rev-parse HEAD) cargo build -p shenko-node
/path/to/shenko-node --manifest
```

Record the actual source SHA used by the build. `--manifest` binds the running
executable's blake3 digest, compile-time source, OS, architecture and schema 1.
If stripping/copying it, inspect the final executable again. Unbound source or
mismatched bytes/platform/schema cannot join. This is a builder provenance
contract, not remote attestation or a substitute for verifying the source tree.

## Local control wire / 本地控制协议

`shenko-node PRIVATE_DATA_DIR` reads one <=1MiB JSON request on stdin and returns
one JSON response after committed work. Existing unrelated data is rejected;
never use a live main-app directory. Storage schema **2** is separate from
record/wire schema **1**. Existing databases first undergo an immutable/read-only
application/schema/installation preflight: even WAL sidecar creation or journal
recovery is not allowed for an unknown database. Then the writable open rechecks
ownership before persistent PRAGMAs. New files use exclusive creation and 0600;
unrelated directory entries and symlinks are rejected.

The only unmarked legacy format eligible for migration is the exact original v1
schema (all non-SQLite schema objects) with valid durable node/ledger/identity
metadata. Its rows, node/self IDs, ledger and seq remain unchanged. One migration
transaction adds the installation UUID, SHNK application_id, receipts table and
storage version 2. This is a trusted local format migration, not cryptographic
proof of remote ownership. Unknown, corrupt or incomplete uninitialized databases
fail closed; this adapter does not overwrite them or repair arbitrary data. New counters on the wire are canonical
unsigned decimal strings. Node UUIDs, agent/branch UUIDs and task UUIDs are distinct.

- `initialize`: stable `node_id`, optional provisioning `identity`; reopen never
  creates or resets an identity. `join`: stable `operation_id`, `expected_node`,
  verified `identity` and actual `manifest`; payload mismatch is a conflict.
- `status`: optional join `operation_id`; returns the old immutable join receipt
  separately from a fresh `running_manifest`, observation time and capabilities.
- `register`: immutable `node_id`, `target_id`, public-trust/binding digest.
- `create`: stable `task_id`, registered `node_id`, bounded `input` byte array;
  returns the same ticket on duplicate input and rejects conflicting creation.
- `execute`: `ticket` and exact `input`; arm before compute, then persist one
  original result/event. Duplicate tickets query; an armed-without-result record
  stays unknown. `query`: original `task_id` and string `epoch`, no redispatch.
- `transfer`: `task_id`, expected string record revision/epoch and registered
  `to_node`; this pure contract alone allows explicit reassignment. It increments
  execution epoch and semantic/record revisions without changing TaskRecord owner.
- `accept`: authenticated `from_node` and original `receipt`. Import stable
  source/event/digest facts, but only the current ticket may create one terminal
  result. Late/stale results remain evidence, not new authority. For authenticated,
  known-schema receipts, E1 commits the full decoded receipt and source event
  **before** task lookup or settlement predicates. Missing-task receipts become
  `unassociated`; transcript/contract/predicate rejections become `invalid`.
  A failed T2 cannot roll back E1; pending receipts remain available for explicit
  reconciliation. Authentication/schema/digest failures are still rejected before
  E1 by this typed adapter; it is not the complete unknown-schema EvidenceIngress.
- `export`: string `after` cursor and `limit` 1..128; native-journey raw events,
  independent of any LLM. `export_receipts` has its own cursor and returns full
  canonical decoded receipts, authenticated provenance and current classification,
  including unassociated/invalid evidence. Its cursor enumerates receipt arrival,
  **not** subsequent classification changes; it is not a resumable status watch.
  Neither command is the complete main-app history exporter. Explicit reaccept
  of the same receipt may reconcile after the task arrives; duplicates never
  grant an old invocation control or settle a terminal task again.

Objects are bounded and stored in the **same SQLite transaction** as references,
so this adapter has no separate filesystem CAS publication window. Assignments,
original work receipts, source events and conflict observations are retained.
The experimental profile uses DELETE journal and read-back FULL/foreign_keys;
it claims process-crash continuity only. It does not claim the SPEC default
WAL/power-loss profile or upgrade the baseline SQLite 3.46.0 WAL guarantees.

## SSH deployment / 部署与断线恢复

`scripts/managed_node.py` consumes an **already persisted trusted C07 parent
TaskInvocation**, not a fabricated production admission. Its config names
`artifact`, `controller_binary`, `controller_data`, `node_id`, `remote_root`,
`identity`, `ssh_driver`, and `parent` (the existing `task_ssh` JSON including
target/receipt-root/context). Key **paths/handles** stay in the local private
plan, never put credentials or these live configs in the repository.

```sh
python3 -I scripts/managed_node.py prepare NEW_PRIVATE_PLAN CONFIG_JSON
python3 -I scripts/managed_node.py deploy PRIVATE_PLAN
python3 -I scripts/managed_node.py query PRIVATE_PLAN install
python3 -I scripts/managed_node.py call PRIVATE_PLAN STABLE_LABEL REQUEST_JSON
```

Prepare creates a schema-2 plan freezing the artifact, deployment UUID, installer
(including C07's shared atomic writer), root-creation implementation and
`staging_root=remote_root/staging-<deployment_id>`. Every upload's destination
is therefore part of the frozen C07 input/resource binding. Two plans sharing a
registered target/root cannot overwrite each other's chunks. Existing plan
retries keep their original paths, code, operation IDs and bytes; changed inputs
require a new explicit plan/operation, never a reseal under an old ID.

Each upload/install/join stage has a stable derived operation ID and logical key.
Installation verifies SHA256, writes and syncs private temporary artifact/receipt
bytes, validates the receipt, then atomically publishes with a no-clobber hard
link and syncs the directory. The receipt preparation reuses C07 `write_atomic`;
its replace semantics are confined to a unique private temporary name. A final
identity-bound name is never overwritten. This is native POSIX/Python3 managed
writing, not a generic cross-platform framework or power-loss test. Temporary
and damaged files remain evidence; existence alone is not a durability marker.

A retry queries existing C07 operations; changed inputs conflict. Transport loss
means unknown, not failure/permission to install again. Query the original
plan/label; if its receipt remains unknown, retain that responsibility rather
than generate another plan to bypass uncertainty. Query uses C07's CAS-sealed
original operation/trust, not the current local `known_hosts` file. Removal or an
unrelated edit to that pathname cannot imply that the remote effect never
happened. New admissions still require the original registered trust digest;
CAS trust still strictly rejects a changed real host key, and remote key
revocation still denies authentication. Local pathname removal is not a
revocation API; authenticated revocation/fencing must occur at the real trust or
authorization boundary. A fresh read-only status uses its own observation
operation, not an old cached reply.

### Existing plans and explicit repair

Schema-1 plans remain queryable with their original deployment/operation identity;
this adapter does not silently execute a legacy staging/installer fallback.
Preserve their original reader/source and all raw receipts. Both `deploy` and
`call` reject such a plan before any task read, request write or SSH execution;
the shared non-query admission boundary also covers direct `invoke`/`node_call`.
It never rebuilds an old sealed operation with new code.

A malformed or conflicting final receipt/artifact is **not** durable success and
is not overwritten, renamed away or deleted by installation. Reconcile the old
C07 operation first. If an explicit repair is needed, create a new config from
the original authorized binding, add `repair_of_plan` pointing to the retained
old plan directory, and `prepare NEW_REPAIR_PLAN CONFIG_JSON`. The new plan records
the original deployment ID and exact plan hash, validates the same target/root,
node and self, and allocates distinct staging/artifact/receipt and C07 operation
IDs. Then deploy that new plan. It does not mutate the old seal or pretend the
old unknown operation never occurred. This lineage is plan-local evidence, not
a claim of full production `OperationRecord.compensates` integration.

## Reproduce the two journeys / 两条真实隔离组合场景

Build the existing `task_ssh` example, then run

```sh
python3 -I crates/shenko-node/tests/managed_journey.py \
  REPO NODE_BINARY SSH_DRIVER SSHD LIB_DIR NEW_PRIVATE_ROOT [V1_DATA_DIR]
```

The fixture requires a native Linux container, root account, OpenSSH server/keygen
and Python3. It creates only a new loopback sshd/proxy with generated test keys;
its temporary authorized-key fixture uses `StrictModes no`. This does not alter
production sshd configuration. The test-only C07 `fixture-init` parent and
identity anchor are synthetic and explicitly not main-app integration.

1. Interleave different artifacts A/B on one registered target/root; resume A
   after B stages, and later install B without overwriting A. Actually close SSH
   only after decoding the complete install **and C07** receipts and checking the
   artifact digest. Query the original operation with missing/unrelated local
   trust, while rejecting new admissions and actual remote key revocation/host
   replacement; resume adoption and repeat without another mutating attempt.
   The same installation journey now also refuses an unrelated WAL database
   without changing bytes/directory entries. If V1_DATA_DIR is supplied, SQLite
   backup into a new directory verifies migration preserves every old table row;
   the original fixture is read-only, never migrated in place.
   An old-format plan projection rejects deploy/call with zero fresh attempts,
   queries its original CAS install twice, then explicitly prepares and deploys a
   distinct schema-2 repair recording predecessor ID/hash. Two repair queries
   retain one install attempt; predecessor plan bytes stay unchanged. This checks
   the schema-1 format boundary, not an original old-executable installation.
2. Query the original durable task result, transfer the pure assignment, reject
   old-owner settlement, execute on the new owner and accept once. Preserve both
   raw sources after separate process reopen; never claim old work was stopped.
   The same task's genuine receipt is delivered to an authenticated receiver
   before its task is known, and a contradictory outer receipt is refused at T2;
   both full receipts remain exportable after reopen. These are assertions within
   the two inherited journeys, not new independent test scenarios. The separate
   wrong-computation predicate branch is statically checked; it is not claimed as
   another directly injected runtime fault.

A bounded direct Python process-fault lifecycle also kills receipt serialization,
checks that no partial final was published, preserves damaged/conflicting finals,
and completes a distinct repair identity. This is a publication-primitive check,
not real SSH interruption, C07 redispatch or physical power-loss evidence.
All owned listeners stop on both success/failure and are read back as refused.

No physical machine, actual model inference, election, partition quorum,
production child scheduler, arbitrary resource fencing, image resurrection or
long-run acceptance follows from these two scenarios. See the machine-readable
[evidence](../evidence/c17-native-first-journey-20261025.json) and [C17](../tasks/C17.md)
for exact candidate and remaining obligations.

## Focused review repair / 复核接续

[PR40](https://github.com/cyjin-yl/shenko/pull/40) on main `04c09a4` repairs
installation ownership and E1/T2 separation; final product source `453fff7e754e46af920dc7fcbc46c9321f41d819`.
The [repair evidence](../evidence/c17-ownership-evidence-repair-20261025.json)
binds the actual binary, schema and inherited journeys. Ledger identity/seq
continuity in the C07/main Store remains an independent C01/#6 P1; this adapter's
own durable ledger does not repair it. Full C17 and production integration remain
incomplete.

The focused script recovery continuation is PR [44](https://github.com/cyjin-yl/shenko/pull/44).
Its [source/artifact-bound evidence](../evidence/c17-script-receipts-20261025.json)
separates the real SSH results from direct Python faults and records the immutable
review snapshot; it does not supersede the original remaining C17 obligations.
