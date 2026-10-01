# Qiankun Downstream Patch Queue

The `v10.4.0` baseline carries the following six functional patches:

### QK-001: deterministic FSM apply gate for failure tests

- Purpose: pause a selected Raft FSM application after consensus commit but
  before SQLite apply, so Qiankun can verify result-unknown and idempotent
  reconciliation semantics without timing sleeps.
- Affected upstream symbols: unexported `Store.testApplyGate` and
  `Store.fsmApply`; no public API, command, storage, wire, or snapshot format is
  changed.
- Benefit: adds deterministic coverage for the highest-risk HA write ambiguity
  window. Production assembly cannot configure the gate; the nil path adds only
  two predictable checks around FSM command processing.
- Verification: `go test -count=10 -run Test_QiankunCommitBeforeApplyLeaderLoss ./store`
  and `go test -race -run Test_QiankunCommitBeforeApplyLeaderLoss ./store`.
  The v10.4.0 rebase also runs `Test_QiankunFatalWriteProcessRecovery`: real
  SQLite capacity failure must exit a child process, keep bound values out of
  logs, and replay the committed transaction when the process reopens without
  that node-local capacity constraint. This adds tests, not a production hook.
- Synchronization risk: low and localized to `Store.fsmApply`. Remove when
  upstream provides an equivalent deterministic FSM apply test seam or the
  Qiankun result-unknown contract no longer needs downstream validation.

### QK-002: structured SQLite statement error codes

- Purpose: carry SQLite primary and extended result codes beside the existing
  statement error text so an embedded Store adapter can classify constraint,
  busy, locked, and other failures without parsing human-readable messages.
- Affected upstream symbols: additive protobuf fields on `QueryRows` (6/7) and
  `ExecuteQueryResponse` (5/6, after upstream `mutated=4`); DB result construction records
  codes only when the underlying error is a `sqlite3.Error`. Existing error
  strings and oneof response shapes remain unchanged.
- Benefit: gives Qiankun a stable machine-readable error boundary while
  preserving upstream clients that only consume error text. The additive wire
  fields do not change SQLite, Raft, snapshot, or request behavior.
- Verification: DB tests cover constraint and query error primary/extended codes;
  protobuf mutation/result/error round trips, encoding tests, Store tests, and
  race tests must pass. Preserve upstream transaction abort and FatalError
  classification; structured codes do not turn a fatal write into a statement error.
- Synchronization risk: low but wire-visible. Remove when upstream exposes an
  equivalent structured statement error or Qiankun no longer embeds the
  upstream command protocol.

### QK-003: connection-bound bootstrap and join authorization

- Purpose: let an embedded deployment bind the authenticated transport peer to
  the node ID and advertised address carried by `NotifyRequest` and
  `JoinRequest`, closing the gap where any certificate from a shared CA could
  claim another member's identity in the cluster-management payload.
- Affected upstream symbols: additive `cluster.NodeRequestAuthorizer`,
  `Service.SetNodeRequestAuthorizer`, and `tcp.trackedConn.Unwrap`. The
  authorizer is optional, is called only for Notify and Join after credential
  authorization, and returns the existing `unauthorized` response on failure.
  No protobuf, Raft, SQLite, snapshot, or persisted format changes.
- Benefit: Qiankun can verify cluster/node/protocol URI SAN identity and the
  advertised Raft address against the exact mTLS connection before Store
  membership mutation. The nil path preserves upstream behavior.
- Verification: cluster service tests cover accepted and rejected Notify/Join,
  exact request projection, manager non-invocation on rejection, and setter
  lifecycle; TCP mux tests cover access to the wrapped connection. Run
  `go test -count=10 ./cluster ./tcp` and `go test -race ./cluster ./tcp`.
  The upstream local-request counter test compares the per-test increment so
  repeated runs do not assume a fresh process-global metric; its service is closed.
- Synchronization risk: low and localized to cluster-management dispatch and a
  wrapper accessor. Remove when upstream offers equivalent connection-bound
  node authorization or Qiankun no longer embeds the cluster service.

### QK-004: cluster client lifecycle closure

- Purpose: let an embedded node deterministically close all idle cluster-client
  connections during shutdown and prevent new remote requests after closure.
- Affected upstream symbols: additive `cluster.Client.Close`, exported
  `cluster.ErrClientClosed`, and one lifecycle flag checked while resolving
  connection pools. No wire, Raft, SQLite, snapshot, or persisted format changes.
- Benefit: prevents pooled mTLS connections from surviving a Qiankun node
  shutdown or in-process restart and gives the embedding layer one complete
  ownership boundary for transport resources.
- Verification: cluster client tests cover pooled use, idempotent close, and
  fail-closed reuse. Run `go test -count=10 ./cluster` and
  `go test -race ./cluster`.
- Synchronization risk: low and localized to client pool ownership. Remove when
  upstream exposes an equivalent client lifecycle method or Qiankun no longer
  embeds the cluster client.

### QK-005: application node eligibility metadata

- Purpose: let an embedded deployment publish short-lived node-local runtime
  eligibility facts over the existing mTLS cluster service without persisting
  seal state or introducing a second control protocol.
- Affected upstream symbols: additive `NodeMeta` protobuf fields,
  `cluster.NodeMetaProvider`, `Service.SetNodeMetaProvider`, and
  `Service.GetNodeMeta`. The provider is optional and its values are cloned;
  Service-owned URL, version, and commit index remain authoritative. The local
  client fast path now uses the same Service projection as remote requests.
- Benefit: Qiankun can observe member identity, runtime mode, sealed/ready
  state, applied index, and migration checksum through the already authenticated
  node channel. No RootKey, share, credential, Raft state, snapshot, or database
  format is added or persisted.
- Verification: cluster service tests cover remote and local projection,
  authoritative field precedence, detached results, nil/default behavior, and
  setter lifecycle. Run `go test -count=10 ./cluster` and
  `go test -race ./cluster`.
- Synchronization risk: low but wire-visible through additive protobuf fields.
  Remove when upstream exposes an equivalent application metadata extension or
  Qiankun no longer embeds the cluster service.

### QK-006: bounded reusable read-only SQLite connections

- Purpose: keep the explicitly bounded read-only connection pool reusable under
  concurrent embedded query load instead of retaining the `database/sql`
  default of only two idle connections and repeatedly reopening SQLite.
- Affected upstream symbols: `DB.SetMaxReadOnlyConns` now applies the same
  positive bound to both open and idle connections; `SwappableDB` retains and
  reapplies the bound after a database swap. A zero bound preserves the
  existing unlimited-open/default-idle behavior.
- Benefit: removes SQLite open/close and connection-registration churn proven by
  Qiankun CPU, mutex, and block profiles while retaining the existing 30-second
  idle timeout and an explicit upper resource bound.
- Verification: DB tests hold and return four concurrent read-only connections
  and assert they remain idle; SwappableDB tests assert the configured maximum
  survives swap. Qiankun runs the same fresh three-voter profile and API
  benchmark before and after the patch.
- Synchronization risk: low and localized to connection-pool sizing. Remove when
  upstream retains a caller-configurable number of idle read-only connections
  or provides an equivalent separate idle-pool setting.

The following files are downstream governance and reproducibility assets, not
runtime feature patches:

- `UPSTREAM.md`
- `PATCHES.md`
- `QIANKUN_BASELINE.md`
- `tools/qiankun-baseline.sh`
- `tools/qiankun-baseline/main.go`
- `.gitignore` entry for local baseline artifacts

## Patch acceptance rule

Every future functional patch must be listed here with:

1. patch identifier and purpose;
2. upstream symbols and dependencies affected;
3. measurable binary-size, build-time, dependency, attack-surface, or runtime
   benefit;
4. verification commands and failure-injection coverage;
5. upstream synchronization risk and removal criteria.

The exclusion order is fixed: Qiankun assembly/import boundaries first, build
tags second, and small source patches last. Source deletion without a measured
benefit is not accepted.

## Initial capability disposition

- Keep for the isolated PoC: Store, Raft/FSM, database layer, command/protobuf,
  snapshot/restore, node transport, leader observation, and cluster client.
- Exclude by Qiankun assembly: upstream public HTTP database API, CLI, console,
  discovery, auto-backup scheduler, OTLP export, CDC endpoint, and cloud SDKs.
- Defer: source-level removal and SQLite static-library prebuild. Both require a
  measured unmodified baseline before a decision.
