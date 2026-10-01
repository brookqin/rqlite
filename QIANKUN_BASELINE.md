# Qiankun rqlite v10.4.0 Baseline

## Provenance and scope

- Captured: 2026-10-01, Asia/Shanghai.
- Upstream release: `v10.4.0`, commit `8d7678b36b26ab2c352b72c10064297390fa5b0a`.
- Upstream tag: lightweight; GitHub commit verification is `verified=true` / `valid`.
- Local upstream signature verification: unavailable because public key `B5690EEEBB952194` is absent. GPG itself is available.
- Downstream branch: `codex/upgrade-v10.4.0`; six functional patches are documented in `PATCHES.md`.
- Host: `darwin/arm64`, Go `1.26.7`, CGO enabled, SQLite compiler `clang-21.0.0`.
- Driver: `github.com/rqlite/go-sqlite3 v1.51.0`; embedded SQLite `3.53.4`.

The baseline below was measured against unmodified upstream runtime code in an isolated checkout, with only the governance helper copied in and its Backup return-value use adapted. It is separate from validation of the replayed downstream patches.

## Reproducible baseline

`tools/qiankun-baseline.sh` passed and captured:

- SQLite compile options: 49; WAL creation, foreign-key enforcement and backup reopen verified.
- Non-standard package closure: full rqlited 411; Store/DB/command 75.
- Cold build: 262472 ms; warm build: 21496 ms; each binary 50879106 bytes.
- Binary SHA-256: `53767fbbf0ab53868ed7d06ace5f4deffe436ddbf4666db0963075933f44dd90`.
- Version output: `rqlited 10 darwin arm64 go1.26.7 sqlite3.53.4`.
- Focused SQLite and snapshot tests passed.

The build cache and artifacts were on external SSD while other verification was active. These timings are provenance, not a controlled performance comparison with v10.2.7.

## Dependency and license evidence

CycloneDX 1.6 output contains 82 components and 83 dependency entries. Automated license detection has the same two known gaps: `hashicorp/go-metrics v0.5.1` and `v0.7.0`; independent go-licenses output classifies both as MIT. Their module LICENSE files remain the source evidence. Untagged-main-module HEAD URLs and CGO/assembly warnings are not treated as immutable provenance or a complete native-code license inventory.

The embedded Qiankun import closure only adds `db/querylog`; QueryLogger remains unconfigured. Upstream CLI, HTTP API, queue/proxy, discovery, CDC endpoints and OTLP export are not assembled by Qiankun. No source trimming is justified by this closure change alone.

## Downstream verification

The complete unmodified-upstream `go test ./...` also passed in the isolated checkout with the corrected temporary-directory setup. The complete downstream `go test ./...` passed, including Store (402.241s) and system_test (393.001s). `go vet ./...` passed. Core race coverage passed for Store (425.523s), DB (63.964s), cluster (35.700s), TCP and command packages. Cluster/TCP repeated ten times passed; the existing process-global counter assertion now checks each test's increment and closes the service. The deterministic commit-before-apply leader-loss case passed ten repetitions.

Qiankun validation with the candidate module passed its full Go suite, core race suite (including Repository, Runtime, API and the rqlite adapter), vet, real OTLP collector smoke, and RootKey generation 1/2/3/PKI backup restore. Darwin arm64 builds and real processes run; Linux arm64 and amd64 CGO binaries built, Linux arm64 started and served live/setup APIs, and Windows amd64 built with pinned LLVM MinGW 20260616 and passed UCRT PE import validation. Linux amd64 also started and passed its healthcheck under Docker emulation; this is not native amd64 hardware validation. Native Windows runtime was not exercised. Product three-voter/live/browser acceptance and final dependency publication are recorded separately in Qiankun's upgrade report. The new protocol uses upstream `mutated=4` and downstream error fields 5/6 on ExecuteQueryResponse. A protobuf round-trip test covers mutation/results/error codes. A subprocess test covers real SQLITE_FULL process exit, no bound-value logging, and complete replay after the node-local capacity constraint is removed; Execute and Request paths have both passed, including race.

Earlier validation attempts ran out of system-disk space or used an external-SSD test-data directory with high fsync latency. Go 1.26 testing.T.TempDir prioritizes GOTMPDIR, so merely setting TMPDIR was insufficient. Subsequent runs use a test-exec wrapper to reset both variables to /tmp for the test process while keeping compiler output/cache on SSD. Concurrent unmodified/fork system suites also share fixed recovery ports and were not used as acceptance evidence. Interrupted or failed environment runs are not counted as passing evidence.
