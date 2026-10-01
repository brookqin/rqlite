# Qiankun Downstream Upstream Policy

This repository is the controlled rqlite downstream used by Qiankun. It is not
developed from the upstream default branch directly.

## Current baseline

- Upstream repository: `https://github.com/rqlite/rqlite`
- Downstream repository: `https://github.com/brookqin/rqlite`
- Upstream release: `v10.4.0`
- Upstream commit: `8d7678b36b26ab2c352b72c10064297390fa5b0a`
- Release date: 2026-09-29
- Go directive: `go 1.26.7`
- SQLite driver: `github.com/rqlite/go-sqlite3 v1.51.0`
- Embedded SQLite: `3.53.4`
- License: MIT

The upstream `v10.4.0` tag is a lightweight tag pointing at the commit above.
GitHub's commit API reports `verified=true` and `reason=valid`; this is
separate from local GPG verification.
The development host has GPG, but does not have upstream signing key
`B5690EEEBB952194`; `git verify-commit` therefore reports “No public key”.
Do not describe upstream signature verification as locally passed.

Platform and test evidence for this baseline belongs in `QIANKUN_BASELINE.md`.
An unmodified-source baseline and the six replayed patches are evaluated
separately; an upstream test pass does not validate downstream behavior.

## Update procedure

1. Fetch and prune `upstream`, including tags.
2. Select the latest stable release tag; never select upstream `master` HEAD.
3. Record the tag, commit, release signature status, license, Go directive,
   SQLite driver version, embedded SQLite version, and module graph changes.
4. Run the unmodified baseline tests and `tools/qiankun-baseline.sh` before
   replaying downstream patches.
5. Rebase or replay every entry in `PATCHES.md` individually and re-run its
   stated verification.
6. Create a signed downstream tag only after provenance, tests, SBOM, license
   inventory, SQLite compile options, and the required release matrix pass.
7. Qiankun consumes a pushed downstream tag or immutable commit. It must not
   commit a local filesystem `replace` directive.

## Security updates

Security advisories affecting rqlite, HashiCorp Raft, SQLite, the SQLite driver,
TLS, protobuf, or snapshot/restore are reviewed against the pinned dependency
graph. Dependency upgrades are performed as a new audited baseline; the SQLite
driver is not upgraded independently of the selected rqlite release without a
documented compatibility and recovery test.
