# Contributing to HappySync

Read [README.md](README.md), [architecture](docs/architecture.md), and the relevant protocol or reconciliation notes before changing behavior. The implementation is early; document evidence and limitations instead of describing untested network paths as working.

## Build and validate

Use Go 1.26 or newer. Dependencies are pinned in `go.mod` and `go.sum`; SQLite uses a pure-Go driver. From the repository root:

```sh
go build -trimpath -o HappySync ./cmd/HappySync
go test ./...
HAPPYSYNC_INTEGRATION_RACE=1 go test -race -count=1 ./...
go vet ./...
```

Some tests open real loopback TCP/QUIC sockets or multicast listeners and start subprocesses. A sandbox or firewall may prevent them. Report environmental failures explicitly. Test fixtures use temporary folders, isolated keys, and content-hash assertions; never run destructive synchronization tests against personal folders. Native CI covers Linux and macOS; these are the supported operating systems. Cross-compilation complements native testing and cannot replace it.

Optional performance tests and measured results are documented with their commands in `docs/` when available. Large-scale acceptance should use disposable storage and retain the exact dataset size, duration, resource measurements, platform, and final hashes.

To instrument the separately spawned CLI workers as well as the Go test harness, run `HAPPYSYNC_INTEGRATION_RACE=1 go test -race -count=1 ./...`. Normal test runs skip the expensive opt-in fixtures. `docs/validation.md` lists their commands; a manually dispatched CI workflow runs them on Linux.

## Repository layout

| Path | Responsibility |
| --- | --- |
| `cmd/HappySync` | Client and server CLI |
| `cmd/release` | Portable cross-platform release builds and checksums |
| `internal/config` | Invitation format, private identity, per-root preferences |
| `internal/daemon` | Root lock, local status/stop IPC, background launch support |
| `internal/network` | Secure streams, membership authentication, discovery, relay |
| `internal/index` | Portable paths, ignores, safe inventory, BLAKE3 manifests |
| `internal/reconcile` | Causal records and deterministic conflict projection |
| `internal/store` | SQLite persistence and recovery journals |
| `internal/transfer` | Verified block cache, concurrent scheduling, assembly |
| `internal/engine` | Watcher/scans, reconciliation, safe filesystem application |
| `tests/integration` | Independent-process content and recovery acceptance |
| `docs` | Design decisions, network acceptance, deployment, protocol |

## Changes and pull requests

Keep changes focused. Explain the concrete problem, resulting behavior, and validation. Add meaningful regression coverage for data safety, authentication, recovery, or concurrency changes. Verify resulting trees by hashes and persistent state, not only status messages or timestamps. Include fault tests where a failure can lose content.

Never commit `.HappySync/`, invitations, private identities, real user data, diagnostics containing secrets, or generated release binaries. Diagnostics must redact secrets. Do not weaken group authentication, silently overwrite concurrent bytes, garbage-collect v1 tombstones, or follow symlinks.

Invitation, wire, record, and database changes need explicit versioning, compatibility tests, migration/recovery notes, and updated documentation. The migration policy is in [architecture](docs/architecture.md); newer unsupported state must fail safely. Local network simulation does not validate public DHT policies, consumer NAT traversal, or production relay capacity.

## Prepare a release

Run tests, race detection, vet, and native CI before tagging. Build with:

```sh
go run ./cmd/release --version v0.1.0
```

The release tool uses `CGO_ENABLED=0`, `-trimpath`, `-buildvcs=false`, and an injected version, then writes `dist/SHA256SUMS` and `dist/build.json`. It refuses unsafe versions and unsupported/duplicate targets. Check the executable's `version` output on each native platform and verify every distributed checksum. Record exact Go/dependency versions and validation gaps in release notes. Rebuild identical inputs to compare artifact hashes.

Pushing a version tag automatically builds and publishes those four binaries, `SHA256SUMS`, and `build.json` after native Linux/macOS tests, instrumented race suites, vet, and artifact checks pass:

```sh
git tag -a v0.1.0 -m 'HappySync v0.1.0'
git push origin v0.1.0
```

Use an unused `vMAJOR.MINOR.PATCH` tag, or a preview such as `v0.2.0-alpha.1`. Preview tags become prereleases; stable releases become latest. Publication verifies the downloaded artifacts and checks the remote tag still points to the tested commit. Assets upload as a draft before it becomes public. Existing releases are never overwritten: if publication is interrupted, inspect any draft and resolve it before rerunning the failed job. Branch pushes and pull requests produce review artifacts without publishing. Optional scale fixtures still require manual workflow dispatch.

Security vulnerabilities should follow [SECURITY.md](SECURITY.md).
