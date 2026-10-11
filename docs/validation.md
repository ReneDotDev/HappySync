# Validation evidence and remaining acceptance

These notes distinguish executable local checks from deployment acceptance. The agreed scope includes all milestones in `PLAN.md`, plus default `.git/` and `node_modules/` exclusions and configurable gitignore-style rules. The user deferred real consumer NAT, public DHT, and VPS acceptance until after implementation; other implementable requirements remain in scope.

## Repeatable local checks

Run from the repository root with Go 1.26.0:

```sh
go test -count=1 ./...
HAPPYSYNC_INTEGRATION_RACE=1 go test -race -count=1 ./...
go vet ./...
HAPPYSYNC_SCALE_SCAN=1 go test -count=1 -v ./internal/index -run TestScan100K
HAPPYSYNC_SCALE_ENGINE=1 go test -count=1 -timeout=15m -v ./tests/integration -run TestHundredThousandFilesEngineIndexAndRestart
HAPPYSYNC_LARGE_TRANSFER=1 go test -count=1 -timeout=15m -v ./tests/integration -run TestMultiGiBProcessTransfer
go run ./cmd/release --version v0.1.0-dev
```

PowerShell uses `$env:HAPPYSYNC_INTEGRATION_RACE = '1'` (and the equivalent environment assignment for the opt-in fixtures) before each relevant command. The race flag instruments the Go test harness; the environment flag additionally instruments spawned HappySync workers. Process/network tests use disposable roots and real local sockets. No personal directory should be used as a synchronization fault fixture.

## Milestone coverage

| Milestone | Implemented local validation |
| --- | --- |
| 0: connectivity spike | Real local QUIC/TCP, fresh membership challenge, mDNS, isolated provider DHT, forced encrypted circuit relay |
| 1: state and CLI | Default CWD and relative folder selection, persistent invitation/identity, mismatched-key rejection, duplicate root lock, READY background lifecycle, private control, status/stop |
| 2: functional synchronization | Independent processes create/edit, verified installation, empty joining peer, offline restart/catch-up, ignored files, specific executable exclusion |
| 3: discovery/connectivity | Periodic hosted registration, isolation/expiry/quotas, unavailable providers, candidate retry, forced relay, connection-mode reporting, relay byte/rate/session limits |
| 4: swarm | Concurrent distinct blocks from two sources, corruption/retry, partial-peer serving before completion, staged checkpoint reuse and process restart |
| 5: causal safety | Concurrent edits and edit/delete, deterministic conflict paths and editable conflict copies, populated tombstoned import, file/directory transitions, portable-path rejection, baseline protection, transaction rollback and abrupt-process recovery |
| 6: packaging/reliability | Six CGO-free release targets, checksum and executable dependency inspection, public repository documentation, native CI configuration, 100,000-file inventory, full-engine index/restart, and multi-GiB process transfer |

Coverage does not establish every behavior on a different operating system or external network. The CI workflow runs uncached native tests, instrumented subprocess tests, race detection, and vet on Linux, macOS, and Windows. Its release job builds all six artifacts; manual workflow dispatch additionally runs scale fixtures. Remote CI has not been run merely by creating its workflow.

## Measured local evidence

Local development platform: macOS arm64, Go 1.26.0, 2026-10-11.

The complete package race suite passed. The independent-process integration suite passed normally in 67.627 seconds and with instrumented workers in 117.649 seconds. Focused cleanup/crash/resume recovery passed in 51.287 seconds; directory lifecycle coverage passed in 6.31 seconds. `go vet ./...`, module checksum verification, and a module-tidy comparison also passed without dependency changes. These are local macOS results; remote CI remains pending.

`go test -race ./internal/network` passed with real loopback/multicast/circuit traffic. It includes actual paced encrypted delivery at 128 KiB/s, reservation/stream/byte-budget refusal, retry after a single discovery announcement, bounded failed-auth accounting, and hosted registration capacity recovery. [Connectivity notes](connectivity-spike.md) list the tested paths and exact derivation.

CLI/config race tests verify bandwidth flag forwarding to detached workers, private configuration reopening, omitted-flag persistence, explicit-zero clearing, unchanged invitation/identity, and rejection of negative or malformed limits. A separate actual detached-process check also confirmed both stored limits across a stop/restart and cleared only upload with explicit zero. Relay allowlist parsing rejects malformed Peer IDs and caps the list; networking tests verify real allowed/denied reservations and circuits.

The 100,000-file fixture scanned and verified 100 directories of 1,000 small files in 3.07 seconds; a cache-disabled rerun measured 2.79 seconds and approximately 118 MiB maximum RSS for the timed `go test` command. The latter is test-command RSS, not an engine memory bound. Details and caveats are in [reconciliation notes](reconciliation.md#automated-evidence).

The separate full-engine fixture creates 100,000 uniquely populated files, starts a real detached worker, completes its first reconciliation, stops, checks SQLite integrity and persisted records/baselines/counter, verifies every file with independent SHA-256, and restarts. Its final-source local run, executed separately from other fixtures and builds, measured:

| Measurement | Result |
| --- | --- |
| Total test duration | 39.63 seconds |
| Fixture generation | 5.19 seconds |
| Initial READY | 8.584 seconds |
| Startup through first reconciliation | 14.991 seconds |
| Restart READY | 4.615 seconds |
| Sampled peak worker RSS | 691,978,240 bytes (approximately 659.9 MiB) |
| Durable records, baselines, and counter | Exactly 100,100 each; unchanged after restart |

The additional 100 observations are directories. This is a real engine index/persistence/restart measurement on a warm local filesystem, separate from the faster index-only scan. It does not measure network replication of 100,000 files or long-lived tombstone histories. Sampling is an observed peak, not a universal memory guarantee.

All six v0.1.0-dev release targets compiled locally and passed SHA-256 verification. An immediate rebuild into a separate output directory produced identical `SHA256SUMS` and `build.json` for all six targets. File inspection confirmed their intended architectures. Both Linux ELF binaries are statically linked. macOS binaries use only OS libraries/frameworks, and Windows PE imports contain `kernel32.dll`, with no separate SQLite or C runtime installation. Native macOS arm64 `version` and `--help` ran successfully. Native Linux/Windows tests must run in CI or on those operating systems; these builds alone cannot establish runtime correctness there.

The multi-GiB fixture uses a 2 GiB + 17 byte file with unique 4 MiB blocks, streams its fixture/hashes with bounded memory, samples worker RSS and staging usage, and independently compares complete SHA-256 hashes after BLAKE3-verified protocol transfer and again after receiver restart. It rejects sampled worker RSS over 768 MiB and staging above verified blocks plus one assembly with a small overhead allowance. Its final-source local run, executed separately from other fixtures and builds, measured:

| Measurement | Result |
| --- | --- |
| Total test duration | 32.91 seconds |
| Transfer, full-hash verification, and restart verification | 28.356 seconds |
| Sampled peak worker RSS | 253,263,872 bytes (approximately 241.5 MiB) |
| Sampled peak receiver staging | 4,273,995,793 bytes (approximately 3.98 GiB) |
| Verified file length | 2,147,483,665 bytes |

The staging peak includes verified blocks and the assembly; completed blocks are released after installation. These local loopback measurements include verification/restart work and must not be presented as Internet throughput or relay capacity. No native Windows RSS measurement is inferred from Unix sampling.

## External acceptance explicitly deferred

Run disposable peers on genuinely separate networks, including consumer NATs and restrictive firewalls. Validate public bootstrap/provider compatibility and relay address publication; exercise public-to-private and private-to-private connectivity; record DCUtR attempts and outcomes; block direct paths to require relay file transfer; allow direct paths and check subsequent direct use. Compare final directory paths and full hashes in every case.

Deploy the same binary to the intended VPS, confirm advertised TCP/UDP reachability externally, and measure sustained transfer with relay pacing, byte/session budgets, concurrent clients, server restart, and capacity exhaustion. Capture peer mode and failure status without invitations or file paths. Public community-relay behavior cannot substitute for the configured relay acceptance.

These items remain pending under the user's explicit testing sequence. Local private-address DHT, forced loopback circuit tests, and cross-compilation must never be reported as successful public DHT, consumer NAT, VPS, or native-platform acceptance.
