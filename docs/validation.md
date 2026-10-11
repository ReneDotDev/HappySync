# Validation evidence and remaining acceptance

These notes distinguish executable checks from broader deployment acceptance. Validation covers synchronization, discovery, conflict preservation, persisted recovery, default `.git/` and `node_modules/` exclusions, and configurable gitignore-style rules. After local implementation validation, the user authorized testing on a disposable EC2 instance. Separate consumer NAT and production capacity acceptance remain distinct from that test.

## Repeatable local checks

Run from the repository root with Go 1.26.0:

```sh
go test -count=1 ./...
HAPPYSYNC_INTEGRATION_RACE=1 go test -race -count=1 ./...
go vet ./...
HAPPYSYNC_MDNS_TEST=1 go test -race -count=1 -v ./internal/network -run TestLocalMDNS
HAPPYSYNC_SCALE_SCAN=1 go test -count=1 -v ./internal/index -run TestScan100K
HAPPYSYNC_SCALE_ENGINE=1 go test -count=1 -timeout=15m -v ./tests/integration -run TestHundredThousandFilesEngineIndexAndRestart
HAPPYSYNC_LARGE_TRANSFER=1 go test -count=1 -timeout=15m -v ./tests/integration -run TestMultiGiBProcessTransfer
go run ./cmd/release --version v0.1.0-dev
```

The race flag instruments the Go test harness; the environment flag additionally instruments spawned HappySync workers. The mDNS fixture requires `HAPPYSYNC_MDNS_TEST=1` on a host with multicast routing available: it must discover and authenticate both peers and verify an encrypted block transfer. Hosted CI does not guarantee local multicast routing, so its portable suite omits this fixture; CI success does not establish mDNS acceptance. Process/network tests use disposable roots and real local sockets. No personal directory should be used as a synchronization fault fixture.

## Milestone coverage

| Milestone | Implemented local validation |
| --- | --- |
| 0: connectivity spike | Real local QUIC/TCP, fresh membership challenge, mDNS, isolated provider DHT, forced encrypted circuit relay |
| 1: state and CLI | Default CWD and relative folder selection, persistent invitation/identity, mismatched-key rejection, duplicate root lock, READY background lifecycle, private control, status/stop |
| 2: functional synchronization | Independent processes create/edit, verified installation, empty joining peer, offline restart/catch-up, ignored files, specific executable exclusion |
| 3: discovery/connectivity | Periodic hosted registration, isolation/expiry/quotas, unavailable providers, candidate retry, forced relay, connection-mode reporting, relay byte/rate/session limits |
| 4: swarm | Concurrent distinct blocks from two sources, corruption/retry, partial-peer serving before completion, staged checkpoint reuse and process restart |
| 5: causal safety | Concurrent edits and edit/delete, deterministic conflict paths and editable conflict copies, populated tombstoned import, file/directory transitions, portable-path rejection, baseline protection, transaction rollback and abrupt-process recovery |
| 6: packaging/reliability | Four CGO-free Linux/macOS release targets, checksum and executable dependency inspection, public repository documentation, native CI configuration, 100,000-file inventory, full-engine index/restart, and multi-GiB process transfer |

Coverage does not establish every behavior on a different operating system or external network. The CI workflow runs uncached native tests, instrumented subprocess tests, race detection, and vet on Linux and macOS. Its release job builds all four artifacts; manual workflow dispatch additionally runs scale fixtures. Native Linux amd64 and macOS arm64 CI passed for the release preparation; platform scope and current evidence are recorded below.

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

All four supported v0.1.0-dev release targets compiled locally and passed SHA-256 verification. An immediate rebuild into a separate output directory produced identical `SHA256SUMS` and `build.json` for all four supported targets. File inspection confirmed their intended architectures. Both Linux ELF binaries are statically linked. macOS binaries use only OS libraries/frameworks, with no separate SQLite or C runtime installation. Native macOS arm64 `version` and `--help` ran successfully. Native Linux arm64 evidence is recorded below; Linux amd64 and macOS arm64 native tests, race detection, and vet passed in GitHub CI; macOS amd64 native acceptance remains pending. Cross-compilation alone cannot establish native correctness.

The multi-GiB fixture uses a 2 GiB + 17 byte file with unique 4 MiB blocks, streams its fixture/hashes with bounded memory, samples worker RSS and staging usage, and independently compares complete SHA-256 hashes after BLAKE3-verified protocol transfer and again after receiver restart. It rejects sampled worker RSS over 768 MiB and staging above verified blocks plus one assembly with a small overhead allowance. Its final-source local run, executed separately from other fixtures and builds, measured:

| Measurement | Result |
| --- | --- |
| Total test duration | 32.91 seconds |
| Transfer, full-hash verification, and restart verification | 28.356 seconds |
| Sampled peak worker RSS | 253,263,872 bytes (approximately 241.5 MiB) |
| Sampled peak receiver staging | 4,273,995,793 bytes (approximately 3.98 GiB) |
| Verified file length | 2,147,483,665 bytes |

The staging peak includes verified blocks and the assembly; completed blocks are released after installation. These local loopback measurements include verification/restart work and must not be presented as Internet throughput or relay capacity.

## Real EC2 validation

On 2026-10-11, the Linux arm64 release binary ran on Amazon Linux 2023 in a disposable `t4g.micro` (2 vCPUs, 1 GiB RAM). Its SHA-256 matched the locally verified release artifact. After the relay publication fix described below, cross-compiled test executables reran natively against the rebuilt release: all 24 network tests and 23 integration tests passed, including two integration subprocess helpers, with no failures. The opt-in 100,000-file and 2 GiB fixtures were skipped on this small instance; their macOS measurements above are separate evidence. This was a native Linux run without race instrumentation, separate from native GitHub CI.

The Mac-to-EC2 TCP and QUIC tests used real public sockets with LAN/DHT discovery disabled and an explicit peer address. The final rebuilt-release runs passed in 62.42 and 61.83 seconds respectively. Independent SHA-256 directory comparisons covered a 10 MiB + 32 byte multi-block transfer, bidirectional creation/replacement/deletion, offline edit/delete catch-up, concurrent offline edits with both variants preserved, unchanged ignored bytes, stable peer identities, and persisted conflict hashes after restart. Both runs additionally exchanged newly created files in both directions after the final restart. The final status showed one authenticated direct sync connection on each peer. See [the remote test harness](../tests/remote/README.md) for reproduction. These fixture durations include SSH operations and verification; they are not throughput measurements.

A separate fresh-identity test enabled default public DHT bootstraps, disabled mDNS, and configured no manual peers, private bootstraps, hosted discovery, or relays. The initial release passed in 41.40 seconds, and the rebuilt release passed again in 197.65 seconds after provider retries; these times include setup, discovery, and transfer. Both final peers discovered and authenticated the other peer directly, and both directories matched the independently computed SHA-256 hashes of two directional markers. This establishes default public provider discovery on this deployment; it does not establish every provider policy, NAT topology, or a fixed discovery latency. Status addresses are captured at startup, so their list does not establish the final public endpoint or transport. The runtime's historical `public acceptance pending` health qualifier is not the evidence for this result; successful authentication and matching bytes are.

The forced configured-relay run passed the same seven content/restart checks in 71.28 seconds. Both sync clients listened only on loopback, so cross-machine direct sync listeners were unavailable. They reported one authenticated relayed and zero direct sync connections each. The separate EC2 server registered both clients in hosted discovery and counted 14,211,458 forwarded bytes, exceeding the multi-block payload. That run used an explicit circuit peer hint: it establishes encrypted circuit delivery and hosted registration, not automatic rendezvous. Server metrics and all final file hashes were checked before stopping the owned clients and server.

Removing the manual circuit hint exposed a real defect: reservations and hosted registrations were active, but neither client authenticated within 240 seconds. A local socket regression reproduced it. Successful explicit reservations were not publishing circuit addresses while upstream AutoRelay waited for a reachability event, and hosted discovery could overwrite a circuit retry candidate with a later direct address. The fix publishes validated, unexpired reservation paths through the host address factory and combines each discovered peer's routes into one retry candidate. It preserves direct endpoints and upstream AutoRelay, renews short leases before expiry, and retains only live paths after transient renewal failure. The new regression verifies authenticated relay transfer with direct dialing blocked; endpoint identity, address bounds, expiry, renewal, and direct-path checks also pass. The complete local network suite passed normally in 13.821 seconds and with race detection in 14.473 seconds; network vet passed. All four supported final release binaries rebuilt, and a second build produced identical checksums and metadata.

The rebuilt release then passed the discovery-only EC2 relay test in 184.33 seconds. Both clients listened only on loopback and configured hosted discovery and relay infrastructure, without manual sync peer hints, mDNS, or DHT. They authenticated a circuit connection, passed all full-hash transfer/conflict/ignore/offline checks, preserved hashes and identities across restart, and exchanged newly created files in both directions after the final restart. Each final status had one authenticated relayed and zero direct sync connections; hosted discovery had two registrations and the server counted 14,283,189 forwarded bytes. The harness waits for reconnection rather than inferring readiness from already matching persisted files. This establishes automatic configured rendezvous and relay publication on this topology; the Linux client and relay shared the EC2 instance, so it does not establish two separate consumer NATs or DCUtR.

After testing, the instance was explicitly terminated. Independent AWS describe calls confirmed its `terminated` state and absence of the owned EBS volume, security group, and imported key pair. Owned smoke roots and the temporary local SSH key were removed. No test infrastructure remains running.

## Remaining external acceptance

Extend the checks to additional separate networks, including two consumer NATs and restrictive firewalls. Repeat public provider and relay publication checks across those topologies; record DCUtR attempts and outcomes; block direct paths to require relay file transfer; allow direct paths and check subsequent direct use. Compare final directory paths and full hashes in every case.

Deploy the same binary to the intended VPS, confirm advertised TCP/UDP reachability externally, and measure sustained transfer with relay pacing, byte/session budgets, concurrent clients, server restart, and capacity exhaustion. Capture peer mode and failure status without invitations or file paths. Public community-relay behavior cannot substitute for the configured relay acceptance.

Local private-address DHT, forced loopback circuit tests, and cross-compilation must never be reported as successful public DHT, consumer NAT, VPS, or native-platform acceptance. The EC2 evidence establishes only the operating system and network paths actually tested above.
