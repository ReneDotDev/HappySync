# HappySync

HappySync synchronizes a folder across devices that share one secret invitation. Each device keeps a complete local copy, can work offline, and reconciles changes when peers reconnect. One executable runs the client or optional self-hosted discovery and relay services.

HappySync uses Go, libp2p secure transports, BLAKE3 verified blocks, and durable SQLite state. There is no mandatory HappySync-operated service or central file storage. LAN discovery, public DHT discovery, manual peers, and explicitly configured infrastructure provide alternative ways to connect.

**Status:** an early implementation with automated local validation and real macOS-to-EC2 tests of TCP, QUIC, automatic hosted discovery through a configured relay, and default public DHT discovery. HappySync supports Linux and macOS on amd64 and arm64. Native Linux arm64 network and integration tests passed on EC2; Linux amd64 and macOS arm64 tests, race detection, and vet passed in GitHub CI. Separate consumer NATs, DCUtR hole punching, sustained server capacity, and native macOS amd64 acceptance remain pending. See [validation evidence](docs/validation.md), [connectivity details](docs/connectivity-spike.md), and [architecture](docs/architecture.md).

## Get started

Download a Linux or macOS binary for your processor from [GitHub Releases](https://github.com/ReneDotDev/HappySync/releases). Preview releases are marked as prereleases. `darwin` means macOS; `amd64` means Intel/AMD 64-bit, and `arm64` covers Apple Silicon and other ARM64 processors. Each release includes `SHA256SUMS` and build metadata.

Verify the downloaded file against `SHA256SUMS`, then rename it to `HappySync` and make it executable with `chmod +x HappySync`.

Alternatively, build from source with Go 1.26 or newer:

```sh
go build -trimpath -o HappySync ./cmd/HappySync
```

Put the binary on your PATH, or use its absolute path. On the first device:

```sh
mkdir shared
cd shared
HappySync
```

Copy the printed `hsync1:...` share key. On another device, choose an existing folder and join:

```sh
mkdir shared
cd shared
HappySync --key 'hsync1:YOUR_SHARE_KEY'
```

The current working directory is the default sync root. `--folder /path/to/shared` selects another existing folder; relative paths resolve against your current directory.

Add, edit, or delete regular files on either side. Peers discover each other where enabled network paths work and eventually converge. A newly joined empty folder cannot delete remote files. Existing contents of a newly joined folder are reconciled as imports.

The share key is a **secret with full read, write, and delete access**. Every successful start prints it, including restarts. To avoid a key in shell history and process arguments, read it from a private file or standard input:

```sh
HappySync --key-file /private/path/invitation.txt --folder ./shared
HappySync --key-stdin --folder ./shared
```

For `--key-stdin`, supply the key followed by EOF, or pipe input from your secret manager. Keep terminal captures private. Regular logs and JSON status omit the key.

## Background operation and status

```sh
HappySync --background --folder ./shared
HappySync status --folder ./shared
HappySync status --folder ./shared --json
HappySync stop --folder ./shared
```

The launcher returns after the child initializes its state and control endpoint and sends READY. One instance owns each root. The stop command waits for worker resource cleanup and root-lock release before reporting success. Restarting reuses its invitation and persistent device identity. A different invitation for an initialized folder is rejected; use a fresh folder to join a different group.

Background application events are redacted and actively rotated in `.HappySync/logs/operations.log`. Native library stderr goes to `worker.log`, whose retention guard truncates it when it exceeds 10 MiB at startup and once per second while running. It may briefly grow between checks; truncation drops older native messages. Keep diagnostics private.

Status distinguishes discovered candidates from authenticated connected synchronization peers, direct from relayed connections, and discovery from relay health. It reports files, pending work, bytes, transfer rates, conflicts, warnings, and errors. A disconnected peer remains offline until a usable path returns; an idle process alone is not proof that every other device has received your changes.

## Ignore rules

By default, `.git/` and `node_modules/` are ignored at every level. HappySync also excludes its `.HappySync/` state and the running executable when that specific file is inside the root. Symlinks and special files are skipped with warnings and never followed.

HappySync reads `.gitignore` and then `.happysyncignore` in each included directory. Add sync-specific rules to `.happysyncignore`, for example:

```gitignore
# Local build products
/dist/
*.log
**/cache/
# Keep a particular log when its parent is included
!important.log
```

Supported syntax includes comments, `*`, `?`, `**`, character classes, `/` anchoring, directory rules ending in `/`, escaping, and `!` negation. Later matching rules win; nested files apply below their directory. An excluded parent is pruned, so a child negation cannot reopen it: include the parent first. Git's global excludes, `.git/info/exclude`, and tracked-file exceptions are not imported. Ignore files themselves are ordinary synchronized files unless excluded.

Repeatable `--ignore '*.tmp'` rules are saved in local configuration and take precedence over ignore files. The built-in defaults remain active. Keep effective rules consistent across peers when you want identical visible trees. Adding an exclusion does not turn the newly ignored local files into deletion operations.

## Connectivity and self-hosting

Clients enable LAN mDNS and DHT discovery by default. DHT uses public libp2p bootstrap nodes unless you supply `--bootstrap`; default public discovery has been verified between fresh Mac and EC2 peers on one deployment. Reachability still depends on your network and public providers, and discovery can take several retry cycles. You can disable providers with `--no-mdns` and `--no-dht`.

For manual connection, copy a peer's printed address, including its `/p2p/<PeerID>` suffix:

```sh
HappySync --key-file /private/path/invitation.txt --folder ./shared \
  --peer '/ip4/192.168.1.20/udp/4001/quic-v1/p2p/PEER_ID'
```

`--listen` on a client accepts a multiaddr, for example `/ip4/0.0.0.0/udp/4001/quic-v1`. The default listens on dynamically selected UDP/QUIC and TCP ports. Each infrastructure address below also includes its server Peer ID:

```sh
HappySync --folder ./shared \
  --discovery-server '/ip4/203.0.113.10/tcp/4001/p2p/SERVER_ID' \
  --relay-peer '/ip4/203.0.113.10/tcp/4001/p2p/SERVER_ID'
```

The example address is a documentation placeholder. Configure discovery on every participating client; they register periodically from startup. Discovery gives peer addresses; relay provides an encrypted communication path when a direct connection is unavailable. Both are optional. See [server deployment and firewall guide](docs/server.md) for the same executable's `serve` mode.

Peer, bootstrap, relay, discovery, listen, ignore, and bandwidth preferences persist in `.HappySync/config.json`. Stop the instance before editing this file; it contains the share key, so never publish it. Repeatable flags replace their corresponding configured lists when supplied. Runtime worker, cache, and scan flags should be supplied again on restart.

For constrained machines, tune `--workers` (default 8; range 1–64), `--staging-limit` (cached block bytes; default 8 GiB), and `--scan-interval` (comprehensive rehash interval; default `10m`). Complete-file assembly needs additional disk space beyond the block cache. Choose a staging limit large enough for your largest file; quota exhaustion leaves it pending until capacity is available. See [resource controls](docs/architecture.md#resource-controls).

Use `--upload-rate` and `--download-rate` to cap aggregate synchronization stream bytes per second across peers; explicitly supplying `0` clears a client cap. For example, `--upload-rate 1048576` caps upload at roughly 1 MiB/s. These limits persist per root, remain active when the flag is omitted on restart, and are shown in running JSON status. Metadata/framing overhead also uses the budget, so file throughput is lower than the configured stream limit. Relay-server limits are independent.

## Data safety and limits

Per-path causal version vectors distinguish newer operations from concurrent edits. Differing concurrent contents survive as deterministic conflict copies; a concurrent deletion retains the tombstone and preserves the edit separately. Tombstones persist in v1 so an unchanged long-offline copy does not resurrect a deleted file. See [reconciliation rules](docs/reconciliation.md).

Files are split into 4 MiB blocks. Downloads use concurrent sources, verify every block and assembled whole-file hash, and retain unfinished verified work across restart. Partially downloaded peers can serve verified blocks. Files become visible only after verified staging and replacement, with durable recovery bookkeeping. Completed cache blocks are released while installed files remain available as sources.

V1 supports regular files and directories with portable names. It rejects traversal, reserved device names, and case/Unicode collisions; inspect status if a path cannot be synchronized. Permissions, ownership, extended attributes, symlinks, and special files are outside the replication model. See [architecture](docs/architecture.md), [protocol](docs/protocol.md), and [security policy](SECURITY.md).

File modification times are preserved on a best-effort basis where supported; causal vectors determine edit ordering. Stop before moving a root, and let interrupted recovery complete at its original location first. Join another device with the invitation in a fresh root; copying `.HappySync/` to another device duplicates its identity and is unsupported.

Anyone with the invitation can change or delete all group data. There are no per-member permissions or revocation; changing membership requires a new group. Traffic is encrypted in transit, including through relay, while local files remain plaintext. Discovery can expose peer identities, addresses, and linkability. **Synchronization is not a backup**: keep separate backups of irreplaceable data.

## Development and releases

```sh
go test ./...
HAPPYSYNC_INTEGRATION_RACE=1 go test -race -count=1 ./...
go vet ./...
go run ./cmd/release --version v0.1.0
```

The release command builds four CGO-free Linux and macOS binaries for amd64 and arm64 into `dist/`, with `SHA256SUMS` and build metadata. `--targets linux/amd64,darwin/arm64` selects a subset. Go is required to build, not to run the resulting client. Identical source, toolchain, dependency checksums, target, and version determine reproducibility; use identical inputs when comparing hashes.

Pushing a new `vMAJOR.MINOR.PATCH` tag automatically publishes those four binaries, checksums, and build metadata after Linux/macOS validation succeeds. Tags with a suffix, such as `v0.2.0-alpha.1`, publish as prereleases. See [release instructions](CONTRIBUTING.md#prepare-a-release).

[CONTRIBUTING.md](CONTRIBUTING.md) covers tests, repository layout, protocol changes, and release review. CI includes native Linux/macOS tests and race detection plus cross-platform release artifacts; a manual CI run also exercises the large-file and 100,000-file fixtures. [Validation notes](docs/validation.md) distinguish measured local evidence from external acceptance still pending. This repository is licensed under the [MIT license](LICENSE).
