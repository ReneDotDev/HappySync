# Security policy

HappySync is an early implementation and has not received an independent security audit. This policy describes its intended trust boundary and disclosure process.

## Report a vulnerability

Use the repository host's private vulnerability reporting feature when available. Include the affected revision, reproducible steps using disposable data, impact, and a minimized test. Do not put real share keys, private identities, user files, or exploit details exposing users in a public issue. If private reporting is not enabled, open a public issue requesting a private contact without disclosing vulnerability details. No response-time commitment is made until maintainers establish one.

## Trust boundary

A share key is a bearer capability granting full read, write, and delete access. It is deliberately printed on every successful start. A malicious authorized member can damage group data; v1 provides no member ACLs or individual revocation. Move to a fresh group and key when membership is compromised. Keep independent backups.

libp2p authenticates transport identities and encrypts network traffic. HappySync additionally requires fresh group-membership proofs on each synchronization stream before returning metadata, availability, or blocks. Relay operators carry encrypted traffic and do not receive file contents or group secrets. Discovery operators receive opaque lookup labels, peer IDs, and addresses; observers can still infer linkability and network presence. HappySync does not provide anonymity.

Local synchronized files are plaintext. `.HappySync/config.json` contains the invitation; `.HappySync/identity.key` contains the device private key. Private state on Linux and macOS uses restrictive owner permissions. The local status/stop endpoint is owner-restricted. Operating-system compromise or access as the same account is outside the threat model. Protect local disks and backups accordingly.

## Safeguards and limits

Versioned invitations reject unsupported versions, malformed encodings, and checksum errors. The invitation checksum detects transcription mistakes and is not an authentication primitive. Streams have bounded framing, deadlines, membership checks, and handler limits. Blocks and assembled files are verified with BLAKE3. Portable paths reject traversal, reserved state paths, symlink traversal, and spelling collisions. Durable causal state, tombstones, deterministic conflict copies, and application journals preserve concurrent and interrupted work.

These mechanisms do not establish protection against every filesystem race, resource-exhaustion attack, or implementation defect. Public DHT/NAT/VPS behavior and native-platform acceptance require their own tests; local simulations cannot prove them. Relay pacing, byte/session budgets, and an optional operator Peer ID allowlist are described in [server documentation](docs/server.md). An infrastructure usage allowlist does not revoke a leaked group invitation or change member permissions.

## Supported versions

Before a stable release, security fixes target the current development revision. Release notes must state supported versions when stable maintenance begins. Do not assume old development binaries remain compatible or receive backported fixes. Stop the process and preserve both user files and `.HappySync/` before an upgrade; see [upgrade policy](docs/architecture.md).
