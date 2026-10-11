# Connectivity prototype and local acceptance

The connectivity gate was adapted with user approval: external tests followed
implementation and local validation. Local tests demonstrate protocol behavior;
the separate EC2 evidence below establishes the Internet paths actually tested.

Pinned versions: Go 1.26, go-libp2p 0.48.0, go-libp2p-kad-dht 0.39.0.
The application uses QUIC/TLS and TCP with libp2p's default secure negotiation,
plus circuit relay v2, AutoRelay for configured relays, NAT port mapping,
reachability detection, and DCUtR. No custom traversal or encryption is used.

`go test ./internal/network -v` passed locally on macOS on 2026-10-11:

- TCP and QUIC authenticated snapshots and BLAKE3-verified blocks.
- Incorrect group keys cannot invoke metadata or content backends.
- Captured authentication proofs fail on a new challenge; reflected roles fail.
- Oversized and malformed framing fails before message processing.
- Hosted discovery registration, group separation, and TTL expiry.
- A 1.8 MB verified block through a circuit relay with direct dialing blocked by
  a connection gater. The test asserts relayed connectivity and equal bytes.
- Two independent hosts discover and authenticate through actual local mDNS.
- A three-host loopback DHT advertises and locates the derived provider CID,
  then authenticates the discovered peer. Private addresses are explicitly
  enabled for this isolated test.

## Frozen discovery derivation

Derive 32 bytes using HKDF-SHA256 with the 32-byte group secret as input key,
the 16-byte Sync ID as salt, and UTF-8 `HappySync/v1/discovery` as info.
Encode these output bytes as a SHA2-256 multihash digest (code 0x12, length 32)
and wrap in CIDv1 with raw codec 0x55. These bytes label a provider namespace,
not a file. The same output's lowercase hex is the hosted-discovery label.

Test vector: Sync ID is sixteen `01` bytes, secret is thirty-two `02` bytes.
The CID is `bafkreihmidh7hq3sgecjbfuslgb5mnfynbbcofrrxbmqg7zdgglumxtgg4`.

## Membership handshake and framing

Derive an independent 32-byte MAC key with HKDF-SHA256, the same salt/input,
and info `HappySync/v1/auth`. On every application stream the responder sends
a fresh random 32-byte nonce. The initiator sends its own fresh nonce and an
HMAC-SHA256 proof. The responder verifies and returns its proof. Proofs cover
length-prefixed domain, negotiated versioned stream protocol, initiator Peer ID,
responder Peer ID, both nonces, and the distinct initiator/responder role.
The derivation binds the Sync ID. libp2p exposes authenticated peer identities
but no common transport exporter in this API; no exporter binding is claimed.
No metadata, availability, or block is served before membership verification.

Messages are typed JSON with a four-byte big-endian length prefix. Unknown
fields and trailing JSON are rejected. Handshakes cap at 1 KiB; metadata pages
at 128 records and 16 MiB; blocks at 4 MiB before JSON encoding; availability
at 256 hashes; cursors at 4 KiB. Every stream has deadlines and client context
cancellation resets blocked streams. There are 64 concurrent application
handlers, 8 candidate dial workers, 256 candidate entries, and failed-auth
rate accounting. Contact addresses are retained with the bounded candidate
table, so one-time discoveries also retry after transient failures. Dial retries
use exponential delay with equal jitter, capped at five minutes. At most 32
members are retained; offline member entries can be evicted when capacity is
needed. There are 64 outgoing application streams and 8 incoming streams per
peer. Unauthenticated attempts cap at 64 per second globally and 12 per minute
per peer, with at most 256 failed-peer accounting entries. Block requests carry
BLAKE3 hash and expected size; the
content address is immutable, so a changed file cannot alter requested bytes.

Optional per-client upload/download byte rates share independent budgets across
all authenticated application streams. Zero is unlimited; negative limits are
rejected. Pacing uses small chunks so a large block does not reserve an entire
frame ahead of metadata. Stream deadlines account for the configured rate;
caller cancellation still resets blocked streams immediately. Authentication
keeps its separate 15-second deadline. Local tests transfer two blocks
concurrently and check that both upload and download limits apply to the
aggregate, preserve verified bytes, and stop promptly on cancellation.

The authenticated `/HappySync/control/1.0.0` state-summary RPC exchanges a
version-1 request and a 64-character hexadecimal state digest in frames capped
at 1 KiB. Engines compare this summary before paging metadata, skipping unchanged
record sets. A missing backend capability, incompatible peer, or invalid/empty
digest falls back to full snapshots. Digest tests cover identical/different
record sets, malformed replies, and denial before backend invocation for wrong
group keys. Digest equality describes replicated metadata, not filesystem
installation or proof that offline members match.

Hosted discovery uses authenticated libp2p transport Peer IDs, a two-minute
registration TTL, a one-second minimum request interval per peer, 32 active
handlers, at most 1,024 labels, 4,096 total registrations, 64 registrations per
label, and 8 labels per Peer ID by default. Group, total registration, and
per-peer registration limits are configurable. Each peer has at most 16 validated
addresses. Rate-accounting entries expire and cap at 4,096. Contact addresses
must include TCP or QUIC endpoints; unspecified/multicast addresses and nested
relay paths are rejected. Responses are bounded and written after releasing
the registration table lock. It does not receive shares, manifests, or file paths.

## Real Internet validation

On 2026-10-11, the Linux arm64 release ran natively on a disposable Amazon Linux
2023 EC2 instance. Public TCP and QUIC connections from macOS passed full-hash
transfer, deletion, offline recovery, conflict preservation, ignore, and restart
checks. A configured EC2 circuit relay passed the same checks with both clients
restricted to loopback listeners: each reported one relayed and zero direct sync
connections. Hosted discovery registered both clients, and server metrics counted
14,211,458 forwarded bytes. That initial relay run used a manual circuit hint.
A separate run without any sync peer hint exposed and then verified a fix for
reserved circuit publication and hosted candidate route retention. The rebuilt
release automatically discovered and authenticated both loopback-only clients,
passed all content checks and fresh bidirectional transfers after restart, and
reported zero direct sync connections with 14,283,189 forwarded bytes.

A separate fresh-identity run used default public DHT bootstraps with no manual
peer addresses, hosted discovery, relays, or mDNS. The Mac discovered EC2, both
authenticated a direct connection, and independently computed SHA-256 hashes
matched for markers in both directions. This is actual public provider discovery
for this deployment, without an inference about the final underlying transport
or other NAT combinations. See [validation evidence](validation.md) and
[remote test instructions](../tests/remote/README.md).

## Further external validation

Other public provider policies, relay publication across other NAT topologies,
DCUtR success across separate NATs, and sustained VPS throughput remain
unverified. Publication health alone does not establish peer discovery or delivery.
Blocked Internet discovery leaves LAN, manual peers, and configured hosted
discovery available. A public community relay is not assumed to carry files.
Hosted discovery and relay are separate options on the same server binary;
DHT bootstrap service is another explicit option.

The relay limits 64 reservations, 4 circuits per peer, 8 reservations per source
IP, five-minute circuit duration, and 256 MiB per direction per circuit by
default. The server adds paced forwarding at 32 MiB/s globally and 8 MiB/s for
each endpoint's combined ingress/egress, with at most 512 relay hop/stop streams.
A circuit consumes two such streams. Byte pacing operates on the upstream
circuit-v2 encrypted stream interfaces, using 2 KiB chunks and cancellable waits;
it never inspects application content. Global bandwidth counts forwarded bytes
once; per-peer accounting includes receiving and sending. All pacing/session
quotas except the source-IP bound are configurable through the server API.
An optional operator Peer ID allowlist uses the upstream circuit-v2 ACL: it
permits reservations only from listed identities and circuits only when both
endpoints are listed. Empty allowlists permit the explicitly enabled, resource
bounded relay to accept other peers. This controls use of the operator's relay;
it does not add membership revocation or access controls to sync groups.
The counters in `Server.Stats()` expose active relay streams/endpoints,
forwarded bytes, bandwidth limits, and discovery counts without lookup labels.

Additional local tests verify actual encrypted relay delivery with a 128 KiB/s
rate and assert elapsed time against payload bytes, byte-budget failure,
single-stream-cap refusal, reservation exhaustion, hosted discovery total and
per-peer quotas, operator allowlist acceptance/refusal, TTL recovery of capacity,
failed-auth bounds, and retry after
one discovery announcement. A quota failure returns no complete verified block;
the application must retry after the circuit closes or capacity becomes free.
These tests exercise real loopback sockets, not a mocked transport. External
throughput and resource-budget acceptance remains pending before claiming
production capacity.
