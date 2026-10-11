# HappySync protocol version 1

This document freezes the current encoding and bounded stream contract. Reference implementations are `internal/config`, `internal/model`, `internal/network`, and `internal/reconcile`. The protocol assumes mutually authenticated encrypted libp2p transport. Possession of the group invitation grants all group permissions.

## Invitation

An invitation is ASCII `hsync1:` followed by strict unpadded base64url of exactly 57 bytes:

| Byte offsets | Contents |
| --- | --- |
| 0 | Version byte `0x01` |
| 1–16 | Random 128-bit Sync ID |
| 17–48 | Random 256-bit bearer secret |
| 49–56 | First eight bytes of SHA-256 over bytes 0–48 |

Decoding requires this exact length, canonical base64url, version, and checksum. Errors omit supplied values. The checksum detects transcription errors and does not establish membership. No invitation extension or infrastructure hint is encoded in v1; servers are explicit local configuration.

## Discovery and authentication keys

HKDF-SHA256 derives 32-byte keys with the group secret as input keying material, Sync ID as salt, and the following UTF-8 info strings:

| Purpose | Info |
| --- | --- |
| Provider lookup | `HappySync/v1/discovery` |
| Stream membership MAC | `HappySync/v1/auth` |

For DHT lookup, treat the discovery output as the digest of a SHA2-256 multihash: varint code `0x12`, varint length `0x20`, and 32 derived bytes. Wrap it in CIDv1 using raw codec `0x55`. This is an opaque provider label, not a hash of user files. Hosted discovery uses lowercase hex of the same derived bytes. Neither provider publishes the raw Sync ID, bearer secret, invitation, or folder path.

Frozen test vector uses Sync ID of sixteen `01` bytes and secret of thirty-two `02` bytes. These public fixture values must not be used for a real group:

| Output | Value |
| --- | --- |
| Discovery key hex | `ec40cff3c37231049096925983d634b86842271631b859037f233197465e6637` |
| Authentication key hex | `677d54f9295005c684033377be0be95f4d32f154a1b0c83779641b7c2927e8b3` |
| Base32 CID | `bafkreihmidh7hq3sgecjbfuslgb5mnfynbbcofrrxbmqg7zdgglumxtgg4` |
| Invitation | `hsync1:AQEBAQEBAQEBAQEBAQEBAQECAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAmGapFN1R2Nh` |

`TestDiscoveryCIDVector` verifies the derivation. All encodings use bytes, independent of platform endianness.

## Stream membership

Every auth, metadata, availability, and block stream authenticates group membership before returning application data:

1. Responder sends `{version:1, nonce:<fresh 32 bytes>}`.
2. Initiator creates another fresh 32-byte nonce and sends version, nonce, and initiator proof.
3. Responder verifies and returns version and responder proof. Initiator verifies it before sending its application request.

Proof is HMAC-SHA256 with the independently derived auth key. Its input is the concatenation of these seven byte strings, each preceded by its four-byte unsigned big-endian length:

1. UTF-8 `HappySync/v1/auth`.
2. Negotiated stream protocol ID.
3. Initiator's canonical libp2p Peer ID string.
4. Responder's canonical libp2p Peer ID string.
5. Initiator nonce.
6. Responder nonce.
7. UTF-8 `initiator` or `responder`, according to proof role.

The Sync ID is bound through key derivation. Both identities come from the authenticated libp2p connection. Nonces and distinct roles reject replay and reflection. Version/protocol IDs distinguish wire generations. The implementation has no transport-exporter binding: libp2p's current API exposes peer identities but no common exporter used here. This application handshake has local replay/reflection/group-isolation tests and has not received an independent cryptographic audit.

Handshake frames are limited to 1 KiB and a 15-second deadline. Successful handshake clears its deadline; application handlers then apply bounded request deadlines. Failed authentication resets the stream and does not expose metadata or content. Handler slots, per-peer limits, and failed-auth rate accounting bound resource use.

## Framing and requests

All frames begin with a four-byte unsigned big-endian byte length, followed by exactly that many UTF-8 JSON bytes. Zero/oversized frames, malformed JSON, unknown fields, and a second trailing JSON value are rejected. Byte slices use JSON base64. Fields are typed; this protocol uses JSON, not protobuf.

| libp2p protocol ID | Application payload |
| --- | --- |
| `/HappySync/auth/1.0.0` | Membership check only |
| `/HappySync/control/1.0.0` | Request `{version:1}`; response `{digest:<64-character-hex>,error?}` |
| `/HappySync/manifest/1.0.0` | Request `{cursor,limit}`; response `{records,cursor,error?}` |
| `/HappySync/blocks/1.0.0` | Request `{hash,size}`; response `{data,error?}` |
| `/HappySync/availability/1.0.0` | Request `{blocks:[{hash,size},...]}`; response `{available:[boolean,...]}` |
| `/HappySync/discovery/1.0.0` | Hosted registration/lookup; uses authenticated transport identity, without group-secret proof |

Metadata pages have at most 128 records and a 16 MiB frame. The engine batches ordinary pages within approximately 4 MiB of record JSON; a single large manifest may occupy up to 12 MiB, leaving framing/reply overhead below the wire cap. Cursors are at most 4 KiB and requests at most 8 KiB. Records are returned in immutable-ID order; an empty next cursor terminates a paging round. New operations whose IDs precede an in-flight cursor are picked up by subsequent rounds. Repeated received records are idempotent.

Authenticated control requests compare a digest of immutable record IDs before paging. The digest is lowercase hexadecimal SHA-256 over record IDs sorted lexicographically, with a single LF byte after each ASCII ID; an empty set uses SHA-256 of empty bytes. Successful insertion invalidates the cached digest; duplicate pages do not. Equal digests allow idle peers to skip redundant snapshots; differing digests trigger normal metadata exchange. Control frames are capped at 1 KiB, require version 1, and reject malformed digests. A peer without the optional digest capability falls back to snapshots. A digest match is an optimization, not acknowledgement that every file is installed: pending downloads and local scans continue independently.

Block requests include a lowercase hexadecimal BLAKE3 digest and expected length, from zero through 4 MiB. Replies are bounded by the 16 MiB frame limit, accommodate base64 overhead, and are checked for exact length and BLAKE3 before acceptance. The requested content address is immutable: a changed source path cannot change bytes accepted under an old digest. Availability carries at most 256 hashes and requires a matching response length. Cancellation resets blocked streams.

The node bounds candidates, application handlers, authenticated peers, and outgoing stream work. Group membership is required independently of whether a candidate came from mDNS, DHT, hosted discovery, or a configured address. Generic bootstrap/DHT/relay connections do not count as authenticated synchronization peers.

Optional client upload/download byte budgets are shared across authenticated application streams. Authentication has its own bounded deadline/capacity controls and is not delayed behind the application budget. Pacing covers framing and metadata as well as content, uses bounded chunks, and honors cancellation/deadlines. A zero configured client rate means no rate cap in that direction; relay-server rate limits are separate.

## Manifests and causal operations

Paths are normalized NFC relative paths using `/`. Every regular file has whole-content BLAKE3, byte length, and an ordered block list `{hash,size}` using fixed 4 MiB chunks. The final chunk may be shorter; empty files have zero blocks and BLAKE3 of empty content. Hashes are lowercase 64-character hex. Length and chunk totals are validated before transfer.

A record encodes `path`, `id`, `origin`, `counter`, `clock`, optional `deleted`, `directory`, `imported`, `manifest`, and optional `mtime`, in that struct order. `mtime` is an int64 Unix timestamp in nanoseconds; zero is omitted. It supports best-effort preservation of regular-file modification times and never selects a causal winner. The origin's vector component equals the operation counter. Operation IDs are BLAKE3 of Go JSON encoding of this record with `id` set to the empty string. Go JSON sorts vector map keys; optional false flags and zero mtime are omitted by the fixed struct tags. Preserve the exact field order/tags of `model.Record` for v1 interoperability. This is a frozen Go-encoding contract, not general canonical JSON. Other implementations must reproduce it exactly.

Records are immutable, including tombstones. Version vectors distinguish causally newer operations from concurrency; wall-clock time never selects a winner. Derived conflict records reuse source causal stamps and use the projected conflict path in their ID. See [reconciliation](reconciliation.md) for projection, coalescing, imports, directories, and explicit edits of conflict copies.

## Hosted discovery and relay

Hosted discovery requests contain `{key,addresses?}`; a nonempty contact list registers the authenticated remote Peer ID and returns peers for that opaque label. A server cannot attest group membership. Contacts have validated transport/endpoints, bounded length and count; replies are bounded and deduplicated by the client. Registrations expire after two minutes and are refreshed from startup, independently of DHT success. Request pacing and table/handler caps are documented in [server guide](server.md).

Circuit relay v2 is upstream libp2p protocol. It carries encrypted inner transports and never receives HappySync plaintext or group keys. Direct connections are preferred when available; authenticated peers are counted by transport mode. Reservation/stream/byte/duration/pacing controls and an optional operator Peer ID allowlist are independent of discovery and group membership. Public DHT policies, relay accessibility, and real hole punching still require external acceptance in the environments described in [connectivity notes](connectivity-spike.md).
