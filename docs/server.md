# Self-hosted discovery, relay, and bootstrap

The same HappySync binary runs optional infrastructure. It stores a persistent private server identity and ephemeral discovery registrations, never synchronized files. Discovery and relay are separate roles; enabling discovery alone does not provide a hole-punch communication path.

## Start a server

Use a machine with reachable TCP and UDP ports, and persistent private storage for its identity:

```sh
HappySync serve --discovery --relay --listen 0.0.0.0:4001 \
  --state-dir ./server-state
```

`--listen` in server mode takes an IP literal and port (`0.0.0.0:4001` or `[::]:4001`). It listens on TCP and UDP/QUIC using the same port. The default identity directory is `.HappySync-server/`; set `--state-dir` to existing durable private storage. Keep `identity.key` across restarts so clients' `/p2p/<SERVER_ID>` addresses remain valid. Do not put this state inside a synchronized root.

When automatic address selection reports private or incorrect addresses, supply public endpoints explicitly:

```sh
HappySync serve --discovery --relay --listen 0.0.0.0:4001 \
  --state-dir ./server-state \
  --advertise /ip4/203.0.113.10/udp/4001/quic-v1 \
  --advertise /ip4/203.0.113.10/tcp/4001
```

`203.0.113.10` is an example placeholder. Replace it with the reachable address. Advertisements describe endpoints without the server Peer ID; the printed addresses add its `/p2p/` suffix. Supply only publicly reachable addresses for public deployment. The server forces public reachability for relay service; this configuration is an operator assertion, not a successful reachability test.

Use `--dht-server` to additionally run a libp2p-compatible DHT server. Repeatable `--bootstrap <multiaddr-with-peer-id>` selects upstream bootstrap peers for that role. Clients use `--bootstrap` to select bootstrap nodes. A private/local bootstrap is useful for isolated tests and does not prove public DHT compatibility.

Server mode stays in the foreground. Stop with Ctrl-C or SIGTERM. Run it under your OS service manager when you need restart-at-boot behavior; HappySync does not install or manage a system service. Protect its working/state directory and monitor process exit and resource use.

## Configure clients

Every client that should use hosted discovery must register with the same reachable server:

```sh
HappySync --folder ./shared \
  --discovery-server /ip4/203.0.113.10/tcp/4001/p2p/SERVER_ID \
  --relay-peer /ip4/203.0.113.10/tcp/4001/p2p/SERVER_ID
```

Replace `SERVER_ID` with the printed Peer ID. QUIC endpoints can also be used. These preferences persist per root. The invitation does not contain server hints, so configure each client explicitly. LAN/DHT discovery continues when enabled; hosted registration starts immediately and refreshes regularly. Relay reservations are retried separately. Inspect `HappySync status --json` to distinguish discovery failure, relay reservation failure, and authenticated synchronization connectivity.

## Firewall and network expectations

- Permit inbound TCP and UDP on the server's selected port, with matching firewall rules, NAT forwarding, and advertised public addresses.
- Permit client outbound QUIC/UDP and TCP to peers, bootstraps, and configured servers. UDP blocking can leave secure TCP paths usable.
- LAN mDNS needs multicast UDP 5353 on the local segment. Networks that suppress multicast require another discovery provider or manual `--peer` addresses.
- A relay must be reachable by both clients. Symmetric NAT, restrictive firewalls, or UDP filtering can prevent DCUtR; a working configured relay may still carry encrypted traffic.
- Server peer IDs and opaque lookup labels are linkable network metadata. Hosting does not create anonymity or central file storage.

No unrestricted community relay is assumed to carry files. Provision your own relay for expected traffic and treat public DHT bootstrap/provider behavior as an external dependency.

## Resource budgets

Default circuit relay v2 budgets are 64 reservations, four circuits per peer, eight reservations per source IP, five-minute circuit lifetime, and 256 MiB in each direction per circuit. Pacing additionally limits aggregate forwarded bytes to 32 MiB/s and combined ingress/egress per endpoint to 8 MiB/s; 512 hop/stop streams may be active. Pacing operates on encrypted protocol traffic, so application throughput is lower. Pacing uses bounded chunks and deadlines rather than an unlimited burst queue.

Circuit exhaustion ends that circuit; clients retry a path and reuse verified downloaded blocks. It does not reset the group or expose a partial final file. Byte/session limits and paced bandwidth are separate: a long-running transfer may require additional streams even when average bandwidth is below the rate ceiling. A server counts each forwarded byte once globally, while endpoint budgets count their combined ingress and egress.

Hosted discovery has a two-minute registration TTL, at least one second between requests from a Peer ID, 32 concurrent handlers, at most 1,024 lookup groups, 64 registrations per group, and 4,096 registrations overall. A Peer ID can register in at most eight groups. Each peer advertises at most 16 validated addresses, each bounded in length. Idle/expired tables are cleaned up; an authenticated transport identity is a resource-accounting key, not proof of group membership. Membership remains the client's separate handshake.

CLI quota flags use byte counts and Go durations such as `5m`; run `HappySync serve --help` for the complete list:

| Flag | Default |
| --- | --- |
| `--relay-total-rate` | 33,554,432 forwarded bytes/second |
| `--relay-peer-rate` | 8,388,608 bytes/second per endpoint |
| `--relay-data-limit` | 268,435,456 bytes per direction per circuit |
| `--relay-duration` | `5m` |
| `--max-reservations` | 64 |
| `--max-circuits` | 4 per peer |
| `--max-relay-streams` | 512 hop/stop streams |
| `--discovery-max-groups` | 1,024 |
| `--discovery-max-registrations` | 4,096 |
| `--discovery-max-per-peer` | 8 |

Zero API settings choose bounded defaults, and invalid negative settings fail. For public service, use firewall access restrictions and a service supervisor as appropriate for your expected users; there is no unrestricted public hosting promise or operator UI.

Restrict relay usage to known client transport identities with repeatable `--allow-peer <CLIENT_PEER_ID>` flags (at most 256 entries). When the list is nonempty, reservations and circuits require authorized endpoints; both source and destination must be listed. This is an infrastructure usage allowlist, independent of a group's invitation and permissions. With no allowlist, the explicitly enabled relay accepts transport identities subject to its quotas. For a private deployment, list all intended client IDs or restrict network access before exposing the service. These flags are server startup options and must be supplied on each restart.

## Operational checks and acceptance

After startup, verify the advertised endpoints from outside the server's network, configure two disposable clients, and confirm authenticated connected peers. Force direct-dial failure, confirm relayed mode, transfer content, and compare complete directory hashes. Then allow direct traffic and check migration to a direct path. Exercise byte/rate/reservation limits and reconnection under sustained traffic before committing production capacity.

Local tests exercise encrypted circuit delivery with direct dialing blocked, discovery registration/expiry limits, and pacing/quota behavior. A disposable EC2 server also discovered and connected macOS/Linux clients without manual sync peer hints, forwarded verified files with zero direct sync connections, and verified new transfers after restart. Both clients registered in hosted discovery. Separate consumer NAT/DCUtR behavior, sustained relay capacity, and other public DHT provider policies still need acceptance testing. See [validation](validation.md) for the evidence that has actually run.
