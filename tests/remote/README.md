# Real remote smoke test

`ec2_smoke.py` runs an existing Linux binary through SSH and connects an existing
macOS binary to its public address. It provisions no infrastructure. The caller
must upload an executable binary, provide an SSH key, allow the selected public
TCP or UDP port, and terminate any temporary infrastructure afterward.

```sh
python3 tests/remote/ec2_smoke.py \
  --host EC2_PUBLIC_IP --ssh-user ubuntu --ssh-key /private/ec2-test.pem \
  --remote-binary /tmp/HappySync --local-binary ./dist/HappySync-v0.1.0-dev-darwin-arm64 \
  --transport tcp --port 4001
```

Run again with `--transport quic` to exercise public UDP/QUIC. `--transport relay`
starts a separate scoped foreground discovery/relay server at `--port` and
restricts both sync clients to loopback listeners at `--port + 1` and
`--port + 2`. It explicitly dials a circuit address, requires relayed connections
with zero direct sync connections, and checks positive server forwarding bytes
and two hosted discovery registrations. The circuit address remains a manual
peer hint: this establishes hosted registration and relay delivery, rather than
discovery-only peer connection. The owned server PID is stopped during cleanup;
its state directory is outside the synchronized roots. Only the server's public
port needs an inbound firewall rule. `--remote-ip`
supplies an IP when the SSH host is a DNS name. Python 3 and OpenSSH are required
on the host; Python 3 is required on the Linux machine. SSH uses a private
temporary known-hosts file with first-connection host-key acceptance.

Add `--discovery-only --timeout 240` to relay mode to omit the manual circuit
peer hint and require automatic hosted discovery and circuit address publication:

```sh
python3 tests/remote/ec2_smoke.py \
  --host EC2_PUBLIC_IP --ssh-user ubuntu --ssh-key /private/ec2-test.pem \
  --remote-binary /tmp/HappySync --local-binary ./dist/HappySync-v0.1.0-dev-darwin-arm64 \
  --transport relay --port 4001 --discovery-only --timeout 240
```

Both sync clients still listen only on loopback and configure the same hosted
discovery and relay server. The JSON report distinguishes discovery-only pairing
from a manual peer hint. `--discovery-only` is valid only in relay mode.

The test validates native startup/status/stop, a file larger than two protocol
blocks, bidirectional creation/replacement/deletion, offline catchup, concurrent
offline edits, stable identities and persisted conflict hashes across restart,
new bidirectional bytes after final restart, and default/configured ignored
files. It compares SHA256 hashes of directory
contents and verifies that distinct ignored bytes remain unchanged. Its final
JSON includes safe statuses and content hashes, never invitations.

Both peers use exclusive temporary directories. The invitation stays in a
private host file and worker state. The harness stops only the workers for those
roots and removes those directories in `finally`; cleanup failure exits nonzero
and retains state rather than erasing a root still used by a worker. Terminating
an instance remains the caller's responsibility even if this test fails.

Each mode establishes only the connection path actually tested. Relay mode also
checks configured circuit delivery and hosted registration; discovery-only
pairing is established only when its additional flag is used successfully.
None of these modes establish public DHT discovery or consumer NAT hole punching
acceptance.

`public_dht_smoke.py` separately tests default public DHT provider discovery with
fresh roots and identities, mDNS disabled, and no manual peers, hosted discovery,
relays, or custom bootstraps. It requires an authenticated connection and matching
SHA256 marker files in both directions, rather than only a provider health label.

```sh
python3 tests/remote/public_dht_smoke.py \
  --host EC2_PUBLIC_IP --ssh-user ec2-user --ssh-key /private/ec2-test.pem \
  --remote-binary /tmp/HappySync --local-binary ./dist/HappySync-v0.1.0-dev-darwin-arm64 \
  --port 4103 --timeout 240
```

Allow public inbound TCP and UDP on that single test port if public reachability
probes are part of the check; SSH can remain restricted to your source IP.
The remote client's stopped private configuration enables both TCP and QUIC on
the selected port. Only this ephemeral root is edited. The deadline bounds peer
discovery and content verification; SSH/startup and cleanup take additional time.
A successful run establishes this deployment's default public DHT path, without
proving every provider policy, private-to-private NAT combination, or DCUtR.
Change `--ssh-user` to match your image (`ec2-user` for Amazon Linux, usually
`ubuntu` for Ubuntu).
