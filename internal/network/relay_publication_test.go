package network

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"strings"
	"testing"
	"time"

	libp2p "github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/p2p/protocol/circuitv2/client"
	"github.com/libp2p/go-libp2p/p2p/protocol/circuitv2/proto"
	ma "github.com/multiformats/go-multiaddr"
	"github.com/zeebo/blake3"
)

// TestHostedDiscoveryPublishesReservedRelayWithoutReachabilityEvent covers
// configured infrastructure without manual synchronization-peer addresses.
// Loopback-only infrastructure cannot provide public AutoNAT observations, and
// the gaters prevent two clients on this machine from bypassing the relay.
// No forced reachability event starts upstream AutoRelay on the clients.
func TestHostedDiscoveryPublishesReservedRelayWithoutReachabilityEvent(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	server, err := Serve(ctx, ServerConfig{Relay: true, Discovery: true,
		Listen: []string{"/ip4/127.0.0.1/tcp/0"}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = server.Close() })
	config := testConfig()
	config.Relays = server.Addresses()
	config.DiscoveryServers = server.Addresses()
	if len(config.Peers) != 0 || !config.DisableMDNS || !config.DisableDHT {
		t.Fatal("fixture unexpectedly has another discovery path")
	}
	clients := make([]*Node, 2)
	for i := range clients {
		clients[i], err = newNode(ctx, config,
			libp2p.ConnectionGater(relayOnlyGater{relay: server.host.ID()}))
		if err != nil {
			t.Fatal(err)
		}
		node := clients[i]
		t.Cleanup(func() { _ = node.Close() })
	}
	source, receiver := clients[0], clients[1]
	data := bytes.Repeat([]byte("verified hosted-discovery circuit bytes\n"), 32768)
	source.SetBackend(&testBackend{data: data})
	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) && (source.Stats().Relay != "reservation active" || receiver.Stats().Relay != "reservation active") {
		time.Sleep(20 * time.Millisecond)
	}
	for _, node := range clients {
		if node.Stats().Relay != "reservation active" {
			t.Fatalf("configured reservation failed: %+v", node.Stats())
		}
		published := false
		for _, address := range node.Addresses() {
			if !strings.Contains(address, "/p2p/"+server.PeerID()+"/p2p-circuit/p2p/"+node.PeerID()) {
				continue
			}
			infos, err := parsePeers([]string{address})
			if err != nil || len(infos) != 1 || infos[0].ID != node.host.ID() {
				t.Fatalf("reservation published an invalid circuit address: %q", address)
			}
			published = true
		}
		if !published {
			t.Fatalf("active configured reservation did not publish a circuit address: addresses=%v stats=%+v", node.Addresses(), node.Stats())
		}
	}
	servers, err := parsePeers(server.Addresses())
	if err != nil {
		t.Fatal(err)
	}
	// Startup registration can precede reservation completion. Refresh over the
	// real hosted protocol after its one-second per-peer request budget resets.
	time.Sleep(1100 * time.Millisecond)
	source.discoverServers(servers)
	receiver.discoverServers(servers)
	deadline = time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) && len(receiver.Peers()) == 0 {
		time.Sleep(20 * time.Millisecond)
	}
	if peers := receiver.Peers(); len(peers) != 1 || peers[0] != source.host.ID() {
		t.Fatalf("hosted discovery did not authenticate its reserved circuit: source=%+v receiver=%+v", source.Stats(), receiver.Stats())
	}
	hash := fmt.Sprintf("%x", blake3.Sum256(data))
	got, err := receiver.FetchBlock(ctx, source.host.ID(), hash, int64(len(data)))
	if err != nil || !bytes.Equal(got, data) {
		t.Fatalf("discovered relay block bytes failed verification: %v", err)
	}
	for _, node := range clients {
		stats := node.Stats()
		if stats.Direct != 0 || stats.Relayed != 1 || stats.Discovery != "registered" {
			t.Fatalf("fixture did not retain its discovered relay-only path: %+v", stats)
		}
	}
	if stats := server.Stats(); stats.RelayForwardedBytes < uint64(len(data)) || stats.DiscoveryRegistrations != 2 {
		t.Fatalf("infrastructure did not record real registration and forwarding: %+v", stats)
	}
}

func relayPublicationPeer(t *testing.T) peer.ID {
	t.Helper()
	_, public, err := crypto.GenerateEd25519Key(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	id, err := peer.IDFromPublicKey(public)
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func TestReservedRelayAddressesValidateEndpointIdentityAndBounds(t *testing.T) {
	now := time.Now()
	id, outsider := relayPublicationPeer(t), relayPublicationPeer(t)
	local := ma.StringCast("/ip4/127.0.0.1/tcp/4001")
	public := ma.StringCast("/ip4/203.0.113.10/tcp/4001/p2p/" + id.String())
	p := peer.AddrInfo{ID: id, Addrs: []ma.Multiaddr{local}}
	valid := &client.Reservation{Expiration: now.Add(time.Hour), Addrs: []ma.Multiaddr{public, public}}
	got := reservationAddresses(p, valid, now)
	if len(got) != 1 || got[0].String() != public.String()+"/p2p-circuit" {
		t.Fatalf("public reservation endpoints were not preferred and deduplicated: %v", got)
	}
	if got := reservationAddresses(p, &client.Reservation{Expiration: now.Add(time.Hour)}, now); len(got) != 1 || !strings.HasPrefix(got[0].String(), local.String()) {
		t.Fatalf("successful private relay without advertised endpoints lost its configured route: %v", got)
	}
	invalid := []*client.Reservation{
		nil,
		{Expiration: now},
		{Expiration: now.Add(-time.Second)},
		{Expiration: now.Add(time.Hour), Addrs: []ma.Multiaddr{nil}},
		{Expiration: now.Add(time.Hour), Addrs: []ma.Multiaddr{ma.StringCast("/ip4/0.0.0.0/tcp/4001")}},
		{Expiration: now.Add(time.Hour), Addrs: []ma.Multiaddr{ma.StringCast("/ip4/224.0.0.1/tcp/4001")}},
		{Expiration: now.Add(time.Hour), Addrs: []ma.Multiaddr{ma.StringCast("/ip4/203.0.113.10/tcp/4001/p2p/" + outsider.String())}},
		{Expiration: now.Add(time.Hour), Addrs: []ma.Multiaddr{public.Encapsulate(ma.StringCast("/p2p-circuit"))}},
		{Expiration: now.Add(time.Hour), Addrs: []ma.Multiaddr{public.Encapsulate(ma.StringCast("/p2p/" + id.String()))}},
		{Expiration: now.Add(time.Hour), Addrs: []ma.Multiaddr{public}, Voucher: &proto.ReservationVoucher{Relay: outsider}},
	}
	for i, reservation := range invalid {
		if got := reservationAddresses(p, reservation, now); len(got) != 0 {
			t.Errorf("invalid reservation %d published a route: %v", i, got)
		}
	}
	var many []ma.Multiaddr
	for port := 4000; port < 4100; port++ {
		many = append(many, ma.StringCast(fmt.Sprintf("/ip4/203.0.113.10/tcp/%d/p2p/%s", port, id)))
	}
	if got := reservationAddresses(p, &client.Reservation{Expiration: now.Add(time.Hour), Addrs: many}, now); len(got) != maxReservedRelayAddresses {
		t.Fatalf("reservation endpoint count was not bounded: %d", len(got))
	}
}

func TestReservedRelayLeaseExpiresAndFailedRenewalPreservesOnlyLiveRoute(t *testing.T) {
	now := time.Now()
	p := peer.AddrInfo{ID: relayPublicationPeer(t), Addrs: []ma.Multiaddr{ma.StringCast("/ip4/127.0.0.1/tcp/4001")}}
	r := newRelayAddresses()
	direct := ma.StringCast("/ip4/127.0.0.1/tcp/5001")
	if r.refresh(p, nil, now) || len(r.factory([]ma.Multiaddr{direct})) != 1 {
		t.Fatal("failed initial reservation invented a published route")
	}
	if r.due(p.ID, now.Add(10*time.Second)) {
		t.Fatal("failed reservation did not back off")
	}
	reservation := &client.Reservation{Expiration: now.Add(20 * time.Second)}
	if !r.refresh(p, reservation, now) {
		t.Fatal("successful reservation was not retained")
	}
	if r.due(p.ID, now.Add(14*time.Second)) || !r.due(p.ID, now.Add(16*time.Second)) {
		t.Fatal("short reservation did not schedule renewal before expiry")
	}
	before := r.factory([]ma.Multiaddr{direct, direct})
	if len(before) != 2 || before[1].String() != direct.String() {
		t.Fatalf("publication removed, duplicated, or displaced direct endpoint: %v", before)
	}
	if !r.refresh(p, nil, now.Add(16*time.Second)) {
		t.Fatal("transient renewal failure revoked a still-live reservation")
	}
	if got := r.factory([]ma.Multiaddr{direct}); len(got) != 2 || got[0].String() != before[0].String() {
		t.Fatalf("failed renewal changed the valid circuit: %v", got)
	}
	// Expiry must be enforced by the factory even without a renewal callback.
	r.mu.Lock()
	lease := r.leases[p.ID]
	lease.expires = now.Add(-time.Second)
	r.leases[p.ID] = lease
	r.mu.Unlock()
	if got := r.factory([]ma.Multiaddr{direct}); len(got) != 1 || got[0].String() != direct.String() {
		t.Fatalf("expired reservation remained published: %v", got)
	}
	if r.refresh(p, nil, now.Add(21*time.Second)) || len(r.factory(nil)) != 0 {
		t.Fatal("failed renewal resurrected an expired reservation")
	}
	farFuture := &client.Reservation{Expiration: now.Add(time.Duration(1<<63 - 1))}
	if !r.refresh(p, farFuture, now) || r.due(p.ID, now.Add(9*time.Minute)) || !r.due(p.ID, now.Add(11*time.Minute)) {
		t.Fatal("far-future reservation overflowed or exceeded its bounded renewal interval")
	}
}
