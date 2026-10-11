package network

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/happysync/happysync/internal/model"
	libp2p "github.com/libp2p/go-libp2p"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	"github.com/libp2p/go-libp2p/core/control"
	netcore "github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/peerstore"
	"github.com/libp2p/go-libp2p/p2p/protocol/circuitv2/client"
	ma "github.com/multiformats/go-multiaddr"
	"github.com/zeebo/blake3"
)

type testBackend struct {
	data  []byte
	calls atomic.Int32
}

func (b *testBackend) Snapshot(context.Context, string, int) ([]model.Record, string, error) {
	b.calls.Add(1)
	return []model.Record{{Path: "test.txt"}}, "", nil
}
func (b *testBackend) Receive(context.Context, []model.Record) error { return nil }
func (b *testBackend) ReadBlock(_ context.Context, hash string, size int64) ([]byte, error) {
	b.calls.Add(1)
	if hash != fmt.Sprintf("%x", blake3.Sum256(b.data)) || size != int64(len(b.data)) {
		return nil, errors.New("missing")
	}
	return b.data, nil
}
func (b *testBackend) HasBlock(_ context.Context, hash string, size int64) bool {
	return hash == fmt.Sprintf("%x", blake3.Sum256(b.data)) && size == int64(len(b.data))
}
func testConfig() Config {
	return Config{SyncID: bytes.Repeat([]byte{1}, 16), Secret: bytes.Repeat([]byte{2}, 32), Listen: []string{"/ip4/127.0.0.1/tcp/0"}, DisableMDNS: true, DisableDHT: true}
}
func testNode(t *testing.T, cfg Config) *Node {
	t.Helper()
	n, e := New(context.Background(), cfg)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { _ = n.Close() })
	return n
}
func connect(t *testing.T, a, b *Node) {
	t.Helper()
	infos, e := parsePeers(b.Addresses())
	if e != nil {
		t.Fatal(e)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if e = a.host.Connect(ctx, infos[0]); e != nil {
		t.Fatal(e)
	}
}
func TestDirectAuthenticatedStreams(t *testing.T) {
	for _, listen := range []string{"/ip4/127.0.0.1/tcp/0", "/ip4/127.0.0.1/udp/0/quic-v1"} {
		t.Run(listen, func(t *testing.T) {
			cfg := testConfig()
			cfg.Listen = []string{listen}
			a := testNode(t, cfg)
			b := testNode(t, cfg)
			backend := &testBackend{data: bytes.Repeat([]byte("content"), 10000)}
			b.SetBackend(backend)
			connect(t, a, b)
			records, cursor, e := a.FetchRecords(context.Background(), b.host.ID(), "", 128)
			if e != nil || len(records) != 1 || cursor != "" {
				t.Fatalf("snapshot: %v %v", records, e)
			}
			hash := fmt.Sprintf("%x", blake3.Sum256(backend.data))
			blocks := []model.Block{{Hash: hash, Size: int64(len(backend.data))}, {Hash: strings.Repeat("0", 64), Size: 4}}
			available, e := a.BlockAvailability(context.Background(), b.host.ID(), blocks)
			if e != nil || !available[0] || available[1] {
				t.Fatalf("availability %v %v", available, e)
			}
			got, e := a.FetchBlock(context.Background(), b.host.ID(), hash, int64(len(backend.data)))
			if e != nil || !bytes.Equal(got, backend.data) {
				t.Fatalf("block integrity: %v", e)
			}
			if a.Stats().Direct != 1 || a.Stats().ReceivedBytes != uint64(len(got)) {
				t.Fatalf("stats: %+v", a.Stats())
			}
		})
	}
}
func TestWrongGroupNoMetadataOrContent(t *testing.T) {
	a := testNode(t, testConfig())
	cfg := testConfig()
	cfg.Secret = bytes.Repeat([]byte{3}, 32)
	b := testNode(t, cfg)
	backend := &testBackend{data: []byte("secret")}
	b.SetBackend(backend)
	connect(t, a, b)
	_, _, e := a.FetchRecords(context.Background(), b.host.ID(), "", 1)
	if e == nil {
		t.Fatal("wrong key accepted")
	}
	_, e = a.FetchBlock(context.Background(), b.host.ID(), fmt.Sprintf("%x", blake3.Sum256(backend.data)), 6)
	if e == nil {
		t.Fatal("wrong key read content")
	}
	if backend.calls.Load() != 0 || len(a.Peers()) != 0 || len(b.Peers()) != 0 {
		t.Fatal("unauthenticated peer accessed backend")
	}
}
func TestReplayAndReflectionRejected(t *testing.T) {
	a := testNode(t, testConfig())
	b := testNode(t, testConfig())
	connect(t, a, b)
	var proof authMessage
	for i := 0; i < 2; i++ {
		s, e := a.host.NewStream(context.Background(), b.host.ID(), AuthProtocol)
		if e != nil {
			t.Fatal(e)
		}
		var hello authMessage
		if e = readFrame(s, &hello, 1024); e != nil {
			t.Fatal(e)
		}
		if i == 0 {
			nonce := bytes.Repeat([]byte{4}, 32)
			proof = authMessage{Version: 1, Nonce: nonce, MAC: authMAC(a.key, s, true, nonce, hello.Nonce, "initiator")}
		}
		if e = writeFrame(s, proof, 1024); e != nil {
			t.Fatal(e)
		}
		var response authMessage
		e = readFrame(s, &response, 1024)
		_ = s.Close()
		if (i == 0) != (e == nil) {
			t.Fatalf("replayed proof acceptance: %v", e)
		}
	}
	s, e := a.host.NewStream(context.Background(), b.host.ID(), AuthProtocol)
	if e != nil {
		t.Fatal(e)
	}
	var hello authMessage
	_ = readFrame(s, &hello, 1024)
	nonce := bytes.Repeat([]byte{5}, 32)
	_ = writeFrame(s, authMessage{Version: 1, Nonce: nonce, MAC: authMAC(a.key, s, true, nonce, hello.Nonce, "responder")}, 1024)
	var response authMessage
	if readFrame(s, &response, 1024) == nil {
		t.Fatal("reflected role accepted")
	}
	_ = s.Close()
}
func TestWireBounds(t *testing.T) {
	for _, wire := range [][]byte{{255, 255, 255, 255}, {0, 0, 0, 1, '{'}, {0, 0, 0, 2, '{', '}'}, {0, 0, 0, 8, '{', '"', 'x', '"', ':', '1', '}', ' '}} {
		var req blockRequest
		e := readFrame(bytes.NewReader(wire), &req, 10)
		if bytes.Equal(wire, []byte{0, 0, 0, 2, '{', '}'}) {
			if e != nil {
				t.Fatal(e)
			}
		} else if e == nil {
			t.Fatal("malformed wire accepted")
		}
	}
	var buf bytes.Buffer
	if writeFrame(&buf, strings.Repeat("x", 20), 10) == nil {
		t.Fatal("oversize write accepted")
	}
}
func TestCustomDiscoveryRegistrationAndGroupIsolation(t *testing.T) {
	srv, e := Serve(context.Background(), ServerConfig{Discovery: true, Listen: []string{"/ip4/127.0.0.1/tcp/0"}})
	if e != nil {
		t.Fatal(e)
	}
	defer srv.Close()
	cfg := testConfig()
	a := testNode(t, cfg)
	b := testNode(t, cfg)
	otherCfg := cfg
	otherCfg.Secret = bytes.Repeat([]byte{7}, 32)
	other := testNode(t, otherCfg)
	servers, e := parsePeers(srv.Addresses())
	if e != nil {
		t.Fatal(e)
	}
	a.discoverServers(servers)
	b.discoverServers(servers)
	other.discoverServers(servers)
	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) && len(b.Peers()) == 0 {
		time.Sleep(20 * time.Millisecond)
	}
	if len(b.Peers()) != 1 || b.Peers()[0] != a.host.ID() {
		t.Fatalf("discovery failed %+v", b.Stats())
	}
	if len(other.Peers()) != 0 {
		t.Fatal("group isolation failed")
	}
	srv.mu.Lock()
	if len(srv.groups[hex.EncodeToString(a.lookup)]) != 2 {
		t.Fatal("did not register both peers")
	}
	for key, group := range srv.groups {
		for p, r := range group {
			r.expires = time.Now().Add(-time.Second)
			group[p] = r
		}
		srv.groups[key] = group
	}
	srv.mu.Unlock()
	time.Sleep(time.Second)
	a.discoverServers(servers)
	srv.mu.Lock()
	defer srv.mu.Unlock()
	if len(srv.groups) != 1 || len(srv.groups[hex.EncodeToString(a.lookup)]) != 1 {
		t.Fatal("registrations did not expire")
	}
}

type relayOnlyGater struct{ relay peer.ID }

func (g relayOnlyGater) InterceptPeerDial(peer.ID) bool { return true }
func (g relayOnlyGater) InterceptAddrDial(p peer.ID, a ma.Multiaddr) bool {
	return p == g.relay || strings.Contains(a.String(), "/p2p-circuit")
}
func (g relayOnlyGater) InterceptAccept(netcore.ConnMultiaddrs) bool { return true }
func (g relayOnlyGater) InterceptSecured(_ netcore.Direction, p peer.ID, a netcore.ConnMultiaddrs) bool {
	return p == g.relay || strings.Contains(a.RemoteMultiaddr().String(), "/p2p-circuit")
}
func (g relayOnlyGater) InterceptUpgraded(netcore.Conn) (bool, control.DisconnectReason) {
	return true, 0
}
func TestForcedCircuitRelayEncryptedBlock(t *testing.T) {
	srv, e := Serve(context.Background(), ServerConfig{Relay: true, Listen: []string{"/ip4/127.0.0.1/tcp/0"}})
	if e != nil {
		t.Fatal(e)
	}
	defer srv.Close()
	b := testNode(t, testConfig())
	backend := &testBackend{data: bytes.Repeat([]byte("relay opaque bytes"), 100000)}
	b.SetBackend(backend)
	infos, e := parsePeers(srv.Addresses())
	if e != nil {
		t.Fatal(e)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if _, e = client.Reserve(ctx, b.host, infos[0]); e != nil {
		t.Fatal(e)
	}
	a, e := newNode(context.Background(), testConfig(), libp2p.ConnectionGater(relayOnlyGater{srv.host.ID()}))
	if e != nil {
		t.Fatal(e)
	}
	defer a.Close()
	relayAddr, e := ma.NewMultiaddr(srv.Addresses()[0] + "/p2p-circuit/p2p/" + b.host.ID().String())
	if e != nil {
		t.Fatal(e)
	}
	target, e := peer.AddrInfoFromP2pAddr(relayAddr)
	if e != nil {
		t.Fatal(e)
	}
	a.host.Peerstore().AddAddrs(target.ID, target.Addrs, peerstore.TempAddrTTL)
	got, e := a.FetchBlock(ctx, b.host.ID(), fmt.Sprintf("%x", blake3.Sum256(backend.data)), int64(len(backend.data)))
	if e != nil || !bytes.Equal(got, backend.data) {
		t.Fatalf("relay transfer: %v", e)
	}
	if a.Stats().Relayed != 1 || a.Stats().Direct != 0 {
		t.Fatalf("test escaped relay: %+v", a.Stats())
	}
}
func TestDiscoveryCIDVector(t *testing.T) {
	cfg := testConfig()
	if hex.EncodeToString(derive(cfg.Secret, cfg.SyncID, "HappySync/v1/discovery")) != "ec40cff3c37231049096925983d634b86842271631b859037f233197465e6637" {
		t.Fatal("discovery HKDF vector")
	}
	if hex.EncodeToString(derive(cfg.Secret, cfg.SyncID, "HappySync/v1/auth")) != "677d54f9295005c684033377be0be95f4d32f154a1b0c83779641b7c2927e8b3" {
		t.Fatal("authentication HKDF vector")
	}
	got := DiscoveryCID(cfg.Secret, cfg.SyncID).String()
	t.Logf("discovery CID vector %s", got)
	if got != "bafkreihmidh7hq3sgecjbfuslgb5mnfynbbcofrrxbmqg7zdgglumxtgg4" || got == DiscoveryCID(bytes.Repeat([]byte{3}, 32), cfg.SyncID).String() {
		t.Fatal("namespace derivation")
	}
}
func TestLocalMDNS(t *testing.T) {
	if os.Getenv("HAPPYSYNC_MDNS_TEST") != "1" {
		t.Skip("set HAPPYSYNC_MDNS_TEST=1 on a host with local multicast available; hosted CI does not guarantee multicast routing")
	}
	cfg := testConfig()
	cfg.DisableMDNS = false
	cfg.Listen = []string{"/ip4/0.0.0.0/udp/0/quic-v1"}
	a := testNode(t, cfg)
	b := testNode(t, cfg)
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) && (len(a.Peers()) == 0 || len(b.Peers()) == 0) {
		time.Sleep(50 * time.Millisecond)
	}
	if len(a.Peers()) != 1 || len(b.Peers()) != 1 {
		t.Fatalf("local multicast discovery failed: A=%+v B=%+v", a.Stats(), b.Stats())
	}
	backend := &testBackend{data: []byte("verified content over an mDNS-discovered peer")}
	b.SetBackend(backend)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	got, err := a.FetchBlock(ctx, b.host.ID(), fmt.Sprintf("%x", blake3.Sum256(backend.data)), int64(len(backend.data)))
	if err != nil || !bytes.Equal(got, backend.data) {
		t.Fatalf("mDNS-discovered encrypted transfer: %v", err)
	}
}
func TestLocalDHTProviderDiscovery(t *testing.T) {
	// Private loopback peers prove provider protocol mechanics only. Public routing
	// and consumer NAT acceptance deliberately remain a separate external gate.
	a := testNode(t, testConfig())
	b := testNode(t, testConfig())
	bootstrap := testNode(t, testConfig())
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	var routers []*dht.IpfsDHT
	for _, n := range []*Node{a, b, bootstrap} {
		router, e := dht.New(ctx, n.host, dht.Mode(dht.ModeServer), dht.AddressFilter(func(addrs []ma.Multiaddr) []ma.Multiaddr { return addrs }))
		if e != nil {
			t.Fatal(e)
		}
		defer router.Close()
		routers = append(routers, router)
	}
	connect(t, a, bootstrap)
	connect(t, b, bootstrap)
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) && (routers[0].RoutingTable().Size() == 0 || routers[1].RoutingTable().Size() == 0) {
		time.Sleep(20 * time.Millisecond)
	}
	if e := routers[0].Provide(ctx, a.provider, true); e != nil {
		t.Fatal(e)
	}
	found := false
	for p := range routers[1].FindProvidersAsync(ctx, b.provider, 10) {
		if p.ID == a.host.ID() {
			found = true
			b.HandlePeerFound(p)
		}
	}
	if !found {
		t.Fatal("local DHT provider not discovered")
	}
	deadline = time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) && len(b.Peers()) == 0 {
		time.Sleep(20 * time.Millisecond)
	}
	if len(b.Peers()) != 1 {
		t.Fatalf("DHT candidate not authenticated: %+v", b.Stats())
	}
}
