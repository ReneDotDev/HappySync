package network

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	libp2p "github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/peerstore"
	"github.com/libp2p/go-libp2p/p2p/protocol/circuitv2/client"
	ma "github.com/multiformats/go-multiaddr"
	"github.com/zeebo/blake3"
)

func TestDiscoveryCapacityRateAndExpiry(t *testing.T) {
	s := &Server{groups: map[string]map[peer.ID]registration{}, requests: map[peer.ID]time.Time{}, maxGroups: 2, maxRegistrations: 2, maxRegistrationsPerPeer: 1}
	now := time.Now()
	addr := []string{"opaque-address-used-only-by-table-test"}
	if r := s.register(now, "a", "one", addr); r.Error != "" {
		t.Fatal(r.Error)
	}
	if r := s.register(now.Add(time.Millisecond), "a", "one", addr); r.Error != "rate limit" {
		t.Fatalf("rate result %+v", r)
	}
	if r := s.register(now.Add(2*time.Second), "a", "two", addr); r.Error != "capacity limit" {
		t.Fatalf("peer quota %+v", r)
	}
	if r := s.register(now.Add(2*time.Second), "b", "two", addr); r.Error != "" {
		t.Fatal(r.Error)
	}
	if r := s.register(now.Add(2*time.Second), "c", "three", addr); r.Error != "capacity limit" {
		t.Fatalf("group quota %+v", r)
	}
	if r := s.register(now.Add(3*time.Second), "c", "one", addr); r.Error != "capacity limit" {
		t.Fatalf("global quota %+v", r)
	}
	if r := s.register(now.Add(3*time.Minute), "c", "three", addr); r.Error != "" {
		t.Fatalf("expiry did not free capacity %+v", r)
	}
	if len(s.groups) != 1 || len(s.requests) != 1 {
		t.Fatalf("tables retained stale entries: groups=%d requests=%d", len(s.groups), len(s.requests))
	}
}

func TestAddressValidationAndRetryBounds(t *testing.T) {
	for _, raw := range []string{"/ip4/0.0.0.0/tcp/4", "/ip4/224.0.0.1/tcp/4", "/ip4/127.0.0.1/udp/4", "/ip4/127.0.0.1/tcp/4/p2p-circuit/p2p-circuit"} {
		addr, err := ma.NewMultiaddr(raw)
		if err != nil {
			t.Fatal(err)
		}
		if validContactAddress(addr) {
			t.Fatalf("unsafe contact accepted %s", raw)
		}
	}
	for failures := uint(0); failures < 16; failures++ {
		for range 10 {
			d := retryDelay(failures)
			if d < 2500*time.Millisecond || d > 5*time.Minute {
				t.Fatalf("retry outside bounds %v", d)
			}
		}
	}
	if _, err := Serve(context.Background(), ServerConfig{Relay: true, RelayPeerBytesPerSecond: -1}); err == nil {
		t.Fatal("negative quota accepted")
	}
}

func relayFetch(t *testing.T, cfg ServerConfig, data []byte) ([]byte, error, time.Duration) {
	t.Helper()
	cfg.Relay = true
	cfg.Listen = []string{"/ip4/127.0.0.1/tcp/0"}
	srv, err := Serve(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()
	b := testNode(t, testConfig())
	b.SetBackend(&testBackend{data: data})
	infos, err := parsePeers(srv.Addresses())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	if _, err = client.Reserve(ctx, b.host, infos[0]); err != nil {
		t.Fatal(err)
	}
	a, err := newNode(context.Background(), testConfig(), libp2p.ConnectionGater(relayOnlyGater{srv.host.ID()}))
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	addr, _ := ma.NewMultiaddr(srv.Addresses()[0] + "/p2p-circuit/p2p/" + b.host.ID().String())
	target, _ := peer.AddrInfoFromP2pAddr(addr)
	a.host.Peerstore().AddAddrs(target.ID, target.Addrs, peerstore.TempAddrTTL)
	start := time.Now()
	got, err := a.FetchBlock(ctx, b.host.ID(), fmt.Sprintf("%x", blake3.Sum256(data)), int64(len(data)))
	if err == nil && a.Stats().Relayed != 1 {
		t.Fatal("quota test escaped relay")
	}
	if err == nil {
		stats := srv.Stats()
		if stats.RelayForwardedBytes < uint64(len(data)) || stats.RelayTotalBytesPerSecond <= 0 || stats.RelayPeerBytesPerSecond <= 0 {
			t.Fatalf("missing relay resource telemetry %+v", stats)
		}
	}
	return got, err, time.Since(start)
}

func TestRelayPacedBandwidth(t *testing.T) {
	data := bytes.Repeat([]byte("paced opaque bytes"), 4096)
	for _, test := range []struct {
		name           string
		total, perPeer int64
	}{{"global", 128 << 10, 8 << 20}, {"per-peer", 32 << 20, 128 << 10}} {
		t.Run(test.name, func(t *testing.T) {
			got, err, elapsed := relayFetch(t, ServerConfig{RelayTotalBytesPerSecond: test.total, RelayPeerBytesPerSecond: test.perPeer}, data)
			if err != nil || !bytes.Equal(got, data) {
				t.Fatalf("paced encrypted transfer: %v", err)
			}
			// Wire data adds framing and encryption; payload duration is a conservative floor.
			minimum := time.Duration(int64(len(data)) * int64(time.Second) / (128 << 10))
			if elapsed < minimum {
				t.Fatalf("bandwidth quota bypassed: elapsed=%v minimum=%v", elapsed, minimum)
			}
		})
	}
}

func TestRelaySessionQuotaFailure(t *testing.T) {
	data := bytes.Repeat([]byte("quota"), 32768)
	for _, cfg := range []ServerConfig{{RelayDataLimit: 4096}, {MaxRelayStreams: 1}} {
		got, err, _ := relayFetch(t, cfg, data)
		if err == nil || len(got) != 0 {
			t.Fatalf("quota must reject complete block got=%d err=%v", len(got), err)
		}
	}
}

func TestRelayReservationCapacity(t *testing.T) {
	srv, err := Serve(context.Background(), ServerConfig{Relay: true, MaxReservations: 1, Listen: []string{"/ip4/127.0.0.1/tcp/0"}})
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()
	infos, _ := parsePeers(srv.Addresses())
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for i := 0; i < 2; i++ {
		n := testNode(t, testConfig())
		_, err := client.Reserve(ctx, n.host, infos[0])
		if (i == 0) != (err == nil) {
			t.Fatalf("reservation %d error=%v", i, err)
		}
	}
}

func TestOneAnnouncementRetriesTransientDial(t *testing.T) {
	a := testNode(t, testConfig())
	b := testNode(t, testConfig())
	infos, _ := parsePeers(b.Addresses())
	a.mu.Lock()
	a.attempts[b.host.ID()] = retryState{next: time.Now().Add(1100 * time.Millisecond), failures: 1}
	a.mu.Unlock()
	a.HandlePeerFound(infos[0])
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) && len(a.Peers()) == 0 {
		time.Sleep(20 * time.Millisecond)
	}
	if len(a.Peers()) != 1 {
		t.Fatalf("one-time announcement was not retried %+v", a.Stats())
	}
}

func TestMalformedMembershipAttemptsBounded(t *testing.T) {
	a := testNode(t, testConfig())
	cfg := testConfig()
	cfg.Secret = bytes.Repeat([]byte{8}, 32)
	b := testNode(t, cfg)
	connect(t, a, b)
	for range 16 {
		_, _, _ = a.FetchRecords(context.Background(), b.host.ID(), "", 1)
	}
	b.mu.RLock()
	defer b.mu.RUnlock()
	if len(b.failedAuth[a.host.ID()]) != 12 || len(b.incoming) != 0 || len(b.authenticated) != 0 {
		t.Fatalf("failed auth accounting %+v", b.failedAuth)
	}
	if strings.Contains(b.stats.LastError, "hsync1:") {
		t.Fatal("secret status")
	}
}

func TestRelayOperatorAllowlist(t *testing.T) {
	a := testNode(t, testConfig())
	b := testNode(t, testConfig())
	outsider := testNode(t, testConfig())
	data := []byte("allowed encrypted payload")
	b.SetBackend(&testBackend{data: data})
	srv, err := Serve(context.Background(), ServerConfig{Relay: true, AllowPeers: []string{a.PeerID(), b.PeerID()}, Listen: []string{"/ip4/127.0.0.1/tcp/0"}})
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	infos, _ := parsePeers(srv.Addresses())
	if _, err := client.Reserve(ctx, b.host, infos[0]); err != nil {
		t.Fatal(err)
	}
	if _, err := client.Reserve(ctx, outsider.host, infos[0]); err == nil {
		t.Fatal("outsider reservation accepted")
	}
	addr, _ := ma.NewMultiaddr(srv.Addresses()[0] + "/p2p-circuit/p2p/" + b.PeerID())
	target, _ := peer.AddrInfoFromP2pAddr(addr)
	for _, source := range []*Node{a, outsider} {
		source.host.Peerstore().AddAddrs(target.ID, target.Addrs, peerstore.TempAddrTTL)
		got, err := source.FetchBlock(ctx, b.host.ID(), fmt.Sprintf("%x", blake3.Sum256(data)), int64(len(data)))
		if source == a && (err != nil || !bytes.Equal(got, data) || source.Stats().Relayed != 1) {
			t.Fatalf("allowed circuit failed: %v", err)
		}
		if source == outsider && (err == nil || len(got) != 0) {
			t.Fatal("outsider relayed connection accepted")
		}
	}
	acl := relayAllowlist{a.host.ID(): true, b.host.ID(): true}
	if acl.AllowConnect(a.host.ID(), nil, outsider.host.ID()) {
		t.Fatal("unlisted destination permitted")
	}
	if _, err := Serve(context.Background(), ServerConfig{Relay: true, AllowPeers: []string{"malformed"}}); err == nil {
		t.Fatal("invalid allowlist accepted")
	}
}

func TestClientBandwidthLimitsAndConcurrentBudget(t *testing.T) {
	data := bytes.Repeat([]byte("client bandwidth"), 4096)
	for _, direction := range []string{"upload", "download"} {
		t.Run(direction, func(t *testing.T) {
			acfg, bcfg := testConfig(), testConfig()
			if direction == "upload" {
				bcfg.UploadBytesPerSecond = 256 << 10
			} else {
				acfg.DownloadBytesPerSecond = 256 << 10
			}
			a, b := testNode(t, acfg), testNode(t, bcfg)
			b.SetBackend(&testBackend{data: data})
			connect(t, a, b)
			var wg sync.WaitGroup
			errors := make(chan error, 2)
			start := time.Now()
			for range 2 {
				wg.Add(1)
				go func() {
					defer wg.Done()
					got, err := a.FetchBlock(context.Background(), b.host.ID(), fmt.Sprintf("%x", blake3.Sum256(data)), int64(len(data)))
					if err == nil && !bytes.Equal(got, data) {
						err = fmt.Errorf("incorrect throttled bytes")
					}
					errors <- err
				}()
			}
			wg.Wait()
			for range 2 {
				if err := <-errors; err != nil {
					t.Fatal(err)
				}
			}
			minimum := time.Duration(int64(2*len(data)) * int64(time.Second) / (256 << 10))
			if time.Since(start) < minimum {
				t.Fatal("concurrent streams bypassed shared client quota")
			}
			if a.Stats().DownloadRateLimit != acfg.DownloadBytesPerSecond || b.Stats().UploadRateLimit != bcfg.UploadBytesPerSecond {
				t.Fatal("client bandwidth limits absent from telemetry")
			}
		})
	}
}

func TestClientBandwidthCancellationAndValidation(t *testing.T) {
	cfg := testConfig()
	cfg.UploadBytesPerSecond = -1
	if n, err := New(context.Background(), cfg); err == nil {
		n.Close()
		t.Fatal("negative client quota accepted")
	}
	cfg = testConfig()
	cfg.DownloadBytesPerSecond = 1
	a, b := testNode(t, cfg), testNode(t, testConfig())
	data := bytes.Repeat([]byte("cancel"), 1024)
	b.SetBackend(&testBackend{data: data})
	connect(t, a, b)
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	start := time.Now()
	if got, err := a.FetchBlock(ctx, b.host.ID(), fmt.Sprintf("%x", blake3.Sum256(data)), int64(len(data))); err == nil || len(got) > 0 {
		t.Fatal("canceled throttled transfer returned content")
	}
	if time.Since(start) > time.Second {
		t.Fatal("client limiter ignored context cancellation")
	}
}
