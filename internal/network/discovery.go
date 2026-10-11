package network

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"sync"
	"time"

	libp2p "github.com/libp2p/go-libp2p"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	netcore "github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/p2p/protocol/circuitv2/client"
	"github.com/libp2p/go-libp2p/p2p/protocol/circuitv2/relay"
	ma "github.com/multiformats/go-multiaddr"
)

func (n *Node) discoverDHT(bootstrap []peer.AddrInfo) {
	ctx, cancel := context.WithTimeout(n.ctx, 20*time.Second)
	defer cancel()
	// Bootstrap dials are independently bounded. Avoid delaying registration or LAN.
	var wg sync.WaitGroup
	for i, p := range bootstrap {
		if i >= 8 {
			break
		}
		wg.Add(1)
		go func(p peer.AddrInfo) { defer wg.Done(); _ = n.host.Connect(ctx, p) }(p)
	}
	wg.Wait()
	_ = n.dht.Bootstrap(ctx)
	if err := n.dht.Provide(ctx, n.provider, true); err != nil {
		n.health("dht", "provider advertise unavailable (public acceptance pending)")
	} else {
		n.health("dht", "provider advertised (public acceptance pending)")
	}
	for p := range n.dht.FindProvidersAsync(ctx, n.provider, 32) {
		n.HandlePeerFound(p)
	}
}
func (n *Node) reserveRelays(relays []peer.AddrInfo) {
	for _, p := range relays {
		if !n.relayAddresses.due(p.ID, time.Now()) {
			continue
		}
		ctx, cancel := context.WithTimeout(n.ctx, 15*time.Second)
		reservation, err := client.Reserve(ctx, n.host, p)
		cancel()
		if err != nil {
			reservation = nil
		}
		n.relayAddresses.refresh(p, reservation, time.Now())
	}
	if len(n.relayAddresses.factory(nil)) > 0 {
		n.health("relay", "reservation active")
	} else {
		n.health("relay", "reservation unavailable; retrying")
	}
}

type discoveryRequest struct {
	Key       string   `json:"key"`
	Addresses []string `json:"addresses,omitempty"`
}
type discoveryReply struct {
	Peers []discoveryPeer `json:"peers"`
	Error string          `json:"error,omitempty"`
}
type discoveryPeer struct {
	ID        string   `json:"id"`
	Addresses []string `json:"addresses"`
}

func (n *Node) discoverServers(servers []peer.AddrInfo) {
	success := false
	for _, server := range servers {
		ctx, cancel := context.WithTimeout(n.ctx, 10*time.Second)
		s, err := n.host.NewStream(netcore.WithAllowLimitedConn(ctx, "discovery"), server.ID, DiscoveryProtocol)
		if err != nil {
			_ = n.host.Connect(ctx, server)
			s, err = n.host.NewStream(ctx, server.ID, DiscoveryProtocol)
		}
		if err == nil {
			_ = s.SetDeadline(time.Now().Add(10 * time.Second))
			req := discoveryRequest{Key: hex.EncodeToString(n.lookup), Addresses: n.Addresses()}
			if len(req.Addresses) > 16 {
				req.Addresses = req.Addresses[:16]
			}
			err = writeFrame(s, req, 16384)
			var reply discoveryReply
			if err == nil {
				err = readFrame(s, &reply, 256<<10)
			}
			_ = s.Close()
			if err == nil && reply.Error == "" && len(reply.Peers) <= 64 {
				success = true
				for _, p := range reply.Peers {
					if len(p.Addresses) > 16 {
						continue
					}
					infos, e := parsePeers(p.Addresses)
					if e != nil {
						continue
					}
					var combined peer.AddrInfo
					for _, info := range infos {
						if info.ID.String() == p.ID {
							combined.ID = info.ID
							combined.Addrs = append(combined.Addrs, info.Addrs...)
						}
					}
					// Keep all routes in one candidate. Separate announcements
					// overwrite retry state with the last (possibly direct-only)
					// address while an earlier attempt is backing off.
					n.HandlePeerFound(combined)
				}
			}
		}
		cancel()
	}
	if success {
		n.health("discovery", "registered")
	} else {
		n.health("discovery", "server unavailable; retrying")
	}
}

type ServerConfig struct {
	AllowPeers                                        []string
	Identity                                          crypto.PrivKey
	Listen, Advertise, Bootstrap                      []string
	Discovery, Relay, DHT                             bool
	RelayDataLimit                                    int64
	RelayDuration                                     time.Duration
	MaxReservations                                   int
	MaxCircuits                                       int
	RelayTotalBytesPerSecond, RelayPeerBytesPerSecond int64
	MaxRelayStreams                                   int
	MaxDiscoveryGroups, MaxDiscoveryRegistrations     int
	MaxDiscoveryRegistrationsPerPeer                  int
}
type registration struct {
	peer    discoveryPeer
	expires time.Time
}
type Server struct {
	host                        host.Host
	dht                         *dht.IpfsDHT
	relay                       *relay.Relay
	ctx                         context.Context
	cancel                      context.CancelFunc
	once                        sync.Once
	mu                          sync.Mutex
	groups                      map[string]map[peer.ID]registration
	requests                    map[peer.ID]time.Time
	slots                       chan struct{}
	maxGroups, maxRegistrations int
	maxRegistrationsPerPeer     int
	pacer                       *relayPacer
}

func Serve(ctx context.Context, cfg ServerConfig) (*Server, error) {
	if !cfg.Discovery && !cfg.Relay && !cfg.DHT {
		return nil, errors.New("enable discovery, relay, or DHT server")
	}
	if cfg.RelayDataLimit < 0 || cfg.RelayDuration < 0 || cfg.MaxReservations < 0 || cfg.MaxCircuits < 0 || cfg.RelayTotalBytesPerSecond < 0 || cfg.RelayPeerBytesPerSecond < 0 || cfg.MaxRelayStreams < 0 || cfg.MaxDiscoveryGroups < 0 || cfg.MaxDiscoveryRegistrations < 0 || cfg.MaxDiscoveryRegistrationsPerPeer < 0 {
		return nil, errors.New("server quotas must be positive")
	}
	if cfg.MaxDiscoveryGroups == 0 {
		cfg.MaxDiscoveryGroups = 1024
	}
	if cfg.MaxDiscoveryRegistrations == 0 {
		cfg.MaxDiscoveryRegistrations = 4096
	}
	if cfg.MaxDiscoveryRegistrationsPerPeer == 0 {
		cfg.MaxDiscoveryRegistrationsPerPeer = 8
	}
	if cfg.RelayTotalBytesPerSecond == 0 {
		cfg.RelayTotalBytesPerSecond = 32 << 20
	}
	if cfg.RelayPeerBytesPerSecond == 0 {
		cfg.RelayPeerBytesPerSecond = 8 << 20
	}
	if cfg.MaxRelayStreams == 0 {
		cfg.MaxRelayStreams = 512
	}
	bootstrap, err := parsePeers(cfg.Bootstrap)
	if err != nil {
		return nil, err
	}
	if len(cfg.AllowPeers) > 256 {
		return nil, errors.New("relay allowlist exceeds 256 peer identities")
	}
	allowed := relayAllowlist{}
	for _, raw := range cfg.AllowPeers {
		id, err := peer.Decode(raw)
		if err != nil {
			return nil, errors.New("invalid relay allowlist peer identity")
		}
		allowed[id] = true
	}
	opts := hostOptions(cfg.Identity, cfg.Listen)
	opts = append(opts, libp2p.ForceReachabilityPublic(), libp2p.EnableNATService(), libp2p.AutoNATServiceRateLimit(30, 3, time.Minute))
	if len(cfg.Advertise) > 0 {
		var addrs []ma.Multiaddr
		for _, a := range cfg.Advertise {
			addr, e := ma.NewMultiaddr(a)
			if e != nil {
				return nil, errors.New("invalid advertised address")
			}
			addrs = append(addrs, addr)
		}
		opts = append(opts, libp2p.AddrsFactory(func([]ma.Multiaddr) []ma.Multiaddr { return addrs }))
	}
	h, err := libp2p.New(opts...)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(ctx)
	s := &Server{host: h, ctx: ctx, cancel: cancel, groups: map[string]map[peer.ID]registration{}, requests: map[peer.ID]time.Time{}, slots: make(chan struct{}, 32), maxGroups: cfg.MaxDiscoveryGroups, maxRegistrations: cfg.MaxDiscoveryRegistrations}
	s.maxRegistrationsPerPeer = cfg.MaxDiscoveryRegistrationsPerPeer
	if cfg.Discovery {
		h.SetStreamHandler(DiscoveryProtocol, s.handleDiscovery)
	}
	if cfg.Relay {
		r := relay.DefaultResources()
		r.MaxReservations = cfg.MaxReservations
		if r.MaxReservations <= 0 {
			r.MaxReservations = 64
		}
		r.MaxCircuits = cfg.MaxCircuits
		if r.MaxCircuits == 0 {
			r.MaxCircuits = 4
		}
		r.MaxReservationsPerIP = 8
		r.Limit = &relay.RelayLimit{Duration: cfg.RelayDuration, Data: cfg.RelayDataLimit}
		if r.Limit.Duration <= 0 {
			r.Limit.Duration = 5 * time.Minute
		}
		if r.Limit.Data <= 0 {
			r.Limit.Data = 256 << 20
		}
		s.pacer = newRelayPacer(ctx, h, cfg.RelayTotalBytesPerSecond, cfg.RelayPeerBytesPerSecond, cfg.MaxRelayStreams)
		options := []relay.Option{relay.WithResources(r)}
		if len(allowed) > 0 {
			options = append(options, relay.WithACL(allowed))
		}
		s.relay, err = relay.New(s.pacer, options...)
		if err != nil {
			s.Close()
			return nil, err
		}
	}
	if cfg.DHT {
		s.dht, err = dht.New(ctx, h, dht.Mode(dht.ModeServer), dht.BootstrapPeers(bootstrap...))
		if err != nil {
			s.Close()
			return nil, err
		}
		_ = s.dht.Bootstrap(ctx)
	}
	return s, nil
}
func (s *Server) PeerID() string      { return s.host.ID().String() }
func (s *Server) Addresses() []string { return addresses(s.host) }

// ServerStats exposes resource usage without lookup labels, file paths, or secrets.
type ServerStats struct {
	DiscoveryGroups          int    `json:"discovery_groups"`
	DiscoveryRegistrations   int    `json:"discovery_registrations"`
	RelayStreams             int    `json:"relay_streams"`
	RelayPeers               int    `json:"relay_peers"`
	RelayForwardedBytes      uint64 `json:"relay_forwarded_bytes"`
	RelayTotalBytesPerSecond int64  `json:"relay_total_bytes_per_second"`
	RelayPeerBytesPerSecond  int64  `json:"relay_peer_bytes_per_second"`
}

func (s *Server) Stats() ServerStats {
	var out ServerStats
	s.mu.Lock()
	for _, group := range s.groups {
		count := 0
		for _, registration := range group {
			if time.Now().Before(registration.expires) {
				count++
			}
		}
		if count > 0 {
			out.DiscoveryGroups++
		}
		out.DiscoveryRegistrations += count
	}
	s.mu.Unlock()
	if s.pacer != nil {
		s.pacer.mu.Lock()
		out.RelayStreams, out.RelayPeers = s.pacer.streams, len(s.pacer.peers)
		s.pacer.mu.Unlock()
		out.RelayForwardedBytes = s.pacer.forwarded.Load()
		out.RelayTotalBytesPerSecond, out.RelayPeerBytesPerSecond = s.pacer.total.rate, s.pacer.peerRate
	}
	return out
}

func (s *Server) Close() error {
	var err error
	s.once.Do(func() {
		s.cancel()
		if s.relay != nil {
			_ = s.relay.Close()
		}
		if s.dht != nil {
			_ = s.dht.Close()
		}
		err = s.host.Close()
	})
	return err
}
func (s *Server) handleDiscovery(stream netcore.Stream) {
	select {
	case s.slots <- struct{}{}:
	default:
		_ = stream.Reset()
		return
	}
	defer func() { <-s.slots }()
	defer stream.Close()
	_ = stream.SetDeadline(time.Now().Add(10 * time.Second))
	var req discoveryRequest
	if readFrame(stream, &req, 16384) != nil || len(req.Key) != 64 || len(req.Addresses) > 16 {
		_ = stream.Reset()
		return
	}
	if _, e := hex.DecodeString(req.Key); e != nil {
		_ = stream.Reset()
		return
	}
	id := stream.Conn().RemotePeer()
	now := time.Now()
	var valid []string
	for _, a := range req.Addresses {
		if len(a) > 512 {
			continue
		}
		infos, e := parsePeers([]string{a})
		if e == nil && len(infos) == 1 && infos[0].ID == id && len(infos[0].Addrs) == 1 && validContactAddress(infos[0].Addrs[0]) {
			valid = append(valid, a)
		}
	}
	reply := s.register(now, id, req.Key, valid)
	// Copy and accounting complete under the table lock; slow consumers cannot
	// block registration/expiry or turn the shared table into a network mutex.
	_ = writeFrame(stream, reply, 256<<10)
}
func (s *Server) register(now time.Time, id peer.ID, key string, valid []string) discoveryReply {
	s.mu.Lock()
	defer s.mu.Unlock()
	total, peerTotal := 0, 0
	for key, group := range s.groups {
		for p, r := range group {
			if !now.Before(r.expires) {
				delete(group, p)
			}
		}
		if len(group) == 0 {
			delete(s.groups, key)
		} else {
			total += len(group)
			if _, registered := group[id]; registered {
				peerTotal++
			}
		}
	}
	for p, last := range s.requests {
		if now.Sub(last) > 2*time.Minute {
			delete(s.requests, p)
		}
	}
	last, tracked := s.requests[id]
	if (tracked && now.Sub(last) < time.Second) || (!tracked && len(s.requests) >= 4096) {
		return discoveryReply{Error: "rate limit"}
	}
	s.requests[id] = now
	group := s.groups[key]
	if group == nil {
		if len(s.groups) >= s.maxGroups {
			return discoveryReply{Error: "capacity limit"}
		}
		if len(valid) > 0 {
			group = map[peer.ID]registration{}
			s.groups[key] = group
		}
	}
	if len(valid) > 0 {
		_, existing := group[id]
		if !existing && (len(group) >= 64 || total >= s.maxRegistrations || peerTotal >= s.maxRegistrationsPerPeer) {
			return discoveryReply{Error: "capacity limit"}
		}
		group[id] = registration{discoveryPeer{id.String(), append([]string(nil), valid...)}, now.Add(2 * time.Minute)}
	}
	reply := discoveryReply{Peers: []discoveryPeer{}}
	bytes := 0
	for p, r := range group {
		if p == id {
			continue
		}
		encoded, _ := json.Marshal(r.peer)
		if bytes+len(encoded) > 240<<10 {
			break
		}
		bytes += len(encoded) + 1
		reply.Peers = append(reply.Peers, r.peer)
	}
	return reply
}
