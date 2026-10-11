// Package network implements bounded authenticated libp2p streams and discovery.
package network

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"

	"github.com/happysync/happysync/internal/model"
	"github.com/ipfs/go-cid"
	libp2p "github.com/libp2p/go-libp2p"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	netcore "github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	"github.com/libp2p/go-libp2p/p2p/discovery/mdns"
	quic "github.com/libp2p/go-libp2p/p2p/transport/quic"
	"github.com/libp2p/go-libp2p/p2p/transport/tcp"
	ma "github.com/multiformats/go-multiaddr"
	mh "github.com/multiformats/go-multihash"
	"github.com/zeebo/blake3"
)

type Config struct {
	UploadBytesPerSecond, DownloadBytesPerSecond       int64
	Identity                                           crypto.PrivKey
	SyncID, Secret                                     []byte
	Listen, Peers, Bootstrap, Relays, DiscoveryServers []string
	DisableMDNS, DisableDHT                            bool
}
type Backend interface {
	Snapshot(context.Context, string, int) ([]model.Record, string, error)
	Receive(context.Context, []model.Record) error
	ReadBlock(context.Context, string, int64) ([]byte, error)
}
type Stats struct {
	UploadRateLimit   int64  `json:"upload_limit_bytes_per_second"`
	DownloadRateLimit int64  `json:"download_limit_bytes_per_second"`
	Discovered        int    `json:"discovered"`
	Authenticated     int    `json:"authenticated"`
	Connected         int    `json:"connected"`
	Direct            int    `json:"direct"`
	Relayed           int    `json:"relayed"`
	ReceivedBytes     uint64 `json:"received_bytes"`
	SentBytes         uint64 `json:"sent_bytes"`
	MDNS              string `json:"mdns"`
	DHT               string `json:"dht"`
	Discovery         string `json:"discovery"`
	Relay             string `json:"relay"`
	LastError         string `json:"last_error,omitempty"`
}
type Node struct {
	upload, download pacedBudget
	host             host.Host
	ctx              context.Context
	cancel           context.CancelFunc
	wg               sync.WaitGroup
	closeOnce        sync.Once
	key, lookup      []byte
	provider         cid.Cid
	relayAddresses   *relayAddresses
	dht              *dht.IpfsDHT
	mdns             mdns.Service
	mu               sync.RWMutex
	backend          Backend
	authenticated    map[peer.ID]bool
	candidates       map[peer.ID]time.Time
	candidatePeers   map[peer.ID]peer.AddrInfo
	attempts         map[peer.ID]retryState
	failedAuth       map[peer.ID][]time.Time
	incoming         map[peer.ID]int
	authWindow       time.Time
	authCount        int
	stats            Stats
	queue            chan peer.AddrInfo
	slots            chan struct{}
	outgoing         chan struct{}
	received, sent   atomic.Uint64
}

func parsePeers(addresses []string) ([]peer.AddrInfo, error) {
	if len(addresses) > 256 {
		return nil, errors.New("too many peer addresses")
	}
	var out []peer.AddrInfo
	for _, str := range addresses {
		if len(str) > 512 {
			return nil, errors.New("peer address exceeds limit")
		}
		a, err := ma.NewMultiaddr(str)
		if err != nil {
			return nil, errors.New("invalid peer multiaddress")
		}
		p, err := peer.AddrInfoFromP2pAddr(a)
		if err != nil {
			return nil, errors.New("peer address requires /p2p/<peer-id>")
		}
		out = append(out, *p)
	}
	return out, nil
}
func hostOptions(identity crypto.PrivKey, listen []string) []libp2p.Option {
	if len(listen) == 0 {
		listen = []string{"/ip4/0.0.0.0/udp/0/quic-v1", "/ip4/0.0.0.0/tcp/0"}
	}
	opts := []libp2p.Option{libp2p.ListenAddrStrings(listen...), libp2p.NoTransports, libp2p.Transport(quic.NewTransport), libp2p.Transport(tcp.NewTCPTransport), libp2p.EnableRelay(), libp2p.EnableHolePunching(), libp2p.NATPortMap(), libp2p.UserAgent("HappySync/1")}
	if identity != nil {
		opts = append(opts, libp2p.Identity(identity))
	}
	return opts
}

// DiscoveryCID uses the HKDF output itself as the SHA2-256 multihash digest,
// not a file digest; this stable provider label contains no raw group identifier.
func DiscoveryCID(secret, syncID []byte) cid.Cid {
	digest := derive(secret, syncID, "HappySync/v1/discovery")
	hash, _ := mh.Encode(digest, mh.SHA2_256)
	return cid.NewCidV1(cid.Raw, hash)
}
func New(ctx context.Context, cfg Config) (*Node, error) {
	return newNode(ctx, cfg)
}
func newNode(ctx context.Context, cfg Config, extra ...libp2p.Option) (*Node, error) {
	if cfg.UploadBytesPerSecond < 0 || cfg.DownloadBytesPerSecond < 0 {
		return nil, errors.New("upload and download limits must be nonnegative")
	}
	if len(cfg.SyncID) != 16 || len(cfg.Secret) != 32 {
		return nil, errors.New("invalid group credential lengths")
	}
	manual, err := parsePeers(cfg.Peers)
	if err != nil {
		return nil, err
	}
	bootstrap, err := parsePeers(cfg.Bootstrap)
	if err != nil {
		return nil, err
	}
	relays, err := parsePeers(cfg.Relays)
	if err != nil {
		return nil, err
	}
	servers, err := parsePeers(cfg.DiscoveryServers)
	if err != nil {
		return nil, err
	}
	opts := hostOptions(cfg.Identity, cfg.Listen)
	opts = append(opts, extra...)
	reservedAddresses := newRelayAddresses()
	if len(relays) > 0 {
		opts = append(opts, libp2p.AddrsFactory(reservedAddresses.factory))
	}
	if len(relays) > 0 {
		opts = append(opts, libp2p.EnableAutoRelayWithStaticRelays(relays))
	}
	h, err := libp2p.New(opts...)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(ctx)
	n := &Node{host: h, ctx: ctx, cancel: cancel, key: derive(cfg.Secret, cfg.SyncID, "HappySync/v1/auth"), lookup: derive(cfg.Secret, cfg.SyncID, "HappySync/v1/discovery"), provider: DiscoveryCID(cfg.Secret, cfg.SyncID), authenticated: map[peer.ID]bool{}, candidates: map[peer.ID]time.Time{}, attempts: map[peer.ID]retryState{}, failedAuth: map[peer.ID][]time.Time{}, incoming: map[peer.ID]int{}, queue: make(chan peer.AddrInfo, 128), slots: make(chan struct{}, 64), outgoing: make(chan struct{}, 64), stats: Stats{MDNS: "disabled", DHT: "disabled", Discovery: "not configured", Relay: "not configured"}}
	n.candidatePeers = make(map[peer.ID]peer.AddrInfo)
	n.relayAddresses = reservedAddresses
	n.upload.rate, n.download.rate = cfg.UploadBytesPerSecond, cfg.DownloadBytesPerSecond
	n.stats.UploadRateLimit, n.stats.DownloadRateLimit = cfg.UploadBytesPerSecond, cfg.DownloadBytesPerSecond
	for _, p := range manual {
		if p.ID == h.ID() {
			n.stats.LastError = "configured peer has this device's identity; join with the share key instead of copying .HappySync"
		}
	}
	for _, id := range []protocol.ID{AuthProtocol, ManifestProtocol, BlockProtocol, AvailabilityProtocol, ControlProtocol} {
		h.SetStreamHandler(id, n.handle)
	}
	for range 8 {
		n.wg.Add(1)
		go n.dialWorker()
	}
	// mDNS may announce a candidate only once. Keep bounded contact information
	// so a transient initial dial failure still retries without a fresh announcement.
	n.loop(time.Second, func() {
		n.mu.RLock()
		var retry []peer.AddrInfo
		for id, info := range n.candidatePeers {
			state := n.attempts[id]
			if !state.active && !time.Now().Before(state.next) {
				retry = append(retry, info)
			}
		}
		n.mu.RUnlock()
		for _, info := range retry {
			n.HandlePeerFound(info)
		}
	})
	n.loop(5*time.Second, func() {
		for _, p := range manual {
			n.HandlePeerFound(p)
		}
	})
	if !cfg.DisableMDNS {
		n.mdns = mdns.NewMdnsService(h, "_happysync._udp", n)
		if err = n.mdns.Start(); err != nil {
			n.health("mdns", "unavailable")
		} else {
			n.health("mdns", "active")
		}
	}
	if !cfg.DisableDHT {
		if len(bootstrap) == 0 {
			bootstrap = dht.GetDefaultBootstrapPeerAddrInfos()
		}
		n.dht, err = dht.New(ctx, h, dht.Mode(dht.ModeAuto), dht.BootstrapPeers(bootstrap...))
		if err != nil {
			n.Close()
			return nil, err
		}
		n.health("dht", "starting (public acceptance pending)")
		n.loop(30*time.Second, func() { n.discoverDHT(bootstrap) })
	}
	if len(servers) > 0 {
		n.health("discovery", "starting")
		n.loop(30*time.Second, func() { n.discoverServers(servers) })
	}
	if len(relays) > 0 {
		n.health("relay", "configured")
		// Only due leases send requests. Checking each second permits short
		// reservations to renew before expiry and bounds failure retry cadence.
		n.loop(time.Second, func() { n.reserveRelays(relays) })
	}
	return n, nil
}
func (n *Node) loop(interval time.Duration, fn func()) {
	n.wg.Add(1)
	go func() {
		defer n.wg.Done()
		fn()
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-n.ctx.Done():
				return
			case <-ticker.C:
				fn()
			}
		}
	}()
}
func (n *Node) health(kind, value string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	switch kind {
	case "mdns":
		n.stats.MDNS = value
	case "dht":
		n.stats.DHT = value
	case "discovery":
		n.stats.Discovery = value
	case "relay":
		n.stats.Relay = value
	}
}
func (n *Node) SetBackend(b Backend) { n.mu.Lock(); n.backend = b; n.mu.Unlock() }
func (n *Node) PeerID() string       { return n.host.ID().String() }
func addresses(h host.Host) []string {
	var out []string
	for _, a := range h.Addrs() {
		out = append(out, a.String()+"/p2p/"+h.ID().String())
	}
	return out
}
func (n *Node) Addresses() []string {
	var out []string
	// libp2p sorts host addresses after applying its factory. Reapply only the
	// ordering here so hosted registration prioritizes live reserved circuits.
	for _, address := range n.relayAddresses.factory(n.host.Addrs()) {
		out = append(out, address.String()+"/p2p/"+n.host.ID().String())
	}
	return out
}
func (n *Node) Close() error {
	var err error
	n.closeOnce.Do(func() {
		n.cancel()
		if n.mdns != nil {
			_ = n.mdns.Close()
		}
		if n.dht != nil {
			_ = n.dht.Close()
		}
		err = n.host.Close()
		n.wg.Wait()
	})
	return err
}

type retryState struct {
	next     time.Time
	failures uint
	active   bool
}

// retryDelay applies equal jitter to exponential delays, bounded at five minutes.
func retryDelay(failures uint) time.Duration {
	if failures > 7 {
		failures = 7
	}
	cap := 5 * time.Second * time.Duration(1<<failures)
	if cap > 5*time.Minute {
		cap = 5 * time.Minute
	}
	return cap/2 + time.Duration(rand.Int64N(int64(cap/2)))
}
func (n *Node) HandlePeerFound(p peer.AddrInfo) {
	if p.ID == n.host.ID() || p.ID == "" || len(p.Addrs) == 0 || len(p.Addrs) > 16 {
		return
	}
	for _, addr := range p.Addrs {
		if !validContactAddress(addr) {
			return
		}
	}
	n.mu.Lock()
	if _, exists := n.candidates[p.ID]; !exists && len(n.candidates) >= 256 {
		var oldest peer.ID
		var at time.Time
		for id, seen := range n.candidates {
			if !n.attempts[id].active && (oldest == "" || seen.Before(at)) {
				oldest, at = id, seen
			}
		}
		if oldest == "" {
			n.mu.Unlock()
			return
		}
		delete(n.candidates, oldest)
		delete(n.candidatePeers, oldest)
		delete(n.attempts, oldest)
	}
	n.candidates[p.ID] = time.Now()
	n.candidatePeers[p.ID] = peer.AddrInfo{ID: p.ID, Addrs: append([]ma.Multiaddr(nil), p.Addrs...)}
	state := n.attempts[p.ID]
	if state.active || time.Now().Before(state.next) {
		n.mu.Unlock()
		return
	}
	state.active = true
	n.attempts[p.ID] = state
	n.mu.Unlock()
	select {
	case n.queue <- p:
	default:
		n.mu.Lock()
		state := n.attempts[p.ID]
		state.active = false
		n.attempts[p.ID] = state
		n.mu.Unlock()
	}
}
func (n *Node) dialWorker() {
	defer n.wg.Done()
	for {
		select {
		case <-n.ctx.Done():
			return
		case p := <-n.queue:
			ctx, cancel := context.WithTimeout(n.ctx, 15*time.Second)
			err := n.host.Connect(ctx, p)
			if err == nil {
				var stream netcore.Stream
				stream, err = n.open(ctx, p.ID, AuthProtocol)
				if err == nil {
					_ = stream.Close()
				}
			}
			cancel()
			n.mu.Lock()
			state := n.attempts[p.ID]
			state.active = false
			if err == nil {
				state.failures = 0
				state.next = time.Now().Add(30 * time.Second)
			} else {
				state.next = time.Now().Add(retryDelay(state.failures))
				if state.failures < 7 {
					state.failures++
				}
				n.stats.LastError = "peer connection or authentication unavailable; retrying"
			}
			n.attempts[p.ID] = state
			n.mu.Unlock()
		}
	}
}
func (n *Node) mark(id peer.ID) bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	if !n.authenticated[id] && len(n.authenticated) >= 32 {
		// Offline sessions can reauthenticate later; stale membership cannot occupy capacity forever.
		for p := range n.authenticated {
			if len(n.host.Network().ConnsToPeer(p)) == 0 {
				delete(n.authenticated, p)
			}
		}
	}
	if !n.authenticated[id] && len(n.authenticated) >= 32 {
		return false
	}
	n.authenticated[id] = true
	delete(n.failedAuth, id)
	return true
}
func (n *Node) Peers() []peer.ID {
	n.mu.RLock()
	defer n.mu.RUnlock()
	var out []peer.ID
	for id := range n.authenticated {
		if len(n.host.Network().ConnsToPeer(id)) > 0 {
			out = append(out, id)
		}
	}
	return out
}
func (n *Node) Stats() Stats {
	n.mu.RLock()
	out := n.stats
	out.Discovered = len(n.candidates)
	out.Authenticated = len(n.authenticated)
	n.mu.RUnlock()
	for _, id := range n.Peers() {
		out.Connected++
		direct := false
		for _, c := range n.host.Network().ConnsToPeer(id) {
			if !c.Stat().Limited {
				direct = true
			}
		}
		if direct {
			out.Direct++
		} else {
			out.Relayed++
		}
	}
	out.ReceivedBytes = n.received.Load()
	out.SentBytes = n.sent.Load()
	return out
}
func (n *Node) open(ctx context.Context, id peer.ID, p protocol.ID) (netcore.Stream, error) {
	select {
	case n.outgoing <- struct{}{}:
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-n.ctx.Done():
		return nil, n.ctx.Err()
	}
	release := func() { <-n.outgoing }
	ctx = netcore.WithAllowLimitedConn(ctx, "HappySync encrypted relay fallback")
	s, err := n.host.NewStream(ctx, id, p)
	if err != nil {
		release()
		return nil, err
	}
	s = &leasedStream{Stream: s, release: release}
	stop := context.AfterFunc(ctx, func() { _ = s.Reset() })
	if err = authenticate(s, n.key, true); err != nil {
		stop()
		_ = s.Reset()
		return nil, err
	}
	stop()
	if !n.mark(id) {
		_ = s.Reset()
		return nil, errors.New("authenticated peer capacity reached")
	}
	s = n.pace(s)
	deadline := time.Now().Add(n.streamTimeout())
	if d, ok := ctx.Deadline(); ok && d.Before(deadline) {
		deadline = d
	}
	_ = s.SetDeadline(deadline)
	return s, nil
}

type manifestRequest struct {
	Cursor string `json:"cursor"`
	Limit  int    `json:"limit"`
}
type manifestReply struct {
	Records []model.Record `json:"records"`
	Cursor  string         `json:"cursor"`
	Error   string         `json:"error,omitempty"`
}
type blockRequest struct {
	Hash string `json:"hash"`
	Size int64  `json:"size"`
}
type blockReply struct {
	Data  []byte `json:"data"`
	Error string `json:"error,omitempty"`
}

func (n *Node) handle(s netcore.Stream) {
	select {
	case n.slots <- struct{}{}:
	default:
		_ = s.Reset()
		return
	}
	defer func() { <-n.slots }()
	defer func() { _ = s.Close() }()
	remote := s.Conn().RemotePeer()
	n.mu.Lock()
	now := time.Now()
	for p, failures := range n.failedAuth {
		var recent []time.Time
		for _, at := range failures {
			if now.Sub(at) < time.Minute {
				recent = append(recent, at)
			}
		}
		if len(recent) == 0 {
			delete(n.failedAuth, p)
		} else {
			n.failedAuth[p] = recent
		}
	}
	if now.Sub(n.authWindow) >= time.Second {
		n.authWindow, n.authCount = now, 0
	}
	denied := n.incoming[remote] >= 8 || len(n.failedAuth[remote]) >= 12 || (!n.authenticated[remote] && (n.authCount >= 64 || (len(n.failedAuth) >= 256 && len(n.failedAuth[remote]) == 0)))
	if !denied {
		n.incoming[remote]++
		if !n.authenticated[remote] {
			n.authCount++
			n.failedAuth[remote] = append(n.failedAuth[remote], now)
		}
	}
	n.mu.Unlock()
	if denied {
		_ = s.Reset()
		return
	}
	defer func() {
		n.mu.Lock()
		n.incoming[remote]--
		if n.incoming[remote] == 0 {
			delete(n.incoming, remote)
		}
		n.mu.Unlock()
	}()
	if err := authenticate(s, n.key, false); err != nil {
		_ = s.Reset()
		return
	}
	if !n.mark(remote) {
		_ = s.Reset()
		return
	}
	if s.Protocol() == AuthProtocol {
		return
	}
	s = n.pace(s)
	_ = s.SetDeadline(time.Now().Add(n.streamTimeout()))
	n.mu.RLock()
	b := n.backend
	n.mu.RUnlock()
	if b == nil {
		_ = s.Reset()
		return
	}
	ctx, cancel := context.WithTimeout(n.ctx, 40*time.Second)
	defer cancel()
	switch s.Protocol() {
	case ControlProtocol:
		var req digestRequest
		if readFrame(s, &req, 1024) != nil || req.Version != 1 {
			_ = s.Reset()
			return
		}
		reply := digestReply{Error: "state summary unavailable"}
		if summarizer, ok := b.(interface {
			Digest(context.Context) (string, error)
		}); ok {
			digest, err := summarizer.Digest(ctx)
			if err == nil && validDigest(digest) {
				reply = digestReply{Digest: digest}
			}
		}
		if writeFrame(s, reply, 1024) != nil {
			_ = s.Reset()
		}
	case AvailabilityProtocol:
		var req availabilityRequest
		if readFrame(s, &req, 32768) != nil || len(req.Blocks) > 256 {
			_ = s.Reset()
			return
		}
		reply := availabilityReply{Available: make([]bool, len(req.Blocks))}
		probe, ok := b.(interface {
			HasBlock(context.Context, string, int64) bool
		})
		for i, block := range req.Blocks {
			if !validBlock(block.Hash, block.Size) {
				_ = s.Reset()
				return
			}
			if ok {
				reply.Available[i] = probe.HasBlock(ctx, block.Hash, block.Size)
			}
		}
		if writeFrame(s, reply, 4096) != nil {
			_ = s.Reset()
		}
	case ManifestProtocol:
		var req manifestRequest
		if readFrame(s, &req, 8192) != nil || len(req.Cursor) > 4096 || req.Limit < 1 || req.Limit > maxPage {
			_ = s.Reset()
			return
		}
		records, cursor, err := b.Snapshot(ctx, req.Cursor, req.Limit)
		reply := manifestReply{Records: records, Cursor: cursor}
		if err != nil || len(records) > req.Limit {
			reply = manifestReply{Error: "snapshot unavailable"}
		}
		if writeFrame(s, reply, maxFrame) != nil {
			_ = s.Reset()
		}
	case BlockProtocol:
		var req blockRequest
		if readFrame(s, &req, 1024) != nil || !validBlock(req.Hash, req.Size) {
			_ = s.Reset()
			return
		}
		data, err := b.ReadBlock(ctx, req.Hash, req.Size)
		reply := blockReply{Data: data}
		if err != nil || int64(len(data)) != req.Size || fmt.Sprintf("%x", blake3.Sum256(data)) != req.Hash {
			reply = blockReply{Error: "verified block unavailable"}
		} else {
			n.sent.Add(uint64(len(data)))
		}
		if writeFrame(s, reply, maxFrame) != nil {
			_ = s.Reset()
		}
	}
}

type digestRequest struct {
	Version int `json:"version"`
}
type digestReply struct {
	Digest string `json:"digest,omitempty"`
	Error  string `json:"error,omitempty"`
}

func validDigest(digest string) bool {
	decoded, err := hex.DecodeString(digest)
	return err == nil && len(decoded) == 32 && len(digest) == 64
}

// StateDigest offers a cheap equality check before requesting metadata pages.
// Unsupported peers/backends return errors so callers can fall back to snapshots.
func (n *Node) StateDigest(ctx context.Context, id peer.ID) (string, error) {
	s, err := n.open(ctx, id, ControlProtocol)
	if err != nil {
		return "", err
	}
	defer s.Close()
	stop := context.AfterFunc(ctx, func() { _ = s.Reset() })
	defer stop()
	if err := writeFrame(s, digestRequest{Version: 1}, 1024); err != nil {
		return "", err
	}
	var reply digestReply
	if err := readFrame(s, &reply, 1024); err != nil {
		return "", err
	}
	if reply.Error != "" {
		return "", errors.New("peer state summary unavailable")
	}
	if !validDigest(reply.Digest) {
		return "", errors.New("invalid peer state summary")
	}
	return reply.Digest, nil
}

type availabilityRequest struct {
	Blocks []model.Block `json:"blocks"`
}
type availabilityReply struct {
	Available []bool `json:"available"`
}

func (n *Node) BlockAvailability(ctx context.Context, id peer.ID, blocks []model.Block) ([]bool, error) {
	if len(blocks) > 256 {
		return nil, errors.New("availability request exceeds limit")
	}
	for _, block := range blocks {
		if !validBlock(block.Hash, block.Size) {
			return nil, errors.New("invalid block")
		}
	}
	s, err := n.open(ctx, id, AvailabilityProtocol)
	if err != nil {
		return nil, err
	}
	defer s.Close()
	stop := context.AfterFunc(ctx, func() { _ = s.Reset() })
	defer stop()
	if err = writeFrame(s, availabilityRequest{blocks}, 32768); err != nil {
		return nil, err
	}
	var reply availabilityReply
	if err = readFrame(s, &reply, 4096); err != nil {
		return nil, err
	}
	if len(reply.Available) != len(blocks) {
		return nil, errors.New("invalid availability response")
	}
	return reply.Available, nil
}
func validBlock(hash string, size int64) bool {
	b, e := hex.DecodeString(hash)
	return e == nil && len(b) == 32 && size >= 0 && size <= model.BlockSize
}
func (n *Node) FetchRecords(ctx context.Context, id peer.ID, cursor string, limit int) ([]model.Record, string, error) {
	if limit < 1 || limit > maxPage || len(cursor) > 4096 {
		return nil, "", errors.New("invalid snapshot page request")
	}
	s, err := n.open(ctx, id, ManifestProtocol)
	if err != nil {
		return nil, "", err
	}
	defer s.Close()
	stop := context.AfterFunc(ctx, func() { _ = s.Reset() })
	defer stop()
	if err = writeFrame(s, manifestRequest{cursor, limit}, 8192); err != nil {
		return nil, "", err
	}
	var reply manifestReply
	if err = readFrame(s, &reply, maxFrame); err != nil {
		return nil, "", err
	}
	if reply.Error != "" {
		return nil, "", errors.New("peer snapshot unavailable")
	}
	if len(reply.Records) > limit || len(reply.Cursor) > 4096 {
		return nil, "", errors.New("invalid snapshot response")
	}
	return reply.Records, reply.Cursor, nil
}
func (n *Node) FetchBlock(ctx context.Context, id peer.ID, hash string, size int64) ([]byte, error) {
	if !validBlock(hash, size) {
		return nil, errors.New("invalid block request")
	}
	s, err := n.open(ctx, id, BlockProtocol)
	if err != nil {
		return nil, err
	}
	defer s.Close()
	stop := context.AfterFunc(ctx, func() { _ = s.Reset() })
	defer stop()
	if err = writeFrame(s, blockRequest{hash, size}, 1024); err != nil {
		return nil, err
	}
	var reply blockReply
	if err = readFrame(s, &reply, maxFrame); err != nil {
		return nil, err
	}
	if reply.Error != "" {
		return nil, errors.New("peer block unavailable")
	}
	if int64(len(reply.Data)) != size || fmt.Sprintf("%x", blake3.Sum256(reply.Data)) != hash {
		return nil, errors.New("peer returned corrupt block")
	}
	n.received.Add(uint64(len(reply.Data)))
	return reply.Data, nil
}
