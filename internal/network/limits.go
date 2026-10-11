package network

import (
	"context"
	"errors"
	"io"
	"net/netip"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/libp2p/go-libp2p/core/host"
	netcore "github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	relayproto "github.com/libp2p/go-libp2p/p2p/protocol/circuitv2/proto"
	ma "github.com/multiformats/go-multiaddr"
)

func validContactAddress(addr ma.Multiaddr) bool {
	if addr == nil || len(addr.String()) > 512 {
		return false
	}
	raw := addr.String()
	if strings.Count(raw, "/p2p-circuit") > 1 {
		return false
	}
	for _, code := range []int{ma.P_IP4, ma.P_IP6} {
		if value, err := addr.ValueForProtocol(code); err == nil {
			ip, err := netip.ParseAddr(value)
			if err != nil || ip.IsUnspecified() || ip.IsMulticast() {
				return false
			}
		}
	}
	transport := false
	if _, err := addr.ValueForProtocol(ma.P_TCP); err == nil {
		transport = true
	}
	if _, err := addr.ValueForProtocol(ma.P_QUIC_V1); err == nil {
		if _, err = addr.ValueForProtocol(ma.P_UDP); err == nil {
			transport = true
		}
	}
	endpoint := false
	for _, code := range []int{ma.P_IP4, ma.P_IP6, ma.P_DNS, ma.P_DNS4, ma.P_DNS6} {
		if value, err := addr.ValueForProtocol(code); err == nil && value != "" && len(value) <= 253 {
			endpoint = true
		}
	}
	return transport && endpoint
}

// relayAllowlist gates infrastructure usage by transport-authenticated identity.
// It does not participate in group membership or inspect encrypted sync content.
type relayAllowlist map[peer.ID]bool

func (a relayAllowlist) AllowReserve(id peer.ID, _ ma.Multiaddr) bool { return a[id] }
func (a relayAllowlist) AllowConnect(src peer.ID, _ ma.Multiaddr, dest peer.ID) bool {
	return a[src] && a[dest]
}

// leasedStream releases application concurrency on every terminal stream operation.
type leasedStream struct {
	netcore.Stream
	once    sync.Once
	release func()
}

func (s *leasedStream) Close() error { defer s.once.Do(s.release); return s.Stream.Close() }
func (s *leasedStream) Reset() error { defer s.once.Do(s.release); return s.Stream.Reset() }
func (s *leasedStream) ResetWithError(code netcore.StreamErrorCode) error {
	defer s.once.Do(s.release)
	return s.Stream.ResetWithError(code)
}

// pacedBudget is a no-burst, shared byte scheduler. Reservations happen before
// forwarding and streams wait without holding either the budget or table lock.
type pacedBudget struct {
	mu   sync.Mutex
	rate int64
	next time.Time
}

func (b *pacedBudget) reserve(size int) time.Time {
	b.mu.Lock()
	defer b.mu.Unlock()
	now := time.Now()
	if b.next.Before(now) {
		b.next = now
	}
	b.next = b.next.Add(time.Duration(int64(size) * int64(time.Second) / b.rate))
	return b.next
}
func awaitBudget(ctx context.Context, closed <-chan struct{}, deadline, at time.Time) error {
	if !deadline.IsZero() && !at.Before(deadline) {
		return context.DeadlineExceeded
	}
	timer := time.NewTimer(time.Until(at))
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-closed:
		return io.ErrClosedPipe
	case <-timer.C:
		return nil
	}
}

type peerBudget struct {
	budget pacedBudget
	refs   int
}

// relayPacer wraps only circuit-v2 hop/stop streams used by the mature upstream
// relay. It preserves protocol, resource scopes, and the opaque encrypted bytes.
// Global pacing counts forwarded bytes once; each endpoint's combined ingress
// and egress shares a per-peer budget. No application plaintext is inspected.
type relayPacer struct {
	host.Host
	ctx        context.Context
	total      pacedBudget
	peerRate   int64
	mu         sync.Mutex
	peers      map[peer.ID]*peerBudget
	maxStreams int
	streams    int
	forwarded  atomic.Uint64
}

func newRelayPacer(ctx context.Context, h host.Host, total, perPeer int64, maxStreams int) *relayPacer {
	return &relayPacer{Host: h, ctx: ctx, total: pacedBudget{rate: total}, peerRate: perPeer, peers: map[peer.ID]*peerBudget{}, maxStreams: maxStreams}
}
func (h *relayPacer) wrap(s netcore.Stream) (netcore.Stream, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.streams >= h.maxStreams {
		_ = s.Reset()
		return nil, errors.New("relay stream capacity reached")
	}
	id := s.Conn().RemotePeer()
	budget := h.peers[id]
	if budget == nil {
		budget = &peerBudget{budget: pacedBudget{rate: h.peerRate}}
		h.peers[id] = budget
	}
	budget.refs++
	h.streams++
	wrapped := &pacedStream{Stream: s, owner: h, peer: budget, closed: make(chan struct{})}
	wrapped.release = func() {
		close(wrapped.closed)
		h.mu.Lock()
		budget.refs--
		h.streams--
		if budget.refs == 0 {
			delete(h.peers, id)
		}
		h.mu.Unlock()
	}
	return wrapped, nil
}
func (h *relayPacer) SetStreamHandler(id protocol.ID, handler netcore.StreamHandler) {
	if id != relayproto.ProtoIDv2Hop {
		h.Host.SetStreamHandler(id, handler)
		return
	}
	h.Host.SetStreamHandler(id, func(s netcore.Stream) {
		wrapped, err := h.wrap(s)
		if err == nil {
			handler(wrapped)
		}
	})
}
func (h *relayPacer) NewStream(ctx context.Context, id peer.ID, protocols ...protocol.ID) (netcore.Stream, error) {
	s, err := h.Host.NewStream(ctx, id, protocols...)
	if err != nil {
		return nil, err
	}
	if s.Protocol() == relayproto.ProtoIDv2Stop {
		return h.wrap(s)
	}
	return s, nil
}

type pacedStream struct {
	netcore.Stream
	owner                       *relayPacer
	peer                        *peerBudget
	mu                          sync.Mutex
	readDeadline, writeDeadline time.Time
	once                        sync.Once
	release                     func()
	closed                      chan struct{}
}

const paceChunk = 2048

func (s *pacedStream) deadline(write bool) time.Time {
	s.mu.Lock()
	defer s.mu.Unlock()
	if write {
		return s.writeDeadline
	}
	return s.readDeadline
}
func (s *pacedStream) Read(p []byte) (int, error) {
	if len(p) > paceChunk {
		p = p[:paceChunk]
	}
	n, err := s.Stream.Read(p)
	if n > 0 {
		if waitErr := awaitBudget(s.owner.ctx, s.closed, s.deadline(false), s.peer.budget.reserve(n)); waitErr != nil {
			return 0, waitErr
		}
	}
	return n, err
}
func (s *pacedStream) Write(p []byte) (int, error) {
	written := 0
	for len(p) > 0 {
		chunk := p
		if len(chunk) > paceChunk {
			chunk = chunk[:paceChunk]
		}
		at := s.owner.total.reserve(len(chunk))
		if peerAt := s.peer.budget.reserve(len(chunk)); peerAt.After(at) {
			at = peerAt
		}
		if err := awaitBudget(s.owner.ctx, s.closed, s.deadline(true), at); err != nil {
			return written, err
		}
		n, err := s.Stream.Write(chunk)
		s.owner.forwarded.Add(uint64(n))
		written += n
		if err != nil {
			return written, err
		}
		if n != len(chunk) {
			return written, io.ErrShortWrite
		}
		p = p[n:]
	}
	return written, nil
}
func (s *pacedStream) SetDeadline(t time.Time) error {
	s.mu.Lock()
	s.readDeadline, s.writeDeadline = t, t
	s.mu.Unlock()
	return s.Stream.SetDeadline(t)
}
func (s *pacedStream) SetReadDeadline(t time.Time) error {
	s.mu.Lock()
	s.readDeadline = t
	s.mu.Unlock()
	return s.Stream.SetReadDeadline(t)
}
func (s *pacedStream) SetWriteDeadline(t time.Time) error {
	s.mu.Lock()
	s.writeDeadline = t
	s.mu.Unlock()
	return s.Stream.SetWriteDeadline(t)
}
func (s *pacedStream) Close() error { defer s.once.Do(s.release); return s.Stream.Close() }
func (s *pacedStream) Reset() error { defer s.once.Do(s.release); return s.Stream.Reset() }
func (s *pacedStream) ResetWithError(code netcore.StreamErrorCode) error {
	defer s.once.Do(s.release)
	return s.Stream.ResetWithError(code)
}

// bandwidthStream shares separate ingress/egress budgets across every
// authenticated application stream of a node, including metadata. Authentication
// remains independently deadline bounded, so throttling cannot extend outsider
// handshake lifetimes. Framing and encrypted transport remain unchanged.
type bandwidthStream struct {
	netcore.Stream
	ctx                         context.Context
	readBudget, writeBudget     *pacedBudget
	mu                          sync.Mutex
	readDeadline, writeDeadline time.Time
	once                        sync.Once
	closed                      chan struct{}
}

func (n *Node) pace(s netcore.Stream) netcore.Stream {
	if n.upload.rate == 0 && n.download.rate == 0 {
		return s
	}
	return &bandwidthStream{Stream: s, ctx: n.ctx, readBudget: &n.download, writeBudget: &n.upload, closed: make(chan struct{})}
}

func (n *Node) streamTimeout() time.Duration {
	duration := 45 * time.Second
	for _, rate := range []int64{n.upload.rate, n.download.rate} {
		if rate > 0 {
			d := 45*time.Second + time.Duration(int64(maxFrame)*int64(time.Second)/rate)
			if d > duration {
				duration = d
			}
		}
	}
	return duration
}

func (s *bandwidthStream) wait(size int, write bool) error {
	s.mu.Lock()
	budget, deadline := s.readBudget, s.readDeadline
	if write {
		budget, deadline = s.writeBudget, s.writeDeadline
	}
	s.mu.Unlock()
	if budget.rate == 0 {
		return nil
	}
	return awaitBudget(s.ctx, s.closed, deadline, budget.reserve(size))
}
func (s *bandwidthStream) Read(p []byte) (int, error) {
	if len(p) > paceChunk {
		p = p[:paceChunk]
	}
	n, err := s.Stream.Read(p)
	if n > 0 {
		if waitErr := s.wait(n, false); waitErr != nil {
			return 0, waitErr
		}
	}
	return n, err
}
func (s *bandwidthStream) Write(p []byte) (int, error) {
	written := 0
	for len(p) > 0 {
		chunk := p
		if len(chunk) > paceChunk {
			chunk = chunk[:paceChunk]
		}
		if err := s.wait(len(chunk), true); err != nil {
			return written, err
		}
		n, err := s.Stream.Write(chunk)
		written += n
		if err != nil {
			return written, err
		}
		if n != len(chunk) {
			return written, io.ErrShortWrite
		}
		p = p[n:]
	}
	return written, nil
}
func (s *bandwidthStream) SetDeadline(t time.Time) error {
	s.mu.Lock()
	s.readDeadline, s.writeDeadline = t, t
	s.mu.Unlock()
	return s.Stream.SetDeadline(t)
}
func (s *bandwidthStream) SetReadDeadline(t time.Time) error {
	s.mu.Lock()
	s.readDeadline = t
	s.mu.Unlock()
	return s.Stream.SetReadDeadline(t)
}
func (s *bandwidthStream) SetWriteDeadline(t time.Time) error {
	s.mu.Lock()
	s.writeDeadline = t
	s.mu.Unlock()
	return s.Stream.SetWriteDeadline(t)
}
func (s *bandwidthStream) Close() error {
	defer s.once.Do(func() { close(s.closed) })
	return s.Stream.Close()
}
func (s *bandwidthStream) Reset() error {
	defer s.once.Do(func() { close(s.closed) })
	return s.Stream.Reset()
}
func (s *bandwidthStream) ResetWithError(code netcore.StreamErrorCode) error {
	defer s.once.Do(func() { close(s.closed) })
	return s.Stream.ResetWithError(code)
}
