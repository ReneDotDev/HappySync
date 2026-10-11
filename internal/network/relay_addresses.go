package network

import (
	"sort"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/p2p/protocol/circuitv2/client"
	ma "github.com/multiformats/go-multiaddr"
)

const maxReservedRelayAddresses = 16

type reservedRelay struct {
	addresses   []ma.Multiaddr
	expires     time.Time
	nextAttempt time.Time
}

// relayAddresses publishes successfully obtained, unexpired reservations.
// Its host address factory never calls host.Addrs: Identify, hosted discovery,
// and DHT provider publication therefore share these paths without recursion.
type relayAddresses struct {
	mu     sync.Mutex
	leases map[peer.ID]reservedRelay
}

func newRelayAddresses() *relayAddresses {
	return &relayAddresses{leases: make(map[peer.ID]reservedRelay)}
}

func (r *relayAddresses) due(id peer.ID, now time.Time) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return !now.Before(r.leases[id].nextAttempt)
}

func (r *relayAddresses) refresh(p peer.AddrInfo, reservation *client.Reservation, now time.Time) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	previous := r.leases[p.ID]
	addresses := reservationAddresses(p, reservation, now)
	if len(addresses) == 0 {
		// Renewal failure does not revoke a still-valid reservation. Its actual
		// expiry nevertheless excludes it from every future advertisement.
		previous.nextAttempt = now.Add(30 * time.Second)
		if now.Before(previous.expires) {
			if retry := now.Add(previous.expires.Sub(now) / 2); retry.Before(previous.nextAttempt) {
				previous.nextAttempt = retry
			}
		} else {
			previous.addresses = nil
			previous.expires = time.Time{}
		}
		if _, exists := r.leases[p.ID]; exists || len(r.leases) < 256 {
			r.leases[p.ID] = previous
		}
		return now.Before(previous.expires) && len(previous.addresses) > 0
	}
	if _, exists := r.leases[p.ID]; !exists && len(r.leases) >= 256 {
		return false
	}
	ttl := reservation.Expiration.Sub(now)
	refreshAfter := ttl - ttl/4
	if refreshAfter > 10*time.Minute {
		refreshAfter = 10 * time.Minute
	}
	r.leases[p.ID] = reservedRelay{addresses: addresses, expires: reservation.Expiration,
		nextAttempt: now.Add(refreshAfter)}
	return true
}

func reservationAddresses(p peer.AddrInfo, reservation *client.Reservation, now time.Time) []ma.Multiaddr {
	if reservation == nil || !now.Before(reservation.Expiration) {
		return nil
	}
	if reservation.Voucher != nil && reservation.Voucher.Relay != p.ID {
		return nil
	}
	// Prefer the reservation's advertised endpoints. A client on the relay
	// machine may dial localhost but needs its public endpoint for other peers.
	endpoints := reservation.Addrs
	if len(endpoints) == 0 {
		// Private/LAN relays can omit endpoints from the reservation response.
		endpoints = p.Addrs
	}
	seen := make(map[string]bool)
	var out []ma.Multiaddr
	for _, endpoint := range endpoints {
		if !validContactAddress(endpoint) {
			continue
		}
		peerComponents := 0
		nestedCircuit := false
		for _, protocol := range endpoint.Protocols() {
			if protocol.Code == ma.P_P2P {
				peerComponents++
			}
			if protocol.Code == ma.P_CIRCUIT {
				nestedCircuit = true
			}
		}
		if nestedCircuit || peerComponents > 1 {
			continue
		}
		if peerComponents == 0 {
			endpoint = endpoint.Encapsulate(ma.StringCast("/p2p/" + p.ID.String()))
		} else if id, err := peer.IDFromP2PAddr(endpoint); err != nil || id != p.ID {
			continue
		}
		circuit := endpoint.Encapsulate(ma.StringCast("/p2p-circuit"))
		// Leave space for the destination Peer ID in hosted discovery addresses.
		if len(circuit.String()) > 400 || seen[circuit.String()] {
			continue
		}
		seen[circuit.String()] = true
		out = append(out, circuit)
		if len(out) == maxReservedRelayAddresses {
			break
		}
	}
	return out
}

func (r *relayAddresses) factory(direct []ma.Multiaddr) []ma.Multiaddr {
	r.mu.Lock()
	defer r.mu.Unlock()
	now := time.Now()
	ids := make([]peer.ID, 0, len(r.leases))
	for id, lease := range r.leases {
		if now.Before(lease.expires) {
			ids = append(ids, id)
		}
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i].String() < ids[j].String() })
	seen := make(map[string]bool)
	out := make([]ma.Multiaddr, 0, maxReservedRelayAddresses+len(direct))
	for _, id := range ids {
		for _, address := range r.leases[id].addresses {
			if !seen[address.String()] && len(out) < maxReservedRelayAddresses {
				out = append(out, address)
				seen[address.String()] = true
			}
		}
	}
	// Prioritize reserved paths under hosted discovery's 16-address cap while
	// preserving direct endpoints and paths managed by upstream AutoRelay.
	for _, address := range direct {
		if !seen[address.String()] {
			out = append(out, address)
			seen[address.String()] = true
		}
	}
	return out
}
