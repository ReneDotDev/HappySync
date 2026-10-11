package engine

import (
	"github.com/happysync/happysync/internal/config"
	"github.com/libp2p/go-libp2p/core/peer"
)

func peerID(c *config.Config) string { p, _ := peer.IDFromPrivateKey(c.Identity); return p.String() }
